//! Worker-owned process supervision, persistence, deadlines, and captured output.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ExitStatus};
#[cfg(test)]
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::fd::AsFd;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use conductor_core::{
    Architecture as WorkerArchitecture, CommandEnvelope, CommandId, EnforcementCapability,
    ExecutionBackend as WorkerExecutionBackend, InspectRun, JobSpec, LogStream,
    MAX_LOG_CHUNK_BYTES, MAX_WORKER_CAPACITY, MAX_WORKER_INVENTORY_PAGE_SIZE, OperatingSystem,
    ReadRunLogs, ReadinessProbe, ReadinessProbeProtocol, ReadinessState, ResolveRun,
    RunFailureReason, RunLogChunk, RunLogStatus, RunObservation, RunState, RunTerminationReason,
    StartRun, StopRun, WorkerCapabilities, WorkerDescriptor, WorkerId, WorkerInventory,
    WorkerInventoryEntry, WorkerInventoryPage, WorkerResourceCapacity, WorkerStatus,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use tokio::runtime::Runtime;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, oneshot, watch};

#[cfg(unix)]
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};

const BUSY_TIMEOUT: Duration = Duration::from_millis(250);
const SCHEMA_VERSION: i64 = 4;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_PERSISTENCE_BUDGET: Duration = Duration::from_secs(2);
const PERSISTENCE_BLOCKING_CAPACITY: usize = 2;
const OUTPUT_DRAIN_GRACE: Duration = Duration::from_millis(500);
pub const DEFAULT_LOG_MAX_BYTES_PER_STREAM: u64 = 8 * 1024 * 1024;
pub const DEFAULT_LOG_RETENTION_RUNS: usize = 100;
pub const MAX_LOG_MAX_BYTES_PER_STREAM: u64 = 1024 * 1024 * 1024;
pub const MAX_LOG_RETENTION_RUNS: usize = 10_000;

/// Worker execution seam implemented by the host-specific process crate.
pub trait ExecutionBackend: Send + Sync {
    /// Spawn one job in a dedicated process group with stdin closed and output piped.
    fn spawn(&self, run_id: &str, job: &JobSpec) -> io::Result<Child>;

    /// Request graceful termination of every process in the execution group.
    fn terminate_process_group(&self, group_id: u32) -> io::Result<()>;

    /// Force-kill every process still in the execution's process group.
    fn kill_process_group(&self, group_id: u32) -> io::Result<()>;

    /// Finish backend-owned resources after the process subtree has been killed.
    /// Implementations must bound this operation because it runs on the reader reaper.
    fn cleanup_execution(&self, _group_id: u32) -> io::Result<()> {
        Ok(())
    }

    /// Kernel enforcement guarantees this backend has been configured to apply.
    fn enforcement_capabilities(&self) -> BTreeSet<EnforcementCapability> {
        BTreeSet::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerError {
    CapacityReached,
    RunConflict,
    WorkerMismatch,
    ShuttingDown,
    LogsNotFound,
    LogsPruned,
    StorageUnavailable,
}

/// A worker report persisted with its command result until the manager
/// acknowledges the report sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredWorkerReport {
    pub command_id: CommandId,
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub report_sequence: u64,
    pub result_json: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogPolicy {
    pub max_bytes_per_stream: u64,
    pub retention_runs: usize,
}

impl Default for LogPolicy {
    fn default() -> Self {
        Self {
            max_bytes_per_stream: DEFAULT_LOG_MAX_BYTES_PER_STREAM,
            retention_runs: DEFAULT_LOG_RETENTION_RUNS,
        }
    }
}

struct WorkerState {
    connection: Connection,
    active_groups: BTreeMap<String, ActiveGroup>,
}

#[derive(Clone)]
struct ActiveGroup {
    group_id: u32,
    intent: Arc<AtomicU8>,
    notify: Arc<Notify>,
}

struct ReaderReapRequest {
    child: Option<Child>,
    readers: Vec<JoinHandle<io::Result<()>>>,
    completion: Option<oneshot::Sender<io::Result<()>>>,
    cleanup_permit: OwnedSemaphorePermit,
    cleanup: Option<Box<dyn FnMut() -> io::Result<()> + Send>>,
}

#[derive(Clone)]
struct ReaderReaper {
    sender: SyncSender<ReaderReapRequest>,
    pending: Arc<AtomicUsize>,
    overflow: Arc<Mutex<VecDeque<ReaderReapRequest>>>,
    #[cfg(test)]
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl ReaderReaper {
    fn new() -> io::Result<Self> {
        let (sender, receiver) = sync_channel::<ReaderReapRequest>(MAX_WORKER_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let reaper_pending = Arc::clone(&pending);
        let overflow = Arc::new(Mutex::new(VecDeque::new()));
        let reaper_overflow = Arc::clone(&overflow);
        #[cfg(test)]
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        #[cfg(test)]
        let reaper_gate = Arc::clone(&gate);
        thread::Builder::new()
            .name("conductor-reader-reaper".to_owned())
            .spawn(move || {
                loop {
                    let queued = reaper_overflow
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .pop_front();
                    let request = match queued {
                        Some(request) => request,
                        None => match receiver.recv() {
                            Ok(request) => request,
                            Err(_) => break,
                        },
                    };
                    #[cfg(test)]
                    {
                        let (paused, changed) = &*reaper_gate;
                        let mut paused = paused
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        while *paused {
                            paused = changed
                                .wait(paused)
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                        }
                        drop(paused);
                    }
                    finish_reap_request(request, &reaper_pending);
                }
            })?;
        Ok(Self {
            sender,
            pending,
            overflow,
            #[cfg(test)]
            gate,
        })
    }

    fn enqueue(
        &self,
        child: Option<Child>,
        readers: Vec<JoinHandle<io::Result<()>>>,
        cleanup_permit: OwnedSemaphorePermit,
        cleanup: Option<Box<dyn FnMut() -> io::Result<()> + Send>>,
    ) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let (completion, receiver) = oneshot::channel();
        if readers.is_empty() && child.is_none() && cleanup.is_none() {
            let _ = completion.send(Ok(()));
            return Ok(receiver);
        }
        self.pending.fetch_add(1, Ordering::SeqCst);
        match self.sender.try_send(ReaderReapRequest {
            child,
            readers,
            completion: Some(completion),
            cleanup_permit,
            cleanup,
        }) {
            Ok(()) => {}
            Err(TrySendError::Full(request)) => {
                self.overflow
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push_back(request);
            }
            Err(TrySendError::Disconnected(request)) => {
                enqueue_detached_fallback(request, Arc::clone(&self.pending), &self.overflow)?;
            }
        }
        Ok(receiver)
    }

    fn enqueue_detached(
        &self,
        child: Option<Child>,
        readers: Vec<JoinHandle<io::Result<()>>>,
        cleanup_permit: OwnedSemaphorePermit,
        cleanup: Option<Box<dyn FnMut() -> io::Result<()> + Send>>,
    ) {
        if readers.is_empty() && child.is_none() && cleanup.is_none() {
            return;
        }
        self.pending.fetch_add(1, Ordering::SeqCst);
        let request = ReaderReapRequest {
            child,
            readers,
            completion: None,
            cleanup_permit,
            cleanup,
        };
        match self.sender.try_send(request) {
            Ok(()) => {}
            Err(TrySendError::Full(request)) => {
                self.overflow
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push_back(request);
            }
            Err(TrySendError::Disconnected(request)) => {
                if let Err(error) =
                    enqueue_detached_fallback(request, Arc::clone(&self.pending), &self.overflow)
                {
                    emit_worker_diagnostic("reader_reaper_unavailable", "", &error);
                }
            }
        }
    }

    fn pending(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn pause_for_test(&self) {
        *self
            .gate
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    }

    #[cfg(test)]
    fn resume_for_test(&self) {
        *self
            .gate
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
        self.gate.1.notify_all();
    }
}

fn finish_reap_request(request: ReaderReapRequest, pending: &AtomicUsize) {
    let ReaderReapRequest {
        child,
        readers,
        completion,
        cleanup_permit: _cleanup_permit,
        cleanup,
    } = request;
    let result = reap_child_and_readers(child, readers);
    if let Some(mut cleanup) = cleanup {
        let mut next_diagnostic = std::time::Instant::now();
        loop {
            match cleanup() {
                Ok(()) => break,
                Err(error) => {
                    if std::time::Instant::now() >= next_diagnostic {
                        emit_worker_diagnostic("execution_cleanup_retry", "", &error);
                        next_diagnostic = std::time::Instant::now() + Duration::from_secs(5);
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }
    pending.fetch_sub(1, Ordering::SeqCst);
    if let Some(completion) = completion {
        let _ = completion.send(result);
    } else if let Err(error) = result {
        emit_worker_diagnostic("execution_cleanup_failed", "", &error);
    }
}

fn enqueue_detached_fallback(
    request: ReaderReapRequest,
    pending: Arc<AtomicUsize>,
    overflow: &Mutex<VecDeque<ReaderReapRequest>>,
) -> io::Result<()> {
    let request = Arc::new(Mutex::new(Some(request)));
    let thread_request = Arc::clone(&request);
    let spawn = thread::Builder::new()
        .name("conductor-reader-reaper-fallback".to_owned())
        .spawn(move || {
            if let Ok(mut request) = thread_request.lock()
                && let Some(request) = request.take()
            {
                finish_reap_request(request, &pending);
            }
        });
    if let Err(error) = spawn {
        if let Ok(mut request) = request.lock()
            && let Some(request) = request.take()
        {
            overflow
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push_back(request);
        }
        return Err(error);
    }
    Ok(())
}

fn join_readers(readers: Vec<JoinHandle<io::Result<()>>>) -> io::Result<()> {
    let mut first_error = None;
    for reader in readers {
        match reader.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error.get_or_insert(io::Error::other("output reader panicked"));
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn reap_child_and_readers(
    child: Option<Child>,
    readers: Vec<JoinHandle<io::Result<()>>>,
) -> io::Result<()> {
    let mut first_error = child.and_then(|mut child| child.wait().err());
    if let Err(error) = join_readers(readers) {
        first_error.get_or_insert(error);
    }
    first_error.map_or(Ok(()), Err)
}

/// Owns a spawned process until supervision takes responsibility for it.
/// Dropping the guard is a last-resort cleanup path; callers should use
/// `cleanup` when they need to observe cleanup failures.
struct SpawnGuard {
    backend: Arc<dyn ExecutionBackend>,
    reader_reaper: ReaderReaper,
    group_id: u32,
    child: Option<Child>,
    readers: Vec<JoinHandle<io::Result<()>>>,
    cleanup_permit: Option<OwnedSemaphorePermit>,
    cancel_readers: Arc<AtomicBool>,
    cleaned: bool,
}

impl SpawnGuard {
    fn cleanup_callback(&self) -> Box<dyn FnMut() -> io::Result<()> + Send> {
        let backend = Arc::clone(&self.backend);
        let group_id = self.group_id;
        Box::new(move || backend.cleanup_execution(group_id))
    }

    fn cleanup(&mut self) -> io::Result<()> {
        if self.cleaned {
            return Ok(());
        }
        self.cancel_readers.store(true, Ordering::SeqCst);
        let mut first_error = self.backend.kill_process_group(self.group_id).err();
        if let Some(child) = &mut self.child {
            if let Err(error) = child.kill()
                && error.kind() != io::ErrorKind::InvalidInput
            {
                first_error.get_or_insert(error);
            }
            if let Err(error) = child.wait() {
                first_error.get_or_insert(error);
            }
            self.child.take();
        }
        if let Err(error) = join_readers(std::mem::take(&mut self.readers)) {
            first_error.get_or_insert(error);
        }
        if first_error.is_none() {
            self.cleaned = true;
        }
        first_error.map_or(Ok(()), Err)
    }

    fn enqueue_readers(&mut self) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let cleanup_permit = self
            .cleanup_permit
            .take()
            .ok_or_else(|| io::Error::other("cleanup reservation is missing"))?;
        self.reader_reaper.enqueue(
            self.child.take(),
            std::mem::take(&mut self.readers),
            cleanup_permit,
            Some(self.cleanup_callback()),
        )
    }
}

trait CaptureRead: Read + Send {
    fn wait_readable(&self, cancelled: &AtomicBool) -> io::Result<bool>;
}

#[cfg(unix)]
impl<T: Read + Send + AsFd> CaptureRead for T {
    fn wait_readable(&self, cancelled: &AtomicBool) -> io::Result<bool> {
        if cancelled.load(Ordering::SeqCst) {
            return Ok(false);
        }
        let mut descriptors = [PollFd::new(
            self.as_fd(),
            PollFlags::POLLIN | PollFlags::POLLHUP | PollFlags::POLLERR,
        )];
        let ready = poll(&mut descriptors, PollTimeout::from(50_u16)).map_err(io::Error::from)?;
        Ok(ready > 0 && !cancelled.load(Ordering::SeqCst))
    }
}

#[cfg(not(unix))]
impl<T: Read + Send> CaptureRead for T {
    fn wait_readable(&self, cancelled: &AtomicBool) -> io::Result<bool> {
        Ok(!cancelled.load(Ordering::SeqCst))
    }
}

impl Drop for SpawnGuard {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        self.cancel_readers.store(true, Ordering::SeqCst);
        let _ = self.backend.kill_process_group(self.group_id);
        if let Some(child) = &mut self.child {
            let _ = child.kill();
        }
        if let Some(cleanup_permit) = self.cleanup_permit.take() {
            self.reader_reaper.enqueue_detached(
                self.child.take(),
                std::mem::take(&mut self.readers),
                cleanup_permit,
                Some(self.cleanup_callback()),
            );
        }
    }
}

const INTENT_NONE: u8 = 0;
const INTENT_CANCEL: u8 = 1;
const INTENT_TIMEOUT: u8 = 2;
const INTENT_SHUTDOWN: u8 = 3;

struct SharedWorkerState(Mutex<WorkerState>);

struct PendingPersistence {
    observations: Mutex<BTreeMap<String, RunObservation>>,
    shutdown_exhausted: Mutex<BTreeSet<String>>,
    degraded: AtomicBool,
}

impl PendingPersistence {
    fn insert(&self, observation: RunObservation) -> io::Result<()> {
        self.degraded.store(true, Ordering::SeqCst);
        self.observations
            .lock()
            .map_err(|_| io::Error::other("pending persistence lock poisoned"))?
            .insert(observation.run_id().to_string(), observation);
        Ok(())
    }

    fn remove(&self, run_id: &str) -> io::Result<()> {
        let mut observations = self
            .observations
            .lock()
            .map_err(|_| io::Error::other("pending persistence lock poisoned"))?;
        observations.remove(run_id);
        self.shutdown_exhausted
            .lock()
            .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?
            .remove(run_id);
        self.degraded
            .store(!observations.is_empty(), Ordering::SeqCst);
        Ok(())
    }

    fn mark_shutdown_exhausted(&self, run_id: &str) -> io::Result<()> {
        self.shutdown_exhausted
            .lock()
            .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?
            .insert(run_id.to_owned());
        Ok(())
    }

    fn all_shutdown_retries_exhausted(&self) -> io::Result<bool> {
        let observations = self
            .observations
            .lock()
            .map_err(|_| io::Error::other("pending persistence lock poisoned"))?;
        let exhausted = self
            .shutdown_exhausted
            .lock()
            .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?;
        Ok(observations.keys().all(|run_id| exhausted.contains(run_id)))
    }

    fn len(&self) -> io::Result<usize> {
        self.observations
            .lock()
            .map(|observations| observations.len())
            .map_err(|_| io::Error::other("pending persistence lock poisoned"))
    }
}

pub struct WorkerService {
    worker_id: WorkerId,
    capacity: usize,
    inventory: WorkerInventory,
    incarnation: u64,
    state_directory: PathBuf,
    log_policy: LogPolicy,
    backend: Arc<dyn ExecutionBackend>,
    state: Arc<SharedWorkerState>,
    pending: Arc<PendingPersistence>,
    persistence_admission: Arc<Semaphore>,
    shutdown_signal: watch::Sender<bool>,
    reader_reaper: ReaderReaper,
    cleanup_admission: Arc<Semaphore>,
    runtime: Runtime,
    supervisors: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    lifecycle: Mutex<()>,
    draining: AtomicBool,
}

impl Drop for WorkerService {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[cfg(test)]
#[path = "reader_reaper_tests.rs"]
mod reader_reaper_tests;

impl WorkerService {
    pub fn open(
        state_directory: impl AsRef<Path>,
        worker_id: WorkerId,
        capacity: usize,
        backend: Arc<dyn ExecutionBackend>,
    ) -> io::Result<Self> {
        Self::open_with_log_policy(
            state_directory,
            worker_id,
            capacity,
            LogPolicy::default(),
            backend,
        )
    }

    pub fn open_with_log_policy(
        state_directory: impl AsRef<Path>,
        worker_id: WorkerId,
        capacity: usize,
        log_policy: LogPolicy,
        backend: Arc<dyn ExecutionBackend>,
    ) -> io::Result<Self> {
        let state_directory = state_directory.as_ref().to_path_buf();
        if capacity == 0 || capacity > MAX_WORKER_CAPACITY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker capacity must be between 1 and 64",
            ));
        }
        let inventory = host_inventory(capacity, backend.as_ref())?;
        if log_policy.max_bytes_per_stream == 0
            || log_policy.max_bytes_per_stream > MAX_LOG_MAX_BYTES_PER_STREAM
            || log_policy.retention_runs > MAX_LOG_RETENTION_RUNS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker log limits are outside the supported range",
            ));
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_io()
            .enable_time()
            .build()?;
        let log_directory = state_directory.join("runs");
        match fs::symlink_metadata(&log_directory) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "worker run storage path must be a real directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&log_directory)?;
            }
            Err(error) => return Err(error),
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&log_directory, fs::Permissions::from_mode(0o700))?;
        }
        let database_path = state_directory.join("worker.sqlite3");
        match fs::symlink_metadata(&database_path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "worker database path must be a regular file",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                #[cfg(unix)]
                options.mode(0o600);
                match options.open(&database_path) {
                    Ok(file) => drop(file),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
        let database_metadata = fs::symlink_metadata(&database_path)?;
        if database_metadata.file_type().is_symlink() || !database_metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker database path must be a regular file",
            ));
        }
        let database_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&database_path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            database_file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        drop(database_file);
        let connection = Connection::open_with_flags(
            database_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(storage_error)?;
        connection
            .busy_timeout(BUSY_TIMEOUT)
            .map_err(storage_error)?;
        let schema_version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(storage_error)?;
        if !(0..=SCHEMA_VERSION).contains(&schema_version) {
            let message = if schema_version > SCHEMA_VERSION {
                format!(
                    "worker database schema version {schema_version} is newer than supported version {SCHEMA_VERSION}"
                )
            } else {
                format!("worker database schema version {schema_version} is invalid")
            };
            return Err(io::Error::new(io::ErrorKind::InvalidData, message));
        }
        if schema_version == 0 {
            let existing_tables: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%')",
                    [],
                    |row| row.get(0),
                )
                .map_err(storage_error)?;
            if existing_tables {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "worker database has an unsupported legacy schema",
                ));
            }
            let transaction = connection.unchecked_transaction().map_err(storage_error)?;
            transaction
                .execute_batch(
                    "CREATE TABLE runs (
                    run_id TEXT PRIMARY KEY NOT NULL,
                    spec_json TEXT NOT NULL,
                    observation_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    revision INTEGER NOT NULL CHECK (revision > 0)
                );
                CREATE TABLE run_logs (
                    run_id TEXT PRIMARY KEY NOT NULL REFERENCES runs(run_id),
                    stdout_closed INTEGER NOT NULL DEFAULT 0 CHECK (stdout_closed IN (0, 1)),
                    stderr_closed INTEGER NOT NULL DEFAULT 0 CHECK (stderr_closed IN (0, 1)),
                    stdout_truncated INTEGER NOT NULL DEFAULT 0 CHECK (stdout_truncated IN (0, 1)),
                    stderr_truncated INTEGER NOT NULL DEFAULT 0 CHECK (stderr_truncated IN (0, 1)),
                    pruned INTEGER NOT NULL DEFAULT 0 CHECK (pruned IN (0, 1)),
                    completed_at_ms INTEGER
                );",
                )
                .map_err(storage_error)?;
            transaction
                .pragma_update(None, "user_version", 2)
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
        } else if schema_version == 1 {
            let transaction = connection.unchecked_transaction().map_err(storage_error)?;
            transaction
                .execute_batch(
                    "CREATE TABLE run_logs (
                    run_id TEXT PRIMARY KEY NOT NULL REFERENCES runs(run_id),
                    stdout_closed INTEGER NOT NULL DEFAULT 1 CHECK (stdout_closed IN (0, 1)),
                    stderr_closed INTEGER NOT NULL DEFAULT 1 CHECK (stderr_closed IN (0, 1)),
                    stdout_truncated INTEGER NOT NULL DEFAULT 0 CHECK (stdout_truncated IN (0, 1)),
                    stderr_truncated INTEGER NOT NULL DEFAULT 0 CHECK (stderr_truncated IN (0, 1)),
                    pruned INTEGER NOT NULL DEFAULT 0 CHECK (pruned IN (0, 1)),
                    completed_at_ms INTEGER
                );
                INSERT INTO run_logs (run_id) SELECT run_id FROM runs;
                PRAGMA user_version = 2;",
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
        } else {
            connection
                .prepare(
                    "SELECT run_id, spec_json, observation_json, state, revision FROM runs LIMIT 0",
                )
                .map_err(storage_error)?;
        }
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(storage_error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(storage_error)?;

        let incarnation = {
            let transaction = connection.unchecked_transaction().map_err(storage_error)?;
            transaction
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS worker_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    incarnation INTEGER NOT NULL CHECK (incarnation >= 0)
                );
                INSERT OR IGNORE INTO worker_meta (id, incarnation) VALUES (1, 0);
                CREATE TABLE IF NOT EXISTS worker_commands (
                    command_id TEXT PRIMARY KEY NOT NULL,
                    worker_id TEXT NOT NULL,
                    worker_incarnation INTEGER NOT NULL CHECK (worker_incarnation >= 0),
                    command_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('accepted', 'completed', 'acknowledged')),
                    result_json TEXT,
                    report_sequence INTEGER CHECK (report_sequence IS NULL OR report_sequence > 0),
                    accepted_at_ms INTEGER NOT NULL,
                    CHECK ((state = 'accepted' AND result_json IS NULL AND report_sequence IS NULL)
                        OR (state IN ('completed', 'acknowledged') AND result_json IS NOT NULL AND report_sequence IS NOT NULL)),
                    UNIQUE (worker_incarnation, report_sequence)
                );
                CREATE INDEX IF NOT EXISTS worker_commands_pending_reports
                    ON worker_commands (worker_incarnation, report_sequence)
                    WHERE state = 'completed';
                CREATE TABLE IF NOT EXISTS worker_report_state (
                    incarnation INTEGER NOT NULL CHECK (incarnation >= 0),
                    sequence INTEGER NOT NULL CHECK (sequence >= 0),
                    PRIMARY KEY (incarnation)
                );
                INSERT OR IGNORE INTO worker_report_state (incarnation, sequence) VALUES (0, 0);
                PRAGMA user_version = 4;",
                )
                .map_err(storage_error)?;
            let changed = transaction
                .execute(
                    "UPDATE worker_meta SET incarnation = incarnation + 1
                     WHERE id = 1 AND incarnation < 9223372036854775807",
                    [],
                )
                .map_err(storage_error)?;
            if changed == 0 {
                return Err(io::Error::other("worker incarnation is exhausted"));
            }
            let generation = transaction
                .query_row(
                    "SELECT incarnation FROM worker_meta WHERE id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT OR IGNORE INTO worker_report_state (incarnation, sequence) VALUES (?1, 0)",
                    [generation],
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
            u64::try_from(generation)
                .map_err(|_| io::Error::other("worker incarnation is invalid"))?
        };

        let service = Self {
            worker_id,
            capacity,
            inventory,
            incarnation,
            state_directory,
            log_policy,
            backend,
            state: Arc::new(SharedWorkerState(Mutex::new(WorkerState {
                connection,
                active_groups: BTreeMap::new(),
            }))),
            pending: Arc::new(PendingPersistence {
                observations: Mutex::new(BTreeMap::new()),
                shutdown_exhausted: Mutex::new(BTreeSet::new()),
                degraded: AtomicBool::new(false),
            }),
            persistence_admission: Arc::new(Semaphore::new(PERSISTENCE_BLOCKING_CAPACITY)),
            shutdown_signal: watch::channel(false).0,
            reader_reaper: ReaderReaper::new()?,
            cleanup_admission: Arc::new(Semaphore::new(capacity)),
            runtime,
            supervisors: Mutex::new(Vec::new()),
            lifecycle: Mutex::new(()),
            draining: AtomicBool::new(false),
        };
        service.mark_interrupted_runs_unknown()?;
        service.enforce_legacy_log_limits()?;
        Ok(service)
    }

    pub fn start(&self, command: StartRun) -> Result<RunObservation, WorkerError> {
        let _lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if self.draining.load(Ordering::SeqCst) {
            return Err(WorkerError::ShuttingDown);
        }
        let spec = command.spec();
        if spec.worker_id() != &self.worker_id {
            return Err(WorkerError::WorkerMismatch);
        }
        let spec_json = serde_json::to_string(spec).map_err(|_| WorkerError::StorageUnavailable)?;
        let now = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let (observation, cleanup_permit) = {
            let mut state = self
                .state
                .0
                .lock()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            let transaction = state
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|_| WorkerError::StorageUnavailable)?;
            let existing = transaction
                .query_row(
                    "SELECT spec_json, observation_json FROM runs WHERE run_id = ?1",
                    [spec.id().as_str()],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            if let Some((stored_spec, stored_observation)) = existing {
                if stored_spec != spec_json {
                    return Err(WorkerError::RunConflict);
                }
                return serde_json::from_str(&stored_observation)
                    .map_err(|_| WorkerError::StorageUnavailable);
            }
            if self.is_degraded() {
                return Err(WorkerError::StorageUnavailable);
            }
            let cleanup_permit = Arc::clone(&self.cleanup_admission)
                .try_acquire_owned()
                .map_err(|_| WorkerError::CapacityReached)?;
            let active_count: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM runs WHERE state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
                [],
                |row| row.get(0),
            ).map_err(|_| WorkerError::StorageUnavailable)?;
            if active_count >= self.capacity as i64 {
                return Err(WorkerError::CapacityReached);
            }
            let observation =
                RunObservation::requested(spec, now).with_worker_incarnation(self.incarnation);
            let observation_json =
                serde_json::to_string(&observation).map_err(|_| WorkerError::StorageUnavailable)?;
            transaction.execute(
                "INSERT INTO runs (run_id, spec_json, observation_json, state, revision) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![spec.id().as_str(), spec_json, observation_json, observation.state().as_str(), observation.revision() as i64],
            ).map_err(|_| WorkerError::StorageUnavailable)?;
            transaction
                .execute(
                    "INSERT INTO run_logs (run_id) VALUES (?1)",
                    [spec.id().as_str()],
                )
                .map_err(|_| WorkerError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            (observation, cleanup_permit)
        };

        let starting = observation
            .next_revision(RunState::Starting, None, None, None, None, None)
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&starting)?;

        let stdout_path = self
            .state_directory
            .join("runs")
            .join(format!("{}.stdout", spec.id()));
        let stderr_path = self
            .state_directory
            .join("runs")
            .join(format!("{}.stderr", spec.id()));
        let stdout_file = match prepare_log_file(&stdout_path) {
            Ok(file) => file,
            Err(_) => {
                self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                return self.finish_backend_failure(starting);
            }
        };
        let stderr_file = match prepare_log_file(&stderr_path) {
            Ok(file) => file,
            Err(_) => {
                drop(stdout_file);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                return self.finish_backend_failure(starting);
            }
        };

        if let Some(expected) = spec.job().executable_digest() {
            match executable_digest_matches(Path::new(&spec.job().command()[0]), expected.value()) {
                Ok(true) => {}
                Ok(false) => {
                    drop(stdout_file);
                    drop(stderr_file);
                    self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                    self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                    return self
                        .finish_failure(starting, RunFailureReason::ExecutableDigestMismatch);
                }
                Err(_) => {
                    drop(stdout_file);
                    drop(stderr_file);
                    self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                    self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                    return self.finish_backend_failure(starting);
                }
            }
        }

        let job = match spec.job().clone().with_environment_overrides(
            spec.service_ports()
                .iter()
                .map(|port| (port.environment_name(), port.port().to_string())),
        ) {
            Ok(job) => job,
            Err(_) => {
                self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                return self.finish_backend_failure(starting);
            }
        };
        let child = match self.backend.spawn(spec.id().as_str(), &job) {
            Ok(child) => child,
            Err(error) => {
                emit_worker_diagnostic("execution_launch_failed", spec.id().as_str(), &error);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stdout);
                self.mark_log_closed(spec.id().as_str(), LogStream::Stderr);
                return self.finish_spawn_failure(starting);
            }
        };
        let launched_at = std::time::Instant::now();
        let group_id = child.id();
        let mut spawned = SpawnGuard {
            backend: Arc::clone(&self.backend),
            reader_reaper: self.reader_reaper.clone(),
            group_id,
            child: Some(child),
            readers: Vec::with_capacity(2),
            cleanup_permit: Some(cleanup_permit),
            cancel_readers: Arc::new(AtomicBool::new(false)),
            cleaned: false,
        };
        let Some(stdout) = spawned.child.as_mut().and_then(|child| child.stdout.take()) else {
            return self.abort_startup(spawned, starting);
        };
        let Some(stderr) = spawned.child.as_mut().and_then(|child| child.stderr.take()) else {
            return self.abort_startup(spawned, starting);
        };
        let stdout_reader = match spawn_capture(
            stdout,
            stdout_file,
            self.log_policy.max_bytes_per_stream,
            Arc::clone(&self.state),
            spec.id().to_string(),
            LogStream::Stdout,
            Arc::clone(&spawned.cancel_readers),
        ) {
            Ok(reader) => reader,
            Err(_) => return self.abort_startup(spawned, starting),
        };
        spawned.readers.push(stdout_reader);
        let stderr_reader = match spawn_capture(
            stderr,
            stderr_file,
            self.log_policy.max_bytes_per_stream,
            Arc::clone(&self.state),
            spec.id().to_string(),
            LogStream::Stderr,
            Arc::clone(&spawned.cancel_readers),
        ) {
            Ok(reader) => reader,
            Err(_) => return self.abort_startup(spawned, starting),
        };
        spawned.readers.push(stderr_reader);
        let started = match now_ms() {
            Ok(started) => started,
            Err(_) => return self.abort_startup(spawned, starting),
        };
        let running = starting
            .next_revision(RunState::Running, Some(started), None, None, None, None)
            .map(|observation| {
                if spec.readiness_probe().is_some() {
                    observation.with_readiness(ReadinessState::Starting)
                } else {
                    observation
                }
            })
            .ok_or(WorkerError::StorageUnavailable);
        let running = match running {
            Ok(running) => running,
            Err(_) => return self.abort_startup(spawned, starting),
        };
        if self.persist(&running).is_err() {
            let _ = self.abort_startup(spawned, running);
            return Err(WorkerError::StorageUnavailable);
        }
        let control_notify = Arc::new(Notify::new());
        {
            let mut state = match self.state.0.lock() {
                Ok(state) => state,
                Err(_) => {
                    let _ = self.abort_startup(spawned, running);
                    return Err(WorkerError::StorageUnavailable);
                }
            };
            state.active_groups.insert(
                spec.id().to_string(),
                ActiveGroup {
                    group_id,
                    intent: Arc::new(AtomicU8::new(INTENT_NONE)),
                    notify: Arc::clone(&control_notify),
                },
            );
        }

        let state = Arc::clone(&self.state);
        let pending = Arc::clone(&self.pending);
        let persistence_admission = Arc::clone(&self.persistence_admission);
        let shutdown_signal = self.shutdown_signal.subscribe();
        let backend = Arc::clone(&self.backend);
        let run_id = spec.id().to_string();
        let control = match self.state.0.lock() {
            Ok(state) => state
                .active_groups
                .get(spec.id().as_str())
                .map(|active| (Arc::clone(&active.intent), Arc::clone(&active.notify)))
                .ok_or(WorkerError::StorageUnavailable),
            Err(_) => Err(WorkerError::StorageUnavailable),
        };
        let (control, control_notify) = match control {
            Ok(control) => control,
            Err(_) => {
                if let Ok(mut state) = self.state.0.lock() {
                    state.active_groups.remove(spec.id().as_str());
                }
                let _ = self.abort_startup(spawned, running);
                return Err(WorkerError::StorageUnavailable);
            }
        };
        let timeout = spec.job().timeout_ms().map(Duration::from_millis);
        let grace = Duration::from_millis(spec.job().termination_grace_ms());
        let log_directory = self.state_directory.join("runs");
        let retention_runs = self.log_policy.retention_runs;
        let supervisor_observation = running.clone();
        let supervisor = self.runtime.spawn(supervise(RunSupervision {
            state,
            pending,
            persistence_admission,
            shutdown_signal,
            backend,
            run_id,
            spawned,
            current: supervisor_observation,
            control,
            control_notify,
            launched_at,
            timeout,
            grace,
            log_directory,
            retention_runs,
            readiness_probe: spec.readiness_probe().cloned(),
            service_ports: spec.service_ports().to_vec(),
        }));
        let mut supervisors = self
            .supervisors
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        supervisors.retain(|old| !old.is_finished());
        supervisors.push(supervisor);
        Ok(running)
    }

    pub fn inspect(&self, command: InspectRun) -> Result<Option<RunObservation>, WorkerError> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        state
            .connection
            .query_row(
                "SELECT observation_json FROM runs WHERE run_id = ?1",
                [command.run_id().as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .map(|json| serde_json::from_str(&json).map_err(|_| WorkerError::StorageUnavailable))
            .transpose()
    }

    pub fn read_logs(&self, command: ReadRunLogs) -> Result<RunLogChunk, WorkerError> {
        if command.limit() == 0 || command.limit() > MAX_LOG_CHUNK_BYTES {
            return Err(WorkerError::StorageUnavailable);
        }
        let id = command.run_id();
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let run_exists: bool = state
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runs WHERE run_id = ?1)",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if !run_exists {
            return Err(WorkerError::LogsNotFound);
        }
        let log_state: Option<(bool, bool, bool)> = state
            .connection
            .query_row(
                "SELECT CASE WHEN ?2 = 'stdout' THEN stdout_closed ELSE stderr_closed END,
                        CASE WHEN ?2 = 'stdout' THEN stdout_truncated ELSE stderr_truncated END,
                        pruned
                 FROM run_logs WHERE run_id = ?1",
                params![id.as_str(), command.stream().as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let (closed, truncated, pruned) = log_state.ok_or(WorkerError::LogsNotFound)?;
        drop(state);
        if pruned {
            return Err(WorkerError::LogsPruned);
        }
        let path = self.log_path(id.as_str(), command.stream());
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(WorkerError::LogsNotFound);
            }
            Err(_) => return Err(WorkerError::StorageUnavailable),
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(WorkerError::StorageUnavailable);
        }
        let file_len = metadata.len();
        let mut file = File::open(path).map_err(|_| WorkerError::StorageUnavailable)?;
        file.seek(SeekFrom::Start(command.offset()))
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut data = Vec::with_capacity(
            command
                .limit()
                .min(file_len.saturating_sub(command.offset()) as usize),
        );
        file.take(command.limit() as u64)
            .read_to_end(&mut data)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let next_offset = command.offset().saturating_add(data.len() as u64);
        let eof = closed && next_offset >= file_len;
        Ok(RunLogChunk::new(
            id.clone(),
            command.stream(),
            command.offset(),
            next_offset,
            data,
            RunLogStatus::new(closed, eof, truncated),
        ))
    }

    pub fn stop(&self, command: StopRun) -> Result<RunObservation, WorkerError> {
        let id = command.run_id();
        let current = self
            .inspect(InspectRun::new(id.clone()))?
            .ok_or(WorkerError::RunConflict)?;
        if current.state().is_terminal() {
            return Ok(current);
        }
        let active = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .active_groups
            .get(id.as_str())
            .cloned();
        let Some(active) = active else {
            // No persisted PID/group is authority after recovery. Keep the run unresolved.
            return Ok(current);
        };
        let stopping = if current.state() == RunState::Stopping {
            current
        } else {
            current
                .next_revision(
                    RunState::Stopping,
                    current.started_at_ms(),
                    None,
                    None,
                    None,
                    None,
                )
                .ok_or(WorkerError::StorageUnavailable)?
                .with_termination_reason(RunTerminationReason::Cancelled)
        };
        let stopping = match self.persist(&stopping) {
            Ok(()) => stopping,
            Err(error) => match self.inspect(InspectRun::new(id.clone()))? {
                Some(latest)
                    if latest.state() == RunState::Stopping || latest.state().is_terminal() =>
                {
                    latest
                }
                _ => return Err(error),
            },
        };
        if stopping.state().is_terminal() {
            return Ok(stopping);
        }
        let _ = active.intent.compare_exchange(
            INTENT_NONE,
            INTENT_CANCEL,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        active.notify.notify_one();
        Ok(stopping)
    }

    /// Resolve an ambiguous run after the operator has verified external cleanup.
    pub fn resolve_unknown(&self, command: ResolveRun) -> Result<RunObservation, WorkerError> {
        let id = command.run_id();
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let raw = transaction
            .query_row(
                "SELECT observation_json FROM runs WHERE run_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .ok_or(WorkerError::RunConflict)?;
        let current: RunObservation =
            serde_json::from_str(&raw).map_err(|_| WorkerError::StorageUnavailable)?;
        if current.state() == RunState::Abandoned {
            if current.resolution_reason() == Some(command.reason()) {
                transaction
                    .commit()
                    .map_err(|_| WorkerError::StorageUnavailable)?;
                return Ok(current);
            }
            return Err(WorkerError::RunConflict);
        }
        if current.state() != RunState::Unknown
            || command.reason().trim().is_empty()
            || command.reason().len() > 512
        {
            return Err(WorkerError::RunConflict);
        }
        let finished = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let resolved = current
            .next_revision(
                RunState::Abandoned,
                current.started_at_ms(),
                Some(finished),
                None,
                None,
                None,
            )
            .ok_or(WorkerError::StorageUnavailable)?
            .with_resolution_reason(command.reason().to_owned());
        let json = serde_json::to_string(&resolved).map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE runs SET observation_json = ?1, state = ?2, revision = ?3 WHERE run_id = ?4 AND revision = ?5",
                params![json, resolved.state().as_str(), resolved.revision() as i64, id.as_str(), current.revision() as i64],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE run_logs SET stdout_closed = 1, stderr_closed = 1, completed_at_ms = ?1 WHERE run_id = ?2",
                params![finished, id.as_str()],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        Ok(resolved)
    }

    /// Durably accepts a command before its side effect is attempted. A report
    /// returned here is the immutable result of an earlier delivery of the
    /// same command ID and contents.
    pub fn accept_command(
        &self,
        envelope: &CommandEnvelope,
    ) -> Result<Option<StoredWorkerReport>, WorkerError> {
        if envelope.target_worker_id != self.worker_id
            || envelope.target_worker_incarnation != self.incarnation
        {
            return Err(WorkerError::WorkerMismatch);
        }
        let command_json =
            serde_json::to_string(envelope).map_err(|_| WorkerError::StorageUnavailable)?;
        if command_json.len() > 128 * 1024 {
            return Err(WorkerError::StorageUnavailable);
        }
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let existing = transaction
            .query_row(
                "SELECT command_json, state, result_json, report_sequence,
                        worker_id, worker_incarnation
                 FROM worker_commands WHERE command_id = ?1",
                [envelope.command_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if let Some((stored_json, status, result_json, sequence, worker_id, incarnation)) = existing
        {
            if stored_json != command_json
                || worker_id != self.worker_id.as_str()
                || u64::try_from(incarnation).ok() != Some(self.incarnation)
            {
                return Err(WorkerError::RunConflict);
            }
            let report = match (result_json, sequence) {
                (Some(result_json), Some(sequence))
                    if matches!(status.as_str(), "completed" | "acknowledged") =>
                {
                    Some(StoredWorkerReport {
                        command_id: envelope.command_id.clone(),
                        worker_id: self.worker_id.clone(),
                        worker_incarnation: self.incarnation,
                        report_sequence: u64::try_from(sequence)
                            .map_err(|_| WorkerError::StorageUnavailable)?,
                        result_json,
                    })
                }
                (None, None) if status == "accepted" => None,
                _ => return Err(WorkerError::StorageUnavailable),
            };
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            return Ok(report);
        }
        transaction
            .execute(
                "INSERT INTO worker_commands
                    (command_id, worker_id, worker_incarnation, command_json,
                     state, accepted_at_ms)
                 VALUES (?1, ?2, ?3, ?4, 'accepted', ?5)",
                params![
                    envelope.command_id.as_str(),
                    self.worker_id.as_str(),
                    i64::try_from(self.incarnation).map_err(|_| WorkerError::StorageUnavailable)?,
                    command_json,
                    now_ms().map_err(|_| WorkerError::StorageUnavailable)?
                ],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        Ok(None)
    }

    /// Commits the command result and its monotonically increasing report
    /// sequence in one SQLite transaction.
    pub fn complete_command(
        &self,
        envelope: &CommandEnvelope,
        result_json: &str,
    ) -> Result<StoredWorkerReport, WorkerError> {
        if result_json.len() > 128 * 1024
            || serde_json::from_str::<serde_json::Value>(result_json).is_err()
        {
            return Err(WorkerError::StorageUnavailable);
        }
        let command_json =
            serde_json::to_string(envelope).map_err(|_| WorkerError::StorageUnavailable)?;
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let existing = transaction
            .query_row(
                "SELECT command_json, state, result_json, report_sequence,
                        worker_id, worker_incarnation
                 FROM worker_commands WHERE command_id = ?1",
                [envelope.command_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .ok_or(WorkerError::RunConflict)?;
        if existing.0 != command_json
            || existing.4 != self.worker_id.as_str()
            || u64::try_from(existing.5).ok() != Some(envelope.target_worker_incarnation)
        {
            return Err(WorkerError::RunConflict);
        }
        if matches!(existing.1.as_str(), "completed" | "acknowledged") {
            let (Some(result_json), Some(sequence)) = (existing.2, existing.3) else {
                return Err(WorkerError::StorageUnavailable);
            };
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            return Ok(StoredWorkerReport {
                command_id: envelope.command_id.clone(),
                worker_id: self.worker_id.clone(),
                worker_incarnation: envelope.target_worker_incarnation,
                report_sequence: u64::try_from(sequence)
                    .map_err(|_| WorkerError::StorageUnavailable)?,
                result_json,
            });
        }
        if existing.1 != "accepted" || existing.2.is_some() || existing.3.is_some() {
            return Err(WorkerError::StorageUnavailable);
        }
        let stored_incarnation = i64::try_from(envelope.target_worker_incarnation)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let sequence: i64 = transaction
            .query_row(
                "SELECT sequence FROM worker_report_state WHERE incarnation = ?1",
                [stored_incarnation],
                |row| row.get(0),
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(WorkerError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE worker_report_state SET sequence = ?1
                 WHERE incarnation = ?2 AND sequence = ?3",
                params![next_sequence, stored_incarnation, sequence],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE worker_commands
                 SET state = 'completed', result_json = ?1, report_sequence = ?2
                 WHERE command_id = ?3 AND state = 'accepted'",
                params![result_json, next_sequence, envelope.command_id.as_str()],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if changed != 1 {
            return Err(WorkerError::StorageUnavailable);
        }
        transaction
            .commit()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        Ok(StoredWorkerReport {
            command_id: envelope.command_id.clone(),
            worker_id: self.worker_id.clone(),
            worker_incarnation: envelope.target_worker_incarnation,
            report_sequence: u64::try_from(next_sequence)
                .map_err(|_| WorkerError::StorageUnavailable)?,
            result_json: result_json.to_owned(),
        })
    }

    /// Returns every completed report not yet acknowledged by the manager for
    /// this worker incarnation, in sequence order.
    pub fn pending_reports(&self) -> Result<Vec<StoredWorkerReport>, WorkerError> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut statement = state
            .connection
            .prepare(
                "SELECT command_id, worker_id, worker_incarnation, report_sequence, result_json
                 FROM worker_commands
                 WHERE state = 'completed'
                 ORDER BY worker_incarnation, report_sequence",
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })
            .map_err(|_| WorkerError::StorageUnavailable)?;
        rows.map(|row| {
            let (command_id, worker_id, incarnation, sequence, result_json) =
                row.map_err(|_| WorkerError::StorageUnavailable)?;
            Ok(StoredWorkerReport {
                command_id: CommandId::new(command_id)
                    .map_err(|_| WorkerError::StorageUnavailable)?,
                worker_id: WorkerId::new(worker_id).map_err(|_| WorkerError::StorageUnavailable)?,
                worker_incarnation: u64::try_from(incarnation)
                    .map_err(|_| WorkerError::StorageUnavailable)?,
                report_sequence: u64::try_from(sequence)
                    .map_err(|_| WorkerError::StorageUnavailable)?,
                result_json,
            })
        })
        .collect()
    }

    /// Read one bounded page of durable run history. The first page captures a
    /// rowid high-water mark; later pages remain bounded to that snapshot.
    pub fn inventory_page(
        &self,
        snapshot_generation: Option<u64>,
        after_sequence: u64,
        limit: usize,
    ) -> Result<WorkerInventoryPage, WorkerError> {
        if limit == 0 || limit > MAX_WORKER_INVENTORY_PAGE_SIZE {
            return Err(WorkerError::StorageUnavailable);
        }
        if snapshot_generation.is_none() && after_sequence != 0 {
            return Err(WorkerError::StorageUnavailable);
        }
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let generation = match snapshot_generation {
            Some(generation) => generation,
            None => {
                let highest: i64 = state
                    .connection
                    .query_row("SELECT COALESCE(MAX(rowid), 0) FROM runs", [], |row| {
                        row.get(0)
                    })
                    .map_err(|_| WorkerError::StorageUnavailable)?;
                u64::try_from(highest).map_err(|_| WorkerError::StorageUnavailable)?
            }
        };
        if after_sequence > generation {
            return Err(WorkerError::StorageUnavailable);
        }
        let after = i64::try_from(after_sequence).map_err(|_| WorkerError::StorageUnavailable)?;
        let generation_i64 =
            i64::try_from(generation).map_err(|_| WorkerError::StorageUnavailable)?;
        let read_limit = i64::try_from(limit + 1).map_err(|_| WorkerError::StorageUnavailable)?;
        let mut statement = state
            .connection
            .prepare(
                "SELECT rowid, observation_json FROM runs
                 WHERE rowid > ?1 AND rowid <= ?2 ORDER BY rowid LIMIT ?3",
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let rows = statement
            .query_map(params![after, generation_i64, read_limit], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut entries = rows
            .map(|row| {
                let (sequence, observation) = row.map_err(|_| WorkerError::StorageUnavailable)?;
                Ok(WorkerInventoryEntry {
                    sequence: u64::try_from(sequence)
                        .map_err(|_| WorkerError::StorageUnavailable)?,
                    observation: serde_json::from_str(&observation)
                        .map_err(|_| WorkerError::StorageUnavailable)?,
                })
            })
            .collect::<Result<Vec<_>, WorkerError>>()?;
        let more = entries.len() > limit;
        if more {
            entries.pop();
        }
        let next_after_sequence = more
            .then(|| entries.last().map(|entry| entry.sequence))
            .flatten();
        let page = WorkerInventoryPage {
            worker_id: self.worker_id.clone(),
            worker_incarnation: self.incarnation,
            snapshot_generation: generation,
            after_sequence,
            entries,
            next_after_sequence,
        };
        if !page.is_valid() {
            return Err(WorkerError::StorageUnavailable);
        }
        Ok(page)
    }

    /// Returns accepted commands whose result has not yet been committed. The
    /// transport reconciles them before polling for new work.
    pub fn accepted_commands(&self) -> Result<Vec<CommandEnvelope>, WorkerError> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut statement = state
            .connection
            .prepare(
                "SELECT command_json FROM worker_commands
                 WHERE state = 'accepted' ORDER BY accepted_at_ms, command_id",
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|_| WorkerError::StorageUnavailable)?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|_| WorkerError::StorageUnavailable)?)
                .map_err(|_| WorkerError::StorageUnavailable)
        })
        .collect()
    }

    /// Records the manager's report acknowledgement so a restart does not
    /// resend a completed report unnecessarily.
    pub fn acknowledge_report(
        &self,
        command_id: &CommandId,
        worker_incarnation: u64,
        report_sequence: u64,
    ) -> Result<(), WorkerError> {
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let record = transaction
            .query_row(
                "SELECT state, worker_incarnation, report_sequence
                 FROM worker_commands WHERE command_id = ?1",
                [command_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| WorkerError::StorageUnavailable)?
            .ok_or(WorkerError::RunConflict)?;
        if u64::try_from(record.1).ok() != Some(worker_incarnation)
            || record.2.and_then(|value| u64::try_from(value).ok()) != Some(report_sequence)
        {
            return Err(WorkerError::RunConflict);
        }
        if record.0 == "acknowledged" {
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            return Ok(());
        }
        if record.0 != "completed" {
            return Err(WorkerError::RunConflict);
        }
        transaction
            .execute(
                "UPDATE worker_commands SET state = 'acknowledged'
                 WHERE command_id = ?1 AND state = 'completed'",
                [command_id.as_str()],
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerError::StorageUnavailable)
    }

    pub fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.worker_id.clone(), self.capacity, true)
            .with_incarnation(self.incarnation)
    }

    pub fn status(&self) -> Result<WorkerStatus, WorkerError> {
        let draining = self.is_draining();
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let (used, unknown): (i64, i64) = state
            .connection
            .query_row(
                "SELECT COUNT(*), SUM(CASE WHEN state = 'unknown' THEN 1 ELSE 0 END) FROM runs WHERE state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
                [],
                |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
            )
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let used = usize::try_from(used).map_err(|_| WorkerError::StorageUnavailable)?;
        let unknown = usize::try_from(unknown).map_err(|_| WorkerError::StorageUnavailable)?;
        let live_processes = state.active_groups.len();
        let observed_at = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let pending = self
            .pending
            .len()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let degraded = self.is_degraded();
        let cleanup_available = self.cleanup_admission.available_permits() > 0;
        Ok(WorkerStatus::new(
            self.descriptor(),
            used,
            unknown,
            !draining && !degraded && cleanup_available,
            draining,
            observed_at,
        )
        .with_runtime_state(live_processes, pending, degraded)
        .with_inventory(self.inventory.clone()))
    }

    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    pub fn is_degraded(&self) -> bool {
        self.pending.degraded.load(Ordering::SeqCst)
    }

    /// Stop admission immediately and notify every live supervisor to begin cleanup.
    pub fn begin_shutdown(&self) {
        self.draining.store(true, Ordering::SeqCst);
        self.shutdown_signal.send_replace(true);
    }

    pub fn shutdown(&self) -> io::Result<()> {
        self.begin_shutdown();
        // Wait for any start already inside the lifecycle gate to either
        // reject startup or transfer its process to a registered supervisor.
        drop(
            self.lifecycle
                .lock()
                .map_err(|_| io::Error::other("worker lifecycle lock poisoned"))?,
        );
        let deadline = std::time::Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            let active = self
                .state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state lock poisoned"))?
                .active_groups
                .len();
            let pending = self
                .pending
                .len()
                .map_err(|_| io::Error::other("pending persistence lock poisoned"))?;
            let retries_exhausted = self
                .pending
                .all_shutdown_retries_exhausted()
                .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?;
            let pending_readers = self.reader_reaper.pending();
            if (active == 0 && (pending == 0 || retries_exhausted) && pending_readers == 0)
                || std::time::Instant::now() >= deadline
            {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let groups = self
            .state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?
            .active_groups
            .values()
            .map(|active| active.group_id)
            .collect::<Vec<_>>();
        let mut first_error = None;
        for group in groups {
            if let Err(error) = self.backend.kill_process_group(group) {
                first_error.get_or_insert(error);
            }
        }
        let reap_deadline = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < reap_deadline {
            let active = self
                .state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state lock poisoned"))?
                .active_groups
                .len();
            let pending = self
                .pending
                .len()
                .map_err(|_| io::Error::other("pending persistence lock poisoned"))?;
            let retries_exhausted = self
                .pending
                .all_shutdown_retries_exhausted()
                .map_err(|_| io::Error::other("pending shutdown set lock poisoned"))?;
            let pending_readers = self.reader_reaper.pending();
            if active == 0 && (pending == 0 || retries_exhausted) && pending_readers == 0 {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        if !self
            .state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?
            .active_groups
            .is_empty()
            || self
                .pending
                .len()
                .map_err(|_| io::Error::other("pending persistence lock poisoned"))?
                > 0
            || self.reader_reaper.pending() > 0
        {
            first_error.get_or_insert(io::Error::new(
                io::ErrorKind::TimedOut,
                "worker shutdown deadline elapsed",
            ));
        }
        let supervisors = self
            .supervisors
            .lock()
            .map_err(|_| io::Error::other("run supervisor lock poisoned"))?
            .drain(..)
            .collect::<Vec<_>>();
        let task_deadline = std::time::Instant::now() + Duration::from_secs(1);
        while supervisors
            .iter()
            .any(|supervisor| !supervisor.is_finished())
            && std::time::Instant::now() < task_deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        if !supervisors.is_empty() {
            self.runtime.block_on(async {
                for supervisor in supervisors {
                    if supervisor.is_finished() && supervisor.await.is_err() {
                        first_error.get_or_insert(io::Error::other("run supervisor panicked"));
                    }
                }
            });
        }
        first_error.map_or(Ok(()), Err)
    }

    fn mark_interrupted_runs_unknown(&self) -> io::Result<()> {
        let mut state = self
            .state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?;
        let transaction = state
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let rows = {
            let mut statement = transaction
                .prepare("SELECT observation_json FROM runs")
                .map_err(storage_error)?;
            let mapped = statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(storage_error)?;
            mapped
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage_error)?
        };
        for json in rows {
            let observation: RunObservation = serde_json::from_str(&json).map_err(storage_error)?;
            if !observation.state().is_active_or_unresolved() {
                continue;
            }
            let unknown = observation
                .next_revision(
                    RunState::Unknown,
                    observation.started_at_ms(),
                    None,
                    None,
                    None,
                    Some(RunFailureReason::OutcomeUnknown),
                )
                .ok_or_else(|| io::Error::other("observation revision exhausted"))?
                .with_worker_incarnation(self.incarnation);
            let serialized = serde_json::to_string(&unknown).map_err(storage_error)?;
            transaction.execute(
                "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision = ?5",
                params![serialized, unknown.revision() as i64, unknown.state().as_str(), unknown.run_id().as_str(), observation.revision() as i64],
            ).map_err(storage_error)?;
        }
        transaction.commit().map_err(storage_error)
    }

    fn log_path(&self, run_id: &str, stream: LogStream) -> PathBuf {
        self.state_directory
            .join("runs")
            .join(format!("{run_id}.{}", stream.as_str()))
    }

    fn mark_log_closed(&self, run_id: &str, stream: LogStream) {
        let Ok(state) = self.state.0.lock() else {
            return;
        };
        let (column, other_column) = match stream {
            LogStream::Stdout => ("stdout_closed", "stderr_closed"),
            LogStream::Stderr => ("stderr_closed", "stdout_closed"),
        };
        let sql = format!(
            "UPDATE run_logs SET {column} = 1,
             completed_at_ms = CASE WHEN {other_column} = 1 THEN ?1 ELSE completed_at_ms END
             WHERE run_id = ?2"
        );
        if let Ok(timestamp) = now_ms() {
            let _ = state.connection.execute(&sql, params![timestamp, run_id]);
        }
    }

    fn enforce_legacy_log_limits(&self) -> io::Result<()> {
        let ids = {
            let state = self
                .state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state lock poisoned"))?;
            let mut statement = state
                .connection
                .prepare("SELECT run_id FROM run_logs WHERE pruned = 0")
                .map_err(storage_error)?;
            statement
                .query_map([], |row| row.get::<_, String>(0))
                .map_err(storage_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(storage_error)?
        };
        for id in ids {
            for stream in [LogStream::Stdout, LogStream::Stderr] {
                let path = self.log_path(&id, stream);
                let length = match fs::symlink_metadata(&path) {
                    Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "worker log path must be a regular file",
                        ));
                    }
                    Ok(metadata) => metadata.len(),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        let file = prepare_log_file(&path)?;
                        drop(file);
                        0
                    }
                    Err(error) => return Err(error),
                };
                if length > self.log_policy.max_bytes_per_stream {
                    let file = OpenOptions::new().write(true).open(&path)?;
                    file.set_len(self.log_policy.max_bytes_per_stream)?;
                    self.mark_log_truncated(&id, stream)?;
                }
            }
            self.mark_log_closed(&id, LogStream::Stdout);
            self.mark_log_closed(&id, LogStream::Stderr);
            if let Ok(state) = self.state.0.lock()
                && let Ok(timestamp) = now_ms()
            {
                let _ = state.connection.execute(
                    "UPDATE run_logs SET completed_at_ms = ?1 WHERE run_id = ?2 AND completed_at_ms IS NULL",
                    params![timestamp, id],
                );
            }
        }
        self.prune_completed_logs()
    }

    fn mark_log_truncated(&self, run_id: &str, stream: LogStream) -> io::Result<()> {
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?;
        let column = match stream {
            LogStream::Stdout => "stdout_truncated",
            LogStream::Stderr => "stderr_truncated",
        };
        let sql = format!("UPDATE run_logs SET {column} = 1 WHERE run_id = ?1");
        state
            .connection
            .execute(&sql, [run_id])
            .map_err(storage_error)?;
        Ok(())
    }

    fn prune_completed_logs(&self) -> io::Result<()> {
        prune_completed_logs_shared(
            &self.state,
            &self.state_directory.join("runs"),
            self.log_policy.retention_runs,
        )
    }

    fn persist(&self, observation: &RunObservation) -> Result<(), WorkerError> {
        let serialized =
            serde_json::to_string(observation).map_err(|_| WorkerError::StorageUnavailable)?;
        let state = self
            .state
            .0
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let changed = state.connection.execute(
            "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
            params![serialized, observation.revision() as i64, observation.state().as_str(), observation.run_id().as_str()],
        ).map_err(|_| WorkerError::StorageUnavailable)?;
        if changed != 1 {
            return Err(WorkerError::StorageUnavailable);
        }
        Ok(())
    }

    fn finish_spawn_failure(&self, current: RunObservation) -> Result<RunObservation, WorkerError> {
        self.finish_failure(current, RunFailureReason::SpawnFailed)
    }

    fn finish_failure(
        &self,
        current: RunObservation,
        failure_reason: RunFailureReason,
    ) -> Result<RunObservation, WorkerError> {
        self.mark_log_closed(current.run_id().as_str(), LogStream::Stdout);
        self.mark_log_closed(current.run_id().as_str(), LogStream::Stderr);
        let finished = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let failed = current
            .next_revision(
                RunState::Failed,
                None,
                Some(finished),
                None,
                None,
                Some(failure_reason),
            )
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&failed)?;
        let _ = self.prune_completed_logs();
        Ok(failed)
    }

    fn abort_startup(
        &self,
        mut spawned: SpawnGuard,
        current: RunObservation,
    ) -> Result<RunObservation, WorkerError> {
        let cleanup = spawned.cleanup();
        let cleanup_succeeded = cleanup.is_ok();
        if let Err(error) = cleanup {
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"error\",\"component\":\"worker\",\"event\":\"startup_cleanup_failed\",\"runId\":{},\"errorKind\":{}}}",
                now_ms().unwrap_or_default(),
                serde_json::to_string(current.run_id().as_str()).unwrap_or_else(|_| "null".into()),
                serde_json::to_string(&format!("{:?}", error.kind()))
                    .unwrap_or_else(|_| "null".into())
            );
        }
        self.mark_log_closed(current.run_id().as_str(), LogStream::Stdout);
        self.mark_log_closed(current.run_id().as_str(), LogStream::Stderr);
        let finished = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let failed = current
            .next_revision(
                if cleanup_succeeded {
                    RunState::Failed
                } else {
                    RunState::Unknown
                },
                current.started_at_ms(),
                cleanup_succeeded.then_some(finished),
                None,
                None,
                Some(if cleanup_succeeded {
                    RunFailureReason::BackendFailure
                } else {
                    RunFailureReason::OutcomeUnknown
                }),
            )
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&failed)?;
        let _ = self.prune_completed_logs();
        Ok(failed)
    }

    fn finish_backend_failure(
        &self,
        current: RunObservation,
    ) -> Result<RunObservation, WorkerError> {
        let finished = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let failed = current
            .next_revision(
                RunState::Failed,
                current.started_at_ms(),
                Some(finished),
                None,
                None,
                Some(RunFailureReason::BackendFailure),
            )
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&failed)?;
        let _ = self.prune_completed_logs();
        Ok(failed)
    }
}

fn prepare_log_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true).read(true);
    #[cfg(unix)]
    options.mode(0o600);
    options.open(path)
}

fn executable_digest_matches(path: &Path, expected: &str) -> io::Result<bool> {
    let mut file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "configured executable is not a regular file",
        ));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let length = file.read(&mut buffer)?;
        if length == 0 {
            break;
        }
        hasher.update(&buffer[..length]);
    }
    let digest = hasher.finalize();
    let alphabet = b"0123456789abcdef";
    let mut encoded = [0_u8; 64];
    for (index, byte) in digest.iter().enumerate() {
        encoded[index * 2] = alphabet[usize::from(byte >> 4)];
        encoded[index * 2 + 1] = alphabet[usize::from(byte & 0x0f)];
    }
    Ok(expected.as_bytes() == encoded)
}

fn spawn_capture<R: CaptureRead + 'static>(
    mut source: R,
    mut output: File,
    byte_cap: u64,
    state: Arc<SharedWorkerState>,
    run_id: String,
    stream: LogStream,
    cancelled: Arc<AtomicBool>,
) -> io::Result<JoinHandle<io::Result<()>>> {
    thread::Builder::new().spawn(move || {
        let mut buffer = [0_u8; 8192];
        let mut written = 0_u64;
        let mut truncated = false;
        let mut was_cancelled = false;
        let mut capture_error = None;
        let mut logging_disabled = false;
        loop {
            match source.wait_readable(&cancelled) {
                Ok(true) => {}
                Ok(false) if cancelled.load(Ordering::SeqCst) => {
                    was_cancelled = true;
                    break;
                }
                Ok(false) => continue,
                Err(error) => {
                    capture_error.get_or_insert(error);
                    break;
                }
            }
            let count = match source.read(&mut buffer) {
                Ok(count) => count,
                Err(error) => {
                    capture_error.get_or_insert(error);
                    break;
                }
            };
            if count == 0 {
                break;
            }
            let keep = (count as u64).min(byte_cap.saturating_sub(written)) as usize;
            if keep > 0 && !logging_disabled {
                match output.write_all(&buffer[..keep]) {
                    Ok(()) => written += keep as u64,
                    Err(error) => {
                        capture_error.get_or_insert(error);
                        logging_disabled = true;
                    }
                }
            }
            if keep < count && !truncated {
                truncated = true;
                if let Err(error) = set_log_truncated(&state, &run_id, stream) {
                    capture_error.get_or_insert(error);
                    logging_disabled = true;
                }
            }
        }
        if !was_cancelled
            && !logging_disabled
            && let Err(error) = output.sync_all()
        {
            capture_error.get_or_insert(error);
        }
        if !was_cancelled && let Err(error) = close_log_stream(&state, &run_id, stream) {
            capture_error.get_or_insert(error);
        }
        capture_error.map_or(Ok(()), Err)
    })
}

fn set_log_truncated(state: &SharedWorkerState, run_id: &str, stream: LogStream) -> io::Result<()> {
    let column = match stream {
        LogStream::Stdout => "stdout_truncated",
        LogStream::Stderr => "stderr_truncated",
    };
    let sql = format!("UPDATE run_logs SET {column} = 1 WHERE run_id = ?1");
    let state = state
        .0
        .lock()
        .map_err(|_| io::Error::other("worker state lock poisoned"))?;
    state
        .connection
        .execute(&sql, [run_id])
        .map_err(storage_error)?;
    Ok(())
}

fn close_log_stream(state: &SharedWorkerState, run_id: &str, stream: LogStream) -> io::Result<()> {
    let (closed, other_closed) = match stream {
        LogStream::Stdout => ("stdout_closed", "stderr_closed"),
        LogStream::Stderr => ("stderr_closed", "stdout_closed"),
    };
    let sql = format!(
        "UPDATE run_logs SET {closed} = 1,
         completed_at_ms = CASE WHEN {other_closed} = 1 THEN ?1 ELSE completed_at_ms END
         WHERE run_id = ?2"
    );
    let timestamp = now_ms()?;
    let state = state
        .0
        .lock()
        .map_err(|_| io::Error::other("worker state lock poisoned"))?;
    state
        .connection
        .execute(&sql, params![timestamp, run_id])
        .map_err(storage_error)?;
    Ok(())
}

fn prune_completed_logs_shared(
    state: &SharedWorkerState,
    log_directory: &Path,
    retention_runs: usize,
) -> io::Result<()> {
    let candidates = {
        let worker_state = state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?;
        let mut statement = worker_state
            .connection
            .prepare(
                "SELECT run_id FROM run_logs
                 WHERE pruned = 0 AND stdout_closed = 1 AND stderr_closed = 1
                 ORDER BY completed_at_ms DESC, run_id DESC LIMIT -1 OFFSET ?1",
            )
            .map_err(storage_error)?;
        statement
            .query_map([retention_runs as i64], |row| row.get::<_, String>(0))
            .map_err(storage_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage_error)?
    };
    for id in candidates {
        for stream in [LogStream::Stdout, LogStream::Stderr] {
            let path = log_directory.join(format!("{id}.{}", stream.as_str()));
            match fs::symlink_metadata(&path) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "worker log path must be a regular file",
                    ));
                }
                Ok(_) => fs::remove_file(path)?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        let worker_state = state
            .0
            .lock()
            .map_err(|_| io::Error::other("worker state lock poisoned"))?;
        worker_state
            .connection
            .execute(
                "UPDATE run_logs SET pruned = 1 WHERE run_id = ?1 AND stdout_closed = 1 AND stderr_closed = 1",
                [id],
            )
            .map_err(storage_error)?;
    }
    Ok(())
}

fn persist_terminal_observation(
    state: &SharedWorkerState,
    terminal: &RunObservation,
) -> io::Result<()> {
    let mut state = state
        .0
        .lock()
        .map_err(|_| io::Error::other("worker state lock poisoned"))?;
    let transaction = state
        .connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(storage_error)?;
    let current_json = transaction
        .query_row(
            "SELECT observation_json FROM runs WHERE run_id = ?1",
            [terminal.run_id().as_str()],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(storage_error)?
        .ok_or_else(|| io::Error::other("terminal run record is missing"))?;
    let current: RunObservation = serde_json::from_str(&current_json).map_err(storage_error)?;
    if current.state().is_terminal() {
        transaction.commit().map_err(storage_error)?;
        return Ok(());
    }
    let next = if current.revision() < terminal.revision() {
        terminal.clone()
    } else {
        current
            .next_revision(
                terminal.state(),
                terminal.started_at_ms(),
                terminal.finished_at_ms(),
                terminal.exit_code(),
                terminal.termination_signal(),
                terminal.failure_reason(),
            )
            .ok_or_else(|| io::Error::other("observation revision exhausted"))?
            .with_optional_termination_reason(terminal.termination_reason())
    };
    let serialized = serde_json::to_string(&next).map_err(storage_error)?;
    let changed = transaction
        .execute(
            "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision = ?5 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
            params![serialized, next.revision() as i64, next.state().as_str(), next.run_id().as_str(), current.revision() as i64],
        )
        .map_err(storage_error)?;
    if changed != 1 {
        return Err(io::Error::other("terminal observation was not persisted"));
    }
    transaction.commit().map_err(storage_error)
}

fn emit_worker_diagnostic(event: &str, run_id: &str, error: &io::Error) {
    eprintln!(
        "{{\"timestampMs\":{},\"severity\":\"error\",\"component\":\"worker\",\"event\":{},\"runId\":{},\"errorKind\":{}}}",
        now_ms().unwrap_or_default(),
        serde_json::to_string(event).unwrap_or_else(|_| "null".into()),
        serde_json::to_string(run_id).unwrap_or_else(|_| "null".into()),
        serde_json::to_string(&format!("{:?}", error.kind())).unwrap_or_else(|_| "null".into()),
    );
}

fn emit_worker_event(event: &str, run_id: &str) {
    eprintln!(
        "{{\"timestampMs\":{},\"severity\":\"info\",\"component\":\"worker\",\"event\":{},\"runId\":{}}}",
        now_ms().unwrap_or_default(),
        serde_json::to_string(event).unwrap_or_else(|_| "null".into()),
        serde_json::to_string(run_id).unwrap_or_else(|_| "null".into()),
    );
}

struct RunSupervision {
    state: Arc<SharedWorkerState>,
    pending: Arc<PendingPersistence>,
    persistence_admission: Arc<Semaphore>,
    shutdown_signal: watch::Receiver<bool>,
    backend: Arc<dyn ExecutionBackend>,
    run_id: String,
    spawned: SpawnGuard,
    current: RunObservation,
    control: Arc<AtomicU8>,
    control_notify: Arc<Notify>,
    launched_at: std::time::Instant,
    timeout: Option<Duration>,
    grace: Duration,
    log_directory: PathBuf,
    retention_runs: usize,
    readiness_probe: Option<ReadinessProbe>,
    service_ports: Vec<conductor_core::AssignedServicePort>,
}

const MAX_READINESS_RESPONSE_BYTES: u64 = 8 * 1024;

async fn run_readiness_probe(
    client: &reqwest::Client,
    probe: &ReadinessProbe,
    port: &conductor_core::AssignedServicePort,
) -> bool {
    let timeout = Duration::from_millis(probe.timeout_ms());
    let loopback = if port
        .advertised_address()
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_ipv6())
    {
        "::1"
    } else {
        "127.0.0.1"
    };
    match probe.protocol() {
        ReadinessProbeProtocol::Tcp => tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect((loopback, port.port())),
        )
        .await
        .is_ok_and(|result| result.is_ok()),
        ReadinessProbeProtocol::Http => {
            let host = if loopback.contains(':') {
                format!("[{loopback}]")
            } else {
                loopback.to_owned()
            };
            let url = format!("http://{host}:{}/", port.port());
            let url = match reqwest::Url::parse(&url) {
                Ok(mut url) => {
                    let Some(path) = probe.path() else {
                        return false;
                    };
                    url.set_path(path);
                    url
                }
                Err(_) => return false,
            };
            let response = match tokio::time::timeout(timeout, client.get(url).send()).await {
                Ok(Ok(response)) => response,
                _ => return false,
            };
            if response
                .content_length()
                .is_some_and(|length| length > MAX_READINESS_RESPONSE_BYTES)
            {
                return false;
            }
            response.status().is_success()
        }
    }
}

async fn persist_readiness(
    state: &Arc<SharedWorkerState>,
    pending: &Arc<PendingPersistence>,
    admission: &Arc<Semaphore>,
    current: &mut RunObservation,
    readiness: ReadinessState,
) {
    if current.readiness() == Some(readiness) {
        return;
    }
    let Some(next) = current.next_readiness_revision(readiness) else {
        pending.degraded.store(true, Ordering::SeqCst);
        return;
    };
    let persisted = run_bounded_blocking(admission, {
        let state = Arc::clone(state);
        let next = next.clone();
        move || {
            let serialized = serde_json::to_string(&next)
                .map_err(|_| io::Error::other("readiness observation serialization failed"))?;
            let state = state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state is unavailable"))?;
            let changed = state.connection.execute(
                "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
                params![serialized, next.revision() as i64, next.state().as_str(), next.run_id().as_str()],
            )
            .map_err(io::Error::other)?;
            if changed != 1 {
                return Err(io::Error::other("readiness observation was superseded"));
            }
            Ok(())
        }
    })
    .await;
    match persisted {
        Ok(()) => *current = next,
        Err(_) => pending.degraded.store(true, Ordering::SeqCst),
    }
}

async fn run_bounded_blocking<T, F>(admission: &Arc<Semaphore>, operation: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    let permit = Arc::clone(admission)
        .try_acquire_owned()
        .map_err(|_| io::Error::other("bounded persistence executor is saturated"))?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    })
    .await
    .map_err(io::Error::other)?
}

async fn run_admitted_blocking<T, F>(admission: &Arc<Semaphore>, operation: F) -> io::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> io::Result<T> + Send + 'static,
{
    let permit = Arc::clone(admission)
        .acquire_owned()
        .await
        .map_err(io::Error::other)?;
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        operation()
    })
    .await
    .map_err(io::Error::other)?
}

async fn kill_process_group_off_executor(
    backend: &Arc<dyn ExecutionBackend>,
    group_id: u32,
) -> io::Result<()> {
    let backend = Arc::clone(backend);
    tokio::task::spawn_blocking(move || backend.kill_process_group(group_id))
        .await
        .map_err(io::Error::other)?
}

async fn supervise(supervision: RunSupervision) {
    let RunSupervision {
        state,
        pending,
        persistence_admission,
        mut shutdown_signal,
        backend,
        run_id,
        mut spawned,
        mut current,
        control: intent,
        control_notify,
        launched_at,
        timeout,
        grace,
        log_directory,
        retention_runs,
        readiness_probe,
        service_ports,
    } = supervision;
    let group_id = spawned.group_id;
    if spawned.child.is_none() {
        emit_worker_diagnostic(
            "supervisor_child_missing",
            run_id.as_str(),
            &io::Error::other("supervisor did not receive the child process"),
        );
        return;
    };
    let timeout_deadline = timeout.map(|limit| tokio::time::Instant::from_std(launched_at + limit));
    let probe_client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .ok();
    let mut probe_tasks = tokio::task::JoinSet::new();
    let mut next_probe_at = readiness_probe.as_ref().map(|probe| {
        tokio::time::Instant::from_std(
            launched_at + Duration::from_millis(probe.startup_grace_ms()),
        )
    });
    let mut consecutive_successes = 0_u16;
    let mut consecutive_failures = 0_u16;
    let mut status = None;
    let mut wait_failed = false;
    if *shutdown_signal.borrow() {
        let _ = intent.compare_exchange(
            INTENT_NONE,
            INTENT_SHUTDOWN,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
    }
    loop {
        if let Some(child) = spawned.child.as_mut() {
            match child.try_wait() {
                Ok(Some(exit)) => {
                    status = Some(exit);
                    wait_failed = false;
                    break;
                }
                Ok(None) => {}
                Err(_) => {
                    wait_failed = true;
                    let _ = intent.compare_exchange(
                        INTENT_NONE,
                        INTENT_SHUTDOWN,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
            }
        }
        if intent.load(Ordering::SeqCst) != INTENT_NONE {
            break;
        }
        if probe_tasks.is_empty()
            && next_probe_at.is_some_and(|deadline| tokio::time::Instant::now() >= deadline)
            && let Some(probe) = readiness_probe.as_ref()
        {
            let port = service_ports
                .iter()
                .find(|port| port.name() == probe.port_name())
                .cloned();
            let probe = probe.clone();
            let client = probe_client.clone();
            probe_tasks.spawn(async move {
                match (client, port) {
                    (Some(client), Some(port)) => run_readiness_probe(&client, &probe, &port).await,
                    _ => false,
                }
            });
        }
        let probe_result = async {
            match probe_tasks.join_next().await {
                Some(result) => result,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(probe_result);
        tokio::select! {
            result = &mut probe_result => {
                let probe_succeeded = result.unwrap_or(false);
                if let Some(probe) = readiness_probe.as_ref() {
                    next_probe_at = Some(tokio::time::Instant::now() + Duration::from_millis(probe.interval_ms()));
                    if probe_succeeded {
                        consecutive_successes = consecutive_successes.saturating_add(1);
                        consecutive_failures = 0;
                        if consecutive_successes >= probe.success_threshold() {
                            persist_readiness(&state, &pending, &persistence_admission, &mut current, ReadinessState::Ready).await;
                        }
                    } else {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        consecutive_successes = 0;
                        if consecutive_failures >= probe.failure_threshold() {
                            persist_readiness(&state, &pending, &persistence_admission, &mut current, ReadinessState::Unready).await;
                        }
                    }
                }
            },
            _ = control_notify.notified() => {},
            changed = shutdown_signal.changed() => {
                if changed.is_ok() && *shutdown_signal.borrow() {
                    let _ = intent.compare_exchange(
                        INTENT_NONE,
                        INTENT_SHUTDOWN,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    );
                }
            },
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            _ = async {
                match timeout_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                let _ = intent.compare_exchange(
                    INTENT_NONE,
                    INTENT_TIMEOUT,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
    }

    probe_tasks.abort_all();

    if intent.load(Ordering::SeqCst) != INTENT_NONE {
        let requested = intent.load(Ordering::SeqCst);
        match run_bounded_blocking(&persistence_admission, {
            let state = Arc::clone(&state);
            let run_id = run_id.clone();
            let mut current = current.clone();
            move || {
                if let Ok(state) = state.0.lock()
                    && let Ok(json) = state.connection.query_row(
                        "SELECT observation_json FROM runs WHERE run_id = ?1",
                        [&run_id],
                        |row| row.get::<_, String>(0),
                    )
                    && let Ok(latest) = serde_json::from_str::<RunObservation>(&json)
                {
                    current = latest;
                }
                let reason = match requested {
                    INTENT_TIMEOUT => RunTerminationReason::DeadlineExceeded,
                    INTENT_SHUTDOWN => RunTerminationReason::Shutdown,
                    _ => RunTerminationReason::Cancelled,
                };
                if !current.state().is_terminal() && current.state() != RunState::Stopping {
                    current = current
                        .next_revision(
                            RunState::Stopping,
                            current.started_at_ms(),
                            None,
                            None,
                            None,
                            None,
                        )
                        .map(|value| value.with_termination_reason(reason))
                        .unwrap_or(current);
                    if let Ok(serialized) = serde_json::to_string(&current)
                        && let Ok(state) = state.0.lock()
                    {
                        let _ = state.connection.execute(
                            "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
                            params![serialized, current.revision() as i64, current.state().as_str(), current.run_id().as_str()],
                        );
                    }
                }
                Ok(current)
            }
        })
        .await
        {
            Ok(updated) => current = updated,
            Err(error) => emit_worker_diagnostic("stopping_observation_failed", run_id.as_str(), &error),
        }
        let _ = backend.terminate_process_group(group_id);
        let grace_deadline = tokio::time::Instant::now() + grace;
        loop {
            if let Some(child) = spawned.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(exit)) => {
                        status = Some(exit);
                        wait_failed = false;
                        break;
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
            }
            if tokio::time::Instant::now() >= grace_deadline {
                let _ = kill_process_group_off_executor(&backend, group_id).await;
                if let Some(child) = spawned.child.as_mut() {
                    let _ = child.kill();
                    let reap_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
                    loop {
                        match child.try_wait() {
                            Ok(Some(exit)) => {
                                status = Some(exit);
                                wait_failed = false;
                                break;
                            }
                            Ok(None) if tokio::time::Instant::now() < reap_deadline => {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            Ok(None) | Err(_) => {
                                wait_failed = true;
                                break;
                            }
                        }
                    }
                } else {
                    wait_failed = true;
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let _ = kill_process_group_off_executor(&backend, group_id).await;
    // Reaping and output drain happen off the Tokio executor. Terminal process
    // state is persisted independently from log capture completion.
    let reader_completion = match spawned.enqueue_readers() {
        Ok(completion) => Some(completion),
        Err(error) => {
            emit_worker_diagnostic("log_capture_failed", run_id.as_str(), &error);
            None
        }
    };
    let cancel_readers = Arc::clone(&spawned.cancel_readers);
    let reader_state = Arc::clone(&state);
    let reader_run_id = run_id.clone();
    let reader_admission = Arc::clone(&persistence_admission);
    let reader_task = reader_completion.map(|completion| {
        tokio::spawn(async move {
            let mut incomplete = false;
            let mut completion = completion;
            match tokio::time::timeout(OUTPUT_DRAIN_GRACE, &mut completion).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    emit_worker_diagnostic("log_capture_failed", &reader_run_id, &error);
                }
                Ok(Err(_)) => {
                    emit_worker_diagnostic(
                        "log_capture_failed",
                        &reader_run_id,
                        &io::Error::new(io::ErrorKind::BrokenPipe, "reader reaper stopped"),
                    );
                }
                Err(_) => {
                    incomplete = true;
                    cancel_readers.store(true, Ordering::SeqCst);
                    emit_worker_diagnostic(
                        "log_capture_incomplete",
                        &reader_run_id,
                        &io::Error::new(io::ErrorKind::TimedOut, "output drain grace elapsed"),
                    );
                    match completion.await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            emit_worker_diagnostic("log_capture_failed", &reader_run_id, &error)
                        }
                        Err(_) => {}
                    }
                }
            }
            let close_run_id = reader_run_id.clone();
            let close_result = run_admitted_blocking(&reader_admission, move || {
                let stdout = close_log_stream(&reader_state, &close_run_id, LogStream::Stdout);
                let stderr = close_log_stream(&reader_state, &close_run_id, LogStream::Stderr);
                stdout.and(stderr)
            })
            .await;
            if let Err(error) = close_result {
                emit_worker_diagnostic(
                    if incomplete {
                        "log_capture_close_failed"
                    } else {
                        "log_capture_failed"
                    },
                    &reader_run_id,
                    &error,
                );
            }
        })
    });
    let finished = now_ms().unwrap_or(current.requested_at_ms());
    let (run_state, exit_code, signal, failure) = match status {
        Some(status) if !wait_failed => status_observation(status),
        _ => (
            RunState::Failed,
            None,
            None,
            Some(RunFailureReason::BackendFailure),
        ),
    };
    let termination = match intent.load(Ordering::SeqCst) {
        INTENT_TIMEOUT => Some(RunTerminationReason::DeadlineExceeded),
        INTENT_CANCEL => Some(RunTerminationReason::Cancelled),
        INTENT_SHUTDOWN => Some(RunTerminationReason::Shutdown),
        _ => None,
    };
    let final_state = match termination {
        Some(RunTerminationReason::DeadlineExceeded) => RunState::TimedOut,
        Some(RunTerminationReason::Cancelled | RunTerminationReason::Shutdown) => {
            RunState::Cancelled
        }
        None => run_state,
    };
    let terminal = current
        .next_revision(
            final_state,
            current.started_at_ms(),
            Some(finished),
            exit_code,
            signal,
            if termination.is_some() { None } else { failure },
        )
        .map(|value| match termination {
            Some(reason) => value.with_termination_reason(reason),
            None => value,
        });
    if let Some(terminal) = terminal
        && let Err(error) = run_bounded_blocking(&persistence_admission, {
            let state = Arc::clone(&state);
            let terminal = terminal.clone();
            move || persist_terminal_observation(&state, &terminal)
        })
        .await
    {
        pending.degraded.store(true, Ordering::SeqCst);
        let _ = pending.insert(terminal.clone());
        if let Ok(mut state) = state.0.lock() {
            state.active_groups.remove(&run_id);
        }
        emit_worker_diagnostic("terminal_persistence_pending", run_id.as_str(), &error);
        let mut retry_delay = Duration::from_millis(50);
        let mut shutdown_deadline = (*shutdown_signal.borrow())
            .then(|| tokio::time::Instant::now() + SHUTDOWN_PERSISTENCE_BUDGET);
        let mut persisted = false;
        loop {
            if shutdown_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                let _ = pending.mark_shutdown_exhausted(run_id.as_str());
                emit_worker_diagnostic(
                    "terminal_persistence_shutdown_budget_elapsed",
                    run_id.as_str(),
                    &io::Error::new(
                        io::ErrorKind::TimedOut,
                        "shutdown persistence budget elapsed",
                    ),
                );
                break;
            }
            if let Some(deadline) = shutdown_deadline {
                tokio::select! {
                    _ = tokio::time::sleep(retry_delay) => {},
                    _ = tokio::time::sleep_until(deadline) => {
                        let _ = pending.mark_shutdown_exhausted(run_id.as_str());
                        emit_worker_diagnostic(
                            "terminal_persistence_shutdown_budget_elapsed",
                            run_id.as_str(),
                            &io::Error::new(io::ErrorKind::TimedOut, "shutdown persistence budget elapsed"),
                        );
                        break;
                    }
                }
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(retry_delay) => {},
                    changed = shutdown_signal.changed() => {
                        if changed.is_ok() && *shutdown_signal.borrow() {
                            shutdown_deadline = Some(tokio::time::Instant::now() + SHUTDOWN_PERSISTENCE_BUDGET);
                        }
                    }
                }
            }
            if shutdown_deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
                let _ = pending.mark_shutdown_exhausted(run_id.as_str());
                emit_worker_diagnostic(
                    "terminal_persistence_shutdown_budget_elapsed",
                    run_id.as_str(),
                    &io::Error::new(
                        io::ErrorKind::TimedOut,
                        "shutdown persistence budget elapsed",
                    ),
                );
                break;
            }
            match run_bounded_blocking(&persistence_admission, {
                let state = Arc::clone(&state);
                let terminal = terminal.clone();
                move || persist_terminal_observation(&state, &terminal)
            })
            .await
            {
                Ok(()) => {
                    let _ = pending.remove(run_id.as_str());
                    persisted = true;
                    break;
                }
                Err(_) => {
                    retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
                }
            }
        }
        if persisted {
            emit_worker_event("terminal_persistence_recovered", run_id.as_str());
        }
    }
    // The process has exited and its terminal observation is durable (or its
    // persistence has been recorded as pending). Log draining and retention do
    // not consume an execution slot.
    if let Ok(mut state) = state.0.lock() {
        state.active_groups.remove(&run_id);
    }
    if let Some(reader_task) = reader_task {
        let _ = reader_task.await;
    }
    if let Err(error) = run_admitted_blocking(&persistence_admission, {
        let state = Arc::clone(&state);
        move || prune_completed_logs_shared(&state, &log_directory, retention_runs)
    })
    .await
    {
        emit_worker_diagnostic("log_retention_failed", run_id.as_str(), &error);
    }
}

fn status_observation(
    status: ExitStatus,
) -> (RunState, Option<i32>, Option<i32>, Option<RunFailureReason>) {
    if status.success() {
        (RunState::Succeeded, status.code(), None, None)
    } else if let Some(code) = status.code() {
        (
            RunState::Failed,
            Some(code),
            None,
            Some(RunFailureReason::NonZeroExit),
        )
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            let signal = status.signal();
            (
                RunState::Failed,
                None,
                signal,
                Some(RunFailureReason::SignalTerminated),
            )
        }
        #[cfg(not(unix))]
        {
            (
                RunState::Failed,
                None,
                None,
                Some(RunFailureReason::SignalTerminated),
            )
        }
    }
}

fn now_ms() -> io::Result<i64> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_millis();
    i64::try_from(milliseconds).map_err(|_| io::Error::other("system time exceeds supported range"))
}

fn host_inventory(capacity: usize, backend: &dyn ExecutionBackend) -> io::Result<WorkerInventory> {
    let available_cpu_count = thread::available_parallelism()?.get();
    let mut system = sysinfo::System::new();
    system.refresh_cpu_all();
    system.refresh_memory_specifics(sysinfo::MemoryRefreshKind::nothing().with_ram());
    let total_cpu_count = system.cpus().len();
    if total_cpu_count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "host CPU capacity is unavailable",
        ));
    }
    let total_cpu_millis = total_cpu_count
        .checked_mul(1_000)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "CPU capacity is out of range")
        })?;
    let total_memory_bytes = system.total_memory();
    let allocatable_cpu_millis = available_cpu_count
        .min(total_cpu_count)
        .checked_mul(800)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "allocatable CPU capacity is out of range",
            )
        })?;
    let allocatable_memory_bytes = total_memory_bytes.saturating_mul(80) / 100;
    let total =
        WorkerResourceCapacity::new(total_cpu_millis, total_memory_bytes).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "host capacity is unavailable")
        })?;
    let allocatable = WorkerResourceCapacity::new(allocatable_cpu_millis, allocatable_memory_bytes)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "allocatable capacity is unavailable",
            )
        })?;

    #[cfg(target_os = "linux")]
    let operating_system = OperatingSystem::Linux;
    #[cfg(target_os = "macos")]
    let operating_system = OperatingSystem::Macos;
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    return Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "worker inventory is supported only on Linux and macOS",
    ));

    #[cfg(target_arch = "x86_64")]
    let architecture = WorkerArchitecture::X86_64;
    #[cfg(target_arch = "aarch64")]
    let architecture = WorkerArchitecture::Aarch64;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    return Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "worker inventory is supported only on x86_64 and aarch64",
    ));

    let capabilities = WorkerCapabilities::new(
        operating_system,
        architecture,
        BTreeSet::from([WorkerExecutionBackend::NativeProcess]),
        backend.enforcement_capabilities(),
    )
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "worker capabilities are invalid",
        )
    })?;
    WorkerInventory::new(capabilities, total, allocatable, capacity)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "worker inventory is invalid"))
}

fn storage_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod host_inventory_tests {
    use std::io;
    use std::process::Child;

    use super::{ExecutionBackend, host_inventory};
    use conductor_core::JobSpec;

    struct NoExec;

    impl ExecutionBackend for NoExec {
        fn spawn(&self, _run_id: &str, _job: &JobSpec) -> io::Result<Child> {
            Err(io::Error::other("test backend does not spawn"))
        }

        fn terminate_process_group(&self, _group_id: u32) -> io::Result<()> {
            Ok(())
        }

        fn kill_process_group(&self, _group_id: u32) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn host_inventory_reports_safe_platform_capacity_with_headroom() {
        let inventory = host_inventory(4, &NoExec).expect("host inventory is available");
        assert!(inventory.total().cpu_millis() > 0);
        assert!(inventory.total().memory_bytes() > 0);
        assert!(inventory.allocatable().fits_within(inventory.total()));
        assert!(inventory.allocatable().cpu_millis() < inventory.total().cpu_millis());
        assert!(inventory.allocatable().memory_bytes() < inventory.total().memory_bytes());
        assert_eq!(inventory.execution_slots(), 4);
        assert!(
            inventory
                .backends()
                .contains(&conductor_core::ExecutionBackend::NativeProcess)
        );
        assert!(inventory.enforcement().is_empty());
    }
}
