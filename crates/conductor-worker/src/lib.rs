//! Worker-owned process supervision, persistence, deadlines, and captured output.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ExitStatus};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::fd::AsFd;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use conductor_core::{
    InspectRun, JobSpec, LogStream, MAX_LOG_CHUNK_BYTES, MAX_WORKER_CAPACITY, ReadRunLogs,
    ResolveRun, RunFailureReason, RunLogChunk, RunLogStatus, RunObservation, RunState,
    RunTerminationReason, StartRun, StopRun, WorkerDescriptor, WorkerId, WorkerStatus,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use tokio::runtime::Runtime;
use tokio::sync::{Notify, Semaphore, oneshot, watch};

#[cfg(unix)]
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};

const BUSY_TIMEOUT: Duration = Duration::from_millis(250);
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
    fn spawn(&self, job: &JobSpec) -> io::Result<Child>;

    /// Request graceful termination of every process in the execution group.
    fn terminate_process_group(&self, group_id: u32) -> io::Result<()>;

    /// Force-kill every process still in the execution's process group.
    fn kill_process_group(&self, group_id: u32) -> io::Result<()>;
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
}

#[derive(Clone)]
struct ReaderReaper {
    sender: SyncSender<ReaderReapRequest>,
    pending: Arc<AtomicUsize>,
}

impl ReaderReaper {
    fn new() -> io::Result<Self> {
        let (sender, receiver) = sync_channel::<ReaderReapRequest>(MAX_WORKER_CAPACITY);
        let pending = Arc::new(AtomicUsize::new(0));
        let reaper_pending = Arc::clone(&pending);
        thread::Builder::new()
            .name("conductor-reader-reaper".to_owned())
            .spawn(move || {
                while let Ok(request) = receiver.recv() {
                    let result = reap_child_and_readers(request.child, request.readers);
                    reaper_pending.fetch_sub(1, Ordering::SeqCst);
                    if let Some(completion) = request.completion {
                        let _ = completion.send(result);
                    }
                }
            })?;
        Ok(Self { sender, pending })
    }

    fn enqueue(
        &self,
        child: Option<Child>,
        readers: Vec<JoinHandle<io::Result<()>>>,
    ) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let (completion, receiver) = oneshot::channel();
        if readers.is_empty() && child.is_none() {
            let _ = completion.send(Ok(()));
            return Ok(receiver);
        }
        self.pending.fetch_add(1, Ordering::SeqCst);
        if self
            .sender
            .send(ReaderReapRequest {
                child,
                readers,
                completion: Some(completion),
            })
            .is_err()
        {
            self.pending.fetch_sub(1, Ordering::SeqCst);
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "output reader reaper is unavailable",
            ));
        }
        Ok(receiver)
    }

    fn enqueue_detached(&self, child: Option<Child>, readers: Vec<JoinHandle<io::Result<()>>>) {
        if readers.is_empty() && child.is_none() {
            return;
        }
        self.pending.fetch_add(1, Ordering::SeqCst);
        if self
            .sender
            .send(ReaderReapRequest {
                child,
                readers,
                completion: None,
            })
            .is_err()
        {
            self.pending.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn pending(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }
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
    cancel_readers: Arc<AtomicBool>,
    cleaned: bool,
}

impl SpawnGuard {
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
        self.reader_reaper
            .enqueue(self.child.take(), std::mem::take(&mut self.readers))
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
        self.reader_reaper
            .enqueue_detached(self.child.take(), std::mem::take(&mut self.readers));
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
    incarnation: u64,
    state_directory: PathBuf,
    log_policy: LogPolicy,
    backend: Arc<dyn ExecutionBackend>,
    state: Arc<SharedWorkerState>,
    pending: Arc<PendingPersistence>,
    persistence_admission: Arc<Semaphore>,
    shutdown_signal: watch::Sender<bool>,
    reader_reaper: ReaderReaper,
    runtime: Runtime,
    supervisors: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    lifecycle: Mutex<bool>,
}

impl Drop for WorkerService {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

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
        if !(0..=3).contains(&schema_version) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "worker database schema is newer than supported",
            ));
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
                UPDATE worker_meta SET incarnation = incarnation + 1 WHERE id = 1;
                PRAGMA user_version = 3;",
                )
                .map_err(storage_error)?;
            let generation = transaction
                .query_row(
                    "SELECT incarnation FROM worker_meta WHERE id = 1",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(storage_error)?;
            transaction.commit().map_err(storage_error)?;
            u64::try_from(generation)
                .map_err(|_| io::Error::other("worker incarnation is invalid"))?
        };

        let service = Self {
            worker_id,
            capacity,
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
            runtime,
            supervisors: Mutex::new(Vec::new()),
            lifecycle: Mutex::new(false),
        };
        service.mark_interrupted_runs_unknown()?;
        service.enforce_legacy_log_limits()?;
        Ok(service)
    }

    pub fn start(&self, command: StartRun) -> Result<RunObservation, WorkerError> {
        let lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        if *lifecycle {
            return Err(WorkerError::ShuttingDown);
        }
        let spec = command.spec();
        if spec.worker_id() != &self.worker_id {
            return Err(WorkerError::WorkerMismatch);
        }
        let spec_json = serde_json::to_string(spec).map_err(|_| WorkerError::StorageUnavailable)?;
        let now = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let observation = {
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
            observation
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

        let child = match self.backend.spawn(spec.job()) {
            Ok(child) => child,
            Err(_) => {
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
        Ok(WorkerStatus::new(
            self.descriptor(),
            used,
            unknown,
            !draining && !degraded,
            draining,
            observed_at,
        )
        .with_runtime_state(live_processes, pending, degraded))
    }

    pub fn is_draining(&self) -> bool {
        self.lifecycle
            .lock()
            .map(|draining| *draining)
            .unwrap_or(true)
    }

    pub fn is_degraded(&self) -> bool {
        self.pending.degraded.load(Ordering::SeqCst)
    }

    /// Stop admission immediately and notify every live supervisor to begin cleanup.
    pub fn begin_shutdown(&self) {
        self.shutdown_signal.send_replace(true);
        if let Ok(mut lifecycle) = self.lifecycle.lock() {
            *lifecycle = true;
        } else {
            return;
        }
        if let Ok(state) = self.state.0.lock() {
            for active in state.active_groups.values() {
                let _ = active.intent.compare_exchange(
                    INTENT_NONE,
                    INTENT_SHUTDOWN,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
                active.notify.notify_one();
            }
        }
    }

    pub fn shutdown(&self) -> io::Result<()> {
        self.begin_shutdown();
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
                Some(RunFailureReason::SpawnFailed),
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
    let mut status = None;
    let mut wait_failed = false;
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
        tokio::select! {
            _ = control_notify.notified() => {},
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
                let _ = backend.kill_process_group(group_id);
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
    let _ = backend.kill_process_group(group_id);
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
            let close_result = run_bounded_blocking(&reader_admission, move || {
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
    if let Ok(mut state) = state.0.lock() {
        state.active_groups.remove(&run_id);
    }
    if let Err(error) = run_bounded_blocking(&persistence_admission, {
        let state = Arc::clone(&state);
        move || prune_completed_logs_shared(&state, &log_directory, retention_runs)
    })
    .await
    {
        emit_worker_diagnostic("log_retention_failed", run_id.as_str(), &error);
    }
    if let Some(reader_task) = reader_task {
        let _ = reader_task.await;
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

fn storage_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
