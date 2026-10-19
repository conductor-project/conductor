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
/// Current worker database schema understood by this build.
pub const CURRENT_SCHEMA_VERSION: i64 = SCHEMA_VERSION;
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

#[path = "structs/stored_worker_report.rs"]
mod stored_worker_report;
pub use stored_worker_report::StoredWorkerReport;

#[path = "structs/log_policy.rs"]
mod log_policy;
pub use log_policy::LogPolicy;

#[path = "structs/worker_state.rs"]
mod worker_state;
use worker_state::WorkerState;

#[path = "structs/active_group.rs"]
mod active_group;
use active_group::ActiveGroup;

#[path = "structs/reader_reap_request.rs"]
mod reader_reap_request;
use reader_reap_request::ReaderReapRequest;

#[path = "structs/reader_reaper.rs"]
mod reader_reaper;
use reader_reaper::ReaderReaper;

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

#[path = "structs/spawn_guard.rs"]
mod spawn_guard;
use spawn_guard::SpawnGuard;

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

const INTENT_NONE: u8 = 0;
const INTENT_CANCEL: u8 = 1;
const INTENT_TIMEOUT: u8 = 2;
const INTENT_SHUTDOWN: u8 = 3;

#[path = "structs/shared_worker_state.rs"]
mod shared_worker_state;
use shared_worker_state::SharedWorkerState;

#[path = "structs/pending_persistence.rs"]
mod pending_persistence;
use pending_persistence::PendingPersistence;

#[path = "structs/worker_service.rs"]
mod worker_service;
pub use worker_service::WorkerService;

#[cfg(test)]
#[path = "reader_reaper_tests.rs"]
mod reader_reaper_tests;

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

#[path = "structs/run_supervision.rs"]
mod run_supervision;
use run_supervision::RunSupervision;

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
    include!("structs/no_exec.rs");

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
