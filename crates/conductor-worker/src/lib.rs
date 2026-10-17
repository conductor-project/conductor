//! Worker-owned process supervision, persistence, deadlines, and captured output.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ExitStatus};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use conductor_core::{
    InspectRun, JobSpec, MAX_WORKER_CAPACITY, RunFailureReason, RunObservation, RunState, StartRun,
    WorkerDescriptor, WorkerId,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

const BUSY_TIMEOUT: Duration = Duration::from_millis(250);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Worker execution seam implemented by the host-specific process crate.
pub trait ExecutionBackend: Send + Sync {
    /// Spawn one job in a dedicated process group with stdin closed and output piped.
    fn spawn(&self, job: &JobSpec) -> io::Result<Child>;

    /// Force-kill every process still in the execution's process group.
    fn kill_process_group(&self, group_id: u32) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerError {
    CapacityReached,
    RunConflict,
    WorkerMismatch,
    ShuttingDown,
    StorageUnavailable,
}

struct WorkerState {
    connection: Connection,
    active_groups: BTreeMap<String, u32>,
}

struct SharedWorkerState(Mutex<WorkerState>);

pub struct WorkerService {
    worker_id: WorkerId,
    capacity: usize,
    state_directory: PathBuf,
    backend: Arc<dyn ExecutionBackend>,
    state: Arc<SharedWorkerState>,
    supervisors: Mutex<Vec<JoinHandle<()>>>,
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
        let state_directory = state_directory.as_ref().to_path_buf();
        if capacity == 0 || capacity > MAX_WORKER_CAPACITY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker capacity must be between 1 and 1000",
            ));
        }
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
        if !(0..=1).contains(&schema_version) {
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
                );",
                )
                .map_err(storage_error)?;
            transaction
                .pragma_update(None, "user_version", 1)
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

        let service = Self {
            worker_id,
            capacity,
            state_directory,
            backend,
            state: Arc::new(SharedWorkerState(Mutex::new(WorkerState {
                connection,
                active_groups: BTreeMap::new(),
            }))),
            supervisors: Mutex::new(Vec::new()),
            lifecycle: Mutex::new(false),
        };
        service.mark_interrupted_runs_unknown()?;
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
            let active_count: i64 = transaction.query_row(
                "SELECT COUNT(*) FROM runs WHERE state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
                [],
                |row| row.get(0),
            ).map_err(|_| WorkerError::StorageUnavailable)?;
            if active_count >= self.capacity as i64 {
                return Err(WorkerError::CapacityReached);
            }
            let observation = RunObservation::requested(spec, now);
            let observation_json =
                serde_json::to_string(&observation).map_err(|_| WorkerError::StorageUnavailable)?;
            transaction.execute(
                "INSERT INTO runs (run_id, spec_json, observation_json, state, revision) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![spec.id().as_str(), spec_json, observation_json, observation.state().as_str(), observation.revision() as i64],
            ).map_err(|_| WorkerError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            observation
        };

        let starting = observation
            .next_revision(RunState::Starting, None, None, None, None, None)
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&starting)?;

        let mut child = match self.backend.spawn(spec.job()) {
            Ok(child) => child,
            Err(_) => {
                return self.finish_spawn_failure(starting);
            }
        };
        let group_id = child.id();
        let Some(stdout) = child.stdout.take() else {
            return self.abort_spawned_child(child, group_id, starting);
        };
        let Some(stderr) = child.stderr.take() else {
            return self.abort_spawned_child(child, group_id, starting);
        };
        let stdout_path = self
            .state_directory
            .join("runs")
            .join(format!("{}.stdout", spec.id()));
        let stderr_path = self
            .state_directory
            .join("runs")
            .join(format!("{}.stderr", spec.id()));
        let stdout_reader = match spawn_capture(stdout, stdout_path) {
            Ok(reader) => reader,
            Err(_) => return self.abort_spawned_child(child, group_id, starting),
        };
        let stderr_reader = match spawn_capture(stderr, stderr_path) {
            Ok(reader) => reader,
            Err(_) => return self.abort_spawned_child(child, group_id, starting),
        };
        let started = now_ms().map_err(|_| WorkerError::StorageUnavailable)?;
        let running = starting
            .next_revision(RunState::Running, Some(started), None, None, None, None)
            .ok_or(WorkerError::StorageUnavailable)?;
        self.persist(&running)?;
        {
            let mut state = self
                .state
                .0
                .lock()
                .map_err(|_| WorkerError::StorageUnavailable)?;
            state.active_groups.insert(spec.id().to_string(), group_id);
        }

        let state = Arc::clone(&self.state);
        let backend = Arc::clone(&self.backend);
        let run_id = spec.id().to_string();
        let supervisor_observation = running.clone();
        let child_slot = Arc::new(Mutex::new(Some(child)));
        let supervisor_child = Arc::clone(&child_slot);
        let supervisor = thread::Builder::new()
            .name(format!("conductor-run-{run_id}"))
            .spawn(move || {
                let child = supervisor_child
                    .lock()
                    .ok()
                    .and_then(|mut child| child.take());
                if let Some(child) = child {
                    supervise(
                        state,
                        backend,
                        run_id,
                        child,
                        stdout_reader,
                        stderr_reader,
                        supervisor_observation,
                    );
                }
            });
        let supervisor = match supervisor {
            Ok(supervisor) => supervisor,
            Err(_) => {
                if let Ok(mut slot) = child_slot.lock()
                    && let Some(mut child) = slot.take()
                {
                    let _ = self.backend.kill_process_group(group_id);
                    let _ = child.kill();
                    let _ = child.wait();
                }
                if let Ok(mut state) = self.state.0.lock() {
                    state.active_groups.remove(spec.id().as_str());
                }
                return self.finish_backend_failure(running);
            }
        };
        let mut supervisors = self
            .supervisors
            .lock()
            .map_err(|_| WorkerError::StorageUnavailable)?;
        let mut unfinished = Vec::with_capacity(supervisors.len() + 1);
        for old in supervisors.drain(..) {
            if old.is_finished() {
                let _ = old.join();
            } else {
                unfinished.push(old);
            }
        }
        unfinished.push(supervisor);
        *supervisors = unfinished;
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

    pub fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.worker_id.clone(), self.capacity, true)
    }

    pub fn shutdown(&self) -> io::Result<()> {
        {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| io::Error::other("worker lifecycle lock poisoned"))?;
            if *lifecycle {
                return Ok(());
            }
            *lifecycle = true;
        }
        let groups = {
            let state = self
                .state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state lock poisoned"))?;
            state.active_groups.values().copied().collect::<Vec<_>>()
        };
        let mut first_error = None;
        for group in groups {
            if let Err(error) = self.backend.kill_process_group(group) {
                first_error.get_or_insert(error);
            }
        }
        let deadline = std::time::Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            let active = self
                .state
                .0
                .lock()
                .map_err(|_| io::Error::other("worker state lock poisoned"))?
                .active_groups
                .len();
            if active == 0 || std::time::Instant::now() >= deadline {
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
        {
            first_error.get_or_insert(io::Error::new(
                io::ErrorKind::TimedOut,
                "worker shutdown deadline elapsed",
            ));
        }
        if let Ok(mut supervisors) = self.supervisors.lock() {
            for supervisor in supervisors.drain(..) {
                if supervisor.is_finished() && supervisor.join().is_err() {
                    first_error.get_or_insert(io::Error::other("run supervisor panicked"));
                }
            }
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
                .ok_or_else(|| io::Error::other("observation revision exhausted"))?;
            let serialized = serde_json::to_string(&unknown).map_err(storage_error)?;
            transaction.execute(
                "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision = ?5",
                params![serialized, unknown.revision() as i64, unknown.state().as_str(), unknown.run_id().as_str(), observation.revision() as i64],
            ).map_err(storage_error)?;
        }
        transaction.commit().map_err(storage_error)
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
        Ok(failed)
    }

    fn abort_spawned_child(
        &self,
        mut child: Child,
        group_id: u32,
        current: RunObservation,
    ) -> Result<RunObservation, WorkerError> {
        let _ = self.backend.kill_process_group(group_id);
        let _ = child.kill();
        let _ = child.wait();
        self.finish_backend_failure(current)
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
        Ok(failed)
    }
}

fn spawn_capture<R: Read + Send + 'static>(
    mut source: R,
    path: PathBuf,
) -> io::Result<JoinHandle<io::Result<()>>> {
    thread::Builder::new().spawn(move || {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut output = options.open(path)?;
        let mut buffer = [0_u8; 8192];
        loop {
            let count = source.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            output.write_all(&buffer[..count])?;
        }
        output.sync_all()?;
        Ok(())
    })
}

fn supervise(
    state: Arc<SharedWorkerState>,
    backend: Arc<dyn ExecutionBackend>,
    run_id: String,
    mut child: Child,
    stdout_reader: JoinHandle<io::Result<()>>,
    stderr_reader: JoinHandle<io::Result<()>>,
    current: RunObservation,
) {
    let group_id = child.id();
    let (status, wait_failed) = match child.wait() {
        Ok(status) => (Some(status), false),
        Err(_) => {
            let _ = backend.kill_process_group(group_id);
            (child.wait().ok(), true)
        }
    };
    let _ = backend.kill_process_group(group_id);
    let output_failed = stdout_reader.join().map_or(true, |result| result.is_err())
        || stderr_reader.join().map_or(true, |result| result.is_err());
    let finished = now_ms().unwrap_or(current.requested_at_ms());
    let (run_state, exit_code, signal, failure) = match status {
        Some(status) if !wait_failed && !output_failed => status_observation(status),
        _ => (
            RunState::Failed,
            None,
            None,
            Some(RunFailureReason::BackendFailure),
        ),
    };
    let terminal = current.next_revision(
        run_state,
        current.started_at_ms(),
        Some(finished),
        exit_code,
        signal,
        failure,
    );
    if let Some(terminal) = terminal
        && let Ok(serialized) = serde_json::to_string(&terminal)
        && let Ok(state) = state.0.lock()
    {
        let _ = state.connection.execute(
            "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
            params![serialized, terminal.revision() as i64, terminal.state().as_str(), terminal.run_id().as_str()],
        );
    }
    if let Ok(mut state) = state.0.lock() {
        state.active_groups.remove(&run_id);
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
