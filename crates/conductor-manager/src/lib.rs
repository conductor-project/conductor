//! Manager-owned application operations and durable job-definition storage.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, io};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use conductor_core::{
    InspectRun, JobId, JobSpec, RunFailureReason, RunId, RunObservation, RunSpec, RunState,
    RunTerminationReason, StartRun, StopRun, WorkerDescriptor, WorkerId,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

/// Temporary limit for active jobs. Pagination is a later milestone.
pub const MAX_JOBS: usize = 1_000;
const MAX_RESERVED_JOB_IDS: usize = 10_000;
const SCHEMA_VERSION: i64 = 2;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobStoreError {
    DuplicateId,
    LimitExceeded,
    ActiveRunConflict,
    StorageUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStoreError {
    JobNotFound,
    JobConflict,
    CapacityReached,
    StorageUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerPortError {
    CapacityReached,
    RunConflict,
    ShuttingDown,
    Unavailable,
}

/// Manager-owned seam for dispatch and observation reads from the selected worker.
pub trait WorkerPort: Send + Sync {
    fn descriptor(&self) -> WorkerDescriptor;
    fn is_draining(&self) -> bool {
        false
    }
    fn start(&self, command: StartRun) -> Result<RunObservation, WorkerPortError>;
    fn inspect(&self, command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError>;
    fn stop(&self, command: StopRun) -> Result<RunObservation, WorkerPortError>;
}

/// Storage port owned by the manager. Implementations preserve deleted IDs.
pub trait JobRepository: Send + Sync {
    fn insert(&self, job: JobSpec) -> Result<(), JobStoreError>;
    fn get(&self, id: &JobId) -> Result<Option<JobSpec>, JobStoreError>;
    fn list(&self) -> Result<Vec<JobSpec>, JobStoreError>;
    fn delete(&self, id: &JobId) -> Result<bool, JobStoreError>;
    fn create_run(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_capacity: usize,
    ) -> Result<RunSpec, RunStoreError>;
    fn get_run(&self, id: &RunId) -> Result<Option<RunObservation>, RunStoreError>;
    fn list_active_runs(
        &self,
        worker_id: &WorkerId,
        limit: usize,
    ) -> Result<Vec<RunObservation>, RunStoreError>;
    fn list_runs(
        &self,
        job_id: Option<&JobId>,
        state: Option<RunState>,
        limit: usize,
    ) -> Result<Vec<RunObservation>, RunStoreError>;
    fn record_observation(&self, observation: &RunObservation) -> Result<(), RunStoreError>;
}

#[derive(Debug)]
pub enum RepositoryOpenError {
    Sqlite(rusqlite::Error),
    Io(io::Error),
    UnsupportedSchemaVersion(i64),
    InvalidLegacySchema,
    TimestampUnavailable,
}

impl std::fmt::Display for RepositoryOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(error) => write!(
                f,
                "manager database could not be opened or migrated: {error}"
            ),
            Self::Io(error) => write!(
                f,
                "manager database file could not be prepared securely: {error}"
            ),
            Self::UnsupportedSchemaVersion(version) => {
                write!(
                    f,
                    "manager database schema version {version} is newer than supported"
                )
            }
            Self::InvalidLegacySchema => {
                f.write_str("manager database has an invalid legacy schema")
            }
            Self::TimestampUnavailable => f.write_str("system time is unavailable"),
        }
    }
}

impl std::error::Error for RepositoryOpenError {}

/// Manager-owned SQLite repository. One serialized connection keeps access bounded and simple.
pub struct SqliteJobRepository {
    connection: Mutex<Connection>,
}

impl SqliteJobRepository {
    /// Open or create the manager database, enable durable local settings, and migrate it.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RepositoryOpenError> {
        prepare_database_file(path.as_ref()).map_err(RepositoryOpenError::Io)?;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(RepositoryOpenError::Sqlite)?;
        connection
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .map_err(RepositoryOpenError::Sqlite)?;

        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(RepositoryOpenError::Sqlite)?;
        if version > SCHEMA_VERSION {
            return Err(RepositoryOpenError::UnsupportedSchemaVersion(version));
        }

        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(RepositoryOpenError::Sqlite)?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(RepositoryOpenError::Sqlite)?;
            let mode: String = connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(RepositoryOpenError::Sqlite)?;
            if !mode.eq_ignore_ascii_case("wal") {
                return Err(RepositoryOpenError::Sqlite(rusqlite::Error::InvalidQuery));
            }
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(RepositoryOpenError::Sqlite)?;
        migrate(&connection).map_err(|error| match error {
            MigrationError::Sqlite(error) => RepositoryOpenError::Sqlite(error),
            MigrationError::InvalidLegacySchema => RepositoryOpenError::InvalidLegacySchema,
        })?;

        Ok(Self {
            connection: Mutex::new(connection),
        })
    }
}

fn prepare_database_file(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "manager database path must be a regular file, not a symbolic link",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut options = fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            options.mode(0o600);
            match options.open(path) {
                Ok(file) => drop(file),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "manager database path must be a regular file, not a symbolic link",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

enum MigrationError {
    Sqlite(rusqlite::Error),
    InvalidLegacySchema,
}

fn migrate(connection: &Connection) -> Result<(), MigrationError> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(MigrationError::Sqlite)?;
    if !(0..=SCHEMA_VERSION).contains(&version) {
        return Err(MigrationError::Sqlite(rusqlite::Error::InvalidQuery));
    }
    if version == SCHEMA_VERSION {
        connection
            .prepare("SELECT id, spec_json, created_at_ms, deleted_at_ms FROM jobs LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT run_id, job_id, worker_id, job_spec_json, observation_json, revision, state FROM runs LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        return Ok(());
    }

    let transaction = connection
        .unchecked_transaction()
        .map_err(MigrationError::Sqlite)?;
    if version == 0 {
        let table_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs')",
                [],
                |row| row.get(0),
            )
            .map_err(MigrationError::Sqlite)?;
        if !table_exists {
            let unexpected_tables: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%')",
                    [],
                    |row| row.get(0),
                )
                .map_err(MigrationError::Sqlite)?;
            if unexpected_tables {
                return Err(MigrationError::InvalidLegacySchema);
            }
            transaction
                .execute_batch(
                    "CREATE TABLE jobs (
                        id TEXT PRIMARY KEY NOT NULL,
                        spec_json TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
                        deleted_at_ms INTEGER CHECK (deleted_at_ms IS NULL OR deleted_at_ms >= 0)
                    );",
                )
                .map_err(MigrationError::Sqlite)?;
        } else {
            let columns = table_columns(&transaction).map_err(MigrationError::Sqlite)?;
            if !columns.iter().any(|column| column == "id")
                || !columns.iter().any(|column| column == "spec_json")
                || !columns.iter().any(|column| column == "created_at_ms")
            {
                return Err(MigrationError::InvalidLegacySchema);
            }
            if !columns.iter().any(|column| column == "deleted_at_ms") {
                transaction
                    .execute(
                        "ALTER TABLE jobs ADD COLUMN deleted_at_ms INTEGER CHECK (deleted_at_ms IS NULL OR deleted_at_ms >= 0)",
                        [],
                    )
                    .map_err(MigrationError::Sqlite)?;
            }
        }
    }
    transaction
        .execute_batch(
            "CREATE TABLE runs (
            run_id TEXT PRIMARY KEY NOT NULL,
            job_id TEXT NOT NULL,
            worker_id TEXT NOT NULL,
            job_spec_json TEXT NOT NULL,
            observation_json TEXT NOT NULL,
            revision INTEGER NOT NULL CHECK (revision > 0),
            state TEXT NOT NULL
        );
        CREATE INDEX runs_by_state_worker ON runs(state, worker_id);
        CREATE INDEX runs_by_job_created ON runs(job_id, run_id);
        CREATE TABLE run_sequence (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL);
        INSERT INTO run_sequence (id, value) VALUES (1, 0);",
        )
        .map_err(MigrationError::Sqlite)?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(MigrationError::Sqlite)?;
    transaction.commit().map_err(MigrationError::Sqlite)
}

fn table_columns(connection: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare("PRAGMA table_info(jobs)")?;
    statement.query_map([], |row| row.get(1))?.collect()
}

impl JobRepository for SqliteJobRepository {
    fn insert(&self, job: JobSpec) -> Result<(), JobStoreError> {
        let spec_json =
            serde_json::to_string(&job).map_err(|_| JobStoreError::StorageUnavailable)?;
        let created_at_ms = now_ms().map_err(|_| JobStoreError::StorageUnavailable)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JobStoreError::StorageUnavailable)?;

        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1)",
                [job.id().as_str()],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if exists {
            return Err(JobStoreError::DuplicateId);
        }
        let active_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE deleted_at_ms IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let reserved_count: i64 = transaction
            .query_row("SELECT COUNT(*) FROM jobs", [], |row| row.get(0))
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if active_count >= MAX_JOBS as i64 || reserved_count >= MAX_RESERVED_JOB_IDS as i64 {
            return Err(JobStoreError::LimitExceeded);
        }
        transaction
            .execute(
                "INSERT INTO jobs (id, spec_json, created_at_ms, deleted_at_ms) VALUES (?1, ?2, ?3, NULL)",
                params![job.id().as_str(), spec_json, created_at_ms],
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| JobStoreError::StorageUnavailable)
    }

    fn get(&self, id: &JobId) -> Result<Option<JobSpec>, JobStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let serialized = connection
            .query_row(
                "SELECT spec_json FROM jobs WHERE id = ?1 AND deleted_at_ms IS NULL",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        serialized
            .map(|value| {
                serde_json::from_str(&value).map_err(|_| JobStoreError::StorageUnavailable)
            })
            .transpose()
    }

    fn list(&self) -> Result<Vec<JobSpec>, JobStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT spec_json FROM jobs WHERE deleted_at_ms IS NULL ORDER BY id COLLATE BINARY",
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        rows.map(|row| {
            let serialized = row.map_err(|_| JobStoreError::StorageUnavailable)?;
            serde_json::from_str(&serialized).map_err(|_| JobStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn delete(&self, id: &JobId) -> Result<bool, JobStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let active_run: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runs WHERE job_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown'))",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if active_run {
            return Err(JobStoreError::ActiveRunConflict);
        }
        let deleted_at_ms = now_ms().map_err(|_| JobStoreError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE jobs SET deleted_at_ms = ?1 WHERE id = ?2 AND deleted_at_ms IS NULL",
                params![deleted_at_ms, id.as_str()],
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        Ok(changed == 1)
    }

    fn create_run(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_capacity: usize,
    ) -> Result<RunSpec, RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let job_json = transaction
            .query_row(
                "SELECT spec_json FROM jobs WHERE id = ?1 AND deleted_at_ms IS NULL",
                [job_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?
            .ok_or(RunStoreError::JobNotFound)?;
        let job: JobSpec =
            serde_json::from_str(&job_json).map_err(|_| RunStoreError::StorageUnavailable)?;
        let existing_for_job: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM runs WHERE job_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown'))",
            [job_id.as_str()],
            |row| row.get(0),
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        if existing_for_job {
            return Err(RunStoreError::JobConflict);
        }
        let active_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM runs WHERE worker_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
            [worker_id.as_str()],
            |row| row.get(0),
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        if active_count >= worker_capacity as i64 {
            return Err(RunStoreError::CapacityReached);
        }
        transaction
            .execute("UPDATE run_sequence SET value = value + 1 WHERE id = 1", [])
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let sequence: i64 = transaction
            .query_row("SELECT value FROM run_sequence WHERE id = 1", [], |row| {
                row.get(0)
            })
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let run_id =
            RunId::new(format!("run-{sequence}")).map_err(|_| RunStoreError::StorageUnavailable)?;
        let spec = RunSpec::new(run_id, worker_id.clone(), job);
        let observation = RunObservation::requested(
            &spec,
            now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
        );
        let spec_json =
            serde_json::to_string(&spec).map_err(|_| RunStoreError::StorageUnavailable)?;
        let observation_json =
            serde_json::to_string(&observation).map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction.execute(
            "INSERT INTO runs (run_id, job_id, worker_id, job_spec_json, observation_json, revision, state) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![spec.id().as_str(), job_id.as_str(), worker_id.as_str(), spec_json, observation_json, observation.revision() as i64, observation.state().as_str()],
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        Ok(spec)
    }

    fn get_run(&self, id: &RunId) -> Result<Option<RunObservation>, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let json = connection
            .query_row(
                "SELECT observation_json FROM runs WHERE run_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        json.map(|value| {
            serde_json::from_str(&value).map_err(|_| RunStoreError::StorageUnavailable)
        })
        .transpose()
    }

    fn list_active_runs(
        &self,
        worker_id: &WorkerId,
        limit: usize,
    ) -> Result<Vec<RunObservation>, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let mut statement = connection.prepare(
            "SELECT observation_json FROM runs WHERE worker_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown') ORDER BY rowid DESC LIMIT ?2",
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map(params![worker_id.as_str(), limit as i64], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|_| RunStoreError::StorageUnavailable)?)
                .map_err(|_| RunStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn list_runs(
        &self,
        job_id: Option<&JobId>,
        state_filter: Option<RunState>,
        limit: usize,
    ) -> Result<Vec<RunObservation>, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let mut statement = connection.prepare(
            "SELECT observation_json FROM runs WHERE (?1 IS NULL OR job_id = ?1) AND (?2 IS NULL OR state = ?2) ORDER BY rowid DESC LIMIT ?3",
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map(
                params![
                    job_id.map(JobId::as_str),
                    state_filter.map(RunState::as_str),
                    limit as i64
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|_| RunStoreError::StorageUnavailable)?)
                .map_err(|_| RunStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn record_observation(&self, observation: &RunObservation) -> Result<(), RunStoreError> {
        let serialized =
            serde_json::to_string(observation).map_err(|_| RunStoreError::StorageUnavailable)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        connection.execute(
            "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
            params![serialized, observation.revision() as i64, observation.state().as_str(), observation.run_id().as_str()],
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        Ok(())
    }
}

fn now_ms() -> Result<i64, RepositoryOpenError> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryOpenError::TimestampUnavailable)?
        .as_millis();
    i64::try_from(milliseconds).map_err(|_| RepositoryOpenError::TimestampUnavailable)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagerError {
    DuplicateJob,
    JobLimitReached,
    JobNotFound,
    JobConflict,
    CapacityReached,
    RunNotFound,
    WorkerUnavailable,
    WorkerDraining,
    StorageUnavailable,
}

pub const MAX_RUNS_RESPONSE: usize = 100;

/// Manager application service for immutable job definitions.
pub struct JobService {
    repository: Arc<dyn JobRepository>,
}

impl JobService {
    pub fn new(repository: Arc<dyn JobRepository>) -> Self {
        Self { repository }
    }

    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self, RepositoryOpenError> {
        Ok(Self::new(Arc::new(SqliteJobRepository::open(path)?)))
    }

    pub fn register(&self, job: JobSpec) -> Result<(), ManagerError> {
        match self.repository.insert(job) {
            Ok(()) => Ok(()),
            Err(JobStoreError::DuplicateId) => Err(ManagerError::DuplicateJob),
            Err(JobStoreError::LimitExceeded) => Err(ManagerError::JobLimitReached),
            Err(JobStoreError::ActiveRunConflict) => Err(ManagerError::JobConflict),
            Err(JobStoreError::StorageUnavailable) => Err(ManagerError::StorageUnavailable),
        }
    }

    pub fn get(&self, id: &JobId) -> Result<Option<JobSpec>, ManagerError> {
        self.repository.get(id).map_err(map_store_error)
    }

    pub fn list(&self) -> Result<Vec<JobSpec>, ManagerError> {
        self.repository.list().map_err(map_store_error)
    }

    pub fn delete(&self, id: &JobId) -> Result<bool, ManagerError> {
        self.repository.delete(id).map_err(map_store_error)
    }

    pub fn start_run(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_capacity: usize,
        worker: &dyn WorkerPort,
    ) -> Result<RunObservation, ManagerError> {
        if worker.is_draining() {
            return Err(ManagerError::WorkerDraining);
        }
        let active = self
            .repository
            .list_active_runs(worker_id, worker_capacity)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        for stored in active {
            if let Ok(Some(newer)) = worker.inspect(InspectRun::new(stored.run_id().clone()))
                && newer.run_id() == stored.run_id()
                && newer.revision() > stored.revision()
            {
                self.repository
                    .record_observation(&newer)
                    .map_err(|_| ManagerError::StorageUnavailable)?;
            }
        }
        let spec = self
            .repository
            .create_run(job_id, worker_id, worker_capacity)
            .map_err(map_run_error)?;
        let observation = match worker.start(StartRun::new(spec.clone())) {
            Ok(observation) => observation,
            Err(WorkerPortError::CapacityReached) => {
                self.mark_dispatch_failure(&spec, RunFailureReason::CapacityRejected, true)?;
                return Err(ManagerError::CapacityReached);
            }
            Err(WorkerPortError::ShuttingDown) => {
                self.mark_dispatch_failure(&spec, RunFailureReason::BackendFailure, true)?;
                return Err(ManagerError::WorkerDraining);
            }
            Err(WorkerPortError::RunConflict | WorkerPortError::Unavailable) => {
                self.mark_dispatch_failure(&spec, RunFailureReason::OutcomeUnknown, false)?;
                return Err(ManagerError::WorkerUnavailable);
            }
        };
        if observation.run_id() != spec.id()
            || observation.job_id() != spec.job().id()
            || observation.worker_id() != spec.worker_id()
        {
            self.mark_dispatch_failure(&spec, RunFailureReason::OutcomeUnknown, false)?;
            return Err(ManagerError::WorkerUnavailable);
        }
        self.repository
            .record_observation(&observation)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        Ok(observation)
    }

    pub fn get_run(
        &self,
        id: &RunId,
        worker: &dyn WorkerPort,
    ) -> Result<Option<RunObservation>, ManagerError> {
        let Some(stored) = self
            .repository
            .get_run(id)
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Ok(None);
        };
        if let Ok(Some(observation)) = worker.inspect(InspectRun::new(id.clone()))
            && observation.run_id() == id
            && observation.job_id() == stored.job_id()
            && observation.worker_id() == stored.worker_id()
            && observation.revision() > stored.revision()
        {
            self.repository
                .record_observation(&observation)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            return Ok(Some(observation));
        }
        Ok(Some(stored))
    }

    /// Persist the stop decision before attempting delivery to the worker.
    pub fn stop_run(
        &self,
        id: &RunId,
        worker: &dyn WorkerPort,
    ) -> Result<RunObservation, ManagerError> {
        let Some(current) = self
            .repository
            .get_run(id)
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Err(ManagerError::RunNotFound);
        };
        if current.state().is_terminal() {
            return Ok(current);
        }
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
                .ok_or(ManagerError::StorageUnavailable)?
                .with_termination_reason(RunTerminationReason::Cancelled)
        };
        self.repository
            .record_observation(&stopping)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        if let Ok(observation) = worker.stop(StopRun::new(id.clone()))
            && observation.run_id() == id
            && observation.job_id() == stopping.job_id()
            && observation.worker_id() == stopping.worker_id()
        {
            let observation = if observation.state().is_terminal()
                && observation.revision() <= stopping.revision()
            {
                stopping
                    .next_revision(
                        observation.state(),
                        observation.started_at_ms(),
                        observation.finished_at_ms(),
                        observation.exit_code(),
                        observation.termination_signal(),
                        observation.failure_reason(),
                    )
                    .map(|value| {
                        value.with_optional_termination_reason(observation.termination_reason())
                    })
                    .ok_or(ManagerError::StorageUnavailable)?
            } else if observation.revision() > stopping.revision() {
                observation
            } else {
                return Ok(stopping);
            };
            self.repository
                .record_observation(&observation)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            return Ok(observation);
        }
        Ok(stopping)
    }

    fn mark_dispatch_failure(
        &self,
        spec: &RunSpec,
        reason: conductor_core::RunFailureReason,
        rejected: bool,
    ) -> Result<(), ManagerError> {
        let Some(current) = self
            .repository
            .get_run(spec.id())
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Err(ManagerError::StorageUnavailable);
        };
        let finished = now_ms().map_err(|_| ManagerError::StorageUnavailable)?;
        let next = current
            .next_revision(
                if rejected {
                    RunState::Failed
                } else {
                    RunState::Unknown
                },
                None,
                if rejected { Some(finished) } else { None },
                None,
                None,
                Some(reason),
            )
            .ok_or(ManagerError::StorageUnavailable)?;
        self.repository
            .record_observation(&next)
            .map_err(|_| ManagerError::StorageUnavailable)
    }

    pub fn list_runs(
        &self,
        job_id: Option<&JobId>,
        state: Option<RunState>,
        limit: usize,
    ) -> Result<Vec<RunObservation>, ManagerError> {
        self.repository
            .list_runs(job_id, state, limit.min(MAX_RUNS_RESPONSE))
            .map_err(|_| ManagerError::StorageUnavailable)
    }
}

fn map_store_error(error: JobStoreError) -> ManagerError {
    match error {
        JobStoreError::DuplicateId => ManagerError::DuplicateJob,
        JobStoreError::LimitExceeded => ManagerError::JobLimitReached,
        JobStoreError::ActiveRunConflict => ManagerError::JobConflict,
        JobStoreError::StorageUnavailable => ManagerError::StorageUnavailable,
    }
}

fn map_run_error(error: RunStoreError) -> ManagerError {
    match error {
        RunStoreError::JobNotFound => ManagerError::JobNotFound,
        RunStoreError::JobConflict => ManagerError::JobConflict,
        RunStoreError::CapacityReached => ManagerError::CapacityReached,
        RunStoreError::StorageUnavailable => ManagerError::StorageUnavailable,
    }
}
