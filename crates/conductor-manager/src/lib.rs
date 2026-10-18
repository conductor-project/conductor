//! Manager-owned application operations and durable job-definition storage.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, io};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use base64::Engine;
use conductor_core::{
    CommandEnvelope, CommandId, InspectRun, JobId, JobSpec, MAX_WORKER_CAPACITY, ReadRunLogs,
    ResolveRun, RunFailureReason, RunId, RunLogChunk, RunObservation, RunSpec, RunState,
    RunTerminationReason, StartRun, StopRun, WorkerCommand, WorkerDescriptor, WorkerId,
    WorkerStatus,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

/// Temporary limit for active jobs. Pagination is a later milestone.
pub const MAX_JOBS: usize = 1_000;
const SCHEMA_VERSION: i64 = 5;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_PENDING_COMMANDS_PER_WORKER: usize = 32;

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
    IdempotencyConflict,
    PendingDeliveryLimit,
    StorageUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandStoreError {
    NotFound,
    Conflict,
    PendingLimit,
    StorageUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryError {
    InvalidCursor,
    StorageUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerIdentityError {
    CredentialInvalid,
    IdentityExists,
    IdentityUnavailable,
    StorageUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

pub const DEFAULT_PAGE_SIZE: usize = 50;
pub const MAX_PAGE_SIZE: usize = 100;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunAdmission {
    pub spec: RunSpec,
    pub replay: Option<RunObservation>,
    pub command: Option<CommandEnvelope>,
}

impl std::ops::Deref for RunAdmission {
    type Target = RunSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerPortError {
    CapacityReached,
    RunConflict,
    ShuttingDown,
    LogsNotFound,
    LogsPruned,
    Unavailable,
}

/// Manager-owned seam for dispatch and observation reads from the selected worker.
pub trait WorkerPort: Send + Sync {
    fn descriptor(&self) -> WorkerDescriptor;
    fn is_draining(&self) -> bool {
        false
    }
    fn is_degraded(&self) -> bool {
        false
    }
    fn start(&self, command: StartRun) -> Result<RunObservation, WorkerPortError>;
    fn dispatch_command(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<RunObservation, WorkerPortError> {
        match envelope.command {
            WorkerCommand::StartRun(command) => self.start(*command),
            WorkerCommand::StopRun(command) => self.stop(command),
            WorkerCommand::ResolveRun(command) => self.resolve(command),
            WorkerCommand::InspectRun(command) => {
                self.inspect(command)?.ok_or(WorkerPortError::RunConflict)
            }
            WorkerCommand::ReadRunLogs(_) => Err(WorkerPortError::Unavailable),
        }
    }
    fn inspect(&self, command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError>;
    fn stop(&self, command: StopRun) -> Result<RunObservation, WorkerPortError>;
    fn resolve(&self, _command: ResolveRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }
    fn status(&self) -> Result<WorkerStatus, WorkerPortError> {
        let draining = self.is_draining();
        Ok(WorkerStatus::new(
            self.descriptor(),
            0,
            0,
            !draining,
            draining,
            0,
        ))
    }
    fn read_logs(&self, command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError>;
}

/// Durable manager command outbox used by transport adapters to restore and
/// acknowledge delivery without making network calls inside a database transaction.
pub trait CommandJournal: Send + Sync {
    fn pending_commands(
        &self,
        worker_id: &WorkerId,
        incarnation: u64,
        limit: usize,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError>;
    fn acknowledge_command(
        &self,
        command_id: &CommandId,
        worker_id: &WorkerId,
        incarnation: u64,
        acknowledgement_json: &str,
    ) -> Result<(), CommandStoreError>;
}

/// Storage port owned by the manager. Implementations preserve deleted IDs.
pub trait JobRepository: Send + Sync + CommandJournal {
    fn insert(&self, job: JobSpec) -> Result<(), JobStoreError>;
    fn get(&self, id: &JobId) -> Result<Option<JobSpec>, JobStoreError>;
    fn list_jobs_page(
        &self,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<JobSpec>, QueryError>;
    fn delete(&self, id: &JobId) -> Result<bool, JobStoreError>;
    fn create_run(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_capacity: usize,
        idempotency_key: Option<&str>,
    ) -> Result<RunAdmission, RunStoreError>;
    fn create_run_for_worker(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        worker_capacity: usize,
        idempotency_key: Option<&str>,
    ) -> Result<RunAdmission, RunStoreError> {
        let _ = worker_incarnation;
        self.create_run(job_id, worker_id, worker_capacity, idempotency_key)
    }
    fn command_for_run(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        command: WorkerCommand,
    ) -> Result<CommandEnvelope, CommandStoreError>;
    fn get_idempotent_run(
        &self,
        key: &str,
    ) -> Result<Option<(JobId, RunObservation)>, RunStoreError>;
    fn delete_unaccepted_run(&self, id: &RunId) -> Result<(), RunStoreError>;
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
    fn list_runs_page(
        &self,
        job_id: Option<&JobId>,
        state: Option<RunState>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<RunObservation>, QueryError>;
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
        connection
            .pragma_update(None, "foreign_keys", "ON")
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

    /// Persist a hashed, one-time enrollment credential. The raw credential is never stored.
    pub fn create_enrollment_credential(
        &self,
        token_hash: &[u8; 32],
        worker_id: &WorkerId,
        expires_at_ms: i64,
    ) -> Result<(), WorkerIdentityError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        connection
            .execute(
                "INSERT INTO enrollment_credentials(token_hash, worker_id, expires_at_ms) VALUES (?1, ?2, ?3)",
                params![token_hash.as_slice(), worker_id.as_str(), expires_at_ms],
            )
            .map(|_| ())
            .map_err(|_| WorkerIdentityError::StorageUnavailable)
    }

    pub fn worker_for_enrollment_credential(
        &self,
        token_hash: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<WorkerId>, WorkerIdentityError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let worker_id = connection
            .query_row(
                "SELECT worker_id FROM enrollment_credentials WHERE token_hash = ?1 AND consumed_at_ms IS NULL AND expires_at_ms > ?2",
                params![token_hash.as_slice(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        worker_id
            .map(WorkerId::new)
            .transpose()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)
    }

    pub fn worker_for_active_certificate(
        &self,
        fingerprint: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<WorkerId>, WorkerIdentityError> {
        self.authenticate_worker_certificate(fingerprint, now_ms)
    }

    /// Atomically consume a valid enrollment credential and install the first certificate.
    pub fn enroll_worker_identity(
        &self,
        token_hash: &[u8; 32],
        fingerprint: &[u8; 32],
        serial: &str,
        not_before_ms: i64,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<WorkerId, WorkerIdentityError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let credential = transaction
            .query_row(
                "SELECT worker_id FROM enrollment_credentials WHERE token_hash = ?1 AND consumed_at_ms IS NULL AND expires_at_ms > ?2",
                params![token_hash.as_slice(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?
            .ok_or(WorkerIdentityError::CredentialInvalid)?;
        let worker_id =
            WorkerId::new(credential).map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let existing = transaction
            .query_row(
                "SELECT 1 FROM worker_identities WHERE worker_id = ?1 AND revoked_at_ms IS NULL",
                [worker_id.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        if existing.is_some() {
            return Err(WorkerIdentityError::IdentityExists);
        }
        transaction
            .execute(
                "UPDATE enrollment_credentials SET consumed_at_ms = ?2 WHERE token_hash = ?1 AND consumed_at_ms IS NULL",
                params![token_hash.as_slice(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO worker_certificates(fingerprint, worker_id, serial, not_before_ms, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![fingerprint.as_slice(), worker_id.as_str(), serial, not_before_ms, expires_at_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO worker_identities(worker_id, active_fingerprint, created_at_ms) VALUES (?1, ?2, ?3) ON CONFLICT(worker_id) DO UPDATE SET active_fingerprint = excluded.active_fingerprint, created_at_ms = excluded.created_at_ms, revoked_at_ms = NULL",
                params![worker_id.as_str(), fingerprint.as_slice(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        Ok(worker_id)
    }

    /// Replace the active certificate only when the presenting certificate is current.
    pub fn renew_worker_identity(
        &self,
        current_fingerprint: &[u8; 32],
        replacement_fingerprint: &[u8; 32],
        serial: &str,
        not_before_ms: i64,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<WorkerId, WorkerIdentityError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let worker_id = transaction
            .query_row(
                "SELECT i.worker_id FROM worker_identities i JOIN worker_certificates c ON c.fingerprint = i.active_fingerprint WHERE i.active_fingerprint = ?1 AND i.revoked_at_ms IS NULL AND c.revoked_at_ms IS NULL AND c.expires_at_ms > ?2",
                params![current_fingerprint.as_slice(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?
            .ok_or(WorkerIdentityError::IdentityUnavailable)?;
        transaction
            .execute(
                "INSERT INTO worker_certificates(fingerprint, worker_id, serial, not_before_ms, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![replacement_fingerprint.as_slice(), worker_id, serial, not_before_ms, expires_at_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE worker_certificates SET revoked_at_ms = ?2 WHERE fingerprint = ?1",
                params![current_fingerprint.as_slice(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE worker_identities SET active_fingerprint = ?2 WHERE worker_id = ?1",
                params![worker_id, replacement_fingerprint.as_slice()],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        WorkerId::new(worker_id).map_err(|_| WorkerIdentityError::StorageUnavailable)
    }

    /// Revoke an identity and all of its certificates immediately.
    pub fn revoke_worker_identity(
        &self,
        worker_id: &WorkerId,
        now_ms: i64,
    ) -> Result<bool, WorkerIdentityError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let transaction = connection
            .unchecked_transaction()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE worker_identities SET revoked_at_ms = COALESCE(revoked_at_ms, ?2) WHERE worker_id = ?1",
                params![worker_id.as_str(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE worker_certificates SET revoked_at_ms = COALESCE(revoked_at_ms, ?2) WHERE worker_id = ?1",
                params![worker_id.as_str(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM enrollment_credentials WHERE worker_id = ?1 AND consumed_at_ms IS NULL",
                [worker_id.as_str()],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        Ok(changed != 0)
    }

    /// Resolve a presented certificate fingerprint only while it is active and unexpired.
    pub fn authenticate_worker_certificate(
        &self,
        fingerprint: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<WorkerId>, WorkerIdentityError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let worker_id = connection
            .query_row(
                "SELECT i.worker_id FROM worker_identities i JOIN worker_certificates c ON c.fingerprint = i.active_fingerprint WHERE c.fingerprint = ?1 AND c.expires_at_ms > ?2 AND c.revoked_at_ms IS NULL AND i.revoked_at_ms IS NULL",
                params![fingerprint.as_slice(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        worker_id
            .map(WorkerId::new)
            .transpose()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)
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
            .prepare("SELECT run_id, job_id, worker_id, job_spec_json, observation_json, revision, state, idempotency_key, request_fingerprint, run_order FROM runs LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT id, value FROM command_sequence LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT command_id, worker_id, worker_incarnation, run_id, envelope_json, state, acknowledgement_json FROM manager_commands LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT token_hash, worker_id, expires_at_ms, consumed_at_ms FROM enrollment_credentials LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT fingerprint, worker_id, serial, not_before_ms, expires_at_ms, revoked_at_ms FROM worker_certificates LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT worker_id, active_fingerprint, created_at_ms, revoked_at_ms FROM worker_identities LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        return Ok(());
    }

    // Schema 4 was published by two independently developed MVP2 branches:
    // #59 added worker identity tables, while #62 added the manager outbox.
    // Complete whichever half is missing before advancing to the unified v5.
    if version == 4 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        let command_sequence_exists =
            table_exists(&transaction, "command_sequence").map_err(MigrationError::Sqlite)?;
        let manager_commands_exists =
            table_exists(&transaction, "manager_commands").map_err(MigrationError::Sqlite)?;
        let enrollment_credentials_exists =
            table_exists(&transaction, "enrollment_credentials").map_err(MigrationError::Sqlite)?;
        let worker_certificates_exists =
            table_exists(&transaction, "worker_certificates").map_err(MigrationError::Sqlite)?;
        let worker_identities_exists =
            table_exists(&transaction, "worker_identities").map_err(MigrationError::Sqlite)?;
        let has_command_schema = command_sequence_exists && manager_commands_exists;
        let no_command_schema = !command_sequence_exists && !manager_commands_exists;
        let has_identity_schema =
            enrollment_credentials_exists && worker_certificates_exists && worker_identities_exists;
        let no_identity_schema = !enrollment_credentials_exists
            && !worker_certificates_exists
            && !worker_identities_exists;
        if !(has_command_schema || no_command_schema)
            || !(has_identity_schema || no_identity_schema)
        {
            return Err(MigrationError::Sqlite(rusqlite::Error::InvalidQuery));
        }
        if no_command_schema {
            create_command_schema(&transaction).map_err(MigrationError::Sqlite)?;
        }
        if no_identity_schema {
            create_identity_schema(&transaction).map_err(MigrationError::Sqlite)?;
        }
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 3 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_command_schema(&transaction).map_err(MigrationError::Sqlite)?;
        create_identity_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 2 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        transaction
            .execute_batch(
                "ALTER TABLE runs ADD COLUMN idempotency_key TEXT;
                 ALTER TABLE runs ADD COLUMN request_fingerprint TEXT;
                 ALTER TABLE runs ADD COLUMN run_order INTEGER NOT NULL DEFAULT 0;
                 UPDATE runs SET run_order = rowid;
                 CREATE UNIQUE INDEX runs_by_idempotency_key ON runs(idempotency_key) WHERE idempotency_key IS NOT NULL;",
            )
            .map_err(MigrationError::Sqlite)?;
        transaction
            .execute_batch(
                "CREATE UNIQUE INDEX runs_by_run_order ON runs(run_order);
                 CREATE INDEX runs_by_job_run_order ON runs(job_id, run_order DESC);
                 CREATE INDEX runs_by_state_run_order ON runs(state, run_order DESC);
                 CREATE INDEX runs_by_job_state_run_order ON runs(job_id, state, run_order DESC);
                 UPDATE run_sequence SET value = MAX(value, (SELECT COALESCE(MAX(run_order), 0) FROM runs));",
            )
            .map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", 3)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
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
            state TEXT NOT NULL,
            idempotency_key TEXT,
            request_fingerprint TEXT,
            run_order INTEGER NOT NULL
        );
        CREATE INDEX runs_by_state_worker ON runs(state, worker_id);
        CREATE INDEX runs_by_job_created ON runs(job_id, run_id);
        CREATE INDEX runs_by_run_order ON runs(run_order DESC);
        CREATE INDEX runs_by_job_run_order ON runs(job_id, run_order DESC);
        CREATE INDEX runs_by_state_run_order ON runs(state, run_order DESC);
        CREATE INDEX runs_by_job_state_run_order ON runs(job_id, state, run_order DESC);
        CREATE UNIQUE INDEX runs_by_idempotency_key ON runs(idempotency_key) WHERE idempotency_key IS NOT NULL;
        CREATE TABLE run_sequence (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL);
        INSERT INTO run_sequence (id, value) VALUES (1, 0);",
        )
        .map_err(MigrationError::Sqlite)?;
    create_command_schema(&transaction).map_err(MigrationError::Sqlite)?;
    create_identity_schema(&transaction).map_err(MigrationError::Sqlite)?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(MigrationError::Sqlite)?;
    transaction.commit().map_err(MigrationError::Sqlite)
}

fn create_command_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE command_sequence (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL CHECK (value >= 0));
         INSERT INTO command_sequence (id, value) VALUES (1, 0);
         CREATE TABLE manager_commands (
             command_id TEXT PRIMARY KEY NOT NULL,
             worker_id TEXT NOT NULL,
             worker_incarnation INTEGER NOT NULL CHECK (worker_incarnation >= 0),
             run_id TEXT NOT NULL,
             envelope_json TEXT NOT NULL,
             state TEXT NOT NULL CHECK (state IN ('pending', 'acknowledged')),
             acknowledgement_json TEXT,
             created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
             acknowledged_at_ms INTEGER CHECK (acknowledged_at_ms IS NULL OR acknowledged_at_ms >= 0),
             CHECK ((state = 'pending' AND acknowledgement_json IS NULL AND acknowledged_at_ms IS NULL)
                 OR (state = 'acknowledged' AND acknowledgement_json IS NOT NULL AND acknowledged_at_ms IS NOT NULL))
         );
         CREATE INDEX manager_commands_by_worker_state ON manager_commands(worker_id, worker_incarnation, state, command_id);
         CREATE INDEX manager_commands_by_run ON manager_commands(run_id, command_id);",
    )
}

fn create_identity_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE enrollment_credentials (
             token_hash BLOB PRIMARY KEY NOT NULL CHECK (length(token_hash) = 32),
             worker_id TEXT NOT NULL,
             expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms >= 0),
             consumed_at_ms INTEGER CHECK (consumed_at_ms IS NULL OR consumed_at_ms >= 0)
         );
         CREATE TABLE worker_certificates (
             fingerprint BLOB PRIMARY KEY NOT NULL CHECK (length(fingerprint) = 32),
             worker_id TEXT NOT NULL,
             serial TEXT NOT NULL UNIQUE,
             not_before_ms INTEGER NOT NULL CHECK (not_before_ms >= 0),
             expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms > not_before_ms),
             revoked_at_ms INTEGER CHECK (revoked_at_ms IS NULL OR revoked_at_ms >= 0)
         );
         CREATE INDEX worker_certificates_by_worker ON worker_certificates(worker_id);
         CREATE TABLE worker_identities (
             worker_id TEXT PRIMARY KEY NOT NULL,
             active_fingerprint BLOB NOT NULL UNIQUE REFERENCES worker_certificates(fingerprint),
             created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
             revoked_at_ms INTEGER CHECK (revoked_at_ms IS NULL OR revoked_at_ms >= 0)
         );",
    )
}

fn table_exists(connection: &Connection, name: &str) -> rusqlite::Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        [name],
        |row| row.get(0),
    )
}

fn next_command_id(transaction: &rusqlite::Transaction<'_>) -> Result<CommandId, RunStoreError> {
    let changed = transaction
        .execute(
            "UPDATE command_sequence SET value = value + 1 WHERE id = 1 AND value < ?1",
            [i64::MAX],
        )
        .map_err(|_| RunStoreError::StorageUnavailable)?;
    if changed != 1 {
        return Err(RunStoreError::StorageUnavailable);
    }
    let sequence: i64 = transaction
        .query_row(
            "SELECT value FROM command_sequence WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| RunStoreError::StorageUnavailable)?;
    CommandId::new(format!("command-{sequence}")).map_err(|_| RunStoreError::StorageUnavailable)
}

fn insert_command(
    transaction: &rusqlite::Transaction<'_>,
    envelope: &CommandEnvelope,
    run_id: &RunId,
    created_at_ms: i64,
) -> rusqlite::Result<()> {
    let envelope_json =
        serde_json::to_string(envelope).map_err(|_| rusqlite::Error::InvalidQuery)?;
    let worker_incarnation = i64::try_from(envelope.target_worker_incarnation)
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    transaction.execute(
        "INSERT INTO manager_commands (command_id, worker_id, worker_incarnation, run_id, envelope_json, state, acknowledgement_json, created_at_ms, acknowledged_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, 'pending', NULL, ?6, NULL)",
        params![
            envelope.command_id.as_str(),
            envelope.target_worker_id.as_str(),
            worker_incarnation,
            run_id.as_str(),
            envelope_json,
            created_at_ms,
        ],
    )?;
    Ok(())
}

fn table_columns(connection: &Connection) -> rusqlite::Result<Vec<String>> {
    let mut statement = connection.prepare("PRAGMA table_info(jobs)")?;
    statement.query_map([], |row| row.get(1))?.collect()
}

fn encode_cursor(payload: &str) -> String {
    let length = payload.len() / 3 * 4
        + match payload.len() % 3 {
            0 => 0,
            remainder => remainder + 1,
        };
    let mut encoded = vec![0; length];
    let Ok(written) =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode_slice(payload, &mut encoded)
    else {
        return String::new();
    };
    encoded.truncate(written);
    String::from_utf8(encoded).unwrap_or_default()
}

fn decode_cursor(cursor: &str) -> Result<String, QueryError> {
    if cursor.is_empty() || cursor.len() > 512 {
        return Err(QueryError::InvalidCursor);
    }
    let mut decoded = [0_u8; 384];
    let length = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode_slice(cursor, &mut decoded)
        .map_err(|_| QueryError::InvalidCursor)?;
    std::str::from_utf8(&decoded[..length])
        .map(str::to_owned)
        .map_err(|_| QueryError::InvalidCursor)
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
        if active_count >= MAX_JOBS as i64 {
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

    fn list_runs_page(
        &self,
        job_id: Option<&JobId>,
        state_filter: Option<RunState>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<RunObservation>, QueryError> {
        if !(1..=MAX_PAGE_SIZE).contains(&limit) {
            return Err(QueryError::InvalidCursor);
        }
        let filter_job = job_id.map(JobId::as_str).unwrap_or("-");
        let filter_state = state_filter.map(RunState::as_str).unwrap_or("-");
        let before_rowid = match cursor {
            Some(cursor) => {
                let payload = decode_cursor(cursor)?;
                let mut parts = payload.split('|');
                if parts.next() != Some("r")
                    || parts.next() != Some(filter_job)
                    || parts.next() != Some(filter_state)
                {
                    return Err(QueryError::InvalidCursor);
                }
                let run_order = parts
                    .next()
                    .ok_or(QueryError::InvalidCursor)?
                    .parse::<i64>()
                    .map_err(|_| QueryError::InvalidCursor)?;
                if parts.next().is_some() || run_order <= 0 {
                    return Err(QueryError::InvalidCursor);
                }
                Some(run_order)
            }
            None => None,
        };
        let connection = self
            .connection
            .lock()
            .map_err(|_| QueryError::StorageUnavailable)?;
        let mut query = String::from("SELECT run_order, observation_json FROM runs WHERE 1 = 1");
        let mut values = Vec::with_capacity(4);
        if let Some(job_id) = job_id {
            query.push_str(" AND job_id = ?");
            values.push(rusqlite::types::Value::Text(job_id.as_str().to_owned()));
        }
        if let Some(state_filter) = state_filter {
            query.push_str(" AND state = ?");
            values.push(rusqlite::types::Value::Text(
                state_filter.as_str().to_owned(),
            ));
        }
        if let Some(before_order) = before_rowid {
            query.push_str(" AND run_order < ?");
            values.push(rusqlite::types::Value::Integer(before_order));
        }
        query.push_str(" ORDER BY run_order DESC LIMIT ?");
        values.push(rusqlite::types::Value::Integer((limit + 1) as i64));
        let mut statement = connection
            .prepare(&query)
            .map_err(|_| QueryError::StorageUnavailable)?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(values), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| QueryError::StorageUnavailable)?;
        let rows = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| QueryError::StorageUnavailable)?;
        let has_more = rows.len() > limit;
        let page_rows = &rows[..rows.len().min(limit)];
        let items = page_rows
            .iter()
            .map(|(_, json)| serde_json::from_str(json).map_err(|_| QueryError::StorageUnavailable))
            .collect::<Result<Vec<RunObservation>, _>>()?;
        let next_cursor = if has_more {
            page_rows
                .last()
                .map(|(rowid, _)| encode_cursor(&format!("r|{filter_job}|{filter_state}|{rowid}")))
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    fn list_jobs_page(
        &self,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<JobSpec>, QueryError> {
        if !(1..=MAX_PAGE_SIZE).contains(&limit) {
            return Err(QueryError::InvalidCursor);
        }
        let after_id = match cursor {
            Some(cursor) => {
                let payload = decode_cursor(cursor)?;
                let Some(id) = payload.strip_prefix("j|") else {
                    return Err(QueryError::InvalidCursor);
                };
                JobId::new(id.to_owned()).map_err(|_| QueryError::InvalidCursor)?;
                Some(id.to_owned())
            }
            None => None,
        };
        let connection = self
            .connection
            .lock()
            .map_err(|_| QueryError::StorageUnavailable)?;
        let query = if after_id.is_some() {
            "SELECT id, spec_json FROM jobs WHERE deleted_at_ms IS NULL AND id > ?1 ORDER BY id COLLATE BINARY ASC LIMIT ?2"
        } else {
            "SELECT id, spec_json FROM jobs WHERE deleted_at_ms IS NULL ORDER BY id COLLATE BINARY ASC LIMIT ?1"
        };
        let mut values = Vec::with_capacity(2);
        if let Some(after_id) = after_id {
            values.push(rusqlite::types::Value::Text(after_id));
        }
        values.push(rusqlite::types::Value::Integer((limit + 1) as i64));
        let mut statement = connection
            .prepare(query)
            .map_err(|_| QueryError::StorageUnavailable)?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(values), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| QueryError::StorageUnavailable)?;
        let mut jobs = rows
            .map(|row| {
                let (_, json) = row.map_err(|_| QueryError::StorageUnavailable)?;
                serde_json::from_str(&json).map_err(|_| QueryError::StorageUnavailable)
            })
            .collect::<Result<Vec<JobSpec>, _>>()?;
        let has_more = jobs.len() > limit;
        jobs.truncate(limit);
        let next_cursor = if has_more {
            jobs.last()
                .map(|job| encode_cursor(&format!("j|{}", job.id())))
        } else {
            None
        };
        Ok(Page {
            items: jobs,
            next_cursor,
        })
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
        idempotency_key: Option<&str>,
    ) -> Result<RunAdmission, RunStoreError> {
        self.create_run_for_worker(job_id, worker_id, 0, worker_capacity, idempotency_key)
    }

    fn create_run_for_worker(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        worker_capacity: usize,
        idempotency_key: Option<&str>,
    ) -> Result<RunAdmission, RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if let Some(key) = idempotency_key {
            let existing: Option<(String, String, String, String, String)> = transaction
                .query_row(
                    "SELECT job_id, job_spec_json, observation_json, request_fingerprint, run_id FROM runs WHERE idempotency_key = ?1",
                    [key],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )
                .optional()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            if let Some((existing_job, spec_json, observation_json, fingerprint, existing_run_id)) =
                existing
            {
                let spec: RunSpec = serde_json::from_str(&spec_json)
                    .map_err(|_| RunStoreError::StorageUnavailable)?;
                let expected_fingerprint = serde_json::to_string(spec.job())
                    .map_err(|_| RunStoreError::StorageUnavailable)?;
                if existing_job != job_id.as_str() || fingerprint != expected_fingerprint {
                    return Err(RunStoreError::IdempotencyConflict);
                }
                let observation: RunObservation = serde_json::from_str(&observation_json)
                    .map_err(|_| RunStoreError::StorageUnavailable)?;
                let command = transaction
                    .query_row(
                        "SELECT envelope_json FROM manager_commands WHERE run_id = ?1 ORDER BY rowid LIMIT 1",
                        [existing_run_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|_| RunStoreError::StorageUnavailable)?
                    .map(|json| serde_json::from_str(&json).map_err(|_| RunStoreError::StorageUnavailable))
                    .transpose()?;
                return Ok(RunAdmission {
                    spec,
                    replay: Some(observation),
                    command,
                });
            }
        }
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
        let pending_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM manager_commands WHERE worker_id = ?1 AND state = 'pending'",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if pending_count >= MAX_PENDING_COMMANDS_PER_WORKER as i64 {
            return Err(RunStoreError::PendingDeliveryLimit);
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
        let command_id = next_command_id(&transaction)?;
        let envelope = CommandEnvelope {
            command_id,
            target_worker_id: worker_id.clone(),
            target_worker_incarnation: worker_incarnation,
            command: WorkerCommand::StartRun(Box::new(StartRun::new(spec.clone()))),
        };
        let observation = RunObservation::requested(
            &spec,
            now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
        );
        let spec_json =
            serde_json::to_string(&spec).map_err(|_| RunStoreError::StorageUnavailable)?;
        let fingerprint =
            serde_json::to_string(spec.job()).map_err(|_| RunStoreError::StorageUnavailable)?;
        let observation_json =
            serde_json::to_string(&observation).map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction.execute(
            "INSERT INTO runs (run_id, job_id, worker_id, job_spec_json, observation_json, revision, state, idempotency_key, request_fingerprint, run_order) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![spec.id().as_str(), job_id.as_str(), worker_id.as_str(), spec_json, observation_json, observation.revision() as i64, observation.state().as_str(), idempotency_key, idempotency_key.map(|_| fingerprint.as_str()), sequence],
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        insert_command(
            &transaction,
            &envelope,
            spec.id(),
            now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
        )
        .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        Ok(RunAdmission {
            spec,
            replay: None,
            command: Some(envelope),
        })
    }

    fn command_for_run(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        command: WorkerCommand,
    ) -> Result<CommandEnvelope, CommandStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let run_worker: Option<String> = transaction
            .query_row(
                "SELECT worker_id FROM runs WHERE run_id = ?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        if run_worker.as_deref() != Some(worker_id.as_str()) {
            return Err(CommandStoreError::NotFound);
        }
        let incarnation =
            i64::try_from(worker_incarnation).map_err(|_| CommandStoreError::Conflict)?;
        let previous: Option<String> = transaction
            .query_row(
                "SELECT envelope_json FROM manager_commands
                 WHERE run_id = ?1 AND worker_id = ?2 AND worker_incarnation = ?3
                 ORDER BY rowid DESC LIMIT 1",
                params![run_id.as_str(), worker_id.as_str(), incarnation],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        if let Some(json) = previous {
            let previous: CommandEnvelope =
                serde_json::from_str(&json).map_err(|_| CommandStoreError::StorageUnavailable)?;
            if previous.command == command {
                transaction
                    .commit()
                    .map_err(|_| CommandStoreError::StorageUnavailable)?;
                return Ok(previous);
            }
        }
        let pending_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM manager_commands WHERE worker_id = ?1 AND state = 'pending'",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        if pending_count >= MAX_PENDING_COMMANDS_PER_WORKER as i64 {
            return Err(CommandStoreError::PendingLimit);
        }
        let command_id =
            next_command_id(&transaction).map_err(|_| CommandStoreError::StorageUnavailable)?;
        let envelope = CommandEnvelope {
            command_id,
            target_worker_id: worker_id.clone(),
            target_worker_incarnation: worker_incarnation,
            command,
        };
        let created_at_ms = now_ms().map_err(|_| CommandStoreError::StorageUnavailable)?;
        insert_command(&transaction, &envelope, run_id, created_at_ms)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        Ok(envelope)
    }

    fn get_idempotent_run(
        &self,
        key: &str,
    ) -> Result<Option<(JobId, RunObservation)>, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let existing = connection
            .query_row(
                "SELECT job_id, observation_json FROM runs WHERE idempotency_key = ?1",
                [key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        existing
            .map(|(job_id, observation)| {
                let job_id = JobId::new(job_id).map_err(|_| RunStoreError::StorageUnavailable)?;
                let observation = serde_json::from_str(&observation)
                    .map_err(|_| RunStoreError::StorageUnavailable)?;
                Ok((job_id, observation))
            })
            .transpose()
    }

    fn delete_unaccepted_run(&self, id: &RunId) -> Result<(), RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM manager_commands WHERE run_id = ?1 AND state = 'pending'",
                [id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM runs WHERE run_id = ?1 AND state = 'requested'",
                [id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)
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

impl CommandJournal for SqliteJobRepository {
    fn pending_commands(
        &self,
        worker_id: &WorkerId,
        incarnation: u64,
        limit: usize,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError> {
        if !(1..=MAX_PENDING_COMMANDS_PER_WORKER).contains(&limit) {
            return Err(CommandStoreError::PendingLimit);
        }
        let incarnation =
            i64::try_from(incarnation).map_err(|_| CommandStoreError::StorageUnavailable)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT envelope_json FROM manager_commands
                 WHERE worker_id = ?1 AND worker_incarnation = ?2 AND state = 'pending'
                 ORDER BY rowid LIMIT ?3",
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map(
                params![worker_id.as_str(), incarnation, limit as i64],
                |row| row.get::<_, String>(0),
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        rows.map(|row| {
            let json = row.map_err(|_| CommandStoreError::StorageUnavailable)?;
            serde_json::from_str(&json).map_err(|_| CommandStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn acknowledge_command(
        &self,
        command_id: &CommandId,
        worker_id: &WorkerId,
        incarnation: u64,
        acknowledgement_json: &str,
    ) -> Result<(), CommandStoreError> {
        if acknowledgement_json.len() > 128 * 1024
            || serde_json::from_str::<serde_json::Value>(acknowledgement_json).is_err()
        {
            return Err(CommandStoreError::Conflict);
        }
        let incarnation = i64::try_from(incarnation).map_err(|_| CommandStoreError::Conflict)?;
        let acknowledged_at_ms = now_ms().map_err(|_| CommandStoreError::StorageUnavailable)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let record = transaction
            .query_row(
                "SELECT worker_id, worker_incarnation, state, acknowledgement_json
                 FROM manager_commands WHERE command_id = ?1",
                [command_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| CommandStoreError::StorageUnavailable)?
            .ok_or(CommandStoreError::NotFound)?;
        if record.0 != worker_id.as_str() || record.1 != incarnation {
            return Err(CommandStoreError::Conflict);
        }
        if record.2 == "acknowledged" {
            return if record.3.as_deref() == Some(acknowledgement_json) {
                transaction
                    .commit()
                    .map_err(|_| CommandStoreError::StorageUnavailable)?;
                Ok(())
            } else {
                Err(CommandStoreError::Conflict)
            };
        }
        transaction
            .execute(
                "UPDATE manager_commands
                 SET state = 'acknowledged', acknowledgement_json = ?1, acknowledged_at_ms = ?2
                 WHERE command_id = ?3 AND state = 'pending'",
                params![
                    acknowledgement_json,
                    acknowledged_at_ms,
                    command_id.as_str()
                ],
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| CommandStoreError::StorageUnavailable)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ManagerError {
    DuplicateJob,
    JobLimitReached,
    JobNotFound,
    JobConflict,
    CapacityReached,
    IdempotencyConflict,
    RunNotFound,
    WorkerUnavailable,
    WorkerDraining,
    ResolutionConflict,
    LogsNotFound,
    LogsPruned,
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

    /// Read bounded worker operational state through its owner port.
    pub fn worker_status(&self, worker: &dyn WorkerPort) -> Result<WorkerStatus, ManagerError> {
        worker.status().map_err(|_| ManagerError::WorkerUnavailable)
    }

    pub fn list_page(
        &self,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<JobSpec>, QueryError> {
        self.repository.list_jobs_page(limit, cursor)
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
        self.start_run_with_key(job_id, worker_id, worker_capacity, None, worker)
    }

    pub fn start_run_with_key(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_capacity: usize,
        idempotency_key: Option<&str>,
        worker: &dyn WorkerPort,
    ) -> Result<RunObservation, ManagerError> {
        if let Some(key) = idempotency_key
            && let Some(observation) = self.replay_run(job_id, key, worker)?
        {
            return Ok(observation);
        }
        if !worker.descriptor().supports_process_execution() {
            return Err(ManagerError::WorkerUnavailable);
        }
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
        let descriptor = worker.descriptor();
        let admission = self
            .repository
            .create_run_for_worker(
                job_id,
                worker_id,
                descriptor.incarnation(),
                worker_capacity,
                idempotency_key,
            )
            .map_err(map_run_error)?;
        if let Some(replay) = admission.replay {
            return self
                .get_run(replay.run_id(), worker)?
                .ok_or(ManagerError::StorageUnavailable);
        }
        let spec = admission.spec;
        let envelope = admission.command.ok_or(ManagerError::StorageUnavailable)?;
        let observation = match worker.dispatch_command(envelope.clone()) {
            Ok(observation) => observation,
            Err(WorkerPortError::CapacityReached) => {
                self.repository
                    .delete_unaccepted_run(spec.id())
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                return Err(ManagerError::CapacityReached);
            }
            Err(WorkerPortError::ShuttingDown) => {
                self.repository
                    .delete_unaccepted_run(spec.id())
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                return Err(ManagerError::WorkerDraining);
            }
            Err(
                WorkerPortError::RunConflict
                | WorkerPortError::Unavailable
                | WorkerPortError::LogsNotFound
                | WorkerPortError::LogsPruned,
            ) => {
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
        if !observation.state().is_terminal()
            && observation.worker_incarnation() != worker.descriptor().incarnation()
        {
            self.mark_dispatch_failure(&spec, RunFailureReason::OutcomeUnknown, false)?;
            return Err(ManagerError::WorkerUnavailable);
        }
        self.repository
            .record_observation(&observation)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        self.acknowledge_command(&envelope)?;
        Ok(observation)
    }

    pub fn replay_run(
        &self,
        job_id: &JobId,
        idempotency_key: &str,
        worker: &dyn WorkerPort,
    ) -> Result<Option<RunObservation>, ManagerError> {
        let Some((existing_job, observation)) = self
            .repository
            .get_idempotent_run(idempotency_key)
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Ok(None);
        };
        if existing_job != *job_id {
            return Err(ManagerError::IdempotencyConflict);
        }
        let descriptor = worker.descriptor();
        if descriptor.worker_id() == observation.worker_id() {
            let pending = self
                .repository
                .pending_commands(
                    observation.worker_id(),
                    descriptor.incarnation(),
                    MAX_PENDING_COMMANDS_PER_WORKER,
                )
                .map_err(|_| ManagerError::StorageUnavailable)?;
            let retry = pending.into_iter().find(|envelope| {
                matches!(
                    &envelope.command,
                    WorkerCommand::StartRun(command) if command.spec().id() == observation.run_id()
                )
            });
            if let Some(envelope) = retry {
                let next = worker
                    .dispatch_command(envelope.clone())
                    .map_err(|_| ManagerError::WorkerUnavailable)?;
                if next.run_id() != observation.run_id()
                    || next.job_id() != job_id
                    || next.worker_id() != observation.worker_id()
                    || (!next.state().is_terminal()
                        && next.worker_incarnation() != descriptor.incarnation())
                {
                    return Err(ManagerError::WorkerUnavailable);
                }
                self.repository
                    .record_observation(&next)
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                self.acknowledge_command(&envelope)?;
            }
        }
        self.get_run(observation.run_id(), worker)
    }

    fn acknowledge_command(&self, envelope: &CommandEnvelope) -> Result<(), ManagerError> {
        self.repository
            .acknowledge_command(
                &envelope.command_id,
                &envelope.target_worker_id,
                envelope.target_worker_incarnation,
                r#"{"accepted":true}"#,
            )
            .map_err(|_| ManagerError::StorageUnavailable)
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
            if !observation.state().is_terminal()
                && observation.worker_incarnation() != worker.descriptor().incarnation()
            {
                self.mark_unknown(&stored)?;
                return self
                    .repository
                    .get_run(id)
                    .map_err(|_| ManagerError::StorageUnavailable);
            }
            self.repository
                .record_observation(&observation)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            return Ok(Some(observation));
        }
        Ok(Some(stored))
    }

    /// Converge persisted manager reservations with the selected worker before reopening admission.
    pub fn reconcile(&self, worker: &dyn WorkerPort) -> Result<(), ManagerError> {
        let descriptor = worker.descriptor();
        let active = self
            .repository
            .list_active_runs(descriptor.worker_id(), MAX_WORKER_CAPACITY)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        for stored in active {
            match worker.inspect(InspectRun::new(stored.run_id().clone())) {
                Ok(Some(observed))
                    if observed.run_id() == stored.run_id()
                        && observed.job_id() == stored.job_id()
                        && observed.worker_id() == stored.worker_id()
                        && observed.revision() > stored.revision() =>
                {
                    if !observed.state().is_terminal()
                        && observed.worker_incarnation() != descriptor.incarnation()
                    {
                        self.mark_unknown(&stored)?;
                    } else {
                        self.repository
                            .record_observation(&observed)
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    self.mark_unknown(&stored)?;
                }
                Err(_) => return Err(ManagerError::WorkerUnavailable),
            }
        }
        Ok(())
    }

    fn mark_unknown(&self, stored: &RunObservation) -> Result<(), ManagerError> {
        let unknown = stored
            .next_revision(
                RunState::Unknown,
                stored.started_at_ms(),
                None,
                None,
                None,
                Some(RunFailureReason::OutcomeUnknown),
            )
            .ok_or(ManagerError::StorageUnavailable)?;
        self.repository
            .record_observation(&unknown)
            .map_err(|_| ManagerError::StorageUnavailable)
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
        let descriptor = worker.descriptor();
        let envelope = self
            .repository
            .command_for_run(
                id,
                stopping.worker_id(),
                descriptor.incarnation(),
                WorkerCommand::StopRun(StopRun::new(id.clone())),
            )
            .map_err(map_command_error)?;
        if let Ok(observation) = worker.dispatch_command(envelope.clone())
            && observation.run_id() == id
            && observation.job_id() == stopping.job_id()
            && observation.worker_id() == stopping.worker_id()
        {
            if !observation.state().is_terminal()
                && observation.worker_incarnation() != descriptor.incarnation()
            {
                return Ok(stopping);
            }
            self.acknowledge_command(&envelope)?;
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

    pub fn resolve_unknown_run(
        &self,
        id: &RunId,
        reason: &str,
        worker: &dyn WorkerPort,
    ) -> Result<RunObservation, ManagerError> {
        if reason.trim().is_empty() || reason.len() > 512 {
            return Err(ManagerError::ResolutionConflict);
        }
        let Some(current) = self
            .repository
            .get_run(id)
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Err(ManagerError::RunNotFound);
        };
        if current.state() == RunState::Abandoned {
            return if current.resolution_reason() == Some(reason) {
                Ok(current)
            } else {
                Err(ManagerError::ResolutionConflict)
            };
        }
        if current.state() != RunState::Unknown {
            return Err(ManagerError::ResolutionConflict);
        }
        let envelope = self
            .repository
            .command_for_run(
                id,
                current.worker_id(),
                worker.descriptor().incarnation(),
                WorkerCommand::ResolveRun(ResolveRun::new(id.clone(), reason.to_owned())),
            )
            .map_err(map_command_error)?;
        let resolved = match worker.dispatch_command(envelope.clone()) {
            Ok(resolved) => resolved,
            Err(WorkerPortError::RunConflict) => return Err(ManagerError::ResolutionConflict),
            Err(_) => return Err(ManagerError::WorkerUnavailable),
        };
        if resolved.run_id() != id
            || resolved.state() != RunState::Abandoned
            || resolved.resolution_reason() != Some(reason)
        {
            return Err(ManagerError::WorkerUnavailable);
        }
        self.acknowledge_command(&envelope)?;
        self.repository
            .record_observation(&resolved)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        Ok(resolved)
    }

    pub fn read_logs(
        &self,
        command: ReadRunLogs,
        worker: &dyn WorkerPort,
    ) -> Result<RunLogChunk, ManagerError> {
        match worker.read_logs(command) {
            Ok(chunk) => Ok(chunk),
            Err(WorkerPortError::LogsNotFound) => Err(ManagerError::LogsNotFound),
            Err(WorkerPortError::LogsPruned) => Err(ManagerError::LogsPruned),
            Err(
                WorkerPortError::Unavailable
                | WorkerPortError::ShuttingDown
                | WorkerPortError::CapacityReached
                | WorkerPortError::RunConflict,
            ) => Err(ManagerError::WorkerUnavailable),
        }
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

    pub fn list_runs_page(
        &self,
        job_id: Option<&JobId>,
        state: Option<RunState>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<RunObservation>, QueryError> {
        self.repository.list_runs_page(job_id, state, limit, cursor)
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
        RunStoreError::IdempotencyConflict => ManagerError::IdempotencyConflict,
        RunStoreError::PendingDeliveryLimit => ManagerError::WorkerUnavailable,
        RunStoreError::StorageUnavailable => ManagerError::StorageUnavailable,
    }
}

fn map_command_error(error: CommandStoreError) -> ManagerError {
    match error {
        CommandStoreError::NotFound => ManagerError::RunNotFound,
        CommandStoreError::PendingLimit => ManagerError::WorkerUnavailable,
        CommandStoreError::Conflict | CommandStoreError::StorageUnavailable => {
            ManagerError::StorageUnavailable
        }
    }
}
