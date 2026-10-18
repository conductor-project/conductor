//! Manager-owned application operations and durable job-definition storage.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{fs, io};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use base64::Engine;
use conductor_core::{
    Allocation, AllocationId, AllocationState, AssignedServicePort, CommandEnvelope, CommandId,
    DeploymentMutation, DeploymentPendingReason, DeploymentStatus, InspectRun, JobDeployment,
    JobId, JobRevision, JobRevisionDraft, JobRevisionId, JobSpec, MAX_DEPLOYMENT_REPLICAS,
    MAX_WORKER_CAPACITY, ReadRunLogs, ResolveRun, ResourceReservation, RunFailureReason, RunId,
    RunLogChunk, RunObservation, RunSpec, RunState, RunTerminationReason, ServiceInstance,
    ServiceInstanceId, StartRun, StopRun, WorkerCommand, WorkerDescriptor, WorkerId,
    WorkerInventoryEntry, WorkerResourceCapacity, WorkerServiceNetwork, WorkerStatus, Workload,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;

/// Temporary limit for active jobs. Pagination is a later milestone.
pub const MAX_JOBS: usize = 1_000;
const SCHEMA_VERSION: i64 = 9;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_PENDING_COMMANDS_PER_WORKER: usize = 32;
const MAX_PENDING_NON_CONTROL_COMMANDS_PER_WORKER: usize = 24;
const MAX_SERVICE_RESTARTS: u32 = 5;
const SERVICE_RESTART_BASE_BACKOFF_MS: u64 = 250;
const SERVICE_RESTART_MAX_BACKOFF_MS: u64 = 30_000;

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
    RunNotFound,
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
pub enum ExecutionStoreError {
    JobNotFound,
    JobLimitReached,
    RevisionNotFound,
    Conflict,
    PreconditionFailed,
    IdempotencyConflict,
    InvalidRequest,
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

/// Durable service-slot allocation and the shared host domain that owns its
/// reservation. Pending allocations have no worker or resource domain yet.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocationRecord {
    allocation: Allocation,
    resource_domain_id: Option<WorkerId>,
}

/// Persisted control state for one service instance's current or next execution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAttemptRecord {
    pub service_instance_id: ServiceInstanceId,
    pub replica_index: u32,
    pub current_allocation_id: Option<AllocationId>,
    pub allocation: Option<AllocationRecord>,
    pub current_run_id: Option<RunId>,
    pub current_run: Option<RunObservation>,
    pub restart_count: u32,
    pub next_start_at_ms: i64,
    pub terminal_failure: bool,
    pub service_ports: Vec<AssignedServicePort>,
    pub service_ports_ready: bool,
}

impl AllocationRecord {
    pub fn allocation(&self) -> &Allocation {
        &self.allocation
    }

    pub fn resource_domain_id(&self) -> Option<&WorkerId> {
        self.resource_domain_id.as_ref()
    }
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
    fn command_record(
        &self,
        command_id: &CommandId,
    ) -> Result<Option<CommandRecord>, CommandStoreError>;
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
    fn reconcile_worker_inventory_page(
        &self,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        entries: &[WorkerInventoryEntry],
    ) -> Result<Vec<RunId>, CommandStoreError>;
    fn finish_worker_inventory(
        &self,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        observed_run_ids: &[RunId],
    ) -> Result<(), CommandStoreError>;
}

/// Durable manager-side state for one worker command. The acknowledgement
/// payload is retained so a worker can safely retry a report after a manager
/// restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandRecord {
    pub envelope: CommandEnvelope,
    pub acknowledgement_json: Option<String>,
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
    fn create_revision(
        &self,
        draft: &JobRevisionDraft,
        idempotency_key: &str,
    ) -> Result<JobRevision, ExecutionStoreError>;
    fn get_revision(&self, id: &JobRevisionId) -> Result<Option<JobRevision>, ExecutionStoreError>;
    fn get_deployment(&self, id: &JobId) -> Result<Option<JobDeployment>, ExecutionStoreError>;
    fn mutate_deployment(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
    ) -> Result<JobDeployment, ExecutionStoreError>;
    fn mutate_deployment_with_workers(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
        workers: &[WorkerStatus],
    ) -> Result<JobDeployment, ExecutionStoreError>;
    fn reconcile_deployment_placement(
        &self,
        id: &JobId,
        workers: &[WorkerStatus],
    ) -> Result<Option<JobDeployment>, ExecutionStoreError>;
    fn list_allocations(&self, id: &JobId) -> Result<Vec<AllocationRecord>, ExecutionStoreError>;
    fn list_active_deployments(&self) -> Result<Vec<JobDeployment>, ExecutionStoreError>;
    fn list_service_attempts(
        &self,
        id: &JobId,
    ) -> Result<Vec<ServiceAttemptRecord>, ExecutionStoreError>;
    fn begin_service_run(
        &self,
        job_id: &JobId,
        allocation_id: &AllocationId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        worker_capacity: usize,
        service_network: Option<&WorkerServiceNetwork>,
    ) -> Result<Option<RunAdmission>, RunStoreError>;
    fn finish_service_attempt(
        &self,
        instance_id: &ServiceInstanceId,
        run_id: &RunId,
        restart_count: u32,
        next_start_at_ms: i64,
        terminal_failure: bool,
    ) -> Result<(), RunStoreError>;
    fn release_service_allocation(
        &self,
        allocation_id: &AllocationId,
    ) -> Result<(), ExecutionStoreError>;
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
    /// Atomically persist a stop intent and its durable command. A terminal run
    /// is returned without creating another command.
    fn persist_stop_intent(
        &self,
        run_id: &RunId,
        worker_incarnation: u64,
    ) -> Result<(RunObservation, Option<CommandEnvelope>), RunStoreError>;
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
        connection
            .prepare(
                "SELECT job_id, revision, revision_json, created_at_ms FROM job_revisions LIMIT 0",
            )
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT job_id, generation, deployment_json, deleted_at_ms FROM job_deployments LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT operation, scope_key, idempotency_key, request_json, response_json FROM execution_idempotency LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT job_id, replica_index, instance_id, current_allocation_id FROM service_instances LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT allocation_id, job_id, revision, service_instance_id, worker_id, resource_domain_id, state, allocation_json, created_at_ms FROM allocations LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT id, value FROM scheduling_sequence LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT service_instance_id, allocation_id, current_run_id, restart_count, next_start_at_ms, terminal_failure FROM service_attempts LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT network_domain_id, protocol, port, allocation_id, run_id FROM service_port_leases LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        return Ok(());
    }

    if version == 8 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_service_port_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 7 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_service_attempt_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", 8)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 6 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_scheduling_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", 7)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 5 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_execution_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", 6)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    // Schema 4 was published by two independently developed MVP2 branches:
    // #59 added worker identity tables, while #62 added the manager outbox.
    // Complete whichever half is missing before advancing to v5; v6 adds the
    // revision and deployment tables after that normalized layout is present.
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
            .pragma_update(None, "user_version", 5)
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
            .pragma_update(None, "user_version", 5)
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
    create_execution_schema(&transaction).map_err(MigrationError::Sqlite)?;
    create_scheduling_schema(&transaction).map_err(MigrationError::Sqlite)?;
    create_service_attempt_schema(&transaction).map_err(MigrationError::Sqlite)?;
    create_service_port_schema(&transaction).map_err(MigrationError::Sqlite)?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(MigrationError::Sqlite)?;
    transaction.commit().map_err(MigrationError::Sqlite)
}

fn create_service_port_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS service_port_leases (
             network_domain_id TEXT NOT NULL,
             protocol TEXT NOT NULL CHECK (protocol = 'tcp'),
             port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
             allocation_id TEXT NOT NULL,
             run_id TEXT NOT NULL,
             name TEXT NOT NULL,
             advertised_address TEXT NOT NULL,
             PRIMARY KEY (network_domain_id, protocol, port),
             UNIQUE (run_id, name)
         );
         CREATE INDEX IF NOT EXISTS service_port_leases_by_allocation ON service_port_leases(allocation_id);
         CREATE INDEX IF NOT EXISTS service_port_leases_by_run ON service_port_leases(run_id);",
    )
}

fn lease_service_ports(
    transaction: &rusqlite::Transaction<'_>,
    allocation_id: &AllocationId,
    run_id: &RunId,
    sequence: i64,
    network: &WorkerServiceNetwork,
    declared: &[conductor_core::ServicePort],
) -> Result<Vec<AssignedServicePort>, RunStoreError> {
    if declared.is_empty() {
        return Ok(Vec::new());
    }
    let start = usize::from(network.port_range_start());
    let end = usize::from(network.port_range_end());
    let range_size = end - start + 1;
    let mut occupied = {
        let mut statement = transaction
            .prepare(
                "SELECT port FROM service_port_leases
                 WHERE network_domain_id = ?1 AND protocol = 'tcp' AND port BETWEEN ?2 AND ?3",
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map(
                params![
                    network.network_domain_id().as_str(),
                    start as i64,
                    end as i64
                ],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        rows.map(|row| {
            u16::try_from(row.map_err(|_| RunStoreError::StorageUnavailable)?)
                .map_err(|_| RunStoreError::StorageUnavailable)
        })
        .collect::<Result<BTreeSet<_>, _>>()?
    };
    let mut assigned = Vec::with_capacity(declared.len());
    for (index, declared_port) in declared.iter().enumerate() {
        let offset = usize::try_from(sequence).unwrap_or(0).wrapping_add(index) % range_size;
        let lease = (0..range_size)
            .map(|step| (start + (offset + step) % range_size) as u16)
            .find(|port| !occupied.contains(port));
        let Some(port) = lease else {
            transaction
                .execute(
                    "DELETE FROM service_port_leases WHERE run_id = ?1",
                    [run_id.as_str()],
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(Vec::new());
        };
        transaction
            .execute(
                "INSERT INTO service_port_leases
                 (network_domain_id, protocol, port, allocation_id, run_id, name, advertised_address)
                 VALUES (?1, 'tcp', ?2, ?3, ?4, ?5, ?6)",
                params![
                    network.network_domain_id().as_str(),
                    i64::from(port),
                    allocation_id.as_str(),
                    run_id.as_str(),
                    declared_port.name(),
                    network.advertised_address(),
                ],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        occupied.insert(port);
        assigned.push(
            AssignedServicePort::new(
                declared_port.name(),
                declared_port.protocol(),
                port,
                network.advertised_address(),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?,
        );
    }
    Ok(assigned)
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

fn create_execution_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE job_revisions (
             job_id TEXT NOT NULL,
             revision INTEGER NOT NULL CHECK (revision > 0),
             revision_json TEXT NOT NULL,
             created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
             PRIMARY KEY (job_id, revision),
             FOREIGN KEY (job_id) REFERENCES jobs(id)
         );
         CREATE TABLE job_deployments (
             job_id TEXT PRIMARY KEY NOT NULL,
             generation INTEGER NOT NULL CHECK (generation > 0),
             deployment_json TEXT NOT NULL,
             deleted_at_ms INTEGER CHECK (deleted_at_ms IS NULL OR deleted_at_ms >= 0),
             FOREIGN KEY (job_id) REFERENCES jobs(id)
         );
         CREATE TABLE execution_idempotency (
             operation TEXT NOT NULL,
             scope_key TEXT NOT NULL,
             idempotency_key TEXT NOT NULL,
             request_json TEXT NOT NULL,
             response_json TEXT NOT NULL,
             created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
             PRIMARY KEY (operation, scope_key, idempotency_key)
         );",
    )
}

fn create_scheduling_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE scheduling_sequence (
             id INTEGER PRIMARY KEY CHECK (id = 1),
             value INTEGER NOT NULL CHECK (value >= 0)
         );
         INSERT INTO scheduling_sequence (id, value) VALUES (1, 0);
         CREATE TABLE service_instances (
             job_id TEXT NOT NULL REFERENCES jobs(id),
             replica_index INTEGER NOT NULL CHECK (replica_index >= 0),
             instance_id TEXT NOT NULL UNIQUE,
             current_allocation_id TEXT,
             PRIMARY KEY (job_id, replica_index)
         );
         CREATE TABLE allocations (
             allocation_id TEXT PRIMARY KEY NOT NULL,
             job_id TEXT NOT NULL,
             revision INTEGER NOT NULL CHECK (revision > 0),
             service_instance_id TEXT NOT NULL REFERENCES service_instances(instance_id),
             worker_id TEXT,
             resource_domain_id TEXT,
             state TEXT NOT NULL CHECK (state IN ('pending', 'assigned', 'uncertain', 'released')),
             allocation_json TEXT NOT NULL,
             created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
             FOREIGN KEY (job_id, revision) REFERENCES job_revisions(job_id, revision),
             CHECK ((state = 'pending' AND worker_id IS NULL AND resource_domain_id IS NULL)
                 OR (state IN ('assigned', 'uncertain') AND worker_id IS NOT NULL AND resource_domain_id IS NOT NULL)
                 OR state = 'released')
         );
         CREATE INDEX allocations_by_worker_state ON allocations(worker_id, state);
         CREATE INDEX allocations_by_domain_state ON allocations(resource_domain_id, state);
         CREATE INDEX allocations_by_job_instance ON allocations(job_id, service_instance_id, created_at_ms);
         CREATE INDEX allocations_by_job_state ON allocations(job_id, state);
         CREATE UNIQUE INDEX service_instances_current_allocation
             ON service_instances(current_allocation_id) WHERE current_allocation_id IS NOT NULL;",
    )
}

fn create_service_attempt_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS service_attempts (
             service_instance_id TEXT PRIMARY KEY NOT NULL REFERENCES service_instances(instance_id),
             allocation_id TEXT,
             current_run_id TEXT UNIQUE,
             restart_count INTEGER NOT NULL DEFAULT 0 CHECK (restart_count >= 0),
             next_start_at_ms INTEGER NOT NULL DEFAULT 0 CHECK (next_start_at_ms >= 0),
             terminal_failure INTEGER NOT NULL DEFAULT 0 CHECK (terminal_failure IN (0, 1))
         );",
    )
}

fn active_allocations_exist(
    transaction: &rusqlite::Transaction<'_>,
    job_id: &JobId,
) -> Result<bool, ExecutionStoreError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM allocations WHERE job_id = ?1 AND state IN ('assigned', 'uncertain')
             )",
            [job_id.as_str()],
            |row| row.get(0),
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)
}

#[derive(Clone, Copy, Default)]
struct ReservedUsage {
    cpu_millis: u64,
    memory_bytes: u64,
    slots: usize,
}

impl ReservedUsage {
    fn add(&mut self, reservation: &ResourceReservation) {
        self.cpu_millis = self
            .cpu_millis
            .saturating_add(u64::from(reservation.cpu_millis().unwrap_or_default()));
        self.memory_bytes = self
            .memory_bytes
            .saturating_add(reservation.memory_bytes().unwrap_or_default());
        self.slots = self.slots.saturating_add(1);
    }

    fn fits(self, request: &ResourceReservation, capacity: WorkerResourceCapacity) -> bool {
        self.cpu_millis
            .checked_add(u64::from(request.cpu_millis().unwrap_or_default()))
            .is_some_and(|used| used <= u64::from(capacity.cpu_millis()))
            && self
                .memory_bytes
                .checked_add(request.memory_bytes().unwrap_or_default())
                .is_some_and(|used| used <= capacity.memory_bytes())
    }
}

fn place_service_slots(
    transaction: &rusqlite::Transaction<'_>,
    revision: &JobRevision,
    deployment: &JobDeployment,
    workers: &[WorkerStatus],
) -> Result<JobDeployment, ExecutionStoreError> {
    let job_id = revision.id().job_id();
    let desired_replicas = if deployment.suspended() {
        0
    } else {
        deployment.desired_replicas()
    };
    let mut usage_by_worker = BTreeMap::<WorkerId, ReservedUsage>::new();
    let mut usage_by_domain = BTreeMap::<WorkerId, ReservedUsage>::new();
    let mut connection = transaction
        .prepare(
            "SELECT allocation_json, resource_domain_id FROM allocations
             WHERE state IN ('assigned', 'uncertain')",
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let active_rows = connection
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    drop(connection);
    for (allocation_json, raw_domain_id) in active_rows {
        let allocation: Allocation = serde_json::from_str(&allocation_json)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let worker_id = allocation
            .assigned_worker_id()
            .ok_or(ExecutionStoreError::StorageUnavailable)?
            .clone();
        let domain_id = raw_domain_id
            .map(WorkerId::new)
            .transpose()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            .ok_or(ExecutionStoreError::StorageUnavailable)?;
        usage_by_worker
            .entry(worker_id)
            .or_default()
            .add(allocation.reservation());
        usage_by_domain
            .entry(domain_id)
            .or_default()
            .add(allocation.reservation());
    }

    let mut first_pending_reason = None;
    for replica_index in 0..desired_replicas {
        let instance_id = ensure_service_instance(transaction, job_id, replica_index)?;
        let current_allocation_id = transaction
            .query_row(
                "SELECT current_allocation_id FROM service_instances
                 WHERE job_id = ?1 AND replica_index = ?2",
                params![job_id.as_str(), i64::from(replica_index)],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            .flatten();
        let mut allocation = current_allocation_id
            .as_deref()
            .map(|allocation_id| load_allocation(transaction, allocation_id))
            .transpose()?
            .flatten();

        let mut needs_replacement = allocation.as_ref().is_none_or(|record| {
            record.allocation.job_revision_id() != revision.id()
                || record.allocation.state() == AllocationState::Released
        });
        if let Some(record) = allocation.as_mut()
            && record.allocation.state() == AllocationState::Assigned
        {
            let worker_is_fresh = record
                .allocation
                .assigned_worker_id()
                .and_then(|id| workers.iter().find(|worker| worker.worker_id() == id))
                .is_some_and(|worker| worker.is_valid() && worker.fresh() && !worker.degraded());
            if !worker_is_fresh {
                record
                    .allocation
                    .mark_uncertain()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                persist_allocation(transaction, record)?;
                needs_replacement = true;
            }
        }
        if allocation
            .as_ref()
            .is_some_and(|record| record.allocation.state() == AllocationState::Uncertain)
        {
            needs_replacement = true;
        }

        if needs_replacement {
            if let Some(record) = allocation.as_mut()
                && record.allocation.state() == AllocationState::Pending
            {
                record
                    .allocation
                    .release()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                persist_allocation(transaction, record)?;
            }
            let pending =
                new_pending_allocation(transaction, revision, &instance_id, replica_index)?;
            persist_allocation(transaction, &pending)?;
            set_current_allocation(transaction, job_id, replica_index, pending.allocation.id())?;
            allocation = Some(pending);
        }

        if let Some(mut record) = allocation
            && record.allocation.state() == AllocationState::Pending
        {
            match choose_worker(
                revision,
                &record.allocation,
                workers,
                &usage_by_worker,
                &usage_by_domain,
            ) {
                Ok((worker_id, domain_id)) => {
                    record
                        .allocation
                        .assign(worker_id.clone())
                        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                    record.resource_domain_id = Some(domain_id.clone());
                    persist_allocation(transaction, &record)?;
                    usage_by_worker
                        .entry(worker_id)
                        .or_default()
                        .add(record.allocation.reservation());
                    usage_by_domain
                        .entry(domain_id)
                        .or_default()
                        .add(record.allocation.reservation());
                }
                Err(reason) => {
                    first_pending_reason.get_or_insert(reason);
                }
            }
        }
    }

    // Slots above the desired replica count retain stable identity. Pending
    // allocations never reached a worker and can be released immediately;
    // assigned or uncertain reservations remain held for explicit cleanup.
    let mut statement = transaction
        .prepare(
            "SELECT replica_index, instance_id, current_allocation_id FROM service_instances
             WHERE job_id = ?1 AND replica_index >= ?2",
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let retired = statement
        .query_map(
            params![job_id.as_str(), i64::from(desired_replicas)],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            },
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    drop(statement);
    for (replica_index, _, current_allocation_id) in retired {
        if let Some(allocation_id) = current_allocation_id {
            if let Some(mut record) = load_allocation(transaction, &allocation_id)?
                && record.allocation.state() == AllocationState::Pending
            {
                record
                    .allocation
                    .release()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                persist_allocation(transaction, &record)?;
            }
            transaction
                .execute(
                    "UPDATE service_instances SET current_allocation_id = NULL
                     WHERE job_id = ?1 AND replica_index = ?2",
                    params![job_id.as_str(), replica_index],
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        }
    }

    let (status, pending_reason, pending_message) = if deployment.suspended() {
        (DeploymentStatus::Suspended, None, None)
    } else if desired_replicas == 0 || first_pending_reason.is_none() {
        (DeploymentStatus::Ready, None, None)
    } else {
        let reason = first_pending_reason.ok_or(ExecutionStoreError::StorageUnavailable)?;
        (
            DeploymentStatus::Pending,
            Some(reason),
            Some(pending_message(reason).to_owned()),
        )
    };
    JobDeployment::new(
        revision,
        deployment.generation(),
        deployment.desired_replicas(),
        deployment.suspended(),
        status,
        pending_reason,
        pending_message,
    )
    .map_err(|_| ExecutionStoreError::InvalidRequest)
}

fn ensure_service_instance(
    transaction: &rusqlite::Transaction<'_>,
    job_id: &JobId,
    replica_index: u32,
) -> Result<ServiceInstanceId, ExecutionStoreError> {
    let existing = transaction
        .query_row(
            "SELECT instance_id FROM service_instances
             WHERE job_id = ?1 AND replica_index = ?2",
            params![job_id.as_str(), i64::from(replica_index)],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    if let Some(existing) = existing {
        return ServiceInstanceId::new(existing)
            .map_err(|_| ExecutionStoreError::StorageUnavailable);
    }
    let instance_id = ServiceInstanceId::new(next_scheduling_id(transaction, "instance")?)
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    transaction
        .execute(
            "INSERT INTO service_instances (job_id, replica_index, instance_id, current_allocation_id)
             VALUES (?1, ?2, ?3, NULL)",
            params![job_id.as_str(), i64::from(replica_index), instance_id.as_str()],
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    Ok(instance_id)
}

fn next_scheduling_id(
    transaction: &rusqlite::Transaction<'_>,
    prefix: &str,
) -> Result<String, ExecutionStoreError> {
    let changed = transaction
        .execute(
            "UPDATE scheduling_sequence SET value = value + 1 WHERE id = 1 AND value < ?1",
            [i64::MAX],
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    if changed != 1 {
        return Err(ExecutionStoreError::StorageUnavailable);
    }
    let sequence = transaction
        .query_row(
            "SELECT value FROM scheduling_sequence WHERE id = 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    Ok(format!("{prefix}-{sequence}"))
}

fn new_pending_allocation(
    transaction: &rusqlite::Transaction<'_>,
    revision: &JobRevision,
    instance_id: &ServiceInstanceId,
    replica_index: u32,
) -> Result<AllocationRecord, ExecutionStoreError> {
    let instance = ServiceInstance::new(instance_id.clone(), revision, replica_index)
        .map_err(|_| ExecutionStoreError::InvalidRequest)?;
    let request = revision.resource_request();
    let reservation = ResourceReservation::new(request.cpu_millis(), request.memory_bytes())
        .ok_or(ExecutionStoreError::InvalidRequest)?;
    let allocation_id = AllocationId::new(next_scheduling_id(transaction, "allocation")?)
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let allocation = Allocation::new(allocation_id, revision, Some(&instance), reservation)
        .map_err(|_| ExecutionStoreError::InvalidRequest)?;
    Ok(AllocationRecord {
        allocation,
        resource_domain_id: None,
    })
}

fn set_current_allocation(
    transaction: &rusqlite::Transaction<'_>,
    job_id: &JobId,
    replica_index: u32,
    allocation_id: &AllocationId,
) -> Result<(), ExecutionStoreError> {
    transaction
        .execute(
            "UPDATE service_instances SET current_allocation_id = ?1
             WHERE job_id = ?2 AND replica_index = ?3",
            params![
                allocation_id.as_str(),
                job_id.as_str(),
                i64::from(replica_index)
            ],
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    Ok(())
}

fn load_allocation(
    transaction: &rusqlite::Transaction<'_>,
    allocation_id: &str,
) -> Result<Option<AllocationRecord>, ExecutionStoreError> {
    transaction
        .query_row(
            "SELECT allocation_json, resource_domain_id FROM allocations WHERE allocation_id = ?1",
            [allocation_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?
        .map(|(allocation_json, raw_domain_id)| {
            let allocation = serde_json::from_str(&allocation_json)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let resource_domain_id = raw_domain_id
                .map(WorkerId::new)
                .transpose()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            Ok(AllocationRecord {
                allocation,
                resource_domain_id,
            })
        })
        .transpose()
}

fn persist_allocation(
    transaction: &rusqlite::Transaction<'_>,
    record: &AllocationRecord,
) -> Result<(), ExecutionStoreError> {
    let allocation = &record.allocation;
    let worker_id = allocation.assigned_worker_id().map(WorkerId::as_str);
    let state = match allocation.state() {
        AllocationState::Pending => "pending",
        AllocationState::Assigned => "assigned",
        AllocationState::Uncertain => "uncertain",
        AllocationState::Released => "released",
    };
    let resource_domain_id = record.resource_domain_id.as_ref().map(WorkerId::as_str);
    let owner_fields_invalid = match allocation.state() {
        AllocationState::Pending => worker_id.is_some() || resource_domain_id.is_some(),
        AllocationState::Assigned | AllocationState::Uncertain => {
            worker_id.is_none() || resource_domain_id.is_none()
        }
        AllocationState::Released => false,
    };
    if owner_fields_invalid {
        return Err(ExecutionStoreError::StorageUnavailable);
    }
    let revision = i64::try_from(allocation.job_revision_id().revision())
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let service_instance_id = allocation
        .service_instance_id()
        .ok_or(ExecutionStoreError::StorageUnavailable)?;
    let allocation_json =
        serde_json::to_string(allocation).map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let created_at_ms = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    transaction
        .execute(
            "INSERT INTO allocations
             (allocation_id, job_id, revision, service_instance_id, worker_id, resource_domain_id, state, allocation_json, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(allocation_id) DO UPDATE SET worker_id = excluded.worker_id,
                 resource_domain_id = excluded.resource_domain_id, state = excluded.state,
                 allocation_json = excluded.allocation_json",
            params![
                allocation.id().as_str(),
                allocation.job_revision_id().job_id().as_str(),
                revision,
                service_instance_id.as_str(),
                worker_id,
                resource_domain_id,
                state,
                allocation_json,
                created_at_ms
            ],
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    Ok(())
}

fn choose_worker(
    revision: &JobRevision,
    allocation: &Allocation,
    workers: &[WorkerStatus],
    usage_by_worker: &BTreeMap<WorkerId, ReservedUsage>,
    usage_by_domain: &BTreeMap<WorkerId, ReservedUsage>,
) -> Result<(WorkerId, WorkerId), DeploymentPendingReason> {
    let fresh = workers
        .iter()
        .filter(|worker| worker.is_valid() && worker.fresh())
        .collect::<Vec<_>>();
    if fresh.is_empty() {
        return Err(DeploymentPendingReason::WorkerDisconnected);
    }
    let available = fresh
        .iter()
        .copied()
        .filter(|worker| worker.available() && !worker.degraded() && !worker.draining())
        .collect::<Vec<_>>();
    if available.is_empty() {
        return Err(DeploymentPendingReason::WorkerDisconnected);
    }
    let requirements = revision.execution_requirements();
    let platform_matches = available
        .into_iter()
        .filter(|worker| {
            let Some(inventory) = worker.inventory() else {
                return false;
            };
            requirements
                .operating_system()
                .is_none_or(|required| required == inventory.operating_system())
                && requirements
                    .architecture()
                    .is_none_or(|required| required == inventory.architecture())
                && requirements
                    .backend()
                    .is_none_or(|required| inventory.backends().contains(&required))
        })
        .collect::<Vec<_>>();
    if platform_matches.is_empty() {
        return Err(DeploymentPendingReason::NoEligibleWorker);
    }
    let enforcement_matches = platform_matches
        .into_iter()
        .filter(|worker| {
            worker.inventory().is_some_and(|inventory| {
                requirements
                    .required_enforcement()
                    .is_subset(inventory.enforcement())
            })
        })
        .collect::<Vec<_>>();
    if enforcement_matches.is_empty() {
        return Err(DeploymentPendingReason::RequiredProtectionUnavailable);
    }
    let label_matches = enforcement_matches
        .into_iter()
        .filter(|worker| requirements.labels_are_satisfied_by(worker.labels()))
        .collect::<Vec<_>>();
    if label_matches.is_empty() {
        return Err(DeploymentPendingReason::NoEligibleWorker);
    }
    let label_matches = if matches!(revision.workload(), Workload::Service { ports, .. } if !ports.is_empty())
    {
        label_matches
            .into_iter()
            .filter(|worker| worker.service_network().is_some())
            .collect::<Vec<_>>()
    } else {
        label_matches
    };
    if label_matches.is_empty() {
        return Err(DeploymentPendingReason::NoEligibleWorker);
    }
    let request = allocation.reservation();
    let mut worker_capacity_match = false;
    let mut shared_capacity_match = false;
    let mut candidates = Vec::new();
    let mut domain_capacities = BTreeMap::<WorkerId, WorkerResourceCapacity>::new();
    let mut inconsistent_domains = BTreeSet::<WorkerId>::new();
    for worker in &label_matches {
        let (Some(domain_id), Some(domain_capacity)) = (
            worker.resource_domain_id(),
            worker.resource_domain_capacity(),
        ) else {
            continue;
        };
        if domain_capacities
            .insert(domain_id.clone(), domain_capacity)
            .is_some_and(|previous| previous != domain_capacity)
        {
            inconsistent_domains.insert(domain_id.clone());
        }
    }
    for worker in label_matches {
        let (Some(inventory), Some(domain_id), Some(domain_capacity)) = (
            worker.inventory(),
            worker.resource_domain_id(),
            worker.resource_domain_capacity(),
        ) else {
            continue;
        };
        if inconsistent_domains.contains(domain_id) {
            continue;
        }
        let worker_usage = usage_by_worker
            .get(worker.worker_id())
            .copied()
            .unwrap_or_default();
        let occupied_slots = worker_usage.slots.max(worker.used_slots());
        if occupied_slots >= worker.capacity()
            || !worker_usage.fits(request, inventory.allocatable())
        {
            continue;
        }
        worker_capacity_match = true;
        let domain_usage = usage_by_domain.get(domain_id).copied().unwrap_or_default();
        if !domain_usage.fits(request, domain_capacity) {
            continue;
        }
        shared_capacity_match = true;
        candidates.push((
            worker_usage.slots,
            worker.worker_id().clone(),
            domain_id.clone(),
        ));
    }
    candidates.sort_unstable();
    if let Some((_, worker_id, domain_id)) = candidates.into_iter().next() {
        return Ok((worker_id, domain_id));
    }
    if worker_capacity_match && !shared_capacity_match {
        Err(DeploymentPendingReason::SharedHostCapacityExhausted)
    } else {
        Err(DeploymentPendingReason::WorkerCapacityExhausted)
    }
}

fn pending_message(reason: DeploymentPendingReason) -> &'static str {
    match reason {
        DeploymentPendingReason::NoEligibleWorker => {
            "no fresh worker matches the platform and label constraints"
        }
        DeploymentPendingReason::WorkerCapacityExhausted => {
            "eligible workers lack unreserved CPU, memory, or execution slots"
        }
        DeploymentPendingReason::WorkerDisconnected => {
            "matching workers are disconnected, degraded, or draining"
        }
        DeploymentPendingReason::RequiredProtectionUnavailable => {
            "no matching worker advertises the required enforcement capabilities"
        }
        DeploymentPendingReason::SharedHostCapacityExhausted => {
            "the shared host resource-domain budget is exhausted"
        }
        DeploymentPendingReason::PlacementPending => {
            "deployment placement has not been evaluated yet"
        }
    }
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

fn is_control_command(command: &WorkerCommand) -> bool {
    matches!(
        command,
        WorkerCommand::StopRun(_) | WorkerCommand::ResolveRun(_)
    )
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
        let active_deployment: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM job_deployments WHERE job_id = ?1 AND deleted_at_ms IS NULL)",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if active_run || active_deployment {
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

    fn create_revision(
        &self,
        draft: &JobRevisionDraft,
        idempotency_key: &str,
    ) -> Result<JobRevision, ExecutionStoreError> {
        let job_id = draft.job_id();
        let request_json =
            serde_json::to_string(draft).map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;

        if let Some((previous_request, previous_response)) = transaction
            .query_row(
                "SELECT request_json, response_json FROM execution_idempotency
                 WHERE operation = 'revision' AND scope_key = ?1 AND idempotency_key = ?2",
                params![job_id.as_str(), idempotency_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
        {
            if previous_request != request_json {
                return Err(ExecutionStoreError::IdempotencyConflict);
            }
            let revision = serde_json::from_str(&previous_response)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(revision);
        }

        let existing_job: Option<Option<i64>> = transaction
            .query_row(
                "SELECT deleted_at_ms FROM jobs WHERE id = ?1",
                [job_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        match existing_job {
            Some(Some(_)) => return Err(ExecutionStoreError::JobNotFound),
            Some(None) => {}
            None => {
                let active_count: i64 = transaction
                    .query_row(
                        "SELECT COUNT(*) FROM jobs WHERE deleted_at_ms IS NULL",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                if active_count >= MAX_JOBS as i64 {
                    return Err(ExecutionStoreError::JobLimitReached);
                }
                let spec_json = serde_json::to_string(draft.definition())
                    .map_err(|_| ExecutionStoreError::InvalidRequest)?;
                let created_at_ms =
                    now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                transaction
                    .execute(
                        "INSERT INTO jobs (id, spec_json, created_at_ms, deleted_at_ms)
                         VALUES (?1, ?2, ?3, NULL)",
                        params![job_id.as_str(), spec_json, created_at_ms],
                    )
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            }
        }

        let latest_revision: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(revision), 0) FROM job_revisions WHERE job_id = ?1",
                [job_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let revision_number = u64::try_from(latest_revision)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(ExecutionStoreError::Conflict)?;
        let stored_revision_number =
            i64::try_from(revision_number).map_err(|_| ExecutionStoreError::Conflict)?;
        let revision = draft
            .clone()
            .into_revision(revision_number)
            .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let revision_json = serde_json::to_string(&revision)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let created_at_ms = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO job_revisions (job_id, revision, revision_json, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    job_id.as_str(),
                    stored_revision_number,
                    revision_json,
                    created_at_ms
                ],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO execution_idempotency
                 (operation, scope_key, idempotency_key, request_json, response_json, created_at_ms)
                 VALUES ('revision', ?1, ?2, ?3, ?4, ?5)",
                params![
                    job_id.as_str(),
                    idempotency_key,
                    request_json,
                    revision_json,
                    created_at_ms
                ],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(revision)
    }

    fn get_revision(&self, id: &JobRevisionId) -> Result<Option<JobRevision>, ExecutionStoreError> {
        let revision_number =
            i64::try_from(id.revision()).map_err(|_| ExecutionStoreError::RevisionNotFound)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let revision_json = connection
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![id.job_id().as_str(), revision_number],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        revision_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|_| ExecutionStoreError::StorageUnavailable)
            })
            .transpose()
    }

    fn get_deployment(&self, id: &JobId) -> Result<Option<JobDeployment>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let deployment_json = connection
            .query_row(
                "SELECT deployment_json FROM job_deployments WHERE job_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        deployment_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|_| ExecutionStoreError::StorageUnavailable)
            })
            .transpose()
    }

    fn mutate_deployment(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
    ) -> Result<JobDeployment, ExecutionStoreError> {
        self.mutate_deployment_with_workers(id, expected_generation, mutation, idempotency_key, &[])
    }

    fn mutate_deployment_with_workers(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
        workers: &[WorkerStatus],
    ) -> Result<JobDeployment, ExecutionStoreError> {
        let request_json = serde_json::to_string(&(expected_generation, mutation))
            .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;

        if let Some((previous_request, previous_response)) = transaction
            .query_row(
                "SELECT request_json, response_json FROM execution_idempotency
                 WHERE operation = 'deployment' AND scope_key = ?1 AND idempotency_key = ?2",
                params![id.as_str(), idempotency_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
        {
            if previous_request != request_json {
                return Err(ExecutionStoreError::IdempotencyConflict);
            }
            let deployment = serde_json::from_str(&previous_response)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(deployment);
        }

        let job_active: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1 AND deleted_at_ms IS NULL)",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if !job_active {
            return Err(ExecutionStoreError::JobNotFound);
        }
        let current_json: Option<String> = transaction
            .query_row(
                "SELECT deployment_json FROM job_deployments WHERE job_id = ?1",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let current = current_json
            .as_deref()
            .map(serde_json::from_str::<JobDeployment>)
            .transpose()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        match (&current, expected_generation) {
            (None, None) if matches!(mutation, DeploymentMutation::Deploy { .. }) => {}
            (Some(existing), Some(expected)) if existing.generation() == expected => {}
            _ => return Err(ExecutionStoreError::PreconditionFailed),
        }
        if current.as_ref().is_some_and(|deployment| {
            matches!(
                deployment.status(),
                DeploymentStatus::Deleting | DeploymentStatus::Deleted
            )
        }) {
            return Err(ExecutionStoreError::Conflict);
        }

        let (revision_number, replicas, suspended) = match (current.as_ref(), mutation) {
            (None, DeploymentMutation::Deploy { revision, replicas }) => {
                (*revision, *replicas, false)
            }
            (Some(existing), DeploymentMutation::Deploy { revision, replicas }) => {
                (*revision, *replicas, existing.suspended())
            }
            (Some(existing), DeploymentMutation::Scale { replicas }) => (
                existing.revision_id().revision(),
                *replicas,
                existing.suspended(),
            ),
            (Some(existing), DeploymentMutation::Suspend) if !existing.suspended() => (
                existing.revision_id().revision(),
                existing.desired_replicas(),
                true,
            ),
            (Some(existing), DeploymentMutation::Resume) if existing.suspended() => (
                existing.revision_id().revision(),
                existing.desired_replicas(),
                false,
            ),
            (Some(existing), DeploymentMutation::Rollback { revision }) => {
                (*revision, existing.desired_replicas(), existing.suspended())
            }
            (Some(_), DeploymentMutation::Delete) => {
                let current = current
                    .as_ref()
                    .ok_or(ExecutionStoreError::StorageUnavailable)?;
                (current.revision_id().revision(), 0, true)
            }
            _ => return Err(ExecutionStoreError::Conflict),
        };
        if replicas > MAX_DEPLOYMENT_REPLICAS {
            return Err(ExecutionStoreError::InvalidRequest);
        }
        let revision_id = JobRevisionId::new(id.clone(), revision_number)
            .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let stored_revision_number =
            i64::try_from(revision_number).map_err(|_| ExecutionStoreError::RevisionNotFound)?;
        let revision_json: Option<String> = transaction
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![id.as_str(), stored_revision_number],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let revision: JobRevision = revision_json
            .ok_or(ExecutionStoreError::RevisionNotFound)
            .and_then(|json| {
                serde_json::from_str(&json).map_err(|_| ExecutionStoreError::StorageUnavailable)
            })?;
        if revision.id() != &revision_id
            || !matches!(
                revision.workload(),
                conductor_core::Workload::Service { .. }
            )
        {
            return Err(ExecutionStoreError::InvalidRequest);
        }
        let (status, pending_reason, pending_message) = if suspended {
            if matches!(mutation, DeploymentMutation::Delete) {
                (DeploymentStatus::Deleted, None, None)
            } else {
                (DeploymentStatus::Suspended, None, None)
            }
        } else if replicas == 0 {
            (DeploymentStatus::Ready, None, None)
        } else {
            (
                DeploymentStatus::Pending,
                Some(DeploymentPendingReason::PlacementPending),
                Some("deployment intent is durable; scheduler placement is pending".to_owned()),
            )
        };
        let generation = current
            .as_ref()
            .map_or(Some(1), |value| value.generation().checked_add(1))
            .ok_or(ExecutionStoreError::Conflict)?;
        let deployment = JobDeployment::new(
            &revision,
            generation,
            replicas,
            suspended,
            status,
            pending_reason,
            pending_message,
        )
        .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let mut deployment = place_service_slots(&transaction, &revision, &deployment, workers)?;
        if matches!(mutation, DeploymentMutation::Delete) {
            let deletion_status = if active_allocations_exist(&transaction, id)? {
                DeploymentStatus::Deleting
            } else {
                DeploymentStatus::Deleted
            };
            deployment =
                JobDeployment::new(&revision, generation, 0, true, deletion_status, None, None)
                    .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        }
        let deployment_json = serde_json::to_string(&deployment)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let deleted_at_ms = if deployment.status() == DeploymentStatus::Deleted {
            Some(now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?)
        } else {
            None
        };
        let created_at_ms = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO job_deployments (job_id, generation, deployment_json, deleted_at_ms)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(job_id) DO UPDATE SET generation = excluded.generation,
                     deployment_json = excluded.deployment_json, deleted_at_ms = excluded.deleted_at_ms",
                params![id.as_str(), generation as i64, deployment_json, deleted_at_ms],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO execution_idempotency
                 (operation, scope_key, idempotency_key, request_json, response_json, created_at_ms)
                 VALUES ('deployment', ?1, ?2, ?3, ?4, ?5)",
                params![
                    id.as_str(),
                    idempotency_key,
                    request_json,
                    deployment_json,
                    created_at_ms
                ],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(deployment)
    }

    fn reconcile_deployment_placement(
        &self,
        id: &JobId,
        workers: &[WorkerStatus],
    ) -> Result<Option<JobDeployment>, ExecutionStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let current_json = transaction
            .query_row(
                "SELECT deployment_json FROM job_deployments WHERE job_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let Some(current_json) = current_json else {
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(None);
        };
        let current: JobDeployment = serde_json::from_str(&current_json)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if current.status() == DeploymentStatus::Deleting {
            if active_allocations_exist(&transaction, id)? {
                transaction
                    .commit()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                return Ok(Some(current));
            }
            let revision_number = i64::try_from(current.revision_id().revision())
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let revision_json = transaction
                .query_row(
                    "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                    params![id.as_str(), revision_number],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?
                .ok_or(ExecutionStoreError::StorageUnavailable)?;
            let revision: JobRevision = serde_json::from_str(&revision_json)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let deleted = JobDeployment::new(
                &revision,
                current.generation(),
                0,
                true,
                DeploymentStatus::Deleted,
                None,
                None,
            )
            .map_err(|_| ExecutionStoreError::InvalidRequest)?;
            let deleted_json = serde_json::to_string(&deleted)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .execute(
                    "UPDATE job_deployments SET deployment_json = ?1, deleted_at_ms = ?2
                     WHERE job_id = ?3 AND generation = ?4",
                    params![
                        deleted_json,
                        now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                        id.as_str(),
                        current.generation() as i64
                    ],
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(Some(deleted));
        }
        if current.suspended() || current.desired_replicas() == 0 {
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(Some(current));
        }
        let revision_number = i64::try_from(current.revision_id().revision())
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let revision_json = transaction
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![id.as_str(), revision_number],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            .ok_or(ExecutionStoreError::StorageUnavailable)?;
        let revision: JobRevision = serde_json::from_str(&revision_json)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let placed = place_service_slots(&transaction, &revision, &current, workers)?;
        let deployment_json =
            serde_json::to_string(&placed).map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if deployment_json != current_json {
            transaction
                .execute(
                    "UPDATE job_deployments SET deployment_json = ?1 WHERE job_id = ?2 AND generation = ?3",
                    params![deployment_json, id.as_str(), current.generation() as i64],
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        }
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(Some(placed))
    }

    fn list_allocations(&self, id: &JobId) -> Result<Vec<AllocationRecord>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT allocation_json, resource_domain_id FROM allocations
                 WHERE job_id = ?1 ORDER BY created_at_ms, allocation_id",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        rows.map(|row| {
            let (allocation_json, resource_domain_id) =
                row.map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let allocation = serde_json::from_str(&allocation_json)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let resource_domain_id = resource_domain_id
                .map(WorkerId::new)
                .transpose()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            Ok(AllocationRecord {
                allocation,
                resource_domain_id,
            })
        })
        .collect()
    }

    fn list_active_deployments(&self) -> Result<Vec<JobDeployment>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT deployment_json FROM job_deployments
                 WHERE deleted_at_ms IS NULL ORDER BY job_id LIMIT ?1",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([MAX_JOBS as i64], |row| row.get::<_, String>(0))
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|_| ExecutionStoreError::StorageUnavailable)?)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn list_service_attempts(
        &self,
        id: &JobId,
    ) -> Result<Vec<ServiceAttemptRecord>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT si.instance_id, si.replica_index, si.current_allocation_id,
                        sa.allocation_id, a.allocation_json, a.resource_domain_id,
                        sa.current_run_id, COALESCE(sa.restart_count, 0),
                        COALESCE(sa.next_start_at_ms, 0), COALESCE(sa.terminal_failure, 0),
                        r.observation_json, r.job_spec_json
                 FROM service_instances si
                 LEFT JOIN service_attempts sa ON sa.service_instance_id = si.instance_id
                 LEFT JOIN allocations a ON a.allocation_id = COALESCE(sa.allocation_id, si.current_allocation_id)
                 LEFT JOIN runs r ON r.run_id = sa.current_run_id
                 WHERE si.job_id = ?1 AND (si.current_allocation_id IS NOT NULL
                     OR sa.current_run_id IS NOT NULL OR sa.allocation_id IS NOT NULL)
                 ORDER BY si.replica_index",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                ))
            })
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        rows.map(|row| {
            let (
                instance,
                replica_index,
                current_allocation_id,
                attempt_allocation_id,
                allocation_json,
                domain,
                run_id,
                count,
                next_start,
                terminal,
                observation,
                run_spec_json,
            ) = row.map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let service_ports = run_spec_json
                .map(|json| serde_json::from_str::<RunSpec>(&json))
                .transpose()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?
                .map(|spec| spec.service_ports().to_vec())
                .unwrap_or_default();
            let current_run = observation
                .map(|json| {
                    serde_json::from_str::<RunObservation>(&json)
                        .map_err(|_| ExecutionStoreError::StorageUnavailable)
                })
                .transpose()?;
            let service_ports_ready = current_run.as_ref().is_some_and(|run| {
                run.state() == RunState::Running
                    && run.readiness() == Some(conductor_core::ReadinessState::Ready)
            });
            Ok(ServiceAttemptRecord {
                service_instance_id: ServiceInstanceId::new(instance)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                replica_index: u32::try_from(replica_index)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                current_allocation_id: current_allocation_id
                    .map(AllocationId::new)
                    .transpose()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                allocation: match (attempt_allocation_id, allocation_json) {
                    (Some(_), Some(json)) => Some(AllocationRecord {
                        allocation: serde_json::from_str(&json)
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                        resource_domain_id: domain
                            .map(WorkerId::new)
                            .transpose()
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                    }),
                    (None, Some(json)) => Some(AllocationRecord {
                        allocation: serde_json::from_str(&json)
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                        resource_domain_id: domain
                            .map(WorkerId::new)
                            .transpose()
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                    }),
                    _ => None,
                },
                current_run_id: run_id
                    .map(RunId::new)
                    .transpose()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                current_run,
                restart_count: u32::try_from(count)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                next_start_at_ms: next_start,
                terminal_failure: terminal != 0,
                service_ports,
                service_ports_ready,
            })
        })
        .collect()
    }

    fn begin_service_run(
        &self,
        job_id: &JobId,
        allocation_id: &AllocationId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        worker_capacity: usize,
        service_network: Option<&WorkerServiceNetwork>,
    ) -> Result<Option<RunAdmission>, RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let row = transaction
            .query_row(
                "SELECT si.instance_id, a.allocation_json, d.deployment_json
                 FROM service_instances si
                 JOIN allocations a ON a.allocation_id = si.current_allocation_id
                 JOIN job_deployments d ON d.job_id = si.job_id
                 WHERE si.job_id = ?1 AND si.current_allocation_id = ?2
                   AND a.worker_id = ?3 AND a.state = 'assigned' AND d.deleted_at_ms IS NULL",
                params![job_id.as_str(), allocation_id.as_str(), worker_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let Some((instance_raw, allocation_json, deployment_json)) = row else {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        };
        let instance =
            ServiceInstanceId::new(instance_raw).map_err(|_| RunStoreError::StorageUnavailable)?;
        let allocation: Allocation = serde_json::from_str(&allocation_json)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let deployment: JobDeployment = serde_json::from_str(&deployment_json)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if deployment.suspended()
            || deployment.desired_replicas() == 0
            || deployment.revision_id() != allocation.job_revision_id()
        {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        }
        transaction
            .execute(
                "INSERT OR IGNORE INTO service_attempts (service_instance_id, allocation_id) VALUES (?1, ?2)",
                params![instance.as_str(), allocation_id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE service_attempts SET allocation_id = ?1, restart_count = 0,
                     next_start_at_ms = 0, terminal_failure = 0
                 WHERE service_instance_id = ?2 AND current_run_id IS NULL
                   AND (allocation_id IS NULL OR allocation_id <> ?1)",
                params![allocation_id.as_str(), instance.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let eligible: bool = transaction
            .query_row(
                "SELECT current_run_id IS NULL AND terminal_failure = 0 AND next_start_at_ms <= ?1
                 FROM service_attempts WHERE service_instance_id = ?2",
                params![
                    now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
                    instance.as_str()
                ],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if !eligible {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        }
        let revision_number = i64::try_from(allocation.job_revision_id().revision())
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let revision_json: String = transaction
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![job_id.as_str(), revision_number],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let revision: JobRevision =
            serde_json::from_str(&revision_json).map_err(|_| RunStoreError::StorageUnavailable)?;
        let active_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM runs WHERE worker_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if active_count >= worker_capacity as i64 {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Err(RunStoreError::CapacityReached);
        }
        let pending_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM manager_commands WHERE worker_id = ?1 AND state = 'pending'",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if pending_count >= MAX_PENDING_NON_CONTROL_COMMANDS_PER_WORKER as i64 {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
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
        let (declared_ports, readiness_probe) = match revision.workload() {
            Workload::Service {
                ports,
                readiness_probe,
                ..
            } => (ports.as_slice(), readiness_probe.as_ref()),
            Workload::OneShot => (&[][..], None),
        };
        if !declared_ports.is_empty() && service_network.is_none() {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        }
        let assigned_ports = match service_network {
            Some(network) => lease_service_ports(
                &transaction,
                allocation_id,
                &run_id,
                sequence,
                network,
                declared_ports,
            )?,
            None => Vec::new(),
        };
        if !declared_ports.is_empty() && assigned_ports.is_empty() {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        }
        let mut spec = RunSpec::new(run_id, worker_id.clone(), revision.definition().clone())
            .with_service_ports(assigned_ports)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if let Some(probe) = readiness_probe {
            spec = spec
                .with_readiness_probe(probe.clone())
                .map_err(|_| RunStoreError::StorageUnavailable)?;
        }
        let command_id =
            next_command_id(&transaction).map_err(|_| RunStoreError::StorageUnavailable)?;
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
        let observation_json =
            serde_json::to_string(&observation).map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO runs (run_id, job_id, worker_id, job_spec_json, observation_json, revision, state, idempotency_key, request_fingerprint, run_order)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8)",
                params![spec.id().as_str(), job_id.as_str(), worker_id.as_str(), spec_json, observation_json, observation.revision() as i64, observation.state().as_str(), sequence],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        insert_command(
            &transaction,
            &envelope,
            spec.id(),
            now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
        )
        .map_err(|_| RunStoreError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE service_attempts SET current_run_id = ?1, allocation_id = ?2
                 WHERE service_instance_id = ?3 AND current_run_id IS NULL AND terminal_failure = 0",
                params![spec.id().as_str(), allocation_id.as_str(), instance.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if changed != 1 {
            return Err(RunStoreError::JobConflict);
        }
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        Ok(Some(RunAdmission {
            spec,
            replay: None,
            command: Some(envelope),
        }))
    }

    fn finish_service_attempt(
        &self,
        instance_id: &ServiceInstanceId,
        run_id: &RunId,
        restart_count: u32,
        next_start_at_ms: i64,
        terminal_failure: bool,
    ) -> Result<(), RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE service_attempts SET current_run_id = NULL, restart_count = ?1,
                     next_start_at_ms = ?2, terminal_failure = ?3
                 WHERE service_instance_id = ?4 AND current_run_id = ?5",
                params![
                    i64::from(restart_count),
                    next_start_at_ms,
                    terminal_failure,
                    instance_id.as_str(),
                    run_id.as_str()
                ],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if changed == 0 {
            return Err(RunStoreError::JobConflict);
        }
        transaction
            .execute(
                "DELETE FROM service_port_leases WHERE run_id = ?1",
                [run_id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)
    }

    fn release_service_allocation(
        &self,
        allocation_id: &AllocationId,
    ) -> Result<(), ExecutionStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let Some(mut record) = load_allocation(&transaction, allocation_id.as_str())? else {
            return Err(ExecutionStoreError::StorageUnavailable);
        };
        if record.allocation.state() != AllocationState::Released {
            record
                .allocation
                .release()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            persist_allocation(&transaction, &record)?;
        }
        transaction
            .execute(
                "DELETE FROM service_port_leases WHERE allocation_id = ?1",
                [allocation_id.as_str()],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)
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
        if pending_count >= MAX_PENDING_NON_CONTROL_COMMANDS_PER_WORKER as i64 {
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
        let existing = {
            let mut statement = transaction
                .prepare(
                    "SELECT envelope_json FROM manager_commands
                     WHERE run_id = ?1 AND worker_id = ?2 AND worker_incarnation = ?3
                     ORDER BY rowid DESC",
                )
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            let rows = statement
                .query_map(
                    params![run_id.as_str(), worker_id.as_str(), incarnation],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            let mut matching = None;
            for row in rows {
                let envelope: CommandEnvelope =
                    serde_json::from_str(&row.map_err(|_| CommandStoreError::StorageUnavailable)?)
                        .map_err(|_| CommandStoreError::StorageUnavailable)?;
                if envelope.command == command {
                    matching = Some(envelope);
                    break;
                }
            }
            matching
        };
        if let Some(existing) = existing {
            transaction
                .commit()
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            return Ok(existing);
        }
        let pending_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM manager_commands WHERE worker_id = ?1 AND state = 'pending'",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let limit = if is_control_command(&command) {
            MAX_PENDING_COMMANDS_PER_WORKER
        } else {
            MAX_PENDING_NON_CONTROL_COMMANDS_PER_WORKER
        };
        if pending_count >= limit as i64 {
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

    fn persist_stop_intent(
        &self,
        run_id: &RunId,
        worker_incarnation: u64,
    ) -> Result<(RunObservation, Option<CommandEnvelope>), RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let stored_json: Option<String> = transaction
            .query_row(
                "SELECT observation_json FROM runs WHERE run_id = ?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let stored_json = stored_json.ok_or(RunStoreError::RunNotFound)?;
        let current: RunObservation =
            serde_json::from_str(&stored_json).map_err(|_| RunStoreError::StorageUnavailable)?;
        if current.state().is_terminal() {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok((current, None));
        }

        let stopping = if current.state() == RunState::Stopping {
            current.clone()
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
                .ok_or(RunStoreError::StorageUnavailable)?
                .with_termination_reason(RunTerminationReason::Cancelled)
        };
        if stopping != current {
            let serialized =
                serde_json::to_string(&stopping).map_err(|_| RunStoreError::StorageUnavailable)?;
            let changed = transaction
                .execute(
                    "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3
                     WHERE run_id = ?4 AND revision = ?5
                       AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
                    params![
                        serialized,
                        stopping.revision() as i64,
                        stopping.state().as_str(),
                        run_id.as_str(),
                        stopping.revision().saturating_sub(1) as i64,
                    ],
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            if changed != 1 {
                return Err(RunStoreError::StorageUnavailable);
            }
        }

        let worker_incarnation =
            i64::try_from(worker_incarnation).map_err(|_| RunStoreError::StorageUnavailable)?;
        let worker_id = stopping.worker_id();
        let command = WorkerCommand::StopRun(StopRun::new(run_id.clone()));
        let existing = {
            let mut statement = transaction
                .prepare(
                    "SELECT envelope_json FROM manager_commands
                     WHERE run_id = ?1 AND worker_id = ?2 AND worker_incarnation = ?3
                     ORDER BY rowid DESC",
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            let rows = statement
                .query_map(
                    params![run_id.as_str(), worker_id.as_str(), worker_incarnation],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            let mut matching = None;
            for row in rows {
                let envelope: CommandEnvelope =
                    serde_json::from_str(&row.map_err(|_| RunStoreError::StorageUnavailable)?)
                        .map_err(|_| RunStoreError::StorageUnavailable)?;
                if envelope.command == command {
                    matching = Some(envelope);
                    break;
                }
            }
            matching
        };
        let envelope = if let Some(existing) = existing {
            existing
        } else {
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
            let envelope = CommandEnvelope {
                command_id: next_command_id(&transaction)?,
                target_worker_id: worker_id.clone(),
                target_worker_incarnation: worker_incarnation as u64,
                command,
            };
            insert_command(
                &transaction,
                &envelope,
                run_id,
                now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
            envelope
        };
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        Ok((stopping, Some(envelope)))
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
    fn command_record(
        &self,
        command_id: &CommandId,
    ) -> Result<Option<CommandRecord>, CommandStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        connection
            .query_row(
                "SELECT envelope_json, acknowledgement_json FROM manager_commands WHERE command_id = ?1",
                [command_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(|_| CommandStoreError::StorageUnavailable)?
            .map(|(envelope, acknowledgement_json)| {
                Ok(CommandRecord {
                    envelope: serde_json::from_str(&envelope)
                        .map_err(|_| CommandStoreError::StorageUnavailable)?,
                    acknowledgement_json,
                })
            })
            .transpose()
    }

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
            transaction
                .commit()
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            return if record.3.as_deref() == Some(acknowledgement_json)
                || acknowledgement_json == r#"{"accepted":true}"# {
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

    fn reconcile_worker_inventory_page(
        &self,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        entries: &[WorkerInventoryEntry],
    ) -> Result<Vec<RunId>, CommandStoreError> {
        let worker_incarnation =
            i64::try_from(worker_incarnation).map_err(|_| CommandStoreError::Conflict)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let mut observed_ids = Vec::with_capacity(entries.len());
        for entry in entries {
            let incoming = &entry.observation;
            if incoming.worker_id() != worker_id
                || incoming.worker_incarnation() > worker_incarnation as u64
            {
                return Err(CommandStoreError::Conflict);
            }
            let stored_json = transaction
                .query_row(
                    "SELECT observation_json FROM runs WHERE run_id = ?1 AND worker_id = ?2",
                    params![incoming.run_id().as_str(), worker_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            let Some(stored_json) = stored_json else {
                // A worker may retain an orphan after manager state was restored
                // from an older backup. Keep it on the worker; never adopt or relaunch it.
                continue;
            };
            let stored: RunObservation = serde_json::from_str(&stored_json)
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            if incoming.job_id() != stored.job_id() || incoming.worker_id() != stored.worker_id() {
                return Err(CommandStoreError::Conflict);
            }
            // Only active assignments need to be retained across pages so the
            // completed-run history can grow without making inventory scans
            // exceed the worker's live-capacity bound.
            if stored.state().is_terminal() {
                continue;
            }
            observed_ids.push(incoming.run_id().clone());

            if incoming.revision() < stored.revision() {
                continue;
            }
            if incoming.revision() == stored.revision() {
                if incoming != &stored {
                    return Err(CommandStoreError::Conflict);
                }
                continue;
            }
            if stored.worker_incarnation() != 0
                && incoming.worker_incarnation() != stored.worker_incarnation()
            {
                if incoming.worker_incarnation() < stored.worker_incarnation() {
                    continue;
                }
                return Err(CommandStoreError::Conflict);
            }
            let mut newer = stored.clone();
            if !newer.apply_newer(incoming.clone()) {
                return Err(CommandStoreError::Conflict);
            }
            let serialized =
                serde_json::to_string(&newer).map_err(|_| CommandStoreError::StorageUnavailable)?;
            transaction
                .execute(
                    "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3
                     WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
                    params![serialized, newer.revision() as i64, newer.state().as_str(), newer.run_id().as_str()],
                )
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
        }
        transaction
            .commit()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        Ok(observed_ids)
    }

    fn finish_worker_inventory(
        &self,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        observed_run_ids: &[RunId],
    ) -> Result<(), CommandStoreError> {
        let incarnation =
            i64::try_from(worker_incarnation).map_err(|_| CommandStoreError::Conflict)?;
        let observed = observed_run_ids.iter().collect::<BTreeSet<_>>();
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let mut statement = transaction
            .prepare(
                "SELECT run_id, observation_json FROM runs
                 WHERE worker_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([worker_id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let runs = rows
            .map(|row| {
                let (id, json) = row.map_err(|_| CommandStoreError::StorageUnavailable)?;
                let id = RunId::new(id).map_err(|_| CommandStoreError::StorageUnavailable)?;
                let observation: RunObservation = serde_json::from_str(&json)
                    .map_err(|_| CommandStoreError::StorageUnavailable)?;
                Ok((id, observation))
            })
            .collect::<Result<Vec<_>, CommandStoreError>>()?;
        drop(statement);

        for (run_id, stored) in runs {
            if observed.contains(&run_id) || stored.state() == RunState::Unknown {
                continue;
            }
            if stored.state() == RunState::Requested
                && has_pending_start_command(&transaction, &run_id, worker_id, incarnation)?
            {
                continue;
            }
            let unknown = stored
                .next_revision(
                    RunState::Unknown,
                    stored.started_at_ms(),
                    None,
                    None,
                    None,
                    Some(RunFailureReason::OutcomeUnknown),
                )
                .ok_or(CommandStoreError::StorageUnavailable)?;
            let serialized = serde_json::to_string(&unknown)
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            transaction
                .execute(
                    "UPDATE runs SET observation_json = ?1, revision = ?2, state = 'unknown'
                     WHERE run_id = ?3 AND revision = ?4 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
                    params![serialized, unknown.revision() as i64, run_id.as_str(), stored.revision() as i64],
                )
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
        }
        transaction
            .commit()
            .map_err(|_| CommandStoreError::StorageUnavailable)
    }
}

fn has_pending_start_command(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &RunId,
    worker_id: &WorkerId,
    worker_incarnation: i64,
) -> Result<bool, CommandStoreError> {
    let mut statement = transaction
        .prepare(
            "SELECT envelope_json FROM manager_commands
             WHERE run_id = ?1 AND worker_id = ?2 AND worker_incarnation = ?3 AND state = 'pending'",
        )
        .map_err(|_| CommandStoreError::StorageUnavailable)?;
    let rows = statement
        .query_map(
            params![run_id.as_str(), worker_id.as_str(), worker_incarnation],
            |row| row.get::<_, String>(0),
        )
        .map_err(|_| CommandStoreError::StorageUnavailable)?;
    for row in rows {
        let json = row.map_err(|_| CommandStoreError::StorageUnavailable)?;
        let envelope: CommandEnvelope =
            serde_json::from_str(&json).map_err(|_| CommandStoreError::StorageUnavailable)?;
        if let WorkerCommand::StartRun(command) = envelope.command
            && command.spec().id() == run_id
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn service_restart_backoff_ms(restart_count: u32) -> u64 {
    let shift = restart_count.saturating_sub(1).min(31);
    SERVICE_RESTART_BASE_BACKOFF_MS
        .saturating_mul(1_u64 << shift)
        .min(SERVICE_RESTART_MAX_BACKOFF_MS)
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
        let status = worker
            .status()
            .map_err(|_| ManagerError::WorkerUnavailable)?;
        if status.is_valid() {
            Ok(status)
        } else {
            Err(ManagerError::WorkerUnavailable)
        }
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

    pub fn create_revision(
        &self,
        draft: &JobRevisionDraft,
        idempotency_key: &str,
    ) -> Result<JobRevision, ExecutionStoreError> {
        self.repository.create_revision(draft, idempotency_key)
    }

    pub fn get_revision(
        &self,
        id: &JobRevisionId,
    ) -> Result<Option<JobRevision>, ExecutionStoreError> {
        self.repository.get_revision(id)
    }

    pub fn get_deployment(&self, id: &JobId) -> Result<Option<JobDeployment>, ExecutionStoreError> {
        self.repository.get_deployment(id)
    }

    pub fn mutate_deployment(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
    ) -> Result<JobDeployment, ExecutionStoreError> {
        self.repository
            .mutate_deployment(id, expected_generation, mutation, idempotency_key)
    }

    pub fn mutate_deployment_with_workers(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
        workers: &[WorkerStatus],
    ) -> Result<JobDeployment, ExecutionStoreError> {
        self.repository.mutate_deployment_with_workers(
            id,
            expected_generation,
            mutation,
            idempotency_key,
            workers,
        )
    }

    pub fn reconcile_deployment_placement(
        &self,
        id: &JobId,
        workers: &[WorkerStatus],
    ) -> Result<Option<JobDeployment>, ExecutionStoreError> {
        self.repository.reconcile_deployment_placement(id, workers)
    }

    pub fn list_allocations(
        &self,
        id: &JobId,
    ) -> Result<Vec<AllocationRecord>, ExecutionStoreError> {
        self.repository.list_allocations(id)
    }

    pub fn list_service_attempts(
        &self,
        id: &JobId,
    ) -> Result<Vec<ServiceAttemptRecord>, ExecutionStoreError> {
        self.repository.list_service_attempts(id)
    }

    /// Reconcile placed service slots with their workers. Repeated calls are
    /// safe: the repository reserves one run ID per service instance before
    /// dispatch, and unresolved worker outcomes are never relaunched.
    pub fn reconcile_services(&self, workers: &[Arc<dyn WorkerPort>]) -> Result<(), ManagerError> {
        if workers.is_empty() {
            return Ok(());
        }
        let statuses = workers
            .iter()
            .filter_map(|worker| worker.status().ok())
            .filter(WorkerStatus::is_valid)
            .collect::<Vec<_>>();
        let deployments = self
            .repository
            .list_active_deployments()
            .map_err(|_| ManagerError::StorageUnavailable)?;
        for deployment in deployments {
            let job_id = deployment.job_id().clone();
            self.repository
                .reconcile_deployment_placement(&job_id, &statuses)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            let allocations = self
                .repository
                .list_allocations(&job_id)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            let attempts = self
                .repository
                .list_service_attempts(&job_id)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            for attempt in attempts {
                let slot_desired = !deployment.suspended()
                    && attempt.replica_index < deployment.desired_replicas();
                if let Some(current) = attempt.current_run {
                    let Some(run_allocation) = attempt.allocation.as_ref() else {
                        continue;
                    };
                    let run_allocation = run_allocation.allocation();
                    let run_is_desired = slot_desired
                        && attempt.current_allocation_id.as_ref() == Some(run_allocation.id())
                        && run_allocation.job_revision_id() == deployment.revision_id();
                    let Some(worker) = workers
                        .iter()
                        .find(|worker| worker.descriptor().worker_id() == current.worker_id())
                    else {
                        continue;
                    };
                    if !run_is_desired && !current.state().is_terminal() {
                        if let Ok(stopped) = self.stop_run(current.run_id(), worker.as_ref())
                            && stopped.state().is_terminal()
                        {
                            self.repository
                                .finish_service_attempt(
                                    &attempt.service_instance_id,
                                    stopped.run_id(),
                                    attempt.restart_count,
                                    now_ms().map_err(|_| ManagerError::StorageUnavailable)?,
                                    true,
                                )
                                .map_err(map_run_error)?;
                            self.repository
                                .release_service_allocation(run_allocation.id())
                                .map_err(|_| ManagerError::StorageUnavailable)?;
                        }
                        continue;
                    }
                    let observed = match worker.inspect(InspectRun::new(current.run_id().clone())) {
                        Ok(Some(observed))
                            if observed.run_id() == current.run_id()
                                && observed.job_id() == current.job_id()
                                && observed.worker_id() == current.worker_id()
                                && observed.revision() > current.revision() =>
                        {
                            observed
                        }
                        Ok(Some(_)) => current.clone(),
                        Ok(None) => continue,
                        Err(_) => continue,
                    };
                    if observed != current {
                        self.repository
                            .record_observation(&observed)
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                    }
                    if !observed.state().is_terminal() {
                        continue;
                    }
                    if !run_is_desired {
                        self.repository
                            .finish_service_attempt(
                                &attempt.service_instance_id,
                                observed.run_id(),
                                attempt.restart_count,
                                now_ms().map_err(|_| ManagerError::StorageUnavailable)?,
                                true,
                            )
                            .map_err(map_run_error)?;
                        self.repository
                            .release_service_allocation(run_allocation.id())
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                        continue;
                    }
                    let restart_count = attempt.restart_count.saturating_add(1);
                    let terminal = restart_count > MAX_SERVICE_RESTARTS;
                    let delay = service_restart_backoff_ms(restart_count);
                    let next = now_ms()
                        .map_err(|_| ManagerError::StorageUnavailable)?
                        .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX));
                    self.repository
                        .finish_service_attempt(
                            &attempt.service_instance_id,
                            observed.run_id(),
                            restart_count,
                            next,
                            terminal,
                        )
                        .map_err(map_run_error)?;
                    continue;
                }
                if attempt.current_run_id.is_some() {
                    continue;
                }
                if !slot_desired {
                    if let Some(allocation) = attempt.allocation.as_ref()
                        && allocation.allocation().state() == AllocationState::Assigned
                    {
                        self.repository
                            .release_service_allocation(allocation.allocation().id())
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                    }
                    continue;
                }
                let Some(current_allocation_id) = attempt.current_allocation_id.as_ref() else {
                    continue;
                };
                if let Some(previous) = attempt.allocation.as_ref()
                    && previous.allocation().id() != current_allocation_id
                    && previous.allocation().state() == AllocationState::Assigned
                {
                    self.repository
                        .release_service_allocation(previous.allocation().id())
                        .map_err(|_| ManagerError::StorageUnavailable)?;
                }
                let Some(allocation) = allocations
                    .iter()
                    .find(|record| record.allocation().id() == current_allocation_id)
                else {
                    continue;
                };
                if allocation.allocation().state() != AllocationState::Assigned {
                    continue;
                }
                let Some(worker_id) = allocation.allocation().assigned_worker_id() else {
                    continue;
                };
                let Some(worker) = workers
                    .iter()
                    .find(|worker| worker.descriptor().worker_id() == worker_id)
                else {
                    continue;
                };
                if (attempt.terminal_failure
                    && attempt.current_allocation_id.as_ref()
                        == attempt
                            .allocation
                            .as_ref()
                            .map(|record| record.allocation().id()))
                    || worker.is_draining()
                    || worker.is_degraded()
                {
                    continue;
                }
                let descriptor = worker.descriptor();
                let service_network = statuses
                    .iter()
                    .find(|status| status.worker_id() == descriptor.worker_id())
                    .and_then(WorkerStatus::service_network);
                let admission = match self.repository.begin_service_run(
                    &job_id,
                    allocation.allocation().id(),
                    descriptor.worker_id(),
                    descriptor.incarnation(),
                    descriptor.capacity(),
                    service_network,
                ) {
                    Ok(Some(admission)) => admission,
                    Ok(None)
                    | Err(
                        RunStoreError::CapacityReached
                        | RunStoreError::JobConflict
                        | RunStoreError::PendingDeliveryLimit,
                    ) => {
                        continue;
                    }
                    Err(error) => return Err(map_run_error(error)),
                };
                let envelope = admission.command.ok_or(ManagerError::StorageUnavailable)?;
                match worker.dispatch_command(envelope.clone()) {
                    Ok(observation)
                        if observation.run_id() == admission.spec.id()
                            && observation.job_id() == admission.spec.job().id()
                            && observation.worker_id() == descriptor.worker_id()
                            && (observation.state().is_terminal()
                                || observation.worker_incarnation()
                                    == descriptor.incarnation()) =>
                    {
                        self.repository
                            .record_observation(&observation)
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                        self.acknowledge_command(&envelope)?;
                    }
                    Err(WorkerPortError::CapacityReached) => {
                        self.repository
                            .delete_unaccepted_run(admission.spec.id())
                            .map_err(map_run_error)?;
                        self.repository
                            .finish_service_attempt(
                                &attempt.service_instance_id,
                                admission.spec.id(),
                                attempt.restart_count,
                                now_ms()
                                    .map_err(|_| ManagerError::StorageUnavailable)?
                                    .saturating_add(
                                        i64::try_from(service_restart_backoff_ms(
                                            attempt.restart_count.saturating_add(1),
                                        ))
                                        .unwrap_or(i64::MAX),
                                    ),
                                false,
                            )
                            .map_err(map_run_error)?;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
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
        let pending = self
            .repository
            .pending_commands(
                descriptor.worker_id(),
                descriptor.incarnation(),
                MAX_PENDING_COMMANDS_PER_WORKER,
            )
            .map_err(|_| ManagerError::StorageUnavailable)?;
        for envelope in pending {
            let expected_run_id = match &envelope.command {
                WorkerCommand::StartRun(command) => command.spec().id().clone(),
                WorkerCommand::InspectRun(command) => command.run_id().clone(),
                WorkerCommand::StopRun(command) => command.run_id().clone(),
                WorkerCommand::ResolveRun(command) => command.run_id().clone(),
                WorkerCommand::ReadRunLogs(command) => command.run_id().clone(),
            };
            if envelope.target_worker_id != *descriptor.worker_id()
                || envelope.target_worker_incarnation != descriptor.incarnation()
            {
                return Err(ManagerError::StorageUnavailable);
            }
            let observation = worker
                .dispatch_command(envelope.clone())
                .map_err(|_| ManagerError::WorkerUnavailable)?;
            if observation.run_id() != &expected_run_id
                || observation.worker_id() != descriptor.worker_id()
                || (!observation.state().is_terminal()
                    && observation.worker_incarnation() != descriptor.incarnation())
            {
                return Err(ManagerError::WorkerUnavailable);
            }
            let stored = self
                .repository
                .get_run(&expected_run_id)
                .map_err(|_| ManagerError::StorageUnavailable)?
                .ok_or(ManagerError::StorageUnavailable)?;
            if observation.job_id() != stored.job_id() {
                return Err(ManagerError::WorkerUnavailable);
            }
            self.repository
                .record_observation(&observation)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            self.acknowledge_command(&envelope)?;
        }
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
        let descriptor = worker.descriptor();
        let (stopping, envelope) = self
            .repository
            .persist_stop_intent(id, descriptor.incarnation())
            .map_err(map_run_error)?;
        let Some(envelope) = envelope else {
            return Ok(stopping);
        };
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
        RunStoreError::RunNotFound => ManagerError::RunNotFound,
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

#[cfg(test)]
mod service_retry_tests {
    use super::{MAX_SERVICE_RESTARTS, SERVICE_RESTART_MAX_BACKOFF_MS, service_restart_backoff_ms};

    #[test]
    fn service_retry_budget_and_backoff_are_bounded() {
        assert_eq!(MAX_SERVICE_RESTARTS, 5);
        assert_eq!(service_restart_backoff_ms(0), 250);
        assert_eq!(service_restart_backoff_ms(1), 250);
        assert_eq!(service_restart_backoff_ms(2), 500);
        assert_eq!(service_restart_backoff_ms(3), 1_000);
        assert_eq!(service_restart_backoff_ms(4), 2_000);
        assert_eq!(service_restart_backoff_ms(5), 4_000);
        assert_eq!(service_restart_backoff_ms(6), 8_000);
        assert_eq!(
            service_restart_backoff_ms(32),
            SERVICE_RESTART_MAX_BACKOFF_MS
        );
    }
}
