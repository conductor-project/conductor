//! Manager-owned application operations and durable job-definition storage.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{fs, io};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use base64::Engine;
use conductor_core::{
    Allocation, AllocationId, AllocationState, AssignedServicePort, CommandEnvelope, CommandId,
    DeploymentMutation, DeploymentPendingReason, DeploymentRollout, DeploymentStatus,
    DeploymentUpdateStrategy, InspectRun, JobDeployment, JobId, JobRevision, JobRevisionDraft,
    JobRevisionId, JobSpec, MAX_DEPLOYMENT_REPLICAS, MAX_WORKER_CAPACITY, ReadRunLogs,
    ReadinessState, ResolveRun, ResourceReservation, RunFailureReason, RunId, RunLogChunk,
    RunObservation, RunSpec, RunState, RunTerminationReason, ServiceIdentity, ServiceInstance,
    ServiceInstanceId, ServicePortProtocol, StartRun, StopRun, WorkerCommand, WorkerDescriptor,
    WorkerId, WorkerInventoryEntry, WorkerResourceCapacity, WorkerServiceNetwork, WorkerStatus,
    Workload,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;

/// Temporary limit for active jobs. Pagination is a later milestone.
pub const MAX_JOBS: usize = 1_000;
const SCHEMA_VERSION: i64 = 14;
/// Current manager database schema understood by this build.
pub const CURRENT_SCHEMA_VERSION: i64 = SCHEMA_VERSION;
const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_PENDING_COMMANDS_PER_WORKER: usize = 32;
const MAX_PENDING_NON_CONTROL_COMMANDS_PER_WORKER: usize = 24;
pub const DEFAULT_COMPLETED_RUN_RETENTION: usize = 10_000;
pub const MIN_COMPLETED_RUN_RETENTION: usize = 1;
pub const MAX_COMPLETED_RUN_RETENTION: usize = 1_000_000;
pub const MAX_HISTORY_CLEANUP_BATCH: usize = 128;
const MAX_SERVICE_RESTARTS: u32 = 5;
const SERVICE_RESTART_BASE_BACKOFF_MS: u64 = 250;
const SERVICE_RESTART_MAX_BACKOFF_MS: u64 = 30_000;

/// Read the manager schema marker without opening the database for writes or running migrations.
/// `None` means that the database file does not exist yet.
pub fn inspect_schema_version(path: &Path) -> io::Result<Option<i64>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(io::Error::other("manager database could not be inspected"));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(io::Error::other("manager database could not be inspected"));
    }
    let resolved_path = fs::canonicalize(path)
        .map_err(|_| io::Error::other("manager database could not be inspected"))?;
    let connection = Connection::open_with_flags(
        resolved_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(|_| io::Error::other("manager database could not be inspected"))?;
    let version = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|_| io::Error::other("manager database could not be inspected"))?;
    Ok(Some(version))
}

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
    WorkerDraining,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlayNetworkError {
    DomainExists,
    DomainNotFound,
    InvalidAddressPool,
    AddressPoolOverlap,
    AddressPoolExhausted,
    PeerLimitReached,
    IdentityUnavailable,
    EndpointIdentityConflict,
    EndpointIdentityReplay,
    PeerQuarantined,
    InvalidLease,
    InvalidDirectCandidates,
    StaleGeneration,
    PeerNotFound,
    ObservationConflict,
    StorageUnavailable,
}

#[path = "structs/page.rs"]
mod page;
pub use page::Page;

#[path = "structs/audit_event.rs"]
mod audit_event;
pub use audit_event::AuditEvent;

#[path = "structs/audit_event_input.rs"]
mod audit_event_input;
pub use audit_event_input::AuditEventInput;

#[path = "structs/operational_metrics.rs"]
mod operational_metrics;
pub use operational_metrics::OperationalMetrics;

#[path = "structs/reconcile_metrics.rs"]
mod reconcile_metrics;
pub use reconcile_metrics::ReconcileMetrics;

pub const DEFAULT_PAGE_SIZE: usize = 50;
pub const MAX_PAGE_SIZE: usize = 100;

#[path = "structs/run_admission.rs"]
mod run_admission;
pub use run_admission::RunAdmission;

#[path = "structs/allocation_record.rs"]
mod allocation_record;
pub use allocation_record::AllocationRecord;

#[path = "structs/service_attempt_record.rs"]
mod service_attempt_record;
pub use service_attempt_record::ServiceAttemptRecord;

#[path = "structs/worker_drain_record.rs"]
mod worker_drain_record;
pub use worker_drain_record::WorkerDrainRecord;

#[path = "structs/worker_drain_status.rs"]
mod worker_drain_status;
pub use worker_drain_status::WorkerDrainStatus;

#[path = "structs/service_catalog_endpoint.rs"]
mod service_catalog_endpoint;
pub use service_catalog_endpoint::ServiceCatalogEndpoint;

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

#[path = "structs/command_record.rs"]
mod command_record;
pub use command_record::CommandRecord;

/// Storage port owned by the manager. Implementations preserve deleted IDs.
pub trait JobRepository: Send + Sync + CommandJournal {
    fn record_audit_event(&self, event: &AuditEventInput) -> Result<(), ExecutionStoreError>;
    fn list_audit_events(&self, limit: usize) -> Result<Vec<AuditEvent>, ExecutionStoreError>;
    fn operational_metrics(&self) -> Result<OperationalMetrics, ExecutionStoreError>;
    fn create_overlay_network_domain(
        &self,
        config: &conductor_core::OverlayNetworkDomainConfig,
        now_ms: i64,
    ) -> Result<(), OverlayNetworkError>;
    fn get_overlay_network_domain(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
    ) -> Result<Option<conductor_core::OverlayNetworkDomainConfig>, OverlayNetworkError>;
    fn register_overlay_peer(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        endpoint_id: &conductor_core::OverlayEndpointId,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<conductor_core::OverlayPeerAssignment, OverlayNetworkError>;
    fn advertise_overlay_candidates(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        peer_generation: u64,
        candidates: &[std::net::SocketAddr],
        now_ms: i64,
    ) -> Result<conductor_core::OverlayDirectCandidateAcknowledgement, OverlayNetworkError>;
    fn revoke_overlay_peer(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        now_ms: i64,
    ) -> Result<bool, OverlayNetworkError>;
    fn overlay_peer_configuration(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        now_ms: i64,
    ) -> Result<Option<conductor_core::OverlayPeerConfiguration>, OverlayNetworkError>;
    fn record_overlay_peer_observation(
        &self,
        observation: &conductor_core::OverlayPeerObservation,
    ) -> Result<(), OverlayNetworkError>;
    fn list_overlay_peers(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        limit: usize,
        now_ms: i64,
    ) -> Result<Vec<conductor_core::OverlayPeerAssignment>, OverlayNetworkError>;
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
    fn list_service_catalog(
        &self,
        identity: &ServiceIdentity,
        workers: &[WorkerStatus],
    ) -> Result<Vec<ServiceCatalogEndpoint>, ExecutionStoreError>;
    fn service_identity_exists(
        &self,
        identity: &ServiceIdentity,
    ) -> Result<bool, ExecutionStoreError>;
    fn request_worker_drain(
        &self,
        worker_id: &WorkerId,
        grace_period_ms: i64,
    ) -> Result<WorkerDrainRecord, ExecutionStoreError>;
    fn get_worker_drain(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Option<WorkerDrainRecord>, ExecutionStoreError>;
    fn list_worker_drains(&self) -> Result<Vec<WorkerDrainRecord>, ExecutionStoreError>;
    fn complete_worker_drain(
        &self,
        worker_id: &WorkerId,
        completed_at_ms: i64,
    ) -> Result<(), ExecutionStoreError>;
    fn cancel_worker_drain(&self, worker_id: &WorkerId) -> Result<bool, ExecutionStoreError>;
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
    fn reject_unaccepted_service_run(
        &self,
        instance_id: &ServiceInstanceId,
        run_id: &RunId,
        next_start_at_ms: i64,
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
    InvalidRetentionPolicy,
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
            Self::InvalidRetentionPolicy => f.write_str("manager history retention is invalid"),
            Self::TimestampUnavailable => f.write_str("system time is unavailable"),
        }
    }
}

impl std::error::Error for RepositoryOpenError {}

#[path = "structs/sqlite_job_repository.rs"]
mod sqlite_job_repository;
pub use sqlite_job_repository::{
    HistoryPruneProgress, HistoryRetentionStatus, SqliteJobRepository,
};

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
        connection
            .prepare("SELECT namespace, service_name, job_id, revision FROM service_catalog_owners LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT worker_id, requested_at_ms, deadline_at_ms, completed_at_ms FROM worker_drains LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT event_sequence, occurred_at_ms, principal, action, resource_type, resource_id, outcome, status_code FROM audit_events LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT network_domain_id, worker_address_pool, service_address_pool, network_generation FROM overlay_network_domains LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT network_domain_id, worker_id, endpoint_id, worker_address, peer_generation, expires_at_ms, state, quarantine_until_ms, direct_candidates_json, direct_candidates_expires_at_ms FROM overlay_peers LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT network_domain_id, endpoint_id, worker_id, peer_generation, retired_at_ms FROM overlay_endpoint_history LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        connection
            .prepare("SELECT network_domain_id, worker_id, peer_generation, applied_network_generation, observed_at_ms, status FROM overlay_peer_observations LIMIT 0")
            .map_err(MigrationError::Sqlite)?;
        return Ok(());
    }

    if version == 12 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_overlay_network_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 13 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        transaction
            .execute_batch(
                "ALTER TABLE overlay_peers ADD COLUMN direct_candidates_json TEXT NOT NULL DEFAULT '[]' CHECK(length(direct_candidates_json) <= 4096);
                 ALTER TABLE overlay_peers ADD COLUMN direct_candidates_expires_at_ms INTEGER CHECK(direct_candidates_expires_at_ms IS NULL OR direct_candidates_expires_at_ms >= 0);",
            )
            .map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 11 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_audit_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", 12)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 10 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_worker_drain_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", 11)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 9 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_service_catalog_owner_schema(&transaction).map_err(MigrationError::Sqlite)?;
        backfill_service_catalog_owners(&transaction)?;
        transaction
            .pragma_update(None, "user_version", 10)
            .map_err(MigrationError::Sqlite)?;
        transaction.commit().map_err(MigrationError::Sqlite)?;
        return migrate(connection);
    }

    if version == 8 {
        let transaction = connection
            .unchecked_transaction()
            .map_err(MigrationError::Sqlite)?;
        create_service_port_schema(&transaction).map_err(MigrationError::Sqlite)?;
        transaction
            .pragma_update(None, "user_version", 9)
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
    create_service_catalog_owner_schema(&transaction).map_err(MigrationError::Sqlite)?;
    create_worker_drain_schema(&transaction).map_err(MigrationError::Sqlite)?;
    create_audit_schema(&transaction).map_err(MigrationError::Sqlite)?;
    create_overlay_network_schema(&transaction).map_err(MigrationError::Sqlite)?;
    transaction
        .pragma_update(None, "user_version", SCHEMA_VERSION)
        .map_err(MigrationError::Sqlite)?;
    transaction.commit().map_err(MigrationError::Sqlite)
}

fn create_audit_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS audit_events (
             event_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
             occurred_at_ms INTEGER NOT NULL CHECK (occurred_at_ms >= 0),
             principal TEXT NOT NULL CHECK (principal IN ('operator', 'local_operator', 'worker_enrollment')),
             action TEXT NOT NULL CHECK (length(action) BETWEEN 1 AND 64),
             resource_type TEXT NOT NULL CHECK (resource_type IN ('job', 'deployment', 'worker', 'run')),
             resource_id TEXT CHECK (resource_id IS NULL OR length(resource_id) BETWEEN 1 AND 128),
             outcome TEXT NOT NULL CHECK (outcome IN ('success', 'failure')),
             status_code INTEGER NOT NULL CHECK (status_code BETWEEN 100 AND 599)
         );
         CREATE INDEX IF NOT EXISTS audit_events_by_time ON audit_events(occurred_at_ms DESC, event_sequence DESC);
         CREATE TRIGGER IF NOT EXISTS audit_events_no_update BEFORE UPDATE ON audit_events BEGIN SELECT RAISE(ABORT, 'audit events are append-only'); END;
         CREATE TRIGGER IF NOT EXISTS audit_events_no_delete BEFORE DELETE ON audit_events BEGIN SELECT RAISE(ABORT, 'audit events are append-only'); END;",
    )
}

fn create_overlay_network_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE overlay_network_domains (
             network_domain_id TEXT PRIMARY KEY NOT NULL,
             worker_address_pool TEXT NOT NULL,
             service_address_pool TEXT NOT NULL,
             network_generation INTEGER NOT NULL CHECK (network_generation > 0),
             created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0)
         );
         CREATE TABLE overlay_peers (
             network_domain_id TEXT NOT NULL REFERENCES overlay_network_domains(network_domain_id),
             worker_id TEXT NOT NULL,
             endpoint_id TEXT NOT NULL,
             worker_address INTEGER NOT NULL CHECK (worker_address BETWEEN 0 AND 4294967295),
             peer_generation INTEGER NOT NULL CHECK (peer_generation > 0),
             expires_at_ms INTEGER NOT NULL CHECK (expires_at_ms >= 0),
             state TEXT NOT NULL CHECK (state IN ('active', 'quarantined')),
             quarantine_until_ms INTEGER CHECK (quarantine_until_ms IS NULL OR quarantine_until_ms >= 0),
             direct_candidates_json TEXT NOT NULL DEFAULT '[]' CHECK(length(direct_candidates_json) <= 4096),
             direct_candidates_expires_at_ms INTEGER CHECK(direct_candidates_expires_at_ms IS NULL OR direct_candidates_expires_at_ms >= 0),
             PRIMARY KEY (network_domain_id, worker_id),
             UNIQUE (network_domain_id, endpoint_id),
             UNIQUE (network_domain_id, worker_address),
             CHECK ((state = 'active' AND quarantine_until_ms IS NULL) OR
                    (state = 'quarantined' AND quarantine_until_ms IS NOT NULL))
         );
         CREATE INDEX overlay_peers_by_domain_state ON overlay_peers(network_domain_id, state, worker_address);
         CREATE TABLE overlay_endpoint_history (
             network_domain_id TEXT NOT NULL REFERENCES overlay_network_domains(network_domain_id),
             endpoint_id TEXT NOT NULL,
             worker_id TEXT NOT NULL,
             peer_generation INTEGER NOT NULL CHECK (peer_generation > 0),
             retired_at_ms INTEGER NOT NULL CHECK (retired_at_ms >= 0),
             PRIMARY KEY (network_domain_id, endpoint_id)
         );
         CREATE TABLE overlay_peer_observations (
             network_domain_id TEXT NOT NULL,
             worker_id TEXT NOT NULL,
             peer_generation INTEGER NOT NULL CHECK (peer_generation > 0),
             applied_network_generation INTEGER NOT NULL CHECK (applied_network_generation >= 0),
             observed_at_ms INTEGER NOT NULL CHECK (observed_at_ms >= 0),
             status TEXT NOT NULL CHECK (status IN ('applied', 'rejected', 'unavailable')),
             PRIMARY KEY (network_domain_id, worker_id),
             FOREIGN KEY (network_domain_id, worker_id)
                 REFERENCES overlay_peers(network_domain_id, worker_id)
         );",
    )
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

fn create_service_catalog_owner_schema(
    transaction: &rusqlite::Transaction<'_>,
) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS service_catalog_owners (
             namespace TEXT NOT NULL,
             service_name TEXT NOT NULL,
             job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(id),
             revision INTEGER NOT NULL CHECK (revision > 0),
             FOREIGN KEY (job_id, revision) REFERENCES job_revisions(job_id, revision)
         );
         CREATE UNIQUE INDEX IF NOT EXISTS service_catalog_owner_by_identity
             ON service_catalog_owners(namespace, service_name);",
    )
}

fn create_worker_drain_schema(transaction: &rusqlite::Transaction<'_>) -> rusqlite::Result<()> {
    transaction.execute_batch(
        "CREATE TABLE IF NOT EXISTS worker_drains (
             worker_id TEXT PRIMARY KEY NOT NULL,
             requested_at_ms INTEGER NOT NULL CHECK (requested_at_ms >= 0),
             deadline_at_ms INTEGER NOT NULL CHECK (deadline_at_ms >= requested_at_ms),
             completed_at_ms INTEGER CHECK (completed_at_ms IS NULL OR completed_at_ms >= requested_at_ms)
         );",
    )
}

fn worker_drain_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<WorkerDrainRecord> {
    let raw_worker_id: String = row.get(0)?;
    let worker_id = WorkerId::new(raw_worker_id).map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(WorkerDrainRecord {
        worker_id,
        requested_at_ms: row.get(1)?,
        deadline_at_ms: row.get(2)?,
        completed_at_ms: row.get(3)?,
    })
}

fn load_worker_drain(
    connection: &Connection,
    worker_id: &WorkerId,
) -> Result<Option<WorkerDrainRecord>, ExecutionStoreError> {
    connection
        .query_row(
            "SELECT worker_id, requested_at_ms, deadline_at_ms, completed_at_ms
             FROM worker_drains WHERE worker_id = ?1",
            [worker_id.as_str()],
            worker_drain_from_row,
        )
        .optional()
        .map_err(|_| ExecutionStoreError::StorageUnavailable)
}

fn backfill_service_catalog_owners(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(), MigrationError> {
    let mut statement = transaction
        .prepare(
            "SELECT job_id, deployment_json FROM job_deployments
             WHERE deleted_at_ms IS NULL ORDER BY job_id",
        )
        .map_err(MigrationError::Sqlite)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(MigrationError::Sqlite)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(MigrationError::Sqlite)?;
    drop(statement);
    for (job_id, deployment_json) in rows {
        let deployment: JobDeployment = serde_json::from_str(&deployment_json)
            .map_err(|_| MigrationError::InvalidLegacySchema)?;
        let revision_number = i64::try_from(deployment.revision_id().revision())
            .map_err(|_| MigrationError::InvalidLegacySchema)?;
        let revision_json: String = transaction
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![job_id, revision_number],
                |row| row.get(0),
            )
            .map_err(MigrationError::Sqlite)?;
        let revision: JobRevision = serde_json::from_str(&revision_json)
            .map_err(|_| MigrationError::InvalidLegacySchema)?;
        if revision.id() != deployment.revision_id() {
            return Err(MigrationError::InvalidLegacySchema);
        }
        let Workload::Service { identity, .. } = revision.workload() else {
            continue;
        };
        let existing_owner: Option<String> = transaction
            .query_row(
                "SELECT job_id FROM service_catalog_owners
                 WHERE namespace = ?1 AND service_name = ?2",
                params![identity.namespace(), identity.name()],
                |row| row.get(0),
            )
            .optional()
            .map_err(MigrationError::Sqlite)?;
        if existing_owner
            .as_deref()
            .is_some_and(|owner| owner != job_id)
        {
            return Err(MigrationError::InvalidLegacySchema);
        }
        transaction
            .execute(
                "INSERT OR IGNORE INTO service_catalog_owners(namespace, service_name, job_id, revision)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    identity.namespace(),
                    identity.name(),
                    job_id,
                    i64::try_from(deployment.revision_id().revision())
                        .map_err(|_| MigrationError::InvalidLegacySchema)?
                ],
            )
            .map_err(MigrationError::Sqlite)?;
    }
    Ok(())
}

fn upsert_service_catalog_owner(
    transaction: &rusqlite::Transaction<'_>,
    job_id: &JobId,
    revision: &JobRevision,
) -> Result<(), ExecutionStoreError> {
    let Workload::Service { identity, .. } = revision.workload() else {
        return Err(ExecutionStoreError::InvalidRequest);
    };
    transaction
        .execute(
            "INSERT INTO service_catalog_owners(namespace, service_name, job_id, revision)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(job_id) DO UPDATE SET namespace = excluded.namespace,
                 service_name = excluded.service_name, revision = excluded.revision",
            params![
                identity.namespace(),
                identity.name(),
                job_id.as_str(),
                i64::try_from(revision.id().revision())
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            ],
        )
        .map_err(|error| {
            if matches!(error, rusqlite::Error::SqliteFailure(ref code, _)
                if code.code == rusqlite::ErrorCode::ConstraintViolation)
            {
                ExecutionStoreError::Conflict
            } else {
                ExecutionStoreError::StorageUnavailable
            }
        })?;
    Ok(())
}

fn ensure_service_identity_available(
    transaction: &rusqlite::Transaction<'_>,
    job_id: &JobId,
    identity: &ServiceIdentity,
) -> Result<(), ExecutionStoreError> {
    let owner: Option<String> = transaction
        .query_row(
            "SELECT job_id FROM service_catalog_owners
             WHERE namespace = ?1 AND service_name = ?2",
            params![identity.namespace(), identity.name()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    if owner
        .as_deref()
        .is_some_and(|owner| owner != job_id.as_str())
    {
        return Err(ExecutionStoreError::Conflict);
    }
    Ok(())
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

fn service_allocation_has_current_run(
    transaction: &rusqlite::Transaction<'_>,
    instance_id: &ServiceInstanceId,
    allocation_id: &AllocationId,
) -> Result<bool, ExecutionStoreError> {
    transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM service_attempts
                 WHERE service_instance_id = ?1 AND allocation_id = ?2
                   AND current_run_id IS NOT NULL
             )",
            params![instance_id.as_str(), allocation_id.as_str()],
            |row| row.get(0),
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)
}

fn service_runtime_pending_reason(
    transaction: &rusqlite::Transaction<'_>,
    job_id: &JobId,
    desired_replicas: u32,
) -> Result<Option<DeploymentPendingReason>, ExecutionStoreError> {
    let now = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let mut statement = transaction
        .prepare(
            "SELECT si.current_allocation_id, a.state, sa.allocation_id,
                    sa.current_run_id, sa.next_start_at_ms, sa.terminal_failure,
                    r.state
             FROM service_instances si
             LEFT JOIN allocations a ON a.allocation_id = si.current_allocation_id
             LEFT JOIN service_attempts sa ON sa.service_instance_id = si.instance_id
             LEFT JOIN runs r ON r.run_id = sa.current_run_id
             WHERE si.job_id = ?1 AND si.replica_index < ?2
             ORDER BY si.replica_index",
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let rows = statement
        .query_map(
            params![job_id.as_str(), i64::from(desired_replicas)],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<bool>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    for row in rows {
        let (
            current_allocation_id,
            allocation_state,
            attempt_allocation_id,
            current_run_id,
            next_start_at_ms,
            terminal_failure,
            run_state,
        ) = row.map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if current_allocation_id.is_none() || allocation_state.as_deref() != Some("assigned") {
            continue;
        }
        if terminal_failure == Some(true) {
            return Ok(Some(DeploymentPendingReason::ServiceRestartBudgetExhausted));
        }
        if current_run_id.is_some() {
            if attempt_allocation_id != current_allocation_id {
                return Ok(Some(DeploymentPendingReason::ServiceAttemptUnresolved));
            }
            let reason = match run_state.as_deref() {
                Some("running") => continue,
                Some("succeeded" | "failed" | "cancelled" | "timed_out" | "abandoned") => {
                    DeploymentPendingReason::ServiceRestartBackoff
                }
                Some("requested" | "starting") => DeploymentPendingReason::ServiceStarting,
                _ => DeploymentPendingReason::ServiceAttemptUnresolved,
            };
            return Ok(Some(reason));
        }
        if next_start_at_ms.is_some_and(|next_start| next_start > now) {
            return Ok(Some(DeploymentPendingReason::ServiceRestartBackoff));
        }
        return Ok(Some(DeploymentPendingReason::ServiceStarting));
    }
    Ok(None)
}

fn service_rollout_pending_reason(
    transaction: &rusqlite::Transaction<'_>,
    job_id: &JobId,
    deployment: &JobDeployment,
) -> Result<Option<DeploymentPendingReason>, ExecutionStoreError> {
    if deployment.rollout().is_none()
        || deployment.suspended()
        || deployment.desired_replicas() == 0
    {
        return Ok(None);
    }
    let mut statement = transaction
        .prepare(
            "SELECT a.revision, a.state, si.current_allocation_id,
                    sa.allocation_id, r.observation_json
             FROM service_instances si
             LEFT JOIN allocations a ON a.allocation_id = si.current_allocation_id
             LEFT JOIN service_attempts sa ON sa.service_instance_id = si.instance_id
             LEFT JOIN runs r ON r.run_id = sa.current_run_id
             WHERE si.job_id = ?1 AND si.replica_index < ?2
             ORDER BY si.replica_index",
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let rows = statement
        .query_map(
            params![job_id.as_str(), i64::from(deployment.desired_replicas())],
            |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let target_revision = i64::try_from(deployment.revision_id().revision())
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let mut pending = None;
    for row in rows {
        let (revision, allocation_state, current_allocation_id, attempt_allocation_id, observation) =
            row.map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if revision != Some(target_revision) || allocation_state.as_deref() != Some("assigned") {
            pending.get_or_insert(DeploymentPendingReason::ServiceStarting);
            continue;
        }
        if current_allocation_id != attempt_allocation_id {
            pending.get_or_insert(DeploymentPendingReason::ServiceStarting);
            continue;
        }
        let Some(observation) = observation else {
            pending.get_or_insert(DeploymentPendingReason::ServiceStarting);
            continue;
        };
        let observation: RunObservation = serde_json::from_str(&observation)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if observation
            .readiness()
            .is_some_and(|readiness| readiness != ReadinessState::Ready)
        {
            return Ok(Some(DeploymentPendingReason::ServiceReadinessPending));
        }
        if observation.state() == RunState::Unknown {
            return Ok(Some(DeploymentPendingReason::ServiceAttemptUnresolved));
        }
        if observation.state() != RunState::Running {
            pending.get_or_insert(DeploymentPendingReason::ServiceStarting);
        }
    }
    Ok(pending)
}

#[path = "structs/reserved_usage.rs"]
mod reserved_usage;
use reserved_usage::ReservedUsage;

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

        if allocation
            .as_ref()
            .is_some_and(|record| record.allocation.job_revision_id() != revision.id())
        {
            let current_id = current_allocation_id
                .as_deref()
                .map(AllocationId::new)
                .transpose()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            if let Some(reusable) = load_reusable_service_allocation(
                transaction,
                &instance_id,
                revision.id(),
                current_id.as_ref(),
            )? {
                if let Some(current_id) = current_id.as_ref()
                    && let Some(mut stale) = load_allocation(transaction, current_id.as_str())?
                {
                    let state = stale.allocation.state();
                    if state != AllocationState::Released
                        && (state == AllocationState::Pending
                            || !service_allocation_has_current_run(
                                transaction,
                                &instance_id,
                                stale.allocation.id(),
                            )?)
                    {
                        stale
                            .allocation
                            .release()
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                        persist_allocation(transaction, &stale)?;
                    }
                }
                set_current_allocation(
                    transaction,
                    job_id,
                    replica_index,
                    reusable.allocation.id(),
                )?;
                allocation = Some(reusable);
            }
        }

        let mut needs_replacement = allocation.as_ref().is_none_or(|record| {
            record.allocation.job_revision_id() != revision.id()
                || record.allocation.state() == AllocationState::Released
        });
        if let Some(record) = allocation.as_mut()
            && record.allocation.state() == AllocationState::Assigned
        {
            let assigned_worker = record
                .allocation
                .assigned_worker_id()
                .and_then(|id| workers.iter().find(|worker| worker.worker_id() == id));
            let worker_is_fresh = assigned_worker
                .is_some_and(|worker| worker.is_valid() && worker.fresh() && !worker.degraded());
            if !worker_is_fresh {
                record
                    .allocation
                    .mark_uncertain()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                persist_allocation(transaction, record)?;
                needs_replacement = true;
            } else if assigned_worker.is_some_and(WorkerStatus::draining) {
                // A draining worker is still known to be alive, so retain the
                // old allocation until replacement placement succeeds. The
                // service reconciler will stop its run only after it sees an
                // assigned replacement (or the recorded drain deadline).
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
            if let Some(record) = allocation.as_mut() {
                let state = record.allocation.state();
                if state != AllocationState::Released
                    && (state == AllocationState::Pending
                        || !service_allocation_has_current_run(
                            transaction,
                            &instance_id,
                            record.allocation.id(),
                        )?)
                {
                    record
                        .allocation
                        .release()
                        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                    persist_allocation(transaction, record)?;
                }
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

    // Slots above the desired replica count retain stable identity. Release
    // allocations that have no run attached to them, even if they were already
    // assigned: the scheduler may be scaled down after placement but before
    // `begin_service_run` creates a service-attempt row. Keep a reservation
    // only while its own allocation has an unresolved run.
    let mut statement = transaction
        .prepare(
            "SELECT si.replica_index, si.instance_id, si.current_allocation_id,
                    EXISTS(
                        SELECT 1 FROM service_attempts sa
                        WHERE sa.service_instance_id = si.instance_id
                          AND sa.allocation_id = si.current_allocation_id
                          AND sa.current_run_id IS NOT NULL
                    )
             FROM service_instances si
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
                    row.get::<_, bool>(3)?,
                ))
            },
        )
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    drop(statement);
    for (replica_index, _, current_allocation_id, has_current_run) in retired {
        if let Some(allocation_id) = current_allocation_id {
            if let Some(mut record) = load_allocation(transaction, &allocation_id)? {
                let state = record.allocation.state();
                if state != AllocationState::Released
                    && (state == AllocationState::Pending || !has_current_run)
                {
                    record
                        .allocation
                        .release()
                        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                    persist_allocation(transaction, &record)?;
                }
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

    let runtime_pending_reason = if desired_replicas == 0 || first_pending_reason.is_some() {
        None
    } else {
        service_runtime_pending_reason(transaction, job_id, desired_replicas)?
    };
    let rollout_pending_reason = if desired_replicas == 0 || first_pending_reason.is_some() {
        None
    } else {
        service_rollout_pending_reason(transaction, job_id, deployment)?
    };
    let pending_reason = first_pending_reason.or_else(|| {
        if deployment.rollout().is_some() {
            rollout_pending_reason.or(runtime_pending_reason)
        } else {
            runtime_pending_reason
        }
    });
    let (status, pending_reason, pending_message) = if deployment.suspended() {
        (DeploymentStatus::Suspended, None, None)
    } else if desired_replicas == 0 || pending_reason.is_none() {
        (DeploymentStatus::Ready, None, None)
    } else {
        let reason = pending_reason.ok_or(ExecutionStoreError::StorageUnavailable)?;
        (
            DeploymentStatus::Pending,
            Some(reason),
            Some(pending_message(reason).to_owned()),
        )
    };
    let rollout = if pending_reason.is_none() {
        None
    } else {
        deployment.rollout().cloned()
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
    .map(|updated| updated.with_rollout(rollout))
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

fn load_reusable_service_allocation(
    transaction: &rusqlite::Transaction<'_>,
    instance_id: &ServiceInstanceId,
    revision_id: &JobRevisionId,
    current_allocation_id: Option<&AllocationId>,
) -> Result<Option<AllocationRecord>, ExecutionStoreError> {
    let revision = i64::try_from(revision_id.revision())
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    let allocation_id = transaction
        .query_row(
            "SELECT a.allocation_id FROM allocations a
             LEFT JOIN service_attempts sa ON sa.service_instance_id = a.service_instance_id
             WHERE a.service_instance_id = ?1 AND a.revision = ?2
               AND a.state IN ('assigned', 'uncertain')
               AND (?3 IS NULL OR a.allocation_id <> ?3)
             ORDER BY CASE WHEN sa.allocation_id = a.allocation_id
                            AND sa.current_run_id IS NOT NULL THEN 0 ELSE 1 END,
                      a.created_at_ms DESC
             LIMIT 1",
            params![
                instance_id.as_str(),
                revision,
                current_allocation_id.map(AllocationId::as_str)
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
    allocation_id
        .as_deref()
        .map(|allocation_id| load_allocation(transaction, allocation_id))
        .transpose()
        .map(Option::flatten)
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
        DeploymentPendingReason::ServiceStarting => {
            "one or more assigned service replicas have not reached the running state"
        }
        DeploymentPendingReason::ServiceRestartBackoff => {
            "one or more service replicas are waiting for their restart backoff"
        }
        DeploymentPendingReason::ServiceRestartBudgetExhausted => {
            "one or more service replicas exhausted their restart retry budget"
        }
        DeploymentPendingReason::ServiceAttemptUnresolved => {
            "one or more service attempts have an unresolved worker outcome"
        }
        DeploymentPendingReason::ServiceReadinessPending => {
            "replacement service is running but has not passed its readiness gate"
        }
        DeploymentPendingReason::RolloutProgressDeadlineExceeded => {
            "revision rollout exceeded its progress deadline and is rolling back"
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

fn now_ms() -> Result<i64, RepositoryOpenError> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryOpenError::TimestampUnavailable)?
        .as_millis();
    i64::try_from(milliseconds).map_err(|_| RepositoryOpenError::TimestampUnavailable)
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
    InvalidDrainRequest,
    ResolutionConflict,
    LogsNotFound,
    LogsPruned,
    StorageUnavailable,
}

pub const MAX_RUNS_RESPONSE: usize = 100;

#[path = "structs/job_service.rs"]
mod job_service;
pub use job_service::JobService;

#[path = "structs/manager_telemetry.rs"]
mod manager_telemetry;
use manager_telemetry::ManagerTelemetry;

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
        RunStoreError::WorkerDraining => ManagerError::WorkerDraining,
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

#[cfg(test)]
mod audit_metrics_tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{AuditEventInput, JobRepository, SqliteJobRepository};
    use conductor_core::WorkerId;

    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    include!("structs/test_database.rs");

    impl TestDatabase {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "conductor-audit-{}-{nonce}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir(&directory).expect("create temporary repository directory");
            Self(directory.join("manager.sqlite3"))
        }

        fn open(&self) -> SqliteJobRepository {
            SqliteJobRepository::open(&self.0).expect("open test repository")
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            if let Some(directory) = self.0.parent() {
                let _ = std::fs::remove_dir_all(directory);
            }
        }
    }

    fn audit_input(action: &str, outcome: &str, occurred_at_ms: i64) -> AuditEventInput {
        AuditEventInput {
            principal: "operator".to_owned(),
            action: action.to_owned(),
            resource_type: "worker".to_owned(),
            resource_id: Some("worker-a".to_owned()),
            outcome: outcome.to_owned(),
            status_code: if outcome == "failure" { 500 } else { 200 },
            occurred_at_ms,
        }
    }

    #[test]
    fn audit_records_are_bounded_redacted_and_append_only() {
        let database = TestDatabase::new();
        let repository = database.open();
        repository
            .record_audit_event(&audit_input("worker.drain.request", "failure", 1))
            .expect("record bounded audit intent");
        repository
            .record_audit_event(&audit_input("worker.drain.request", "success", 2))
            .expect("record bounded audit result");
        assert!(
            repository
                .record_audit_event(&audit_input("rm -rf secret-command", "success", 3))
                .is_err()
        );
        assert!(repository.list_audit_events(1_001).is_err());

        let events = JobRepository::list_audit_events(&repository, 10).expect("read audit events");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 2);
        assert_eq!(events[0].outcome, "success");
        assert_eq!(events[1].outcome, "failure");
        assert!(
            events
                .iter()
                .all(|event| event.resource_id.as_deref() == Some("worker-a"))
        );
        let serialized = serde_json::to_string(&events).expect("serialize safe audit fields");
        assert!(!serialized.contains("secret-command"));
        assert!(
            repository
                .connection
                .lock()
                .expect("lock test connection")
                .execute("DELETE FROM audit_events", [])
                .is_err()
        );
    }

    #[test]
    fn worker_enrollment_and_revocation_are_audited_without_credentials() {
        let database = TestDatabase::new();
        let repository = database.open();
        let worker = WorkerId::new("worker-a").expect("valid worker ID");
        let credential_hash = [17_u8; 32];
        let certificate_fingerprint = [23_u8; 32];
        repository
            .create_enrollment_credential(&credential_hash, &worker, 10_000)
            .expect("create enrollment credential");
        repository
            .enroll_worker_identity(
                &credential_hash,
                &certificate_fingerprint,
                "certificate-1",
                1,
                10_000,
                2,
            )
            .expect("enroll worker");
        repository
            .renew_worker_identity(
                &certificate_fingerprint,
                &[29_u8; 32],
                "certificate-2",
                2,
                20_000,
                3,
            )
            .expect("renew worker certificate");
        repository
            .revoke_worker_identity(&worker, 4)
            .expect("revoke worker");

        let events = JobRepository::list_audit_events(&repository, 10).expect("read audit events");
        assert_eq!(events.len(), 4);
        assert_eq!(events[0].action, "worker.revoke");
        assert_eq!(events[1].action, "worker.certificate.renew");
        assert_eq!(events[1].principal, "worker_enrollment");
        assert_eq!(events[2].action, "worker.enroll");
        assert_eq!(events[3].action, "worker.enrollment_credential.create");
        assert!(events.iter().all(|event| {
            event.resource_id.as_deref() == Some("worker-a")
                && event.status_code >= 100
                && event.status_code <= 599
        }));
        let serialized = serde_json::to_string(&events).expect("serialize safe audit fields");
        assert!(!serialized.contains("111111"));
        assert!(!serialized.contains("171717"));
    }

    #[test]
    fn schema_eleven_migrates_to_audit_storage() {
        let database = TestDatabase::new();
        drop(database.open());
        {
            let connection = rusqlite::Connection::open(&database.0).expect("open old database");
            connection
                .execute_batch(
                    "DROP TRIGGER audit_events_no_update;
                     DROP TRIGGER audit_events_no_delete;
                     DROP INDEX audit_events_by_time;
                     DROP TABLE audit_events;
                     DROP TABLE overlay_peer_observations;
                     DROP TABLE overlay_endpoint_history;
                     DROP TABLE overlay_peers;
                     DROP TABLE overlay_network_domains;
                     PRAGMA user_version = 11;",
                )
                .expect("restore schema 11 shape");
        }
        let repository = database.open();
        assert_eq!(
            repository
                .connection
                .lock()
                .expect("lock migrated connection")
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .expect("read schema version"),
            super::CURRENT_SCHEMA_VERSION
        );
        repository
            .record_audit_event(&AuditEventInput {
                principal: "operator".to_owned(),
                action: "deployment.scale".to_owned(),
                resource_type: "deployment".to_owned(),
                resource_id: Some("job-a".to_owned()),
                outcome: "success".to_owned(),
                status_code: 202,
                occurred_at_ms: 10,
            })
            .expect("write event after schema migration");
        assert_eq!(
            JobRepository::list_audit_events(&repository, 10)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn schema_twelve_migrates_overlay_storage_transactionally() {
        let database = TestDatabase::new();
        drop(database.open());
        {
            let connection = rusqlite::Connection::open(&database.0).expect("open old database");
            connection
                .execute_batch(
                    "DROP TABLE overlay_peer_observations;
                     DROP TABLE overlay_endpoint_history;
                     DROP TABLE overlay_peers;
                     DROP TABLE overlay_network_domains;
                     PRAGMA user_version = 12;",
                )
                .expect("restore schema 12 shape");
        }
        let repository = database.open();
        let schema_version: i64 = repository
            .connection
            .lock()
            .expect("lock migrated connection")
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read schema version");
        assert_eq!(schema_version, super::CURRENT_SCHEMA_VERSION);
        assert_eq!(
            repository
                .get_overlay_network_domain(
                    &conductor_core::NetworkDomainId::new("missing").expect("valid domain ID")
                )
                .expect("query migrated table"),
            None
        );
    }
}

#[cfg(test)]
mod overlay_network_tests {
    use std::sync::Arc;
    use std::thread;

    use super::{JobRepository, OverlayNetworkError, SqliteJobRepository};
    use conductor_core::{
        NetworkDomainId, OverlayEndpointId, OverlayNetworkDomainConfig, OverlayPeerObservation,
        OverlayPeerObservationStatus, OverlayPeerState, WorkerId,
    };

    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    include!("structs/test_database.rs");

    impl TestDatabase {
        fn new() -> Self {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos();
            let sequence = SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "conductor-overlay-{}-{nonce}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir(&directory).expect("create temporary repository directory");
            Self(directory.join("manager.sqlite3"))
        }

        fn open(&self) -> SqliteJobRepository {
            SqliteJobRepository::open(&self.0).expect("open test repository")
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            if let Some(directory) = self.0.parent() {
                let _ = std::fs::remove_dir_all(directory);
            }
        }
    }

    fn endpoint(index: u8) -> OverlayEndpointId {
        OverlayEndpointId::new(format!("{index:02x}").repeat(32)).expect("valid endpoint ID")
    }

    fn enroll(repository: &SqliteJobRepository, id: &str, index: u8) -> WorkerId {
        let worker = WorkerId::new(id).expect("valid worker ID");
        let token = [index; 32];
        let fingerprint = [index.wrapping_add(100); 32];
        repository
            .create_enrollment_credential(&token, &worker, 100_000)
            .expect("create enrollment credential");
        repository
            .enroll_worker_identity(
                &token,
                &fingerprint,
                &format!("certificate-{id}"),
                1,
                100_000,
                2,
            )
            .expect("enroll worker identity");
        worker
    }

    fn domain() -> OverlayNetworkDomainConfig {
        OverlayNetworkDomainConfig {
            network_domain_id: NetworkDomainId::new("overlay-a").expect("valid domain ID"),
            worker_address_pool: "10.88.0.0/24".to_owned(),
            service_address_pool: "10.88.1.0/24".to_owned(),
        }
    }

    #[test]
    fn domain_pools_reject_public_or_overlapping_ranges() {
        let database = TestDatabase::new();
        let repository = database.open();
        let mut invalid = domain();
        invalid.worker_address_pool = "192.0.2.0/24".to_owned();
        assert_eq!(
            repository.create_overlay_network_domain(&invalid, 1),
            Err(OverlayNetworkError::InvalidAddressPool)
        );
        let mut overlapping = domain();
        overlapping.service_address_pool = "10.88.0.128/25".to_owned();
        assert_eq!(
            repository.create_overlay_network_domain(&overlapping, 1),
            Err(OverlayNetworkError::AddressPoolOverlap)
        );
        repository
            .create_overlay_network_domain(&domain(), 1)
            .expect("create domain");
        let mut second = domain();
        second.network_domain_id = NetworkDomainId::new("overlay-b").expect("valid domain ID");
        second.worker_address_pool = "10.88.0.0/25".to_owned();
        second.service_address_pool = "10.88.2.0/24".to_owned();
        assert_eq!(
            repository.create_overlay_network_domain(&second, 2),
            Err(OverlayNetworkError::AddressPoolOverlap)
        );
    }

    #[test]
    fn allocation_is_durable_bounded_and_identity_replay_is_rejected() {
        let database = TestDatabase::new();
        let repository = database.open();
        let first = enroll(&repository, "worker-a", 1);
        let second = enroll(&repository, "worker-b", 2);
        let domain = domain();
        repository
            .create_overlay_network_domain(&domain, 1)
            .expect("create domain");
        let first_assignment = repository
            .register_overlay_peer(&domain.network_domain_id, &first, &endpoint(1), 60_000, 2)
            .expect("assign first peer");
        let second_assignment = repository
            .register_overlay_peer(&domain.network_domain_id, &second, &endpoint(2), 60_000, 3)
            .expect("assign second peer");
        assert_eq!(first_assignment.worker_address.to_string(), "10.88.0.1");
        assert_eq!(second_assignment.worker_address.to_string(), "10.88.0.2");
        assert_eq!(
            second_assignment.network_generation,
            first_assignment.network_generation + 1
        );
        let config = repository
            .overlay_peer_configuration(&domain.network_domain_id, &first, 10)
            .expect("read peer config")
            .expect("active peer has config");
        assert_eq!(config.peers.len(), 1);
        assert_eq!(config.peers[0].worker_id, second);
        drop(repository);

        let repository = database.open();
        let restored = repository
            .register_overlay_peer(&domain.network_domain_id, &first, &endpoint(1), 70_000, 20)
            .expect("same identity renews after restart");
        assert_eq!(restored.worker_address, first_assignment.worker_address);
        assert_eq!(restored.peer_generation, first_assignment.peer_generation);
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &second,
                &endpoint(1),
                70_000,
                21
            ),
            Err(OverlayNetworkError::EndpointIdentityConflict)
        );
        repository
            .revoke_overlay_peer(&domain.network_domain_id, &first, 30)
            .expect("revoke peer");
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &first,
                &endpoint(1),
                70_000,
                31
            ),
            Err(OverlayNetworkError::PeerQuarantined)
        );
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &second,
                &endpoint(1),
                70_000,
                32
            ),
            Err(OverlayNetworkError::EndpointIdentityReplay)
        );
    }

    #[test]
    fn endpoint_rotation_preserves_address_fences_generation_and_retires_old_key() {
        let database = TestDatabase::new();
        let repository = database.open();
        let worker = enroll(&repository, "worker-a", 1);
        let other_worker = enroll(&repository, "worker-b", 2);
        let domain = domain();
        repository
            .create_overlay_network_domain(&domain, 1)
            .expect("create domain");
        repository
            .register_overlay_peer(
                &domain.network_domain_id,
                &other_worker,
                &endpoint(2),
                60_000,
                2,
            )
            .expect("assign another active peer");
        let old_assignment = repository
            .register_overlay_peer(&domain.network_domain_id, &worker, &endpoint(1), 60_000, 2)
            .expect("assign original endpoint");
        let candidate: std::net::SocketAddr = "192.168.50.12:42500"
            .parse()
            .expect("valid direct candidate");
        repository
            .advertise_overlay_candidates(
                &domain.network_domain_id,
                &worker,
                old_assignment.peer_generation,
                &[candidate],
                3,
            )
            .expect("advertise original endpoint candidate");

        let rotated = repository
            .register_overlay_peer(&domain.network_domain_id, &worker, &endpoint(3), 70_000, 4)
            .expect("rotate endpoint for authenticated worker");
        assert_eq!(rotated.worker_address, old_assignment.worker_address);
        assert_eq!(rotated.peer_generation, old_assignment.peer_generation + 1);
        assert!(rotated.network_generation > old_assignment.network_generation);
        let configuration = repository
            .overlay_peer_configuration(&domain.network_domain_id, &other_worker, 5)
            .expect("read configuration for another peer")
            .expect("other peer remains active");
        let rotated_peer = configuration
            .peers
            .iter()
            .find(|peer| peer.worker_id == worker)
            .expect("rotated peer remains assigned");
        assert_eq!(rotated_peer.endpoint_id, endpoint(3));
        assert!(rotated_peer.direct_candidates.is_empty());
        assert_eq!(rotated_peer.direct_candidates_expires_at_ms, None);
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &worker,
                &endpoint(1),
                70_000,
                6
            ),
            Err(OverlayNetworkError::EndpointIdentityReplay)
        );
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &other_worker,
                &endpoint(3),
                70_000,
                7
            ),
            Err(OverlayNetworkError::EndpointIdentityConflict)
        );
    }

    #[test]
    fn expired_offline_authorization_stays_expired_across_repository_restart() {
        let database = TestDatabase::new();
        let repository = database.open();
        let worker = enroll(&repository, "worker-a", 1);
        let domain = domain();
        repository
            .create_overlay_network_domain(&domain, 1)
            .expect("create domain");
        let assignment = repository
            .register_overlay_peer(&domain.network_domain_id, &worker, &endpoint(1), 1_000, 2)
            .expect("assign bounded peer authorization");
        drop(repository);

        let repository = database.open();
        assert!(
            repository
                .overlay_peer_configuration(&domain.network_domain_id, &worker, 1_000)
                .expect("read expired peer configuration")
                .is_none()
        );
        let peers = repository
            .list_overlay_peers(&domain.network_domain_id, 10, 1_000)
            .expect("read expired peer state");
        assert_eq!(peers[0].state, OverlayPeerState::Quarantined);
        assert_eq!(peers[0].quarantine_until_ms, Some(31_000));
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &worker,
                &endpoint(1),
                60_000,
                1_001
            ),
            Err(OverlayNetworkError::PeerQuarantined)
        );
        drop(repository);

        let repository = database.open();
        let replacement = repository
            .register_overlay_peer(
                &domain.network_domain_id,
                &worker,
                &endpoint(2),
                60_000,
                31_001,
            )
            .expect("new endpoint can rejoin after durable quarantine");
        assert_eq!(replacement.worker_address, assignment.worker_address);
        assert!(replacement.peer_generation > assignment.peer_generation);
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &worker,
                &endpoint(1),
                60_000,
                31_002
            ),
            Err(OverlayNetworkError::EndpointIdentityReplay)
        );
    }

    #[test]
    fn direct_candidates_are_generation_fenced_and_distributed_in_peer_maps() {
        let database = TestDatabase::new();
        let repository = database.open();
        let first = enroll(&repository, "worker-a", 1);
        let second = enroll(&repository, "worker-b", 2);
        let domain = domain();
        repository
            .create_overlay_network_domain(&domain, 1)
            .expect("create domain");
        repository
            .register_overlay_peer(&domain.network_domain_id, &first, &endpoint(1), 60_000, 2)
            .expect("assign first peer");
        let assignment = repository
            .register_overlay_peer(&domain.network_domain_id, &second, &endpoint(2), 60_000, 2)
            .expect("assign peer");
        let candidates = ["192.168.50.12:42500".parse().expect("candidate address")];
        let acknowledgement = repository
            .advertise_overlay_candidates(
                &domain.network_domain_id,
                &second,
                assignment.peer_generation,
                &candidates,
                3,
            )
            .expect("accept authenticated candidate advertisement");
        assert_eq!(acknowledgement.peer_generation, assignment.peer_generation);
        assert_eq!(acknowledgement.candidates_expires_at_ms, Some(60_000));
        assert_eq!(
            acknowledgement.network_generation,
            assignment.network_generation + 1
        );
        let config = repository
            .overlay_peer_configuration(&domain.network_domain_id, &first, 4)
            .expect("read peer map")
            .expect("first worker configuration exists");
        assert_eq!(config.peers[0].direct_candidates, candidates);
        assert_eq!(
            config.peers[0].direct_candidates_expires_at_ms,
            Some(60_000)
        );

        assert_eq!(
            repository.advertise_overlay_candidates(
                &domain.network_domain_id,
                &second,
                assignment.peer_generation - 1,
                &candidates,
                5,
            ),
            Err(OverlayNetworkError::StaleGeneration)
        );
        let invalid = ["127.0.0.1:42500".parse().expect("loopback address")];
        assert_eq!(
            repository.advertise_overlay_candidates(
                &domain.network_domain_id,
                &second,
                assignment.peer_generation,
                &invalid,
                5,
            ),
            Err(OverlayNetworkError::InvalidDirectCandidates)
        );
    }

    #[test]
    fn concurrent_enrollment_allocates_unique_addresses() {
        let database = TestDatabase::new();
        let repository = Arc::new(database.open());
        let domain = domain();
        repository
            .create_overlay_network_domain(&domain, 1)
            .expect("create domain");
        let workers = (0..8)
            .map(|index| enroll(&repository, &format!("worker-{index}"), index + 1))
            .collect::<Vec<_>>();
        let threads = workers
            .into_iter()
            .enumerate()
            .map(|(index, worker)| {
                let repository = Arc::clone(&repository);
                let domain_id = domain.network_domain_id.clone();
                thread::spawn(move || {
                    repository
                        .register_overlay_peer(
                            &domain_id,
                            &worker,
                            &endpoint(index as u8 + 1),
                            60_000,
                            2,
                        )
                        .expect("allocate concurrently")
                })
            })
            .collect::<Vec<_>>();
        let mut addresses = threads
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .expect("allocation thread completes")
                    .worker_address
            })
            .collect::<Vec<_>>();
        addresses.sort_unstable();
        addresses.dedup();
        assert_eq!(addresses.len(), 8);
    }

    #[test]
    fn observations_reject_stale_or_future_generations() {
        let database = TestDatabase::new();
        let repository = database.open();
        let worker = enroll(&repository, "worker-a", 1);
        let domain = domain();
        repository
            .create_overlay_network_domain(&domain, 1)
            .expect("create domain");
        let assignment = repository
            .register_overlay_peer(&domain.network_domain_id, &worker, &endpoint(1), 60_000, 2)
            .expect("assign peer");
        let observation = OverlayPeerObservation {
            network_domain_id: domain.network_domain_id.clone(),
            worker_id: worker.clone(),
            peer_generation: assignment.peer_generation,
            applied_network_generation: assignment.network_generation,
            observed_at_ms: 3,
            status: OverlayPeerObservationStatus::Applied,
        };
        repository
            .record_overlay_peer_observation(&observation)
            .expect("record current observation");
        let mut stale = observation.clone();
        stale.observed_at_ms = 2;
        assert_eq!(
            repository.record_overlay_peer_observation(&stale),
            Err(OverlayNetworkError::StaleGeneration)
        );
        let mut future = observation;
        future.applied_network_generation += 10;
        assert_eq!(
            repository.record_overlay_peer_observation(&future),
            Err(OverlayNetworkError::ObservationConflict)
        );
        assert_eq!(
            repository
                .list_overlay_peers(&domain.network_domain_id, 10, 10)
                .expect("list peer status")[0]
                .state,
            OverlayPeerState::Active
        );
    }

    #[test]
    fn expired_peer_is_retired_then_its_address_is_reused_after_quarantine() {
        let database = TestDatabase::new();
        let repository = database.open();
        let first = enroll(&repository, "worker-a", 1);
        let second = enroll(&repository, "worker-b", 2);
        let domain = OverlayNetworkDomainConfig {
            network_domain_id: NetworkDomainId::new("overlay-small").expect("valid domain ID"),
            worker_address_pool: "10.89.0.0/30".to_owned(),
            service_address_pool: "10.89.1.0/30".to_owned(),
        };
        repository
            .create_overlay_network_domain(&domain, 1)
            .expect("create domain");
        repository
            .register_overlay_peer(&domain.network_domain_id, &first, &endpoint(1), 1_000, 2)
            .expect("assign first peer");

        let peers = repository
            .list_overlay_peers(&domain.network_domain_id, 10, 1_000)
            .expect("list expired peer");
        assert_eq!(peers[0].state, OverlayPeerState::Quarantined);
        assert_eq!(peers[0].quarantine_until_ms, Some(31_000));
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &first,
                &endpoint(1),
                60_000,
                1_001
            ),
            Err(OverlayNetworkError::PeerQuarantined)
        );
        let second_assignment = repository
            .register_overlay_peer(
                &domain.network_domain_id,
                &second,
                &endpoint(2),
                60_000,
                31_001,
            )
            .expect("reuse released address after quarantine");
        assert_eq!(second_assignment.worker_address.to_string(), "10.89.0.1");
        assert_eq!(
            repository.register_overlay_peer(
                &domain.network_domain_id,
                &first,
                &endpoint(1),
                60_000,
                31_002
            ),
            Err(OverlayNetworkError::EndpointIdentityReplay)
        );
    }

    #[test]
    fn worker_certificate_revocation_quarantines_overlay_membership() {
        let database = TestDatabase::new();
        let repository = database.open();
        let worker = enroll(&repository, "worker-a", 1);
        let domain = domain();
        repository
            .create_overlay_network_domain(&domain, 1)
            .expect("create domain");
        repository
            .register_overlay_peer(&domain.network_domain_id, &worker, &endpoint(1), 60_000, 2)
            .expect("assign peer");
        assert!(
            repository
                .revoke_worker_identity(&worker, 3)
                .expect("revoke worker identity")
        );
        assert!(
            repository
                .overlay_peer_configuration(&domain.network_domain_id, &worker, 4)
                .expect("read revoked peer config")
                .is_none()
        );
        let peers = repository
            .list_overlay_peers(&domain.network_domain_id, 10, 4)
            .expect("list peer state");
        assert_eq!(peers[0].state, OverlayPeerState::Quarantined);
        assert_eq!(peers[0].quarantine_until_ms, Some(90_000));
    }
}
