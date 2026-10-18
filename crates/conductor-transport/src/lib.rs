//! Bounded, worker-initiated HTTP transport for manager/worker commands.

mod identity;
pub use identity::{
    CertificateAuthority, IdentityError, IssuedServerIdentity, IssuedWorkerIdentity,
    WorkerEnrollmentService, WorkerKeyMaterial,
};

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::Router;
use axum::extract::{DefaultBodyLimit, Extension, State};
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use base64::Engine;
use conductor_core::{
    CommandId, InspectRun, LogStream, MAX_WORKER_CAPACITY, ProtocolCapability,
    ProtocolNegotiationError, ProtocolOffer, ReadRunLogs, ResolveRun, RunId, RunLogChunk,
    RunLogStatus, RunObservation, StartRun, StopRun, WorkerDescriptor, WorkerId,
    WorkerInventoryPage, WorkerMetadata, WorkerStatus,
};
use conductor_manager::{CommandJournal, CommandStoreError, WorkerPort, WorkerPortError};
use conductor_worker::{StoredWorkerReport, WorkerError, WorkerService};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, Semaphore, watch};

pub const MAX_TRANSPORT_BODY_BYTES: usize = 128 * 1024;
const MAX_REGISTERED_WORKERS: usize = 1_024;
const MAX_PENDING_COMMANDS: usize = 32;
const MAX_RECENT_REPORTS: usize = 64;
const POLL_TIMEOUT: Duration = Duration::from_secs(25);
const COMMAND_REDELIVERY_INTERVAL: Duration = Duration::from_secs(2);
const COMMAND_RESPONSE_TIMEOUT: Duration = Duration::from_secs(8);
const WORKER_FRESHNESS_TIMEOUT: Duration = Duration::from_secs(45);
const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(35);
const BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

pub use conductor_core::{CommandEnvelope, WorkerCommand};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum WorkerResult {
    Observation(RunObservation),
    Inspection(Option<RunObservation>),
    Logs(TransportLogChunk),
    Error(RemoteWorkerError),
}

#[path = "structs/transport_log_chunk.rs"]
mod transport_log_chunk;
pub use transport_log_chunk::TransportLogChunk;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteWorkerError {
    CapacityReached,
    RunConflict,
    ShuttingDown,
    LogsNotFound,
    LogsPruned,
    Unavailable,
}

impl From<WorkerError> for RemoteWorkerError {
    fn from(error: WorkerError) -> Self {
        match error {
            WorkerError::CapacityReached => Self::CapacityReached,
            WorkerError::RunConflict => Self::RunConflict,
            WorkerError::ShuttingDown => Self::ShuttingDown,
            WorkerError::LogsNotFound => Self::LogsNotFound,
            WorkerError::LogsPruned => Self::LogsPruned,
            WorkerError::WorkerMismatch | WorkerError::StorageUnavailable => Self::Unavailable,
        }
    }
}

impl From<RemoteWorkerError> for WorkerPortError {
    fn from(error: RemoteWorkerError) -> Self {
        match error {
            RemoteWorkerError::CapacityReached => Self::CapacityReached,
            RemoteWorkerError::RunConflict => Self::RunConflict,
            RemoteWorkerError::ShuttingDown => Self::ShuttingDown,
            RemoteWorkerError::LogsNotFound => Self::LogsNotFound,
            RemoteWorkerError::LogsPruned => Self::LogsPruned,
            RemoteWorkerError::Unavailable => Self::Unavailable,
        }
    }
}

#[path = "structs/worker_report.rs"]
mod worker_report;
pub use worker_report::WorkerReport;

#[path = "structs/worker_poll_request.rs"]
mod worker_poll_request;
pub use worker_poll_request::WorkerPollRequest;

#[path = "structs/worker_poll_response.rs"]
mod worker_poll_response;
pub use worker_poll_response::WorkerPollResponse;

#[path = "structs/worker_inventory_acknowledgement.rs"]
mod worker_inventory_acknowledgement;
pub use worker_inventory_acknowledgement::WorkerInventoryAcknowledgement;

#[path = "structs/report_acknowledgement.rs"]
mod report_acknowledgement;
pub use report_acknowledgement::ReportAcknowledgement;

#[path = "structs/worker_report_request.rs"]
mod worker_report_request;
pub use worker_report_request::WorkerReportRequest;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportError {
    InvalidEndpoint,
    InvalidIdentity,
    Protocol(ProtocolNegotiationError),
    Unavailable,
    QueueFull,
    WorkerConflict,
    InvalidCommand,
    MalformedResponse,
    Http,
    ShuttingDown,
    WorkerLimitReached,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidEndpoint => {
                "worker transport endpoint must be an HTTPS URL without credentials or query data"
            }
            Self::InvalidIdentity => "worker transport client identity is invalid",
            Self::Protocol(_) => "worker and manager do not share a supported transport protocol",
            Self::Unavailable => "remote worker is unavailable",
            Self::QueueFull => "remote worker command queue is full",
            Self::WorkerConflict => {
                "worker identity or incarnation conflicts with the registered session"
            }
            Self::InvalidCommand => "remote worker returned a mismatched command result",
            Self::MalformedResponse => "remote worker returned an invalid transport response",
            Self::Http => "remote worker transport request failed",
            Self::ShuttingDown => "worker transport is shutting down",
            Self::WorkerLimitReached => "worker transport worker registry is full",
        })
    }
}

impl std::error::Error for TransportError {}

#[path = "structs/worker_transport_hub.rs"]
mod worker_transport_hub;
pub use worker_transport_hub::WorkerTransportHub;

/// Authentication integration point. Implementations must derive the worker
/// identity from verified transport credentials, not request JSON.
pub trait WorkerAuthenticator: Send + Sync + 'static {
    fn authenticate(&self, request: &Request<axum::body::Body>) -> Option<WorkerId>;
}

#[path = "structs/authenticated_worker.rs"]
mod authenticated_worker;
use authenticated_worker::AuthenticatedWorker;

async fn authenticate_worker(
    State(authenticator): State<Arc<dyn WorkerAuthenticator>>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(worker_id) = authenticator.authenticate(&request) else {
        return (StatusCode::UNAUTHORIZED, "worker authentication required").into_response();
    };
    request
        .extensions_mut()
        .insert(AuthenticatedWorker(worker_id));
    next.run(request).await
}

#[path = "structs/hub_state.rs"]
mod hub_state;
use hub_state::HubState;

async fn poll_worker(
    State(state): State<HubState>,
    Extension(authenticated): Extension<AuthenticatedWorker>,
    Json(request): Json<WorkerPollRequest>,
) -> Response {
    if request.worker_id != authenticated.0 {
        return (StatusCode::FORBIDDEN, "worker identity mismatch").into_response();
    }
    let session = match state.0.session(&authenticated.0) {
        Ok(session) => session,
        Err(status) => return (status, "worker session unavailable").into_response(),
    };
    match session.poll(request).await {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(status) => (status, "worker poll rejected").into_response(),
    }
}

async fn report_worker(
    State(state): State<HubState>,
    Extension(authenticated): Extension<AuthenticatedWorker>,
    Json(request): Json<WorkerReportRequest>,
) -> Response {
    if request.worker_id != authenticated.0
        || request.report.worker_id != authenticated.0
        || request.worker_incarnation != request.report.worker_incarnation
    {
        return (StatusCode::FORBIDDEN, "worker identity mismatch").into_response();
    }
    let session = match state.0.session(&authenticated.0) {
        Ok(session) => session,
        Err(status) => return (status, "worker session unavailable").into_response(),
    };
    match session.report(request.report) {
        Ok(acknowledgement) => (StatusCode::OK, Json(acknowledgement)).into_response(),
        Err(status) => (status, "worker report rejected").into_response(),
    }
}

#[path = "structs/pending_command.rs"]
mod pending_command;
use pending_command::PendingCommand;

#[path = "structs/worker_session.rs"]
mod worker_session;
use worker_session::WorkerSession;

#[path = "structs/worker_inventory_sync.rs"]
mod worker_inventory_sync;
use worker_inventory_sync::WorkerInventorySync;

fn manager_time_ms() -> Option<i64> {
    let milliseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();
    i64::try_from(milliseconds).ok()
}

fn result_matches(command: &WorkerCommand, result: &WorkerResult) -> bool {
    matches!(
        (command, result),
        (WorkerCommand::StartRun(_), WorkerResult::Observation(_))
            | (WorkerCommand::StopRun(_), WorkerResult::Observation(_))
            | (WorkerCommand::ResolveRun(_), WorkerResult::Observation(_))
            | (WorkerCommand::InspectRun(_), WorkerResult::Inspection(_))
            | (WorkerCommand::ReadRunLogs(_), WorkerResult::Logs(_))
            | (_, WorkerResult::Error(_))
    )
}

#[path = "structs/remote_worker_port.rs"]
mod remote_worker_port;
pub use remote_worker_port::RemoteWorkerPort;

fn map_transport_error(error: TransportError) -> WorkerPortError {
    match error {
        TransportError::QueueFull => WorkerPortError::Unavailable,
        TransportError::ShuttingDown | TransportError::Unavailable => WorkerPortError::Unavailable,
        TransportError::InvalidEndpoint
        | TransportError::InvalidIdentity
        | TransportError::Protocol(_)
        | TransportError::WorkerConflict
        | TransportError::WorkerLimitReached
        | TransportError::InvalidCommand
        | TransportError::MalformedResponse
        | TransportError::Http => WorkerPortError::Unavailable,
    }
}

#[path = "structs/worker_transport_client.rs"]
mod worker_transport_client;
pub use worker_transport_client::WorkerTransportClient;

fn report_from_stored(stored: StoredWorkerReport) -> Result<WorkerReport, TransportError> {
    Ok(WorkerReport {
        command_id: stored.command_id,
        worker_id: stored.worker_id,
        worker_incarnation: stored.worker_incarnation,
        report_sequence: stored.report_sequence,
        result: serde_json::from_str(&stored.result_json)
            .map_err(|_| TransportError::Unavailable)?,
    })
}

fn recover_accepted_commands(worker: &WorkerService) -> Result<(), TransportError> {
    let incarnation = worker.descriptor().incarnation();
    for envelope in worker
        .accepted_commands()
        .map_err(|_| TransportError::Unavailable)?
    {
        let result = match &envelope.command {
            WorkerCommand::StartRun(command)
                if envelope.target_worker_incarnation != incarnation =>
            {
                match worker.inspect(conductor_core::InspectRun::new(command.spec().id().clone())) {
                    Ok(Some(_)) => execute_command(worker, envelope.command.clone()),
                    Ok(None) | Err(_) => WorkerResult::Error(RemoteWorkerError::Unavailable),
                }
            }
            _ => execute_command(worker, envelope.command.clone()),
        };
        let result_json =
            serde_json::to_string(&result).map_err(|_| TransportError::Unavailable)?;
        worker
            .complete_command(&envelope, &result_json)
            .map_err(|_| TransportError::Unavailable)?;
    }
    Ok(())
}

#[path = "structs/bounded_json_body.rs"]
mod bounded_json_body;
use bounded_json_body::BoundedJsonBody;

fn backoff(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(6);
    BACKOFF_INITIAL
        .checked_mul(1_u32 << exponent)
        .unwrap_or(BACKOFF_MAX)
        .min(BACKOFF_MAX)
}

fn execute_command(worker: &WorkerService, command: WorkerCommand) -> WorkerResult {
    match command {
        WorkerCommand::StartRun(command) => match worker.start(*command) {
            Ok(observation) => WorkerResult::Observation(observation),
            Err(error) => WorkerResult::Error(error.into()),
        },
        WorkerCommand::InspectRun(command) => match worker.inspect(command) {
            Ok(observation) => WorkerResult::Inspection(observation),
            Err(error) => WorkerResult::Error(error.into()),
        },
        WorkerCommand::StopRun(command) => match worker.stop(command) {
            Ok(observation) => WorkerResult::Observation(observation),
            Err(error) => WorkerResult::Error(error.into()),
        },
        WorkerCommand::ResolveRun(command) => match worker.resolve_unknown(command) {
            Ok(observation) => WorkerResult::Observation(observation),
            Err(error) => WorkerResult::Error(error.into()),
        },
        WorkerCommand::ReadRunLogs(command) => match worker.read_logs(command) {
            Ok(chunk) => WorkerResult::Logs(TransportLogChunk::from_chunk(chunk)),
            Err(error) => WorkerResult::Error(error.into()),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::PathBuf;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use conductor_core::{
        Architecture, ExecutionBackend, JobSpec, OperatingSystem, ProtocolVersionRange, RunSpec,
        RunState, WorkerCapabilities, WorkerDescriptor, WorkerInventory, WorkerInventoryEntry,
        WorkerResourceCapacity,
    };
    use conductor_manager::{CommandJournal, JobRepository, SqliteJobRepository};
    use tower::ServiceExt;
    include!("structs/test_database.rs");

    impl TestDatabase {
        fn new() -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after Unix epoch")
                .as_nanos();
            Self(std::env::temp_dir().join(format!(
                "conductor-transport-test-{}-{nonce}.sqlite3",
                std::process::id()
            )))
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            for path in [
                self.0.clone(),
                PathBuf::from(format!("{}-wal", self.0.display())),
                PathBuf::from(format!("{}-shm", self.0.display())),
            ] {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    use super::*;

    fn worker_id(value: &str) -> WorkerId {
        WorkerId::new(value).expect("valid worker id")
    }

    fn offer(version: u16) -> ProtocolOffer {
        ProtocolOffer::new(
            ProtocolVersionRange::new(version, version).expect("valid range"),
            BTreeSet::from([
                ProtocolCapability::OrderedReports,
                ProtocolCapability::WorkerIncarnations,
                ProtocolCapability::WorkerResourceInventory,
            ]),
            BTreeSet::from([ProtocolCapability::WorkerResourceInventory]),
        )
        .expect("valid protocol offer")
    }

    fn test_inventory(slots: usize) -> WorkerInventory {
        test_inventory_for_os(slots, OperatingSystem::Linux)
    }

    fn test_inventory_for_os(slots: usize, os: OperatingSystem) -> WorkerInventory {
        let capabilities = WorkerCapabilities::new(
            os,
            Architecture::X86_64,
            BTreeSet::from([ExecutionBackend::NativeProcess]),
            BTreeSet::new(),
        )
        .expect("valid worker capabilities");
        let capacity = WorkerResourceCapacity::new(8_000, 8 * 1024 * 1024 * 1024)
            .expect("valid test capacity");
        WorkerInventory::new(capabilities, capacity, capacity, slots).expect("worker inventory")
    }

    fn session() -> WorkerSession {
        WorkerSession::new(
            WorkerDescriptor::new(worker_id("worker-a"), 4, true).with_incarnation(9),
            offer(3),
            None,
        )
    }

    fn uninitialized_session() -> WorkerSession {
        WorkerSession::new(
            WorkerDescriptor::new(worker_id("worker-a"), 4, true),
            offer(3),
            None,
        )
    }

    fn poll_request(id: &str, version: u16) -> WorkerPollRequest {
        poll_request_with_incarnation(id, version, 9)
    }

    fn poll_request_with_incarnation(
        id: &str,
        version: u16,
        incarnation: u64,
    ) -> WorkerPollRequest {
        let descriptor =
            WorkerDescriptor::new(worker_id(id), 4, true).with_incarnation(incarnation);
        WorkerPollRequest {
            worker_id: worker_id(id),
            worker_incarnation: incarnation,
            status: WorkerStatus::new(descriptor, 0, 0, true, false, 1)
                .with_inventory(test_inventory(4)),
            inventory_page: WorkerInventoryPage {
                worker_id: worker_id(id),
                worker_incarnation: incarnation,
                snapshot_generation: 0,
                after_sequence: 0,
                entries: Vec::new(),
                next_after_sequence: None,
            },
            protocol_offer: offer(version),
        }
    }

    #[tokio::test]
    async fn poll_negotiates_protocol_and_returns_the_same_command_after_reconnect() {
        let session = session();
        let _receiver = session
            .enqueue(WorkerCommand::InspectRun(InspectRun::new(
                RunId::new("run-a").expect("valid run id"),
            )))
            .expect("enqueue command");
        let command_id = session
            .pending
            .lock()
            .expect("pending lock")
            .keys()
            .next()
            .unwrap()
            .clone();
        let first = session
            .poll(poll_request("worker-a", 3))
            .await
            .expect("compatible worker poll");
        let first_command = first.command.expect("queued command");
        assert_eq!(first.protocol_version, 3);
        assert_eq!(first_command.command_id, command_id);
        assert!(session.is_fresh());
        assert_eq!(session.status().expect("worker status").capacity(), 4);

        session
            .pending
            .lock()
            .expect("pending lock")
            .get_mut(&command_id)
            .expect("pending command")
            .retry_at = Instant::now();
        let reconnected = session
            .poll(poll_request("worker-a", 3))
            .await
            .expect("reconnected worker poll");
        assert_eq!(reconnected.command, Some(first_command));
    }

    #[tokio::test]
    async fn durable_command_envelope_keeps_its_id_when_queued_for_remote_delivery() {
        let session = session();
        let envelope = CommandEnvelope {
            command_id: CommandId::new("command-persisted").expect("valid command id"),
            target_worker_id: worker_id("worker-a"),
            target_worker_incarnation: 9,
            command: WorkerCommand::InspectRun(InspectRun::new(
                RunId::new("run-a").expect("valid run id"),
            )),
        };
        let _receiver = session
            .enqueue_envelope(envelope.clone())
            .expect("enqueue durable command");
        let response = session
            .poll(poll_request("worker-a", 3))
            .await
            .expect("compatible worker poll");
        assert_eq!(response.command, Some(envelope));
    }

    #[tokio::test]
    async fn rejects_wrong_incarnation_and_incompatible_protocol() {
        let session = session();
        let mut wrong_incarnation = poll_request("worker-a", 3);
        wrong_incarnation.worker_incarnation = 8;
        assert_eq!(
            session.poll(wrong_incarnation).await,
            Err(StatusCode::CONFLICT)
        );
        assert_eq!(
            session.poll(poll_request("worker-a", 2)).await,
            Err(StatusCode::UPGRADE_REQUIRED)
        );
        assert!(!session.is_fresh());
    }

    #[tokio::test]
    async fn rejected_first_poll_does_not_bind_worker_incarnation() {
        let session = Arc::new(uninitialized_session());

        let mut invalid_status = poll_request("worker-a", 3);
        invalid_status.status = WorkerStatus::new(
            WorkerDescriptor::new(worker_id("worker-a"), 8, true).with_incarnation(9),
            0,
            0,
            true,
            false,
            1,
        )
        .with_inventory(test_inventory(8));
        assert_eq!(
            session.poll(invalid_status).await,
            Err(StatusCode::CONFLICT)
        );
        assert_eq!(session.descriptor().incarnation(), 0);

        let incompatible = poll_request("worker-a", 2);
        assert_eq!(
            session.poll(incompatible).await,
            Err(StatusCode::UPGRADE_REQUIRED)
        );
        assert_eq!(session.descriptor().incarnation(), 0);

        let poll_session = Arc::clone(&session);
        let poll =
            tokio::spawn(async move { poll_session.poll(poll_request("worker-a", 3)).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.descriptor().incarnation() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("valid first poll should bind incarnation");
        let _receiver = session
            .enqueue(WorkerCommand::InspectRun(InspectRun::new(
                RunId::new("run-first-valid-poll").expect("valid run id"),
            )))
            .expect("queue command for immediate valid poll response");
        let response = poll
            .await
            .expect("valid poll task")
            .expect("valid worker poll");
        assert_eq!(session.descriptor().incarnation(), 9);
        assert!(session.is_fresh());
        assert_eq!(
            response
                .command
                .expect("pending command")
                .target_worker_incarnation,
            9
        );
    }

    #[tokio::test]
    async fn rejects_inventory_changes_and_worker_supplied_manager_metadata() {
        let session = session();
        let command_id = CommandId::new("first-inventory-command").expect("command ID");
        let _receiver = session
            .enqueue_envelope(conductor_core::CommandEnvelope {
                command_id,
                target_worker_id: worker_id("worker-a"),
                target_worker_incarnation: 9,
                command: conductor_core::WorkerCommand::InspectRun(InspectRun::new(
                    RunId::new("inventory-check").expect("run ID"),
                )),
            })
            .expect("enqueue first poll command");
        session
            .poll(poll_request("worker-a", 3))
            .await
            .expect("initial inventory is accepted");

        let mut changed = poll_request("worker-a", 3);
        changed.status = changed
            .status
            .with_inventory(test_inventory_for_os(4, OperatingSystem::Macos));
        assert_eq!(session.poll(changed).await, Err(StatusCode::CONFLICT));

        let mut forged = poll_request("worker-a", 3);
        forged.status = forged
            .status
            .with_inventory(test_inventory(4))
            .with_freshness(true);
        assert_eq!(session.poll(forged).await, Err(StatusCode::CONFLICT));
    }

    #[tokio::test]
    async fn stale_remote_status_retains_inventory_and_reports_freshness() {
        let session = Arc::new(session());
        let command_id = CommandId::new("freshness-command").expect("command ID");
        let _receiver = session
            .enqueue_envelope(conductor_core::CommandEnvelope {
                command_id,
                target_worker_id: worker_id("worker-a"),
                target_worker_incarnation: 9,
                command: conductor_core::WorkerCommand::InspectRun(InspectRun::new(
                    RunId::new("freshness-run").expect("run ID"),
                )),
            })
            .expect("enqueue command");
        session
            .poll(poll_request("worker-a", 3))
            .await
            .expect("worker poll accepted");
        *session.last_seen.lock().expect("last seen lock") =
            Instant::now() - WORKER_FRESHNESS_TIMEOUT - Duration::from_millis(1);
        let remote = RemoteWorkerPort {
            session: Arc::clone(&session),
        };
        let status = remote.status().expect("stale status remains inspectable");
        assert!(!status.fresh());
        assert!(status.inventory().is_some());
        assert!(status.last_seen_at_ms().is_some());
        assert!(remote.is_degraded());
    }

    #[tokio::test]
    async fn worker_restart_replays_old_report_then_adopts_new_incarnation() {
        let session = Arc::new(session());
        let old_command = CommandEnvelope {
            command_id: CommandId::new("old-incarnation-report").unwrap(),
            target_worker_id: worker_id("worker-a"),
            target_worker_incarnation: 9,
            command: WorkerCommand::InspectRun(InspectRun::new(RunId::new("run-old").unwrap())),
        };
        let old_receiver = session
            .enqueue_envelope(old_command.clone())
            .expect("queue old-incarnation command");
        let report = WorkerReport {
            command_id: old_command.command_id.clone(),
            worker_id: worker_id("worker-a"),
            worker_incarnation: 9,
            report_sequence: 1,
            result: WorkerResult::Inspection(None),
        };
        assert_eq!(
            session.report(report.clone()),
            Ok(ReportAcknowledgement {
                report_sequence: 1,
                duplicate: false,
            })
        );
        assert_eq!(old_receiver.recv().unwrap(), report.result);

        let stale = CommandEnvelope {
            command_id: CommandId::new("old-incarnation-not-received").unwrap(),
            target_worker_id: worker_id("worker-a"),
            target_worker_incarnation: 9,
            command: WorkerCommand::InspectRun(InspectRun::new(RunId::new("run-stale").unwrap())),
        };
        let stale_receiver = session
            .enqueue_envelope(stale)
            .expect("queue stale command");
        let mut restarted_request = poll_request_with_incarnation("worker-a", 3, 10);
        restarted_request.status = restarted_request
            .status
            .with_inventory(test_inventory_for_os(4, OperatingSystem::Macos));
        let polling_session = Arc::clone(&session);
        let poll = tokio::spawn(async move { polling_session.poll(restarted_request).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while session.descriptor().incarnation() != 10 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("new worker incarnation should be adopted");
        assert_eq!(
            stale_receiver.recv().unwrap(),
            WorkerResult::Error(RemoteWorkerError::Unavailable)
        );
        let new_command = CommandEnvelope {
            command_id: CommandId::new("new-incarnation-command").unwrap(),
            target_worker_id: worker_id("worker-a"),
            target_worker_incarnation: 10,
            command: WorkerCommand::InspectRun(InspectRun::new(RunId::new("run-new").unwrap())),
        };
        let _new_receiver = session
            .enqueue_envelope(new_command.clone())
            .expect("queue command for new incarnation");
        let response = poll.await.unwrap().unwrap();
        assert_eq!(response.command, Some(new_command.clone()));
        let new_report = WorkerReport {
            command_id: new_command.command_id,
            worker_id: worker_id("worker-a"),
            worker_incarnation: 10,
            report_sequence: 1,
            result: WorkerResult::Inspection(None),
        };
        assert_eq!(
            session.report(new_report),
            Ok(ReportAcknowledgement {
                report_sequence: 1,
                duplicate: false,
            })
        );
    }

    #[test]
    fn inventory_reconciliation_is_monotonic_and_keeps_uncertain_ownership() {
        let database = TestDatabase::new();
        let repository = SqliteJobRepository::open(&database.0).expect("open manager journal");
        let job: JobSpec =
            serde_json::from_str(r#"{"id":"inventory-job","command":["/bin/true"],"cwd":"/tmp"}"#)
                .expect("valid job");
        repository.insert(job.clone()).expect("insert job");
        let worker_key = worker_id("inventory-worker");
        let admission = repository
            .create_run_for_worker(job.id(), &worker_key, 9, 4, None)
            .expect("create durable assignment");
        let requested = repository
            .get_run(admission.spec.id())
            .expect("read requested run")
            .expect("run exists");
        let running = requested
            .next_revision(RunState::Running, Some(2), None, None, None, None)
            .expect("new observation revision")
            .with_worker_incarnation(9);
        let entry = WorkerInventoryEntry {
            sequence: 1,
            observation: running.clone(),
        };

        assert_eq!(
            repository
                .reconcile_worker_inventory_page(&worker_key, 9, std::slice::from_ref(&entry))
                .expect("reconcile worker inventory"),
            vec![admission.spec.id().clone()]
        );
        repository
            .finish_worker_inventory(&worker_key, 9, std::slice::from_ref(admission.spec.id()))
            .expect("finish complete inventory round");
        assert_eq!(
            repository
                .get_run(admission.spec.id())
                .unwrap()
                .unwrap()
                .state(),
            RunState::Running
        );

        repository
            .finish_worker_inventory(&worker_key, 9, &[])
            .expect("missing assigned run becomes uncertain");
        let uncertain = repository.get_run(admission.spec.id()).unwrap().unwrap();
        assert_eq!(uncertain.state(), RunState::Unknown);
        assert_eq!(
            repository
                .list_active_runs(&worker_key, 4)
                .expect("uncertain assignment remains reserved")
                .len(),
            1
        );

        repository
            .reconcile_worker_inventory_page(&worker_key, 9, std::slice::from_ref(&entry))
            .expect("stale inventory is ignored");
        assert_eq!(
            repository.get_run(admission.spec.id()).unwrap().unwrap(),
            uncertain
        );

        let pending_job: JobSpec = serde_json::from_str(
            r#"{"id":"inventory-pending-job","command":["/bin/true"],"cwd":"/tmp"}"#,
        )
        .expect("valid pending job");
        repository
            .insert(pending_job.clone())
            .expect("insert pending job");
        let pending = repository
            .create_run_for_worker(pending_job.id(), &worker_key, 9, 4, None)
            .expect("create second pending assignment");
        repository
            .finish_worker_inventory(&worker_key, 9, &[])
            .expect("pending manager intent is not treated as missing worker state");
        assert_eq!(
            repository
                .get_run(pending.spec.id())
                .unwrap()
                .unwrap()
                .state(),
            RunState::Requested
        );

        let future = WorkerInventoryEntry {
            sequence: 2,
            observation: running.with_worker_incarnation(10),
        };
        assert_eq!(
            repository.reconcile_worker_inventory_page(&worker_key, 9, &[future]),
            Err(CommandStoreError::Conflict)
        );
    }

    #[test]
    fn worker_inventory_scan_restarts_safely_after_manager_session_loss() {
        let database = TestDatabase::new();
        let repository =
            Arc::new(SqliteJobRepository::open(&database.0).expect("open manager journal"));
        let worker_key = worker_id("worker-a");
        let job: JobSpec = serde_json::from_str(
            r#"{"id":"inventory-page-job","command":["/bin/true"],"cwd":"/tmp"}"#,
        )
        .expect("valid inventory job");
        let entries = (1..=32)
            .map(|sequence| {
                let run_id = RunId::new(format!("inventory-run-{sequence:02}"))
                    .expect("valid inventory run id");
                let spec = RunSpec::new(run_id, worker_key.clone(), job.clone());
                WorkerInventoryEntry {
                    sequence,
                    observation: RunObservation::requested(&spec, 1),
                }
            })
            .collect::<Vec<_>>();
        let first_page = WorkerInventoryPage {
            worker_id: worker_key.clone(),
            worker_incarnation: 9,
            snapshot_generation: 33,
            after_sequence: 0,
            entries,
            next_after_sequence: Some(32),
        };
        let final_page = WorkerInventoryPage {
            worker_id: worker_key.clone(),
            worker_incarnation: 9,
            snapshot_generation: 33,
            after_sequence: 32,
            entries: vec![WorkerInventoryEntry {
                sequence: 33,
                observation: RunObservation::requested(
                    &RunSpec::new(
                        RunId::new("inventory-run-33").unwrap(),
                        worker_key.clone(),
                        job,
                    ),
                    1,
                ),
            }],
            next_after_sequence: None,
        };

        let first_session = WorkerSession::new(
            WorkerDescriptor::new(worker_key.clone(), 4, true).with_incarnation(9),
            offer(3),
            Some(Arc::clone(&repository) as Arc<dyn CommandJournal>),
        );
        let acknowledgement = first_session
            .reconcile_inventory(&first_page)
            .expect("first inventory page accepted");
        assert_eq!(acknowledgement.next_after_sequence, Some(32));
        assert!(!acknowledgement.restart_scan);

        let restarted_session = WorkerSession::new(
            WorkerDescriptor::new(worker_key, 4, true).with_incarnation(9),
            offer(3),
            Some(repository),
        );
        assert!(
            restarted_session
                .reconcile_inventory(&final_page)
                .expect("session loss requests a fresh scan")
                .restart_scan
        );
        restarted_session
            .reconcile_inventory(&first_page)
            .expect("rescan first page");
        let final_ack = restarted_session
            .reconcile_inventory(&final_page)
            .expect("rescan final page");
        assert_eq!(final_ack.next_after_sequence, None);
        assert!(!final_ack.restart_scan);
    }

    #[test]
    fn manager_restart_restores_commands_and_deduplicates_report_retries() {
        let database = TestDatabase::new();
        let repository =
            Arc::new(SqliteJobRepository::open(&database.0).expect("open manager journal"));
        let job: conductor_core::JobSpec =
            serde_json::from_str(r#"{"id":"job-durable","command":["/bin/true"],"cwd":"/tmp"}"#)
                .expect("valid job");
        repository.insert(job.clone()).expect("insert job");
        let worker_key = worker_id("worker-a");
        let admission = repository
            .create_run_for_worker(job.id(), &worker_key, 9, 4, None)
            .expect("create run");
        let envelope = admission.command.expect("durable command envelope");

        let restarted_session = WorkerSession::new(
            WorkerDescriptor::new(worker_key.clone(), 4, true).with_incarnation(9),
            offer(3),
            Some(repository.clone()),
        );
        restarted_session
            .restore_pending(9)
            .expect("restore pending command from journal");
        assert!(
            restarted_session
                .pending
                .lock()
                .expect("pending lock")
                .contains_key(&envelope.command_id)
        );

        let report = WorkerReport {
            command_id: envelope.command_id.clone(),
            worker_id: worker_key,
            worker_incarnation: 9,
            report_sequence: 1,
            result: WorkerResult::Observation(RunObservation::requested(&admission.spec, 1)),
        };
        let report_session = WorkerSession::new(
            WorkerDescriptor::new(worker_id("worker-a"), 4, true).with_incarnation(9),
            offer(3),
            Some(repository.clone()),
        );
        assert_eq!(
            report_session.report(report.clone()),
            Ok(ReportAcknowledgement {
                report_sequence: 1,
                duplicate: false,
            })
        );
        let stored = repository
            .command_record(&report.command_id)
            .expect("read durable command")
            .expect("command record exists");
        assert_eq!(
            stored.acknowledgement_json.as_deref(),
            Some(serde_json::to_string(&report).unwrap().as_str())
        );

        let next_manager_session = WorkerSession::new(
            WorkerDescriptor::new(worker_id("worker-a"), 4, true).with_incarnation(9),
            offer(3),
            Some(repository),
        );
        assert_eq!(
            next_manager_session.report(report),
            Ok(ReportAcknowledgement {
                report_sequence: 1,
                duplicate: true,
            })
        );
    }

    #[test]
    fn command_queue_and_concurrent_poll_admission_are_bounded() {
        let session = session();
        for index in 0..MAX_PENDING_COMMANDS {
            session
                .enqueue(WorkerCommand::InspectRun(InspectRun::new(
                    RunId::new(format!("run-{index}")).expect("valid run id"),
                )))
                .expect("command within queue bound");
        }
        assert!(matches!(
            session.enqueue(WorkerCommand::InspectRun(InspectRun::new(
                RunId::new("overflow").expect("valid run id"),
            ))),
            Err(TransportError::QueueFull)
        ));

        let _slow_poll = session
            .poll_gate
            .try_acquire()
            .expect("first poll admitted");
        assert!(session.poll_gate.try_acquire().is_err());
    }

    #[test]
    fn worker_registry_has_a_fixed_capacity() {
        let hub = WorkerTransportHub::default();
        for index in 0..MAX_REGISTERED_WORKERS {
            hub.register_worker(
                WorkerDescriptor::new(worker_id(&format!("worker-{index}")), 1, true),
                offer(3),
            )
            .expect("worker within registry bound");
        }
        assert!(matches!(
            hub.register_worker(
                WorkerDescriptor::new(worker_id("worker-overflow"), 1, true),
                offer(3),
            ),
            Err(TransportError::WorkerLimitReached)
        ));
    }

    #[test]
    fn reports_are_ordered_deduplicated_and_correlated_to_commands() {
        let session = session();
        let receiver = session
            .enqueue(WorkerCommand::InspectRun(InspectRun::new(
                RunId::new("run-report").expect("valid run id"),
            )))
            .expect("enqueue command");
        let command_id = session
            .pending
            .lock()
            .expect("pending lock")
            .keys()
            .next()
            .expect("command id")
            .clone();
        let report = WorkerReport {
            command_id,
            worker_id: worker_id("worker-a"),
            worker_incarnation: 9,
            report_sequence: 1,
            result: WorkerResult::Inspection(None),
        };
        assert_eq!(
            session.report(report.clone()),
            Ok(ReportAcknowledgement {
                report_sequence: 1,
                duplicate: false,
            })
        );
        assert_eq!(receiver.recv().expect("command response"), report.result);
        assert_eq!(
            session.report(report.clone()),
            Ok(ReportAcknowledgement {
                report_sequence: 1,
                duplicate: true,
            })
        );

        let mut conflicting_duplicate = report.clone();
        conflicting_duplicate.result = WorkerResult::Error(RemoteWorkerError::Unavailable);
        assert_eq!(
            session.report(conflicting_duplicate),
            Err(StatusCode::CONFLICT)
        );
        let mut skipped_sequence = report;
        skipped_sequence.report_sequence = 3;
        assert_eq!(session.report(skipped_sequence), Err(StatusCode::CONFLICT));
    }

    #[test]
    fn new_session_accepts_worker_report_sequence_after_manager_restart() {
        let session = session();
        let first_receiver = session
            .enqueue(WorkerCommand::InspectRun(InspectRun::new(
                RunId::new("run-after-restart-first").expect("valid run id"),
            )))
            .expect("enqueue first command");
        let first_command = session
            .pending
            .lock()
            .expect("pending lock")
            .keys()
            .next()
            .expect("first command ID")
            .clone();
        let first_report = WorkerReport {
            command_id: first_command,
            worker_id: worker_id("worker-a"),
            worker_incarnation: 9,
            report_sequence: 7,
            result: WorkerResult::Inspection(None),
        };
        let mut zero_sequence = first_report.clone();
        zero_sequence.report_sequence = 0;
        assert_eq!(session.report(zero_sequence), Err(StatusCode::CONFLICT));
        assert_eq!(
            session.report(first_report.clone()),
            Ok(ReportAcknowledgement {
                report_sequence: 7,
                duplicate: false,
            })
        );
        let _ = first_receiver.recv().expect("first command response");

        let second_receiver = session
            .enqueue(WorkerCommand::InspectRun(InspectRun::new(
                RunId::new("run-after-restart-second").expect("valid run id"),
            )))
            .expect("enqueue second command");
        let second_command = session
            .pending
            .lock()
            .expect("pending lock")
            .keys()
            .find(|command_id| **command_id != first_report.command_id)
            .expect("second command ID")
            .clone();
        let second_report = WorkerReport {
            command_id: second_command,
            worker_id: worker_id("worker-a"),
            worker_incarnation: 9,
            report_sequence: 8,
            result: WorkerResult::Inspection(None),
        };
        assert_eq!(
            session.report(second_report),
            Ok(ReportAcknowledgement {
                report_sequence: 8,
                duplicate: false,
            })
        );
        let _ = second_receiver.recv().expect("second command response");
    }
    include!("structs/test_authenticator.rs");

    impl WorkerAuthenticator for TestAuthenticator {
        fn authenticate(&self, _request: &Request<Body>) -> Option<WorkerId> {
            self.0.clone()
        }
    }

    fn request(body: impl Into<Body>) -> Request<Body> {
        request_at("/v1/worker/poll", body)
    }

    fn request_at(path: &str, body: impl Into<Body>) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(body.into())
            .expect("valid request")
    }

    #[tokio::test]
    async fn routes_require_identity_and_reject_malformed_or_oversized_json() {
        let hub = WorkerTransportHub::default();
        hub.register_worker(
            WorkerDescriptor::new(worker_id("worker-a"), 4, true).with_incarnation(9),
            offer(3),
        )
        .expect("register worker");

        let unauthenticated = hub
            .router(Arc::new(TestAuthenticator(None)))
            .oneshot(request("{"))
            .await
            .expect("unauthenticated route response");
        assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

        let router = hub.router(Arc::new(TestAuthenticator(Some(worker_id("worker-a")))));
        let malformed = router
            .clone()
            .oneshot(request("{"))
            .await
            .expect("malformed route response");
        assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

        let oversized = router
            .oneshot(request(vec![b' '; MAX_TRANSPORT_BODY_BYTES + 1]))
            .await
            .expect("oversized route response");
        assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn authenticated_worker_cannot_poll_or_report_for_another_worker() {
        let hub = WorkerTransportHub::default();
        hub.register_worker(
            WorkerDescriptor::new(worker_id("worker-a"), 4, true).with_incarnation(9),
            offer(3),
        )
        .expect("register authenticated worker");
        let router = hub.router(Arc::new(TestAuthenticator(Some(worker_id("worker-a")))));

        let poll = serde_json::to_vec(&poll_request("worker-b", 3)).expect("serialize poll");
        let poll_response = router
            .clone()
            .oneshot(request(poll))
            .await
            .expect("cross-worker poll response");
        assert_eq!(poll_response.status(), StatusCode::FORBIDDEN);

        let report = WorkerReportRequest {
            worker_id: worker_id("worker-a"),
            worker_incarnation: 9,
            report: WorkerReport {
                command_id: CommandId::new("command-a").expect("valid command id"),
                worker_id: worker_id("worker-b"),
                worker_incarnation: 9,
                report_sequence: 1,
                result: WorkerResult::Inspection(None),
            },
        };
        let body = serde_json::to_vec(&report).expect("serialize report");
        let report_response = router
            .oneshot(request_at("/v1/worker/report", body))
            .await
            .expect("cross-worker report response");
        assert_eq!(report_response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn endpoint_requires_https_and_credentials_are_not_part_of_url() {
        let http = WorkerTransportClient::from_pem_identity("http://localhost/", b"invalid");
        assert_eq!(http.err(), Some(TransportError::InvalidEndpoint));
        let credentialed = WorkerTransportClient::from_pem_identity(
            "https://user:password@example.test/",
            b"invalid",
        );
        assert_eq!(credentialed.err(), Some(TransportError::InvalidEndpoint));
    }

    #[test]
    fn request_body_and_reconnect_backoff_stay_within_configured_bounds() {
        let mut body = BoundedJsonBody(Vec::new());
        body.write_all(&vec![b'x'; MAX_TRANSPORT_BODY_BYTES])
            .expect("body at limit");
        assert_eq!(body.0.len(), MAX_TRANSPORT_BODY_BYTES);
        assert!(body.write_all(b"x").is_err());

        assert_eq!(backoff(1), Duration::from_millis(100));
        assert_eq!(backoff(2), Duration::from_millis(200));
        assert_eq!(backoff(10), BACKOFF_MAX);
        assert_eq!(backoff(u32::MAX), BACKOFF_MAX);
    }
}
