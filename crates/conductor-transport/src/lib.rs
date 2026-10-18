//! Bounded, worker-initiated HTTP transport for manager/worker commands.

mod identity;
pub use identity::{
    CertificateAuthority, IdentityError, IssuedServerIdentity, IssuedWorkerIdentity,
    WorkerEnrollmentService, WorkerKeyMaterial,
};

use std::collections::{BTreeMap, VecDeque};
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
    CommandId, InspectRun, LogStream, ProtocolCapability, ProtocolNegotiationError, ProtocolOffer,
    ReadRunLogs, ResolveRun, RunId, RunLogChunk, RunLogStatus, RunObservation, StartRun, StopRun,
    WorkerDescriptor, WorkerId, WorkerStatus,
};
use conductor_manager::{CommandJournal, WorkerPort, WorkerPortError};
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TransportLogChunk {
    pub run_id: RunId,
    pub stream: LogStream,
    pub offset: u64,
    pub next_offset: u64,
    pub data_base64: String,
    pub stream_closed: bool,
    pub eof: bool,
    pub truncated: bool,
}

impl TransportLogChunk {
    fn from_chunk(chunk: RunLogChunk) -> Self {
        Self {
            run_id: chunk.run_id().clone(),
            stream: chunk.stream(),
            offset: chunk.offset(),
            next_offset: chunk.next_offset(),
            data_base64: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(chunk.data()),
            stream_closed: chunk.stream_closed(),
            eof: chunk.eof(),
            truncated: chunk.truncated(),
        }
    }

    fn into_chunk(self) -> Result<RunLogChunk, TransportError> {
        let data = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(self.data_base64.as_bytes())
            .map_err(|_| TransportError::MalformedResponse)?;
        Ok(RunLogChunk::new(
            self.run_id,
            self.stream,
            self.offset,
            self.next_offset,
            data,
            RunLogStatus::new(self.stream_closed, self.eof, self.truncated),
        ))
    }
}

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerReport {
    pub command_id: CommandId,
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub report_sequence: u64,
    pub result: WorkerResult,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerPollRequest {
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub status: WorkerStatus,
    pub protocol_offer: ProtocolOffer,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerPollResponse {
    pub protocol_version: u16,
    pub capabilities: Vec<ProtocolCapability>,
    pub command: Option<CommandEnvelope>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReportAcknowledgement {
    pub report_sequence: u64,
    pub duplicate: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerReportRequest {
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub report: WorkerReport,
}

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

#[derive(Clone)]
pub struct WorkerTransportHub {
    sessions: Arc<RwLock<BTreeMap<WorkerId, Arc<WorkerSession>>>>,
    journal: Option<Arc<dyn CommandJournal>>,
}

impl Default for WorkerTransportHub {
    fn default() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(BTreeMap::new())),
            journal: None,
        }
    }
}

impl WorkerTransportHub {
    /// Create a transport hub backed by the manager's durable command journal.
    pub fn with_journal(journal: Arc<dyn CommandJournal>) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(BTreeMap::new())),
            journal: Some(journal),
        }
    }

    pub fn register_worker(
        &self,
        descriptor: WorkerDescriptor,
        protocol_offer: ProtocolOffer,
    ) -> Result<Arc<RemoteWorkerPort>, TransportError> {
        let mut sessions = self
            .sessions
            .write()
            .map_err(|_| TransportError::Unavailable)?;
        let worker_id = descriptor.worker_id().clone();
        if sessions.contains_key(&worker_id) {
            return Err(TransportError::WorkerConflict);
        }
        if sessions.len() >= MAX_REGISTERED_WORKERS {
            return Err(TransportError::WorkerLimitReached);
        }
        let session = Arc::new(WorkerSession::new(
            descriptor,
            protocol_offer,
            self.journal.clone(),
        ));
        sessions.insert(worker_id, Arc::clone(&session));
        Ok(Arc::new(RemoteWorkerPort { session }))
    }

    pub fn unregister_worker(&self, worker_id: &WorkerId) -> Result<bool, TransportError> {
        self.sessions
            .write()
            .map(|mut sessions| sessions.remove(worker_id).is_some())
            .map_err(|_| TransportError::Unavailable)
    }

    /// Build routes that always require an identity supplied by the configured
    /// authenticator. No unauthenticated router constructor is provided.
    pub fn router(&self, authenticator: Arc<dyn WorkerAuthenticator>) -> Router {
        Router::new()
            .route("/v1/worker/poll", post(poll_worker))
            .route("/v1/worker/report", post(report_worker))
            .layer(DefaultBodyLimit::max(MAX_TRANSPORT_BODY_BYTES))
            .layer(middleware::from_fn_with_state(
                authenticator,
                authenticate_worker,
            ))
            .with_state(HubState(self.clone()))
    }

    fn session(&self, worker_id: &WorkerId) -> Result<Arc<WorkerSession>, StatusCode> {
        self.sessions
            .read()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
            .get(worker_id)
            .cloned()
            .ok_or(StatusCode::NOT_FOUND)
    }
}

/// Authentication integration point. Implementations must derive the worker
/// identity from verified transport credentials, not request JSON.
pub trait WorkerAuthenticator: Send + Sync + 'static {
    fn authenticate(&self, request: &Request<axum::body::Body>) -> Option<WorkerId>;
}

#[derive(Clone)]
struct AuthenticatedWorker(WorkerId);

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

#[derive(Clone)]
struct HubState(WorkerTransportHub);

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

struct PendingCommand {
    envelope: CommandEnvelope,
    responses: Vec<SyncSender<WorkerResult>>,
    retry_at: Instant,
}

struct WorkerSession {
    descriptor: WorkerDescriptor,
    incarnation: AtomicU64,
    protocol_offer: ProtocolOffer,
    journal: Option<Arc<dyn CommandJournal>>,
    status: Mutex<WorkerStatus>,
    pending: Mutex<BTreeMap<CommandId, PendingCommand>>,
    restored_incarnation: AtomicU64,
    last_report_sequence: Mutex<BTreeMap<u64, u64>>,
    recent_reports: Mutex<VecDeque<WorkerReport>>,
    changed: Notify,
    poll_gate: Semaphore,
    last_seen: Mutex<Instant>,
    connected: AtomicBool,
    next_command: AtomicU64,
}

impl WorkerSession {
    fn new(
        descriptor: WorkerDescriptor,
        protocol_offer: ProtocolOffer,
        journal: Option<Arc<dyn CommandJournal>>,
    ) -> Self {
        let incarnation = descriptor.incarnation();
        Self {
            status: Mutex::new(WorkerStatus::new(descriptor.clone(), 0, 0, false, false, 0)),
            descriptor,
            incarnation: AtomicU64::new(incarnation),
            protocol_offer,
            journal,
            pending: Mutex::new(BTreeMap::new()),
            restored_incarnation: AtomicU64::new(0),
            last_report_sequence: Mutex::new(BTreeMap::new()),
            recent_reports: Mutex::new(VecDeque::new()),
            changed: Notify::new(),
            poll_gate: Semaphore::new(1),
            last_seen: Mutex::new(Instant::now()),
            connected: AtomicBool::new(false),
            next_command: AtomicU64::new(0),
        }
    }

    fn command_id(&self) -> Result<CommandId, TransportError> {
        let sequence = self
            .next_command
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| TransportError::Unavailable)?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TransportError::Unavailable)?
            .as_nanos();
        let value = format!("cmd-{}-{nanos}-{sequence}", std::process::id());
        CommandId::new(value).map_err(|_| TransportError::Unavailable)
    }

    fn descriptor(&self) -> WorkerDescriptor {
        self.descriptor
            .clone()
            .with_incarnation(self.incarnation.load(Ordering::Acquire))
    }

    fn enqueue(
        &self,
        command: WorkerCommand,
    ) -> Result<mpsc::Receiver<WorkerResult>, TransportError> {
        let command_id = self.command_id()?;
        let envelope = CommandEnvelope {
            command_id: command_id.clone(),
            target_worker_id: self.descriptor.worker_id().clone(),
            target_worker_incarnation: self.incarnation.load(Ordering::Acquire),
            command,
        };
        self.enqueue_envelope(envelope)
    }

    fn enqueue_envelope(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<mpsc::Receiver<WorkerResult>, TransportError> {
        if envelope.target_worker_id != *self.descriptor.worker_id()
            || envelope.target_worker_incarnation != self.incarnation.load(Ordering::Acquire)
        {
            return Err(TransportError::WorkerConflict);
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| TransportError::Unavailable)?;
        if pending.len() >= MAX_PENDING_COMMANDS {
            return Err(TransportError::QueueFull);
        }
        if let Some(existing) = pending.get(&envelope.command_id) {
            if existing.envelope != envelope {
                return Err(TransportError::WorkerConflict);
            }
            if existing.responses.len() >= MAX_PENDING_COMMANDS {
                return Err(TransportError::QueueFull);
            }
            pending
                .get_mut(&envelope.command_id)
                .ok_or(TransportError::Unavailable)?
                .responses
                .push(sender);
            return Ok(receiver);
        }
        pending.insert(
            envelope.command_id.clone(),
            PendingCommand {
                envelope,
                responses: vec![sender],
                retry_at: Instant::now(),
            },
        );
        drop(pending);
        self.changed.notify_one();
        Ok(receiver)
    }

    async fn poll(&self, request: WorkerPollRequest) -> Result<WorkerPollResponse, StatusCode> {
        let current_incarnation = self.incarnation.load(Ordering::Acquire);
        let request_incarnation = request.worker_incarnation;
        if request_incarnation < current_incarnation {
            return Err(StatusCode::CONFLICT);
        }
        if request.status.worker_id() != self.descriptor.worker_id()
            || request.status.incarnation() != request_incarnation
            || request.status.capacity() != self.descriptor.capacity()
            || request.status.supports_process_execution()
                != self.descriptor.supports_process_execution()
        {
            return Err(StatusCode::CONFLICT);
        }
        let negotiated = self
            .protocol_offer
            .negotiate(&request.protocol_offer)
            .map_err(|_| StatusCode::UPGRADE_REQUIRED)?;
        let _poll = self
            .poll_gate
            .try_acquire()
            .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
        if request_incarnation > current_incarnation {
            self.adopt_incarnation(request_incarnation)?;
        }
        self.restore_pending(request_incarnation)?;
        *self
            .status
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)? = request.status;
        self.mark_seen()?;

        let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
        let command = loop {
            let notified = self.changed.notified();
            let (ready, next_retry) = self.next_poll_command(request_incarnation)?;
            if let Some(command) = ready {
                break Some(command);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break None;
            }
            let wake_at = next_retry
                .map(tokio::time::Instant::from_std)
                .unwrap_or(deadline)
                .min(deadline);
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep_until(wake_at) => {},
            }
        };

        let mut capabilities = negotiated
            .capabilities()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        capabilities.sort_unstable();
        Ok(WorkerPollResponse {
            protocol_version: negotiated.version(),
            capabilities,
            command,
        })
    }

    fn adopt_incarnation(&self, incarnation: u64) -> Result<(), StatusCode> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let stale = pending
            .iter()
            .filter(|(_, item)| item.envelope.target_worker_incarnation != incarnation)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for command_id in stale {
            if let Some(item) = pending.remove(&command_id) {
                for response in item.responses {
                    let _ = response.send(WorkerResult::Error(RemoteWorkerError::Unavailable));
                }
            }
        }
        self.incarnation.store(incarnation, Ordering::Release);
        Ok(())
    }

    fn restore_pending(&self, incarnation: u64) -> Result<(), StatusCode> {
        let Some(journal) = &self.journal else {
            return Ok(());
        };
        if self.restored_incarnation.load(Ordering::Acquire) == incarnation {
            return Ok(());
        }
        let commands = journal
            .pending_commands(
                self.descriptor.worker_id(),
                incarnation,
                MAX_PENDING_COMMANDS,
            )
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        for envelope in commands {
            if pending.contains_key(&envelope.command_id) {
                continue;
            }
            if pending.len() >= MAX_PENDING_COMMANDS {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            let (response, receiver): (SyncSender<WorkerResult>, Receiver<WorkerResult>) =
                mpsc::sync_channel(1);
            drop(receiver);
            pending.insert(
                envelope.command_id.clone(),
                PendingCommand {
                    envelope,
                    responses: vec![response],
                    retry_at: Instant::now(),
                },
            );
        }
        self.restored_incarnation
            .store(incarnation, Ordering::Release);
        Ok(())
    }

    fn next_poll_command(
        &self,
        incarnation: u64,
    ) -> Result<(Option<CommandEnvelope>, Option<Instant>), StatusCode> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let now = Instant::now();
        if let Some((_, item)) = pending.iter_mut().find(|(_, item)| {
            item.envelope.target_worker_incarnation == incarnation && item.retry_at <= now
        }) {
            item.retry_at = now + COMMAND_REDELIVERY_INTERVAL;
            Ok((Some(item.envelope.clone()), None))
        } else {
            Ok((
                None,
                pending
                    .values()
                    .filter(|item| item.envelope.target_worker_incarnation == incarnation)
                    .map(|item| item.retry_at)
                    .min(),
            ))
        }
    }

    fn mark_seen(&self) -> Result<(), StatusCode> {
        let mut last_seen = self
            .last_seen
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        *last_seen = Instant::now();
        self.connected.store(true, Ordering::Relaxed);
        Ok(())
    }

    fn report(&self, report: WorkerReport) -> Result<ReportAcknowledgement, StatusCode> {
        let mut recent = self
            .recent_reports
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if let Some(previous) = recent.iter().find(|previous| {
            previous.worker_incarnation == report.worker_incarnation
                && previous.report_sequence == report.report_sequence
        }) {
            return if previous == &report {
                Ok(ReportAcknowledgement {
                    report_sequence: report.report_sequence,
                    duplicate: true,
                })
            } else {
                Err(StatusCode::CONFLICT)
            };
        }
        let mut last_sequences = self
            .last_report_sequence
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let last_sequence = last_sequences
            .get(&report.worker_incarnation)
            .copied()
            .unwrap_or_default();
        let first_report = last_sequence == 0;
        if report.report_sequence == 0
            || (!first_report && report.report_sequence != last_sequence.saturating_add(1))
        {
            return Err(StatusCode::CONFLICT);
        }
        let acknowledgement_json =
            serde_json::to_string(&report).map_err(|_| StatusCode::CONFLICT)?;
        let durable_record = if let Some(journal) = &self.journal {
            journal
                .command_record(&report.command_id)
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        } else {
            None
        };
        if let Some(record) = &durable_record {
            if record.envelope.target_worker_id != report.worker_id
                || record.envelope.target_worker_incarnation != report.worker_incarnation
                || !result_matches(&record.envelope.command, &report.result)
            {
                return Err(StatusCode::CONFLICT);
            }
            if let Some(previous) = &record.acknowledgement_json {
                if previous != &acknowledgement_json {
                    return Err(StatusCode::CONFLICT);
                }
                self.mark_seen()?;
                return Ok(ReportAcknowledgement {
                    report_sequence: report.report_sequence,
                    duplicate: true,
                });
            }
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let item = pending.get(&report.command_id);
        if item.is_none() && durable_record.is_none() {
            return Err(StatusCode::CONFLICT);
        }
        if let Some(item) = item
            && (item.envelope.target_worker_id != report.worker_id
                || item.envelope.target_worker_incarnation != report.worker_incarnation
                || !result_matches(&item.envelope.command, &report.result))
        {
            return Err(StatusCode::CONFLICT);
        }
        if let Some(journal) = &self.journal
            && durable_record.is_some()
        {
            journal
                .acknowledge_command(
                    &report.command_id,
                    &report.worker_id,
                    report.worker_incarnation,
                    &acknowledgement_json,
                )
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        }
        let Some(item) = pending.remove(&report.command_id) else {
            last_sequences.insert(report.worker_incarnation, report.report_sequence);
            recent.push_back(report.clone());
            while recent.len() > MAX_RECENT_REPORTS {
                recent.pop_front();
            }
            self.mark_seen()?;
            return Ok(ReportAcknowledgement {
                report_sequence: report.report_sequence,
                duplicate: false,
            });
        };
        for response in item.responses {
            let _ = response.send(report.result.clone());
        }
        last_sequences.insert(report.worker_incarnation, report.report_sequence);
        recent.push_back(report.clone());
        while recent.len() > MAX_RECENT_REPORTS {
            recent.pop_front();
        }
        self.mark_seen()?;
        self.changed.notify_waiters();
        Ok(ReportAcknowledgement {
            report_sequence: report.report_sequence,
            duplicate: false,
        })
    }

    fn is_fresh(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
            && self
                .last_seen
                .lock()
                .is_ok_and(|last_seen| last_seen.elapsed() <= WORKER_FRESHNESS_TIMEOUT)
    }

    fn status(&self) -> Result<WorkerStatus, WorkerPortError> {
        self.status
            .lock()
            .map(|status| status.clone())
            .map_err(|_| WorkerPortError::Unavailable)
    }
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

/// Manager-side adapter that makes a connected remote worker usable through
/// the same explicit `WorkerPort` interface as the local worker.
pub struct RemoteWorkerPort {
    session: Arc<WorkerSession>,
}

impl RemoteWorkerPort {
    fn dispatch(&self, command: WorkerCommand) -> Result<WorkerResult, WorkerPortError> {
        let receiver = self.session.enqueue(command).map_err(map_transport_error)?;
        receiver
            .recv_timeout(COMMAND_RESPONSE_TIMEOUT)
            .map_err(|_| WorkerPortError::Unavailable)
    }

    fn observation(&self, command: WorkerCommand) -> Result<RunObservation, WorkerPortError> {
        match self.dispatch(command)? {
            WorkerResult::Observation(observation) => Ok(observation),
            WorkerResult::Error(error) => Err(error.into()),
            _ => Err(WorkerPortError::Unavailable),
        }
    }

    fn dispatch_envelope(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<WorkerResult, WorkerPortError> {
        let receiver = self
            .session
            .enqueue_envelope(envelope)
            .map_err(map_transport_error)?;
        receiver
            .recv_timeout(COMMAND_RESPONSE_TIMEOUT)
            .map_err(|_| WorkerPortError::Unavailable)
    }
}

impl WorkerPort for RemoteWorkerPort {
    fn descriptor(&self) -> WorkerDescriptor {
        self.session.descriptor()
    }

    fn is_draining(&self) -> bool {
        self.session
            .status
            .lock()
            .map(|status| status.draining())
            .unwrap_or(true)
    }

    fn is_degraded(&self) -> bool {
        !self.session.is_fresh()
            || self
                .session
                .status
                .lock()
                .map(|status| status.degraded())
                .unwrap_or(true)
    }

    fn start(&self, command: StartRun) -> Result<RunObservation, WorkerPortError> {
        self.observation(WorkerCommand::StartRun(Box::new(command)))
    }

    fn dispatch_command(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<RunObservation, WorkerPortError> {
        match self.dispatch_envelope(envelope)? {
            WorkerResult::Observation(observation) => Ok(observation),
            WorkerResult::Error(error) => Err(error.into()),
            _ => Err(WorkerPortError::Unavailable),
        }
    }

    fn inspect(&self, command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        match self.dispatch(WorkerCommand::InspectRun(command))? {
            WorkerResult::Inspection(observation) => Ok(observation),
            WorkerResult::Error(error) => Err(error.into()),
            _ => Err(WorkerPortError::Unavailable),
        }
    }

    fn stop(&self, command: StopRun) -> Result<RunObservation, WorkerPortError> {
        self.observation(WorkerCommand::StopRun(command))
    }

    fn resolve(&self, command: ResolveRun) -> Result<RunObservation, WorkerPortError> {
        self.observation(WorkerCommand::ResolveRun(command))
    }

    fn status(&self) -> Result<WorkerStatus, WorkerPortError> {
        if !self.session.is_fresh() {
            return Err(WorkerPortError::Unavailable);
        }
        self.session.status()
    }

    fn read_logs(&self, command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError> {
        match self.dispatch(WorkerCommand::ReadRunLogs(command))? {
            WorkerResult::Logs(chunk) => {
                chunk.into_chunk().map_err(|_| WorkerPortError::Unavailable)
            }
            WorkerResult::Error(error) => Err(error.into()),
            _ => Err(WorkerPortError::Unavailable),
        }
    }
}

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

/// Client certificate credentials and HTTPS endpoint for one worker.
pub struct WorkerTransportClient {
    endpoint: Url,
    client: reqwest::Client,
}

impl WorkerTransportClient {
    pub fn from_pem_identity(
        endpoint: &str,
        certificate_and_private_key_pem: &[u8],
    ) -> Result<Self, TransportError> {
        Self::build(endpoint, certificate_and_private_key_pem, None)
    }

    /// Construct a worker client using the provided bootstrap CA trust anchor.
    pub fn from_pem_identity_with_root(
        endpoint: &str,
        certificate_and_private_key_pem: &[u8],
        certificate_authority_pem: &[u8],
    ) -> Result<Self, TransportError> {
        let root = reqwest::Certificate::from_pem(certificate_authority_pem)
            .map_err(|_| TransportError::InvalidIdentity)?;
        Self::build(endpoint, certificate_and_private_key_pem, Some(root))
    }

    fn build(
        endpoint: &str,
        certificate_and_private_key_pem: &[u8],
        root: Option<reqwest::Certificate>,
    ) -> Result<Self, TransportError> {
        let endpoint = Url::parse(endpoint).map_err(|_| TransportError::InvalidEndpoint)?;
        if endpoint.scheme() != "https"
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(TransportError::InvalidEndpoint);
        }
        let identity = reqwest::Identity::from_pem(certificate_and_private_key_pem)
            .map_err(|_| TransportError::InvalidIdentity)?;
        let mut builder = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CLIENT_REQUEST_TIMEOUT)
            .identity(identity);
        if let Some(root) = root {
            builder = builder.tls_certs_only([root]);
        }
        let client = builder
            .build()
            .map_err(|_| TransportError::InvalidIdentity)?;
        Ok(Self { endpoint, client })
    }

    pub async fn poll(
        &self,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        status: &WorkerStatus,
        protocol_offer: &ProtocolOffer,
    ) -> Result<WorkerPollResponse, TransportError> {
        self.post_json(
            "v1/worker/poll",
            &WorkerPollRequest {
                worker_id: worker_id.clone(),
                worker_incarnation,
                status: status.clone(),
                protocol_offer: protocol_offer.clone(),
            },
        )
        .await
    }

    async fn send_report(
        &self,
        report: &WorkerReport,
    ) -> Result<ReportAcknowledgement, TransportError> {
        let acknowledgement: ReportAcknowledgement = self
            .post_json(
                "v1/worker/report",
                &WorkerReportRequest {
                    worker_id: report.worker_id.clone(),
                    worker_incarnation: report.worker_incarnation,
                    report: report.clone(),
                },
            )
            .await?;
        if acknowledgement.report_sequence != report.report_sequence {
            return Err(TransportError::MalformedResponse);
        }
        Ok(acknowledgement)
    }

    async fn post_json<RequestBody, ResponseBody>(
        &self,
        path: &str,
        request: &RequestBody,
    ) -> Result<ResponseBody, TransportError>
    where
        RequestBody: Serialize,
        ResponseBody: for<'de> Deserialize<'de>,
    {
        let url = self
            .endpoint
            .join(path)
            .map_err(|_| TransportError::InvalidEndpoint)?;
        let mut body = BoundedJsonBody(Vec::new());
        serde_json::to_writer(&mut body, request).map_err(|_| TransportError::InvalidCommand)?;
        let mut response = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.0)
            .send()
            .await
            .map_err(|_| TransportError::Http)?;
        if !response.status().is_success() {
            return Err(TransportError::Http);
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_TRANSPORT_BODY_BYTES as u64)
        {
            return Err(TransportError::MalformedResponse);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| TransportError::Http)? {
            if bytes.len().saturating_add(chunk.len()) > MAX_TRANSPORT_BODY_BYTES {
                return Err(TransportError::MalformedResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| TransportError::MalformedResponse)
    }

    /// Polls, executes, and reports worker commands until shutdown or until a
    /// permanent transport/configuration error occurs. Temporary network
    /// errors use bounded exponential reconnect backoff.
    pub async fn run(
        self: Arc<Self>,
        worker: Arc<WorkerService>,
        protocol_offer: ProtocolOffer,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), TransportError> {
        let descriptor = worker.descriptor();
        let worker_id = descriptor.worker_id().clone();
        let incarnation = descriptor.incarnation();
        let recovery_worker = Arc::clone(&worker);
        tokio::task::spawn_blocking(move || recover_accepted_commands(&recovery_worker))
            .await
            .map_err(|_| TransportError::Unavailable)??;
        let mut failures = 0_u32;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let pending = worker
                .pending_reports()
                .map_err(|_| TransportError::Unavailable)?;
            if let Some(stored) = pending.into_iter().next() {
                let report = report_from_stored(stored)?;
                loop {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        result = self.send_report(&report) => match result {
                            Ok(_) => {
                                worker.acknowledge_report(
                                    &report.command_id,
                                    report.worker_incarnation,
                                    report.report_sequence,
                                ).map_err(|_| TransportError::Unavailable)?;
                                failures = 0;
                                break;
                            }
                            Err(_) => {
                                failures = failures.saturating_add(1);
                                tokio::time::sleep(backoff(failures)).await;
                            }
                        }
                    }
                }
                continue;
            }
            let polled = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
                result = async {
                    let status = worker.status().map_err(|_| TransportError::Unavailable)?;
                    self.poll(&worker_id, incarnation, &status, &protocol_offer).await
                } => result,
            };
            match polled {
                Ok(response) => {
                    failures = 0;
                    let Some(command) = response.command else {
                        continue;
                    };
                    if command.target_worker_id != worker_id
                        || command.target_worker_incarnation != incarnation
                    {
                        return Err(TransportError::WorkerConflict);
                    }
                    let worker_for_command = Arc::clone(&worker);
                    let report = tokio::task::spawn_blocking(move || {
                        let accepted = worker_for_command.accept_command(&command)?;
                        if let Some(stored) = accepted {
                            return Ok::<_, WorkerError>(stored);
                        }
                        let result =
                            execute_command(worker_for_command.as_ref(), command.command.clone());
                        let result_json = serde_json::to_string(&result)
                            .map_err(|_| WorkerError::StorageUnavailable)?;
                        worker_for_command.complete_command(&command, &result_json)
                    })
                    .await
                    .map_err(|_| TransportError::Unavailable)?
                    .map_err(|_| TransportError::Unavailable)?;
                    let report = report_from_stored(report)?;
                    loop {
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    return Ok(());
                                }
                            }
                            result = self.send_report(&report) => match result {
                                Ok(_) => {
                                    worker.acknowledge_report(
                                        &report.command_id,
                                        report.worker_incarnation,
                                        report.report_sequence,
                                    ).map_err(|_| TransportError::Unavailable)?;
                                    break;
                                }
                                Err(_) => {
                                    failures = failures.saturating_add(1);
                                    tokio::time::sleep(backoff(failures)).await;
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    failures = failures.saturating_add(1);
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                        _ = tokio::time::sleep(backoff(failures)) => {}
                    }
                }
            }
        }
    }
}

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

struct BoundedJsonBody(Vec<u8>);

impl Write for BoundedJsonBody {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let new_len = self
            .0
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_TRANSPORT_BODY_BYTES)
            .ok_or_else(|| io::Error::other("transport request exceeds body limit"))?;
        self.0
            .try_reserve(new_len - self.0.len())
            .map_err(io::Error::other)?;
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

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
    use conductor_core::{ProtocolVersionRange, WorkerDescriptor};
    use conductor_manager::{JobRepository, SqliteJobRepository};
    use tower::ServiceExt;

    struct TestDatabase(PathBuf);

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
            ]),
            BTreeSet::new(),
        )
        .expect("valid protocol offer")
    }

    fn session() -> WorkerSession {
        WorkerSession::new(
            WorkerDescriptor::new(worker_id("worker-a"), 4, true).with_incarnation(9),
            offer(1),
            None,
        )
    }

    fn uninitialized_session() -> WorkerSession {
        WorkerSession::new(
            WorkerDescriptor::new(worker_id("worker-a"), 4, true),
            offer(1),
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
            status: WorkerStatus::new(descriptor, 0, 0, true, false, 1),
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
            .poll(poll_request("worker-a", 1))
            .await
            .expect("compatible worker poll");
        let first_command = first.command.expect("queued command");
        assert_eq!(first.protocol_version, 1);
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
            .poll(poll_request("worker-a", 1))
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
            .poll(poll_request("worker-a", 1))
            .await
            .expect("compatible worker poll");
        assert_eq!(response.command, Some(envelope));
    }

    #[tokio::test]
    async fn rejects_wrong_incarnation_and_incompatible_protocol() {
        let session = session();
        let mut wrong_incarnation = poll_request("worker-a", 1);
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

        let mut invalid_status = poll_request("worker-a", 1);
        invalid_status.status = WorkerStatus::new(
            WorkerDescriptor::new(worker_id("worker-a"), 8, true).with_incarnation(9),
            0,
            0,
            true,
            false,
            1,
        );
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
            tokio::spawn(async move { poll_session.poll(poll_request("worker-a", 1)).await });
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
        let polling_session = Arc::clone(&session);
        let poll = tokio::spawn(async move {
            polling_session
                .poll(poll_request_with_incarnation("worker-a", 1, 10))
                .await
        });
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
            offer(1),
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
            offer(1),
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
            offer(1),
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
                offer(1),
            )
            .expect("worker within registry bound");
        }
        assert!(matches!(
            hub.register_worker(
                WorkerDescriptor::new(worker_id("worker-overflow"), 1, true),
                offer(1),
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

    struct TestAuthenticator(Option<WorkerId>);

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
            offer(1),
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
            offer(1),
        )
        .expect("register authenticated worker");
        let router = hub.router(Arc::new(TestAuthenticator(Some(worker_id("worker-a")))));

        let poll = serde_json::to_vec(&poll_request("worker-b", 1)).expect("serialize poll");
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
