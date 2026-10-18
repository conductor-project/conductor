//! Bounded, worker-initiated HTTP transport for manager/worker commands.

use std::collections::{BTreeMap, VecDeque};
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, SyncSender};
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
use conductor_manager::{WorkerPort, WorkerPortError};
use conductor_worker::{WorkerError, WorkerService};
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "payload",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum WorkerCommand {
    StartRun(Box<StartRun>),
    InspectRun(InspectRun),
    StopRun(StopRun),
    ResolveRun(ResolveRun),
    ReadRunLogs(ReadRunLogs),
}

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
pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub target_worker_id: WorkerId,
    pub target_worker_incarnation: u64,
    pub command: WorkerCommand,
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
}

impl Default for WorkerTransportHub {
    fn default() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl WorkerTransportHub {
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
        let session = Arc::new(WorkerSession::new(descriptor, protocol_offer));
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
    response: SyncSender<WorkerResult>,
    retry_at: Instant,
}

struct WorkerSession {
    descriptor: WorkerDescriptor,
    protocol_offer: ProtocolOffer,
    status: Mutex<WorkerStatus>,
    pending: Mutex<BTreeMap<CommandId, PendingCommand>>,
    last_report_sequence: Mutex<u64>,
    recent_reports: Mutex<VecDeque<WorkerReport>>,
    changed: Notify,
    poll_gate: Semaphore,
    last_seen: Mutex<Instant>,
    connected: AtomicBool,
    next_command: AtomicU64,
}

impl WorkerSession {
    fn new(descriptor: WorkerDescriptor, protocol_offer: ProtocolOffer) -> Self {
        Self {
            status: Mutex::new(WorkerStatus::new(descriptor.clone(), 0, 0, false, false, 0)),
            descriptor,
            protocol_offer,
            pending: Mutex::new(BTreeMap::new()),
            last_report_sequence: Mutex::new(0),
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

    fn enqueue(
        &self,
        command: WorkerCommand,
    ) -> Result<mpsc::Receiver<WorkerResult>, TransportError> {
        let command_id = self.command_id()?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let envelope = CommandEnvelope {
            command_id: command_id.clone(),
            target_worker_id: self.descriptor.worker_id().clone(),
            target_worker_incarnation: self.descriptor.incarnation(),
            command,
        };
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| TransportError::Unavailable)?;
        if pending.len() >= MAX_PENDING_COMMANDS {
            return Err(TransportError::QueueFull);
        }
        pending.insert(
            command_id,
            PendingCommand {
                envelope,
                response: sender,
                retry_at: Instant::now(),
            },
        );
        drop(pending);
        self.changed.notify_one();
        Ok(receiver)
    }

    async fn poll(&self, request: WorkerPollRequest) -> Result<WorkerPollResponse, StatusCode> {
        if request.worker_incarnation != self.descriptor.incarnation() {
            return Err(StatusCode::CONFLICT);
        }
        if request.status.worker_id() != self.descriptor.worker_id()
            || request.status.incarnation() != self.descriptor.incarnation()
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
        *self
            .status
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)? = request.status;
        self.mark_seen()?;

        let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
        let command = loop {
            let notified = self.changed.notified();
            let (ready, next_retry) = self.next_poll_command()?;
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

    fn next_poll_command(&self) -> Result<(Option<CommandEnvelope>, Option<Instant>), StatusCode> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let now = Instant::now();
        if let Some((_, item)) = pending.iter_mut().find(|(_, item)| item.retry_at <= now) {
            item.retry_at = now + COMMAND_REDELIVERY_INTERVAL;
            Ok((Some(item.envelope.clone()), None))
        } else {
            Ok((None, pending.values().map(|item| item.retry_at).min()))
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
        if report.worker_incarnation != self.descriptor.incarnation() {
            return Err(StatusCode::CONFLICT);
        }
        let mut recent = self
            .recent_reports
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if let Some(previous) = recent
            .iter()
            .find(|previous| previous.report_sequence == report.report_sequence)
        {
            return if previous == &report {
                Ok(ReportAcknowledgement {
                    report_sequence: report.report_sequence,
                    duplicate: true,
                })
            } else {
                Err(StatusCode::CONFLICT)
            };
        }
        let mut last_sequence = self
            .last_report_sequence
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if report.report_sequence != last_sequence.saturating_add(1) {
            return Err(StatusCode::CONFLICT);
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let Some(item) = pending.get(&report.command_id) else {
            return Err(StatusCode::CONFLICT);
        };
        if item.envelope.target_worker_id != report.worker_id
            || item.envelope.target_worker_incarnation != report.worker_incarnation
            || !result_matches(&item.envelope.command, &report.result)
        {
            return Err(StatusCode::CONFLICT);
        }
        let Some(item) = pending.remove(&report.command_id) else {
            return Err(StatusCode::CONFLICT);
        };
        let _ = item.response.send(report.result.clone());
        *last_sequence = report.report_sequence;
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
}

impl WorkerPort for RemoteWorkerPort {
    fn descriptor(&self) -> WorkerDescriptor {
        self.session.descriptor.clone()
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
    report_sequence: AtomicU64,
    recent_reports: Mutex<VecDeque<WorkerReport>>,
}

impl WorkerTransportClient {
    pub fn from_pem_identity(
        endpoint: &str,
        certificate_and_private_key_pem: &[u8],
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
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CLIENT_REQUEST_TIMEOUT)
            .identity(identity)
            .build()
            .map_err(|_| TransportError::InvalidIdentity)?;
        Ok(Self {
            endpoint,
            client,
            report_sequence: AtomicU64::new(0),
            recent_reports: Mutex::new(VecDeque::new()),
        })
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

    async fn send_report(&self, report: &WorkerReport) -> Result<(), TransportError> {
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
        Ok(())
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
        let mut failures = 0_u32;
        loop {
            if *shutdown.borrow() {
                return Ok(());
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
                    let cached = self.cached_report(&command.command_id);
                    let report = match cached {
                        Some(report) => report,
                        None => {
                            let command_id = command.command_id.clone();
                            let worker_id = worker_id.clone();
                            let worker = Arc::clone(&worker);
                            let sequence = self.next_report_sequence()?;
                            let result = tokio::task::spawn_blocking(move || {
                                execute_command(worker.as_ref(), command.command)
                            })
                            .await
                            .map_err(|_| TransportError::Unavailable)?;
                            let report = WorkerReport {
                                command_id,
                                worker_id,
                                worker_incarnation: incarnation,
                                report_sequence: sequence,
                                result,
                            };
                            self.cache_report(report.clone())?;
                            report
                        }
                    };
                    loop {
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    return Ok(());
                                }
                            }
                            result = self.send_report(&report) => match result {
                                Ok(()) => break,
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

    fn next_report_sequence(&self) -> Result<u64, TransportError> {
        self.report_sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                sequence.checked_add(1)
            })
            .map(|sequence| sequence + 1)
            .map_err(|_| TransportError::Unavailable)
    }

    fn cached_report(&self, command_id: &CommandId) -> Option<WorkerReport> {
        self.recent_reports
            .lock()
            .ok()?
            .iter()
            .find(|report| &report.command_id == command_id)
            .cloned()
    }

    fn cache_report(&self, report: WorkerReport) -> Result<(), TransportError> {
        let mut reports = self
            .recent_reports
            .lock()
            .map_err(|_| TransportError::Unavailable)?;
        reports.push_back(report);
        while reports.len() > MAX_RECENT_REPORTS {
            reports.pop_front();
        }
        Ok(())
    }
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

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use conductor_core::{ProtocolVersionRange, WorkerDescriptor};
    use tower::ServiceExt;

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
        )
    }

    fn poll_request(id: &str, version: u16) -> WorkerPollRequest {
        let descriptor = WorkerDescriptor::new(worker_id(id), 4, true).with_incarnation(9);
        WorkerPollRequest {
            worker_id: worker_id(id),
            worker_incarnation: 9,
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

    struct TestAuthenticator(Option<WorkerId>);

    impl WorkerAuthenticator for TestAuthenticator {
        fn authenticate(&self, _request: &Request<Body>) -> Option<WorkerId> {
            self.0.clone()
        }
    }

    fn request(body: impl Into<Body>) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/worker/poll")
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
