//! Shared, transport-neutral identifiers and contracts for Conductor.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};

const MAX_IDENTIFIER_LENGTH: usize = 128;
const DEFAULT_TERMINATION_GRACE_MS: u64 = 5_000;
const MAX_TERMINATION_GRACE_MS: u64 = 300_000;
const MAX_TIMEOUT_MS: u64 = 604_800_000;
pub const MAX_WORKER_CAPACITY: usize = 1_000;
pub const DEFAULT_LOG_CHUNK_BYTES: usize = 4_096;
pub const MAX_LOG_CHUNK_BYTES: usize = 65_536;

/// An identifier failed the stable ASCII identifier format.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidIdentifier;

impl fmt::Display for InvalidIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("identifier must contain 1-128 ASCII letters, digits, '-', '_' or '.'")
    }
}

impl std::error::Error for InvalidIdentifier {}

macro_rules! stable_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Validate and construct a stable identifier.
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidIdentifier> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > MAX_IDENTIFIER_LENGTH
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                {
                    return Err(InvalidIdentifier);
                }
                Ok(Self(value))
            }

            /// Borrow the identifier without allocating.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidIdentifier;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(serde::de::Error::custom)
            }
        }
    };
}

stable_id!(JobId);
stable_id!(RunId);
stable_id!(WorkerId);

/// Local worker registration advertised through the manager-worker contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerDescriptor {
    worker_id: WorkerId,
    capacity: usize,
    process_execution: bool,
    #[serde(default)]
    incarnation: u64,
}

impl WorkerDescriptor {
    pub fn new(worker_id: WorkerId, capacity: usize, process_execution: bool) -> Self {
        Self {
            worker_id,
            capacity,
            process_execution,
            incarnation: 0,
        }
    }
    pub fn with_incarnation(mut self, incarnation: u64) -> Self {
        self.incarnation = incarnation;
        self
    }
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn supports_process_execution(&self) -> bool {
        self.process_execution
    }
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }
}

/// Bounded operational snapshot for one locally registered worker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerStatus {
    worker_id: WorkerId,
    incarnation: u64,
    process_execution: bool,
    capacity: usize,
    used_slots: usize,
    live_processes: usize,
    unknown_runs: usize,
    pending_persistence: usize,
    available_slots: usize,
    available: bool,
    degraded: bool,
    draining: bool,
    observed_at_ms: i64,
}

impl WorkerStatus {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        descriptor: WorkerDescriptor,
        used_slots: usize,
        unknown_runs: usize,
        available: bool,
        draining: bool,
        observed_at_ms: i64,
    ) -> Self {
        let capacity = descriptor.capacity();
        Self {
            worker_id: descriptor.worker_id().clone(),
            incarnation: descriptor.incarnation(),
            process_execution: descriptor.supports_process_execution(),
            capacity,
            used_slots: used_slots.min(capacity),
            live_processes: used_slots.min(capacity),
            unknown_runs: unknown_runs.min(used_slots),
            pending_persistence: 0,
            available_slots: capacity.saturating_sub(used_slots),
            available,
            degraded: false,
            draining,
            observed_at_ms,
        }
    }

    pub fn with_runtime_state(
        mut self,
        live_processes: usize,
        pending_persistence: usize,
        degraded: bool,
    ) -> Self {
        self.live_processes = live_processes.min(self.capacity);
        self.pending_persistence = pending_persistence.min(self.capacity);
        self.degraded = degraded;
        if degraded {
            self.available = false;
        }
        self
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }
    pub fn supports_process_execution(&self) -> bool {
        self.process_execution
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
    pub fn used_slots(&self) -> usize {
        self.used_slots
    }
    pub fn live_processes(&self) -> usize {
        self.live_processes
    }
    pub fn unknown_runs(&self) -> usize {
        self.unknown_runs
    }
    pub fn pending_persistence(&self) -> usize {
        self.pending_persistence
    }
    pub fn available_slots(&self) -> usize {
        self.available_slots
    }
    pub fn available(&self) -> bool {
        self.available
    }
    pub fn degraded(&self) -> bool {
        self.degraded
    }
    pub fn draining(&self) -> bool {
        self.draining
    }
    pub fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
}

/// A fixed job-definition snapshot assigned to one worker execution attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunSpec {
    id: RunId,
    worker_id: WorkerId,
    job: JobSpec,
}

impl RunSpec {
    pub fn new(id: RunId, worker_id: WorkerId, job: JobSpec) -> Self {
        Self { id, worker_id, job }
    }

    pub fn id(&self) -> &RunId {
        &self.id
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn job(&self) -> &JobSpec {
        &self.job
    }
}

/// The manager's durable view of a run, updated only by increasing revisions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Requested,
    Starting,
    Running,
    Succeeded,
    Failed,
    Stopping,
    Cancelled,
    TimedOut,
    Unknown,
    Abandoned,
}

impl RunState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::TimedOut | Self::Abandoned
        )
    }

    pub fn is_active_or_unresolved(self) -> bool {
        !self.is_terminal()
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Stopping => "stopping",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Unknown => "unknown",
            Self::Abandoned => "abandoned",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunFailureReason {
    SpawnFailed,
    NonZeroExit,
    SignalTerminated,
    CapacityRejected,
    BackendFailure,
    OutcomeUnknown,
}

/// Why the worker initiated process termination, independent of the final exit status.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTerminationReason {
    Cancelled,
    DeadlineExceeded,
    Shutdown,
}

/// Serializable worker observation. Process IDs are deliberately not part of the contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunObservation {
    run_id: RunId,
    job_id: JobId,
    worker_id: WorkerId,
    revision: u64,
    state: RunState,
    requested_at_ms: i64,
    started_at_ms: Option<i64>,
    finished_at_ms: Option<i64>,
    exit_code: Option<i32>,
    termination_signal: Option<i32>,
    failure_reason: Option<RunFailureReason>,
    #[serde(default)]
    termination_reason: Option<RunTerminationReason>,
    #[serde(default)]
    resolution_reason: Option<String>,
    #[serde(default)]
    worker_incarnation: u64,
}

impl RunObservation {
    pub fn requested(spec: &RunSpec, requested_at_ms: i64) -> Self {
        Self {
            run_id: spec.id.clone(),
            job_id: spec.job.id.clone(),
            worker_id: spec.worker_id.clone(),
            revision: 1,
            state: RunState::Requested,
            requested_at_ms,
            started_at_ms: None,
            finished_at_ms: None,
            exit_code: None,
            termination_signal: None,
            failure_reason: None,
            termination_reason: None,
            resolution_reason: None,
            worker_incarnation: 0,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn state(&self) -> RunState {
        self.state
    }
    pub fn requested_at_ms(&self) -> i64 {
        self.requested_at_ms
    }
    pub fn started_at_ms(&self) -> Option<i64> {
        self.started_at_ms
    }
    pub fn finished_at_ms(&self) -> Option<i64> {
        self.finished_at_ms
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }
    pub fn termination_signal(&self) -> Option<i32> {
        self.termination_signal
    }
    pub fn failure_reason(&self) -> Option<RunFailureReason> {
        self.failure_reason
    }
    pub fn termination_reason(&self) -> Option<RunTerminationReason> {
        self.termination_reason
    }
    pub fn resolution_reason(&self) -> Option<&str> {
        self.resolution_reason.as_deref()
    }

    pub fn with_resolution_reason(mut self, reason: String) -> Self {
        self.resolution_reason = Some(reason);
        self
    }
    pub fn with_worker_incarnation(mut self, incarnation: u64) -> Self {
        self.worker_incarnation = incarnation;
        self
    }
    pub fn worker_incarnation(&self) -> u64 {
        self.worker_incarnation
    }

    pub fn with_termination_reason(mut self, reason: RunTerminationReason) -> Self {
        self.termination_reason = Some(reason);
        self
    }
    pub fn with_optional_termination_reason(
        mut self,
        reason: Option<RunTerminationReason>,
    ) -> Self {
        self.termination_reason = reason;
        self
    }

    pub fn next_revision(
        &self,
        state: RunState,
        started_at_ms: Option<i64>,
        finished_at_ms: Option<i64>,
        exit_code: Option<i32>,
        termination_signal: Option<i32>,
        failure_reason: Option<RunFailureReason>,
    ) -> Option<Self> {
        Some(Self {
            run_id: self.run_id.clone(),
            job_id: self.job_id.clone(),
            worker_id: self.worker_id.clone(),
            revision: self.revision.checked_add(1)?,
            state,
            requested_at_ms: self.requested_at_ms,
            started_at_ms,
            finished_at_ms,
            exit_code,
            termination_signal,
            failure_reason,
            termination_reason: self.termination_reason,
            resolution_reason: self.resolution_reason.clone(),
            worker_incarnation: self.worker_incarnation,
        })
    }

    /// Apply an observation only if it is newer than the currently accepted revision.
    pub fn apply_newer(&mut self, newer: Self) -> bool {
        if self.state.is_terminal()
            || newer.run_id != self.run_id
            || newer.job_id != self.job_id
            || newer.worker_id != self.worker_id
            || newer.revision <= self.revision
        {
            return false;
        }
        *self = newer;
        true
    }
}

/// Idempotent request to stop one accepted run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StopRun {
    run_id: RunId,
}

/// Operator-authorized resolution of an ambiguous run after external cleanup.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolveRun {
    run_id: RunId,
    reason: String,
}

impl ResolveRun {
    pub fn new(run_id: RunId, reason: String) -> Self {
        Self { run_id, reason }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// One worker-owned output stream for a run.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogStream {
    Stdout,
    Stderr,
}

impl LogStream {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        }
    }
}

/// Bounded byte-range read from one run output stream.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadRunLogs {
    run_id: RunId,
    stream: LogStream,
    offset: u64,
    limit: usize,
}

impl ReadRunLogs {
    pub fn new(run_id: RunId, stream: LogStream, offset: u64, limit: usize) -> Self {
        Self {
            run_id,
            stream,
            offset,
            limit,
        }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn stream(&self) -> LogStream {
        self.stream
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Reply to a bounded log read. `data` contains raw bytes for transport encoding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunLogChunk {
    run_id: RunId,
    stream: LogStream,
    offset: u64,
    next_offset: u64,
    data: Vec<u8>,
    stream_closed: bool,
    eof: bool,
    truncated: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunLogStatus {
    stream_closed: bool,
    eof: bool,
    truncated: bool,
}

impl RunLogStatus {
    pub fn new(stream_closed: bool, eof: bool, truncated: bool) -> Self {
        Self {
            stream_closed,
            eof,
            truncated,
        }
    }
}

impl RunLogChunk {
    pub fn new(
        run_id: RunId,
        stream: LogStream,
        offset: u64,
        next_offset: u64,
        data: Vec<u8>,
        status: RunLogStatus,
    ) -> Self {
        Self {
            run_id,
            stream,
            offset,
            next_offset,
            data,
            stream_closed: status.stream_closed,
            eof: status.eof,
            truncated: status.truncated,
        }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn stream(&self) -> LogStream {
        self.stream
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
    pub fn data(&self) -> &[u8] {
        &self.data
    }
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
    pub fn stream_closed(&self) -> bool {
        self.stream_closed
    }
    pub fn eof(&self) -> bool {
        self.eof
    }
    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

impl StopRun {
    pub fn new(run_id: RunId) -> Self {
        Self { run_id }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartRun {
    spec: RunSpec,
}

impl StartRun {
    pub fn new(spec: RunSpec) -> Self {
        Self { spec }
    }
    pub fn spec(&self) -> &RunSpec {
        &self.spec
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InspectRun {
    run_id: RunId,
}

impl InspectRun {
    pub fn new(run_id: RunId) -> Self {
        Self { run_id }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
}

/// Invalid process/job specification input. Messages never include submitted values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobValidationError {
    EmptyCommand,
    ExecutableMustBeAbsolute,
    WorkingDirectoryMustBeAbsolute,
    NulByte,
    InvalidEnvironmentName,
    InvalidEnvironmentValue,
    InvalidTimeout,
    InvalidTerminationGrace,
}

impl fmt::Display for JobValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyCommand => "command must contain an executable",
            Self::ExecutableMustBeAbsolute => "command executable must be an absolute path",
            Self::WorkingDirectoryMustBeAbsolute => "cwd must be an absolute path",
            Self::NulByte => "process arguments and paths must not contain NUL bytes",
            Self::InvalidEnvironmentName => "environment names must be valid ASCII identifiers",
            Self::InvalidEnvironmentValue => "environment values must not contain NUL bytes",
            Self::InvalidTimeout => "timeoutMs must be between 1 and 604800000",
            Self::InvalidTerminationGrace => "terminationGraceMs must be between 0 and 300000",
        })
    }
}

impl std::error::Error for JobValidationError {}

/// An immutable, validated process definition. Registration never executes it.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobSpec {
    id: JobId,
    command: Vec<String>,
    cwd: PathBuf,
    env: BTreeMap<String, String>,
    inherit_env: bool,
    timeout_ms: Option<u64>,
    termination_grace_ms: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawJobSpec {
    id: JobId,
    command: Vec<String>,
    cwd: PathBuf,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    inherit_env: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default = "default_termination_grace_ms")]
    termination_grace_ms: u64,
}

impl JobSpec {
    /// Validate and construct a job specification.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: JobId,
        command: Vec<String>,
        cwd: PathBuf,
        env: BTreeMap<String, String>,
        inherit_env: bool,
        timeout_ms: Option<u64>,
        termination_grace_ms: u64,
    ) -> Result<Self, JobValidationError> {
        if command.is_empty() {
            return Err(JobValidationError::EmptyCommand);
        }
        if !Path::new(&command[0]).is_absolute() {
            return Err(JobValidationError::ExecutableMustBeAbsolute);
        }
        if !cwd.is_absolute() {
            return Err(JobValidationError::WorkingDirectoryMustBeAbsolute);
        }
        if command.iter().any(|part| part.as_bytes().contains(&0))
            || cwd.to_string_lossy().as_bytes().contains(&0)
        {
            return Err(JobValidationError::NulByte);
        }
        if env.keys().any(|name| !valid_environment_name(name)) {
            return Err(JobValidationError::InvalidEnvironmentName);
        }
        if env.values().any(|value| value.as_bytes().contains(&0)) {
            return Err(JobValidationError::InvalidEnvironmentValue);
        }
        if timeout_ms.is_some_and(|value| value == 0 || value > MAX_TIMEOUT_MS) {
            return Err(JobValidationError::InvalidTimeout);
        }
        if termination_grace_ms > MAX_TERMINATION_GRACE_MS {
            return Err(JobValidationError::InvalidTerminationGrace);
        }

        Ok(Self {
            id,
            command,
            cwd,
            env,
            inherit_env,
            timeout_ms,
            termination_grace_ms,
        })
    }

    pub fn id(&self) -> &JobId {
        &self.id
    }
    pub fn command(&self) -> &[String] {
        &self.command
    }
    pub fn cwd(&self) -> &Path {
        &self.cwd
    }
    pub fn env(&self) -> &BTreeMap<String, String> {
        &self.env
    }
    pub fn inherit_env(&self) -> bool {
        self.inherit_env
    }
    pub fn timeout_ms(&self) -> Option<u64> {
        self.timeout_ms
    }
    pub fn termination_grace_ms(&self) -> u64 {
        self.termination_grace_ms
    }
}

impl TryFrom<RawJobSpec> for JobSpec {
    type Error = JobValidationError;

    fn try_from(raw: RawJobSpec) -> Result<Self, Self::Error> {
        Self::new(
            raw.id,
            raw.command,
            raw.cwd,
            raw.env,
            raw.inherit_env,
            raw.timeout_ms,
            raw.termination_grace_ms,
        )
    }
}

impl<'de> Deserialize<'de> for JobSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawJobSpec::deserialize(deserializer)?;
        Self::try_from(raw).map_err(serde::de::Error::custom)
    }
}

fn default_termination_grace_ms() -> u64 {
    DEFAULT_TERMINATION_GRACE_MS
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
