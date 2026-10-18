//! Shared, transport-neutral identifiers and contracts for Conductor.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};

const MAX_IDENTIFIER_LENGTH: usize = 128;
const MAX_FILESYSTEM_GRANTS_PER_JOB: usize = 64;
const DEFAULT_TERMINATION_GRACE_MS: u64 = 5_000;
const MAX_TERMINATION_GRACE_MS: u64 = 300_000;
const MAX_TIMEOUT_MS: u64 = 604_800_000;
pub const MAX_WORKER_CAPACITY: usize = 64;
pub const MAX_WORKER_INVENTORY_PAGE_SIZE: usize = 32;
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
stable_id!(CommandId);
stable_id!(WorkerId);

/// Transport-neutral command sent by a manager to one worker.
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

/// Stable manager-owned command identity and worker-incarnation target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub target_worker_id: WorkerId,
    pub target_worker_incarnation: u64,
    pub command: WorkerCommand,
}

mod execution;
mod inventory;
mod protocol;
pub use execution::{
    Allocation, AllocationId, AllocationState, Architecture, AssignedServicePort, AttemptState,
    DeploymentMutation, DeploymentPendingReason, DeploymentRollout, DeploymentStatus,
    DeploymentUpdateStrategy, DigestAlgorithm, EnforcementCapability, ExecutableDigest,
    ExecutionAttempt, ExecutionAttemptId, ExecutionBackend, ExecutionModelError,
    ExecutionRequirements, JobDeployment, JobRevision, JobRevisionDraft, JobRevisionId,
    MAX_DEPLOYMENT_REPLICAS, OperatingSystem, ReadinessProbe, ReadinessProbeProtocol,
    ResourceRequest, ResourceRequestProvenance, ResourceReservation, ServiceIdentity,
    ServiceInstance, ServiceInstanceId, ServicePort, ServicePortProtocol, WorkerCapabilities,
    Workload,
};
pub use inventory::{
    WorkerInventory, WorkerInventoryEntry, WorkerInventoryError, WorkerInventoryPage,
    WorkerMetadata, WorkerResourceCapacity, WorkerServiceNetwork,
};
pub use protocol::{
    CURRENT_PROTOCOL_VERSION, NegotiatedProtocol, ProtocolCapability, ProtocolNegotiationError,
    ProtocolOffer, ProtocolVersionRange,
};

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
    #[serde(default)]
    inventory: Option<WorkerInventory>,
    #[serde(default)]
    resource_domain_id: Option<WorkerId>,
    #[serde(default)]
    resource_domain_capacity: Option<WorkerResourceCapacity>,
    #[serde(default)]
    labels: BTreeMap<String, String>,
    #[serde(default)]
    fresh: bool,
    #[serde(default)]
    last_seen_at_ms: Option<i64>,
    #[serde(default)]
    service_network: Option<WorkerServiceNetwork>,
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
            inventory: None,
            resource_domain_id: None,
            resource_domain_capacity: None,
            labels: BTreeMap::new(),
            fresh: false,
            last_seen_at_ms: None,
            service_network: None,
        }
    }

    pub fn with_inventory(mut self, inventory: WorkerInventory) -> Self {
        self.inventory = Some(inventory);
        self
    }

    pub fn with_freshness(mut self, fresh: bool) -> Self {
        self.fresh = fresh;
        self
    }

    /// Overlay manager-owned drain intent on the worker's reported snapshot.
    pub fn with_draining(mut self) -> Self {
        self.draining = true;
        self.available = false;
        self
    }

    /// Attach manager-configured, disjoint host-port allocation metadata.
    pub fn with_service_network(mut self, service_network: WorkerServiceNetwork) -> Option<Self> {
        if !service_network.is_valid() {
            return None;
        }
        self.service_network = Some(service_network);
        Some(self)
    }

    pub fn service_network(&self) -> Option<&WorkerServiceNetwork> {
        self.service_network.as_ref()
    }

    /// Attach manager-controlled labels, resource-domain metadata, and freshness.
    pub fn with_manager_metadata(
        mut self,
        resource_domain_id: WorkerId,
        resource_domain_capacity: WorkerResourceCapacity,
        labels: BTreeMap<String, String>,
        last_seen_at_ms: i64,
        fresh: bool,
    ) -> Option<Self> {
        if !valid_worker_labels(&labels) || last_seen_at_ms < 0 {
            return None;
        }
        self.inventory.as_ref()?;
        self.resource_domain_id = Some(resource_domain_id);
        self.resource_domain_capacity = Some(resource_domain_capacity);
        self.labels = labels;
        self.service_network = None;
        self.last_seen_at_ms = Some(last_seen_at_ms);
        self.fresh = fresh;
        Some(self)
    }

    /// Validate all worker-reported and manager-enriched fields before use.
    pub fn is_valid(&self) -> bool {
        if self.capacity == 0
            || self.capacity > MAX_WORKER_CAPACITY
            || self.used_slots > self.capacity
            || self.available_slots != self.capacity.saturating_sub(self.used_slots)
            || self.unknown_runs > self.used_slots
            || self.live_processes > self.capacity
            || self.pending_persistence > self.capacity
            || self.observed_at_ms < 0
            || !valid_worker_labels(&self.labels)
            || self.last_seen_at_ms.is_some_and(|timestamp| timestamp < 0)
            || self
                .service_network
                .as_ref()
                .is_some_and(|network| !network.is_valid())
        {
            return false;
        }
        if let Some(inventory) = &self.inventory
            && (inventory.execution_slots() != self.capacity || inventory.capabilities().is_err())
        {
            return false;
        }
        match (&self.resource_domain_id, self.resource_domain_capacity) {
            // A resource domain may represent a physical host shared by
            // several worker processes, so its aggregate budget need not fit
            // within any single worker's allocatable capacity.
            (Some(_), Some(_)) => self.inventory.is_some(),
            (None, None) => self.labels.is_empty() && self.last_seen_at_ms.is_none() && !self.fresh,
            _ => false,
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
    pub fn inventory(&self) -> Option<&WorkerInventory> {
        self.inventory.as_ref()
    }
    pub fn resource_domain_id(&self) -> Option<&WorkerId> {
        self.resource_domain_id.as_ref()
    }
    pub fn resource_domain_capacity(&self) -> Option<WorkerResourceCapacity> {
        self.resource_domain_capacity
    }
    pub fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }
    pub fn fresh(&self) -> bool {
        self.fresh
    }
    pub fn last_seen_at_ms(&self) -> Option<i64> {
        self.last_seen_at_ms
    }
    pub fn observed_at_ms(&self) -> i64 {
        self.observed_at_ms
    }
}

pub const MAX_WORKER_LABELS: usize = 32;
pub const MAX_WORKER_LABEL_BYTES: usize = 4_096;

pub fn valid_worker_labels(labels: &BTreeMap<String, String>) -> bool {
    labels.len() <= MAX_WORKER_LABELS
        && labels.iter().all(|(key, value)| {
            !key.is_empty()
                && key.len() <= 64
                && key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                && value.len() <= 256
                && !value.bytes().any(|byte| byte.is_ascii_control())
        })
        && labels
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>()
            <= MAX_WORKER_LABEL_BYTES
}

/// A fixed job-definition snapshot assigned to one worker execution attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "RawRunSpec")]
pub struct RunSpec {
    id: RunId,
    worker_id: WorkerId,
    job: JobSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    service_ports: Vec<AssignedServicePort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    readiness_probe: Option<ReadinessProbe>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawRunSpec {
    id: RunId,
    worker_id: WorkerId,
    job: JobSpec,
    #[serde(default)]
    service_ports: Vec<AssignedServicePort>,
    #[serde(default)]
    readiness_probe: Option<ReadinessProbe>,
}

impl TryFrom<RawRunSpec> for RunSpec {
    type Error = ExecutionModelError;

    fn try_from(raw: RawRunSpec) -> Result<Self, Self::Error> {
        validate_assigned_service_ports(&raw.job, &raw.service_ports)?;
        validate_readiness_probe(raw.readiness_probe.as_ref(), &raw.service_ports)?;
        Ok(Self {
            id: raw.id,
            worker_id: raw.worker_id,
            job: raw.job,
            service_ports: raw.service_ports,
            readiness_probe: raw.readiness_probe,
        })
    }
}

fn validate_readiness_probe(
    probe: Option<&ReadinessProbe>,
    service_ports: &[AssignedServicePort],
) -> Result<(), ExecutionModelError> {
    if let Some(probe) = probe
        && (!probe.is_valid()
            || !service_ports
                .iter()
                .any(|port| port.name() == probe.port_name()))
    {
        return Err(ExecutionModelError::InvalidReadinessProbe);
    }
    Ok(())
}

fn validate_assigned_service_ports(
    job: &JobSpec,
    service_ports: &[AssignedServicePort],
) -> Result<(), ExecutionModelError> {
    if service_ports.len() > 32 {
        return Err(ExecutionModelError::InvalidServicePort);
    }
    let mut names = BTreeSet::new();
    let mut environment_names = BTreeSet::new();
    for port in service_ports {
        let environment_name = port.environment_name();
        if !port.is_valid()
            || !names.insert(port.name())
            || !environment_names.insert(environment_name.clone())
            || job.env().contains_key(&environment_name)
        {
            return Err(ExecutionModelError::InvalidServicePort);
        }
    }
    Ok(())
}

impl RunSpec {
    pub fn new(id: RunId, worker_id: WorkerId, job: JobSpec) -> Self {
        Self {
            id,
            worker_id,
            job,
            service_ports: Vec::new(),
            readiness_probe: None,
        }
    }

    pub fn with_service_ports(
        mut self,
        service_ports: Vec<AssignedServicePort>,
    ) -> Result<Self, ExecutionModelError> {
        validate_assigned_service_ports(&self.job, &service_ports)?;
        validate_readiness_probe(self.readiness_probe.as_ref(), &service_ports)?;
        self.service_ports = service_ports;
        Ok(self)
    }

    pub fn with_readiness_probe(
        mut self,
        readiness_probe: ReadinessProbe,
    ) -> Result<Self, ExecutionModelError> {
        if !readiness_probe.is_valid()
            || !self
                .service_ports
                .iter()
                .any(|port| port.name() == readiness_probe.port_name())
        {
            return Err(ExecutionModelError::InvalidReadinessProbe);
        }
        self.readiness_probe = Some(readiness_probe);
        Ok(self)
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

    pub fn service_ports(&self) -> &[AssignedServicePort] {
        &self.service_ports
    }

    pub fn readiness_probe(&self) -> Option<&ReadinessProbe> {
        self.readiness_probe.as_ref()
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

/// Readiness is independent of process state and restart policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessState {
    Starting,
    Ready,
    Unready,
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
    ExecutableDigestMismatch,
    /// Retained to read observations written by older workers. Native
    /// executable compatibility is delegated to the operating system.
    IncompatibleExecutable,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    readiness: Option<ReadinessState>,
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
            readiness: None,
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

    pub fn readiness(&self) -> Option<ReadinessState> {
        self.readiness
    }

    pub fn with_readiness(mut self, readiness: ReadinessState) -> Self {
        self.readiness = Some(readiness);
        self
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
            readiness: self.readiness,
        })
    }

    pub fn next_readiness_revision(&self, readiness: ReadinessState) -> Option<Self> {
        let mut next = self.next_revision(
            self.state,
            self.started_at_ms,
            self.finished_at_ms,
            self.exit_code,
            self.termination_signal,
            self.failure_reason,
        )?;
        next.readiness = Some(readiness);
        Some(next)
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
    InvalidCpuLimit,
    InvalidMemoryLimit,
    InvalidProcessLimit,
    InvalidFilesystemPolicy,
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
            Self::InvalidCpuLimit => {
                "CPU quota and period must be supplied together and within supported bounds"
            }
            Self::InvalidMemoryLimit => {
                "memory limits must be at least 4 MiB and within supported bounds"
            }
            Self::InvalidProcessLimit => "pidsMax must be between 1 and 1048576",
            Self::InvalidFilesystemPolicy => {
                "filesystemPolicy must contain at most 64 unique grant identifiers"
            }
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
    #[serde(default, skip_serializing_if = "ResourceLimits::is_empty")]
    resource_limits: ResourceLimits,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    filesystem_policy: Option<FilesystemPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    executable_digest: Option<ExecutableDigest>,
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
    #[serde(default)]
    resource_limits: ResourceLimits,
    #[serde(default)]
    filesystem_policy: Option<FilesystemPolicy>,
    #[serde(default)]
    executable_digest: Option<ExecutableDigest>,
}

/// An immutable selection of administrator-approved filesystem grants.
/// Jobs select configured grants by identifier and cannot submit paths.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FilesystemPolicy {
    grants: Vec<String>,
}

impl FilesystemPolicy {
    pub fn new(grants: Vec<String>) -> Result<Self, JobValidationError> {
        let mut seen = std::collections::BTreeSet::new();
        if grants.len() > MAX_FILESYSTEM_GRANTS_PER_JOB
            || grants.iter().any(|grant| {
                grant.is_empty()
                    || grant.len() > MAX_IDENTIFIER_LENGTH
                    || !grant
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                    || !seen.insert(grant.as_str())
            })
        {
            return Err(JobValidationError::InvalidFilesystemPolicy);
        }
        Ok(Self { grants })
    }

    pub fn grants(&self) -> &[String] {
        &self.grants
    }
}

/// Optional Linux cgroup v2 resource limits requested by an immutable job.
/// These values never grant privileges and are always constrained by the
/// administrator's configured worker policy.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cpu_quota_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cpu_period_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_swap_max_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pids_max: Option<u32>,
}

impl ResourceLimits {
    pub fn new(
        cpu_quota_us: Option<u64>,
        cpu_period_us: Option<u64>,
        memory_max_bytes: Option<u64>,
        memory_swap_max_bytes: Option<u64>,
        pids_max: Option<u32>,
    ) -> Result<Self, JobValidationError> {
        if cpu_quota_us.is_some() != cpu_period_us.is_some()
            || cpu_quota_us.is_some_and(|quota| !(1_000..=1_000_000_000).contains(&quota))
            || cpu_period_us.is_some_and(|period| !(1_000..=1_000_000).contains(&period))
        {
            return Err(JobValidationError::InvalidCpuLimit);
        }
        if memory_max_bytes.is_some_and(|bytes| !(4 * 1024 * 1024..=u64::MAX - 1).contains(&bytes))
            || memory_swap_max_bytes.is_some_and(|bytes| bytes == u64::MAX)
        {
            return Err(JobValidationError::InvalidMemoryLimit);
        }
        if pids_max.is_some_and(|pids| !(1..=1_048_576).contains(&pids)) {
            return Err(JobValidationError::InvalidProcessLimit);
        }
        Ok(Self {
            cpu_quota_us,
            cpu_period_us,
            memory_max_bytes,
            memory_swap_max_bytes,
            pids_max,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.cpu_quota_us.is_none()
            && self.cpu_period_us.is_none()
            && self.memory_max_bytes.is_none()
            && self.memory_swap_max_bytes.is_none()
            && self.pids_max.is_none()
    }

    pub fn cpu_quota_us(&self) -> Option<u64> {
        self.cpu_quota_us
    }
    pub fn cpu_period_us(&self) -> Option<u64> {
        self.cpu_period_us
    }
    pub fn memory_max_bytes(&self) -> Option<u64> {
        self.memory_max_bytes
    }
    pub fn memory_swap_max_bytes(&self) -> Option<u64> {
        self.memory_swap_max_bytes
    }
    pub fn pids_max(&self) -> Option<u32> {
        self.pids_max
    }
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
            resource_limits: ResourceLimits::default(),
            filesystem_policy: None,
            executable_digest: None,
        })
    }

    pub fn with_resource_limits(
        mut self,
        resource_limits: ResourceLimits,
    ) -> Result<Self, JobValidationError> {
        self.resource_limits = ResourceLimits::new(
            resource_limits.cpu_quota_us,
            resource_limits.cpu_period_us,
            resource_limits.memory_max_bytes,
            resource_limits.memory_swap_max_bytes,
            resource_limits.pids_max,
        )?;
        Ok(self)
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

    /// Return a copy with validated manager-assigned environment values.
    pub fn with_environment_overrides(
        mut self,
        overrides: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, JobValidationError> {
        for (name, value) in overrides {
            if !valid_environment_name(&name) {
                return Err(JobValidationError::InvalidEnvironmentName);
            }
            if value.as_bytes().contains(&0) {
                return Err(JobValidationError::InvalidEnvironmentValue);
            }
            self.env.insert(name, value);
        }
        Ok(self)
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
    pub fn resource_limits(&self) -> &ResourceLimits {
        &self.resource_limits
    }
    pub fn with_filesystem_policy(
        mut self,
        filesystem_policy: Option<FilesystemPolicy>,
    ) -> Result<Self, JobValidationError> {
        self.filesystem_policy = filesystem_policy
            .map(|policy| FilesystemPolicy::new(policy.grants))
            .transpose()?;
        Ok(self)
    }
    pub fn filesystem_policy(&self) -> Option<&FilesystemPolicy> {
        self.filesystem_policy.as_ref()
    }

    /// Attach an operator-provisioned executable digest. Workers verify it
    /// immediately before launching the configured executable.
    pub fn with_executable_digest(mut self, digest: Option<ExecutableDigest>) -> Self {
        self.executable_digest = digest;
        self
    }

    pub fn executable_digest(&self) -> Option<&ExecutableDigest> {
        self.executable_digest.as_ref()
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
        )?
        .with_resource_limits(raw.resource_limits)?
        .with_filesystem_policy(raw.filesystem_policy)
        .map(|job| job.with_executable_digest(raw.executable_digest))
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
