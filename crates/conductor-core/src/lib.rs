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

#[path = "structs/invalid_identifier.rs"]
mod invalid_identifier;
pub use invalid_identifier::InvalidIdentifier;

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

#[path = "structs/job_id.rs"]
mod job_id;
pub use job_id::JobId;
#[path = "structs/run_id.rs"]
mod run_id;
pub use run_id::RunId;
#[path = "structs/command_id.rs"]
mod command_id;
pub use command_id::CommandId;
#[path = "structs/worker_id.rs"]
mod worker_id;
pub use worker_id::WorkerId;

#[path = "structs/overlay_network.rs"]
mod overlay_network;
pub use overlay_network::{
    MAX_OVERLAY_APPLICATION_PROBE_RESPONSE_BYTES, MAX_OVERLAY_APPLICATION_PROBE_TIMEOUT_MS,
    MAX_OVERLAY_DIRECT_CANDIDATES, OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION,
    OverlayApplicationProbeDiagnostic, OverlayApplicationProbeOutcome,
    OverlayApplicationProbeReport, OverlayDirectCandidateAcknowledgement,
    OverlayDirectCandidateAdvertisement, OverlayDirectLinkDiagnostic, OverlayEndpointId,
    OverlayNetworkDomainConfig, OverlayPeer, OverlayPeerAssignment, OverlayPeerConfiguration,
    OverlayPeerDiagnostic, OverlayPeerObservation, OverlayPeerObservationStatus, OverlayPeerState,
    OverlayReachability, OverlayRuntimeDiagnostic, valid_overlay_direct_candidates,
};
#[path = "structs/network_domain_id.rs"]
mod network_domain_id;
pub use network_domain_id::NetworkDomainId;

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

#[path = "structs/command_envelope.rs"]
mod command_envelope;
pub use command_envelope::CommandEnvelope;

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

#[path = "structs/worker_descriptor.rs"]
mod worker_descriptor;
pub use worker_descriptor::WorkerDescriptor;

#[path = "structs/worker_status.rs"]
mod worker_status;
pub use worker_status::WorkerStatus;

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

#[path = "structs/run_spec.rs"]
mod run_spec;
pub use run_spec::RunSpec;

#[path = "structs/raw_run_spec.rs"]
mod raw_run_spec;
use raw_run_spec::RawRunSpec;

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

#[path = "structs/run_observation.rs"]
mod run_observation;
pub use run_observation::RunObservation;

#[path = "structs/stop_run.rs"]
mod stop_run;
pub use stop_run::StopRun;

#[path = "structs/resolve_run.rs"]
mod resolve_run;
pub use resolve_run::ResolveRun;

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

#[path = "structs/read_run_logs.rs"]
mod read_run_logs;
pub use read_run_logs::ReadRunLogs;

#[path = "structs/run_log_chunk.rs"]
mod run_log_chunk;
pub use run_log_chunk::RunLogChunk;

#[path = "structs/run_log_status.rs"]
mod run_log_status;
pub use run_log_status::RunLogStatus;

#[path = "structs/start_run.rs"]
mod start_run;
pub use start_run::StartRun;

#[path = "structs/inspect_run.rs"]
mod inspect_run;
pub use inspect_run::InspectRun;

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

#[path = "structs/job_spec.rs"]
mod job_spec;
pub use job_spec::JobSpec;

#[path = "structs/raw_job_spec.rs"]
mod raw_job_spec;
use raw_job_spec::RawJobSpec;

#[path = "structs/filesystem_policy.rs"]
mod filesystem_policy;
pub use filesystem_policy::FilesystemPolicy;

#[path = "structs/resource_limits.rs"]
mod resource_limits;
pub use resource_limits::ResourceLimits;

fn default_termination_grace_ms() -> u64 {
    DEFAULT_TERMINATION_GRACE_MS
}

fn valid_environment_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'A'..=b'Z' | b'a'..=b'z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
