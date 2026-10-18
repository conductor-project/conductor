//! Transport-neutral MVP2 execution contracts.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use serde::{Deserialize, Deserializer, Serialize};

use crate::{InvalidIdentifier, JobId, JobSpec, WorkerId};

macro_rules! execution_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, InvalidIdentifier> {
                let value = value.into();
                if value.is_empty()
                    || value.len() > super::MAX_IDENTIFIER_LENGTH
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                {
                    return Err(InvalidIdentifier);
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
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

execution_id!(AllocationId);
execution_id!(ExecutionAttemptId);
execution_id!(ServiceInstanceId);

const DEFAULT_NAMESPACE: &str = "default";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionModelError {
    InvalidRevision,
    InvalidDeployment,
    DeploymentGenerationExhausted,
    InvalidServiceName,
    InvalidServicePort,
    InvalidReadinessProbe,
    UnsupportedNamespace,
    InvalidExecutableDigest,
    RevisionJobMismatch,
    InvalidServiceInstance,
    IncompleteServiceResourceRequest,
    ResourceRequestBelowExecutionLimit,
    InvalidWorkerLabelConstraint,
    ConflictingExecutableDigest,
    MissingExecutionBackend,
    UnsupportedPlatformCapability,
    InvalidTransition,
    AssignmentGenerationExhausted,
}

impl fmt::Display for ExecutionModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRevision => "job revision must be positive",
            Self::InvalidDeployment => "deployment intent is invalid for its job revision",
            Self::DeploymentGenerationExhausted => "deployment generation is exhausted",
            Self::InvalidServiceName => "service name must be a DNS-compatible label",
            Self::InvalidServicePort => {
                "service port must have a unique DNS-compatible name and valid endpoint"
            }
            Self::InvalidReadinessProbe => "service readiness probe configuration is invalid",
            Self::UnsupportedNamespace => "only the default service namespace is supported",
            Self::InvalidExecutableDigest => {
                "executable digest must be a 64-character SHA-256 hex value"
            }
            Self::RevisionJobMismatch => {
                "job revision identifier does not match its job definition"
            }
            Self::InvalidServiceInstance => "service instance does not match the job revision",
            Self::IncompleteServiceResourceRequest => {
                "service revisions require positive CPU and memory requests"
            }
            Self::ResourceRequestBelowExecutionLimit => {
                "service resource requests must cover their configured CPU and memory limits"
            }
            Self::InvalidWorkerLabelConstraint => "worker label constraints are invalid",
            Self::ConflictingExecutableDigest => {
                "revision and job definition executable digests must match"
            }
            Self::MissingExecutionBackend => "worker must advertise at least one execution backend",
            Self::UnsupportedPlatformCapability => {
                "worker cannot advertise Linux enforcement on macOS"
            }
            Self::InvalidTransition => "execution state transition is not allowed",
            Self::AssignmentGenerationExhausted => "allocation assignment generation is exhausted",
        })
    }
}

impl std::error::Error for ExecutionModelError {}

/// Identity of an immutable revision of a logical job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobRevisionId {
    job_id: JobId,
    revision: NonZeroU64,
}

impl JobRevisionId {
    pub fn new(job_id: JobId, revision: u64) -> Result<Self, ExecutionModelError> {
        Ok(Self {
            job_id,
            revision: NonZeroU64::new(revision).ok_or(ExecutionModelError::InvalidRevision)?,
        })
    }

    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    pub fn revision(&self) -> u64 {
        self.revision.get()
    }
}

/// An operator-provisioned executable identity. This contract records the
/// expected digest; verification is performed by the worker before execution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutableDigest {
    algorithm: DigestAlgorithm,
    value: String,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestAlgorithm {
    Sha256,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawExecutableDigest {
    algorithm: DigestAlgorithm,
    value: String,
}

impl ExecutableDigest {
    pub fn sha256(value: impl Into<String>) -> Result<Self, ExecutionModelError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ExecutionModelError::InvalidExecutableDigest);
        }
        Ok(Self {
            algorithm: DigestAlgorithm::Sha256,
            value,
        })
    }

    pub fn algorithm(&self) -> DigestAlgorithm {
        self.algorithm
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl<'de> Deserialize<'de> for ExecutableDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawExecutableDigest::deserialize(deserializer)?;
        match raw.algorithm {
            DigestAlgorithm::Sha256 => Self::sha256(raw.value).map_err(serde::de::Error::custom),
        }
    }
}

/// Logical service name. MVP2 reserves the namespace field but implements only
/// `default`; namespace authorization and isolation are future work.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceIdentity {
    namespace: String,
    name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawServiceIdentity {
    namespace: String,
    name: String,
}

impl ServiceIdentity {
    pub fn new(name: impl Into<String>) -> Result<Self, ExecutionModelError> {
        Self::in_namespace(DEFAULT_NAMESPACE, name)
    }

    pub fn in_namespace(
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Self, ExecutionModelError> {
        let namespace = namespace.into();
        let name = name.into();
        if namespace != DEFAULT_NAMESPACE {
            return Err(ExecutionModelError::UnsupportedNamespace);
        }
        if !valid_dns_label(&name) {
            return Err(ExecutionModelError::InvalidServiceName);
        }
        Ok(Self { namespace, name })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl<'de> Deserialize<'de> for ServiceIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawServiceIdentity::deserialize(deserializer)?;
        Self::in_namespace(raw.namespace, raw.name).map_err(serde::de::Error::custom)
    }
}

fn valid_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !value.ends_with('-')
}

fn valid_srv_label(value: &str) -> bool {
    value.len() <= 62 && valid_dns_label(value)
}

/// Desired lifecycle for a job revision. Existing MVP1 jobs remain plain
/// `JobSpec` values and map to `OneShot` when represented as an MVP2 revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Workload {
    OneShot,
    Service {
        identity: ServiceIdentity,
        #[serde(default)]
        ports: Vec<ServicePort>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        readiness_probe: Option<ReadinessProbe>,
    },
}

/// A bounded worker-side readiness check for one declared service port.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadinessProbe {
    port_name: String,
    protocol: ReadinessProbeProtocol,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    interval_ms: u64,
    timeout_ms: u64,
    startup_grace_ms: u64,
    success_threshold: u16,
    failure_threshold: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadinessProbeProtocol {
    Tcp,
    Http,
}

impl ReadinessProbe {
    pub fn tcp(port_name: impl Into<String>) -> Result<Self, ExecutionModelError> {
        Self::new(
            port_name.into(),
            ReadinessProbeProtocol::Tcp,
            None,
            1_000,
            500,
            0,
            1,
            1,
        )
    }

    pub fn http(
        port_name: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Self, ExecutionModelError> {
        Self::new(
            port_name.into(),
            ReadinessProbeProtocol::Http,
            Some(path.into()),
            1_000,
            500,
            0,
            1,
            1,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn configured(
        port_name: impl Into<String>,
        protocol: ReadinessProbeProtocol,
        path: Option<String>,
        interval_ms: u64,
        timeout_ms: u64,
        startup_grace_ms: u64,
        success_threshold: u16,
        failure_threshold: u16,
    ) -> Result<Self, ExecutionModelError> {
        Self::new(
            port_name.into(),
            protocol,
            path,
            interval_ms,
            timeout_ms,
            startup_grace_ms,
            success_threshold,
            failure_threshold,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        port_name: String,
        protocol: ReadinessProbeProtocol,
        path: Option<String>,
        interval_ms: u64,
        timeout_ms: u64,
        startup_grace_ms: u64,
        success_threshold: u16,
        failure_threshold: u16,
    ) -> Result<Self, ExecutionModelError> {
        if !valid_srv_label(&port_name)
            || (protocol == ReadinessProbeProtocol::Http
                && path.as_ref().is_none_or(|value| {
                    !value.starts_with('/')
                        || value.len() > 1_024
                        || value.contains(['#', '?', '\n', '\r'])
                }))
            || (protocol == ReadinessProbeProtocol::Tcp && path.is_some())
            || !(50..=60_000).contains(&interval_ms)
            || timeout_ms == 0
            || timeout_ms > 10_000
            || timeout_ms > interval_ms
            || startup_grace_ms > 600_000
            || !(1..=10).contains(&success_threshold)
            || !(1..=10).contains(&failure_threshold)
        {
            return Err(ExecutionModelError::InvalidReadinessProbe);
        }
        Ok(Self {
            port_name,
            protocol,
            path,
            interval_ms,
            timeout_ms,
            startup_grace_ms,
            success_threshold,
            failure_threshold,
        })
    }

    pub fn port_name(&self) -> &str {
        &self.port_name
    }
    pub fn protocol(&self) -> ReadinessProbeProtocol {
        self.protocol
    }
    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }
    pub fn interval_ms(&self) -> u64 {
        self.interval_ms
    }
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
    }
    pub fn startup_grace_ms(&self) -> u64 {
        self.startup_grace_ms
    }
    pub fn success_threshold(&self) -> u16 {
        self.success_threshold
    }
    pub fn failure_threshold(&self) -> u16 {
        self.failure_threshold
    }

    pub fn is_valid(&self) -> bool {
        Self::new(
            self.port_name.clone(),
            self.protocol,
            self.path.clone(),
            self.interval_ms,
            self.timeout_ms,
            self.startup_grace_ms,
            self.success_threshold,
            self.failure_threshold,
        )
        .is_ok()
    }
}

/// A named TCP listener exposed by a native service process.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServicePort {
    name: String,
    #[serde(default = "tcp_protocol")]
    protocol: ServicePortProtocol,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServicePortProtocol {
    #[default]
    Tcp,
}

fn tcp_protocol() -> ServicePortProtocol {
    ServicePortProtocol::Tcp
}

impl ServicePort {
    pub fn tcp(name: impl Into<String>) -> Result<Self, ExecutionModelError> {
        let name = name.into();
        if !valid_srv_label(&name) {
            return Err(ExecutionModelError::InvalidServicePort);
        }
        Ok(Self {
            name,
            protocol: ServicePortProtocol::Tcp,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn protocol(&self) -> ServicePortProtocol {
        self.protocol
    }

    pub fn environment_name(&self) -> String {
        let mut name = String::with_capacity("CONDUCTOR_PORT_".len() + self.name.len());
        name.push_str("CONDUCTOR_PORT_");
        name.extend(
            self.name
                .bytes()
                .map(|byte| {
                    if byte == b'-' {
                        b'_'
                    } else {
                        byte.to_ascii_uppercase()
                    }
                })
                .map(char::from),
        );
        name
    }
}

/// An assigned host endpoint injected into one service execution attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "RawAssignedServicePort")]
pub struct AssignedServicePort {
    name: String,
    protocol: ServicePortProtocol,
    port: u16,
    advertised_address: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawAssignedServicePort {
    name: String,
    protocol: ServicePortProtocol,
    port: u16,
    advertised_address: String,
}

impl TryFrom<RawAssignedServicePort> for AssignedServicePort {
    type Error = ExecutionModelError;

    fn try_from(raw: RawAssignedServicePort) -> Result<Self, Self::Error> {
        Self::new(raw.name, raw.protocol, raw.port, raw.advertised_address)
    }
}

impl AssignedServicePort {
    pub fn new(
        name: impl Into<String>,
        protocol: ServicePortProtocol,
        port: u16,
        advertised_address: impl Into<String>,
    ) -> Result<Self, ExecutionModelError> {
        let name = name.into();
        let advertised_address = advertised_address.into();
        if !valid_srv_label(&name) || port == 0 || !valid_advertised_address(&advertised_address) {
            return Err(ExecutionModelError::InvalidServicePort);
        }
        Ok(Self {
            name,
            protocol,
            port,
            advertised_address,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn protocol(&self) -> ServicePortProtocol {
        self.protocol
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn advertised_address(&self) -> &str {
        &self.advertised_address
    }

    pub fn is_valid(&self) -> bool {
        valid_srv_label(&self.name)
            && self.port > 0
            && valid_advertised_address(&self.advertised_address)
    }

    pub fn environment_name(&self) -> String {
        let mut name = String::with_capacity("CONDUCTOR_PORT_".len() + self.name.len());
        name.push_str("CONDUCTOR_PORT_");
        name.extend(
            self.name
                .bytes()
                .map(|byte| {
                    if byte == b'-' {
                        b'_'
                    } else {
                        byte.to_ascii_uppercase()
                    }
                })
                .map(char::from),
        );
        name
    }
}

fn valid_advertised_address(value: &str) -> bool {
    crate::inventory::valid_advertised_address(value)
}

/// Maximum requested service replica count accepted by the public API.
pub const MAX_DEPLOYMENT_REPLICAS: u32 = 10_000;

/// Lifecycle status reported for mutable deployment intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentStatus {
    Pending,
    Ready,
    Suspended,
    Deleting,
    Deleted,
}

/// Actionable reason that a persisted deployment has not reached its desired
/// replica count. These values are stable API identifiers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentPendingReason {
    NoEligibleWorker,
    WorkerCapacityExhausted,
    WorkerDisconnected,
    RequiredProtectionUnavailable,
    SharedHostCapacityExhausted,
    PlacementPending,
    ServiceStarting,
    ServiceRestartBackoff,
    ServiceRestartBudgetExhausted,
    ServiceAttemptUnresolved,
    ServiceReadinessPending,
    RolloutProgressDeadlineExceeded,
}

/// Bounded policy for replacing service replicas during a revision update.
/// This manager currently supports zero surge because each stable service
/// instance has one durable execution attempt at a time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeploymentUpdateStrategy {
    max_surge: u32,
    max_unavailable: u32,
    progress_deadline_seconds: u32,
}

impl Default for DeploymentUpdateStrategy {
    fn default() -> Self {
        Self {
            max_surge: 0,
            max_unavailable: 1,
            progress_deadline_seconds: 300,
        }
    }
}

impl DeploymentUpdateStrategy {
    pub fn new(
        max_surge: u32,
        max_unavailable: u32,
        progress_deadline_seconds: u32,
    ) -> Result<Self, ExecutionModelError> {
        if max_surge != 0
            || max_unavailable == 0
            || max_unavailable > MAX_DEPLOYMENT_REPLICAS
            || !(1..=86_400).contains(&progress_deadline_seconds)
        {
            return Err(ExecutionModelError::InvalidDeployment);
        }
        Ok(Self {
            max_surge,
            max_unavailable,
            progress_deadline_seconds,
        })
    }

    pub fn max_surge(self) -> u32 {
        self.max_surge
    }

    pub fn max_unavailable(self) -> u32 {
        self.max_unavailable
    }

    pub fn progress_deadline_seconds(self) -> u32 {
        self.progress_deadline_seconds
    }
}

/// Durable state for an in-progress revision replacement. The selected
/// revision remains the deployment's desired revision; this record retains
/// the source revision until the target has passed its rollout gate.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeploymentRollout {
    previous_revision_id: JobRevisionId,
    started_at_ms: i64,
    progress_deadline_at_ms: Option<i64>,
    strategy: DeploymentUpdateStrategy,
    rolling_back: bool,
}

impl DeploymentRollout {
    pub fn new(
        previous_revision_id: JobRevisionId,
        started_at_ms: i64,
        progress_deadline_at_ms: Option<i64>,
        strategy: DeploymentUpdateStrategy,
        rolling_back: bool,
    ) -> Result<Self, ExecutionModelError> {
        if started_at_ms < 0
            || progress_deadline_at_ms.is_some_and(|deadline| deadline < started_at_ms)
            || DeploymentUpdateStrategy::new(
                strategy.max_surge,
                strategy.max_unavailable,
                strategy.progress_deadline_seconds,
            )
            .is_err()
        {
            return Err(ExecutionModelError::InvalidDeployment);
        }
        Ok(Self {
            previous_revision_id,
            started_at_ms,
            progress_deadline_at_ms,
            strategy,
            rolling_back,
        })
    }

    pub fn previous_revision_id(&self) -> &JobRevisionId {
        &self.previous_revision_id
    }

    pub fn started_at_ms(&self) -> i64 {
        self.started_at_ms
    }

    pub fn progress_deadline_at_ms(&self) -> Option<i64> {
        self.progress_deadline_at_ms
    }

    pub fn strategy(&self) -> DeploymentUpdateStrategy {
        self.strategy
    }

    pub fn rolling_back(&self) -> bool {
        self.rolling_back
    }
}

/// Mutable service intent. Revision contents remain immutable; scaling and
/// suspension advance this generation while retaining the selected revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobDeployment {
    job_id: JobId,
    revision_id: JobRevisionId,
    generation: NonZeroU64,
    desired_replicas: u32,
    suspended: bool,
    status: DeploymentStatus,
    pending_reason: Option<DeploymentPendingReason>,
    pending_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rollout: Option<DeploymentRollout>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawJobDeployment {
    job_id: JobId,
    revision_id: JobRevisionId,
    generation: NonZeroU64,
    desired_replicas: u32,
    suspended: bool,
    status: DeploymentStatus,
    #[serde(default)]
    pending_reason: Option<DeploymentPendingReason>,
    #[serde(default)]
    pending_message: Option<String>,
    #[serde(default)]
    rollout: Option<DeploymentRollout>,
}

impl JobDeployment {
    pub fn new(
        revision: &JobRevision,
        generation: u64,
        desired_replicas: u32,
        suspended: bool,
        status: DeploymentStatus,
        pending_reason: Option<DeploymentPendingReason>,
        pending_message: Option<String>,
    ) -> Result<Self, ExecutionModelError> {
        if !matches!(revision.workload(), Workload::Service { .. })
            || desired_replicas > MAX_DEPLOYMENT_REPLICAS
            || matches!(
                status,
                DeploymentStatus::Suspended
                    | DeploymentStatus::Deleting
                    | DeploymentStatus::Deleted
            ) != suspended
            || (suspended && pending_reason.is_some())
            || (pending_reason.is_some() != pending_message.is_some())
            || (status == DeploymentStatus::Pending
                && (desired_replicas == 0 || pending_reason.is_none()))
            || (status == DeploymentStatus::Ready && pending_reason.is_some())
            || (matches!(
                status,
                DeploymentStatus::Deleting | DeploymentStatus::Deleted
            ) && (!suspended || desired_replicas != 0))
            || pending_message
                .as_ref()
                .is_some_and(|message| message.trim().is_empty() || message.len() > 512)
        {
            return Err(ExecutionModelError::InvalidDeployment);
        }
        Ok(Self {
            job_id: revision.id().job_id().clone(),
            revision_id: revision.id().clone(),
            generation: NonZeroU64::new(generation).ok_or(ExecutionModelError::InvalidRevision)?,
            desired_replicas,
            suspended,
            status,
            pending_reason,
            pending_message,
            rollout: None,
        })
    }

    pub fn with_rollout(mut self, rollout: Option<DeploymentRollout>) -> Self {
        self.rollout = rollout;
        self
    }

    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }

    pub fn revision_id(&self) -> &JobRevisionId {
        &self.revision_id
    }

    pub fn generation(&self) -> u64 {
        self.generation.get()
    }

    pub fn desired_replicas(&self) -> u32 {
        self.desired_replicas
    }

    pub fn suspended(&self) -> bool {
        self.suspended
    }

    pub fn status(&self) -> DeploymentStatus {
        self.status
    }

    pub fn pending_reason(&self) -> Option<DeploymentPendingReason> {
        self.pending_reason
    }

    pub fn pending_message(&self) -> Option<&str> {
        self.pending_message.as_deref()
    }

    pub fn rollout(&self) -> Option<&DeploymentRollout> {
        self.rollout.as_ref()
    }

    pub fn next_generation(
        &self,
        revision: &JobRevision,
        desired_replicas: u32,
        suspended: bool,
        status: DeploymentStatus,
        pending_reason: Option<DeploymentPendingReason>,
        pending_message: Option<String>,
    ) -> Result<Self, ExecutionModelError> {
        let generation = self
            .generation()
            .checked_add(1)
            .ok_or(ExecutionModelError::DeploymentGenerationExhausted)?;
        Self::new(
            revision,
            generation,
            desired_replicas,
            suspended,
            status,
            pending_reason,
            pending_message,
        )
        .map(|next| {
            let rollout = if desired_replicas == 0 || &self.revision_id != revision.id() {
                None
            } else {
                self.rollout.clone()
            };
            next.with_rollout(rollout)
        })
    }
}

impl<'de> Deserialize<'de> for JobDeployment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawJobDeployment::deserialize(deserializer)?;
        if raw.job_id != *raw.revision_id.job_id()
            || raw.desired_replicas > MAX_DEPLOYMENT_REPLICAS
            || matches!(
                raw.status,
                DeploymentStatus::Suspended
                    | DeploymentStatus::Deleting
                    | DeploymentStatus::Deleted
            ) != raw.suspended
            || (raw.suspended && raw.pending_reason.is_some())
            || (raw.pending_reason.is_some() != raw.pending_message.is_some())
            || (raw.status == DeploymentStatus::Pending
                && (raw.desired_replicas == 0 || raw.pending_reason.is_none()))
            || (raw.status == DeploymentStatus::Ready && raw.pending_reason.is_some())
            || (matches!(
                raw.status,
                DeploymentStatus::Deleting | DeploymentStatus::Deleted
            ) && (!raw.suspended || raw.desired_replicas != 0))
            || raw
                .pending_message
                .as_ref()
                .is_some_and(|message| message.trim().is_empty() || message.len() > 512)
            || raw.rollout.as_ref().is_some_and(|rollout| {
                rollout.previous_revision_id.job_id() != &raw.job_id
                    || rollout.previous_revision_id == raw.revision_id
                    || rollout.started_at_ms < 0
                    || rollout
                        .progress_deadline_at_ms
                        .is_some_and(|deadline| deadline < rollout.started_at_ms)
                    || DeploymentUpdateStrategy::new(
                        rollout.strategy.max_surge,
                        rollout.strategy.max_unavailable,
                        rollout.strategy.progress_deadline_seconds,
                    )
                    .is_err()
            })
        {
            return Err(serde::de::Error::custom(
                "deployment state, revision, and pending fields disagree",
            ));
        }
        Ok(Self {
            job_id: raw.job_id,
            revision_id: raw.revision_id,
            generation: raw.generation,
            desired_replicas: raw.desired_replicas,
            suspended: raw.suspended,
            status: raw.status,
            pending_reason: raw.pending_reason,
            pending_message: raw.pending_message,
            rollout: raw.rollout,
        })
    }
}

/// Requirements a worker must satisfy before an allocation can be assigned.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecutionRequirements {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operating_system: Option<OperatingSystem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    architecture: Option<Architecture>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backend: Option<ExecutionBackend>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    required_enforcement: BTreeSet<EnforcementCapability>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    worker_labels: BTreeMap<String, String>,
}

impl ExecutionRequirements {
    pub fn new(
        operating_system: Option<OperatingSystem>,
        architecture: Option<Architecture>,
        backend: Option<ExecutionBackend>,
        required_enforcement: BTreeSet<EnforcementCapability>,
    ) -> Self {
        Self {
            operating_system,
            architecture,
            backend,
            required_enforcement,
            worker_labels: BTreeMap::new(),
        }
    }

    pub fn with_worker_labels(
        mut self,
        worker_labels: BTreeMap<String, String>,
    ) -> Result<Self, ExecutionModelError> {
        if !crate::valid_worker_labels(&worker_labels) {
            return Err(ExecutionModelError::InvalidWorkerLabelConstraint);
        }
        self.worker_labels = worker_labels;
        Ok(self)
    }

    pub fn operating_system(&self) -> Option<OperatingSystem> {
        self.operating_system
    }

    pub fn architecture(&self) -> Option<Architecture> {
        self.architecture
    }

    pub fn backend(&self) -> Option<ExecutionBackend> {
        self.backend
    }

    pub fn required_enforcement(&self) -> &BTreeSet<EnforcementCapability> {
        &self.required_enforcement
    }

    /// Exact manager-configured worker labels required for placement.
    pub fn worker_labels(&self) -> &BTreeMap<String, String> {
        &self.worker_labels
    }

    pub fn labels_are_satisfied_by(&self, labels: &BTreeMap<String, String>) -> bool {
        self.worker_labels
            .iter()
            .all(|(key, required)| labels.get(key) == Some(required))
    }

    fn with_required_enforcement(
        mut self,
        required: impl IntoIterator<Item = EnforcementCapability>,
    ) -> Result<Self, ExecutionModelError> {
        self.required_enforcement.extend(required);
        if self.operating_system == Some(OperatingSystem::Macos)
            && self.required_enforcement.iter().any(|capability| {
                matches!(
                    capability,
                    EnforcementCapability::CgroupV2 | EnforcementCapability::Landlock
                )
            })
        {
            return Err(ExecutionModelError::UnsupportedPlatformCapability);
        }
        Ok(self)
    }

    pub fn is_satisfied_by(&self, capabilities: &WorkerCapabilities) -> bool {
        self.operating_system
            .is_none_or(|required| required == capabilities.operating_system)
            && self
                .architecture
                .is_none_or(|required| required == capabilities.architecture)
            && self
                .backend
                .is_none_or(|required| capabilities.backends.contains(&required))
            && self
                .required_enforcement
                .is_subset(&capabilities.enforcement)
    }
}

/// Execution-platform facts advertised by a worker. Reservations are not
/// enforcement capabilities; only capabilities listed here may satisfy a
/// job's required-enforcement set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerCapabilities {
    operating_system: OperatingSystem,
    architecture: Architecture,
    backends: BTreeSet<ExecutionBackend>,
    #[serde(default)]
    enforcement: BTreeSet<EnforcementCapability>,
}

impl WorkerCapabilities {
    pub fn new(
        operating_system: OperatingSystem,
        architecture: Architecture,
        backends: BTreeSet<ExecutionBackend>,
        enforcement: BTreeSet<EnforcementCapability>,
    ) -> Result<Self, ExecutionModelError> {
        if backends.is_empty() {
            return Err(ExecutionModelError::MissingExecutionBackend);
        }
        if operating_system == OperatingSystem::Macos && !enforcement.is_empty() {
            return Err(ExecutionModelError::UnsupportedPlatformCapability);
        }
        Ok(Self {
            operating_system,
            architecture,
            backends,
            enforcement,
        })
    }

    pub fn operating_system(&self) -> OperatingSystem {
        self.operating_system
    }

    pub fn architecture(&self) -> Architecture {
        self.architecture
    }

    pub fn backends(&self) -> &BTreeSet<ExecutionBackend> {
        &self.backends
    }

    pub fn enforcement(&self) -> &BTreeSet<EnforcementCapability> {
        &self.enforcement
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawWorkerCapabilities {
    operating_system: OperatingSystem,
    architecture: Architecture,
    backends: BTreeSet<ExecutionBackend>,
    #[serde(default)]
    enforcement: BTreeSet<EnforcementCapability>,
}

impl<'de> Deserialize<'de> for WorkerCapabilities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawWorkerCapabilities::deserialize(deserializer)?;
        Self::new(
            raw.operating_system,
            raw.architecture,
            raw.backends,
            raw.enforcement,
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperatingSystem {
    Linux,
    Macos,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Architecture {
    Aarch64,
    X86_64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionBackend {
    NativeProcess,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnforcementCapability {
    CgroupV2,
    Landlock,
}

/// Scheduling demand. It is not a kernel limit.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cpu_millis: Option<NonZeroU32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_bytes: Option<NonZeroU64>,
}

impl ResourceRequest {
    pub fn new(cpu_millis: Option<u32>, memory_bytes: Option<u64>) -> Option<Self> {
        Some(Self {
            cpu_millis: match cpu_millis {
                Some(value) => Some(NonZeroU32::new(value)?),
                None => None,
            },
            memory_bytes: match memory_bytes {
                Some(value) => Some(NonZeroU64::new(value)?),
                None => None,
            },
        })
    }

    pub fn cpu_millis(&self) -> Option<u32> {
        self.cpu_millis.map(NonZeroU32::get)
    }

    pub fn memory_bytes(&self) -> Option<u64> {
        self.memory_bytes.map(NonZeroU64::get)
    }

    pub fn is_complete(&self) -> bool {
        self.cpu_millis.is_some() && self.memory_bytes.is_some()
    }
}

/// Capacity reserved for one allocation by the scheduler, stored separately
/// from the request and from any enforceable process limits.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceReservation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cpu_millis: Option<NonZeroU32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    memory_bytes: Option<NonZeroU64>,
}

impl ResourceReservation {
    pub fn new(cpu_millis: Option<u32>, memory_bytes: Option<u64>) -> Option<Self> {
        Some(Self {
            cpu_millis: match cpu_millis {
                Some(value) => Some(NonZeroU32::new(value)?),
                None => None,
            },
            memory_bytes: match memory_bytes {
                Some(value) => Some(NonZeroU64::new(value)?),
                None => None,
            },
        })
    }

    pub fn cpu_millis(&self) -> Option<u32> {
        self.cpu_millis.map(NonZeroU32::get)
    }

    pub fn memory_bytes(&self) -> Option<u64> {
        self.memory_bytes.map(NonZeroU64::get)
    }
}

/// Complete immutable desired execution contract for a numbered job revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobRevision {
    id: JobRevisionId,
    definition: JobSpec,
    workload: Workload,
    executable_digest: ExecutableDigest,
    execution_requirements: ExecutionRequirements,
    resource_request: ResourceRequest,
    resource_request_provenance: ResourceRequestProvenance,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawJobRevision {
    id: JobRevisionId,
    definition: JobSpec,
    workload: Workload,
    executable_digest: ExecutableDigest,
    #[serde(default)]
    execution_requirements: ExecutionRequirements,
    #[serde(default)]
    resource_request: ResourceRequest,
    #[serde(default)]
    resource_request_provenance: Option<ResourceRequestProvenance>,
}

/// Whether effective scheduling requests came from the client or were absent.
/// MVP2 has no administrator defaults: omitted one-shot requests remain
/// unspecified and service requests must be explicit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceRequestProvenance {
    Explicit,
    Unspecified,
}

/// Strict public input for creating an immutable job revision. The manager
/// assigns the revision number and returns a `JobRevision`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct JobRevisionDraft {
    definition: JobSpec,
    workload: Workload,
    executable_digest: ExecutableDigest,
    #[serde(default)]
    execution_requirements: ExecutionRequirements,
    #[serde(default)]
    resource_request: ResourceRequest,
}

fn is_default_deployment_update_strategy(strategy: &DeploymentUpdateStrategy) -> bool {
    strategy == &DeploymentUpdateStrategy::default()
}

/// Conditional update requested for the mutable deployment of one logical job.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum DeploymentMutation {
    Deploy {
        revision: u64,
        replicas: u32,
        #[serde(default, skip_serializing_if = "is_default_deployment_update_strategy")]
        strategy: DeploymentUpdateStrategy,
    },
    Scale {
        replicas: u32,
    },
    Suspend,
    Resume,
    Rollback {
        revision: u64,
    },
    Delete,
}

impl JobRevisionDraft {
    pub fn new(
        definition: JobSpec,
        workload: Workload,
        executable_digest: ExecutableDigest,
        execution_requirements: ExecutionRequirements,
        resource_request: ResourceRequest,
    ) -> Self {
        Self {
            definition,
            workload,
            executable_digest,
            execution_requirements,
            resource_request,
        }
    }

    pub fn job_id(&self) -> &JobId {
        self.definition.id()
    }

    pub fn definition(&self) -> &JobSpec {
        &self.definition
    }

    pub fn into_revision(self, revision: u64) -> Result<JobRevision, ExecutionModelError> {
        JobRevision::new(
            JobRevisionId::new(self.definition.id().clone(), revision)?,
            self.definition,
            self.workload,
            self.executable_digest,
            self.execution_requirements,
            self.resource_request,
        )
    }
}

impl JobRevision {
    pub fn new(
        id: JobRevisionId,
        definition: JobSpec,
        workload: Workload,
        executable_digest: ExecutableDigest,
        execution_requirements: ExecutionRequirements,
        resource_request: ResourceRequest,
    ) -> Result<Self, ExecutionModelError> {
        if id.job_id() != definition.id() {
            return Err(ExecutionModelError::RevisionJobMismatch);
        }
        if !crate::valid_worker_labels(execution_requirements.worker_labels()) {
            return Err(ExecutionModelError::InvalidWorkerLabelConstraint);
        }
        if let Workload::Service {
            ports,
            readiness_probe,
            ..
        } = &workload
        {
            if ports.len() > 32 {
                return Err(ExecutionModelError::InvalidServicePort);
            }
            let mut seen = std::collections::BTreeSet::new();
            for port in ports {
                if !valid_srv_label(port.name())
                    || !seen.insert(port.name())
                    || definition.env().contains_key(&port.environment_name())
                {
                    return Err(ExecutionModelError::InvalidServicePort);
                }
            }
            if let Some(probe) = readiness_probe
                && (!probe.is_valid() || !seen.contains(probe.port_name()))
            {
                return Err(ExecutionModelError::InvalidReadinessProbe);
            }
        }
        let resource_request_provenance = if resource_request.cpu_millis().is_some()
            || resource_request.memory_bytes().is_some()
        {
            ResourceRequestProvenance::Explicit
        } else {
            ResourceRequestProvenance::Unspecified
        };
        if matches!(workload, Workload::Service { .. }) && !resource_request.is_complete() {
            return Err(ExecutionModelError::IncompleteServiceResourceRequest);
        }
        if matches!(workload, Workload::Service { .. }) {
            let limits = definition.resource_limits();
            if let (Some(quota), Some(period), Some(requested)) = (
                limits.cpu_quota_us(),
                limits.cpu_period_us(),
                resource_request.cpu_millis(),
            ) {
                let required_millis = quota
                    .checked_mul(1_000)
                    .and_then(|value| value.checked_add(period - 1))
                    .map(|value| value / period)
                    .ok_or(ExecutionModelError::ResourceRequestBelowExecutionLimit)?;
                if u64::from(requested) < required_millis {
                    return Err(ExecutionModelError::ResourceRequestBelowExecutionLimit);
                }
            }
            if limits.memory_max_bytes().is_some_and(|limit| {
                resource_request
                    .memory_bytes()
                    .is_none_or(|request| request < limit)
            }) {
                return Err(ExecutionModelError::ResourceRequestBelowExecutionLimit);
            }
        }
        if definition
            .executable_digest()
            .is_some_and(|digest| digest != &executable_digest)
        {
            return Err(ExecutionModelError::ConflictingExecutableDigest);
        }
        let definition = definition.with_executable_digest(Some(executable_digest.clone()));
        let mut derived_enforcement = BTreeSet::new();
        if !definition.resource_limits().is_empty() {
            derived_enforcement.insert(EnforcementCapability::CgroupV2);
        }
        if definition
            .filesystem_policy()
            .is_some_and(|policy| !policy.grants().is_empty())
        {
            derived_enforcement.insert(EnforcementCapability::Landlock);
        }
        let execution_requirements =
            execution_requirements.with_required_enforcement(derived_enforcement)?;
        Ok(Self {
            id,
            definition,
            workload,
            executable_digest,
            execution_requirements,
            resource_request,
            resource_request_provenance,
        })
    }

    pub fn id(&self) -> &JobRevisionId {
        &self.id
    }

    pub fn definition(&self) -> &JobSpec {
        &self.definition
    }

    pub fn workload(&self) -> &Workload {
        &self.workload
    }

    pub fn executable_digest(&self) -> &ExecutableDigest {
        &self.executable_digest
    }

    pub fn execution_requirements(&self) -> &ExecutionRequirements {
        &self.execution_requirements
    }

    pub fn resource_request(&self) -> &ResourceRequest {
        &self.resource_request
    }

    pub fn resource_request_provenance(&self) -> ResourceRequestProvenance {
        self.resource_request_provenance
    }
}

impl<'de> Deserialize<'de> for JobRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawJobRevision::deserialize(deserializer)?;
        let revision = Self::new(
            raw.id,
            raw.definition,
            raw.workload,
            raw.executable_digest,
            raw.execution_requirements,
            raw.resource_request,
        )
        .map_err(serde::de::Error::custom)?;
        if raw
            .resource_request_provenance
            .is_some_and(|source| source != revision.resource_request_provenance())
        {
            return Err(serde::de::Error::custom(
                "resource request provenance does not match the submitted request",
            ));
        }
        Ok(revision)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationState {
    Pending,
    Assigned,
    Uncertain,
    Released,
}

/// Durable ownership and reservation for one desired service slot or one-shot
/// execution. Service slot identity survives replacement attempts.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    id: AllocationId,
    job_revision_id: JobRevisionId,
    service_instance_id: Option<ServiceInstanceId>,
    assigned_worker_id: Option<WorkerId>,
    assignment_generation: u64,
    state: AllocationState,
    reservation: ResourceReservation,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawAllocation {
    id: AllocationId,
    job_revision_id: JobRevisionId,
    service_instance_id: Option<ServiceInstanceId>,
    assigned_worker_id: Option<WorkerId>,
    assignment_generation: u64,
    state: AllocationState,
    reservation: ResourceReservation,
}

impl Allocation {
    pub fn new(
        id: AllocationId,
        revision: &JobRevision,
        service_instance: Option<&ServiceInstance>,
        reservation: ResourceReservation,
    ) -> Result<Self, ExecutionModelError> {
        let service_instance_id = match (revision.workload(), service_instance) {
            (Workload::OneShot, None) => None,
            (Workload::Service { identity, .. }, Some(instance))
                if instance.job_revision_id() == revision.id()
                    && instance.service() == identity =>
            {
                Some(instance.id().clone())
            }
            _ => return Err(ExecutionModelError::InvalidServiceInstance),
        };
        Ok(Self {
            id,
            job_revision_id: revision.id().clone(),
            service_instance_id,
            assigned_worker_id: None,
            assignment_generation: 0,
            state: AllocationState::Pending,
            reservation,
        })
    }

    pub fn id(&self) -> &AllocationId {
        &self.id
    }

    pub fn job_revision_id(&self) -> &JobRevisionId {
        &self.job_revision_id
    }

    pub fn service_instance_id(&self) -> Option<&ServiceInstanceId> {
        self.service_instance_id.as_ref()
    }

    pub fn assigned_worker_id(&self) -> Option<&WorkerId> {
        self.assigned_worker_id.as_ref()
    }

    pub fn assignment_generation(&self) -> u64 {
        self.assignment_generation
    }

    pub fn state(&self) -> AllocationState {
        self.state
    }

    pub fn reservation(&self) -> &ResourceReservation {
        &self.reservation
    }

    pub fn assign(&mut self, worker_id: WorkerId) -> Result<(), ExecutionModelError> {
        if self.state != AllocationState::Pending {
            return Err(ExecutionModelError::InvalidTransition);
        }
        self.assignment_generation = self
            .assignment_generation
            .checked_add(1)
            .ok_or(ExecutionModelError::AssignmentGenerationExhausted)?;
        self.assigned_worker_id = Some(worker_id);
        self.state = AllocationState::Assigned;
        Ok(())
    }

    pub fn mark_uncertain(&mut self) -> Result<(), ExecutionModelError> {
        if self.state != AllocationState::Assigned {
            return Err(ExecutionModelError::InvalidTransition);
        }
        self.state = AllocationState::Uncertain;
        Ok(())
    }

    /// Clear an uncertain assignment only after reconciliation established that
    /// the worker never accepted or started the allocation.
    pub fn resolve_uncertain_without_execution(&mut self) -> Result<(), ExecutionModelError> {
        if self.state != AllocationState::Uncertain {
            return Err(ExecutionModelError::InvalidTransition);
        }
        self.assigned_worker_id = None;
        self.state = AllocationState::Pending;
        Ok(())
    }

    /// Release is explicit; an uncertain allocation is never implicitly freed.
    pub fn release(&mut self) -> Result<(), ExecutionModelError> {
        if self.state == AllocationState::Released {
            return Err(ExecutionModelError::InvalidTransition);
        }
        self.state = AllocationState::Released;
        Ok(())
    }
}

impl<'de> Deserialize<'de> for Allocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawAllocation::deserialize(deserializer)?;
        let valid = match raw.state {
            AllocationState::Pending => raw.assigned_worker_id.is_none(),
            AllocationState::Assigned | AllocationState::Uncertain => {
                raw.assigned_worker_id.is_some() && raw.assignment_generation > 0
            }
            AllocationState::Released => true,
        };
        if !valid {
            return Err(serde::de::Error::custom(
                "allocation state and assignment fields disagree",
            ));
        }
        Ok(Self {
            id: raw.id,
            job_revision_id: raw.job_revision_id,
            service_instance_id: raw.service_instance_id,
            assigned_worker_id: raw.assigned_worker_id,
            assignment_generation: raw.assignment_generation,
            state: raw.state,
            reservation: raw.reservation,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Queued,
    Starting,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
}

impl AttemptState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

/// One execution try for an allocation. Retries receive a new ID and strictly
/// increasing attempt number; old attempts remain immutable evidence.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionAttempt {
    id: ExecutionAttemptId,
    allocation_id: AllocationId,
    worker_id: WorkerId,
    number: NonZeroU32,
    state: AttemptState,
    revision: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawExecutionAttempt {
    id: ExecutionAttemptId,
    allocation_id: AllocationId,
    worker_id: WorkerId,
    number: NonZeroU32,
    state: AttemptState,
    revision: u64,
}

impl ExecutionAttempt {
    pub fn new(
        id: ExecutionAttemptId,
        allocation: &Allocation,
        number: u32,
    ) -> Result<Self, ExecutionModelError> {
        let worker_id = allocation
            .assigned_worker_id()
            .filter(|_| allocation.state() == AllocationState::Assigned)
            .cloned()
            .ok_or(ExecutionModelError::InvalidTransition)?;
        let number = NonZeroU32::new(number).ok_or(ExecutionModelError::InvalidTransition)?;
        Ok(Self {
            id,
            allocation_id: allocation.id().clone(),
            worker_id,
            number,
            state: AttemptState::Queued,
            revision: 1,
        })
    }

    pub fn id(&self) -> &ExecutionAttemptId {
        &self.id
    }

    pub fn allocation_id(&self) -> &AllocationId {
        &self.allocation_id
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn number(&self) -> u32 {
        self.number.get()
    }

    pub fn state(&self) -> AttemptState {
        self.state
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn transition_to(&mut self, next: AttemptState) -> Result<(), ExecutionModelError> {
        let allowed = matches!(
            (self.state, next),
            (
                AttemptState::Queued,
                AttemptState::Starting | AttemptState::Cancelled | AttemptState::Unknown
            ) | (
                AttemptState::Starting,
                AttemptState::Running
                    | AttemptState::Failed
                    | AttemptState::Cancelled
                    | AttemptState::Unknown
            ) | (
                AttemptState::Running,
                AttemptState::Succeeded
                    | AttemptState::Failed
                    | AttemptState::Cancelled
                    | AttemptState::Unknown
            ) | (
                AttemptState::Unknown,
                AttemptState::Running
                    | AttemptState::Succeeded
                    | AttemptState::Failed
                    | AttemptState::Cancelled
            )
        );
        if !allowed {
            return Err(ExecutionModelError::InvalidTransition);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(ExecutionModelError::InvalidTransition)?;
        self.state = next;
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ExecutionAttempt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawExecutionAttempt::deserialize(deserializer)?;
        if raw.revision == 0 {
            return Err(serde::de::Error::custom(
                "attempt revision must be positive",
            ));
        }
        Ok(Self {
            id: raw.id,
            allocation_id: raw.allocation_id,
            worker_id: raw.worker_id,
            number: raw.number,
            state: raw.state,
            revision: raw.revision,
        })
    }
}

/// Stable identity for one desired service replica slot. It is retained across
/// allocation replacement and execution retries.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceInstance {
    id: ServiceInstanceId,
    service: ServiceIdentity,
    job_revision_id: JobRevisionId,
    replica_index: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawServiceInstance {
    id: ServiceInstanceId,
    service: ServiceIdentity,
    job_revision_id: JobRevisionId,
    replica_index: u32,
}

impl ServiceInstance {
    pub fn new(
        id: ServiceInstanceId,
        revision: &JobRevision,
        replica_index: u32,
    ) -> Result<Self, ExecutionModelError> {
        let Workload::Service { identity, .. } = revision.workload() else {
            return Err(ExecutionModelError::InvalidServiceInstance);
        };
        if replica_index >= MAX_DEPLOYMENT_REPLICAS {
            return Err(ExecutionModelError::InvalidServiceInstance);
        }
        Ok(Self {
            id,
            service: identity.clone(),
            job_revision_id: revision.id().clone(),
            replica_index,
        })
    }

    pub fn id(&self) -> &ServiceInstanceId {
        &self.id
    }

    pub fn service(&self) -> &ServiceIdentity {
        &self.service
    }

    pub fn job_revision_id(&self) -> &JobRevisionId {
        &self.job_revision_id
    }

    pub fn replica_index(&self) -> u32 {
        self.replica_index
    }
}

impl<'de> Deserialize<'de> for ServiceInstance {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawServiceInstance::deserialize(deserializer)?;
        Ok(Self {
            id: raw.id,
            service: raw.service,
            job_revision_id: raw.job_revision_id,
            replica_index: raw.replica_index,
        })
    }
}
