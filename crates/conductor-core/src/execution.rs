//! Transport-neutral MVP2 execution contracts.

use std::collections::BTreeSet;
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
    InvalidServiceName,
    UnsupportedNamespace,
    InvalidExecutableDigest,
    RevisionJobMismatch,
    InvalidServiceInstance,
    MissingExecutionBackend,
    UnsupportedPlatformCapability,
    InvalidTransition,
    AssignmentGenerationExhausted,
}

impl fmt::Display for ExecutionModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRevision => "job revision must be positive",
            Self::InvalidServiceName => "service name must be a DNS-compatible label",
            Self::UnsupportedNamespace => "only the default service namespace is supported",
            Self::InvalidExecutableDigest => {
                "executable digest must be a 64-character SHA-256 hex value"
            }
            Self::RevisionJobMismatch => {
                "job revision identifier does not match its job definition"
            }
            Self::InvalidServiceInstance => "service instance does not match the job revision",
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

/// Desired lifecycle for a job revision. Existing MVP1 jobs remain plain
/// `JobSpec` values and map to `OneShot` when represented as an MVP2 revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Workload {
    OneShot,
    Service {
        identity: ServiceIdentity,
        desired_replicas: NonZeroU32,
    },
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
        }
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
        Ok(Self {
            id,
            definition,
            workload,
            executable_digest,
            execution_requirements,
            resource_request,
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
}

impl<'de> Deserialize<'de> for JobRevision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawJobRevision::deserialize(deserializer)?;
        Self::new(
            raw.id,
            raw.definition,
            raw.workload,
            raw.executable_digest,
            raw.execution_requirements,
            raw.resource_request,
        )
        .map_err(serde::de::Error::custom)
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
            (
                Workload::Service {
                    identity,
                    desired_replicas,
                },
                Some(instance),
            ) if instance.job_revision_id() == revision.id()
                && instance.service() == identity
                && instance.replica_index() < desired_replicas.get() =>
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
        let Workload::Service {
            identity,
            desired_replicas,
        } = revision.workload()
        else {
            return Err(ExecutionModelError::InvalidServiceInstance);
        };
        if replica_index >= desired_replicas.get() {
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
