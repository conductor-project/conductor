//! Bounded worker inventory pages used during manager-worker reconnect.

use std::num::{NonZeroU32, NonZeroU64};

use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    Architecture, ExecutionModelError, MAX_WORKER_INVENTORY_PAGE_SIZE, OperatingSystem,
    RunObservation, WorkerCapabilities, WorkerId,
};

/// CPU and memory capacity expressed in scheduler-facing units.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerResourceCapacity {
    cpu_millis: NonZeroU32,
    memory_bytes: NonZeroU64,
}

impl WorkerResourceCapacity {
    pub fn new(cpu_millis: u32, memory_bytes: u64) -> Option<Self> {
        Some(Self {
            cpu_millis: NonZeroU32::new(cpu_millis)?,
            memory_bytes: NonZeroU64::new(memory_bytes)?,
        })
    }

    pub fn cpu_millis(self) -> u32 {
        self.cpu_millis.get()
    }

    pub fn memory_bytes(self) -> u64 {
        self.memory_bytes.get()
    }

    pub fn fits_within(self, total: Self) -> bool {
        self.cpu_millis <= total.cpu_millis && self.memory_bytes <= total.memory_bytes
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawWorkerResourceCapacity {
    cpu_millis: NonZeroU32,
    memory_bytes: NonZeroU64,
}

impl<'de> Deserialize<'de> for WorkerResourceCapacity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawWorkerResourceCapacity::deserialize(deserializer)?;
        Ok(Self {
            cpu_millis: raw.cpu_millis,
            memory_bytes: raw.memory_bytes,
        })
    }
}

/// Static worker hardware and execution capabilities, separate from runtime utilization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInventory {
    operating_system: OperatingSystem,
    architecture: Architecture,
    backends: std::collections::BTreeSet<crate::ExecutionBackend>,
    enforcement: std::collections::BTreeSet<crate::EnforcementCapability>,
    total: WorkerResourceCapacity,
    allocatable: WorkerResourceCapacity,
    execution_slots: u16,
}

impl WorkerInventory {
    pub fn new(
        capabilities: WorkerCapabilities,
        total: WorkerResourceCapacity,
        allocatable: WorkerResourceCapacity,
        execution_slots: usize,
    ) -> Result<Self, WorkerInventoryError> {
        let execution_slots = u16::try_from(execution_slots)
            .ok()
            .filter(|slots| (1..=64).contains(slots))
            .ok_or(WorkerInventoryError::InvalidExecutionSlots)?;
        if !allocatable.fits_within(total) {
            return Err(WorkerInventoryError::AllocatableExceedsTotal);
        }
        Ok(Self {
            operating_system: capabilities.operating_system(),
            architecture: capabilities.architecture(),
            backends: capabilities.backends().clone(),
            enforcement: capabilities.enforcement().clone(),
            total,
            allocatable,
            execution_slots,
        })
    }

    pub fn capabilities(&self) -> Result<WorkerCapabilities, ExecutionModelError> {
        WorkerCapabilities::new(
            self.operating_system,
            self.architecture,
            self.backends.clone(),
            self.enforcement.clone(),
        )
    }

    pub fn operating_system(&self) -> OperatingSystem {
        self.operating_system
    }

    pub fn architecture(&self) -> Architecture {
        self.architecture
    }

    pub fn backends(&self) -> &std::collections::BTreeSet<crate::ExecutionBackend> {
        &self.backends
    }

    pub fn enforcement(&self) -> &std::collections::BTreeSet<crate::EnforcementCapability> {
        &self.enforcement
    }

    pub fn total(&self) -> WorkerResourceCapacity {
        self.total
    }

    pub fn allocatable(&self) -> WorkerResourceCapacity {
        self.allocatable
    }

    pub fn execution_slots(&self) -> usize {
        usize::from(self.execution_slots)
    }

    pub fn with_allocatable(
        &self,
        allocatable: WorkerResourceCapacity,
    ) -> Result<Self, WorkerInventoryError> {
        Self::new(
            self.capabilities()
                .map_err(|_| WorkerInventoryError::InvalidCapabilities)?,
            self.total,
            allocatable,
            self.execution_slots(),
        )
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawWorkerInventory {
    operating_system: OperatingSystem,
    architecture: Architecture,
    backends: std::collections::BTreeSet<crate::ExecutionBackend>,
    #[serde(default)]
    enforcement: std::collections::BTreeSet<crate::EnforcementCapability>,
    total: WorkerResourceCapacity,
    allocatable: WorkerResourceCapacity,
    execution_slots: u16,
}

impl<'de> Deserialize<'de> for WorkerInventory {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawWorkerInventory::deserialize(deserializer)?;
        let capabilities = WorkerCapabilities::new(
            raw.operating_system,
            raw.architecture,
            raw.backends,
            raw.enforcement,
        )
        .map_err(serde::de::Error::custom)?;
        Self::new(
            capabilities,
            raw.total,
            raw.allocatable,
            usize::from(raw.execution_slots),
        )
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerInventoryError {
    InvalidExecutionSlots,
    AllocatableExceedsTotal,
    InvalidCapabilities,
}

impl std::fmt::Display for WorkerInventoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::InvalidExecutionSlots => "worker execution slots must be between 1 and 64",
            Self::AllocatableExceedsTotal => "allocatable worker capacity exceeds total capacity",
            Self::InvalidCapabilities => "worker capabilities are invalid",
        })
    }
}

impl std::error::Error for WorkerInventoryError {}

/// Manager-owned labels and aggregate scheduling capacity for one physical host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerMetadata {
    resource_domain_id: WorkerId,
    capacity: Option<WorkerResourceCapacity>,
    labels: std::collections::BTreeMap<String, String>,
    service_network: Option<WorkerServiceNetwork>,
}

/// Manager-configured network namespace identity and disjoint host-port range
/// for one native worker. Workers sharing a host network must share the domain
/// ID and use non-overlapping ranges.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerServiceNetwork {
    network_domain_id: WorkerId,
    port_range_start: u16,
    port_range_end: u16,
    advertised_address: String,
}

impl WorkerServiceNetwork {
    pub fn new(
        network_domain_id: WorkerId,
        port_range_start: u16,
        port_range_end: u16,
        advertised_address: impl Into<String>,
    ) -> Option<Self> {
        let advertised_address = advertised_address.into();
        if port_range_start == 0
            || port_range_start > port_range_end
            || !valid_advertised_address(&advertised_address)
        {
            return None;
        }
        Some(Self {
            network_domain_id,
            port_range_start,
            port_range_end,
            advertised_address,
        })
    }

    pub fn network_domain_id(&self) -> &WorkerId {
        &self.network_domain_id
    }
    pub fn port_range_start(&self) -> u16 {
        self.port_range_start
    }
    pub fn port_range_end(&self) -> u16 {
        self.port_range_end
    }
    pub fn advertised_address(&self) -> &str {
        &self.advertised_address
    }

    pub fn is_valid(&self) -> bool {
        self.port_range_start > 0
            && self.port_range_start <= self.port_range_end
            && valid_advertised_address(&self.advertised_address)
    }
}

pub(crate) fn valid_advertised_address(value: &str) -> bool {
    if value.is_empty() || value.len() > 253 {
        return false;
    }
    if let Ok(address) = value.parse::<std::net::IpAddr>() {
        return !address.is_loopback() && !address.is_unspecified() && !address.is_multicast();
    }
    if value.contains(['/', ':', '@', '?', '#'])
        || value.eq_ignore_ascii_case("localhost")
        || value.to_ascii_lowercase().ends_with(".localhost")
    {
        return false;
    }
    value.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && label.as_bytes()[0].is_ascii_alphanumeric()
            && label
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && !label.ends_with('-')
    })
}

impl WorkerMetadata {
    pub fn automatic(resource_domain_id: WorkerId) -> Self {
        Self {
            resource_domain_id,
            capacity: None,
            labels: std::collections::BTreeMap::new(),
            service_network: None,
        }
    }

    pub fn new(
        resource_domain_id: WorkerId,
        capacity: Option<WorkerResourceCapacity>,
        labels: std::collections::BTreeMap<String, String>,
    ) -> Option<Self> {
        crate::valid_worker_labels(&labels).then_some(Self {
            resource_domain_id,
            capacity,
            labels,
            service_network: None,
        })
    }

    pub fn resource_domain_id(&self) -> &WorkerId {
        &self.resource_domain_id
    }

    pub fn capacity(&self) -> Option<WorkerResourceCapacity> {
        self.capacity
    }

    pub fn labels(&self) -> &std::collections::BTreeMap<String, String> {
        &self.labels
    }

    pub fn with_service_network(mut self, service_network: WorkerServiceNetwork) -> Self {
        self.service_network = Some(service_network);
        self
    }

    pub fn service_network(&self) -> Option<&WorkerServiceNetwork> {
        self.service_network.as_ref()
    }
}

/// One durable worker run record and its worker-database ordering key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInventoryEntry {
    pub sequence: u64,
    pub observation: RunObservation,
}

/// One bounded, keyset-paginated view of a worker's durable run history.
///
/// `snapshot_generation` is the highest worker-database row sequence visible
/// when the first page was read. Later pages use it as an upper bound so new
/// runs cannot shift or extend a scan already in progress.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInventoryPage {
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub snapshot_generation: u64,
    pub after_sequence: u64,
    pub entries: Vec<WorkerInventoryEntry>,
    pub next_after_sequence: Option<u64>,
}

impl WorkerInventoryPage {
    /// Validate wire bounds and cursor ordering before reconciling a page.
    pub fn is_valid(&self) -> bool {
        if self.entries.len() > MAX_WORKER_INVENTORY_PAGE_SIZE
            || self.after_sequence > self.snapshot_generation
        {
            return false;
        }
        let mut previous = self.after_sequence;
        for entry in &self.entries {
            if entry.sequence <= previous
                || entry.sequence > self.snapshot_generation
                || entry.observation.worker_id() != &self.worker_id
            {
                return false;
            }
            previous = entry.sequence;
        }
        match self.next_after_sequence {
            Some(next) => {
                self.entries
                    .last()
                    .is_some_and(|last| last.sequence == next)
                    && next > self.after_sequence
                    && next < self.snapshot_generation
            }
            None => true,
        }
    }
}
