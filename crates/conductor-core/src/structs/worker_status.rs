use super::*;

/// Bounded operational snapshot for one locally registered worker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerStatus {
    pub(super) worker_id: WorkerId,
    pub(super) incarnation: u64,
    pub(super) process_execution: bool,
    pub(super) capacity: usize,
    pub(super) used_slots: usize,
    pub(super) live_processes: usize,
    pub(super) unknown_runs: usize,
    pub(super) pending_persistence: usize,
    pub(super) available_slots: usize,
    pub(super) available: bool,
    pub(super) degraded: bool,
    pub(super) draining: bool,
    pub(super) observed_at_ms: i64,
    #[serde(default)]
    pub(super) inventory: Option<WorkerInventory>,
    #[serde(default)]
    pub(super) resource_domain_id: Option<WorkerId>,
    #[serde(default)]
    pub(super) resource_domain_capacity: Option<WorkerResourceCapacity>,
    #[serde(default)]
    pub(super) labels: BTreeMap<String, String>,
    #[serde(default)]
    pub(super) fresh: bool,
    #[serde(default)]
    pub(super) last_seen_at_ms: Option<i64>,
    #[serde(default)]
    pub(super) service_network: Option<WorkerServiceNetwork>,
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
