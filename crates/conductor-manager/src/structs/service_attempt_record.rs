use super::*;

/// Persisted control state for one service instance's current or next execution.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAttemptRecord {
    pub service_instance_id: ServiceInstanceId,
    pub replica_index: u32,
    pub current_allocation_id: Option<AllocationId>,
    pub allocation: Option<AllocationRecord>,
    pub current_run_id: Option<RunId>,
    pub current_run: Option<RunObservation>,
    pub restart_count: u32,
    pub next_start_at_ms: i64,
    pub terminal_failure: bool,
    pub service_ports: Vec<AssignedServicePort>,
    pub service_ports_ready: bool,
}
