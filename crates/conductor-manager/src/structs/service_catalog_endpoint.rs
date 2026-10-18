use super::*;

/// A manager-derived catalog registration for one persisted allocation/run port.
/// Readiness and worker connectivity are evaluated from durable observations and
/// the current worker-status snapshot so a manager restart cannot resurrect a
/// stale endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceCatalogEndpoint {
    pub job_id: JobId,
    pub service_identity: ServiceIdentity,
    pub service_instance_id: ServiceInstanceId,
    pub allocation_id: AllocationId,
    pub run_id: RunId,
    pub worker_id: WorkerId,
    pub port: AssignedServicePort,
    pub readiness: Option<ReadinessState>,
    pub worker_fresh: bool,
    pub worker_draining: bool,
    pub eligible: bool,
}
