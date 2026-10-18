use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServiceEndpoint {
    pub(super) job_id: JobId,
    pub(super) namespace: String,
    pub(super) service_name: String,
    pub(super) service_instance_id: conductor_core::ServiceInstanceId,
    pub(super) allocation_id: conductor_core::AllocationId,
    pub(super) run_id: RunId,
    pub(super) worker_id: WorkerId,
    pub(super) name: String,
    pub(super) protocol: conductor_core::ServicePortProtocol,
    pub(super) advertised_address: String,
    pub(super) port: u16,
    pub(super) ready: bool,
    pub(super) worker_fresh: bool,
    pub(super) worker_draining: bool,
}
