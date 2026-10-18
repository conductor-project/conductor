//! Transport-level JSON shapes shared by the server and client.

use conductor_core::{
    AllocationId, AllocationState, AssignedServicePort, JobRevisionId, JobSpec, LogStream,
    ResourceReservation, RunId, RunObservation, ServiceInstanceId, ServicePortProtocol, WorkerId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobCollection {
    pub jobs: Vec<JobSpec>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunCollection {
    pub runs: Vec<RunObservation>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceEndpoint {
    pub job_id: conductor_core::JobId,
    pub namespace: String,
    pub service_name: String,
    pub service_instance_id: ServiceInstanceId,
    pub allocation_id: AllocationId,
    pub run_id: RunId,
    pub worker_id: WorkerId,
    pub name: String,
    pub protocol: ServicePortProtocol,
    pub advertised_address: String,
    pub port: u16,
    pub ready: bool,
    pub worker_fresh: bool,
    pub worker_draining: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceEndpointCollection {
    pub endpoints: Vec<ServiceEndpoint>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceWatchResponse {
    pub endpoints: Vec<ServiceEndpoint>,
    pub changed: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeployRequest {
    pub revision: u64,
    pub replicas: u32,
    #[serde(default)]
    pub strategy: conductor_core::DeploymentUpdateStrategy,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ScaleDeploymentRequest {
    pub replicas: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RollbackRequest {
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolveRunRequest {
    pub resolution: String,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerDrainRequest {
    pub grace_period_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDrainStatus {
    pub worker_id: WorkerId,
    pub state: String,
    pub requested_at_ms: i64,
    pub deadline_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub active_runs: usize,
    pub blocked_reason: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAttemptCollection {
    pub service_attempts: Vec<ServiceAttemptRecord>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocationRecord {
    pub allocation: Allocation,
    pub resource_domain_id: Option<WorkerId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    pub id: AllocationId,
    pub job_revision_id: JobRevisionId,
    pub service_instance_id: Option<ServiceInstanceId>,
    pub assigned_worker_id: Option<WorkerId>,
    pub assignment_generation: u64,
    pub state: AllocationState,
    pub reservation: ResourceReservation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ServiceWatchQuery {
    pub wait_seconds: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RunLogsResponse {
    pub run_id: String,
    pub stream: LogStream,
    pub offset: u64,
    pub next_offset: u64,
    pub data_encoding: String,
    pub data: String,
    pub stream_closed: bool,
    pub eof: bool,
    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadinessResponse {
    pub status: String,
    pub role: String,
    pub capabilities: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_response_contracts_decode_server_json_shapes() {
        let jobs: JobCollection = serde_json::from_str(r#"{"jobs":[],"nextCursor":"next"}"#)
            .expect("job collection contract");
        assert_eq!(jobs.next_cursor.as_deref(), Some("next"));

        let attempts: ServiceAttemptCollection =
            serde_json::from_str(r#"{"serviceAttempts":[]}"#).expect("service attempt contract");
        assert!(attempts.service_attempts.is_empty());

        let drain: WorkerDrainStatus = serde_json::from_str(
            r#"{"workerId":"worker-1","state":"draining","requestedAtMs":1,"deadlineAtMs":2,"completedAtMs":null,"activeRuns":0,"blockedReason":null}"#,
        )
        .expect("worker drain contract");
        assert_eq!(drain.state, "draining");
    }

    #[test]
    fn mutation_contracts_reject_unknown_fields() {
        assert!(
            serde_json::from_str::<DeployRequest>(
                r#"{"revision":1,"replicas":2,"unexpected":true}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<WorkerDrainRequest>(
                r#"{"gracePeriodSeconds":30,"unexpected":true}"#
            )
            .is_err()
        );
    }
}
