use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerReportRequest {
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub report: WorkerReport,
}
