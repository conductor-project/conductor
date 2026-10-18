use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerReport {
    pub command_id: CommandId,
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub report_sequence: u64,
    pub result: WorkerResult,
}
