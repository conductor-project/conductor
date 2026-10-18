use super::*;

/// Durable intent to stop scheduling and drain work from one worker.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDrainRecord {
    pub worker_id: WorkerId,
    pub requested_at_ms: i64,
    pub deadline_at_ms: i64,
    pub completed_at_ms: Option<i64>,
}
