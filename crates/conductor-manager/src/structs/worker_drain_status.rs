use super::*;

/// Current worker drain progress derived from durable runs and allocations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDrainStatus {
    pub worker_id: WorkerId,
    pub state: &'static str,
    pub requested_at_ms: i64,
    pub deadline_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub active_runs: usize,
    pub blocked_reason: Option<&'static str>,
}
