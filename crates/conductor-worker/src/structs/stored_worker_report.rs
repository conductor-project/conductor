use super::*;

/// A worker report persisted with its command result until the manager
/// acknowledges the report sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredWorkerReport {
    pub command_id: CommandId,
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub report_sequence: u64,
    pub result_json: String,
}
