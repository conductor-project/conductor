use super::*;

pub(super) struct WorkerInventorySync {
    pub(super) worker_incarnation: u64,
    pub(super) snapshot_generation: u64,
    pub(super) expected_after_sequence: u64,
    pub(super) observed_manager_runs: BTreeSet<RunId>,
}
