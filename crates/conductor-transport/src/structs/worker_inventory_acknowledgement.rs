use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerInventoryAcknowledgement {
    pub snapshot_generation: u64,
    pub next_after_sequence: Option<u64>,
    pub restart_scan: bool,
}
