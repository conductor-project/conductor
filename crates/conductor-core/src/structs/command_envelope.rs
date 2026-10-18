use super::*;

/// Stable manager-owned command identity and worker-incarnation target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommandEnvelope {
    pub command_id: CommandId,
    pub target_worker_id: WorkerId,
    pub target_worker_incarnation: u64,
    pub command: WorkerCommand,
}
