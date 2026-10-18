use super::*;

pub(super) struct PendingCommand {
    pub(super) envelope: CommandEnvelope,
    pub(super) responses: Vec<SyncSender<WorkerResult>>,
    pub(super) retry_at: Instant,
}
