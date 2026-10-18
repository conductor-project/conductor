use super::*;

/// Durable manager-side state for one worker command. The acknowledgement
/// payload is retained so a worker can safely retry a report after a manager
/// restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandRecord {
    pub envelope: CommandEnvelope,
    pub acknowledgement_json: Option<String>,
}
