use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerPollResponse {
    pub protocol_version: u16,
    pub capabilities: Vec<ProtocolCapability>,
    pub inventory_acknowledgement: WorkerInventoryAcknowledgement,
    pub command: Option<CommandEnvelope>,
}
