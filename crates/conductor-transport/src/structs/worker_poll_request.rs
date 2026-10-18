use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkerPollRequest {
    pub worker_id: WorkerId,
    pub worker_incarnation: u64,
    pub status: WorkerStatus,
    pub inventory_page: WorkerInventoryPage,
    pub protocol_offer: ProtocolOffer,
}
