use super::*;

/// An append-only operator or enrollment action. Values contain stable IDs and
/// a bounded principal/action vocabulary, never request or credential data.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEvent {
    pub sequence: u64,
    pub occurred_at_ms: i64,
    pub principal: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub outcome: String,
    pub status_code: u16,
}
