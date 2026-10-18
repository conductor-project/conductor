/// One audit write request. Values are validated against fixed vocabularies and
/// length bounds before being persisted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditEventInput {
    pub principal: String,
    pub action: String,
    pub resource_type: String,
    pub resource_id: Option<String>,
    pub outcome: String,
    pub status_code: u16,
    pub occurred_at_ms: i64,
}
