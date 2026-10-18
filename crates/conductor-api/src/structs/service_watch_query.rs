use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ServiceWatchQuery {
    pub(super) wait_seconds: Option<u64>,
}
