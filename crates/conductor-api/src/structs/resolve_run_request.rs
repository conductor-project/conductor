use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ResolveRunRequest {
    pub(super) resolution: String,
    pub(super) reason: String,
}
