use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PageFilters {
    pub(super) limit: Option<usize>,
    pub(super) cursor: Option<String>,
}
