use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RunFilters {
    pub(super) job_id: Option<String>,
    pub(super) state: Option<RunState>,
    pub(super) limit: Option<usize>,
    pub(super) cursor: Option<String>,
}
