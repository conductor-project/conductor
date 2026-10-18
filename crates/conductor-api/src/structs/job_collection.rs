use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct JobCollection {
    pub(super) jobs: Vec<JobSpec>,
    pub(super) next_cursor: Option<String>,
}
