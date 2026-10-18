use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RunCollection {
    pub(super) runs: Vec<conductor_core::RunObservation>,
    pub(super) next_cursor: Option<String>,
}
