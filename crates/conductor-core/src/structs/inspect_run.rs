use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InspectRun {
    pub(super) run_id: RunId,
}

impl InspectRun {
    pub fn new(run_id: RunId) -> Self {
        Self { run_id }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
}
