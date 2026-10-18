use super::*;

/// Idempotent request to stop one accepted run.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StopRun {
    pub(super) run_id: RunId,
}

impl StopRun {
    pub fn new(run_id: RunId) -> Self {
        Self { run_id }
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
}
