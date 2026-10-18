use super::*;

/// Operator-authorized resolution of an ambiguous run after external cleanup.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolveRun {
    pub(super) run_id: RunId,
    pub(super) reason: String,
}

impl ResolveRun {
    pub fn new(run_id: RunId, reason: String) -> Self {
        Self { run_id, reason }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn reason(&self) -> &str {
        &self.reason
    }
}
