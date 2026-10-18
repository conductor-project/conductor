use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartRun {
    pub(super) spec: RunSpec,
}

impl StartRun {
    pub fn new(spec: RunSpec) -> Self {
        Self { spec }
    }
    pub fn spec(&self) -> &RunSpec {
        &self.spec
    }
}
