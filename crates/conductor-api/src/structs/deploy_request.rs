use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeployRequest {
    pub(super) revision: u64,
    pub(super) replicas: u32,
    #[serde(default)]
    pub(super) strategy: DeploymentUpdateStrategy,
}
