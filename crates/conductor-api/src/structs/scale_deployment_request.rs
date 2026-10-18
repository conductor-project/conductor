use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ScaleDeploymentRequest {
    pub(super) replicas: u32,
}
