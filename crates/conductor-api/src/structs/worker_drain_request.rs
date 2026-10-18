use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WorkerDrainRequest {
    pub(super) grace_period_seconds: Option<u64>,
}
