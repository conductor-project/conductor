use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServiceAttemptCollection {
    pub(super) service_attempts: Vec<ServiceAttemptRecord>,
}
