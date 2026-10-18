use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServiceWatchResponse {
    pub(super) endpoints: Vec<ServiceEndpoint>,
    pub(super) changed: bool,
}
