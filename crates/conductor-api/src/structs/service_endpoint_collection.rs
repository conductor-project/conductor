use super::*;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ServiceEndpointCollection {
    pub(super) endpoints: Vec<ServiceEndpoint>,
}
