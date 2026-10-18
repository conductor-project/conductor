use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RawRunSpec {
    pub(super) id: RunId,
    pub(super) worker_id: WorkerId,
    pub(super) job: JobSpec,
    #[serde(default)]
    pub(super) service_ports: Vec<AssignedServicePort>,
    #[serde(default)]
    pub(super) readiness_probe: Option<ReadinessProbe>,
}
