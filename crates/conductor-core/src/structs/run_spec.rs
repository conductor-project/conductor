use super::*;

/// A fixed job-definition snapshot assigned to one worker execution attempt.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", try_from = "RawRunSpec")]
pub struct RunSpec {
    pub(super) id: RunId,
    pub(super) worker_id: WorkerId,
    pub(super) job: JobSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(super) service_ports: Vec<AssignedServicePort>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) readiness_probe: Option<ReadinessProbe>,
}

impl RunSpec {
    pub fn new(id: RunId, worker_id: WorkerId, job: JobSpec) -> Self {
        Self {
            id,
            worker_id,
            job,
            service_ports: Vec::new(),
            readiness_probe: None,
        }
    }

    pub fn with_service_ports(
        mut self,
        service_ports: Vec<AssignedServicePort>,
    ) -> Result<Self, ExecutionModelError> {
        validate_assigned_service_ports(&self.job, &service_ports)?;
        validate_readiness_probe(self.readiness_probe.as_ref(), &service_ports)?;
        self.service_ports = service_ports;
        Ok(self)
    }

    pub fn with_readiness_probe(
        mut self,
        readiness_probe: ReadinessProbe,
    ) -> Result<Self, ExecutionModelError> {
        if !readiness_probe.is_valid()
            || !self
                .service_ports
                .iter()
                .any(|port| port.name() == readiness_probe.port_name())
        {
            return Err(ExecutionModelError::InvalidReadinessProbe);
        }
        self.readiness_probe = Some(readiness_probe);
        Ok(self)
    }

    pub fn id(&self) -> &RunId {
        &self.id
    }

    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    pub fn job(&self) -> &JobSpec {
        &self.job
    }

    pub fn service_ports(&self) -> &[AssignedServicePort] {
        &self.service_ports
    }

    pub fn readiness_probe(&self) -> Option<&ReadinessProbe> {
        self.readiness_probe.as_ref()
    }
}

impl TryFrom<RawRunSpec> for RunSpec {
    type Error = ExecutionModelError;

    fn try_from(raw: RawRunSpec) -> Result<Self, Self::Error> {
        validate_assigned_service_ports(&raw.job, &raw.service_ports)?;
        validate_readiness_probe(raw.readiness_probe.as_ref(), &raw.service_ports)?;
        Ok(Self {
            id: raw.id,
            worker_id: raw.worker_id,
            job: raw.job,
            service_ports: raw.service_ports,
            readiness_probe: raw.readiness_probe,
        })
    }
}
