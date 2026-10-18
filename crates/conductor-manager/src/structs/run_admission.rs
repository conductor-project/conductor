use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunAdmission {
    pub spec: RunSpec,
    pub replay: Option<RunObservation>,
    pub command: Option<CommandEnvelope>,
}

impl std::ops::Deref for RunAdmission {
    type Target = RunSpec;

    fn deref(&self) -> &Self::Target {
        &self.spec
    }
}
