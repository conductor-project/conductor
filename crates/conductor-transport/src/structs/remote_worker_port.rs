use super::*;

/// Manager-side adapter that makes a connected remote worker usable through
/// the same explicit `WorkerPort` interface as the local worker.
pub struct RemoteWorkerPort {
    pub(super) session: Arc<WorkerSession>,
}

impl WorkerPort for RemoteWorkerPort {
    fn descriptor(&self) -> WorkerDescriptor {
        self.session.descriptor()
    }

    fn is_draining(&self) -> bool {
        self.session
            .status
            .lock()
            .map(|status| status.draining())
            .unwrap_or(true)
    }

    fn is_degraded(&self) -> bool {
        !self.session.is_fresh()
            || self
                .session
                .status
                .lock()
                .map(|status| status.degraded())
                .unwrap_or(true)
    }

    fn start(&self, command: StartRun) -> Result<RunObservation, WorkerPortError> {
        self.observation(WorkerCommand::StartRun(Box::new(command)))
    }

    fn dispatch_command(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<RunObservation, WorkerPortError> {
        match self.dispatch_envelope(envelope)? {
            WorkerResult::Observation(observation) => Ok(observation),
            WorkerResult::Error(error) => Err(error.into()),
            _ => Err(WorkerPortError::Unavailable),
        }
    }

    fn inspect(&self, command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        match self.dispatch(WorkerCommand::InspectRun(command))? {
            WorkerResult::Inspection(observation) => Ok(observation),
            WorkerResult::Error(error) => Err(error.into()),
            _ => Err(WorkerPortError::Unavailable),
        }
    }

    fn stop(&self, command: StopRun) -> Result<RunObservation, WorkerPortError> {
        self.observation(WorkerCommand::StopRun(command))
    }

    fn resolve(&self, command: ResolveRun) -> Result<RunObservation, WorkerPortError> {
        self.observation(WorkerCommand::ResolveRun(command))
    }

    fn status(&self) -> Result<WorkerStatus, WorkerPortError> {
        self.session
            .status()
            .map(|status| status.with_freshness(self.session.is_fresh()))
    }

    fn read_logs(&self, command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError> {
        match self.dispatch(WorkerCommand::ReadRunLogs(command))? {
            WorkerResult::Logs(chunk) => {
                chunk.into_chunk().map_err(|_| WorkerPortError::Unavailable)
            }
            WorkerResult::Error(error) => Err(error.into()),
            _ => Err(WorkerPortError::Unavailable),
        }
    }
}

impl RemoteWorkerPort {
    pub(super) fn dispatch(&self, command: WorkerCommand) -> Result<WorkerResult, WorkerPortError> {
        let receiver = self.session.enqueue(command).map_err(map_transport_error)?;
        receiver
            .recv_timeout(COMMAND_RESPONSE_TIMEOUT)
            .map_err(|_| WorkerPortError::Unavailable)
    }

    pub(super) fn observation(
        &self,
        command: WorkerCommand,
    ) -> Result<RunObservation, WorkerPortError> {
        match self.dispatch(command)? {
            WorkerResult::Observation(observation) => Ok(observation),
            WorkerResult::Error(error) => Err(error.into()),
            _ => Err(WorkerPortError::Unavailable),
        }
    }

    pub(super) fn dispatch_envelope(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<WorkerResult, WorkerPortError> {
        let receiver = self
            .session
            .enqueue_envelope(envelope)
            .map_err(map_transport_error)?;
        receiver
            .recv_timeout(COMMAND_RESPONSE_TIMEOUT)
            .map_err(|_| WorkerPortError::Unavailable)
    }
}
