use super::*;

/// Serializable worker observation. Process IDs are deliberately not part of the contract.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RunObservation {
    pub(super) run_id: RunId,
    pub(super) job_id: JobId,
    pub(super) worker_id: WorkerId,
    pub(super) revision: u64,
    pub(super) state: RunState,
    pub(super) requested_at_ms: i64,
    pub(super) started_at_ms: Option<i64>,
    pub(super) finished_at_ms: Option<i64>,
    pub(super) exit_code: Option<i32>,
    pub(super) termination_signal: Option<i32>,
    pub(super) failure_reason: Option<RunFailureReason>,
    #[serde(default)]
    pub(super) termination_reason: Option<RunTerminationReason>,
    #[serde(default)]
    pub(super) resolution_reason: Option<String>,
    #[serde(default)]
    pub(super) worker_incarnation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) readiness: Option<ReadinessState>,
}

impl RunObservation {
    pub fn requested(spec: &RunSpec, requested_at_ms: i64) -> Self {
        Self {
            run_id: spec.id.clone(),
            job_id: spec.job.id.clone(),
            worker_id: spec.worker_id.clone(),
            revision: 1,
            state: RunState::Requested,
            requested_at_ms,
            started_at_ms: None,
            finished_at_ms: None,
            exit_code: None,
            termination_signal: None,
            failure_reason: None,
            termination_reason: None,
            resolution_reason: None,
            worker_incarnation: 0,
            readiness: None,
        }
    }

    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn job_id(&self) -> &JobId {
        &self.job_id
    }
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn state(&self) -> RunState {
        self.state
    }
    pub fn requested_at_ms(&self) -> i64 {
        self.requested_at_ms
    }
    pub fn started_at_ms(&self) -> Option<i64> {
        self.started_at_ms
    }
    pub fn finished_at_ms(&self) -> Option<i64> {
        self.finished_at_ms
    }
    pub fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }
    pub fn termination_signal(&self) -> Option<i32> {
        self.termination_signal
    }
    pub fn failure_reason(&self) -> Option<RunFailureReason> {
        self.failure_reason
    }
    pub fn termination_reason(&self) -> Option<RunTerminationReason> {
        self.termination_reason
    }
    pub fn resolution_reason(&self) -> Option<&str> {
        self.resolution_reason.as_deref()
    }

    pub fn with_resolution_reason(mut self, reason: String) -> Self {
        self.resolution_reason = Some(reason);
        self
    }
    pub fn with_worker_incarnation(mut self, incarnation: u64) -> Self {
        self.worker_incarnation = incarnation;
        self
    }
    pub fn worker_incarnation(&self) -> u64 {
        self.worker_incarnation
    }

    pub fn readiness(&self) -> Option<ReadinessState> {
        self.readiness
    }

    pub fn with_readiness(mut self, readiness: ReadinessState) -> Self {
        self.readiness = Some(readiness);
        self
    }

    pub fn with_termination_reason(mut self, reason: RunTerminationReason) -> Self {
        self.termination_reason = Some(reason);
        self
    }
    pub fn with_optional_termination_reason(
        mut self,
        reason: Option<RunTerminationReason>,
    ) -> Self {
        self.termination_reason = reason;
        self
    }

    pub fn next_revision(
        &self,
        state: RunState,
        started_at_ms: Option<i64>,
        finished_at_ms: Option<i64>,
        exit_code: Option<i32>,
        termination_signal: Option<i32>,
        failure_reason: Option<RunFailureReason>,
    ) -> Option<Self> {
        Some(Self {
            run_id: self.run_id.clone(),
            job_id: self.job_id.clone(),
            worker_id: self.worker_id.clone(),
            revision: self.revision.checked_add(1)?,
            state,
            requested_at_ms: self.requested_at_ms,
            started_at_ms,
            finished_at_ms,
            exit_code,
            termination_signal,
            failure_reason,
            termination_reason: self.termination_reason,
            resolution_reason: self.resolution_reason.clone(),
            worker_incarnation: self.worker_incarnation,
            readiness: self.readiness,
        })
    }

    pub fn next_readiness_revision(&self, readiness: ReadinessState) -> Option<Self> {
        let mut next = self.next_revision(
            self.state,
            self.started_at_ms,
            self.finished_at_ms,
            self.exit_code,
            self.termination_signal,
            self.failure_reason,
        )?;
        next.readiness = Some(readiness);
        Some(next)
    }

    /// Apply an observation only if it is newer than the currently accepted revision.
    pub fn apply_newer(&mut self, newer: Self) -> bool {
        if self.state.is_terminal()
            || newer.run_id != self.run_id
            || newer.job_id != self.job_id
            || newer.worker_id != self.worker_id
            || newer.revision <= self.revision
        {
            return false;
        }
        *self = newer;
        true
    }
}
