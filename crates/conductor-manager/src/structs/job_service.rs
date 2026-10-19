use super::*;

/// Manager application service for immutable job definitions.
pub struct JobService {
    pub(super) repository: Arc<dyn JobRepository>,
    pub(super) telemetry: ManagerTelemetry,
}

impl JobService {
    pub fn new(repository: Arc<dyn JobRepository>) -> Self {
        Self {
            repository,
            telemetry: ManagerTelemetry::default(),
        }
    }

    pub fn record_audit_event(
        &self,
        principal: &str,
        action: &str,
        resource_type: &str,
        resource_id: Option<&str>,
        outcome: &str,
        status_code: u16,
    ) -> Result<(), ExecutionStoreError> {
        self.repository.record_audit_event(&AuditEventInput {
            principal: principal.to_owned(),
            action: action.to_owned(),
            resource_type: resource_type.to_owned(),
            resource_id: resource_id.map(str::to_owned),
            outcome: outcome.to_owned(),
            status_code,
            occurred_at_ms: now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?,
        })
    }

    pub fn list_audit_events(&self, limit: usize) -> Result<Vec<AuditEvent>, ExecutionStoreError> {
        self.repository.list_audit_events(limit)
    }

    pub fn operational_metrics(&self) -> Result<OperationalMetrics, ExecutionStoreError> {
        self.repository.operational_metrics()
    }

    pub fn create_overlay_network_domain(
        &self,
        config: &conductor_core::OverlayNetworkDomainConfig,
        now_ms: i64,
    ) -> Result<(), OverlayNetworkError> {
        self.repository
            .create_overlay_network_domain(config, now_ms)
    }

    pub fn get_overlay_network_domain(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
    ) -> Result<Option<conductor_core::OverlayNetworkDomainConfig>, OverlayNetworkError> {
        self.repository
            .get_overlay_network_domain(network_domain_id)
    }

    pub fn register_overlay_peer(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        endpoint_id: &conductor_core::OverlayEndpointId,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<conductor_core::OverlayPeerAssignment, OverlayNetworkError> {
        self.repository.register_overlay_peer(
            network_domain_id,
            worker_id,
            endpoint_id,
            expires_at_ms,
            now_ms,
        )
    }

    pub fn revoke_overlay_peer(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        now_ms: i64,
    ) -> Result<bool, OverlayNetworkError> {
        self.repository
            .revoke_overlay_peer(network_domain_id, worker_id, now_ms)
    }

    pub fn overlay_peer_configuration(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        now_ms: i64,
    ) -> Result<Option<conductor_core::OverlayPeerConfiguration>, OverlayNetworkError> {
        self.repository
            .overlay_peer_configuration(network_domain_id, worker_id, now_ms)
    }

    pub fn record_overlay_peer_observation(
        &self,
        observation: &conductor_core::OverlayPeerObservation,
    ) -> Result<(), OverlayNetworkError> {
        self.repository.record_overlay_peer_observation(observation)
    }

    pub fn list_overlay_peers(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        limit: usize,
        now_ms: i64,
    ) -> Result<Vec<conductor_core::OverlayPeerAssignment>, OverlayNetworkError> {
        self.repository
            .list_overlay_peers(network_domain_id, limit, now_ms)
    }

    pub fn reconcile_metrics(&self) -> ReconcileMetrics {
        self.telemetry.snapshot()
    }

    pub fn open_sqlite(path: impl AsRef<Path>) -> Result<Self, RepositoryOpenError> {
        Ok(Self::new(Arc::new(SqliteJobRepository::open(path)?)))
    }

    pub fn register(&self, job: JobSpec) -> Result<(), ManagerError> {
        match self.repository.insert(job) {
            Ok(()) => Ok(()),
            Err(JobStoreError::DuplicateId) => Err(ManagerError::DuplicateJob),
            Err(JobStoreError::LimitExceeded) => Err(ManagerError::JobLimitReached),
            Err(JobStoreError::ActiveRunConflict) => Err(ManagerError::JobConflict),
            Err(JobStoreError::StorageUnavailable) => Err(ManagerError::StorageUnavailable),
        }
    }

    pub fn get(&self, id: &JobId) -> Result<Option<JobSpec>, ManagerError> {
        self.repository.get(id).map_err(map_store_error)
    }

    /// Read bounded worker operational state through its owner port.
    pub fn worker_status(&self, worker: &dyn WorkerPort) -> Result<WorkerStatus, ManagerError> {
        let status = worker
            .status()
            .map_err(|_| ManagerError::WorkerUnavailable)?;
        if status.is_valid() {
            let worker_id = status.worker_id().clone();
            let manager_drain = self
                .repository
                .get_worker_drain(&worker_id)
                .map_err(|_| ManagerError::StorageUnavailable)?
                .is_some();
            Ok(if manager_drain {
                status.with_draining()
            } else {
                status
            })
        } else {
            Err(ManagerError::WorkerUnavailable)
        }
    }

    pub fn request_worker_drain(
        &self,
        worker_id: &WorkerId,
        grace_period_ms: i64,
    ) -> Result<WorkerDrainStatus, ManagerError> {
        if grace_period_ms <= 0 {
            return Err(ManagerError::InvalidDrainRequest);
        }
        self.repository
            .request_worker_drain(worker_id, grace_period_ms)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        self.worker_drain_status(worker_id)?
            .ok_or(ManagerError::StorageUnavailable)
    }

    pub fn worker_drain_status(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Option<WorkerDrainStatus>, ManagerError> {
        let Some(mut record) = self
            .repository
            .get_worker_drain(worker_id)
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Ok(None);
        };
        let active_runs = self
            .repository
            .list_active_runs(worker_id, MAX_WORKER_CAPACITY)
            .map_err(map_run_error)?;
        let mut replacement_pending = false;
        if !active_runs.is_empty() {
            let deployments = self
                .repository
                .list_active_deployments()
                .map_err(|_| ManagerError::StorageUnavailable)?;
            for deployment in deployments {
                let attempts = self
                    .repository
                    .list_service_attempts(deployment.job_id())
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                let allocations = self
                    .repository
                    .list_allocations(deployment.job_id())
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                for attempt in attempts {
                    let is_draining_run = attempt.current_run.as_ref().is_some_and(|run| {
                        run.worker_id() == worker_id && !run.state().is_terminal()
                    });
                    let replacement_is_pending = attempt
                        .current_allocation_id
                        .as_ref()
                        .and_then(|current_id| {
                            allocations
                                .iter()
                                .find(|allocation| allocation.allocation().id() == current_id)
                        })
                        .is_some_and(|allocation| {
                            allocation.allocation().state() == AllocationState::Pending
                        });
                    if is_draining_run
                        && attempt.current_allocation_id.as_ref()
                            != attempt
                                .allocation
                                .as_ref()
                                .map(|allocation| allocation.allocation().id())
                        && replacement_is_pending
                    {
                        replacement_pending = true;
                    }
                }
            }
        }
        if active_runs.is_empty() && record.completed_at_ms.is_none() {
            self.repository
                .complete_worker_drain(
                    worker_id,
                    now_ms().map_err(|_| ManagerError::StorageUnavailable)?,
                )
                .map_err(|_| ManagerError::StorageUnavailable)?;
            record = self
                .repository
                .get_worker_drain(worker_id)
                .map_err(|_| ManagerError::StorageUnavailable)?
                .ok_or(ManagerError::StorageUnavailable)?;
        }
        let deadline_elapsed =
            now_ms().map_err(|_| ManagerError::StorageUnavailable)? >= record.deadline_at_ms;
        let blocked_reason = if replacement_pending {
            Some("replacement_capacity")
        } else if deadline_elapsed && !active_runs.is_empty() {
            Some("termination_pending")
        } else {
            None
        };
        Ok(Some(WorkerDrainStatus {
            worker_id: record.worker_id,
            state: if record.completed_at_ms.is_some() {
                "completed"
            } else if blocked_reason.is_some() {
                "blocked"
            } else {
                "draining"
            },
            requested_at_ms: record.requested_at_ms,
            deadline_at_ms: record.deadline_at_ms,
            completed_at_ms: record.completed_at_ms,
            active_runs: active_runs.len(),
            blocked_reason,
        }))
    }

    pub fn cancel_worker_drain(&self, worker_id: &WorkerId) -> Result<bool, ManagerError> {
        self.repository
            .cancel_worker_drain(worker_id)
            .map_err(|_| ManagerError::StorageUnavailable)
    }

    pub fn list_page(
        &self,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<JobSpec>, QueryError> {
        self.repository.list_jobs_page(limit, cursor)
    }

    pub fn delete(&self, id: &JobId) -> Result<bool, ManagerError> {
        self.repository.delete(id).map_err(map_store_error)
    }

    pub fn create_revision(
        &self,
        draft: &JobRevisionDraft,
        idempotency_key: &str,
    ) -> Result<JobRevision, ExecutionStoreError> {
        self.repository.create_revision(draft, idempotency_key)
    }

    pub fn get_revision(
        &self,
        id: &JobRevisionId,
    ) -> Result<Option<JobRevision>, ExecutionStoreError> {
        self.repository.get_revision(id)
    }

    pub fn get_deployment(&self, id: &JobId) -> Result<Option<JobDeployment>, ExecutionStoreError> {
        self.repository.get_deployment(id)
    }

    pub fn mutate_deployment(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
    ) -> Result<JobDeployment, ExecutionStoreError> {
        self.repository
            .mutate_deployment(id, expected_generation, mutation, idempotency_key)
    }

    pub fn mutate_deployment_with_workers(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
        workers: &[WorkerStatus],
    ) -> Result<JobDeployment, ExecutionStoreError> {
        self.repository.mutate_deployment_with_workers(
            id,
            expected_generation,
            mutation,
            idempotency_key,
            workers,
        )
    }

    pub fn reconcile_deployment_placement(
        &self,
        id: &JobId,
        workers: &[WorkerStatus],
    ) -> Result<Option<JobDeployment>, ExecutionStoreError> {
        self.repository.reconcile_deployment_placement(id, workers)
    }

    pub fn list_allocations(
        &self,
        id: &JobId,
    ) -> Result<Vec<AllocationRecord>, ExecutionStoreError> {
        self.repository.list_allocations(id)
    }

    pub fn list_service_attempts(
        &self,
        id: &JobId,
    ) -> Result<Vec<ServiceAttemptRecord>, ExecutionStoreError> {
        self.repository.list_service_attempts(id)
    }

    pub fn list_service_catalog(
        &self,
        identity: &ServiceIdentity,
        workers: &[WorkerStatus],
    ) -> Result<Vec<ServiceCatalogEndpoint>, ExecutionStoreError> {
        self.repository.list_service_catalog(identity, workers)
    }

    pub fn service_identity_exists(
        &self,
        identity: &ServiceIdentity,
    ) -> Result<bool, ExecutionStoreError> {
        self.repository.service_identity_exists(identity)
    }

    /// Reconcile placed service slots with their workers. Repeated calls are
    /// safe: the repository reserves one run ID per service instance before
    /// dispatch, and unresolved worker outcomes are never relaunched.
    pub fn reconcile_services(&self, workers: &[Arc<dyn WorkerPort>]) -> Result<(), ManagerError> {
        let started = Instant::now();
        let result = self.reconcile_services_inner(workers);
        self.telemetry
            .record_reconcile(started.elapsed(), result.is_err());
        result
    }

    pub(super) fn reconcile_services_inner(
        &self,
        workers: &[Arc<dyn WorkerPort>],
    ) -> Result<(), ManagerError> {
        let statuses = workers
            .iter()
            .filter_map(|worker| self.worker_status(worker.as_ref()).ok())
            .filter(WorkerStatus::is_valid)
            .collect::<Vec<_>>();
        let drain_records = self
            .repository
            .list_worker_drains()
            .map_err(|_| ManagerError::StorageUnavailable)?
            .into_iter()
            .filter(|record| record.completed_at_ms.is_none())
            .collect::<Vec<_>>();
        let mut service_run_ids = BTreeSet::new();
        let deployments = self
            .repository
            .list_active_deployments()
            .map_err(|_| ManagerError::StorageUnavailable)?;
        for mut deployment in deployments {
            let job_id = deployment.job_id().clone();
            if !statuses.is_empty() || deployment.rollout().is_some() {
                self.repository
                    .reconcile_deployment_placement(&job_id, &statuses)
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                if let Some(updated) = self
                    .repository
                    .get_deployment(&job_id)
                    .map_err(|_| ManagerError::StorageUnavailable)?
                {
                    deployment = updated;
                }
            }
            let allocations = self
                .repository
                .list_allocations(&job_id)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            let attempts = self
                .repository
                .list_service_attempts(&job_id)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            let rollout_wave_slots = if deployment.rollout().is_some() {
                attempts
                    .iter()
                    .filter(|attempt| {
                        if attempt.replica_index >= deployment.desired_replicas()
                            || deployment.suspended()
                        {
                            return false;
                        }
                        let target_allocation = attempt
                            .current_allocation_id
                            .as_ref()
                            .and_then(|allocation_id| {
                                allocations
                                    .iter()
                                    .find(|record| record.allocation().id() == allocation_id)
                            })
                            .is_some_and(|record| {
                                record.allocation().job_revision_id() == deployment.revision_id()
                            });
                        if !target_allocation {
                            return false;
                        }
                        match (&attempt.current_run, &attempt.allocation) {
                            (Some(run), Some(record))
                                if record.allocation().job_revision_id()
                                    == deployment.revision_id() =>
                            {
                                run.state() != RunState::Running
                                    || run
                                        .readiness()
                                        .is_some_and(|state| state != ReadinessState::Ready)
                            }
                            (Some(run), _) => {
                                run.state().is_terminal()
                                    || matches!(run.state(), RunState::Stopping | RunState::Unknown)
                            }
                            (None, _) => true,
                        }
                    })
                    .map(|attempt| attempt.replica_index)
                    .collect::<BTreeSet<_>>()
            } else {
                BTreeSet::new()
            };
            let mut rollout_unavailable = rollout_wave_slots.len();
            for attempt in attempts {
                let slot_desired = !deployment.suspended()
                    && attempt.replica_index < deployment.desired_replicas();
                if let Some(current) = attempt.current_run {
                    service_run_ids.insert(current.run_id().clone());
                    let Some(run_allocation) = attempt.allocation.as_ref() else {
                        continue;
                    };
                    let run_allocation = run_allocation.allocation();
                    let run_is_desired = slot_desired
                        && attempt.current_allocation_id.as_ref() == Some(run_allocation.id())
                        && run_allocation.job_revision_id() == deployment.revision_id();
                    let worker = workers
                        .iter()
                        .find(|worker| worker.descriptor().worker_id() == current.worker_id())
                        .map(AsRef::as_ref);
                    if !run_is_desired && !current.state().is_terminal() {
                        if current.state() != RunState::Stopping
                            && let Some(rollout) = deployment.rollout()
                            && run_allocation.job_revision_id() != deployment.revision_id()
                            && u32::try_from(rollout_unavailable).unwrap_or(u32::MAX)
                                >= rollout.strategy().max_unavailable()
                        {
                            continue;
                        }
                        let replacement_wait = if slot_desired
                            && attempt.current_allocation_id.as_ref() != Some(run_allocation.id())
                            && statuses.iter().any(|status| {
                                status.worker_id() == current.worker_id() && status.draining()
                            }) {
                            let replacement_is_assigned = attempt
                                .current_allocation_id
                                .as_ref()
                                .and_then(|allocation_id| {
                                    allocations
                                        .iter()
                                        .find(|record| record.allocation().id() == allocation_id)
                                })
                                .is_some_and(|record| {
                                    record.allocation().state() == AllocationState::Assigned
                                });
                            let manager_drain = self
                                .repository
                                .get_worker_drain(current.worker_id())
                                .map_err(|_| ManagerError::StorageUnavailable)?;
                            match manager_drain {
                                Some(drain) if drain.completed_at_ms.is_none() => {
                                    let deadline_elapsed = now_ms()
                                        .map_err(|_| ManagerError::StorageUnavailable)?
                                        >= drain.deadline_at_ms;
                                    !replacement_is_assigned && !deadline_elapsed
                                }
                                _ => false,
                            }
                        } else {
                            false
                        };
                        if replacement_wait {
                            continue;
                        }
                        let Some(worker) = worker else {
                            continue;
                        };
                        if current.state() == RunState::Stopping {
                            // The stop intent is persisted before delivery. A
                            // failed dispatch therefore leaves the run in
                            // Stopping with a pending command that must be
                            // retried during ordinary service reconciliation.
                            if let Ok(pending) = self.repository.pending_commands(
                                current.worker_id(),
                                worker.descriptor().incarnation(),
                                MAX_PENDING_COMMANDS_PER_WORKER,
                            ) && pending.into_iter().any(|envelope| {
                                matches!(
                                    &envelope.command,
                                    WorkerCommand::StopRun(command)
                                        if command.run_id() == current.run_id()
                                )
                            }) && let Ok(stopped) = self.stop_run(current.run_id(), worker)
                                && stopped.state().is_terminal()
                            {
                                self.repository
                                    .finish_service_attempt(
                                        &attempt.service_instance_id,
                                        stopped.run_id(),
                                        attempt.restart_count,
                                        now_ms().map_err(|_| ManagerError::StorageUnavailable)?,
                                        true,
                                    )
                                    .map_err(map_run_error)?;
                                self.repository
                                    .release_service_allocation(run_allocation.id())
                                    .map_err(|_| ManagerError::StorageUnavailable)?;
                                continue;
                            }
                        } else {
                            if let Ok(stopped) = self.stop_run(current.run_id(), worker) {
                                if deployment.rollout().is_some()
                                    && run_allocation.job_revision_id() != deployment.revision_id()
                                {
                                    rollout_unavailable = rollout_unavailable.saturating_add(1);
                                }
                                if stopped.state().is_terminal() {
                                    self.repository
                                        .finish_service_attempt(
                                            &attempt.service_instance_id,
                                            stopped.run_id(),
                                            attempt.restart_count,
                                            now_ms()
                                                .map_err(|_| ManagerError::StorageUnavailable)?,
                                            true,
                                        )
                                        .map_err(map_run_error)?;
                                    self.repository
                                        .release_service_allocation(run_allocation.id())
                                        .map_err(|_| ManagerError::StorageUnavailable)?;
                                }
                            }
                            continue;
                        }
                    }
                    // A persisted terminal observation is durable evidence that
                    // the old process has ended. Consume it without requiring
                    // the worker to still be reachable (including after a
                    // manager restart between recording the result and
                    // updating the service-attempt row).
                    let observed = if current.state().is_terminal() {
                        current.clone()
                    } else {
                        let Some(worker) = worker else {
                            continue;
                        };
                        match worker.inspect(InspectRun::new(current.run_id().clone())) {
                            Ok(Some(observed))
                                if observed.run_id() == current.run_id()
                                    && observed.job_id() == current.job_id()
                                    && observed.worker_id() == current.worker_id()
                                    && observed.revision() > current.revision() =>
                            {
                                observed
                            }
                            Ok(Some(_)) => current.clone(),
                            Ok(None) | Err(_) => continue,
                        }
                    };
                    if observed != current {
                        self.repository
                            .record_observation(&observed)
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                    }
                    if !observed.state().is_terminal() {
                        continue;
                    }
                    if !run_is_desired {
                        self.repository
                            .finish_service_attempt(
                                &attempt.service_instance_id,
                                observed.run_id(),
                                attempt.restart_count,
                                now_ms().map_err(|_| ManagerError::StorageUnavailable)?,
                                true,
                            )
                            .map_err(map_run_error)?;
                        self.repository
                            .release_service_allocation(run_allocation.id())
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                        continue;
                    }
                    let restart_count = attempt.restart_count.saturating_add(1);
                    let terminal = restart_count > MAX_SERVICE_RESTARTS;
                    let delay = service_restart_backoff_ms(restart_count);
                    let next = now_ms()
                        .map_err(|_| ManagerError::StorageUnavailable)?
                        .saturating_add(i64::try_from(delay).unwrap_or(i64::MAX));
                    self.repository
                        .finish_service_attempt(
                            &attempt.service_instance_id,
                            observed.run_id(),
                            restart_count,
                            next,
                            terminal,
                        )
                        .map_err(map_run_error)?;
                    continue;
                }
                if attempt.current_run_id.is_some() {
                    continue;
                }
                if !slot_desired {
                    if let Some(allocation) = attempt.allocation.as_ref()
                        && allocation.allocation().state() == AllocationState::Assigned
                    {
                        self.repository
                            .release_service_allocation(allocation.allocation().id())
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                    }
                    continue;
                }
                let Some(current_allocation_id) = attempt.current_allocation_id.as_ref() else {
                    continue;
                };
                if let Some(previous) = attempt.allocation.as_ref()
                    && previous.allocation().id() != current_allocation_id
                    && previous.allocation().state() == AllocationState::Assigned
                {
                    self.repository
                        .release_service_allocation(previous.allocation().id())
                        .map_err(|_| ManagerError::StorageUnavailable)?;
                }
                let Some(allocation) = allocations
                    .iter()
                    .find(|record| record.allocation().id() == current_allocation_id)
                else {
                    continue;
                };
                if allocation.allocation().state() != AllocationState::Assigned {
                    continue;
                }
                if deployment.rollout().is_some()
                    && !rollout_wave_slots.contains(&attempt.replica_index)
                {
                    continue;
                }
                let Some(worker_id) = allocation.allocation().assigned_worker_id() else {
                    continue;
                };
                let Some(worker) = workers
                    .iter()
                    .find(|worker| worker.descriptor().worker_id() == worker_id)
                else {
                    continue;
                };
                if (attempt.terminal_failure
                    && attempt.current_allocation_id.as_ref()
                        == attempt
                            .allocation
                            .as_ref()
                            .map(|record| record.allocation().id()))
                    || worker.is_draining()
                    || worker.is_degraded()
                {
                    continue;
                }
                let descriptor = worker.descriptor();
                let service_network = statuses
                    .iter()
                    .find(|status| status.worker_id() == descriptor.worker_id())
                    .and_then(WorkerStatus::service_network);
                let admission = match self.repository.begin_service_run(
                    &job_id,
                    allocation.allocation().id(),
                    descriptor.worker_id(),
                    descriptor.incarnation(),
                    descriptor.capacity(),
                    service_network,
                ) {
                    Ok(Some(admission)) => admission,
                    Ok(None)
                    | Err(
                        RunStoreError::CapacityReached
                        | RunStoreError::WorkerDraining
                        | RunStoreError::JobConflict
                        | RunStoreError::PendingDeliveryLimit,
                    ) => {
                        continue;
                    }
                    Err(error) => return Err(map_run_error(error)),
                };
                let envelope = admission.command.ok_or(ManagerError::StorageUnavailable)?;
                match worker.dispatch_command(envelope.clone()) {
                    Ok(observation)
                        if observation.run_id() == admission.spec.id()
                            && observation.job_id() == admission.spec.job().id()
                            && observation.worker_id() == descriptor.worker_id()
                            && (observation.state().is_terminal()
                                || observation.worker_incarnation()
                                    == descriptor.incarnation()) =>
                    {
                        self.repository
                            .record_observation(&observation)
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                        self.acknowledge_command(&envelope)?;
                    }
                    Err(WorkerPortError::CapacityReached) => {
                        self.repository
                            .reject_unaccepted_service_run(
                                &attempt.service_instance_id,
                                admission.spec.id(),
                                now_ms()
                                    .map_err(|_| ManagerError::StorageUnavailable)?
                                    .saturating_add(
                                        i64::try_from(service_restart_backoff_ms(
                                            attempt.restart_count.saturating_add(1),
                                        ))
                                        .unwrap_or(i64::MAX),
                                    ),
                            )
                            .map_err(map_run_error)?;
                    }
                    _ => {}
                }
            }
            // Refresh the durable deployment condition after dispatch and
            // observation reconciliation for this pass.
            if !statuses.is_empty() || deployment.rollout().is_some() {
                self.repository
                    .reconcile_deployment_placement(&job_id, &statuses)
                    .map_err(|_| ManagerError::StorageUnavailable)?;
            }
        }
        for drain in drain_records {
            let Some(worker) = workers
                .iter()
                .find(|worker| worker.descriptor().worker_id() == &drain.worker_id)
            else {
                continue;
            };
            let now = now_ms().map_err(|_| ManagerError::StorageUnavailable)?;
            let active = self
                .repository
                .list_active_runs(&drain.worker_id, MAX_WORKER_CAPACITY)
                .map_err(map_run_error)?;
            if active.is_empty() {
                self.repository
                    .complete_worker_drain(&drain.worker_id, now)
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                continue;
            }
            if now < drain.deadline_at_ms {
                continue;
            }
            for run in active {
                if service_run_ids.contains(run.run_id()) || run.state() == RunState::Unknown {
                    continue;
                }
                if !run.state().is_terminal() {
                    let _ = self.stop_run(run.run_id(), worker.as_ref());
                }
            }
            if self
                .repository
                .list_active_runs(&drain.worker_id, MAX_WORKER_CAPACITY)
                .map_err(map_run_error)?
                .is_empty()
            {
                self.repository
                    .complete_worker_drain(&drain.worker_id, now)
                    .map_err(|_| ManagerError::StorageUnavailable)?;
            }
        }
        Ok(())
    }

    pub fn start_run(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_capacity: usize,
        worker: &dyn WorkerPort,
    ) -> Result<RunObservation, ManagerError> {
        self.start_run_with_key(job_id, worker_id, worker_capacity, None, worker)
    }

    pub fn start_run_with_key(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_capacity: usize,
        idempotency_key: Option<&str>,
        worker: &dyn WorkerPort,
    ) -> Result<RunObservation, ManagerError> {
        if let Some(key) = idempotency_key
            && let Some(observation) = self.replay_run(job_id, key, worker)?
        {
            return Ok(observation);
        }
        if !worker.descriptor().supports_process_execution() {
            return Err(ManagerError::WorkerUnavailable);
        }
        let manager_draining = self
            .repository
            .get_worker_drain(worker_id)
            .map_err(|_| ManagerError::StorageUnavailable)?
            .is_some();
        if worker.is_draining() || manager_draining {
            return Err(ManagerError::WorkerDraining);
        }
        let active = self
            .repository
            .list_active_runs(worker_id, worker_capacity)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        for stored in active {
            if let Ok(Some(newer)) = worker.inspect(InspectRun::new(stored.run_id().clone()))
                && newer.run_id() == stored.run_id()
                && newer.revision() > stored.revision()
            {
                self.repository
                    .record_observation(&newer)
                    .map_err(|_| ManagerError::StorageUnavailable)?;
            }
        }
        let descriptor = worker.descriptor();
        let admission = self
            .repository
            .create_run_for_worker(
                job_id,
                worker_id,
                descriptor.incarnation(),
                worker_capacity,
                idempotency_key,
            )
            .map_err(map_run_error)?;
        if let Some(replay) = admission.replay {
            return self
                .get_run(replay.run_id(), worker)?
                .ok_or(ManagerError::StorageUnavailable);
        }
        let spec = admission.spec;
        let envelope = admission.command.ok_or(ManagerError::StorageUnavailable)?;
        let observation = match worker.dispatch_command(envelope.clone()) {
            Ok(observation) => observation,
            Err(WorkerPortError::CapacityReached) => {
                self.repository
                    .delete_unaccepted_run(spec.id())
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                return Err(ManagerError::CapacityReached);
            }
            Err(WorkerPortError::ShuttingDown) => {
                self.repository
                    .delete_unaccepted_run(spec.id())
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                return Err(ManagerError::WorkerDraining);
            }
            Err(
                WorkerPortError::RunConflict
                | WorkerPortError::Unavailable
                | WorkerPortError::LogsNotFound
                | WorkerPortError::LogsPruned,
            ) => {
                self.mark_dispatch_failure(&spec, RunFailureReason::OutcomeUnknown, false)?;
                return Err(ManagerError::WorkerUnavailable);
            }
        };
        if observation.run_id() != spec.id()
            || observation.job_id() != spec.job().id()
            || observation.worker_id() != spec.worker_id()
        {
            self.mark_dispatch_failure(&spec, RunFailureReason::OutcomeUnknown, false)?;
            return Err(ManagerError::WorkerUnavailable);
        }
        if !observation.state().is_terminal()
            && observation.worker_incarnation() != worker.descriptor().incarnation()
        {
            self.mark_dispatch_failure(&spec, RunFailureReason::OutcomeUnknown, false)?;
            return Err(ManagerError::WorkerUnavailable);
        }
        self.repository
            .record_observation(&observation)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        self.acknowledge_command(&envelope)?;
        Ok(observation)
    }

    pub fn replay_run(
        &self,
        job_id: &JobId,
        idempotency_key: &str,
        worker: &dyn WorkerPort,
    ) -> Result<Option<RunObservation>, ManagerError> {
        let Some((existing_job, observation)) = self
            .repository
            .get_idempotent_run(idempotency_key)
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Ok(None);
        };
        if existing_job != *job_id {
            return Err(ManagerError::IdempotencyConflict);
        }
        let descriptor = worker.descriptor();
        if descriptor.worker_id() == observation.worker_id() {
            let pending = self
                .repository
                .pending_commands(
                    observation.worker_id(),
                    descriptor.incarnation(),
                    MAX_PENDING_COMMANDS_PER_WORKER,
                )
                .map_err(|_| ManagerError::StorageUnavailable)?;
            let retry = pending.into_iter().find(|envelope| {
                matches!(
                    &envelope.command,
                    WorkerCommand::StartRun(command) if command.spec().id() == observation.run_id()
                )
            });
            if let Some(envelope) = retry {
                let next = worker
                    .dispatch_command(envelope.clone())
                    .map_err(|_| ManagerError::WorkerUnavailable)?;
                if next.run_id() != observation.run_id()
                    || next.job_id() != job_id
                    || next.worker_id() != observation.worker_id()
                    || (!next.state().is_terminal()
                        && next.worker_incarnation() != descriptor.incarnation())
                {
                    return Err(ManagerError::WorkerUnavailable);
                }
                self.repository
                    .record_observation(&next)
                    .map_err(|_| ManagerError::StorageUnavailable)?;
                self.acknowledge_command(&envelope)?;
            }
        }
        self.get_run(observation.run_id(), worker)
    }

    pub(super) fn acknowledge_command(
        &self,
        envelope: &CommandEnvelope,
    ) -> Result<(), ManagerError> {
        self.repository
            .acknowledge_command(
                &envelope.command_id,
                &envelope.target_worker_id,
                envelope.target_worker_incarnation,
                r#"{"accepted":true}"#,
            )
            .map_err(|_| ManagerError::StorageUnavailable)
    }

    pub fn get_run(
        &self,
        id: &RunId,
        worker: &dyn WorkerPort,
    ) -> Result<Option<RunObservation>, ManagerError> {
        let Some(stored) = self
            .repository
            .get_run(id)
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Ok(None);
        };
        if let Ok(Some(observation)) = worker.inspect(InspectRun::new(id.clone()))
            && observation.run_id() == id
            && observation.job_id() == stored.job_id()
            && observation.worker_id() == stored.worker_id()
            && observation.revision() > stored.revision()
        {
            if !observation.state().is_terminal()
                && observation.worker_incarnation() != worker.descriptor().incarnation()
            {
                self.mark_unknown(&stored)?;
                return self
                    .repository
                    .get_run(id)
                    .map_err(|_| ManagerError::StorageUnavailable);
            }
            self.repository
                .record_observation(&observation)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            return Ok(Some(observation));
        }
        Ok(Some(stored))
    }

    /// Converge persisted manager reservations with the selected worker before reopening admission.
    pub fn reconcile(&self, worker: &dyn WorkerPort) -> Result<(), ManagerError> {
        let descriptor = worker.descriptor();
        let pending = self
            .repository
            .pending_commands(
                descriptor.worker_id(),
                descriptor.incarnation(),
                MAX_PENDING_COMMANDS_PER_WORKER,
            )
            .map_err(|_| ManagerError::StorageUnavailable)?;
        for envelope in pending {
            let expected_run_id = match &envelope.command {
                WorkerCommand::StartRun(command) => command.spec().id().clone(),
                WorkerCommand::InspectRun(command) => command.run_id().clone(),
                WorkerCommand::StopRun(command) => command.run_id().clone(),
                WorkerCommand::ResolveRun(command) => command.run_id().clone(),
                WorkerCommand::ReadRunLogs(command) => command.run_id().clone(),
            };
            if envelope.target_worker_id != *descriptor.worker_id()
                || envelope.target_worker_incarnation != descriptor.incarnation()
            {
                return Err(ManagerError::StorageUnavailable);
            }
            let observation = worker
                .dispatch_command(envelope.clone())
                .map_err(|_| ManagerError::WorkerUnavailable)?;
            if observation.run_id() != &expected_run_id
                || observation.worker_id() != descriptor.worker_id()
                || (!observation.state().is_terminal()
                    && observation.worker_incarnation() != descriptor.incarnation())
            {
                return Err(ManagerError::WorkerUnavailable);
            }
            let stored = self
                .repository
                .get_run(&expected_run_id)
                .map_err(|_| ManagerError::StorageUnavailable)?
                .ok_or(ManagerError::StorageUnavailable)?;
            if observation.job_id() != stored.job_id() {
                return Err(ManagerError::WorkerUnavailable);
            }
            self.repository
                .record_observation(&observation)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            self.acknowledge_command(&envelope)?;
        }
        let active = self
            .repository
            .list_active_runs(descriptor.worker_id(), MAX_WORKER_CAPACITY)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        for stored in active {
            match worker.inspect(InspectRun::new(stored.run_id().clone())) {
                Ok(Some(observed))
                    if observed.run_id() == stored.run_id()
                        && observed.job_id() == stored.job_id()
                        && observed.worker_id() == stored.worker_id()
                        && observed.revision() > stored.revision() =>
                {
                    if !observed.state().is_terminal()
                        && observed.worker_incarnation() != descriptor.incarnation()
                    {
                        self.mark_unknown(&stored)?;
                    } else {
                        self.repository
                            .record_observation(&observed)
                            .map_err(|_| ManagerError::StorageUnavailable)?;
                    }
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    self.mark_unknown(&stored)?;
                }
                Err(_) => return Err(ManagerError::WorkerUnavailable),
            }
        }
        Ok(())
    }

    pub(super) fn mark_unknown(&self, stored: &RunObservation) -> Result<(), ManagerError> {
        let unknown = stored
            .next_revision(
                RunState::Unknown,
                stored.started_at_ms(),
                None,
                None,
                None,
                Some(RunFailureReason::OutcomeUnknown),
            )
            .ok_or(ManagerError::StorageUnavailable)?;
        self.repository
            .record_observation(&unknown)
            .map_err(|_| ManagerError::StorageUnavailable)
    }

    /// Persist the stop decision before attempting delivery to the worker.
    pub fn stop_run(
        &self,
        id: &RunId,
        worker: &dyn WorkerPort,
    ) -> Result<RunObservation, ManagerError> {
        let descriptor = worker.descriptor();
        let (stopping, envelope) = self
            .repository
            .persist_stop_intent(id, descriptor.incarnation())
            .map_err(map_run_error)?;
        let Some(envelope) = envelope else {
            return Ok(stopping);
        };
        if let Ok(observation) = worker.dispatch_command(envelope.clone())
            && observation.run_id() == id
            && observation.job_id() == stopping.job_id()
            && observation.worker_id() == stopping.worker_id()
        {
            if !observation.state().is_terminal()
                && observation.worker_incarnation() != descriptor.incarnation()
            {
                return Ok(stopping);
            }
            self.acknowledge_command(&envelope)?;
            let observation = if observation.state().is_terminal()
                && observation.revision() <= stopping.revision()
            {
                stopping
                    .next_revision(
                        observation.state(),
                        observation.started_at_ms(),
                        observation.finished_at_ms(),
                        observation.exit_code(),
                        observation.termination_signal(),
                        observation.failure_reason(),
                    )
                    .map(|value| {
                        value.with_optional_termination_reason(observation.termination_reason())
                    })
                    .ok_or(ManagerError::StorageUnavailable)?
            } else if observation.revision() > stopping.revision() {
                observation
            } else {
                return Ok(stopping);
            };
            self.repository
                .record_observation(&observation)
                .map_err(|_| ManagerError::StorageUnavailable)?;
            return Ok(observation);
        }
        Ok(stopping)
    }

    pub fn resolve_unknown_run(
        &self,
        id: &RunId,
        reason: &str,
        worker: &dyn WorkerPort,
    ) -> Result<RunObservation, ManagerError> {
        if reason.trim().is_empty() || reason.len() > 512 {
            return Err(ManagerError::ResolutionConflict);
        }
        let Some(current) = self
            .repository
            .get_run(id)
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Err(ManagerError::RunNotFound);
        };
        if current.state() == RunState::Abandoned {
            return if current.resolution_reason() == Some(reason) {
                Ok(current)
            } else {
                Err(ManagerError::ResolutionConflict)
            };
        }
        if current.state() != RunState::Unknown {
            return Err(ManagerError::ResolutionConflict);
        }
        let envelope = self
            .repository
            .command_for_run(
                id,
                current.worker_id(),
                worker.descriptor().incarnation(),
                WorkerCommand::ResolveRun(ResolveRun::new(id.clone(), reason.to_owned())),
            )
            .map_err(map_command_error)?;
        let resolved = match worker.dispatch_command(envelope.clone()) {
            Ok(resolved) => resolved,
            Err(WorkerPortError::RunConflict) => return Err(ManagerError::ResolutionConflict),
            Err(_) => return Err(ManagerError::WorkerUnavailable),
        };
        if resolved.run_id() != id
            || resolved.state() != RunState::Abandoned
            || resolved.resolution_reason() != Some(reason)
        {
            return Err(ManagerError::WorkerUnavailable);
        }
        self.acknowledge_command(&envelope)?;
        self.repository
            .record_observation(&resolved)
            .map_err(|_| ManagerError::StorageUnavailable)?;
        Ok(resolved)
    }

    pub fn read_logs(
        &self,
        command: ReadRunLogs,
        worker: &dyn WorkerPort,
    ) -> Result<RunLogChunk, ManagerError> {
        match worker.read_logs(command) {
            Ok(chunk) => Ok(chunk),
            Err(WorkerPortError::LogsNotFound) => Err(ManagerError::LogsNotFound),
            Err(WorkerPortError::LogsPruned) => Err(ManagerError::LogsPruned),
            Err(
                WorkerPortError::Unavailable
                | WorkerPortError::ShuttingDown
                | WorkerPortError::CapacityReached
                | WorkerPortError::RunConflict,
            ) => Err(ManagerError::WorkerUnavailable),
        }
    }

    pub(super) fn mark_dispatch_failure(
        &self,
        spec: &RunSpec,
        reason: conductor_core::RunFailureReason,
        rejected: bool,
    ) -> Result<(), ManagerError> {
        let Some(current) = self
            .repository
            .get_run(spec.id())
            .map_err(|_| ManagerError::StorageUnavailable)?
        else {
            return Err(ManagerError::StorageUnavailable);
        };
        let finished = now_ms().map_err(|_| ManagerError::StorageUnavailable)?;
        let next = current
            .next_revision(
                if rejected {
                    RunState::Failed
                } else {
                    RunState::Unknown
                },
                None,
                if rejected { Some(finished) } else { None },
                None,
                None,
                Some(reason),
            )
            .ok_or(ManagerError::StorageUnavailable)?;
        self.repository
            .record_observation(&next)
            .map_err(|_| ManagerError::StorageUnavailable)
    }

    pub fn list_runs(
        &self,
        job_id: Option<&JobId>,
        state: Option<RunState>,
        limit: usize,
    ) -> Result<Vec<RunObservation>, ManagerError> {
        self.repository
            .list_runs(job_id, state, limit.min(MAX_RUNS_RESPONSE))
            .map_err(|_| ManagerError::StorageUnavailable)
    }

    pub fn list_runs_page(
        &self,
        job_id: Option<&JobId>,
        state: Option<RunState>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<RunObservation>, QueryError> {
        self.repository.list_runs_page(job_id, state, limit, cursor)
    }
}
