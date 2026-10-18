use super::*;

pub(super) struct WorkerSession {
    pub(super) descriptor: WorkerDescriptor,
    pub(super) metadata: WorkerMetadata,
    pub(super) incarnation: AtomicU64,
    pub(super) protocol_offer: ProtocolOffer,
    pub(super) journal: Option<Arc<dyn CommandJournal>>,
    pub(super) inventory_sync: Mutex<Option<WorkerInventorySync>>,
    pub(super) status: Mutex<WorkerStatus>,
    pub(super) pending: Mutex<BTreeMap<CommandId, PendingCommand>>,
    pub(super) restored_incarnation: AtomicU64,
    pub(super) last_report_sequence: Mutex<BTreeMap<u64, u64>>,
    pub(super) recent_reports: Mutex<VecDeque<WorkerReport>>,
    pub(super) changed: Notify,
    pub(super) poll_gate: Semaphore,
    pub(super) last_seen: Mutex<Instant>,
    pub(super) connected: AtomicBool,
    pub(super) next_command: AtomicU64,
}

impl WorkerSession {
    #[cfg(test)]
    pub(super) fn new(
        descriptor: WorkerDescriptor,
        protocol_offer: ProtocolOffer,
        journal: Option<Arc<dyn CommandJournal>>,
    ) -> Self {
        let metadata = WorkerMetadata::automatic(descriptor.worker_id().clone());
        Self::new_with_metadata(descriptor, protocol_offer, journal, metadata)
    }

    pub(super) fn new_with_metadata(
        descriptor: WorkerDescriptor,
        protocol_offer: ProtocolOffer,
        journal: Option<Arc<dyn CommandJournal>>,
        metadata: WorkerMetadata,
    ) -> Self {
        let incarnation = descriptor.incarnation();
        Self {
            status: Mutex::new(WorkerStatus::new(descriptor.clone(), 0, 0, false, false, 0)),
            descriptor,
            metadata,
            incarnation: AtomicU64::new(incarnation),
            protocol_offer,
            journal,
            inventory_sync: Mutex::new(None),
            pending: Mutex::new(BTreeMap::new()),
            restored_incarnation: AtomicU64::new(0),
            last_report_sequence: Mutex::new(BTreeMap::new()),
            recent_reports: Mutex::new(VecDeque::new()),
            changed: Notify::new(),
            poll_gate: Semaphore::new(1),
            last_seen: Mutex::new(Instant::now()),
            connected: AtomicBool::new(false),
            next_command: AtomicU64::new(0),
        }
    }

    pub(super) fn command_id(&self) -> Result<CommandId, TransportError> {
        let sequence = self
            .next_command
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| TransportError::Unavailable)?;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TransportError::Unavailable)?
            .as_nanos();
        let value = format!("cmd-{}-{nanos}-{sequence}", std::process::id());
        CommandId::new(value).map_err(|_| TransportError::Unavailable)
    }

    pub(super) fn descriptor(&self) -> WorkerDescriptor {
        self.descriptor
            .clone()
            .with_incarnation(self.incarnation.load(Ordering::Acquire))
    }

    pub(super) fn enqueue(
        &self,
        command: WorkerCommand,
    ) -> Result<mpsc::Receiver<WorkerResult>, TransportError> {
        let command_id = self.command_id()?;
        let envelope = CommandEnvelope {
            command_id: command_id.clone(),
            target_worker_id: self.descriptor.worker_id().clone(),
            target_worker_incarnation: self.incarnation.load(Ordering::Acquire),
            command,
        };
        self.enqueue_envelope(envelope)
    }

    pub(super) fn enqueue_envelope(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<mpsc::Receiver<WorkerResult>, TransportError> {
        if envelope.target_worker_id != *self.descriptor.worker_id()
            || envelope.target_worker_incarnation != self.incarnation.load(Ordering::Acquire)
        {
            return Err(TransportError::WorkerConflict);
        }
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| TransportError::Unavailable)?;
        if pending.len() >= MAX_PENDING_COMMANDS {
            return Err(TransportError::QueueFull);
        }
        if let Some(existing) = pending.get(&envelope.command_id) {
            if existing.envelope != envelope {
                return Err(TransportError::WorkerConflict);
            }
            if existing.responses.len() >= MAX_PENDING_COMMANDS {
                return Err(TransportError::QueueFull);
            }
            pending
                .get_mut(&envelope.command_id)
                .ok_or(TransportError::Unavailable)?
                .responses
                .push(sender);
            return Ok(receiver);
        }
        pending.insert(
            envelope.command_id.clone(),
            PendingCommand {
                envelope,
                responses: vec![sender],
                retry_at: Instant::now(),
            },
        );
        drop(pending);
        self.changed.notify_one();
        Ok(receiver)
    }

    pub(super) async fn poll(
        &self,
        request: WorkerPollRequest,
    ) -> Result<WorkerPollResponse, StatusCode> {
        let current_incarnation = self.incarnation.load(Ordering::Acquire);
        let request_incarnation = request.worker_incarnation;
        if request_incarnation < current_incarnation {
            return Err(StatusCode::CONFLICT);
        }
        if request.status.worker_id() != self.descriptor.worker_id()
            || request.status.incarnation() != request_incarnation
            || request.status.capacity() != self.descriptor.capacity()
            || request.status.supports_process_execution()
                != self.descriptor.supports_process_execution()
        {
            return Err(StatusCode::CONFLICT);
        }
        if request.status.inventory().is_none()
            || !request.status.is_valid()
            || request.status.resource_domain_id().is_some()
            || request.status.resource_domain_capacity().is_some()
            || !request.status.labels().is_empty()
            || request.status.fresh()
            || request.status.last_seen_at_ms().is_some()
        {
            return Err(StatusCode::CONFLICT);
        }
        if request_incarnation == current_incarnation {
            let current = self
                .status
                .lock()
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            if current
                .inventory()
                .is_some_and(|inventory| Some(inventory) != request.status.inventory())
            {
                return Err(StatusCode::CONFLICT);
            }
        }
        let negotiated = self
            .protocol_offer
            .negotiate(&request.protocol_offer)
            .map_err(|_| StatusCode::UPGRADE_REQUIRED)?;
        let _poll = self
            .poll_gate
            .try_acquire()
            .map_err(|_| StatusCode::TOO_MANY_REQUESTS)?;
        if request_incarnation > current_incarnation {
            self.adopt_incarnation(request_incarnation)?;
        }
        self.restore_pending(request_incarnation)?;
        let inventory_acknowledgement = self.reconcile_inventory(&request.inventory_page)?;
        let worker_allocatable = request
            .status
            .inventory()
            .map(|inventory| inventory.allocatable())
            .ok_or(StatusCode::CONFLICT)?;
        let domain_capacity = self.metadata.capacity().unwrap_or(worker_allocatable);
        let mut status = request.status;
        let last_seen_at_ms = manager_time_ms().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
        status = status
            .with_manager_metadata(
                self.metadata.resource_domain_id().clone(),
                domain_capacity,
                self.metadata.labels().clone(),
                last_seen_at_ms,
                true,
            )
            .and_then(|status| match self.metadata.service_network() {
                Some(network) => status.with_service_network(network.clone()),
                None => Some(status),
            })
            .ok_or(StatusCode::CONFLICT)?;
        *self
            .status
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)? = status;
        self.mark_seen()?;

        let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
        let command = loop {
            let notified = self.changed.notified();
            let (ready, next_retry) = self.next_poll_command(request_incarnation)?;
            if let Some(command) = ready {
                break Some(command);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                break None;
            }
            let wake_at = next_retry
                .map(tokio::time::Instant::from_std)
                .unwrap_or(deadline)
                .min(deadline);
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep_until(wake_at) => {},
            }
        };

        let mut capabilities = negotiated
            .capabilities()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        capabilities.sort_unstable();
        Ok(WorkerPollResponse {
            protocol_version: negotiated.version(),
            capabilities,
            inventory_acknowledgement,
            command,
        })
    }

    pub(super) fn reconcile_inventory(
        &self,
        page: &WorkerInventoryPage,
    ) -> Result<WorkerInventoryAcknowledgement, StatusCode> {
        if page.worker_id != *self.descriptor.worker_id()
            || page.worker_incarnation != self.incarnation.load(Ordering::Acquire)
            || !page.is_valid()
        {
            return Err(StatusCode::CONFLICT);
        }
        let Some(journal) = &self.journal else {
            return Ok(WorkerInventoryAcknowledgement {
                snapshot_generation: page.snapshot_generation,
                next_after_sequence: page.next_after_sequence,
                restart_scan: false,
            });
        };
        let mut sync = self
            .inventory_sync
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if page.after_sequence == 0 {
            *sync = Some(WorkerInventorySync {
                worker_incarnation: page.worker_incarnation,
                snapshot_generation: page.snapshot_generation,
                expected_after_sequence: 0,
                observed_manager_runs: BTreeSet::new(),
            });
        }
        let Some(current) = sync.as_mut() else {
            return Ok(WorkerInventoryAcknowledgement {
                snapshot_generation: page.snapshot_generation,
                next_after_sequence: None,
                restart_scan: true,
            });
        };
        if current.worker_incarnation != page.worker_incarnation
            || current.snapshot_generation != page.snapshot_generation
            || current.expected_after_sequence != page.after_sequence
        {
            *sync = None;
            return Ok(WorkerInventoryAcknowledgement {
                snapshot_generation: page.snapshot_generation,
                next_after_sequence: None,
                restart_scan: true,
            });
        }
        let seen = journal
            .reconcile_worker_inventory_page(
                &page.worker_id,
                page.worker_incarnation,
                &page.entries,
            )
            .map_err(|error| match error {
                CommandStoreError::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE,
                CommandStoreError::NotFound
                | CommandStoreError::Conflict
                | CommandStoreError::PendingLimit => StatusCode::CONFLICT,
            })?;
        current.observed_manager_runs.extend(seen);
        if current.observed_manager_runs.len() > MAX_WORKER_CAPACITY {
            *sync = None;
            return Err(StatusCode::CONFLICT);
        }
        if let Some(next_after_sequence) = page.next_after_sequence {
            current.expected_after_sequence = next_after_sequence;
        } else {
            journal
                .finish_worker_inventory(
                    &page.worker_id,
                    page.worker_incarnation,
                    &current
                        .observed_manager_runs
                        .iter()
                        .cloned()
                        .collect::<Vec<_>>(),
                )
                .map_err(|error| match error {
                    CommandStoreError::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE,
                    CommandStoreError::NotFound
                    | CommandStoreError::Conflict
                    | CommandStoreError::PendingLimit => StatusCode::CONFLICT,
                })?;
            *sync = None;
        }
        Ok(WorkerInventoryAcknowledgement {
            snapshot_generation: page.snapshot_generation,
            next_after_sequence: page.next_after_sequence,
            restart_scan: false,
        })
    }

    pub(super) fn adopt_incarnation(&self, incarnation: u64) -> Result<(), StatusCode> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let stale = pending
            .iter()
            .filter(|(_, item)| item.envelope.target_worker_incarnation != incarnation)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for command_id in stale {
            if let Some(item) = pending.remove(&command_id) {
                for response in item.responses {
                    let _ = response.send(WorkerResult::Error(RemoteWorkerError::Unavailable));
                }
            }
        }
        self.incarnation.store(incarnation, Ordering::Release);
        *self
            .inventory_sync
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)? = None;
        Ok(())
    }

    pub(super) fn restore_pending(&self, incarnation: u64) -> Result<(), StatusCode> {
        let Some(journal) = &self.journal else {
            return Ok(());
        };
        if self.restored_incarnation.load(Ordering::Acquire) == incarnation {
            return Ok(());
        }
        let commands = journal
            .pending_commands(
                self.descriptor.worker_id(),
                incarnation,
                MAX_PENDING_COMMANDS,
            )
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        for envelope in commands {
            if pending.contains_key(&envelope.command_id) {
                continue;
            }
            if pending.len() >= MAX_PENDING_COMMANDS {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            let (response, receiver): (SyncSender<WorkerResult>, Receiver<WorkerResult>) =
                mpsc::sync_channel(1);
            drop(receiver);
            pending.insert(
                envelope.command_id.clone(),
                PendingCommand {
                    envelope,
                    responses: vec![response],
                    retry_at: Instant::now(),
                },
            );
        }
        self.restored_incarnation
            .store(incarnation, Ordering::Release);
        Ok(())
    }

    pub(super) fn next_poll_command(
        &self,
        incarnation: u64,
    ) -> Result<(Option<CommandEnvelope>, Option<Instant>), StatusCode> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let now = Instant::now();
        if let Some((_, item)) = pending.iter_mut().find(|(_, item)| {
            item.envelope.target_worker_incarnation == incarnation && item.retry_at <= now
        }) {
            item.retry_at = now + COMMAND_REDELIVERY_INTERVAL;
            Ok((Some(item.envelope.clone()), None))
        } else {
            Ok((
                None,
                pending
                    .values()
                    .filter(|item| item.envelope.target_worker_incarnation == incarnation)
                    .map(|item| item.retry_at)
                    .min(),
            ))
        }
    }

    pub(super) fn mark_seen(&self) -> Result<(), StatusCode> {
        let mut last_seen = self
            .last_seen
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        *last_seen = Instant::now();
        self.connected.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn report(&self, report: WorkerReport) -> Result<ReportAcknowledgement, StatusCode> {
        let mut recent = self
            .recent_reports
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if let Some(previous) = recent.iter().find(|previous| {
            previous.worker_incarnation == report.worker_incarnation
                && previous.report_sequence == report.report_sequence
        }) {
            return if previous == &report {
                Ok(ReportAcknowledgement {
                    report_sequence: report.report_sequence,
                    duplicate: true,
                })
            } else {
                Err(StatusCode::CONFLICT)
            };
        }
        let mut last_sequences = self
            .last_report_sequence
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let last_sequence = last_sequences
            .get(&report.worker_incarnation)
            .copied()
            .unwrap_or_default();
        let first_report = last_sequence == 0;
        if report.report_sequence == 0
            || (!first_report && report.report_sequence != last_sequence.saturating_add(1))
        {
            return Err(StatusCode::CONFLICT);
        }
        let acknowledgement_json =
            serde_json::to_string(&report).map_err(|_| StatusCode::CONFLICT)?;
        let durable_record = if let Some(journal) = &self.journal {
            journal
                .command_record(&report.command_id)
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        } else {
            None
        };
        if let Some(record) = &durable_record {
            if record.envelope.target_worker_id != report.worker_id
                || record.envelope.target_worker_incarnation != report.worker_incarnation
                || !result_matches(&record.envelope.command, &report.result)
            {
                return Err(StatusCode::CONFLICT);
            }
            if let Some(previous) = &record.acknowledgement_json {
                if previous != &acknowledgement_json {
                    return Err(StatusCode::CONFLICT);
                }
                self.mark_seen()?;
                return Ok(ReportAcknowledgement {
                    report_sequence: report.report_sequence,
                    duplicate: true,
                });
            }
        }
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        let item = pending.get(&report.command_id);
        if item.is_none() && durable_record.is_none() {
            return Err(StatusCode::CONFLICT);
        }
        if let Some(item) = item
            && (item.envelope.target_worker_id != report.worker_id
                || item.envelope.target_worker_incarnation != report.worker_incarnation
                || !result_matches(&item.envelope.command, &report.result))
        {
            return Err(StatusCode::CONFLICT);
        }
        if let Some(journal) = &self.journal
            && durable_record.is_some()
        {
            journal
                .acknowledge_command(
                    &report.command_id,
                    &report.worker_id,
                    report.worker_incarnation,
                    &acknowledgement_json,
                )
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        }
        let Some(item) = pending.remove(&report.command_id) else {
            last_sequences.insert(report.worker_incarnation, report.report_sequence);
            recent.push_back(report.clone());
            while recent.len() > MAX_RECENT_REPORTS {
                recent.pop_front();
            }
            self.mark_seen()?;
            return Ok(ReportAcknowledgement {
                report_sequence: report.report_sequence,
                duplicate: false,
            });
        };
        for response in item.responses {
            let _ = response.send(report.result.clone());
        }
        last_sequences.insert(report.worker_incarnation, report.report_sequence);
        recent.push_back(report.clone());
        while recent.len() > MAX_RECENT_REPORTS {
            recent.pop_front();
        }
        self.mark_seen()?;
        self.changed.notify_waiters();
        Ok(ReportAcknowledgement {
            report_sequence: report.report_sequence,
            duplicate: false,
        })
    }

    pub(super) fn is_fresh(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
            && self
                .last_seen
                .lock()
                .is_ok_and(|last_seen| last_seen.elapsed() <= WORKER_FRESHNESS_TIMEOUT)
    }

    pub(super) fn status(&self) -> Result<WorkerStatus, WorkerPortError> {
        self.status
            .lock()
            .map(|status| status.clone())
            .map_err(|_| WorkerPortError::Unavailable)
    }
}
