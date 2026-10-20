use super::*;

/// Manager-owned SQLite repository. One serialized connection keeps access bounded and simple.
pub struct SqliteJobRepository {
    pub(super) connection: Mutex<Connection>,
    completed_run_retention: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryPruneProgress {
    pub eligible_runs: usize,
    pub removed_runs: usize,
    pub more_eligible: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryRetentionStatus {
    pub completed_runs: u64,
    pub active_or_unresolved_runs: u64,
    pub eligible_runs: u64,
    pub idempotency_protected_runs: u64,
    pub pending_commands: u64,
    pub current_service_attempts: u64,
    pub service_port_leases: u64,
    pub manager_commands: u64,
    pub audit_events: u64,
    pub execution_idempotency_records: u64,
    pub eligible_runs_to_prune: u64,
    pub sqlite_page_count: u64,
    pub sqlite_freelist_pages: u64,
    pub sqlite_page_size_bytes: u64,
    pub completed_run_retention: usize,
    pub cleanup_batch_limit: usize,
}

impl CommandJournal for SqliteJobRepository {
    fn command_record(
        &self,
        command_id: &CommandId,
    ) -> Result<Option<CommandRecord>, CommandStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        connection
            .query_row(
                "SELECT envelope_json, acknowledgement_json FROM manager_commands WHERE command_id = ?1",
                [command_id.as_str()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()
            .map_err(|_| CommandStoreError::StorageUnavailable)?
            .map(|(envelope, acknowledgement_json)| {
                Ok(CommandRecord {
                    envelope: serde_json::from_str(&envelope)
                        .map_err(|_| CommandStoreError::StorageUnavailable)?,
                    acknowledgement_json,
                })
            })
            .transpose()
    }

    fn pending_commands(
        &self,
        worker_id: &WorkerId,
        incarnation: u64,
        limit: usize,
    ) -> Result<Vec<CommandEnvelope>, CommandStoreError> {
        if !(1..=MAX_PENDING_COMMANDS_PER_WORKER).contains(&limit) {
            return Err(CommandStoreError::PendingLimit);
        }
        let incarnation =
            i64::try_from(incarnation).map_err(|_| CommandStoreError::StorageUnavailable)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT envelope_json FROM manager_commands
                 WHERE worker_id = ?1 AND worker_incarnation = ?2 AND state = 'pending'
                 ORDER BY rowid LIMIT ?3",
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map(
                params![worker_id.as_str(), incarnation, limit as i64],
                |row| row.get::<_, String>(0),
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        rows.map(|row| {
            let json = row.map_err(|_| CommandStoreError::StorageUnavailable)?;
            serde_json::from_str(&json).map_err(|_| CommandStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn acknowledge_command(
        &self,
        command_id: &CommandId,
        worker_id: &WorkerId,
        incarnation: u64,
        acknowledgement_json: &str,
    ) -> Result<(), CommandStoreError> {
        if acknowledgement_json.len() > 128 * 1024
            || serde_json::from_str::<serde_json::Value>(acknowledgement_json).is_err()
        {
            return Err(CommandStoreError::Conflict);
        }
        let incarnation = i64::try_from(incarnation).map_err(|_| CommandStoreError::Conflict)?;
        let acknowledged_at_ms = now_ms().map_err(|_| CommandStoreError::StorageUnavailable)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let record = transaction
            .query_row(
                "SELECT worker_id, worker_incarnation, state, acknowledgement_json
                 FROM manager_commands WHERE command_id = ?1",
                [command_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| CommandStoreError::StorageUnavailable)?
            .ok_or(CommandStoreError::NotFound)?;
        if record.0 != worker_id.as_str() || record.1 != incarnation {
            return Err(CommandStoreError::Conflict);
        }
        if record.2 == "acknowledged" {
            transaction
                .commit()
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            return if record.3.as_deref() == Some(acknowledgement_json)
                || acknowledgement_json == r#"{"accepted":true}"# {
                Ok(())
            } else {
                Err(CommandStoreError::Conflict)
            };
        }
        transaction
            .execute(
                "UPDATE manager_commands
                 SET state = 'acknowledged', acknowledgement_json = ?1, acknowledged_at_ms = ?2
                 WHERE command_id = ?3 AND state = 'pending'",
                params![
                    acknowledgement_json,
                    acknowledged_at_ms,
                    command_id.as_str()
                ],
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| CommandStoreError::StorageUnavailable)
    }

    fn reconcile_worker_inventory_page(
        &self,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        entries: &[WorkerInventoryEntry],
    ) -> Result<Vec<RunId>, CommandStoreError> {
        let worker_incarnation =
            i64::try_from(worker_incarnation).map_err(|_| CommandStoreError::Conflict)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let mut observed_ids = Vec::with_capacity(entries.len());
        let mut terminal_observation_updated = false;
        for entry in entries {
            let incoming = &entry.observation;
            if incoming.worker_id() != worker_id
                || incoming.worker_incarnation() > worker_incarnation as u64
            {
                return Err(CommandStoreError::Conflict);
            }
            let stored_json = transaction
                .query_row(
                    "SELECT observation_json FROM runs WHERE run_id = ?1 AND worker_id = ?2",
                    params![incoming.run_id().as_str(), worker_id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            let Some(stored_json) = stored_json else {
                // A worker may retain an orphan after manager state was restored
                // from an older backup. Keep it on the worker; never adopt or relaunch it.
                continue;
            };
            let stored: RunObservation = serde_json::from_str(&stored_json)
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            if incoming.job_id() != stored.job_id() || incoming.worker_id() != stored.worker_id() {
                return Err(CommandStoreError::Conflict);
            }
            // Only active assignments need to be retained across pages so the
            // completed-run history can grow without making inventory scans
            // exceed the worker's live-capacity bound.
            if stored.state().is_terminal() {
                continue;
            }
            observed_ids.push(incoming.run_id().clone());

            if incoming.revision() < stored.revision() {
                continue;
            }
            if incoming.revision() == stored.revision() {
                if incoming != &stored {
                    return Err(CommandStoreError::Conflict);
                }
                continue;
            }
            if stored.worker_incarnation() != 0
                && incoming.worker_incarnation() != stored.worker_incarnation()
            {
                if incoming.worker_incarnation() < stored.worker_incarnation() {
                    continue;
                }
                return Err(CommandStoreError::Conflict);
            }
            let mut newer = stored.clone();
            if !newer.apply_newer(incoming.clone()) {
                return Err(CommandStoreError::Conflict);
            }
            let serialized =
                serde_json::to_string(&newer).map_err(|_| CommandStoreError::StorageUnavailable)?;
            let changed = transaction
                .execute(
                    "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3
                     WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
                    params![serialized, newer.revision() as i64, newer.state().as_str(), newer.run_id().as_str()],
                )
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            terminal_observation_updated |= changed == 1 && newer.state().is_terminal();
        }
        transaction
            .commit()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        drop(connection);
        if terminal_observation_updated && self.prune_completed_run_history().is_err() {
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"warning\",\"component\":\"manager\",\"event\":\"run_history_retention_failed\"}}",
                now_ms().unwrap_or_default(),
            );
        }
        Ok(observed_ids)
    }

    fn finish_worker_inventory(
        &self,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        observed_run_ids: &[RunId],
    ) -> Result<(), CommandStoreError> {
        let incarnation =
            i64::try_from(worker_incarnation).map_err(|_| CommandStoreError::Conflict)?;
        let observed = observed_run_ids.iter().collect::<BTreeSet<_>>();
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let mut statement = transaction
            .prepare(
                "SELECT run_id, observation_json FROM runs
                 WHERE worker_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([worker_id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let runs = rows
            .map(|row| {
                let (id, json) = row.map_err(|_| CommandStoreError::StorageUnavailable)?;
                let id = RunId::new(id).map_err(|_| CommandStoreError::StorageUnavailable)?;
                let observation: RunObservation = serde_json::from_str(&json)
                    .map_err(|_| CommandStoreError::StorageUnavailable)?;
                Ok((id, observation))
            })
            .collect::<Result<Vec<_>, CommandStoreError>>()?;
        drop(statement);

        for (run_id, stored) in runs {
            if observed.contains(&run_id) || stored.state() == RunState::Unknown {
                continue;
            }
            if stored.state() == RunState::Requested
                && has_pending_start_command(&transaction, &run_id, worker_id, incarnation)?
            {
                continue;
            }
            let unknown = stored
                .next_revision(
                    RunState::Unknown,
                    stored.started_at_ms(),
                    None,
                    None,
                    None,
                    Some(RunFailureReason::OutcomeUnknown),
                )
                .ok_or(CommandStoreError::StorageUnavailable)?;
            let serialized = serde_json::to_string(&unknown)
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            transaction
                .execute(
                    "UPDATE runs SET observation_json = ?1, revision = ?2, state = 'unknown'
                     WHERE run_id = ?3 AND revision = ?4 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
                    params![serialized, unknown.revision() as i64, run_id.as_str(), stored.revision() as i64],
                )
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
        }
        transaction
            .commit()
            .map_err(|_| CommandStoreError::StorageUnavailable)
    }
}

const MAX_OVERLAY_PEERS_PER_DOMAIN: usize = 128;
const MAX_OVERLAY_CANDIDATE_LEASE_MS: i64 = 5 * 60 * 1_000;
const MAX_OVERLAY_LEASE_MS: i64 = 5 * 60 * 1_000;
const OVERLAY_CLOCK_SKEW_MS: i64 = 30_000;

fn parse_overlay_pool(value: &str) -> Result<ipnet::Ipv4Net, OverlayNetworkError> {
    let pool = value
        .parse::<ipnet::Ipv4Net>()
        .map_err(|_| OverlayNetworkError::InvalidAddressPool)?;
    let address = pool.addr();
    let private = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
        .iter()
        .filter_map(|range| range.parse::<ipnet::Ipv4Net>().ok())
        .any(|range| range.contains(&address) && range.prefix_len() <= pool.prefix_len());
    if !private || pool.network() != address || !(16..=30).contains(&pool.prefix_len()) {
        return Err(OverlayNetworkError::InvalidAddressPool);
    }
    Ok(pool)
}

fn next_overlay_generation(current: i64) -> Result<i64, OverlayNetworkError> {
    current
        .checked_add(1)
        .filter(|generation| *generation > 0)
        .ok_or(OverlayNetworkError::StorageUnavailable)
}

fn overlay_pools_overlap(left: &ipnet::Ipv4Net, right: &ipnet::Ipv4Net) -> bool {
    u32::from(left.network()) <= u32::from(right.broadcast())
        && u32::from(right.network()) <= u32::from(left.broadcast())
}

fn overlay_address(value: i64) -> Result<std::net::Ipv4Addr, OverlayNetworkError> {
    let value = u32::try_from(value).map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    Ok(std::net::Ipv4Addr::from(value))
}

struct OverlayPeerAssignmentRow {
    network_domain_id: String,
    worker_id: String,
    endpoint_id: String,
    worker_address: i64,
    peer_generation: i64,
    network_generation: i64,
    expires_at_ms: i64,
    state: String,
    quarantine_until_ms: Option<i64>,
}

fn overlay_peer_assignment(
    row: OverlayPeerAssignmentRow,
) -> Result<conductor_core::OverlayPeerAssignment, OverlayNetworkError> {
    use conductor_core::{NetworkDomainId, OverlayEndpointId, OverlayPeerState, WorkerId};

    Ok(conductor_core::OverlayPeerAssignment {
        network_domain_id: NetworkDomainId::new(row.network_domain_id)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
        worker_id: WorkerId::new(row.worker_id)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
        endpoint_id: OverlayEndpointId::new(row.endpoint_id)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
        worker_address: overlay_address(row.worker_address)?,
        peer_generation: u64::try_from(row.peer_generation)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
        network_generation: u64::try_from(row.network_generation)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
        expires_at_ms: row.expires_at_ms,
        state: match row.state.as_str() {
            "active" => OverlayPeerState::Active,
            "quarantined" => OverlayPeerState::Quarantined,
            _ => return Err(OverlayNetworkError::StorageUnavailable),
        },
        quarantine_until_ms: row.quarantine_until_ms,
    })
}

fn find_overlay_address(
    transaction: &rusqlite::Transaction<'_>,
    pool: ipnet::Ipv4Net,
) -> Result<i64, OverlayNetworkError> {
    let mut used = BTreeSet::new();
    let mut statement = transaction
        .prepare("SELECT worker_address FROM overlay_peers")
        .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    let rows = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    for address in rows {
        used.insert(address.map_err(|_| OverlayNetworkError::StorageUnavailable)?);
    }
    drop(statement);

    let first = u32::from(pool.network()) + 1;
    let last = u32::from(pool.broadcast()) - 1;
    for address in first..=last {
        let candidate = i64::from(address);
        if !used.contains(&candidate) {
            return Ok(candidate);
        }
    }
    Err(OverlayNetworkError::AddressPoolExhausted)
}

fn expire_overlay_peers(
    transaction: &rusqlite::Transaction<'_>,
    network_domain_id: &conductor_core::NetworkDomainId,
    now_ms: i64,
) -> Result<(), OverlayNetworkError> {
    let mut statement = transaction
        .prepare(
            "SELECT worker_id, endpoint_id, peer_generation, expires_at_ms FROM overlay_peers
             WHERE network_domain_id = ?1 AND state = 'active' AND expires_at_ms <= ?2",
        )
        .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    let rows = statement
        .query_map(params![network_domain_id.as_str(), now_ms], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    let expired = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    drop(statement);
    if expired.is_empty() {
        return Ok(());
    }
    for (worker_id, endpoint_id, peer_generation, expires_at_ms) in &expired {
        transaction
            .execute(
                "INSERT OR IGNORE INTO overlay_endpoint_history
                 (network_domain_id, endpoint_id, worker_id, peer_generation, retired_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    network_domain_id.as_str(),
                    endpoint_id,
                    worker_id,
                    peer_generation,
                    expires_at_ms
                ],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE overlay_peers SET state = 'quarantined', quarantine_until_ms = ?1
                 WHERE network_domain_id = ?2 AND worker_id = ?3 AND state = 'active'",
                params![
                    expires_at_ms.saturating_add(OVERLAY_CLOCK_SKEW_MS),
                    network_domain_id.as_str(),
                    worker_id
                ],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM overlay_peer_observations WHERE network_domain_id = ?1 AND worker_id = ?2",
                params![network_domain_id.as_str(), worker_id],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    }
    let current: Option<i64> = transaction
        .query_row(
            "SELECT network_generation FROM overlay_network_domains WHERE network_domain_id = ?1",
            [network_domain_id.as_str()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    let current = current.ok_or(OverlayNetworkError::DomainNotFound)?;
    transaction
        .execute(
            "UPDATE overlay_network_domains SET network_generation = ?1 WHERE network_domain_id = ?2",
            params![next_overlay_generation(current)?, network_domain_id.as_str()],
        )
        .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
    Ok(())
}

impl JobRepository for SqliteJobRepository {
    fn record_audit_event(&self, event: &AuditEventInput) -> Result<(), ExecutionStoreError> {
        if !matches!(
            event.principal.as_str(),
            "operator" | "local_operator" | "worker_enrollment"
        ) || event.action.is_empty()
            || event.action.len() > 64
            || !event
                .action
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'.')
            || !matches!(
                event.resource_type.as_str(),
                "job" | "deployment" | "worker" | "run"
            )
            || event.resource_id.as_deref().is_some_and(|id| {
                id.is_empty()
                    || id.len() > 128
                    || !id.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
                    })
            })
            || !matches!(event.outcome.as_str(), "success" | "failure")
            || !(100..=599).contains(&event.status_code)
            || event.occurred_at_ms < 0
        {
            return Err(ExecutionStoreError::InvalidRequest);
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        connection
            .execute(
                "INSERT INTO audit_events(occurred_at_ms, principal, action, resource_type, resource_id, outcome, status_code)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![event.occurred_at_ms, event.principal, event.action, event.resource_type, event.resource_id, event.outcome, event.status_code],
            )
            .map(|_| ())
            .map_err(|_| ExecutionStoreError::StorageUnavailable)
    }

    fn list_audit_events(&self, limit: usize) -> Result<Vec<AuditEvent>, ExecutionStoreError> {
        if !(1..=1_000).contains(&limit) {
            return Err(ExecutionStoreError::InvalidRequest);
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT event_sequence, occurred_at_ms, principal, action, resource_type, resource_id, outcome, status_code
                 FROM audit_events ORDER BY event_sequence DESC LIMIT ?1",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([limit as i64], |row| {
                Ok(AuditEvent {
                    sequence: row.get::<_, i64>(0)? as u64,
                    occurred_at_ms: row.get(1)?,
                    principal: row.get(2)?,
                    action: row.get(3)?,
                    resource_type: row.get(4)?,
                    resource_id: row.get(5)?,
                    outcome: row.get(6)?,
                    status_code: row.get(7)?,
                })
            })
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)
    }

    fn operational_metrics(&self) -> Result<OperationalMetrics, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut metrics = OperationalMetrics::default();
        let mut statement = connection
            .prepare(
                "SELECT deployment_json FROM job_deployments WHERE deleted_at_ms IS NULL LIMIT ?1",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let deployments = statement
            .query_map([MAX_JOBS as i64], |row| row.get::<_, String>(0))
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        for deployment in deployments {
            let deployment: JobDeployment = serde_json::from_str(
                &deployment.map_err(|_| ExecutionStoreError::StorageUnavailable)?,
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            match deployment.status() {
                DeploymentStatus::Pending => metrics.deployments_pending += 1,
                DeploymentStatus::Ready => metrics.deployments_ready += 1,
                DeploymentStatus::Suspended => metrics.deployments_suspended += 1,
                DeploymentStatus::Deleting | DeploymentStatus::Deleted => {
                    metrics.deployments_deleting += 1;
                }
            }
            if let Some(reason) = deployment.pending_reason() {
                let index = match reason {
                    DeploymentPendingReason::NoEligibleWorker => 0,
                    DeploymentPendingReason::WorkerCapacityExhausted => 1,
                    DeploymentPendingReason::WorkerDisconnected => 2,
                    DeploymentPendingReason::RequiredProtectionUnavailable => 3,
                    DeploymentPendingReason::SharedHostCapacityExhausted => 4,
                    DeploymentPendingReason::PlacementPending => 5,
                    DeploymentPendingReason::ServiceStarting => 6,
                    DeploymentPendingReason::ServiceRestartBackoff => 7,
                    DeploymentPendingReason::ServiceRestartBudgetExhausted => 8,
                    DeploymentPendingReason::ServiceAttemptUnresolved => 9,
                    DeploymentPendingReason::ServiceReadinessPending => 10,
                    DeploymentPendingReason::RolloutProgressDeadlineExceeded => 11,
                };
                metrics.pending_reasons[index] += 1;
            }
        }
        (
            metrics.pending_allocations,
            metrics.retried_service_attempts,
            metrics.terminal_service_attempts,
            metrics.service_catalog_owners,
        ) = connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM allocations WHERE state = 'pending'),
                    (SELECT COUNT(*) FROM service_attempts WHERE restart_count > 0),
                    (SELECT COUNT(*) FROM service_attempts WHERE terminal_failure != 0),
                    (SELECT COUNT(*) FROM service_catalog_owners)",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)? as u64,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, i64>(2)? as u64,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(metrics)
    }

    fn create_overlay_network_domain(
        &self,
        config: &conductor_core::OverlayNetworkDomainConfig,
        now_ms: i64,
    ) -> Result<(), OverlayNetworkError> {
        if now_ms < 0 {
            return Err(OverlayNetworkError::InvalidAddressPool);
        }
        let worker_pool = parse_overlay_pool(&config.worker_address_pool)?;
        let service_pool = parse_overlay_pool(&config.service_address_pool)?;
        if overlay_pools_overlap(&worker_pool, &service_pool) {
            return Err(OverlayNetworkError::AddressPoolOverlap);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let domain_exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM overlay_network_domains WHERE network_domain_id = ?1)",
                [config.network_domain_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        if domain_exists {
            return Err(OverlayNetworkError::DomainExists);
        }
        let mut statement = transaction
            .prepare(
                "SELECT worker_address_pool, service_address_pool FROM overlay_network_domains",
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        for row in rows {
            let (existing_worker, existing_service) =
                row.map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            let existing_worker = parse_overlay_pool(&existing_worker)?;
            let existing_service = parse_overlay_pool(&existing_service)?;
            if overlay_pools_overlap(&worker_pool, &existing_worker)
                || overlay_pools_overlap(&worker_pool, &existing_service)
                || overlay_pools_overlap(&service_pool, &existing_worker)
                || overlay_pools_overlap(&service_pool, &existing_service)
            {
                return Err(OverlayNetworkError::AddressPoolOverlap);
            }
        }
        drop(statement);
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO overlay_network_domains
             (network_domain_id, worker_address_pool, service_address_pool, network_generation, created_at_ms)
             VALUES (?1, ?2, ?3, 1, ?4)",
            params![
                config.network_domain_id.as_str(),
                worker_pool.to_string(),
                service_pool.to_string(),
                now_ms
            ],
        ).map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        if inserted == 0 {
            return Err(OverlayNetworkError::DomainExists);
        }
        transaction
            .commit()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)
    }

    fn get_overlay_network_domain(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
    ) -> Result<Option<conductor_core::OverlayNetworkDomainConfig>, OverlayNetworkError> {
        use conductor_core::NetworkDomainId;
        let connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        connection
            .query_row(
                "SELECT network_domain_id, worker_address_pool, service_address_pool
                 FROM overlay_network_domains WHERE network_domain_id = ?1",
                [network_domain_id.as_str()],
                |row| {
                    Ok(conductor_core::OverlayNetworkDomainConfig {
                        network_domain_id: NetworkDomainId::new(row.get::<_, String>(0)?)
                            .map_err(|_| rusqlite::Error::InvalidQuery)?,
                        worker_address_pool: row.get(1)?,
                        service_address_pool: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)
    }

    fn register_overlay_peer(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        endpoint_id: &conductor_core::OverlayEndpointId,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<conductor_core::OverlayPeerAssignment, OverlayNetworkError> {
        if now_ms < 0
            || expires_at_ms <= now_ms
            || expires_at_ms.saturating_sub(now_ms) > MAX_OVERLAY_LEASE_MS
        {
            return Err(OverlayNetworkError::InvalidLease);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let identity_is_active: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM worker_identities
                 WHERE worker_id = ?1 AND revoked_at_ms IS NULL)",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        if !identity_is_active {
            return Err(OverlayNetworkError::IdentityUnavailable);
        }
        let pool_text = transaction
            .query_row(
                "SELECT worker_address_pool FROM overlay_network_domains
                 WHERE network_domain_id = ?1",
                [network_domain_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?
            .ok_or(OverlayNetworkError::DomainNotFound)?;
        let pool = parse_overlay_pool(&pool_text)?;
        expire_overlay_peers(&transaction, network_domain_id, now_ms)?;
        let mut network_generation = transaction
            .query_row(
                "SELECT network_generation FROM overlay_network_domains WHERE network_domain_id = ?1",
                [network_domain_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;

        // Expired quarantine records release addresses but preserve endpoint tombstones.
        transaction
            .execute(
                "DELETE FROM overlay_peer_observations WHERE network_domain_id = ?1
                 AND worker_id IN (SELECT worker_id FROM overlay_peers
                     WHERE network_domain_id = ?1 AND state = 'quarantined'
                     AND quarantine_until_ms <= ?2)",
                params![network_domain_id.as_str(), now_ms],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM overlay_peers WHERE network_domain_id = ?1
                 AND state = 'quarantined' AND quarantine_until_ms <= ?2",
                params![network_domain_id.as_str(), now_ms],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;

        let existing = transaction
            .query_row(
                "SELECT endpoint_id, worker_address, peer_generation, expires_at_ms, state, quarantine_until_ms
                 FROM overlay_peers WHERE network_domain_id = ?1 AND worker_id = ?2",
                params![network_domain_id.as_str(), worker_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;

        let (address, peer_generation) = if let Some((
            old_endpoint,
            address,
            old_generation,
            _old_expiry,
            state,
            _quarantine,
        )) = existing
        {
            if state != "active" {
                return Err(OverlayNetworkError::PeerQuarantined);
            }
            if old_endpoint == endpoint_id.as_str() {
                network_generation = next_overlay_generation(network_generation)?;
                transaction
                    .execute(
                        "UPDATE overlay_peers SET expires_at_ms = ?1
                         WHERE network_domain_id = ?2 AND worker_id = ?3 AND state = 'active'",
                        params![
                            expires_at_ms,
                            network_domain_id.as_str(),
                            worker_id.as_str()
                        ],
                    )
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                (address, old_generation)
            } else {
                let retired: bool = transaction
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM overlay_endpoint_history
                         WHERE network_domain_id = ?1 AND endpoint_id = ?2)",
                        params![network_domain_id.as_str(), endpoint_id.as_str()],
                        |row| row.get(0),
                    )
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                if retired {
                    return Err(OverlayNetworkError::EndpointIdentityReplay);
                }
                let assigned: bool = transaction
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM overlay_peers
                         WHERE network_domain_id = ?1 AND endpoint_id = ?2 AND worker_id != ?3)",
                        params![
                            network_domain_id.as_str(),
                            endpoint_id.as_str(),
                            worker_id.as_str()
                        ],
                        |row| row.get(0),
                    )
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                if assigned {
                    return Err(OverlayNetworkError::EndpointIdentityConflict);
                }
                transaction
                    .execute(
                        "INSERT OR IGNORE INTO overlay_endpoint_history
                         (network_domain_id, endpoint_id, worker_id, peer_generation, retired_at_ms)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            network_domain_id.as_str(),
                            old_endpoint,
                            worker_id.as_str(),
                            old_generation,
                            now_ms
                        ],
                    )
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                let next_generation = next_overlay_generation(old_generation)?;
                network_generation = next_overlay_generation(network_generation)?;
                transaction
                    .execute(
                        "UPDATE overlay_peers SET endpoint_id = ?1, peer_generation = ?2,
                         expires_at_ms = ?3, state = 'active', quarantine_until_ms = NULL,
                         direct_candidates_json = '[]', direct_candidates_expires_at_ms = NULL
                         WHERE network_domain_id = ?4 AND worker_id = ?5",
                        params![
                            endpoint_id.as_str(),
                            next_generation,
                            expires_at_ms,
                            network_domain_id.as_str(),
                            worker_id.as_str()
                        ],
                    )
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                transaction
                    .execute(
                        "DELETE FROM overlay_peer_observations WHERE network_domain_id = ?1 AND worker_id = ?2",
                        params![network_domain_id.as_str(), worker_id.as_str()],
                    )
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                (address, next_generation)
            }
        } else {
            let retired: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM overlay_endpoint_history
                     WHERE network_domain_id = ?1 AND endpoint_id = ?2)",
                    params![network_domain_id.as_str(), endpoint_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            if retired {
                return Err(OverlayNetworkError::EndpointIdentityReplay);
            }
            let assigned: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM overlay_peers
                     WHERE network_domain_id = ?1 AND endpoint_id = ?2)",
                    params![network_domain_id.as_str(), endpoint_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            if assigned {
                return Err(OverlayNetworkError::EndpointIdentityConflict);
            }
            let count: i64 = transaction
                .query_row(
                    "SELECT COUNT(*) FROM overlay_peers WHERE network_domain_id = ?1",
                    [network_domain_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            if count >= MAX_OVERLAY_PEERS_PER_DOMAIN as i64 {
                return Err(OverlayNetworkError::PeerLimitReached);
            }
            let last_generation: Option<i64> = transaction
                .query_row(
                    "SELECT MAX(peer_generation) FROM overlay_endpoint_history
                     WHERE network_domain_id = ?1 AND worker_id = ?2",
                    params![network_domain_id.as_str(), worker_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            let peer_generation = next_overlay_generation(last_generation.unwrap_or(0))?;
            let address = find_overlay_address(&transaction, pool)?;
            network_generation = next_overlay_generation(network_generation)?;
            transaction
                .execute(
                    "INSERT INTO overlay_peers
                     (network_domain_id, worker_id, endpoint_id, worker_address, peer_generation,
                      expires_at_ms, state, quarantine_until_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', NULL)",
                    params![
                        network_domain_id.as_str(),
                        worker_id.as_str(),
                        endpoint_id.as_str(),
                        address,
                        peer_generation,
                        expires_at_ms
                    ],
                )
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            (address, peer_generation)
        };

        transaction
            .execute(
                "UPDATE overlay_network_domains SET network_generation = ?1 WHERE network_domain_id = ?2",
                params![network_generation, network_domain_id.as_str()],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let result = overlay_peer_assignment(OverlayPeerAssignmentRow {
            network_domain_id: network_domain_id.as_str().to_owned(),
            worker_id: worker_id.as_str().to_owned(),
            endpoint_id: endpoint_id.as_str().to_owned(),
            worker_address: address,
            peer_generation,
            network_generation,
            expires_at_ms,
            state: "active".to_owned(),
            quarantine_until_ms: None,
        })?;
        transaction
            .commit()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        Ok(result)
    }

    fn advertise_overlay_candidates(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        peer_generation: u64,
        candidates: &[std::net::SocketAddr],
        now_ms: i64,
    ) -> Result<conductor_core::OverlayDirectCandidateAcknowledgement, OverlayNetworkError> {
        if now_ms < 0 || !conductor_core::valid_overlay_direct_candidates(candidates) {
            return Err(OverlayNetworkError::InvalidDirectCandidates);
        }
        let peer_generation =
            i64::try_from(peer_generation).map_err(|_| OverlayNetworkError::StaleGeneration)?;
        let mut canonical_candidates = candidates.to_vec();
        canonical_candidates.sort_unstable();
        let candidates_json = serde_json::to_string(&canonical_candidates)
            .map_err(|_| OverlayNetworkError::InvalidDirectCandidates)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        expire_overlay_peers(&transaction, network_domain_id, now_ms)?;

        let identity_is_active: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM worker_identities
                 WHERE worker_id = ?1 AND revoked_at_ms IS NULL)",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        if !identity_is_active {
            return Err(OverlayNetworkError::IdentityUnavailable);
        }

        let current = transaction
            .query_row(
                "SELECT p.peer_generation, p.expires_at_ms, p.direct_candidates_json,
                        p.direct_candidates_expires_at_ms, d.network_generation
                 FROM overlay_peers p JOIN overlay_network_domains d USING (network_domain_id)
                 WHERE p.network_domain_id = ?1 AND p.worker_id = ?2
                   AND p.state = 'active' AND p.expires_at_ms > ?3",
                params![network_domain_id.as_str(), worker_id.as_str(), now_ms],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<i64>>(3)?,
                        row.get::<_, i64>(4)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?
            .ok_or(OverlayNetworkError::PeerNotFound)?;
        let (
            current_peer_generation,
            peer_expires_at_ms,
            current_candidates_json,
            current_candidates_expires_at_ms,
            mut network_generation,
        ) = current;
        if current_peer_generation != peer_generation {
            return Err(OverlayNetworkError::StaleGeneration);
        }
        let candidates_expires_at_ms = (!candidates.is_empty()).then(|| {
            now_ms
                .saturating_add(MAX_OVERLAY_CANDIDATE_LEASE_MS)
                .min(peer_expires_at_ms)
        });
        if current_candidates_json != candidates_json
            || current_candidates_expires_at_ms != candidates_expires_at_ms
        {
            network_generation = next_overlay_generation(network_generation)?;
        }
        transaction
            .execute(
                "UPDATE overlay_peers SET direct_candidates_json = ?1,
                 direct_candidates_expires_at_ms = ?2
                 WHERE network_domain_id = ?3 AND worker_id = ?4 AND peer_generation = ?5
                   AND state = 'active' AND expires_at_ms > ?6",
                params![
                    candidates_json,
                    candidates_expires_at_ms,
                    network_domain_id.as_str(),
                    worker_id.as_str(),
                    peer_generation,
                    now_ms
                ],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE overlay_network_domains SET network_generation = ?1
                 WHERE network_domain_id = ?2",
                params![network_generation, network_domain_id.as_str()],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let acknowledgement = conductor_core::OverlayDirectCandidateAcknowledgement {
            worker_id: worker_id.clone(),
            peer_generation: u64::try_from(current_peer_generation)
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
            network_generation: u64::try_from(network_generation)
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
            candidates_expires_at_ms,
        };
        transaction
            .commit()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        Ok(acknowledgement)
    }

    fn revoke_overlay_peer(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        now_ms: i64,
    ) -> Result<bool, OverlayNetworkError> {
        if now_ms < 0 {
            return Err(OverlayNetworkError::InvalidLease);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let current = transaction
            .query_row(
                "SELECT endpoint_id, worker_address, peer_generation, expires_at_ms, state, quarantine_until_ms
                 FROM overlay_peers WHERE network_domain_id = ?1 AND worker_id = ?2",
                params![network_domain_id.as_str(), worker_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let Some((endpoint, _address, generation, expiry, state, _already_quarantined_until)) =
            current
        else {
            return Err(OverlayNetworkError::PeerNotFound);
        };
        if state == "quarantined" {
            transaction
                .commit()
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            return Ok(false);
        }
        let effective_quarantine = expiry.max(now_ms).saturating_add(OVERLAY_CLOCK_SKEW_MS);
        transaction
            .execute(
                "UPDATE overlay_peers SET state = 'quarantined', quarantine_until_ms = ?1
                 WHERE network_domain_id = ?2 AND worker_id = ?3 AND state = 'active'",
                params![
                    effective_quarantine,
                    network_domain_id.as_str(),
                    worker_id.as_str()
                ],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO overlay_endpoint_history
                 (network_domain_id, endpoint_id, worker_id, peer_generation, retired_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    network_domain_id.as_str(),
                    endpoint,
                    worker_id.as_str(),
                    generation,
                    now_ms
                ],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let generation: i64 = transaction
            .query_row(
                "SELECT network_generation FROM overlay_network_domains WHERE network_domain_id = ?1",
                [network_domain_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?
            .ok_or(OverlayNetworkError::DomainNotFound)?;
        let next = next_overlay_generation(generation)?;
        transaction
            .execute(
                "UPDATE overlay_network_domains SET network_generation = ?1 WHERE network_domain_id = ?2",
                params![next, network_domain_id.as_str()],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        Ok(true)
    }

    fn overlay_peer_configuration(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        worker_id: &WorkerId,
        now_ms: i64,
    ) -> Result<Option<conductor_core::OverlayPeerConfiguration>, OverlayNetworkError> {
        use conductor_core::{OverlayPeer, OverlayPeerConfiguration};
        if now_ms < 0 {
            return Err(OverlayNetworkError::InvalidLease);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        expire_overlay_peers(&transaction, network_domain_id, now_ms)?;
        let current = transaction
            .query_row(
                "SELECT p.worker_address, p.peer_generation, p.expires_at_ms, d.network_generation
                 FROM overlay_peers p JOIN overlay_network_domains d USING (network_domain_id)
                 WHERE p.network_domain_id = ?1 AND p.worker_id = ?2
                   AND p.state = 'active' AND p.expires_at_ms > ?3
                   AND EXISTS (SELECT 1 FROM worker_identities i
                       WHERE i.worker_id = p.worker_id AND i.revoked_at_ms IS NULL)",
                params![network_domain_id.as_str(), worker_id.as_str(), now_ms],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let Some((address, peer_generation, expires_at_ms, network_generation)) = current else {
            transaction
                .commit()
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            return Ok(None);
        };
        let mut statement = transaction
            .prepare(
                "SELECT worker_id, endpoint_id, worker_address, peer_generation,
                        direct_candidates_json, direct_candidates_expires_at_ms
                 FROM overlay_peers WHERE network_domain_id = ?1 AND worker_id != ?2
                   AND state = 'active' AND expires_at_ms > ?3
                 ORDER BY worker_address LIMIT ?4",
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let rows = statement
            .query_map(
                params![
                    network_domain_id.as_str(),
                    worker_id.as_str(),
                    now_ms,
                    MAX_OVERLAY_PEERS_PER_DOMAIN as i64
                ],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let peers = rows
            .map(|row| {
                let (
                    worker,
                    endpoint,
                    address,
                    generation,
                    direct_candidates_json,
                    direct_candidates_expires_at_ms,
                ) = row.map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                let direct_candidates = if direct_candidates_expires_at_ms
                    .is_some_and(|expires_at_ms| expires_at_ms > now_ms)
                {
                    serde_json::from_str(&direct_candidates_json)
                        .map_err(|_| OverlayNetworkError::StorageUnavailable)?
                } else {
                    Vec::new()
                };
                Ok(OverlayPeer {
                    worker_id: WorkerId::new(worker)
                        .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
                    endpoint_id: conductor_core::OverlayEndpointId::new(endpoint)
                        .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
                    worker_address: overlay_address(address)?,
                    peer_generation: u64::try_from(generation)
                        .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
                    direct_candidates,
                    direct_candidates_expires_at_ms,
                })
            })
            .collect::<Result<Vec<_>, OverlayNetworkError>>()?;
        drop(statement);
        let configuration = OverlayPeerConfiguration {
            schema_version: conductor_core::OVERLAY_PEER_CONFIGURATION_SCHEMA_VERSION,
            network_domain_id: network_domain_id.clone(),
            worker_id: worker_id.clone(),
            worker_address: overlay_address(address)?,
            peer_generation: u64::try_from(peer_generation)
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
            network_generation: u64::try_from(network_generation)
                .map_err(|_| OverlayNetworkError::StorageUnavailable)?,
            expires_at_ms,
            peers,
        };
        transaction
            .commit()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        Ok(Some(configuration))
    }

    fn record_overlay_peer_observation(
        &self,
        observation: &conductor_core::OverlayPeerObservation,
    ) -> Result<(), OverlayNetworkError> {
        let peer_generation = i64::try_from(observation.peer_generation)
            .map_err(|_| OverlayNetworkError::StaleGeneration)?;
        let applied_generation = i64::try_from(observation.applied_network_generation)
            .map_err(|_| OverlayNetworkError::StaleGeneration)?;
        if observation.observed_at_ms < 0 {
            return Err(OverlayNetworkError::ObservationConflict);
        }
        let status = match observation.status {
            conductor_core::OverlayPeerObservationStatus::Applied => "applied",
            conductor_core::OverlayPeerObservationStatus::Rejected => "rejected",
            conductor_core::OverlayPeerObservationStatus::Unavailable => "unavailable",
        };
        let runtime_json = observation
            .runtime
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|_| OverlayNetworkError::ObservationConflict)?;
        if runtime_json
            .as_ref()
            .is_some_and(|runtime| runtime.len() > 65_536)
        {
            return Err(OverlayNetworkError::ObservationConflict);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let (stored_peer_generation, network_generation, worker_address) = transaction
            .query_row(
                "SELECT p.peer_generation, d.network_generation, p.worker_address FROM overlay_peers p
                 JOIN overlay_network_domains d USING (network_domain_id)
                 WHERE p.network_domain_id = ?1 AND p.worker_id = ?2 AND p.state = 'active'",
                params![
                    observation.network_domain_id.as_str(),
                    observation.worker_id.as_str()
                ],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?)),
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?
            .ok_or(OverlayNetworkError::PeerNotFound)?;
        if peer_generation != stored_peer_generation {
            return Err(OverlayNetworkError::StaleGeneration);
        }
        if applied_generation > network_generation {
            return Err(OverlayNetworkError::ObservationConflict);
        }
        if let Some(runtime) = observation.runtime.as_ref() {
            let expected_address = u32::try_from(worker_address)
                .ok()
                .map(std::net::Ipv4Addr::from);
            if !runtime.interface_name.starts_with("cnd-")
                || runtime.interface_name.len() > 15
                || !runtime.interface_name.is_ascii()
                || !runtime.interface_up
                || expected_address != Some(runtime.local_address)
                || runtime.mtu == 0
                || runtime.installed_routes.len() > 128
                || runtime.direct_links.len() > 128
                || runtime
                    .installed_routes
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
                || runtime.installed_routes.contains(&runtime.local_address)
            {
                return Err(OverlayNetworkError::ObservationConflict);
            }
            if applied_generation == network_generation {
                let mut authorized_routes = transaction
                    .prepare(
                        "SELECT worker_address FROM overlay_peers
                         WHERE network_domain_id = ?1 AND worker_id != ?2
                           AND state = 'active' AND expires_at_ms > ?3
                         ORDER BY worker_address",
                    )
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?
                    .query_map(
                        params![
                            observation.network_domain_id.as_str(),
                            observation.worker_id.as_str(),
                            observation.observed_at_ms
                        ],
                        |row| row.get::<_, i64>(0),
                    )
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?
                    .map(|route| {
                        let route = route.map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                        u32::try_from(route)
                            .map(std::net::Ipv4Addr::from)
                            .map_err(|_| OverlayNetworkError::StorageUnavailable)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                authorized_routes.sort_unstable();
                if runtime.installed_routes != authorized_routes {
                    return Err(OverlayNetworkError::ObservationConflict);
                }
            }
            let mut seen_endpoints = std::collections::BTreeSet::new();
            for link in &runtime.direct_links {
                if !seen_endpoints.insert(link.endpoint_id.as_str().to_owned())
                    || !transaction
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM overlay_peers
                             WHERE network_domain_id = ?1 AND endpoint_id = ?2 AND worker_id != ?3 AND state = 'active')",
                            params![observation.network_domain_id.as_str(), link.endpoint_id.as_str(), observation.worker_id.as_str()],
                            |row| row.get::<_, bool>(0),
                        )
                        .map_err(|_| OverlayNetworkError::StorageUnavailable)?
                    || link.selected_address.is_some_and(|address| {
                        !conductor_core::valid_overlay_direct_candidates(&[address])
                    })
                    || (link.connected
                        && (link.selected_address.is_none()
                            || link.last_handshake_at_ms.is_none()))
                    || link.last_handshake_at_ms.is_some_and(|time| {
                        time < 0 || time > observation.observed_at_ms
                    })
                {
                    return Err(OverlayNetworkError::ObservationConflict);
                }
            }
        }
        let previous = transaction
            .query_row(
                "SELECT peer_generation, applied_network_generation, observed_at_ms, status
                 FROM overlay_peer_observations WHERE network_domain_id = ?1 AND worker_id = ?2",
                params![
                    observation.network_domain_id.as_str(),
                    observation.worker_id.as_str()
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        if let Some((old_peer, old_network, old_time, old_status)) = previous {
            if old_peer > peer_generation
                || (old_peer == peer_generation
                    && (old_network > applied_generation || old_time > observation.observed_at_ms))
            {
                return Err(OverlayNetworkError::StaleGeneration);
            }
            if old_peer == peer_generation
                && old_network == applied_generation
                && old_time == observation.observed_at_ms
                && old_status != status
            {
                return Err(OverlayNetworkError::ObservationConflict);
            }
        }
        transaction
            .execute(
                "INSERT INTO overlay_peer_observations
                 (network_domain_id, worker_id, peer_generation, applied_network_generation, observed_at_ms, status, runtime_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(network_domain_id, worker_id) DO UPDATE SET
                   peer_generation = excluded.peer_generation,
                   applied_network_generation = excluded.applied_network_generation,
                   observed_at_ms = excluded.observed_at_ms,
                   status = excluded.status,
                   runtime_json = excluded.runtime_json",
                params![observation.network_domain_id.as_str(), observation.worker_id.as_str(), peer_generation, applied_generation, observation.observed_at_ms, status, runtime_json],
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)
    }

    fn list_overlay_peers(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        limit: usize,
        now_ms: i64,
    ) -> Result<Vec<conductor_core::OverlayPeerAssignment>, OverlayNetworkError> {
        if !(1..=MAX_OVERLAY_PEERS_PER_DOMAIN).contains(&limit) || now_ms < 0 {
            return Err(OverlayNetworkError::InvalidLease);
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        expire_overlay_peers(&transaction, network_domain_id, now_ms)?;
        let generation: i64 = transaction
            .query_row(
                "SELECT network_generation FROM overlay_network_domains WHERE network_domain_id = ?1",
                [network_domain_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?
            .ok_or(OverlayNetworkError::DomainNotFound)?;
        let mut statement = transaction
            .prepare(
                "SELECT network_domain_id, worker_id, endpoint_id, worker_address, peer_generation,
                        expires_at_ms, state, quarantine_until_ms
                 FROM overlay_peers WHERE network_domain_id = ?1
                   AND (state = 'active' OR quarantine_until_ms > ?3)
                 ORDER BY worker_address LIMIT ?2",
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let rows = statement
            .query_map(
                params![network_domain_id.as_str(), limit as i64, now_ms],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, String>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                    ))
                },
            )
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        let peers = rows.map(|row| {
            let (domain, worker, endpoint, address, peer_generation, expiry, state, quarantine) =
                row.map_err(|_| OverlayNetworkError::StorageUnavailable)?;
            overlay_peer_assignment(OverlayPeerAssignmentRow {
                network_domain_id: domain,
                worker_id: worker,
                endpoint_id: endpoint,
                worker_address: address,
                peer_generation,
                network_generation: generation,
                expires_at_ms: expiry,
                state,
                quarantine_until_ms: quarantine,
            })
        }).collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        transaction
            .commit()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        Ok(peers)
    }

    fn list_overlay_peer_diagnostics(
        &self,
        network_domain_id: &conductor_core::NetworkDomainId,
        limit: usize,
        now_ms: i64,
    ) -> Result<Vec<conductor_core::OverlayPeerDiagnostic>, OverlayNetworkError> {
        let peers = self.list_overlay_peers(network_domain_id, limit, now_ms)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
        peers
            .into_iter()
            .map(|assignment| {
                let observation = connection
                    .query_row(
                        "SELECT peer_generation, applied_network_generation, observed_at_ms, status, runtime_json
                         FROM overlay_peer_observations
                         WHERE network_domain_id = ?1 AND worker_id = ?2",
                        params![network_domain_id.as_str(), assignment.worker_id.as_str()],
                        |row| {
                            Ok((
                                row.get::<_, i64>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, i64>(2)?,
                                row.get::<_, String>(3)?,
                                row.get::<_, Option<String>>(4)?,
                            ))
                        },
                    )
                    .optional()
                    .map_err(|_| OverlayNetworkError::StorageUnavailable)?
                    .filter(|(peer_generation, _, _, _, _)| {
                        *peer_generation >= 0
                            && *peer_generation as u64 == assignment.peer_generation
                    });
                let (
                    observation_status,
                    observation_network_generation,
                    observation_at_ms,
                    observation_fresh,
                    runtime,
                ) = if let Some((_, network_generation, observed_at_ms, status, runtime_json)) = observation {
                    let status = match status.as_str() {
                        "applied" => conductor_core::OverlayPeerObservationStatus::Applied,
                        "rejected" => conductor_core::OverlayPeerObservationStatus::Rejected,
                        "unavailable" => conductor_core::OverlayPeerObservationStatus::Unavailable,
                        _ => return Err(OverlayNetworkError::StorageUnavailable),
                    };
                    let current_generation = network_generation >= 0
                        && network_generation as u64 == assignment.network_generation;
                    let fresh = current_generation
                        && observed_at_ms >= 0
                        && observed_at_ms <= now_ms
                        && now_ms.saturating_sub(observed_at_ms) <= 60_000;
                    let runtime = runtime_json
                        .map(|runtime| serde_json::from_str(&runtime))
                        .transpose()
                        .map_err(|_| OverlayNetworkError::StorageUnavailable)?;
                    (
                        Some(status),
                        u64::try_from(network_generation).ok(),
                        Some(observed_at_ms),
                        fresh,
                        runtime,
                    )
                } else {
                    (None, None, None, false, None)
                };
                Ok(conductor_core::OverlayPeerDiagnostic {
                    assignment,
                    observation_status,
                    observation_network_generation,
                    observation_at_ms,
                    observation_fresh,
                    runtime,
                    reachability: conductor_core::OverlayReachability::NotVerified,
                })
            })
            .collect()
    }

    fn insert(&self, job: JobSpec) -> Result<(), JobStoreError> {
        let spec_json =
            serde_json::to_string(&job).map_err(|_| JobStoreError::StorageUnavailable)?;
        let created_at_ms = now_ms().map_err(|_| JobStoreError::StorageUnavailable)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JobStoreError::StorageUnavailable)?;

        let exists: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1)",
                [job.id().as_str()],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if exists {
            return Err(JobStoreError::DuplicateId);
        }
        let active_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM jobs WHERE deleted_at_ms IS NULL",
                [],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if active_count >= MAX_JOBS as i64 {
            return Err(JobStoreError::LimitExceeded);
        }
        transaction
            .execute(
                "INSERT INTO jobs (id, spec_json, created_at_ms, deleted_at_ms) VALUES (?1, ?2, ?3, NULL)",
                params![job.id().as_str(), spec_json, created_at_ms],
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| JobStoreError::StorageUnavailable)
    }

    fn get(&self, id: &JobId) -> Result<Option<JobSpec>, JobStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let serialized = connection
            .query_row(
                "SELECT spec_json FROM jobs WHERE id = ?1 AND deleted_at_ms IS NULL",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        serialized
            .map(|value| {
                serde_json::from_str(&value).map_err(|_| JobStoreError::StorageUnavailable)
            })
            .transpose()
    }

    fn list_runs_page(
        &self,
        job_id: Option<&JobId>,
        state_filter: Option<RunState>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<RunObservation>, QueryError> {
        if !(1..=MAX_PAGE_SIZE).contains(&limit) {
            return Err(QueryError::InvalidCursor);
        }
        let filter_job = job_id.map(JobId::as_str).unwrap_or("-");
        let filter_state = state_filter.map(RunState::as_str).unwrap_or("-");
        let before_rowid = match cursor {
            Some(cursor) => {
                let payload = decode_cursor(cursor)?;
                let mut parts = payload.split('|');
                if parts.next() != Some("r")
                    || parts.next() != Some(filter_job)
                    || parts.next() != Some(filter_state)
                {
                    return Err(QueryError::InvalidCursor);
                }
                let run_order = parts
                    .next()
                    .ok_or(QueryError::InvalidCursor)?
                    .parse::<i64>()
                    .map_err(|_| QueryError::InvalidCursor)?;
                if parts.next().is_some() || run_order <= 0 {
                    return Err(QueryError::InvalidCursor);
                }
                Some(run_order)
            }
            None => None,
        };
        let connection = self
            .connection
            .lock()
            .map_err(|_| QueryError::StorageUnavailable)?;
        let mut query = String::from("SELECT run_order, observation_json FROM runs WHERE 1 = 1");
        let mut values = Vec::with_capacity(4);
        if let Some(job_id) = job_id {
            query.push_str(" AND job_id = ?");
            values.push(rusqlite::types::Value::Text(job_id.as_str().to_owned()));
        }
        if let Some(state_filter) = state_filter {
            query.push_str(" AND state = ?");
            values.push(rusqlite::types::Value::Text(
                state_filter.as_str().to_owned(),
            ));
        }
        if let Some(before_order) = before_rowid {
            query.push_str(" AND run_order < ?");
            values.push(rusqlite::types::Value::Integer(before_order));
        }
        query.push_str(" ORDER BY run_order DESC LIMIT ?");
        values.push(rusqlite::types::Value::Integer((limit + 1) as i64));
        let mut statement = connection
            .prepare(&query)
            .map_err(|_| QueryError::StorageUnavailable)?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(values), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| QueryError::StorageUnavailable)?;
        let rows = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| QueryError::StorageUnavailable)?;
        let has_more = rows.len() > limit;
        let page_rows = &rows[..rows.len().min(limit)];
        let items = page_rows
            .iter()
            .map(|(_, json)| serde_json::from_str(json).map_err(|_| QueryError::StorageUnavailable))
            .collect::<Result<Vec<RunObservation>, _>>()?;
        let next_cursor = if has_more {
            page_rows
                .last()
                .map(|(rowid, _)| encode_cursor(&format!("r|{filter_job}|{filter_state}|{rowid}")))
        } else {
            None
        };
        Ok(Page { items, next_cursor })
    }

    fn list_jobs_page(
        &self,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<Page<JobSpec>, QueryError> {
        if !(1..=MAX_PAGE_SIZE).contains(&limit) {
            return Err(QueryError::InvalidCursor);
        }
        let after_id = match cursor {
            Some(cursor) => {
                let payload = decode_cursor(cursor)?;
                let Some(id) = payload.strip_prefix("j|") else {
                    return Err(QueryError::InvalidCursor);
                };
                JobId::new(id.to_owned()).map_err(|_| QueryError::InvalidCursor)?;
                Some(id.to_owned())
            }
            None => None,
        };
        let connection = self
            .connection
            .lock()
            .map_err(|_| QueryError::StorageUnavailable)?;
        let query = if after_id.is_some() {
            "SELECT id, spec_json FROM jobs WHERE deleted_at_ms IS NULL AND id > ?1 ORDER BY id COLLATE BINARY ASC LIMIT ?2"
        } else {
            "SELECT id, spec_json FROM jobs WHERE deleted_at_ms IS NULL ORDER BY id COLLATE BINARY ASC LIMIT ?1"
        };
        let mut values = Vec::with_capacity(2);
        if let Some(after_id) = after_id {
            values.push(rusqlite::types::Value::Text(after_id));
        }
        values.push(rusqlite::types::Value::Integer((limit + 1) as i64));
        let mut statement = connection
            .prepare(query)
            .map_err(|_| QueryError::StorageUnavailable)?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(values), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| QueryError::StorageUnavailable)?;
        let mut jobs = rows
            .map(|row| {
                let (_, json) = row.map_err(|_| QueryError::StorageUnavailable)?;
                serde_json::from_str(&json).map_err(|_| QueryError::StorageUnavailable)
            })
            .collect::<Result<Vec<JobSpec>, _>>()?;
        let has_more = jobs.len() > limit;
        jobs.truncate(limit);
        let next_cursor = if has_more {
            jobs.last()
                .map(|job| encode_cursor(&format!("j|{}", job.id())))
        } else {
            None
        };
        Ok(Page {
            items: jobs,
            next_cursor,
        })
    }

    fn delete(&self, id: &JobId) -> Result<bool, JobStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let active_run: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM runs WHERE job_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown'))",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        let active_deployment: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM job_deployments WHERE job_id = ?1 AND deleted_at_ms IS NULL)",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        if active_run || active_deployment {
            return Err(JobStoreError::ActiveRunConflict);
        }
        let deleted_at_ms = now_ms().map_err(|_| JobStoreError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE jobs SET deleted_at_ms = ?1 WHERE id = ?2 AND deleted_at_ms IS NULL",
                params![deleted_at_ms, id.as_str()],
            )
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| JobStoreError::StorageUnavailable)?;
        Ok(changed == 1)
    }

    fn create_revision(
        &self,
        draft: &JobRevisionDraft,
        idempotency_key: &str,
    ) -> Result<JobRevision, ExecutionStoreError> {
        let job_id = draft.job_id();
        let request_json =
            serde_json::to_string(draft).map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;

        if let Some((previous_request, previous_response)) = transaction
            .query_row(
                "SELECT request_json, response_json FROM execution_idempotency
                 WHERE operation = 'revision' AND scope_key = ?1 AND idempotency_key = ?2",
                params![job_id.as_str(), idempotency_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
        {
            if previous_request != request_json {
                return Err(ExecutionStoreError::IdempotencyConflict);
            }
            let revision = serde_json::from_str(&previous_response)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(revision);
        }

        let existing_job: Option<Option<i64>> = transaction
            .query_row(
                "SELECT deleted_at_ms FROM jobs WHERE id = ?1",
                [job_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        match existing_job {
            Some(Some(_)) => return Err(ExecutionStoreError::JobNotFound),
            Some(None) => {}
            None => {
                let active_count: i64 = transaction
                    .query_row(
                        "SELECT COUNT(*) FROM jobs WHERE deleted_at_ms IS NULL",
                        [],
                        |row| row.get(0),
                    )
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                if active_count >= MAX_JOBS as i64 {
                    return Err(ExecutionStoreError::JobLimitReached);
                }
                let spec_json = serde_json::to_string(draft.definition())
                    .map_err(|_| ExecutionStoreError::InvalidRequest)?;
                let created_at_ms =
                    now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                transaction
                    .execute(
                        "INSERT INTO jobs (id, spec_json, created_at_ms, deleted_at_ms)
                         VALUES (?1, ?2, ?3, NULL)",
                        params![job_id.as_str(), spec_json, created_at_ms],
                    )
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            }
        }

        let latest_revision: i64 = transaction
            .query_row(
                "SELECT COALESCE(MAX(revision), 0) FROM job_revisions WHERE job_id = ?1",
                [job_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let revision_number = u64::try_from(latest_revision)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(ExecutionStoreError::Conflict)?;
        let stored_revision_number =
            i64::try_from(revision_number).map_err(|_| ExecutionStoreError::Conflict)?;
        let revision = draft
            .clone()
            .into_revision(revision_number)
            .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let revision_json = serde_json::to_string(&revision)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let created_at_ms = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO job_revisions (job_id, revision, revision_json, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    job_id.as_str(),
                    stored_revision_number,
                    revision_json,
                    created_at_ms
                ],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO execution_idempotency
                 (operation, scope_key, idempotency_key, request_json, response_json, created_at_ms)
                 VALUES ('revision', ?1, ?2, ?3, ?4, ?5)",
                params![
                    job_id.as_str(),
                    idempotency_key,
                    request_json,
                    revision_json,
                    created_at_ms
                ],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(revision)
    }

    fn get_revision(&self, id: &JobRevisionId) -> Result<Option<JobRevision>, ExecutionStoreError> {
        let revision_number =
            i64::try_from(id.revision()).map_err(|_| ExecutionStoreError::RevisionNotFound)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let revision_json = connection
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![id.job_id().as_str(), revision_number],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        revision_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|_| ExecutionStoreError::StorageUnavailable)
            })
            .transpose()
    }

    fn get_deployment(&self, id: &JobId) -> Result<Option<JobDeployment>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let deployment_json = connection
            .query_row(
                "SELECT deployment_json FROM job_deployments WHERE job_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        deployment_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|_| ExecutionStoreError::StorageUnavailable)
            })
            .transpose()
    }

    fn mutate_deployment(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
    ) -> Result<JobDeployment, ExecutionStoreError> {
        self.mutate_deployment_with_workers(id, expected_generation, mutation, idempotency_key, &[])
    }

    fn mutate_deployment_with_workers(
        &self,
        id: &JobId,
        expected_generation: Option<u64>,
        mutation: &DeploymentMutation,
        idempotency_key: &str,
        workers: &[WorkerStatus],
    ) -> Result<JobDeployment, ExecutionStoreError> {
        if let DeploymentMutation::Deploy { strategy, .. } = mutation
            && DeploymentUpdateStrategy::new(
                strategy.max_surge(),
                strategy.max_unavailable(),
                strategy.progress_deadline_seconds(),
            )
            .is_err()
        {
            return Err(ExecutionStoreError::InvalidRequest);
        }
        let request_json = serde_json::to_string(&(expected_generation, mutation))
            .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;

        if let Some((previous_request, previous_response)) = transaction
            .query_row(
                "SELECT request_json, response_json FROM execution_idempotency
                 WHERE operation = 'deployment' AND scope_key = ?1 AND idempotency_key = ?2",
                params![id.as_str(), idempotency_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
        {
            if previous_request != request_json {
                return Err(ExecutionStoreError::IdempotencyConflict);
            }
            let deployment = serde_json::from_str(&previous_response)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(deployment);
        }

        let job_active: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE id = ?1 AND deleted_at_ms IS NULL)",
                [id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if !job_active {
            return Err(ExecutionStoreError::JobNotFound);
        }
        let current_json: Option<String> = transaction
            .query_row(
                "SELECT deployment_json FROM job_deployments WHERE job_id = ?1",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let current = current_json
            .as_deref()
            .map(serde_json::from_str::<JobDeployment>)
            .transpose()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        match (&current, expected_generation) {
            (None, None) if matches!(mutation, DeploymentMutation::Deploy { .. }) => {}
            (Some(existing), Some(expected)) if existing.generation() == expected => {}
            _ => return Err(ExecutionStoreError::PreconditionFailed),
        }
        if current.as_ref().is_some_and(|deployment| {
            matches!(
                deployment.status(),
                DeploymentStatus::Deleting | DeploymentStatus::Deleted
            )
        }) {
            return Err(ExecutionStoreError::Conflict);
        }
        if current.as_ref().and_then(JobDeployment::rollout).is_some()
            && matches!(
                (current.as_ref(), mutation),
                (
                    Some(existing),
                    DeploymentMutation::Deploy { revision, .. }
                ) if *revision != existing.revision_id().revision()
            )
        {
            return Err(ExecutionStoreError::Conflict);
        }

        let (revision_number, replicas, suspended) = match (current.as_ref(), mutation) {
            (
                None,
                DeploymentMutation::Deploy {
                    revision, replicas, ..
                },
            ) => (*revision, *replicas, false),
            (
                Some(existing),
                DeploymentMutation::Deploy {
                    revision, replicas, ..
                },
            ) => (*revision, *replicas, existing.suspended()),
            (Some(existing), DeploymentMutation::Scale { replicas }) => (
                existing.revision_id().revision(),
                *replicas,
                existing.suspended(),
            ),
            (Some(existing), DeploymentMutation::Suspend) if !existing.suspended() => (
                existing.revision_id().revision(),
                existing.desired_replicas(),
                true,
            ),
            (Some(existing), DeploymentMutation::Resume) if existing.suspended() => (
                existing.revision_id().revision(),
                existing.desired_replicas(),
                false,
            ),
            (Some(existing), DeploymentMutation::Rollback { revision }) => {
                (*revision, existing.desired_replicas(), existing.suspended())
            }
            (Some(_), DeploymentMutation::Delete) => {
                let current = current
                    .as_ref()
                    .ok_or(ExecutionStoreError::StorageUnavailable)?;
                (current.revision_id().revision(), 0, true)
            }
            _ => return Err(ExecutionStoreError::Conflict),
        };
        if replicas > MAX_DEPLOYMENT_REPLICAS {
            return Err(ExecutionStoreError::InvalidRequest);
        }
        let revision_id = JobRevisionId::new(id.clone(), revision_number)
            .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        let stored_revision_number =
            i64::try_from(revision_number).map_err(|_| ExecutionStoreError::RevisionNotFound)?;
        let revision_json: Option<String> = transaction
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![id.as_str(), stored_revision_number],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let revision: JobRevision = revision_json
            .ok_or(ExecutionStoreError::RevisionNotFound)
            .and_then(|json| {
                serde_json::from_str(&json).map_err(|_| ExecutionStoreError::StorageUnavailable)
            })?;
        if revision.id() != &revision_id
            || !matches!(
                revision.workload(),
                conductor_core::Workload::Service { .. }
            )
        {
            return Err(ExecutionStoreError::InvalidRequest);
        }
        if !matches!(mutation, DeploymentMutation::Delete)
            && let Workload::Service { identity, .. } = revision.workload()
        {
            ensure_service_identity_available(&transaction, id, identity)?;
        }
        let (status, pending_reason, pending_message) = if suspended {
            if matches!(mutation, DeploymentMutation::Delete) {
                (DeploymentStatus::Deleted, None, None)
            } else {
                (DeploymentStatus::Suspended, None, None)
            }
        } else if replicas == 0 {
            (DeploymentStatus::Ready, None, None)
        } else {
            (
                DeploymentStatus::Pending,
                Some(DeploymentPendingReason::PlacementPending),
                Some("deployment intent is durable; scheduler placement is pending".to_owned()),
            )
        };
        let generation = current
            .as_ref()
            .map_or(Some(1), |value| value.generation().checked_add(1))
            .ok_or(ExecutionStoreError::Conflict)?;
        let strategy = match mutation {
            DeploymentMutation::Deploy { strategy, .. } => *strategy,
            DeploymentMutation::Rollback { .. } => current
                .as_ref()
                .and_then(JobDeployment::rollout)
                .map(DeploymentRollout::strategy)
                .unwrap_or_default(),
            _ => current
                .as_ref()
                .and_then(JobDeployment::rollout)
                .map(DeploymentRollout::strategy)
                .unwrap_or_default(),
        };
        let rollout = if replicas == 0 {
            None
        } else if let Some(existing) = current.as_ref()
            && existing.revision_id().revision() != revision_number
        {
            let now = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let rolling_back = matches!(mutation, DeploymentMutation::Rollback { .. });
            let deadline = if rolling_back {
                None
            } else {
                Some(now.saturating_add(i64::from(strategy.progress_deadline_seconds()) * 1000))
            };
            Some(
                DeploymentRollout::new(
                    existing.revision_id().clone(),
                    now,
                    deadline,
                    strategy,
                    rolling_back,
                )
                .map_err(|_| ExecutionStoreError::InvalidRequest)?,
            )
        } else {
            current.as_ref().and_then(JobDeployment::rollout).cloned()
        };
        let deployment = JobDeployment::new(
            &revision,
            generation,
            replicas,
            suspended,
            status,
            pending_reason,
            pending_message,
        )
        .map_err(|_| ExecutionStoreError::InvalidRequest)?
        .with_rollout(rollout);
        let mut deployment = place_service_slots(&transaction, &revision, &deployment, workers)?;
        if matches!(mutation, DeploymentMutation::Delete) {
            let deletion_status = if active_allocations_exist(&transaction, id)? {
                DeploymentStatus::Deleting
            } else {
                DeploymentStatus::Deleted
            };
            deployment =
                JobDeployment::new(&revision, generation, 0, true, deletion_status, None, None)
                    .map_err(|_| ExecutionStoreError::InvalidRequest)?;
        }
        if deployment.status() == DeploymentStatus::Deleted {
            transaction
                .execute(
                    "DELETE FROM service_catalog_owners WHERE job_id = ?1",
                    [id.as_str()],
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        } else {
            upsert_service_catalog_owner(&transaction, id, &revision)?;
        }
        let deployment_json = serde_json::to_string(&deployment)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let deleted_at_ms = if deployment.status() == DeploymentStatus::Deleted {
            Some(now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?)
        } else {
            None
        };
        let created_at_ms = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO job_deployments (job_id, generation, deployment_json, deleted_at_ms)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(job_id) DO UPDATE SET generation = excluded.generation,
                     deployment_json = excluded.deployment_json, deleted_at_ms = excluded.deleted_at_ms",
                params![id.as_str(), generation as i64, deployment_json, deleted_at_ms],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO execution_idempotency
                 (operation, scope_key, idempotency_key, request_json, response_json, created_at_ms)
                 VALUES ('deployment', ?1, ?2, ?3, ?4, ?5)",
                params![
                    id.as_str(),
                    idempotency_key,
                    request_json,
                    deployment_json,
                    created_at_ms
                ],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(deployment)
    }

    fn reconcile_deployment_placement(
        &self,
        id: &JobId,
        workers: &[WorkerStatus],
    ) -> Result<Option<JobDeployment>, ExecutionStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let current_json = transaction
            .query_row(
                "SELECT deployment_json FROM job_deployments WHERE job_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let Some(current_json) = current_json else {
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(None);
        };
        let mut current: JobDeployment = serde_json::from_str(&current_json)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if current.status() == DeploymentStatus::Deleting {
            if active_allocations_exist(&transaction, id)? {
                transaction
                    .commit()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                return Ok(Some(current));
            }
            let revision_number = i64::try_from(current.revision_id().revision())
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let revision_json = transaction
                .query_row(
                    "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                    params![id.as_str(), revision_number],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?
                .ok_or(ExecutionStoreError::StorageUnavailable)?;
            let revision: JobRevision = serde_json::from_str(&revision_json)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let deleted = JobDeployment::new(
                &revision,
                current.generation(),
                0,
                true,
                DeploymentStatus::Deleted,
                None,
                None,
            )
            .map_err(|_| ExecutionStoreError::InvalidRequest)?;
            let deleted_json = serde_json::to_string(&deleted)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .execute(
                    "UPDATE job_deployments SET deployment_json = ?1, deleted_at_ms = ?2
                     WHERE job_id = ?3 AND generation = ?4",
                    params![
                        deleted_json,
                        now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                        id.as_str(),
                        current.generation() as i64
                    ],
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .execute(
                    "DELETE FROM service_catalog_owners WHERE job_id = ?1",
                    [id.as_str()],
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(Some(deleted));
        }
        if !current.suspended()
            && current.desired_replicas() > 0
            && let Some(rollout) = current.rollout()
            && !rollout.rolling_back()
            && rollout
                .progress_deadline_at_ms()
                .is_some_and(|deadline| now_ms().is_ok_and(|now| now >= deadline))
            && service_rollout_pending_reason(&transaction, id, &current)?.is_some()
        {
            let rollout = current
                .rollout()
                .cloned()
                .ok_or(ExecutionStoreError::StorageUnavailable)?;
            let previous_revision_number = i64::try_from(rollout.previous_revision_id().revision())
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let previous_revision_json = transaction
                .query_row(
                    "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                    params![id.as_str(), previous_revision_number],
                    |row| row.get::<_, String>(0),
                )
                .optional()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?
                .ok_or(ExecutionStoreError::StorageUnavailable)?;
            let previous_revision: JobRevision = serde_json::from_str(&previous_revision_json)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let now = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let rollback = JobDeployment::new(
                &previous_revision,
                current
                    .generation()
                    .checked_add(1)
                    .ok_or(ExecutionStoreError::Conflict)?,
                current.desired_replicas(),
                current.suspended(),
                DeploymentStatus::Pending,
                Some(DeploymentPendingReason::RolloutProgressDeadlineExceeded),
                Some(
                    "rollout progress deadline elapsed; restoring the previous revision".to_owned(),
                ),
            )
            .map_err(|_| ExecutionStoreError::InvalidRequest)?
            .with_rollout(Some(
                DeploymentRollout::new(
                    current.revision_id().clone(),
                    now,
                    None,
                    rollout.strategy(),
                    true,
                )
                .map_err(|_| ExecutionStoreError::InvalidRequest)?,
            ));
            current = rollback;
        }
        if current.suspended() || current.desired_replicas() == 0 {
            transaction
                .commit()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            return Ok(Some(current));
        }
        let revision_number = i64::try_from(current.revision_id().revision())
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let revision_json = transaction
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![id.as_str(), revision_number],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            .ok_or(ExecutionStoreError::StorageUnavailable)?;
        let revision: JobRevision = serde_json::from_str(&revision_json)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let placed = place_service_slots(&transaction, &revision, &current, workers)?;
        upsert_service_catalog_owner(&transaction, id, &revision)?;
        let deployment_json =
            serde_json::to_string(&placed).map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        if deployment_json != current_json {
            transaction
                .execute(
                    "UPDATE job_deployments SET generation = ?1, deployment_json = ?2
                     WHERE job_id = ?3",
                    params![placed.generation() as i64, deployment_json, id.as_str()],
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        }
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(Some(placed))
    }

    fn list_allocations(&self, id: &JobId) -> Result<Vec<AllocationRecord>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT allocation_json, resource_domain_id FROM allocations
                 WHERE job_id = ?1 ORDER BY created_at_ms, allocation_id",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
            })
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        rows.map(|row| {
            let (allocation_json, resource_domain_id) =
                row.map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let allocation = serde_json::from_str(&allocation_json)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let resource_domain_id = resource_domain_id
                .map(WorkerId::new)
                .transpose()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            Ok(AllocationRecord {
                allocation,
                resource_domain_id,
            })
        })
        .collect()
    }

    fn list_active_deployments(&self) -> Result<Vec<JobDeployment>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT deployment_json FROM job_deployments
                 WHERE deleted_at_ms IS NULL ORDER BY job_id LIMIT ?1",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([MAX_JOBS as i64], |row| row.get::<_, String>(0))
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|_| ExecutionStoreError::StorageUnavailable)?)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn list_service_attempts(
        &self,
        id: &JobId,
    ) -> Result<Vec<ServiceAttemptRecord>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare(
                "SELECT si.instance_id, si.replica_index, si.current_allocation_id,
                        sa.allocation_id, a.allocation_json, a.resource_domain_id,
                        sa.current_run_id, COALESCE(sa.restart_count, 0),
                        COALESCE(sa.next_start_at_ms, 0), COALESCE(sa.terminal_failure, 0),
                        r.observation_json, r.job_spec_json
                 FROM service_instances si
                 LEFT JOIN service_attempts sa ON sa.service_instance_id = si.instance_id
                 LEFT JOIN allocations a ON a.allocation_id = COALESCE(sa.allocation_id, si.current_allocation_id)
                 LEFT JOIN runs r ON r.run_id = sa.current_run_id
                 WHERE si.job_id = ?1 AND (si.current_allocation_id IS NOT NULL
                     OR sa.current_run_id IS NOT NULL OR sa.allocation_id IS NOT NULL)
                 ORDER BY si.replica_index",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, Option<String>>(10)?,
                    row.get::<_, Option<String>>(11)?,
                ))
            })
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        rows.map(|row| {
            let (
                instance,
                replica_index,
                current_allocation_id,
                attempt_allocation_id,
                allocation_json,
                domain,
                run_id,
                count,
                next_start,
                terminal,
                observation,
                run_spec_json,
            ) = row.map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let service_ports = run_spec_json
                .map(|json| serde_json::from_str::<RunSpec>(&json))
                .transpose()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?
                .map(|spec| spec.service_ports().to_vec())
                .unwrap_or_default();
            let current_run = observation
                .map(|json| {
                    serde_json::from_str::<RunObservation>(&json)
                        .map_err(|_| ExecutionStoreError::StorageUnavailable)
                })
                .transpose()?;
            let service_ports_ready = current_run.as_ref().is_some_and(|run| {
                run.state() == RunState::Running
                    && run.readiness() == Some(conductor_core::ReadinessState::Ready)
            });
            Ok(ServiceAttemptRecord {
                service_instance_id: ServiceInstanceId::new(instance)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                replica_index: u32::try_from(replica_index)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                current_allocation_id: current_allocation_id
                    .map(AllocationId::new)
                    .transpose()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                allocation: match (attempt_allocation_id, allocation_json) {
                    (Some(_), Some(json)) => Some(AllocationRecord {
                        allocation: serde_json::from_str(&json)
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                        resource_domain_id: domain
                            .map(WorkerId::new)
                            .transpose()
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                    }),
                    (None, Some(json)) => Some(AllocationRecord {
                        allocation: serde_json::from_str(&json)
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                        resource_domain_id: domain
                            .map(WorkerId::new)
                            .transpose()
                            .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                    }),
                    _ => None,
                },
                current_run_id: run_id
                    .map(RunId::new)
                    .transpose()
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                current_run,
                restart_count: u32::try_from(count)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                next_start_at_ms: next_start,
                terminal_failure: terminal != 0,
                service_ports,
                service_ports_ready,
            })
        })
        .collect()
    }

    fn list_service_catalog(
        &self,
        identity: &ServiceIdentity,
        workers: &[WorkerStatus],
    ) -> Result<Vec<ServiceCatalogEndpoint>, ExecutionStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Deferred)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut deployments_statement = transaction
            .prepare(
                "SELECT d.job_id, d.deployment_json
                 FROM service_catalog_owners o
                 JOIN job_deployments d ON d.job_id = o.job_id
                 WHERE o.namespace = ?1 AND o.service_name = ?2
                   AND d.deleted_at_ms IS NULL
                 ORDER BY d.job_id",
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let deployments = deployments_statement
            .query_map(params![identity.namespace(), identity.name()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        drop(deployments_statement);

        let mut endpoints = Vec::new();
        for (raw_job_id, deployment_json) in deployments {
            let job_id =
                JobId::new(raw_job_id).map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let deployment: JobDeployment = serde_json::from_str(&deployment_json)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            if deployment.suspended() || deployment.desired_replicas() == 0 {
                continue;
            }
            let revision_number = i64::try_from(deployment.revision_id().revision())
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let revision_json: String = transaction
                .query_row(
                    "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                    params![job_id.as_str(), revision_number],
                    |row| row.get(0),
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let revision: JobRevision = serde_json::from_str(&revision_json)
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            if revision.id() != deployment.revision_id()
                || !matches!(
                    revision.workload(),
                    Workload::Service { identity: current, .. } if current == identity
                )
            {
                continue;
            }

            let mut statement = transaction
                .prepare(
                    "SELECT si.instance_id, si.replica_index, si.current_allocation_id,
                            a.allocation_json, sa.current_run_id, r.observation_json,
                            r.job_spec_json, l.name, l.protocol, l.port, l.advertised_address
                     FROM service_instances si
                     JOIN allocations a ON a.allocation_id = si.current_allocation_id
                         AND a.job_id = si.job_id AND a.revision = ?2 AND a.state = 'assigned'
                     JOIN service_attempts sa ON sa.service_instance_id = si.instance_id
                         AND sa.allocation_id = a.allocation_id
                         AND sa.current_run_id IS NOT NULL
                     JOIN runs r ON r.run_id = sa.current_run_id AND r.job_id = si.job_id
                     JOIN service_port_leases l ON l.run_id = r.run_id
                         AND l.allocation_id = a.allocation_id
                     WHERE si.job_id = ?1 AND si.replica_index < ?3
                     ORDER BY si.replica_index, l.name",
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            let rows = statement
                .query_map(
                    params![
                        job_id.as_str(),
                        revision_number,
                        i64::from(deployment.desired_replicas())
                    ],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, String>(8)?,
                            row.get::<_, i64>(9)?,
                            row.get::<_, String>(10)?,
                        ))
                    },
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            drop(statement);

            for (
                raw_instance_id,
                replica_index,
                current_allocation_id,
                allocation_json,
                raw_run_id,
                observation_json,
                run_spec_json,
                port_name,
                protocol,
                raw_port,
                advertised_address,
            ) in rows
            {
                let replica_index = u32::try_from(replica_index)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                if replica_index >= deployment.desired_replicas() {
                    continue;
                }
                let instance_id = ServiceInstanceId::new(raw_instance_id)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                let allocation: Allocation = serde_json::from_str(&allocation_json)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                let allocation_id = allocation.id().clone();
                if current_allocation_id.as_deref() != Some(allocation_id.as_str())
                    || allocation.state() != AllocationState::Assigned
                    || allocation.job_revision_id() != deployment.revision_id()
                {
                    continue;
                }
                let worker_id = allocation
                    .assigned_worker_id()
                    .filter(|_| allocation.state() == AllocationState::Assigned)
                    .cloned()
                    .ok_or(ExecutionStoreError::StorageUnavailable)?;
                let run_id =
                    RunId::new(raw_run_id).map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                let observation: RunObservation = serde_json::from_str(&observation_json)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                let run_spec: RunSpec = serde_json::from_str(&run_spec_json)
                    .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                let port = AssignedServicePort::new(
                    port_name,
                    match protocol.as_str() {
                        "tcp" => ServicePortProtocol::Tcp,
                        _ => return Err(ExecutionStoreError::StorageUnavailable),
                    },
                    u16::try_from(raw_port).map_err(|_| ExecutionStoreError::StorageUnavailable)?,
                    advertised_address,
                )
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
                if !run_spec.service_ports().contains(&port) {
                    return Err(ExecutionStoreError::StorageUnavailable);
                }
                let observation_matches_attempt = observation.run_id() == &run_id
                    && observation.job_id() == &job_id
                    && observation.worker_id() == &worker_id
                    && run_spec.id() == &run_id
                    && run_spec.worker_id() == &worker_id
                    && run_spec.job().id() == &job_id;
                let worker = workers
                    .iter()
                    .find(|worker| worker.worker_id() == &worker_id);
                let worker_fresh = worker.is_some_and(|worker| worker.is_valid() && worker.fresh());
                let worker_draining = worker.is_some_and(WorkerStatus::draining);
                let worker_available = worker.is_some_and(|worker| {
                    worker.is_valid()
                        && worker.available()
                        && !worker.degraded()
                        && worker.incarnation() == observation.worker_incarnation()
                });
                let readiness = observation.readiness();
                let eligible = observation_matches_attempt
                    && observation.state() == RunState::Running
                    && readiness == Some(ReadinessState::Ready)
                    && worker_fresh
                    && worker_available
                    && !worker_draining;
                endpoints.push(ServiceCatalogEndpoint {
                    job_id: job_id.clone(),
                    service_identity: identity.clone(),
                    service_instance_id: instance_id,
                    allocation_id,
                    run_id,
                    worker_id,
                    port,
                    readiness,
                    worker_fresh,
                    worker_draining,
                    eligible,
                });
            }
        }
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(endpoints)
    }

    fn service_identity_exists(
        &self,
        identity: &ServiceIdentity,
    ) -> Result<bool, ExecutionStoreError> {
        self.connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM service_catalog_owners
                     WHERE namespace = ?1 AND service_name = ?2
                 )",
                params![identity.namespace(), identity.name()],
                |row| row.get(0),
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)
    }

    fn request_worker_drain(
        &self,
        worker_id: &WorkerId,
        grace_period_ms: i64,
    ) -> Result<WorkerDrainRecord, ExecutionStoreError> {
        let requested_at_ms = now_ms().map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let deadline_at_ms = requested_at_ms.saturating_add(grace_period_ms);
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT OR IGNORE INTO worker_drains
                 (worker_id, requested_at_ms, deadline_at_ms, completed_at_ms)
                 VALUES (?1, ?2, ?3, NULL)",
                params![worker_id.as_str(), requested_at_ms, deadline_at_ms],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let record = load_worker_drain(&transaction, worker_id)?
            .ok_or(ExecutionStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(record)
    }

    fn get_worker_drain(
        &self,
        worker_id: &WorkerId,
    ) -> Result<Option<WorkerDrainRecord>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        load_worker_drain(&connection, worker_id)
    }

    fn list_worker_drains(&self) -> Result<Vec<WorkerDrainRecord>, ExecutionStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let mut statement = connection
            .prepare("SELECT worker_id, requested_at_ms, deadline_at_ms, completed_at_ms FROM worker_drains ORDER BY requested_at_ms, worker_id")
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map([], worker_drain_from_row)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        rows.map(|row| row.map_err(|_| ExecutionStoreError::StorageUnavailable))
            .collect()
    }

    fn complete_worker_drain(
        &self,
        worker_id: &WorkerId,
        completed_at_ms: i64,
    ) -> Result<(), ExecutionStoreError> {
        self.connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            .execute(
                "UPDATE worker_drains SET completed_at_ms = COALESCE(completed_at_ms, ?2)
                 WHERE worker_id = ?1",
                params![worker_id.as_str(), completed_at_ms],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        Ok(())
    }

    fn cancel_worker_drain(&self, worker_id: &WorkerId) -> Result<bool, ExecutionStoreError> {
        self.connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?
            .execute(
                "DELETE FROM worker_drains WHERE worker_id = ?1 AND completed_at_ms IS NOT NULL",
                [worker_id.as_str()],
            )
            .map(|changed| changed != 0)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)
    }

    fn begin_service_run(
        &self,
        job_id: &JobId,
        allocation_id: &AllocationId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        worker_capacity: usize,
        service_network: Option<&WorkerServiceNetwork>,
    ) -> Result<Option<RunAdmission>, RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let worker_draining: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM worker_drains WHERE worker_id = ?1)",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if worker_draining {
            return Err(RunStoreError::WorkerDraining);
        }
        let row = transaction
            .query_row(
                "SELECT si.instance_id, a.allocation_json, d.deployment_json
                 FROM service_instances si
                 JOIN allocations a ON a.allocation_id = si.current_allocation_id
                 JOIN job_deployments d ON d.job_id = si.job_id
                 WHERE si.job_id = ?1 AND si.current_allocation_id = ?2
                   AND a.worker_id = ?3 AND a.state = 'assigned' AND d.deleted_at_ms IS NULL",
                params![job_id.as_str(), allocation_id.as_str(), worker_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let Some((instance_raw, allocation_json, deployment_json)) = row else {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        };
        let instance =
            ServiceInstanceId::new(instance_raw).map_err(|_| RunStoreError::StorageUnavailable)?;
        let allocation: Allocation = serde_json::from_str(&allocation_json)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let deployment: JobDeployment = serde_json::from_str(&deployment_json)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if deployment.suspended()
            || deployment.desired_replicas() == 0
            || deployment.revision_id() != allocation.job_revision_id()
        {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        }
        transaction
            .execute(
                "INSERT OR IGNORE INTO service_attempts (service_instance_id, allocation_id) VALUES (?1, ?2)",
                params![instance.as_str(), allocation_id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE service_attempts SET allocation_id = ?1, restart_count = 0,
                     next_start_at_ms = 0, terminal_failure = 0
                 WHERE service_instance_id = ?2 AND current_run_id IS NULL
                   AND (allocation_id IS NULL OR allocation_id <> ?1)",
                params![allocation_id.as_str(), instance.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let eligible: bool = transaction
            .query_row(
                "SELECT current_run_id IS NULL AND terminal_failure = 0 AND next_start_at_ms <= ?1
                 FROM service_attempts WHERE service_instance_id = ?2",
                params![
                    now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
                    instance.as_str()
                ],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if !eligible {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        }
        let revision_number = i64::try_from(allocation.job_revision_id().revision())
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let revision_json: String = transaction
            .query_row(
                "SELECT revision_json FROM job_revisions WHERE job_id = ?1 AND revision = ?2",
                params![job_id.as_str(), revision_number],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let revision: JobRevision =
            serde_json::from_str(&revision_json).map_err(|_| RunStoreError::StorageUnavailable)?;
        let active_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM runs WHERE worker_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if active_count >= worker_capacity as i64 {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Err(RunStoreError::CapacityReached);
        }
        let pending_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM manager_commands WHERE worker_id = ?1 AND state = 'pending'",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if pending_count >= MAX_PENDING_NON_CONTROL_COMMANDS_PER_WORKER as i64 {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Err(RunStoreError::PendingDeliveryLimit);
        }
        transaction
            .execute("UPDATE run_sequence SET value = value + 1 WHERE id = 1", [])
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let sequence: i64 = transaction
            .query_row("SELECT value FROM run_sequence WHERE id = 1", [], |row| {
                row.get(0)
            })
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let run_id =
            RunId::new(format!("run-{sequence}")).map_err(|_| RunStoreError::StorageUnavailable)?;
        let (declared_ports, readiness_probe) = match revision.workload() {
            Workload::Service {
                ports,
                readiness_probe,
                ..
            } => (ports.as_slice(), readiness_probe.as_ref()),
            Workload::OneShot => (&[][..], None),
        };
        if !declared_ports.is_empty() && service_network.is_none() {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        }
        let assigned_ports = match service_network {
            Some(network) => lease_service_ports(
                &transaction,
                allocation_id,
                &run_id,
                sequence,
                network,
                declared_ports,
            )?,
            None => Vec::new(),
        };
        if !declared_ports.is_empty() && assigned_ports.is_empty() {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok(None);
        }
        let mut spec = RunSpec::new(run_id, worker_id.clone(), revision.definition().clone())
            .with_service_ports(assigned_ports)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if let Some(probe) = readiness_probe {
            spec = spec
                .with_readiness_probe(probe.clone())
                .map_err(|_| RunStoreError::StorageUnavailable)?;
        }
        let command_id =
            next_command_id(&transaction).map_err(|_| RunStoreError::StorageUnavailable)?;
        let envelope = CommandEnvelope {
            command_id,
            target_worker_id: worker_id.clone(),
            target_worker_incarnation: worker_incarnation,
            command: WorkerCommand::StartRun(Box::new(StartRun::new(spec.clone()))),
        };
        let observation = RunObservation::requested(
            &spec,
            now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
        );
        let spec_json =
            serde_json::to_string(&spec).map_err(|_| RunStoreError::StorageUnavailable)?;
        let observation_json =
            serde_json::to_string(&observation).map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO runs (run_id, job_id, worker_id, job_spec_json, observation_json, revision, state, idempotency_key, request_fingerprint, run_order)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8)",
                params![spec.id().as_str(), job_id.as_str(), worker_id.as_str(), spec_json, observation_json, observation.revision() as i64, observation.state().as_str(), sequence],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        insert_command(
            &transaction,
            &envelope,
            spec.id(),
            now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
        )
        .map_err(|_| RunStoreError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE service_attempts SET current_run_id = ?1, allocation_id = ?2
                 WHERE service_instance_id = ?3 AND current_run_id IS NULL AND terminal_failure = 0",
                params![spec.id().as_str(), allocation_id.as_str(), instance.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if changed != 1 {
            return Err(RunStoreError::JobConflict);
        }
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        Ok(Some(RunAdmission {
            spec,
            replay: None,
            command: Some(envelope),
        }))
    }

    fn finish_service_attempt(
        &self,
        instance_id: &ServiceInstanceId,
        run_id: &RunId,
        restart_count: u32,
        next_start_at_ms: i64,
        terminal_failure: bool,
    ) -> Result<(), RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE service_attempts SET current_run_id = NULL, restart_count = ?1,
                     next_start_at_ms = ?2, terminal_failure = ?3
                 WHERE service_instance_id = ?4 AND current_run_id = ?5",
                params![
                    i64::from(restart_count),
                    next_start_at_ms,
                    terminal_failure,
                    instance_id.as_str(),
                    run_id.as_str()
                ],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if changed == 0 {
            return Err(RunStoreError::JobConflict);
        }
        transaction
            .execute(
                "DELETE FROM service_port_leases WHERE run_id = ?1",
                [run_id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)
    }

    fn reject_unaccepted_service_run(
        &self,
        instance_id: &ServiceInstanceId,
        run_id: &RunId,
        next_start_at_ms: i64,
    ) -> Result<(), RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM manager_commands WHERE run_id = ?1 AND state = 'pending'",
                [run_id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let deleted = transaction
            .execute(
                "DELETE FROM runs WHERE run_id = ?1 AND state = 'requested'",
                [run_id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if deleted != 1 {
            return Err(RunStoreError::JobConflict);
        }
        let changed = transaction
            .execute(
                "UPDATE service_attempts SET current_run_id = NULL, next_start_at_ms = ?1
                 WHERE service_instance_id = ?2 AND current_run_id = ?3",
                params![next_start_at_ms, instance_id.as_str(), run_id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if changed != 1 {
            return Err(RunStoreError::JobConflict);
        }
        transaction
            .execute(
                "DELETE FROM service_port_leases WHERE run_id = ?1",
                [run_id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)
    }

    fn release_service_allocation(
        &self,
        allocation_id: &AllocationId,
    ) -> Result<(), ExecutionStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        let Some(mut record) = load_allocation(&transaction, allocation_id.as_str())? else {
            return Err(ExecutionStoreError::StorageUnavailable);
        };
        if record.allocation.state() != AllocationState::Released {
            record
                .allocation
                .release()
                .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
            persist_allocation(&transaction, &record)?;
        }
        transaction
            .execute(
                "DELETE FROM service_port_leases WHERE allocation_id = ?1",
                [allocation_id.as_str()],
            )
            .map_err(|_| ExecutionStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| ExecutionStoreError::StorageUnavailable)
    }

    fn create_run(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_capacity: usize,
        idempotency_key: Option<&str>,
    ) -> Result<RunAdmission, RunStoreError> {
        self.create_run_for_worker(job_id, worker_id, 0, worker_capacity, idempotency_key)
    }

    fn create_run_for_worker(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        worker_capacity: usize,
        idempotency_key: Option<&str>,
    ) -> Result<RunAdmission, RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if let Some(key) = idempotency_key {
            let existing: Option<(String, String, String, String, String)> = transaction
                .query_row(
                    "SELECT job_id, job_spec_json, observation_json, request_fingerprint, run_id FROM runs WHERE idempotency_key = ?1",
                    [key],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )
                .optional()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            if let Some((existing_job, spec_json, observation_json, fingerprint, existing_run_id)) =
                existing
            {
                let spec: RunSpec = serde_json::from_str(&spec_json)
                    .map_err(|_| RunStoreError::StorageUnavailable)?;
                let expected_fingerprint = serde_json::to_string(spec.job())
                    .map_err(|_| RunStoreError::StorageUnavailable)?;
                if existing_job != job_id.as_str() || fingerprint != expected_fingerprint {
                    return Err(RunStoreError::IdempotencyConflict);
                }
                let observation: RunObservation = serde_json::from_str(&observation_json)
                    .map_err(|_| RunStoreError::StorageUnavailable)?;
                let command = transaction
                    .query_row(
                        "SELECT envelope_json FROM manager_commands WHERE run_id = ?1 ORDER BY rowid LIMIT 1",
                        [existing_run_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()
                    .map_err(|_| RunStoreError::StorageUnavailable)?
                    .map(|json| serde_json::from_str(&json).map_err(|_| RunStoreError::StorageUnavailable))
                    .transpose()?;
                return Ok(RunAdmission {
                    spec,
                    replay: Some(observation),
                    command,
                });
            }
        }
        let worker_draining: bool = transaction
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM worker_drains WHERE worker_id = ?1)",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if worker_draining {
            return Err(RunStoreError::WorkerDraining);
        }
        let job_json = transaction
            .query_row(
                "SELECT spec_json FROM jobs WHERE id = ?1 AND deleted_at_ms IS NULL",
                [job_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?
            .ok_or(RunStoreError::JobNotFound)?;
        let job: JobSpec =
            serde_json::from_str(&job_json).map_err(|_| RunStoreError::StorageUnavailable)?;
        let existing_for_job: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM runs WHERE job_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown'))",
            [job_id.as_str()],
            |row| row.get(0),
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        if existing_for_job {
            return Err(RunStoreError::JobConflict);
        }
        let active_count: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM runs WHERE worker_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown')",
            [worker_id.as_str()],
            |row| row.get(0),
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        if active_count >= worker_capacity as i64 {
            return Err(RunStoreError::CapacityReached);
        }
        let pending_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM manager_commands WHERE worker_id = ?1 AND state = 'pending'",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        if pending_count >= MAX_PENDING_NON_CONTROL_COMMANDS_PER_WORKER as i64 {
            return Err(RunStoreError::PendingDeliveryLimit);
        }
        transaction
            .execute("UPDATE run_sequence SET value = value + 1 WHERE id = 1", [])
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let sequence: i64 = transaction
            .query_row("SELECT value FROM run_sequence WHERE id = 1", [], |row| {
                row.get(0)
            })
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let run_id =
            RunId::new(format!("run-{sequence}")).map_err(|_| RunStoreError::StorageUnavailable)?;
        let spec = RunSpec::new(run_id, worker_id.clone(), job);
        let command_id = next_command_id(&transaction)?;
        let envelope = CommandEnvelope {
            command_id,
            target_worker_id: worker_id.clone(),
            target_worker_incarnation: worker_incarnation,
            command: WorkerCommand::StartRun(Box::new(StartRun::new(spec.clone()))),
        };
        let observation = RunObservation::requested(
            &spec,
            now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
        );
        let spec_json =
            serde_json::to_string(&spec).map_err(|_| RunStoreError::StorageUnavailable)?;
        let fingerprint =
            serde_json::to_string(spec.job()).map_err(|_| RunStoreError::StorageUnavailable)?;
        let observation_json =
            serde_json::to_string(&observation).map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction.execute(
            "INSERT INTO runs (run_id, job_id, worker_id, job_spec_json, observation_json, revision, state, idempotency_key, request_fingerprint, run_order) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![spec.id().as_str(), job_id.as_str(), worker_id.as_str(), spec_json, observation_json, observation.revision() as i64, observation.state().as_str(), idempotency_key, idempotency_key.map(|_| fingerprint.as_str()), sequence],
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        insert_command(
            &transaction,
            &envelope,
            spec.id(),
            now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
        )
        .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        Ok(RunAdmission {
            spec,
            replay: None,
            command: Some(envelope),
        })
    }

    fn command_for_run(
        &self,
        run_id: &RunId,
        worker_id: &WorkerId,
        worker_incarnation: u64,
        command: WorkerCommand,
    ) -> Result<CommandEnvelope, CommandStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let run_worker: Option<String> = transaction
            .query_row(
                "SELECT worker_id FROM runs WHERE run_id = ?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        if run_worker.as_deref() != Some(worker_id.as_str()) {
            return Err(CommandStoreError::NotFound);
        }
        let incarnation =
            i64::try_from(worker_incarnation).map_err(|_| CommandStoreError::Conflict)?;
        let existing = {
            let mut statement = transaction
                .prepare(
                    "SELECT envelope_json FROM manager_commands
                     WHERE run_id = ?1 AND worker_id = ?2 AND worker_incarnation = ?3
                     ORDER BY rowid DESC",
                )
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            let rows = statement
                .query_map(
                    params![run_id.as_str(), worker_id.as_str(), incarnation],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            let mut matching = None;
            for row in rows {
                let envelope: CommandEnvelope =
                    serde_json::from_str(&row.map_err(|_| CommandStoreError::StorageUnavailable)?)
                        .map_err(|_| CommandStoreError::StorageUnavailable)?;
                if envelope.command == command {
                    matching = Some(envelope);
                    break;
                }
            }
            matching
        };
        if let Some(existing) = existing {
            transaction
                .commit()
                .map_err(|_| CommandStoreError::StorageUnavailable)?;
            return Ok(existing);
        }
        let pending_count: i64 = transaction
            .query_row(
                "SELECT COUNT(*) FROM manager_commands WHERE worker_id = ?1 AND state = 'pending'",
                [worker_id.as_str()],
                |row| row.get(0),
            )
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        let limit = if is_control_command(&command) {
            MAX_PENDING_COMMANDS_PER_WORKER
        } else {
            MAX_PENDING_NON_CONTROL_COMMANDS_PER_WORKER
        };
        if pending_count >= limit as i64 {
            return Err(CommandStoreError::PendingLimit);
        }
        let command_id =
            next_command_id(&transaction).map_err(|_| CommandStoreError::StorageUnavailable)?;
        let envelope = CommandEnvelope {
            command_id,
            target_worker_id: worker_id.clone(),
            target_worker_incarnation: worker_incarnation,
            command,
        };
        let created_at_ms = now_ms().map_err(|_| CommandStoreError::StorageUnavailable)?;
        insert_command(&transaction, &envelope, run_id, created_at_ms)
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| CommandStoreError::StorageUnavailable)?;
        Ok(envelope)
    }

    fn persist_stop_intent(
        &self,
        run_id: &RunId,
        worker_incarnation: u64,
    ) -> Result<(RunObservation, Option<CommandEnvelope>), RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let stored_json: Option<String> = transaction
            .query_row(
                "SELECT observation_json FROM runs WHERE run_id = ?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let stored_json = stored_json.ok_or(RunStoreError::RunNotFound)?;
        let current: RunObservation =
            serde_json::from_str(&stored_json).map_err(|_| RunStoreError::StorageUnavailable)?;
        if current.state().is_terminal() {
            transaction
                .commit()
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            return Ok((current, None));
        }

        let stopping = if current.state() == RunState::Stopping {
            current.clone()
        } else {
            current
                .next_revision(
                    RunState::Stopping,
                    current.started_at_ms(),
                    None,
                    None,
                    None,
                    None,
                )
                .ok_or(RunStoreError::StorageUnavailable)?
                .with_termination_reason(RunTerminationReason::Cancelled)
        };
        if stopping != current {
            let serialized =
                serde_json::to_string(&stopping).map_err(|_| RunStoreError::StorageUnavailable)?;
            let changed = transaction
                .execute(
                    "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3
                     WHERE run_id = ?4 AND revision = ?5
                       AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
                    params![
                        serialized,
                        stopping.revision() as i64,
                        stopping.state().as_str(),
                        run_id.as_str(),
                        stopping.revision().saturating_sub(1) as i64,
                    ],
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            if changed != 1 {
                return Err(RunStoreError::StorageUnavailable);
            }
        }

        let worker_incarnation =
            i64::try_from(worker_incarnation).map_err(|_| RunStoreError::StorageUnavailable)?;
        let worker_id = stopping.worker_id();
        let command = WorkerCommand::StopRun(StopRun::new(run_id.clone()));
        let existing = {
            let mut statement = transaction
                .prepare(
                    "SELECT envelope_json FROM manager_commands
                     WHERE run_id = ?1 AND worker_id = ?2 AND worker_incarnation = ?3
                     ORDER BY rowid DESC",
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            let rows = statement
                .query_map(
                    params![run_id.as_str(), worker_id.as_str(), worker_incarnation],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            let mut matching = None;
            for row in rows {
                let envelope: CommandEnvelope =
                    serde_json::from_str(&row.map_err(|_| RunStoreError::StorageUnavailable)?)
                        .map_err(|_| RunStoreError::StorageUnavailable)?;
                if envelope.command == command {
                    matching = Some(envelope);
                    break;
                }
            }
            matching
        };
        let envelope = if let Some(existing) = existing {
            existing
        } else {
            let pending_count: i64 = transaction
                .query_row(
                    "SELECT COUNT(*) FROM manager_commands WHERE worker_id = ?1 AND state = 'pending'",
                    [worker_id.as_str()],
                    |row| row.get(0),
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            if pending_count >= MAX_PENDING_COMMANDS_PER_WORKER as i64 {
                return Err(RunStoreError::PendingDeliveryLimit);
            }
            let envelope = CommandEnvelope {
                command_id: next_command_id(&transaction)?,
                target_worker_id: worker_id.clone(),
                target_worker_incarnation: worker_incarnation as u64,
                command,
            };
            insert_command(
                &transaction,
                &envelope,
                run_id,
                now_ms().map_err(|_| RunStoreError::StorageUnavailable)?,
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
            envelope
        };
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        Ok((stopping, Some(envelope)))
    }

    fn get_idempotent_run(
        &self,
        key: &str,
    ) -> Result<Option<(JobId, RunObservation)>, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let existing = connection
            .query_row(
                "SELECT job_id, observation_json FROM runs WHERE idempotency_key = ?1",
                [key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        existing
            .map(|(job_id, observation)| {
                let job_id = JobId::new(job_id).map_err(|_| RunStoreError::StorageUnavailable)?;
                let observation = serde_json::from_str(&observation)
                    .map_err(|_| RunStoreError::StorageUnavailable)?;
                Ok((job_id, observation))
            })
            .transpose()
    }

    fn delete_unaccepted_run(&self, id: &RunId) -> Result<(), RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM manager_commands WHERE run_id = ?1 AND state = 'pending'",
                [id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM runs WHERE run_id = ?1 AND state = 'requested'",
                [id.as_str()],
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)
    }

    fn get_run(&self, id: &RunId) -> Result<Option<RunObservation>, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let json = connection
            .query_row(
                "SELECT observation_json FROM runs WHERE run_id = ?1",
                [id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        json.map(|value| {
            serde_json::from_str(&value).map_err(|_| RunStoreError::StorageUnavailable)
        })
        .transpose()
    }

    fn list_active_runs(
        &self,
        worker_id: &WorkerId,
        limit: usize,
    ) -> Result<Vec<RunObservation>, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let mut statement = connection.prepare(
            "SELECT observation_json FROM runs WHERE worker_id = ?1 AND state IN ('requested', 'starting', 'running', 'stopping', 'unknown') ORDER BY rowid DESC LIMIT ?2",
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map(params![worker_id.as_str(), limit as i64], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|_| RunStoreError::StorageUnavailable)?)
                .map_err(|_| RunStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn list_runs(
        &self,
        job_id: Option<&JobId>,
        state_filter: Option<RunState>,
        limit: usize,
    ) -> Result<Vec<RunObservation>, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let mut statement = connection.prepare(
            "SELECT observation_json FROM runs WHERE (?1 IS NULL OR job_id = ?1) AND (?2 IS NULL OR state = ?2) ORDER BY rowid DESC LIMIT ?3",
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        let rows = statement
            .query_map(
                params![
                    job_id.map(JobId::as_str),
                    state_filter.map(RunState::as_str),
                    limit as i64
                ],
                |row| row.get::<_, String>(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        rows.map(|row| {
            serde_json::from_str(&row.map_err(|_| RunStoreError::StorageUnavailable)?)
                .map_err(|_| RunStoreError::StorageUnavailable)
        })
        .collect()
    }

    fn record_observation(&self, observation: &RunObservation) -> Result<(), RunStoreError> {
        let serialized =
            serde_json::to_string(observation).map_err(|_| RunStoreError::StorageUnavailable)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let changed = connection.execute(
            "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4 AND revision < ?2 AND state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
            params![serialized, observation.revision() as i64, observation.state().as_str(), observation.run_id().as_str()],
        ).map_err(|_| RunStoreError::StorageUnavailable)?;
        drop(connection);
        if changed == 1
            && observation.state().is_terminal()
            && self.prune_completed_run_history().is_err()
        {
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"warning\",\"component\":\"manager\",\"event\":\"run_history_retention_failed\"}}",
                now_ms().unwrap_or_default(),
            );
        }
        Ok(())
    }
}

impl SqliteJobRepository {
    /// Inspect an existing manager database without migrating or changing its contents.
    /// SQLite may create its WAL coordination sidecar files during the read-only connection.
    pub fn inspect_read_only(
        path: impl AsRef<Path>,
        completed_run_retention: usize,
    ) -> Result<HistoryRetentionStatus, RepositoryOpenError> {
        if !(MIN_COMPLETED_RUN_RETENTION..=MAX_COMPLETED_RUN_RETENTION)
            .contains(&completed_run_retention)
        {
            return Err(RepositoryOpenError::InvalidRetentionPolicy);
        }
        let metadata = fs::symlink_metadata(path.as_ref()).map_err(RepositoryOpenError::Io)?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(RepositoryOpenError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "manager database path must be a regular file",
            )));
        }
        for suffix in ["-wal", "-shm"] {
            let mut sidecar_path = path.as_ref().as_os_str().to_owned();
            sidecar_path.push(suffix);
            match fs::symlink_metadata(Path::new(&sidecar_path)) {
                Ok(sidecar_metadata)
                    if sidecar_metadata.file_type().is_symlink() || !sidecar_metadata.is_file() =>
                {
                    return Err(RepositoryOpenError::Io(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "manager SQLite sidecar path must be a regular file",
                    )));
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(RepositoryOpenError::Io(error)),
            }
        }
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(RepositoryOpenError::Sqlite)?;
        connection
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .map_err(RepositoryOpenError::Sqlite)?;
        let version = connection
            .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
            .map_err(RepositoryOpenError::Sqlite)?;
        if version != SCHEMA_VERSION {
            return Err(RepositoryOpenError::InvalidLegacySchema);
        }
        connection
            .execute_batch("BEGIN DEFERRED")
            .map_err(RepositoryOpenError::Sqlite)?;
        let repository = Self {
            connection: Mutex::new(connection),
            completed_run_retention,
        };
        let status = repository
            .history_retention_status()
            .map_err(|_| RepositoryOpenError::Sqlite(rusqlite::Error::InvalidQuery))?;
        repository
            .connection
            .lock()
            .map_err(|_| RepositoryOpenError::Sqlite(rusqlite::Error::InvalidQuery))?
            .execute_batch("COMMIT")
            .map_err(RepositoryOpenError::Sqlite)?;
        Ok(status)
    }

    /// Open or create the manager database, enable durable local settings, and migrate it.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RepositoryOpenError> {
        Self::open_with_retention(path, DEFAULT_COMPLETED_RUN_RETENTION)
    }

    /// Open or create the manager database with a bounded completed-run history policy.
    pub fn open_with_retention(
        path: impl AsRef<Path>,
        completed_run_retention: usize,
    ) -> Result<Self, RepositoryOpenError> {
        if !(MIN_COMPLETED_RUN_RETENTION..=MAX_COMPLETED_RUN_RETENTION)
            .contains(&completed_run_retention)
        {
            return Err(RepositoryOpenError::InvalidRetentionPolicy);
        }
        prepare_database_file(path.as_ref()).map_err(RepositoryOpenError::Io)?;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(RepositoryOpenError::Sqlite)?;
        connection
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .map_err(RepositoryOpenError::Sqlite)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(RepositoryOpenError::Sqlite)?;

        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(RepositoryOpenError::Sqlite)?;
        if version > SCHEMA_VERSION {
            return Err(RepositoryOpenError::UnsupportedSchemaVersion(version));
        }

        let journal_mode: String = connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(RepositoryOpenError::Sqlite)?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(RepositoryOpenError::Sqlite)?;
            let mode: String = connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(RepositoryOpenError::Sqlite)?;
            if !mode.eq_ignore_ascii_case("wal") {
                return Err(RepositoryOpenError::Sqlite(rusqlite::Error::InvalidQuery));
            }
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(RepositoryOpenError::Sqlite)?;
        migrate(&connection).map_err(|error| match error {
            MigrationError::Sqlite(error) => RepositoryOpenError::Sqlite(error),
            MigrationError::InvalidLegacySchema => RepositoryOpenError::InvalidLegacySchema,
        })?;

        Ok(Self {
            connection: Mutex::new(connection),
            completed_run_retention,
        })
    }

    /// Remove one bounded batch of eligible completed-run history.
    ///
    /// Runs with idempotency keys, unresolved delivery, or live service references
    /// remain stored. The run sequence remains monotonic, so removed run IDs are
    /// still permanently reserved.
    pub fn prune_completed_run_history(&self) -> Result<HistoryPruneProgress, RunStoreError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let eligible_count = transaction
            .query_row(
                "SELECT COUNT(*) FROM runs AS r
                 WHERE r.state IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')
                   AND r.idempotency_key IS NULL
                   AND NOT EXISTS (
                       SELECT 1 FROM manager_commands AS c
                       WHERE c.run_id = r.run_id AND c.state = 'pending'
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM service_attempts AS a WHERE a.current_run_id = r.run_id
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM service_port_leases AS l WHERE l.run_id = r.run_id
                   )",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let eligible_runs =
            usize::try_from(eligible_count).map_err(|_| RunStoreError::StorageUnavailable)?;
        let candidates = {
            let mut statement = transaction
                .prepare(
                    "SELECT r.run_id FROM runs AS r
                     WHERE r.state IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')
                       AND r.idempotency_key IS NULL
                       AND NOT EXISTS (
                           SELECT 1 FROM manager_commands AS c
                           WHERE c.run_id = r.run_id AND c.state = 'pending'
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM service_attempts AS a WHERE a.current_run_id = r.run_id
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM service_port_leases AS l WHERE l.run_id = r.run_id
                       )
                     ORDER BY r.run_order DESC
                     LIMIT ?1 OFFSET ?2",
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            statement
                .query_map(
                    params![
                        i64::try_from(MAX_HISTORY_CLEANUP_BATCH + 1)
                            .map_err(|_| RunStoreError::StorageUnavailable)?,
                        i64::try_from(self.completed_run_retention)
                            .map_err(|_| RunStoreError::StorageUnavailable)?
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| RunStoreError::StorageUnavailable)?
        };
        let more_eligible = candidates.len() > MAX_HISTORY_CLEANUP_BATCH;
        let run_ids = candidates
            .into_iter()
            .take(MAX_HISTORY_CLEANUP_BATCH)
            .collect::<Vec<_>>();
        for run_id in &run_ids {
            transaction
                .execute(
                    "DELETE FROM manager_commands WHERE run_id = ?1 AND state = 'acknowledged'",
                    [run_id],
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            let removed = transaction
                .execute(
                    "DELETE FROM runs WHERE run_id = ?1
                       AND state IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')
                       AND idempotency_key IS NULL
                       AND NOT EXISTS (
                           SELECT 1 FROM manager_commands AS c
                           WHERE c.run_id = runs.run_id AND c.state = 'pending'
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM service_attempts AS a WHERE a.current_run_id = runs.run_id
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM service_port_leases AS l WHERE l.run_id = runs.run_id
                       )",
                    [run_id],
                )
                .map_err(|_| RunStoreError::StorageUnavailable)?;
            if removed != 1 {
                return Err(RunStoreError::StorageUnavailable);
            }
        }
        transaction
            .commit()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let progress = HistoryPruneProgress {
            eligible_runs,
            removed_runs: run_ids.len(),
            more_eligible,
        };
        if progress.removed_runs > 0 {
            eprintln!(
                "{{\"timestampMs\":{},\"severity\":\"info\",\"component\":\"manager\",\"event\":\"run_history_retention_progress\",\"eligibleRuns\":{},\"removedRuns\":{},\"retentionRuns\":{},\"moreEligible\":{}}}",
                now_ms().unwrap_or_default(),
                progress.eligible_runs,
                progress.removed_runs,
                self.completed_run_retention,
                progress.more_eligible,
            );
        }
        Ok(progress)
    }

    /// Return bounded, nonsensitive counts and SQLite page-reclamation measurements.
    pub fn history_retention_status(&self) -> Result<HistoryRetentionStatus, RunStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| RunStoreError::StorageUnavailable)?;
        let count = |sql: &str| -> Result<u64, RunStoreError> {
            connection
                .query_row(sql, [], |row| row.get::<_, i64>(0))
                .map_err(|_| RunStoreError::StorageUnavailable)
                .and_then(|value| {
                    u64::try_from(value).map_err(|_| RunStoreError::StorageUnavailable)
                })
        };
        let page_count = |name: &str| -> Result<u64, RunStoreError> {
            connection
                .pragma_query_value(None, name, |row| row.get::<_, i64>(0))
                .map_err(|_| RunStoreError::StorageUnavailable)
                .and_then(|value| {
                    u64::try_from(value).map_err(|_| RunStoreError::StorageUnavailable)
                })
        };
        let eligible_runs = count(
            "SELECT COUNT(*) FROM runs AS r
             WHERE r.state IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')
               AND r.idempotency_key IS NULL
               AND NOT EXISTS (SELECT 1 FROM manager_commands AS c WHERE c.run_id = r.run_id AND c.state = 'pending')
               AND NOT EXISTS (SELECT 1 FROM service_attempts AS a WHERE a.current_run_id = r.run_id)
               AND NOT EXISTS (SELECT 1 FROM service_port_leases AS l WHERE l.run_id = r.run_id)",
        )?;
        Ok(HistoryRetentionStatus {
            completed_runs: count(
                "SELECT COUNT(*) FROM runs
                 WHERE state IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
            )?,
            active_or_unresolved_runs: count(
                "SELECT COUNT(*) FROM runs
                 WHERE state NOT IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')",
            )?,
            eligible_runs,
            idempotency_protected_runs: count(
                "SELECT COUNT(*) FROM runs
                 WHERE state IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'abandoned')
                   AND idempotency_key IS NOT NULL",
            )?,
            pending_commands: count(
                "SELECT COUNT(*) FROM manager_commands WHERE state = 'pending'",
            )?,
            current_service_attempts: count(
                "SELECT COUNT(*) FROM service_attempts WHERE current_run_id IS NOT NULL",
            )?,
            service_port_leases: count("SELECT COUNT(*) FROM service_port_leases")?,
            manager_commands: count("SELECT COUNT(*) FROM manager_commands")?,
            audit_events: count("SELECT COUNT(*) FROM audit_events")?,
            execution_idempotency_records: count("SELECT COUNT(*) FROM execution_idempotency")?,
            eligible_runs_to_prune: eligible_runs
                .saturating_sub(self.completed_run_retention as u64),
            sqlite_page_count: page_count("page_count")?,
            sqlite_freelist_pages: page_count("freelist_count")?,
            sqlite_page_size_bytes: page_count("page_size")?,
            completed_run_retention: self.completed_run_retention,
            cleanup_batch_limit: MAX_HISTORY_CLEANUP_BATCH,
        })
    }

    /// Persist a hashed, one-time enrollment credential. The raw credential is never stored.
    pub fn create_enrollment_credential(
        &self,
        token_hash: &[u8; 32],
        worker_id: &WorkerId,
        expires_at_ms: i64,
    ) -> Result<(), WorkerIdentityError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let transaction = connection
            .unchecked_transaction()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO enrollment_credentials(token_hash, worker_id, expires_at_ms) VALUES (?1, ?2, ?3)",
                params![token_hash.as_slice(), worker_id.as_str(), expires_at_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)
            .and_then(|_| {
                transaction
                    .execute(
                        "INSERT INTO audit_events(occurred_at_ms, principal, action, resource_type, resource_id, outcome, status_code)
                         VALUES (?1, 'local_operator', 'worker.enrollment_credential.create', 'worker', ?2, 'success', 201)",
                        params![now_ms().map_err(|_| WorkerIdentityError::StorageUnavailable)?, worker_id.as_str()],
                    )
                    .map_err(|_| WorkerIdentityError::StorageUnavailable)
            })?;
        transaction
            .commit()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)
    }

    pub fn worker_for_enrollment_credential(
        &self,
        token_hash: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<WorkerId>, WorkerIdentityError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let worker_id = connection
            .query_row(
                "SELECT worker_id FROM enrollment_credentials WHERE token_hash = ?1 AND consumed_at_ms IS NULL AND expires_at_ms > ?2",
                params![token_hash.as_slice(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        worker_id
            .map(WorkerId::new)
            .transpose()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)
    }

    pub fn worker_for_active_certificate(
        &self,
        fingerprint: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<WorkerId>, WorkerIdentityError> {
        self.authenticate_worker_certificate(fingerprint, now_ms)
    }

    /// Atomically consume a valid enrollment credential and install the first certificate.
    pub fn enroll_worker_identity(
        &self,
        token_hash: &[u8; 32],
        fingerprint: &[u8; 32],
        serial: &str,
        not_before_ms: i64,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<WorkerId, WorkerIdentityError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let credential = transaction
            .query_row(
                "SELECT worker_id FROM enrollment_credentials WHERE token_hash = ?1 AND consumed_at_ms IS NULL AND expires_at_ms > ?2",
                params![token_hash.as_slice(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?
            .ok_or(WorkerIdentityError::CredentialInvalid)?;
        let worker_id =
            WorkerId::new(credential).map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let existing = transaction
            .query_row(
                "SELECT 1 FROM worker_identities WHERE worker_id = ?1 AND revoked_at_ms IS NULL",
                [worker_id.as_str()],
                |_| Ok(()),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        if existing.is_some() {
            return Err(WorkerIdentityError::IdentityExists);
        }
        transaction
            .execute(
                "UPDATE enrollment_credentials SET consumed_at_ms = ?2 WHERE token_hash = ?1 AND consumed_at_ms IS NULL",
                params![token_hash.as_slice(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO worker_certificates(fingerprint, worker_id, serial, not_before_ms, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![fingerprint.as_slice(), worker_id.as_str(), serial, not_before_ms, expires_at_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO worker_identities(worker_id, active_fingerprint, created_at_ms) VALUES (?1, ?2, ?3) ON CONFLICT(worker_id) DO UPDATE SET active_fingerprint = excluded.active_fingerprint, created_at_ms = excluded.created_at_ms, revoked_at_ms = NULL",
                params![worker_id.as_str(), fingerprint.as_slice(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO audit_events(occurred_at_ms, principal, action, resource_type, resource_id, outcome, status_code)
                 VALUES (?1, 'worker_enrollment', 'worker.enroll', 'worker', ?2, 'success', 201)",
                params![now_ms, worker_id.as_str()],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        Ok(worker_id)
    }

    /// Replace the active certificate only when the presenting certificate is current.
    pub fn renew_worker_identity(
        &self,
        current_fingerprint: &[u8; 32],
        replacement_fingerprint: &[u8; 32],
        serial: &str,
        not_before_ms: i64,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> Result<WorkerId, WorkerIdentityError> {
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let worker_id = transaction
            .query_row(
                "SELECT i.worker_id FROM worker_identities i JOIN worker_certificates c ON c.fingerprint = i.active_fingerprint WHERE i.active_fingerprint = ?1 AND i.revoked_at_ms IS NULL AND c.revoked_at_ms IS NULL AND c.expires_at_ms > ?2",
                params![current_fingerprint.as_slice(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?
            .ok_or(WorkerIdentityError::IdentityUnavailable)?;
        transaction
            .execute(
                "INSERT INTO worker_certificates(fingerprint, worker_id, serial, not_before_ms, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![replacement_fingerprint.as_slice(), worker_id, serial, not_before_ms, expires_at_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE worker_certificates SET revoked_at_ms = ?2 WHERE fingerprint = ?1",
                params![current_fingerprint.as_slice(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE worker_identities SET active_fingerprint = ?2 WHERE worker_id = ?1",
                params![worker_id, replacement_fingerprint.as_slice()],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "INSERT INTO audit_events(occurred_at_ms, principal, action, resource_type, resource_id, outcome, status_code)
                 VALUES (?1, 'worker_enrollment', 'worker.certificate.renew', 'worker', ?2, 'success', 200)",
                params![now_ms, worker_id],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        WorkerId::new(worker_id).map_err(|_| WorkerIdentityError::StorageUnavailable)
    }

    /// Revoke an identity and all of its certificates immediately.
    pub fn revoke_worker_identity(
        &self,
        worker_id: &WorkerId,
        now_ms: i64,
    ) -> Result<bool, WorkerIdentityError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let transaction = connection
            .unchecked_transaction()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let changed = transaction
            .execute(
                "UPDATE worker_identities SET revoked_at_ms = COALESCE(revoked_at_ms, ?2) WHERE worker_id = ?1",
                params![worker_id.as_str(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "UPDATE worker_certificates SET revoked_at_ms = COALESCE(revoked_at_ms, ?2) WHERE worker_id = ?1",
                params![worker_id.as_str(), now_ms],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .execute(
                "DELETE FROM enrollment_credentials WHERE worker_id = ?1 AND consumed_at_ms IS NULL",
                [worker_id.as_str()],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let mut statement = transaction
            .prepare(
                "SELECT network_domain_id, endpoint_id, peer_generation, expires_at_ms
                 FROM overlay_peers WHERE worker_id = ?1 AND state = 'active'",
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let rows = statement
            .query_map([worker_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let peers = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        drop(statement);
        let mut affected_domains = BTreeSet::new();
        for (domain, endpoint, generation, expires_at_ms) in peers {
            transaction
                .execute(
                    "INSERT OR IGNORE INTO overlay_endpoint_history
                     (network_domain_id, endpoint_id, worker_id, peer_generation, retired_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![domain, endpoint, worker_id.as_str(), generation, now_ms],
                )
                .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
            transaction
                .execute(
                    "UPDATE overlay_peers SET state = 'quarantined', quarantine_until_ms = ?1
                     WHERE network_domain_id = ?2 AND worker_id = ?3 AND state = 'active'",
                    params![
                        expires_at_ms
                            .max(now_ms)
                            .saturating_add(OVERLAY_CLOCK_SKEW_MS),
                        domain,
                        worker_id.as_str()
                    ],
                )
                .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
            transaction
                .execute(
                    "DELETE FROM overlay_peer_observations WHERE network_domain_id = ?1 AND worker_id = ?2",
                    params![domain, worker_id.as_str()],
                )
                .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
            affected_domains.insert(domain);
        }
        for domain in affected_domains {
            let generation: i64 = transaction
                .query_row(
                    "SELECT network_generation FROM overlay_network_domains WHERE network_domain_id = ?1",
                    [&domain],
                    |row| row.get(0),
                )
                .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
            let next = generation
                .checked_add(1)
                .filter(|next| *next > 0)
                .ok_or(WorkerIdentityError::StorageUnavailable)?;
            transaction
                .execute(
                    "UPDATE overlay_network_domains SET network_generation = ?1 WHERE network_domain_id = ?2",
                    params![next, domain],
                )
                .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        }
        transaction
            .execute(
                "INSERT INTO audit_events(occurred_at_ms, principal, action, resource_type, resource_id, outcome, status_code)
                 VALUES (?1, 'local_operator', 'worker.revoke', 'worker', ?2, 'success', 200)",
                params![now_ms, worker_id.as_str()],
            )
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        transaction
            .commit()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        Ok(changed != 0)
    }

    /// Resolve a presented certificate fingerprint only while it is active and unexpired.
    pub fn authenticate_worker_certificate(
        &self,
        fingerprint: &[u8; 32],
        now_ms: i64,
    ) -> Result<Option<WorkerId>, WorkerIdentityError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        let worker_id = connection
            .query_row(
                "SELECT i.worker_id FROM worker_identities i JOIN worker_certificates c ON c.fingerprint = i.active_fingerprint WHERE c.fingerprint = ?1 AND c.expires_at_ms > ?2 AND c.revoked_at_ms IS NULL AND i.revoked_at_ms IS NULL",
                params![fingerprint.as_slice(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)?;
        worker_id
            .map(WorkerId::new)
            .transpose()
            .map_err(|_| WorkerIdentityError::StorageUnavailable)
    }
}
