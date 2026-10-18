use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use conductor_core::{
    AllocationState, Architecture, CommandEnvelope, CommandId, DeploymentMutation,
    DeploymentPendingReason, DeploymentStatus, EnforcementCapability, ExecutionBackend, InspectRun,
    JobId, JobRevisionDraft, JobSpec, LogStream, OperatingSystem, ReadRunLogs, ReadinessState,
    ResolveRun, RunFailureReason, RunId, RunLogChunk, RunLogStatus, RunObservation, RunState,
    StartRun, StopRun, WorkerCapabilities, WorkerCommand, WorkerDescriptor, WorkerId,
    WorkerInventory, WorkerResourceCapacity, WorkerServiceNetwork, WorkerStatus,
};
use conductor_manager::{
    CommandJournal, CommandStoreError, ExecutionStoreError, JobRepository, JobService, MAX_JOBS,
    ManagerError, RepositoryOpenError, RunStoreError, SqliteJobRepository, WorkerIdentityError,
    WorkerPort, WorkerPortError,
};
use rusqlite::Connection;

struct TempDir(PathBuf);

static TEMP_DIR_SEQUENCE: AtomicU64 = AtomicU64::new(0);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "conductor-manager-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create temporary manager directory");
        Self(path)
    }

    fn database(&self) -> PathBuf {
        self.0.join("manager.sqlite3")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn job(id: &str) -> JobSpec {
    JobSpec::new(
        JobId::new(id).expect("valid ID"),
        vec!["/usr/bin/true".to_owned()],
        "/tmp".into(),
        BTreeMap::new(),
        false,
        None,
        5_000,
    )
    .expect("valid spec")
}

#[test]
fn worker_identity_enrollment_is_one_time_and_survives_reopen() {
    let path = TempDir::new();
    let worker_id = WorkerId::new("worker-a").expect("valid worker id");
    let store = SqliteJobRepository::open(path.database()).expect("open manager store");
    let token_hash = [1; 32];
    let first_fingerprint = [2; 32];
    let replacement_fingerprint = [3; 32];

    store
        .create_enrollment_credential(&token_hash, &worker_id, 10_000)
        .expect("store hashed credential");
    assert_eq!(
        store.enroll_worker_identity(&[9; 32], &first_fingerprint, "02", 1_000, 50_000, 2_000),
        Err(WorkerIdentityError::CredentialInvalid)
    );
    assert_eq!(
        store
            .enroll_worker_identity(&token_hash, &first_fingerprint, "02", 1_000, 50_000, 2_000)
            .expect("consume enrollment token"),
        worker_id
    );
    assert_eq!(
        store.authenticate_worker_certificate(&first_fingerprint, 2_000),
        Ok(Some(worker_id.clone()))
    );
    assert_eq!(
        store.authenticate_worker_certificate(&first_fingerprint, 50_000),
        Ok(None)
    );
    assert_eq!(
        store.renew_worker_identity(&first_fingerprint, &[9; 32], "09", 50_000, 80_000, 50_000),
        Err(WorkerIdentityError::IdentityUnavailable)
    );
    assert_eq!(
        store.enroll_worker_identity(&token_hash, &[4; 32], "04", 1_000, 50_000, 2_000),
        Err(WorkerIdentityError::CredentialInvalid)
    );

    drop(store);
    let store = SqliteJobRepository::open(path.database()).expect("reopen manager store");
    assert_eq!(
        store
            .renew_worker_identity(
                &first_fingerprint,
                &replacement_fingerprint,
                "03",
                2_500,
                60_000,
                2_500
            )
            .expect("renew using current certificate"),
        worker_id
    );
    assert_eq!(
        store.authenticate_worker_certificate(&first_fingerprint, 3_000),
        Ok(None)
    );
    assert_eq!(
        store.authenticate_worker_certificate(&replacement_fingerprint, 3_000),
        Ok(Some(WorkerId::new("worker-a").expect("valid worker id")))
    );
    assert!(
        store
            .revoke_worker_identity(&WorkerId::new("worker-a").expect("valid worker id"), 4_000)
            .expect("revoke identity")
    );
    assert_eq!(
        store.authenticate_worker_certificate(&replacement_fingerprint, 4_000),
        Ok(None)
    );
    let reenrollment_hash = [7; 32];
    let new_fingerprint = [8; 32];
    store
        .create_enrollment_credential(&reenrollment_hash, &worker_id, 20_000)
        .expect("create replacement enrollment credential");
    assert_eq!(
        store
            .enroll_worker_identity(
                &reenrollment_hash,
                &new_fingerprint,
                "08",
                5_000,
                70_000,
                5_000
            )
            .expect("reenroll after explicit revocation"),
        worker_id
    );
    assert_eq!(
        store.authenticate_worker_certificate(&new_fingerprint, 5_000),
        Ok(Some(WorkerId::new("worker-a").expect("valid worker id")))
    );
}

#[test]
fn expired_worker_enrollment_credential_is_rejected() {
    let path = TempDir::new();
    let store = SqliteJobRepository::open(path.database()).expect("open manager store");
    let worker_id = WorkerId::new("worker-expired").expect("valid worker id");
    let token_hash = [5; 32];
    store
        .create_enrollment_credential(&token_hash, &worker_id, 2_000)
        .expect("store hashed credential");
    assert_eq!(
        store.enroll_worker_identity(&token_hash, &[6; 32], "06", 1_000, 50_000, 2_000),
        Err(WorkerIdentityError::CredentialInvalid)
    );
}

fn service(path: &TempDir) -> JobService {
    JobService::open_sqlite(path.database()).expect("open manager database")
}

fn service_revision_draft(command_arg: &str) -> JobRevisionDraft {
    let json = format!(
        r#"{{
            "definition": {{
                "id": "web",
                "command": ["/usr/bin/true", "{command_arg}"],
                "cwd": "/tmp",
                "terminationGraceMs": 5000
            }},
            "workload": {{
                "kind": "service",
                "identity": {{"namespace": "default", "name": "web"}}
            }},
            "executableDigest": {{"algorithm": "sha256", "value": "{}"}},
            "executionRequirements": {{}},
            "resourceRequest": {{"cpuMillis": 250, "memoryBytes": 268435456}}
        }}"#,
        "a".repeat(64)
    );
    serde_json::from_str(&json).expect("valid service revision draft")
}

fn service_revision_with_ports(command_arg: &str) -> JobRevisionDraft {
    let json = format!(
        r#"{{
            "definition": {{
                "id": "web",
                "command": ["/usr/bin/true", "{command_arg}"],
                "cwd": "/tmp",
                "terminationGraceMs": 5000
            }},
            "workload": {{
                "kind": "service",
                "identity": {{"namespace": "default", "name": "web"}},
                "ports": [{{"name": "http"}}]
            }},
            "executableDigest": {{"algorithm": "sha256", "value": "{}"}},
            "executionRequirements": {{}},
            "resourceRequest": {{"cpuMillis": 250, "memoryBytes": 268435456}}
        }}"#,
        "a".repeat(64)
    );
    serde_json::from_str(&json).expect("valid service revision with named port")
}

fn service_revision_with_probe(command_arg: &str) -> JobRevisionDraft {
    let mut value = serde_json::to_value(service_revision_with_ports(command_arg)).unwrap();
    value["workload"]["readinessProbe"] = serde_json::json!({
        "portName": "http",
        "protocol": "tcp",
        "intervalMs": 100,
        "timeoutMs": 50,
        "startupGraceMs": 0,
        "successThreshold": 2,
        "failureThreshold": 3
    });
    serde_json::from_value(value).expect("valid service readiness probe revision")
}

#[allow(clippy::too_many_arguments)]
fn scheduling_worker(
    worker_id: &str,
    domain_id: &str,
    worker_cpu_millis: u32,
    worker_memory_bytes: u64,
    domain_cpu_millis: u32,
    domain_memory_bytes: u64,
    labels: BTreeMap<String, String>,
    fresh: bool,
) -> WorkerStatus {
    scheduling_worker_with_capabilities(
        worker_id,
        domain_id,
        worker_cpu_millis,
        worker_memory_bytes,
        domain_cpu_millis,
        domain_memory_bytes,
        labels,
        fresh,
        OperatingSystem::Linux,
        BTreeSet::from([
            EnforcementCapability::CgroupV2,
            EnforcementCapability::Landlock,
        ]),
    )
}

#[allow(clippy::too_many_arguments)]
fn scheduling_worker_with_capabilities(
    worker_id: &str,
    domain_id: &str,
    worker_cpu_millis: u32,
    worker_memory_bytes: u64,
    domain_cpu_millis: u32,
    domain_memory_bytes: u64,
    labels: BTreeMap<String, String>,
    fresh: bool,
    operating_system: OperatingSystem,
    enforcement: BTreeSet<EnforcementCapability>,
) -> WorkerStatus {
    let worker_id = WorkerId::new(worker_id).expect("valid worker ID");
    let domain_id = WorkerId::new(domain_id).expect("valid resource-domain ID");
    let capacity = WorkerResourceCapacity::new(worker_cpu_millis, worker_memory_bytes)
        .expect("positive worker capacity");
    let domain_capacity = WorkerResourceCapacity::new(domain_cpu_millis, domain_memory_bytes)
        .expect("positive domain capacity");
    let capabilities = WorkerCapabilities::new(
        operating_system,
        Architecture::X86_64,
        BTreeSet::from([ExecutionBackend::NativeProcess]),
        enforcement,
    )
    .expect("valid worker capabilities");
    let inventory =
        WorkerInventory::new(capabilities, capacity, capacity, 4).expect("valid worker inventory");
    WorkerStatus::new(
        WorkerDescriptor::new(worker_id, 4, true),
        0,
        0,
        true,
        false,
        1_000,
    )
    .with_inventory(inventory)
    .with_manager_metadata(domain_id, domain_capacity, labels, 1_000, fresh)
    .expect("valid manager worker metadata")
}

fn deployment_request(replicas: u32) -> DeploymentMutation {
    DeploymentMutation::Deploy {
        revision: 1,
        replicas,
        strategy: conductor_core::DeploymentUpdateStrategy::default(),
    }
}

struct ServiceRunner {
    worker_id: WorkerId,
    status: WorkerStatus,
    starts: AtomicUsize,
    disconnected: AtomicBool,
    runs: Mutex<BTreeMap<RunId, RunObservation>>,
    stale_inspection: Mutex<Option<RunObservation>>,
}

impl ServiceRunner {
    fn mark_running_with_readiness(&self, readiness: ReadinessState) {
        let mut runs = self.runs.lock().expect("worker run lock");
        for observation in runs.values_mut() {
            if observation.state().is_terminal() {
                continue;
            }
            let running = if observation.state() == RunState::Running {
                observation.clone()
            } else {
                observation
                    .next_revision(RunState::Running, Some(2), None, None, None, None)
                    .expect("requested run can start")
            };
            *observation = running.with_readiness(readiness);
        }
    }

    fn mark_run_running_with_readiness(&self, run_id: &RunId, readiness: ReadinessState) {
        let mut runs = self.runs.lock().expect("worker run lock");
        let observation = runs.get_mut(run_id).expect("known worker run");
        let running = if observation.state() == RunState::Running {
            observation.clone()
        } else {
            observation
                .next_revision(RunState::Running, Some(2), None, None, None, None)
                .expect("requested run can start")
        };
        *observation = running.with_readiness(readiness);
    }

    fn fail_all(&self) {
        let mut runs = self.runs.lock().expect("worker run lock");
        for observation in runs.values_mut() {
            if observation.state().is_terminal() {
                continue;
            }
            let running = observation
                .next_revision(RunState::Running, Some(2), None, None, None, None)
                .expect("requested run can start");
            *observation = running
                .next_revision(
                    RunState::Failed,
                    Some(2),
                    Some(3),
                    Some(1),
                    None,
                    Some(RunFailureReason::NonZeroExit),
                )
                .expect("running run can fail");
        }
    }

    fn disconnect(&self) {
        self.disconnected.store(true, Ordering::Release);
    }
}

impl WorkerPort for ServiceRunner {
    fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.worker_id.clone(), 4, true)
    }

    fn start(&self, command: StartRun) -> Result<RunObservation, WorkerPortError> {
        self.starts.fetch_add(1, Ordering::Relaxed);
        let observation = RunObservation::requested(command.spec(), 1);
        self.runs
            .lock()
            .map_err(|_| WorkerPortError::Unavailable)?
            .insert(observation.run_id().clone(), observation.clone());
        Ok(observation)
    }

    fn inspect(&self, command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        if self.disconnected.load(Ordering::Acquire) {
            return Err(WorkerPortError::Unavailable);
        }
        if let Some(stale) = self
            .stale_inspection
            .lock()
            .map_err(|_| WorkerPortError::Unavailable)?
            .as_ref()
        {
            return Ok(Some(stale.clone()));
        }
        Ok(self
            .runs
            .lock()
            .map_err(|_| WorkerPortError::Unavailable)?
            .get(command.run_id())
            .cloned())
    }

    fn stop(&self, command: StopRun) -> Result<RunObservation, WorkerPortError> {
        if self.disconnected.load(Ordering::Acquire) {
            return Err(WorkerPortError::Unavailable);
        }
        let mut runs = self.runs.lock().map_err(|_| WorkerPortError::Unavailable)?;
        let current = runs
            .get(command.run_id())
            .cloned()
            .ok_or(WorkerPortError::RunConflict)?;
        let stopped = current
            .next_revision(
                RunState::Cancelled,
                current.started_at_ms(),
                Some(2),
                None,
                None,
                None,
            )
            .ok_or(WorkerPortError::RunConflict)?;
        runs.insert(stopped.run_id().clone(), stopped.clone());
        Ok(stopped)
    }

    fn status(&self) -> Result<WorkerStatus, WorkerPortError> {
        Ok(self.status.clone())
    }

    fn read_logs(&self, _command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }
}

struct CapacityRejectingWorker {
    worker_id: WorkerId,
    status: WorkerStatus,
}

impl WorkerPort for CapacityRejectingWorker {
    fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.worker_id.clone(), 4, true)
    }

    fn start(&self, _command: StartRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::CapacityReached)
    }

    fn inspect(&self, _command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        Ok(None)
    }

    fn stop(&self, _command: StopRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn status(&self) -> Result<WorkerStatus, WorkerPortError> {
        Ok(self.status.clone())
    }

    fn read_logs(&self, _command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }
}

#[test]
fn service_reconciliation_starts_each_replica_once_and_keeps_one_shot_semantics() {
    let directory = TempDir::new();
    let manager = service(&directory);
    manager.register(job("one-shot")).unwrap();
    let revision = manager
        .create_revision(&service_revision_draft("v1"), "service-reconcile-revision")
        .unwrap();
    let worker_status = scheduling_worker(
        "worker-a",
        "host-a",
        2_000,
        2_147_483_648,
        2_000,
        2_147_483_648,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 2,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "service-reconcile-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let worker_id = WorkerId::new("worker-a").unwrap();
    let worker: Arc<dyn WorkerPort> = Arc::new(ServiceRunner {
        worker_id,
        status: worker_status,
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });

    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    assert!(
        manager
            .list_runs(Some(&JobId::new("one-shot").unwrap()), None, 10)
            .unwrap()
            .is_empty()
    );
    let runs = manager
        .list_runs(Some(revision.id().job_id()), None, 10)
        .unwrap();
    assert_eq!(runs.len(), 2, "both desired replicas should start");
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    assert_eq!(
        manager
            .list_runs(Some(revision.id().job_id()), None, 10)
            .unwrap()
            .len(),
        2,
        "a repeated reconciliation must not duplicate a live attempt"
    );

    let original_allocations = manager.list_allocations(revision.id().job_id()).unwrap();
    let original_instances = original_allocations
        .iter()
        .filter_map(|allocation| allocation.allocation().service_instance_id())
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let scaled = manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            Some(1),
            &DeploymentMutation::Scale { replicas: 0 },
            "service-reconcile-scale-zero",
            &[],
        )
        .unwrap();
    assert_eq!(scaled.desired_replicas(), 0);
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    assert!(
        manager
            .list_allocations(revision.id().job_id())
            .unwrap()
            .iter()
            .filter(|allocation| original_allocations
                .iter()
                .any(|old| { old.allocation().id() == allocation.allocation().id() }))
            .all(|allocation| allocation.allocation().state() == AllocationState::Released)
    );

    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            Some(2),
            &DeploymentMutation::Scale { replicas: 2 },
            "service-reconcile-resume",
            &[worker.status().expect("worker status remains available")],
        )
        .unwrap();
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    assert_eq!(
        manager
            .list_runs(Some(revision.id().job_id()), None, 10)
            .unwrap()
            .len(),
        4,
        "resuming scale-to-zero should start one new run for each stable slot"
    );
    let resumed_instances = manager
        .list_allocations(revision.id().job_id())
        .unwrap()
        .iter()
        .filter(|allocation| allocation.allocation().state() != AllocationState::Released)
        .filter_map(|allocation| allocation.allocation().service_instance_id())
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    assert_eq!(resumed_instances, original_instances);
}

#[test]
fn revision_rollout_is_readiness_gated_and_deadline_restores_the_previous_revision() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision_one = manager
        .create_revision(&service_revision_with_probe("v1"), "rollout-revision-one")
        .unwrap();
    let service_network = WorkerServiceNetwork::new(
        WorkerId::new("host-rollout").unwrap(),
        32_000,
        32_010,
        "192.0.2.21",
    )
    .unwrap();
    let worker_status = scheduling_worker(
        "worker-rollout",
        "host-rollout",
        2_000,
        2_147_483_648,
        2_000,
        2_147_483_648,
        BTreeMap::new(),
        true,
    )
    .with_service_network(service_network)
    .unwrap();
    manager
        .mutate_deployment_with_workers(
            revision_one.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 2,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "rollout-deploy-one",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let runner = Arc::new(ServiceRunner {
        worker_id: WorkerId::new("worker-rollout").unwrap(),
        status: worker_status.clone(),
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    let worker: Arc<dyn WorkerPort> = runner.clone();
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    runner.mark_running_with_readiness(ReadinessState::Ready);
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let stable_instances = manager
        .list_service_attempts(revision_one.id().job_id())
        .unwrap()
        .iter()
        .map(|attempt| attempt.service_instance_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(stable_instances.len(), 2);
    let old_run_ids = manager
        .list_service_attempts(revision_one.id().job_id())
        .unwrap()
        .into_iter()
        .filter_map(|attempt| attempt.current_run_id)
        .collect::<BTreeSet<_>>();

    let revision_two = manager
        .create_revision(&service_revision_with_probe("v2"), "rollout-revision-two")
        .unwrap();
    let revision_three = manager
        .create_revision(&service_revision_with_probe("v3"), "rollout-revision-three")
        .unwrap();
    let updated = manager
        .mutate_deployment_with_workers(
            revision_one.id().job_id(),
            Some(1),
            &DeploymentMutation::Deploy {
                revision: revision_two.id().revision(),
                replicas: 2,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "rollout-deploy-two",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    assert_eq!(updated.rollout().unwrap().strategy().max_unavailable(), 1);
    assert_eq!(
        manager.mutate_deployment_with_workers(
            revision_one.id().job_id(),
            Some(updated.generation()),
            &DeploymentMutation::Deploy {
                revision: revision_three.id().revision(),
                replicas: 2,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "rollout-conflicting-deploy",
            std::slice::from_ref(&worker_status),
        ),
        Err(conductor_manager::ExecutionStoreError::Conflict),
        "a second revision update waits for the active rollout to finish"
    );
    drop(manager);
    let manager = service(&directory);
    let old_terminal_count = || {
        manager
            .list_runs(Some(revision_one.id().job_id()), None, 100)
            .unwrap()
            .into_iter()
            .filter(|run| old_run_ids.contains(run.run_id()) && run.state().is_terminal())
            .count()
    };

    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    assert_eq!(
        old_terminal_count(),
        1,
        "only one old replica may be unavailable before a replacement is ready"
    );

    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    runner.mark_running_with_readiness(ReadinessState::Ready);
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    assert_eq!(
        old_terminal_count(),
        2,
        "the next old replica is retired after the first replacement is ready"
    );
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    runner.mark_running_with_readiness(ReadinessState::Ready);
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let ready = manager
        .get_deployment(revision_one.id().job_id())
        .unwrap()
        .unwrap();
    assert_eq!(ready.status(), DeploymentStatus::Ready);
    assert!(ready.rollout().is_none());
    let attempts = manager
        .list_service_attempts(revision_one.id().job_id())
        .unwrap();
    assert_eq!(
        attempts
            .iter()
            .map(|attempt| attempt.service_instance_id.clone())
            .collect::<BTreeSet<_>>(),
        stable_instances,
        "replacement attempts retain stable replica identities"
    );
    assert!(attempts.iter().all(|attempt| {
        attempt
            .allocation
            .as_ref()
            .is_some_and(|record| record.allocation().job_revision_id() == revision_two.id())
            && attempt.current_run.as_ref().is_some_and(|run| {
                run.state() == RunState::Running && run.readiness() == Some(ReadinessState::Ready)
            })
    }));

    let deploy_three = manager
        .mutate_deployment_with_workers(
            revision_one.id().job_id(),
            Some(ready.generation()),
            &DeploymentMutation::Deploy {
                revision: revision_three.id().revision(),
                replicas: 2,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "rollout-deploy-three",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let scaled_to_zero = manager
        .mutate_deployment_with_workers(
            revision_one.id().job_id(),
            Some(deploy_three.generation()),
            &DeploymentMutation::Scale { replicas: 0 },
            "rollout-scale-zero",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    assert_eq!(scaled_to_zero.desired_replicas(), 0);
    assert!(scaled_to_zero.rollout().is_none());
    drop(manager);
    let manager = service(&directory);
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    assert!(
        manager
            .list_service_attempts(revision_one.id().job_id())
            .unwrap()
            .iter()
            .all(|attempt| attempt
                .current_run
                .as_ref()
                .is_none_or(|run| run.state().is_terminal()))
    );

    let scaled_up = manager
        .mutate_deployment_with_workers(
            revision_one.id().job_id(),
            Some(scaled_to_zero.generation()),
            &DeploymentMutation::Scale { replicas: 2 },
            "rollout-scale-up",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    runner.mark_running_with_readiness(ReadinessState::Ready);
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let resumed = manager
        .get_deployment(revision_one.id().job_id())
        .unwrap()
        .unwrap();
    assert_eq!(resumed.revision_id(), revision_three.id());
    assert_eq!(resumed.status(), DeploymentStatus::Ready);
    assert_eq!(resumed.generation(), scaled_up.generation());

    let revision_four = manager
        .create_revision(&service_revision_with_probe("v4"), "rollout-revision-four")
        .unwrap();
    manager
        .mutate_deployment_with_workers(
            revision_one.id().job_id(),
            Some(resumed.generation()),
            &DeploymentMutation::Deploy {
                revision: revision_four.id().revision(),
                replicas: 2,
                strategy: conductor_core::DeploymentUpdateStrategy::new(0, 1, 1).unwrap(),
            },
            "rollout-deploy-four",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let unready_attempt =
        manager
            .list_service_attempts(revision_one.id().job_id())
            .unwrap()
            .into_iter()
            .find(|attempt| {
                attempt.allocation.as_ref().is_some_and(|record| {
                    record.allocation().job_revision_id() == revision_four.id()
                }) && attempt.current_run_id.is_some()
            })
            .expect("one replacement run starts before its readiness gate");
    runner.mark_run_running_with_readiness(
        unready_attempt.current_run_id.as_ref().unwrap(),
        ReadinessState::Unready,
    );
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let pending = manager
        .get_deployment(revision_one.id().job_id())
        .unwrap()
        .unwrap();
    assert_eq!(pending.status(), DeploymentStatus::Pending);
    assert_eq!(
        pending.pending_reason(),
        Some(DeploymentPendingReason::ServiceReadinessPending)
    );
    assert!(
        manager
            .list_service_attempts(revision_one.id().job_id())
            .unwrap()
            .iter()
            .any(|attempt| attempt
                .current_run
                .as_ref()
                .is_some_and(|run| run.state() == RunState::Running)
                && attempt
                    .allocation
                    .as_ref()
                    .is_some_and(
                        |record| record.allocation().job_revision_id() == revision_three.id()
                    ))
    );

    let connection = Connection::open(directory.database()).unwrap();
    let mut deployment_json: serde_json::Value = connection
        .query_row(
            "SELECT deployment_json FROM job_deployments WHERE job_id = ?1",
            [revision_one.id().job_id().as_str()],
            |row| row.get::<_, String>(0),
        )
        .map(|json| serde_json::from_str(&json).unwrap())
        .unwrap();
    deployment_json["rollout"]["progressDeadlineAtMs"] = serde_json::json!(0);
    deployment_json["rollout"]["startedAtMs"] = serde_json::json!(0);
    connection
        .execute(
            "UPDATE job_deployments SET deployment_json = ?1 WHERE job_id = ?2",
            rusqlite::params![
                serde_json::to_string(&deployment_json).unwrap(),
                revision_one.id().job_id().as_str(),
            ],
        )
        .unwrap();
    drop(connection);
    assert_eq!(
        manager
            .get_deployment(revision_one.id().job_id())
            .unwrap()
            .unwrap()
            .rollout()
            .unwrap()
            .progress_deadline_at_ms(),
        Some(0)
    );
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let rolling_back = manager
        .get_deployment(revision_one.id().job_id())
        .unwrap()
        .unwrap();
    assert_eq!(rolling_back.revision_id(), revision_three.id());
    assert_eq!(rolling_back.generation(), pending.generation() + 1);
    assert!(
        rolling_back
            .rollout()
            .is_some_and(|rollout| rollout.rolling_back())
    );

    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    runner.mark_running_with_readiness(ReadinessState::Ready);
    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let rolled_back = manager
        .get_deployment(revision_one.id().job_id())
        .unwrap()
        .unwrap();
    assert_eq!(rolled_back.revision_id(), revision_three.id());
    assert_eq!(rolled_back.status(), DeploymentStatus::Ready);
    assert!(rolled_back.rollout().is_none());
}

#[test]
fn scale_to_zero_before_first_run_releases_assigned_reservations() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(
            &service_revision_draft("scale-before-start"),
            "scale-before-start-revision",
        )
        .unwrap();
    let worker_status = scheduling_worker(
        "worker-before-start",
        "host-before-start",
        1_000,
        1_073_741_824,
        1_000,
        1_073_741_824,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 1,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "scale-before-start-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();

    let allocated = manager.list_allocations(revision.id().job_id()).unwrap();
    assert_eq!(allocated.len(), 1);
    assert_eq!(allocated[0].allocation().state(), AllocationState::Assigned);
    assert!(
        manager
            .list_service_attempts(revision.id().job_id())
            .unwrap()
            .iter()
            .all(|attempt| attempt.current_run_id.is_none()),
        "placement has not started a worker run yet"
    );

    let replacement_revision = manager
        .create_revision(
            &service_revision_draft("scale-before-start-v2"),
            "scale-before-start-replacement-revision",
        )
        .unwrap();
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            Some(1),
            &DeploymentMutation::Deploy {
                revision: replacement_revision.id().revision(),
                replicas: 1,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "scale-before-start-replace",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let after_replacement = manager.list_allocations(revision.id().job_id()).unwrap();
    assert_eq!(after_replacement.len(), 2);
    assert_eq!(
        after_replacement[0].allocation().state(),
        AllocationState::Released,
        "revision replacement must release the unstarted old reservation"
    );
    assert_eq!(
        after_replacement[1].allocation().state(),
        AllocationState::Assigned
    );

    let scaled = manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            Some(2),
            &DeploymentMutation::Scale { replicas: 0 },
            "scale-before-start-zero",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    assert_eq!(scaled.desired_replicas(), 0);
    assert_eq!(scaled.status(), DeploymentStatus::Ready);
    assert_eq!(
        manager
            .list_allocations(revision.id().job_id())
            .unwrap()
            .iter()
            .map(|record| record.allocation().state())
            .collect::<Vec<_>>(),
        [AllocationState::Released, AllocationState::Released],
        "scale-down must release every unstarted assigned reservation"
    );

    let deleted = manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            Some(3),
            &DeploymentMutation::Delete,
            "scale-before-start-delete",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    assert_eq!(deleted.status(), DeploymentStatus::Deleted);
}

#[test]
fn named_service_ports_are_leased_uniquely_persisted_and_not_ready_without_health_checks() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(
            &service_revision_with_ports("ports"),
            "named-ports-revision",
        )
        .unwrap();
    let worker_id = WorkerId::new("worker-ports").unwrap();
    let service_network = WorkerServiceNetwork::new(
        WorkerId::new("host-ports").unwrap(),
        31_000,
        31_001,
        "192.0.2.11",
    )
    .unwrap();
    let worker_status = scheduling_worker(
        "worker-ports",
        "host-ports",
        2_000,
        2_147_483_648,
        2_000,
        2_147_483_648,
        BTreeMap::new(),
        true,
    )
    .with_service_network(service_network)
    .unwrap();
    let second_draft = {
        let mut value = serde_json::to_value(service_revision_with_probe("second-job")).unwrap();
        value["definition"]["id"] = serde_json::json!("other-web");
        serde_json::from_value::<JobRevisionDraft>(value).unwrap()
    };
    let second_revision = manager
        .create_revision(&second_draft, "second-probe-revision")
        .unwrap();
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &deployment_request(2),
            "named-ports-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    assert_eq!(
        manager.mutate_deployment_with_workers(
            second_revision.id().job_id(),
            None,
            &deployment_request(1),
            "second-probe-deploy",
            std::slice::from_ref(&worker_status),
        ),
        Err(ExecutionStoreError::Conflict),
        "one active job owns a logical service identity"
    );
    let worker: Arc<dyn WorkerPort> = Arc::new(ServiceRunner {
        worker_id,
        status: worker_status,
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });

    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let attempts = manager
        .list_service_attempts(revision.id().job_id())
        .unwrap();
    assert_eq!(attempts.len(), 2);
    let ports = attempts
        .iter()
        .map(|attempt| {
            assert_eq!(attempt.service_ports.len(), 1);
            assert!(!attempt.service_ports_ready);
            assert_eq!(attempt.service_ports[0].name(), "http");
            assert_eq!(attempt.service_ports[0].advertised_address(), "192.0.2.11");
            attempt.service_ports[0].port()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        ports.len(),
        2,
        "simultaneous service attempts need unique ports"
    );

    drop(manager);
    let reopened = service(&directory);
    let recovered = reopened
        .list_service_attempts(revision.id().job_id())
        .unwrap();
    assert_eq!(
        recovered
            .iter()
            .map(|attempt| attempt.service_ports[0].port())
            .collect::<BTreeSet<_>>(),
        ports,
        "manager restart must preserve the assigned endpoints"
    );
    let status = worker.status().unwrap();
    reopened
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            Some(1),
            &DeploymentMutation::Scale { replicas: 3 },
            "named-ports-scale",
            std::slice::from_ref(&status),
        )
        .unwrap();
    reopened
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    assert_eq!(
        reopened
            .list_runs(Some(revision.id().job_id()), None, 10)
            .unwrap()
            .len(),
        2,
        "active leases remain unavailable after manager restart"
    );
}

#[test]
fn manager_publishes_service_ports_ready_only_after_a_newer_probe_observation() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(&service_revision_with_probe("readiness"), "probe-revision")
        .unwrap();
    let worker_id = WorkerId::new("worker-probe").unwrap();
    let service_network = WorkerServiceNetwork::new(
        WorkerId::new("host-probe").unwrap(),
        31_000,
        31_000,
        "192.0.2.12",
    )
    .unwrap();
    let worker_status = scheduling_worker(
        "worker-probe",
        "host-probe",
        2_000,
        2_147_483_648,
        2_000,
        2_147_483_648,
        BTreeMap::new(),
        true,
    )
    .with_service_network(service_network)
    .unwrap();
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &deployment_request(1),
            "probe-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let worker = Arc::new(ServiceRunner {
        worker_id,
        status: worker_status.clone(),
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    let worker_port: Arc<dyn WorkerPort> = worker.clone();

    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();
    let attempt = manager
        .list_service_attempts(revision.id().job_id())
        .unwrap()
        .pop()
        .unwrap();
    assert!(!attempt.service_ports_ready);
    let identity = match revision.workload() {
        conductor_core::Workload::Service { identity, .. } => identity,
        _ => unreachable!("service revision"),
    };
    let catalog = manager
        .list_service_catalog(identity, std::slice::from_ref(&worker_status))
        .unwrap();
    assert!(!catalog.is_empty());
    assert!(catalog.iter().all(|endpoint| !endpoint.eligible));
    let run_id = attempt.current_run_id.unwrap();
    let current_run = attempt.current_run.unwrap();
    let stale_spec = conductor_core::RunSpec::new(
        RunId::new("old-service-attempt").unwrap(),
        current_run.worker_id().clone(),
        revision.definition().clone(),
    );
    let stale_ready = RunObservation::requested(&stale_spec, 1)
        .next_revision(RunState::Running, Some(2), None, None, None, None)
        .unwrap()
        .with_readiness(ReadinessState::Ready);
    *worker.stale_inspection.lock().unwrap() = Some(stale_ready);
    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();
    assert!(
        !manager
            .list_service_attempts(revision.id().job_id())
            .unwrap()[0]
            .service_ports_ready,
        "a stale attempt observation must not publish the current endpoint"
    );
    *worker.stale_inspection.lock().unwrap() = None;
    {
        let mut runs = worker.runs.lock().unwrap();
        let current = runs.get_mut(&run_id).unwrap();
        let running = current
            .next_revision(RunState::Running, Some(2), None, None, None, None)
            .unwrap()
            .with_readiness(ReadinessState::Starting);
        *current = running
            .next_readiness_revision(ReadinessState::Ready)
            .unwrap();
    }
    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();
    let attempt = manager
        .list_service_attempts(revision.id().job_id())
        .unwrap()
        .pop()
        .unwrap();
    assert!(attempt.service_ports_ready);
    assert_eq!(attempt.current_run.unwrap().state(), RunState::Running);
    let catalog = manager
        .list_service_catalog(identity, std::slice::from_ref(&worker_status))
        .unwrap();
    assert_eq!(catalog.len(), 1);
    assert!(catalog[0].eligible);
    assert_eq!(catalog[0].readiness, Some(ReadinessState::Ready));
    let stale_worker = worker_status.clone().with_freshness(false);
    let stale_catalog = manager
        .list_service_catalog(identity, std::slice::from_ref(&stale_worker))
        .unwrap();
    assert!(!stale_catalog[0].worker_fresh);
    assert!(!stale_catalog[0].eligible);

    let reopened = service(&directory);
    let reconstructed = reopened
        .list_service_catalog(identity, std::slice::from_ref(&worker_status))
        .unwrap();
    assert_eq!(
        reconstructed, catalog,
        "catalog is reconstructed after restart"
    );
}

#[test]
fn schema_nine_migration_backfills_indexed_service_owners() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let first = manager
        .create_revision(&service_revision_draft("catalog-owner"), "owner-revision")
        .unwrap();
    manager
        .mutate_deployment(
            first.id().job_id(),
            None,
            &deployment_request(0),
            "owner-deploy",
        )
        .unwrap();
    drop(manager);

    let legacy = Connection::open(directory.database()).unwrap();
    legacy
        .execute_batch(
            "DROP TABLE service_catalog_owners;
             PRAGMA user_version = 9;",
        )
        .unwrap();
    drop(legacy);

    let migrated = service(&directory);
    let second_draft = {
        let mut value = serde_json::to_value(service_revision_draft("second-owner")).unwrap();
        value["definition"]["id"] = serde_json::json!("second-web");
        serde_json::from_value::<JobRevisionDraft>(value).unwrap()
    };
    let second = migrated
        .create_revision(&second_draft, "second-owner-revision")
        .unwrap();
    assert_eq!(
        migrated.mutate_deployment(
            second.id().job_id(),
            None,
            &deployment_request(0),
            "second-owner-deploy",
        ),
        Err(ExecutionStoreError::Conflict)
    );
}

#[test]
fn worker_drain_intent_persists_and_completed_workers_stay_out_of_placement() {
    let directory = TempDir::new();
    let worker_id = WorkerId::new("worker-drain").unwrap();
    let store = SqliteJobRepository::open(directory.database()).unwrap();
    store.insert(job("drain-admission")).unwrap();
    let first = store.request_worker_drain(&worker_id, 30_000).unwrap();
    assert_eq!(
        store.create_run_for_worker(
            &JobId::new("drain-admission").unwrap(),
            &worker_id,
            0,
            4,
            None,
        ),
        Err(RunStoreError::WorkerDraining),
        "the storage transaction must reject a start after drain intent commits"
    );
    assert_eq!(
        store.request_worker_drain(&worker_id, 1_000).unwrap(),
        first,
        "repeated drain requests must preserve the original deadline"
    );
    assert!(!store.cancel_worker_drain(&worker_id).unwrap());
    drop(store);

    let manager = service(&directory);
    let worker_status = scheduling_worker(
        "worker-drain",
        "host-drain",
        1_000,
        1_000_000,
        1_000,
        1_000_000,
        BTreeMap::new(),
        true,
    );
    let worker = CapacityRejectingWorker {
        worker_id: worker_id.clone(),
        status: worker_status.clone(),
    };
    let drain = manager
        .worker_drain_status(&worker_id)
        .unwrap()
        .expect("drain intent survives manager restart");
    assert_eq!(drain.state, "completed");
    let effective = manager.worker_status(&worker).unwrap();
    assert!(effective.draining());
    assert!(!effective.available());
    drop(manager);

    let reopened = service(&directory);
    assert!(reopened.worker_status(&worker).unwrap().draining());
    let status = reopened.worker_drain_status(&worker_id).unwrap().unwrap();
    assert_eq!(status.active_runs, 0);
    assert_eq!(status.state, "completed");
    assert!(reopened.cancel_worker_drain(&worker_id).unwrap());
    assert!(!reopened.worker_status(&worker).unwrap().draining());
}

#[test]
fn service_drain_withdraws_catalog_and_waits_for_replacement_capacity() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(&service_revision_with_ports("drain"), "drain-revision")
        .unwrap();
    let source_id = WorkerId::new("worker-drain-source").unwrap();
    let source_network = WorkerServiceNetwork::new(
        WorkerId::new("host-drain-source").unwrap(),
        31_000,
        31_000,
        "192.0.2.21",
    )
    .unwrap();
    let source_status = scheduling_worker(
        "worker-drain-source",
        "host-drain-source",
        1_000,
        1_000_000_000,
        1_000,
        1_000_000_000,
        BTreeMap::new(),
        true,
    )
    .with_service_network(source_network)
    .unwrap();
    let source: Arc<dyn WorkerPort> = Arc::new(ServiceRunner {
        worker_id: source_id.clone(),
        status: source_status.clone(),
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &deployment_request(1),
            "drain-deploy",
            std::slice::from_ref(&source_status),
        )
        .unwrap();
    manager
        .reconcile_services(std::slice::from_ref(&source))
        .unwrap();
    let identity = match revision.workload() {
        conductor_core::Workload::Service { identity, .. } => identity,
        _ => unreachable!("service revision"),
    };
    manager.request_worker_drain(&source_id, 30_000).unwrap();
    let withdrawn_status = manager.worker_status(source.as_ref()).unwrap();
    assert!(withdrawn_status.draining());
    let withdrawn = manager
        .list_service_catalog(identity, &[withdrawn_status])
        .unwrap();
    assert_eq!(withdrawn.len(), 1);
    assert!(withdrawn[0].worker_draining);
    assert!(!withdrawn[0].eligible);

    manager
        .reconcile_services(std::slice::from_ref(&source))
        .unwrap();
    let blocked = manager.worker_drain_status(&source_id).unwrap().unwrap();
    assert_eq!(blocked.state, "blocked");
    assert_eq!(blocked.blocked_reason, Some("replacement_capacity"));
    assert_eq!(
        blocked.active_runs, 1,
        "keep the source run alive without capacity"
    );

    let destination_id = WorkerId::new("worker-drain-destination").unwrap();
    let destination_network = WorkerServiceNetwork::new(
        WorkerId::new("host-drain-destination").unwrap(),
        32_000,
        32_000,
        "192.0.2.22",
    )
    .unwrap();
    let destination_status = scheduling_worker(
        "worker-drain-destination",
        "host-drain-destination",
        1_000,
        1_000_000_000,
        1_000,
        1_000_000_000,
        BTreeMap::new(),
        true,
    )
    .with_service_network(destination_network)
    .unwrap();
    let destination: Arc<dyn WorkerPort> = Arc::new(ServiceRunner {
        worker_id: destination_id.clone(),
        status: destination_status.clone(),
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    let workers = [Arc::clone(&source), Arc::clone(&destination)];
    manager.reconcile_services(&workers).unwrap();
    manager.reconcile_services(&workers).unwrap();
    let attempt = manager
        .list_service_attempts(revision.id().job_id())
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(attempt.current_run.unwrap().worker_id(), &destination_id);
    assert_eq!(
        manager
            .worker_drain_status(&source_id)
            .unwrap()
            .unwrap()
            .state,
        "completed"
    );
    let statuses = [
        manager.worker_status(source.as_ref()).unwrap(),
        manager.worker_status(destination.as_ref()).unwrap(),
    ];
    let endpoints = manager.list_service_catalog(identity, &statuses).unwrap();
    assert!(
        endpoints
            .iter()
            .all(|endpoint| endpoint.worker_id == destination_id)
    );
}

#[test]
fn drain_deadline_stops_remaining_one_shot_runs_and_completes() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let spec = job("drain-one-shot");
    let job_id = spec.id().clone();
    manager.register(spec).unwrap();
    let worker_id = WorkerId::new("worker-drain-run").unwrap();
    let worker_status = scheduling_worker(
        "worker-drain-run",
        "host-drain-run",
        1_000,
        1_000_000,
        1_000,
        1_000_000,
        BTreeMap::new(),
        true,
    );
    let worker: Arc<dyn WorkerPort> = Arc::new(ServiceRunner {
        worker_id: worker_id.clone(),
        status: worker_status,
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    let run = manager
        .start_run_with_key(&job_id, &worker_id, 4, None, worker.as_ref())
        .unwrap();
    let drain = manager.request_worker_drain(&worker_id, 30_000).unwrap();
    Connection::open(directory.database())
        .unwrap()
        .execute(
            "UPDATE worker_drains SET deadline_at_ms = requested_at_ms WHERE worker_id = ?1",
            [worker_id.as_str()],
        )
        .unwrap();

    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let stopped = manager
        .get_run(run.run_id(), worker.as_ref())
        .unwrap()
        .unwrap();
    assert!(stopped.state().is_terminal());
    let status = manager.worker_drain_status(&worker_id).unwrap().unwrap();
    assert_eq!(status.state, "completed");
    assert_eq!(status.active_runs, 0);
    assert!(drain.deadline_at_ms > drain.requested_at_ms);
}

#[test]
fn schema_ten_migration_creates_persistent_worker_drains() {
    let directory = TempDir::new();
    let manager = service(&directory);
    drop(manager);
    Connection::open(directory.database())
        .unwrap()
        .execute_batch("DROP TABLE worker_drains; PRAGMA user_version = 10;")
        .unwrap();
    let migrated = SqliteJobRepository::open(directory.database()).unwrap();
    let worker_id = WorkerId::new("worker-migrated-drain").unwrap();
    assert!(migrated.get_worker_drain(&worker_id).unwrap().is_none());
    migrated.request_worker_drain(&worker_id, 1_000).unwrap();
    assert!(migrated.get_worker_drain(&worker_id).unwrap().is_some());
}

#[test]
fn service_restart_backoff_survives_manager_restart_and_retries_after_bounded_polling() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(&service_revision_draft("crash"), "restart-revision")
        .unwrap();
    let worker_status = scheduling_worker(
        "worker-a",
        "host-a",
        1_000,
        1_073_741_824,
        1_000,
        1_073_741_824,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 1,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "restart-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let worker_id = WorkerId::new("worker-a").unwrap();
    let worker = Arc::new(ServiceRunner {
        worker_id,
        status: worker_status,
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    let worker_port: Arc<dyn WorkerPort> = worker.clone();
    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();
    worker.fail_all();
    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();
    assert_eq!(
        manager
            .list_runs(Some(revision.id().job_id()), None, 10)
            .unwrap()
            .len(),
        1,
        "a failed service waits for its durable backoff"
    );
    drop(manager);

    let reopened = service(&directory);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        reopened
            .reconcile_services(std::slice::from_ref(&worker_port))
            .unwrap();
        let runs = reopened
            .list_runs(Some(revision.id().job_id()), None, 10)
            .unwrap();
        if runs.len() == 2 {
            assert_eq!(worker.starts.load(Ordering::Relaxed), 2);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "service retry did not become eligible"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn persisted_terminal_service_result_is_consumed_after_worker_disconnect() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(
            &service_revision_draft("terminal-report"),
            "terminal-report-revision",
        )
        .unwrap();
    let worker_status = scheduling_worker(
        "worker-terminal-report",
        "host-terminal-report",
        1_000,
        1_073_741_824,
        1_000,
        1_073_741_824,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 1,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "terminal-report-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let worker = Arc::new(ServiceRunner {
        worker_id: WorkerId::new("worker-terminal-report").unwrap(),
        status: worker_status,
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    let worker_port: Arc<dyn WorkerPort> = worker.clone();
    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();
    let attempt = manager
        .list_service_attempts(revision.id().job_id())
        .unwrap()
        .pop()
        .unwrap();
    let current = attempt.current_run.unwrap();
    let failed = current
        .next_revision(RunState::Running, Some(2), None, None, None, None)
        .unwrap()
        .next_revision(
            RunState::Failed,
            Some(2),
            Some(3),
            Some(1),
            None,
            Some(RunFailureReason::NonZeroExit),
        )
        .unwrap();
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute(
            "UPDATE runs SET observation_json = ?1, revision = ?2, state = ?3 WHERE run_id = ?4",
            rusqlite::params![
                serde_json::to_string(&failed).unwrap(),
                failed.revision() as i64,
                failed.state().as_str(),
                failed.run_id().as_str(),
            ],
        )
        .unwrap();
    drop(connection);
    drop(manager);
    worker.disconnect();

    let reopened = service(&directory);
    reopened.reconcile_services(&[]).unwrap();
    let attempt = reopened
        .list_service_attempts(revision.id().job_id())
        .unwrap()
        .pop()
        .unwrap();
    assert!(attempt.current_run_id.is_none());
    assert_eq!(attempt.restart_count, 1);
    assert!(attempt.next_start_at_ms > 0);
}

#[test]
fn capacity_rejection_atomically_clears_the_unaccepted_service_attempt() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(
            &service_revision_draft("capacity-rejection"),
            "capacity-rejection-revision",
        )
        .unwrap();
    let worker_id = WorkerId::new("worker-capacity-rejection").unwrap();
    let worker_status = scheduling_worker(
        "worker-capacity-rejection",
        "host-capacity-rejection",
        1_000,
        1_073_741_824,
        1_000,
        1_073_741_824,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 1,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "capacity-rejection-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let worker: Arc<dyn WorkerPort> = Arc::new(CapacityRejectingWorker {
        worker_id,
        status: worker_status,
    });

    manager
        .reconcile_services(std::slice::from_ref(&worker))
        .unwrap();
    let attempts = manager
        .list_service_attempts(revision.id().job_id())
        .unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].current_run_id.is_none());
    assert!(attempts[0].next_start_at_ms > 0);
    assert!(
        manager
            .list_runs(Some(revision.id().job_id()), None, 10)
            .unwrap()
            .is_empty(),
        "a run rejected before acceptance is removed atomically with attempt cleanup"
    );
    let deployment = manager
        .get_deployment(revision.id().job_id())
        .unwrap()
        .unwrap();
    assert_eq!(deployment.status(), DeploymentStatus::Pending);
    assert_eq!(
        deployment.pending_reason(),
        Some(DeploymentPendingReason::ServiceRestartBackoff)
    );
}

#[test]
fn exhausted_service_retry_budget_is_reported_and_stops_relaunching() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(
            &service_revision_draft("retry-budget"),
            "retry-budget-revision",
        )
        .unwrap();
    let worker_status = scheduling_worker(
        "worker-retry-budget",
        "host-retry-budget",
        1_000,
        1_073_741_824,
        1_000,
        1_073_741_824,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 1,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "retry-budget-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let worker = Arc::new(ServiceRunner {
        worker_id: WorkerId::new("worker-retry-budget").unwrap(),
        status: worker_status,
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    let worker_port: Arc<dyn WorkerPort> = worker.clone();

    for failure_count in 1..=6 {
        if failure_count > 1 {
            let instance = manager
                .list_service_attempts(revision.id().job_id())
                .unwrap()
                .pop()
                .unwrap()
                .service_instance_id;
            let connection = Connection::open(directory.database()).unwrap();
            connection
                .execute(
                    "UPDATE service_attempts SET next_start_at_ms = 0 WHERE service_instance_id = ?1",
                    [instance.as_str()],
                )
                .unwrap();
            drop(connection);
            manager
                .reconcile_services(std::slice::from_ref(&worker_port))
                .unwrap();
        } else {
            manager
                .reconcile_services(std::slice::from_ref(&worker_port))
                .unwrap();
        }
        assert_eq!(worker.starts.load(Ordering::Relaxed), failure_count);
        worker.fail_all();
        manager
            .reconcile_services(std::slice::from_ref(&worker_port))
            .unwrap();
        let attempt = manager
            .list_service_attempts(revision.id().job_id())
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(attempt.restart_count, failure_count as u32);
        assert_eq!(attempt.terminal_failure, failure_count == 6);
    }

    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();
    assert_eq!(worker.starts.load(Ordering::Relaxed), 6);
    let deployment = manager
        .get_deployment(revision.id().job_id())
        .unwrap()
        .unwrap();
    assert_eq!(deployment.status(), DeploymentStatus::Pending);
    assert_eq!(
        deployment.pending_reason(),
        Some(DeploymentPendingReason::ServiceRestartBudgetExhausted)
    );
}

#[test]
fn unresolved_service_run_is_not_relaunched_during_worker_disconnect() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(&service_revision_draft("disconnect"), "disconnect-revision")
        .unwrap();
    let worker_status = scheduling_worker(
        "worker-a",
        "host-a",
        1_000,
        1_073_741_824,
        1_000,
        1_073_741_824,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 1,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "disconnect-deploy",
            std::slice::from_ref(&worker_status),
        )
        .unwrap();
    let worker = Arc::new(ServiceRunner {
        worker_id: WorkerId::new("worker-a").unwrap(),
        status: worker_status,
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    let worker_port: Arc<dyn WorkerPort> = worker.clone();
    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();
    worker.disconnect();
    for _ in 0..5 {
        manager
            .reconcile_services(std::slice::from_ref(&worker_port))
            .unwrap();
    }
    assert_eq!(worker.starts.load(Ordering::Relaxed), 1);
    assert_eq!(
        manager
            .list_runs(Some(revision.id().job_id()), None, 10)
            .unwrap()
            .len(),
        1,
        "an unresolved disconnected run must stay attached to its service slot"
    );
}

#[test]
fn revisions_and_deployment_mutations_are_durable_conditional_and_idempotent() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let draft = service_revision_draft("v1");
    let revision_one = manager
        .create_revision(&draft, "revision-one")
        .expect("create first revision");
    assert_eq!(revision_one.id().revision(), 1);
    assert_eq!(
        manager.create_revision(&draft, "revision-one").unwrap(),
        revision_one
    );
    assert_eq!(
        manager.create_revision(&service_revision_draft("different"), "revision-one"),
        Err(ExecutionStoreError::IdempotencyConflict)
    );

    let initial = manager
        .mutate_deployment(
            revision_one.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 3,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "deploy-one",
        )
        .expect("persist initial deployment");
    assert_eq!(initial.generation(), 1);
    assert_eq!(initial.status(), DeploymentStatus::Pending);
    assert_eq!(
        initial.pending_reason(),
        Some(DeploymentPendingReason::WorkerDisconnected)
    );
    assert_eq!(
        manager
            .mutate_deployment(
                revision_one.id().job_id(),
                None,
                &DeploymentMutation::Deploy {
                    revision: 1,
                    replicas: 3,
                    strategy: conductor_core::DeploymentUpdateStrategy::default(),
                },
                "deploy-one",
            )
            .unwrap(),
        initial
    );

    let scaled_to_zero = manager
        .mutate_deployment(
            revision_one.id().job_id(),
            Some(1),
            &DeploymentMutation::Scale { replicas: 0 },
            "scale-zero",
        )
        .expect("scale to zero");
    assert_eq!(scaled_to_zero.generation(), 2);
    assert_eq!(scaled_to_zero.desired_replicas(), 0);
    assert_eq!(scaled_to_zero.status(), DeploymentStatus::Ready);
    assert_eq!(
        manager
            .mutate_deployment(
                revision_one.id().job_id(),
                Some(1),
                &DeploymentMutation::Scale { replicas: 0 },
                "scale-zero",
            )
            .unwrap(),
        scaled_to_zero
    );
    assert_eq!(
        manager.mutate_deployment(
            revision_one.id().job_id(),
            Some(1),
            &DeploymentMutation::Suspend,
            "stale-suspend",
        ),
        Err(ExecutionStoreError::PreconditionFailed)
    );

    let suspended = manager
        .mutate_deployment(
            revision_one.id().job_id(),
            Some(2),
            &DeploymentMutation::Suspend,
            "suspend",
        )
        .expect("suspend deployment");
    assert_eq!(suspended.status(), DeploymentStatus::Suspended);
    let resumed = manager
        .mutate_deployment(
            revision_one.id().job_id(),
            Some(3),
            &DeploymentMutation::Resume,
            "resume",
        )
        .expect("resume zero replica deployment");
    assert_eq!(resumed.status(), DeploymentStatus::Ready);
    assert_eq!(resumed.desired_replicas(), 0);

    let revision_two = manager
        .create_revision(&service_revision_draft("v2"), "revision-two")
        .expect("create update revision");
    assert_eq!(revision_two.id().revision(), 2);
    let updated = manager
        .mutate_deployment(
            revision_one.id().job_id(),
            Some(4),
            &DeploymentMutation::Deploy {
                revision: 2,
                replicas: 3,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "deploy-two",
        )
        .expect("deploy new revision");
    let rolled_back = manager
        .mutate_deployment(
            revision_one.id().job_id(),
            Some(updated.generation()),
            &DeploymentMutation::Rollback { revision: 1 },
            "rollback-one",
        )
        .expect("rollback execution revision");
    assert_eq!(rolled_back.revision_id(), revision_one.id());
    assert_eq!(rolled_back.desired_replicas(), 3);
    assert!(!rolled_back.suspended());
    drop(manager);

    let reopened = service(&directory);
    assert_eq!(
        reopened.get_revision(revision_two.id()).unwrap(),
        Some(revision_two)
    );
    assert_eq!(
        reopened.get_deployment(revision_one.id().job_id()).unwrap(),
        Some(rolled_back.clone())
    );
    assert_eq!(
        reopened
            .mutate_deployment(
                revision_one.id().job_id(),
                Some(updated.generation()),
                &DeploymentMutation::Rollback { revision: 1 },
                "rollback-one",
            )
            .unwrap(),
        rolled_back
    );
    let deleted = reopened
        .mutate_deployment(
            revision_one.id().job_id(),
            Some(rolled_back.generation()),
            &DeploymentMutation::Delete,
            "delete-deployment",
        )
        .expect("delete deployment");
    assert_eq!(deleted.status(), DeploymentStatus::Deleted);
    assert!(
        reopened
            .delete(revision_one.id().job_id())
            .expect("delete inactive job")
    );
}

#[test]
fn invalid_revision_submission_does_not_reserve_its_job_or_idempotency_key() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let mut invalid_json = serde_json::to_value(service_revision_draft("v1")).unwrap();
    invalid_json
        .as_object_mut()
        .unwrap()
        .remove("resourceRequest");
    let invalid_draft: JobRevisionDraft = serde_json::from_value(invalid_json).unwrap();
    assert_eq!(
        manager.create_revision(&invalid_draft, "revision-retry"),
        Err(ExecutionStoreError::InvalidRequest)
    );
    assert_eq!(manager.get(invalid_draft.job_id()).unwrap(), None);

    let accepted = manager
        .create_revision(&service_revision_draft("v1"), "revision-retry")
        .expect("valid retry reuses key after validation failure");
    assert_eq!(accepted.id().revision(), 1);
}

#[test]
fn placement_spreads_replicas_deterministically_and_honors_worker_labels() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let mut draft = serde_json::to_value(service_revision_draft("v1")).unwrap();
    draft["executionRequirements"]["workerLabels"] = serde_json::json!({"pool": "blue"});
    let draft: JobRevisionDraft = serde_json::from_value(draft).unwrap();
    let revision = manager.create_revision(&draft, "revision").unwrap();
    let workers = [
        scheduling_worker(
            "worker-a",
            "host-a",
            500,
            512 * 1024 * 1024,
            500,
            512 * 1024 * 1024,
            BTreeMap::from([("pool".into(), "blue".into())]),
            true,
        ),
        scheduling_worker(
            "worker-b",
            "host-b",
            500,
            512 * 1024 * 1024,
            500,
            512 * 1024 * 1024,
            BTreeMap::from([("pool".into(), "blue".into())]),
            true,
        ),
        scheduling_worker(
            "worker-c",
            "host-c",
            500,
            512 * 1024 * 1024,
            500,
            512 * 1024 * 1024,
            BTreeMap::from([("pool".into(), "green".into())]),
            true,
        ),
    ];
    let deployment = manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &deployment_request(3),
            "deploy",
            &workers,
        )
        .unwrap();
    assert_eq!(deployment.status(), DeploymentStatus::Pending);
    assert_eq!(
        deployment.pending_reason(),
        Some(DeploymentPendingReason::ServiceStarting)
    );
    let assignments = manager
        .list_allocations(revision.id().job_id())
        .unwrap()
        .into_iter()
        .map(|record| {
            assert_eq!(record.allocation().state(), AllocationState::Assigned);
            record
                .allocation()
                .assigned_worker_id()
                .unwrap()
                .as_str()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(assignments, ["worker-a", "worker-b", "worker-a"]);
}

#[test]
fn placement_enforces_aggregate_shared_host_capacity() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(&service_revision_draft("v1"), "revision")
        .unwrap();
    let workers = [
        scheduling_worker(
            "worker-a",
            "shared-host",
            500,
            512 * 1024 * 1024,
            250,
            256 * 1024 * 1024,
            BTreeMap::new(),
            true,
        ),
        scheduling_worker(
            "worker-b",
            "shared-host",
            500,
            512 * 1024 * 1024,
            250,
            256 * 1024 * 1024,
            BTreeMap::new(),
            true,
        ),
    ];
    let deployment = manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &deployment_request(2),
            "deploy",
            &workers,
        )
        .unwrap();
    assert_eq!(deployment.status(), DeploymentStatus::Pending);
    assert_eq!(
        deployment.pending_reason(),
        Some(DeploymentPendingReason::SharedHostCapacityExhausted)
    );
    assert_eq!(
        manager
            .list_allocations(revision.id().job_id())
            .unwrap()
            .iter()
            .filter(|record| record.allocation().state() == AllocationState::Assigned)
            .count(),
        1
    );
}

#[test]
fn placement_never_downgrades_unavailable_execution_protection() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let mut draft = serde_json::to_value(service_revision_draft("limited")).unwrap();
    draft["definition"]["resourceLimits"] = serde_json::json!({
        "cpuQuotaUs": 25_000,
        "cpuPeriodUs": 100_000
    });
    let draft: JobRevisionDraft = serde_json::from_value(draft).unwrap();
    let revision = manager.create_revision(&draft, "revision").unwrap();
    let macos_worker = scheduling_worker_with_capabilities(
        "macos-worker",
        "macos-host",
        500,
        512 * 1024 * 1024,
        500,
        512 * 1024 * 1024,
        BTreeMap::new(),
        true,
        OperatingSystem::Macos,
        BTreeSet::new(),
    );
    let deployment = manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &deployment_request(1),
            "deploy",
            &[macos_worker],
        )
        .unwrap();
    assert_eq!(deployment.status(), DeploymentStatus::Pending);
    assert_eq!(
        deployment.pending_reason(),
        Some(DeploymentPendingReason::RequiredProtectionUnavailable)
    );
    assert_eq!(
        manager.list_allocations(revision.id().job_id()).unwrap()[0]
            .allocation()
            .state(),
        AllocationState::Pending
    );
}

#[test]
fn uncertain_allocations_keep_reservations_during_replacement_placement() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(&service_revision_draft("v1"), "revision")
        .unwrap();
    let initial_worker = scheduling_worker(
        "worker-a",
        "shared-host",
        500,
        512 * 1024 * 1024,
        250,
        256 * 1024 * 1024,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &deployment_request(1),
            "deploy",
            std::slice::from_ref(&initial_worker),
        )
        .unwrap();
    let worker_port: Arc<dyn WorkerPort> = Arc::new(ServiceRunner {
        worker_id: WorkerId::new("worker-a").unwrap(),
        status: initial_worker,
        starts: AtomicUsize::new(0),
        disconnected: AtomicBool::new(false),
        runs: Mutex::new(BTreeMap::new()),
        stale_inspection: Mutex::new(None),
    });
    manager
        .reconcile_services(std::slice::from_ref(&worker_port))
        .unwrap();

    let pending = manager
        .reconcile_deployment_placement(revision.id().job_id(), &[])
        .unwrap()
        .unwrap();
    assert_eq!(pending.status(), DeploymentStatus::Pending);
    assert_eq!(
        pending.pending_reason(),
        Some(DeploymentPendingReason::WorkerDisconnected)
    );
    let allocations = manager.list_allocations(revision.id().job_id()).unwrap();
    assert_eq!(allocations.len(), 2);
    assert_eq!(
        allocations
            .iter()
            .filter(|record| record.allocation().state() == AllocationState::Uncertain)
            .count(),
        1
    );
    assert_eq!(
        allocations
            .iter()
            .filter(|record| record.allocation().state() == AllocationState::Pending)
            .count(),
        1
    );

    let replacement_worker = scheduling_worker(
        "worker-b",
        "shared-host",
        500,
        512 * 1024 * 1024,
        250,
        256 * 1024 * 1024,
        BTreeMap::new(),
        true,
    );
    let placed = manager
        .reconcile_deployment_placement(revision.id().job_id(), &[replacement_worker])
        .unwrap()
        .unwrap();
    assert_eq!(placed.status(), DeploymentStatus::Pending);
    assert_eq!(
        placed.pending_reason(),
        Some(DeploymentPendingReason::SharedHostCapacityExhausted)
    );
    let allocations = manager.list_allocations(revision.id().job_id()).unwrap();
    assert_eq!(
        allocations
            .iter()
            .filter(|record| record.allocation().state() == AllocationState::Uncertain)
            .count(),
        1
    );
    assert_eq!(
        allocations
            .iter()
            .filter(|record| record.allocation().state() == AllocationState::Pending)
            .count(),
        1
    );
}

#[test]
fn deleting_deployment_releases_allocations_without_started_runs() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let revision = manager
        .create_revision(&service_revision_draft("v1"), "revision")
        .unwrap();
    let worker = scheduling_worker(
        "worker-a",
        "host-a",
        500,
        512 * 1024 * 1024,
        500,
        512 * 1024 * 1024,
        BTreeMap::new(),
        true,
    );
    manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            None,
            &deployment_request(1),
            "deploy",
            &[worker],
        )
        .unwrap();

    let deleting = manager
        .mutate_deployment_with_workers(
            revision.id().job_id(),
            Some(1),
            &DeploymentMutation::Delete,
            "delete",
            &[],
        )
        .unwrap();
    assert_eq!(deleting.status(), DeploymentStatus::Deleted);
    assert_eq!(deleting.desired_replicas(), 0);
    assert_eq!(
        manager.list_allocations(revision.id().job_id()).unwrap()[0]
            .allocation()
            .state(),
        AllocationState::Released
    );
    assert_eq!(
        manager
            .reconcile_deployment_placement(revision.id().job_id(), &[])
            .unwrap()
            .unwrap()
            .status(),
        DeploymentStatus::Deleted
    );
}

#[test]
fn concurrent_deployments_cannot_oversubscribe_a_shared_resource_domain() {
    let directory = TempDir::new();
    let manager = Arc::new(service(&directory));
    let first_revision = manager
        .create_revision(&service_revision_draft("one"), "revision-one")
        .unwrap();
    let mut second_draft = serde_json::to_value(service_revision_draft("two")).unwrap();
    second_draft["definition"]["id"] = serde_json::json!("web-two");
    second_draft["workload"]["identity"]["name"] = serde_json::json!("web-two");
    let second_draft: JobRevisionDraft = serde_json::from_value(second_draft).unwrap();
    let second_revision = manager
        .create_revision(&second_draft, "revision-two")
        .unwrap();
    let workers = vec![
        scheduling_worker(
            "worker-a",
            "shared-host",
            500,
            512 * 1024 * 1024,
            250,
            256 * 1024 * 1024,
            BTreeMap::new(),
            true,
        ),
        scheduling_worker(
            "worker-b",
            "shared-host",
            500,
            512 * 1024 * 1024,
            250,
            256 * 1024 * 1024,
            BTreeMap::new(),
            true,
        ),
    ];
    let barrier = Arc::new(Barrier::new(3));
    let deploy = |revision: conductor_core::JobRevision, key: &'static str| {
        let manager = Arc::clone(&manager);
        let barrier = Arc::clone(&barrier);
        let workers = workers.clone();
        thread::spawn(move || {
            barrier.wait();
            manager.mutate_deployment_with_workers(
                revision.id().job_id(),
                None,
                &deployment_request(1),
                key,
                &workers,
            )
        })
    };
    let first = deploy(first_revision, "deploy-one");
    let second = deploy(second_revision, "deploy-two");
    barrier.wait();
    let deployments = [
        first.join().unwrap().unwrap(),
        second.join().unwrap().unwrap(),
    ];
    assert_eq!(
        deployments
            .iter()
            .filter(|deployment| deployment.pending_reason()
                == Some(DeploymentPendingReason::ServiceStarting))
            .count(),
        1
    );
    assert_eq!(
        deployments
            .iter()
            .filter(|deployment| {
                deployment.pending_reason()
                    == Some(DeploymentPendingReason::SharedHostCapacityExhausted)
            })
            .count(),
        1
    );
}

#[test]
fn concurrent_deployment_updates_cannot_overwrite_the_same_generation() {
    let directory = TempDir::new();
    let manager = Arc::new(service(&directory));
    let revision = manager
        .create_revision(&service_revision_draft("v1"), "revision")
        .unwrap();
    manager
        .mutate_deployment(
            revision.id().job_id(),
            None,
            &DeploymentMutation::Deploy {
                revision: 1,
                replicas: 2,
                strategy: conductor_core::DeploymentUpdateStrategy::default(),
            },
            "deploy",
        )
        .unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let scale = {
        let manager = Arc::clone(&manager);
        let barrier = Arc::clone(&barrier);
        let job_id = revision.id().job_id().clone();
        thread::spawn(move || {
            barrier.wait();
            manager.mutate_deployment(
                &job_id,
                Some(1),
                &DeploymentMutation::Scale { replicas: 4 },
                "concurrent-scale",
            )
        })
    };
    let suspend = {
        let manager = Arc::clone(&manager);
        let barrier = Arc::clone(&barrier);
        let job_id = revision.id().job_id().clone();
        thread::spawn(move || {
            barrier.wait();
            manager.mutate_deployment(
                &job_id,
                Some(1),
                &DeploymentMutation::Suspend,
                "concurrent-suspend",
            )
        })
    };
    barrier.wait();
    let results = [scale.join().unwrap(), suspend.join().unwrap()];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(ExecutionStoreError::PreconditionFailed))
            .count(),
        1
    );
}

#[test]
fn schema_five_migrates_execution_tables_without_losing_jobs() {
    let directory = TempDir::new();
    let manager = service(&directory);
    manager.register(job("kept-job")).unwrap();
    drop(manager);
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch(
            "DROP TABLE execution_idempotency;
             DROP TABLE job_deployments;
             DROP TABLE job_revisions;
             DROP TABLE allocations;
             DROP TABLE service_instances;
             DROP TABLE scheduling_sequence;
             PRAGMA user_version = 5;",
        )
        .unwrap();
    drop(connection);

    let migrated = service(&directory);
    assert_eq!(
        migrated.get(&JobId::new("kept-job").unwrap()).unwrap(),
        Some(job("kept-job"))
    );
    drop(migrated);
    let connection = Connection::open(directory.database()).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 11);
    assert!(table_exists(&connection, "job_revisions"));
    assert!(table_exists(&connection, "job_deployments"));
    assert!(table_exists(&connection, "execution_idempotency"));
}

#[test]
fn schema_six_adds_durable_scheduling_tables() {
    let directory = TempDir::new();
    let manager = service(&directory);
    drop(manager);
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch(
            "DROP TABLE allocations;
             DROP TABLE service_instances;
             DROP TABLE scheduling_sequence;
             PRAGMA user_version = 6;",
        )
        .unwrap();
    drop(connection);

    let _migrated = service(&directory);
    let connection = Connection::open(directory.database()).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 11);
    assert!(table_exists(&connection, "service_instances"));
    assert!(table_exists(&connection, "allocations"));
    assert!(table_exists(&connection, "scheduling_sequence"));
}

#[test]
fn schema_seven_migrates_service_attempt_state() {
    let directory = TempDir::new();
    let manager = service(&directory);
    drop(manager);
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch("DROP TABLE service_attempts; PRAGMA user_version = 7;")
        .unwrap();
    drop(connection);

    let _migrated = service(&directory);
    let connection = Connection::open(directory.database()).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 11);
    assert!(table_exists(&connection, "service_attempts"));
}

fn all_jobs(manager: &JobService) -> Vec<JobSpec> {
    let mut jobs = Vec::new();
    let mut cursor = None;
    loop {
        let page = manager
            .list_page(100, cursor.as_deref())
            .expect("read bounded job page");
        jobs.extend(page.items);
        let Some(next) = page.next_cursor else {
            return jobs;
        };
        cursor = Some(next);
    }
}

#[test]
fn jobs_and_timestamps_survive_reopen_and_deleted_ids_remain_reserved() {
    let directory = TempDir::new();
    let manager = service(&directory);
    manager.register(job("zeta")).expect("register zeta");
    manager.register(job("alpha")).expect("register alpha");
    assert_eq!(
        all_jobs(&manager)
            .iter()
            .map(|value| value.id().as_str())
            .collect::<Vec<_>>(),
        ["alpha", "zeta"]
    );

    let connection = Connection::open(directory.database()).expect("inspect manager database");
    let (created_at, deleted_at): (i64, Option<i64>) = connection
        .query_row(
            "SELECT created_at_ms, deleted_at_ms FROM jobs WHERE id = 'zeta'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("persist job timestamps");
    assert!(created_at > 0);
    assert_eq!(deleted_at, None);
    drop(connection);

    assert!(manager.delete(&JobId::new("zeta").unwrap()).unwrap());
    drop(manager);

    let reopened = service(&directory);
    assert_eq!(
        reopened.get(&JobId::new("alpha").unwrap()).unwrap(),
        Some(job("alpha"))
    );
    assert!(
        reopened
            .get(&JobId::new("zeta").unwrap())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reopened.register(job("zeta")),
        Err(ManagerError::DuplicateJob)
    );
    assert_eq!(all_jobs(&reopened).len(), 1);

    let connection = Connection::open(directory.database()).expect("inspect reopened database");
    let (persisted_created_at, persisted_deleted_at): (i64, Option<i64>) = connection
        .query_row(
            "SELECT created_at_ms, deleted_at_ms FROM jobs WHERE id = 'zeta'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("retain deleted row");
    assert_eq!(persisted_created_at, created_at);
    assert!(persisted_deleted_at.is_some());
}

#[test]
fn legacy_schema_migrates_transactionally_and_repeated_startup_is_stable() {
    let directory = TempDir::new();
    let connection = Connection::open(directory.database()).expect("create v0 fixture database");
    connection
        .execute_batch(
            "CREATE TABLE jobs (id TEXT PRIMARY KEY NOT NULL, spec_json TEXT NOT NULL, created_at_ms INTEGER NOT NULL);
             INSERT INTO jobs (id, spec_json, created_at_ms) VALUES ('legacy', '{\"id\":\"legacy\",\"command\":[\"/usr/bin/true\"],\"cwd\":\"/tmp\",\"env\":{},\"inheritEnv\":false,\"timeoutMs\":null,\"terminationGraceMs\":5000}', 1234);",
        )
        .expect("write legacy fixture");
    drop(connection);

    let manager = service(&directory);
    assert_eq!(
        manager.get(&JobId::new("legacy").unwrap()).unwrap(),
        Some(job("legacy"))
    );
    drop(manager);

    let reopened = service(&directory);
    assert_eq!(all_jobs(&reopened), vec![job("legacy")]);
    drop(reopened);
    let connection = Connection::open(directory.database()).expect("inspect migrated database");
    let (version, created_at, deleted_at): (i64, i64, Option<i64>) = connection
        .query_row(
            "SELECT (SELECT user_version FROM pragma_user_version), created_at_ms, deleted_at_ms FROM jobs WHERE id = 'legacy'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read migrated schema and fixture");
    assert_eq!(version, 11);
    assert_eq!(created_at, 1234);
    assert_eq!(deleted_at, None);
}

#[test]
fn schema_two_migration_adds_idempotency_and_indexed_run_order() {
    let directory = TempDir::new();
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE jobs (
                id TEXT PRIMARY KEY NOT NULL,
                spec_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL CHECK (created_at_ms >= 0),
                deleted_at_ms INTEGER CHECK (deleted_at_ms IS NULL OR deleted_at_ms >= 0)
             );
             CREATE TABLE runs (
                run_id TEXT PRIMARY KEY NOT NULL,
                job_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                job_spec_json TEXT NOT NULL,
                observation_json TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK (revision > 0),
                state TEXT NOT NULL
             );
             CREATE INDEX runs_by_state_worker ON runs(state, worker_id);
             CREATE INDEX runs_by_job_created ON runs(job_id, run_id);
             INSERT INTO runs (run_id, job_id, worker_id, job_spec_json, observation_json, revision, state)
                 VALUES ('legacy-run', 'legacy-job', 'legacy-worker', 'legacy-spec', 'legacy-observation', 4, 'succeeded');
             CREATE TABLE run_sequence (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL);
             INSERT INTO run_sequence (id, value) VALUES (1, 0);
             PRAGMA user_version = 2;",
        )
        .unwrap();
    drop(connection);

    let manager = service(&directory);
    drop(manager);
    let connection = Connection::open(directory.database()).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    let columns = connection
        .prepare("PRAGMA table_info(runs)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(version, 11);
    assert!(columns.iter().any(|column| column == "idempotency_key"));
    assert!(columns.iter().any(|column| column == "request_fingerprint"));
    assert!(columns.iter().any(|column| column == "run_order"));
    let history: (String, String, String, String, String, i64, String, i64) = connection
        .query_row(
            "SELECT run_id, job_id, worker_id, job_spec_json, observation_json, revision, state, run_order FROM runs WHERE run_id = 'legacy-run'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?)),
        )
        .unwrap();
    assert_eq!(
        history,
        (
            "legacy-run".into(),
            "legacy-job".into(),
            "legacy-worker".into(),
            "legacy-spec".into(),
            "legacy-observation".into(),
            4,
            "succeeded".into(),
            1,
        )
    );
}

#[test]
fn schema_four_identity_layout_migrates_with_command_outbox_without_losing_tokens() {
    let directory = TempDir::new();
    let repository = SqliteJobRepository::open(directory.database()).unwrap();
    let worker_id = WorkerId::new("identity-worker").unwrap();
    let token_hash = [7_u8; 32];
    repository
        .create_enrollment_credential(&token_hash, &worker_id, 10_000)
        .unwrap();
    drop(repository);

    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch(
            "DROP TABLE manager_commands;
             DROP TABLE command_sequence;
             DROP TABLE execution_idempotency;
             DROP TABLE job_deployments;
             DROP TABLE job_revisions;
             DROP TABLE allocations;
             DROP TABLE service_instances;
             DROP TABLE scheduling_sequence;
             PRAGMA user_version = 4;",
        )
        .unwrap();
    drop(connection);

    let migrated = SqliteJobRepository::open(directory.database()).unwrap();
    assert_eq!(
        migrated
            .worker_for_enrollment_credential(&token_hash, 1)
            .unwrap(),
        Some(worker_id)
    );
    let connection = Connection::open(directory.database()).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    let outbox_exists = table_exists(&connection, "manager_commands");
    assert_eq!(version, 11);
    assert!(outbox_exists);
}

#[test]
fn schema_four_command_layout_migrates_with_identity_tables_without_resetting_sequence() {
    let directory = TempDir::new();
    let repository = SqliteJobRepository::open(directory.database()).unwrap();
    drop(repository);

    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch(
            "UPDATE command_sequence SET value = 17;
             DROP TABLE worker_identities;
             DROP TABLE worker_certificates;
             DROP TABLE enrollment_credentials;
             DROP TABLE execution_idempotency;
             DROP TABLE job_deployments;
             DROP TABLE job_revisions;
             DROP TABLE allocations;
             DROP TABLE service_instances;
             DROP TABLE scheduling_sequence;
             PRAGMA user_version = 4;",
        )
        .unwrap();
    drop(connection);

    let _migrated = SqliteJobRepository::open(directory.database()).unwrap();
    let connection = Connection::open(directory.database()).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    let sequence: i64 = connection
        .query_row(
            "SELECT value FROM command_sequence WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let identity_exists = table_exists(&connection, "worker_identities");
    assert_eq!(version, 11);
    assert_eq!(sequence, 17);
    assert!(identity_exists);
}

fn table_exists(connection: &Connection, name: &str) -> bool {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [name],
            |row| row.get(0),
        )
        .unwrap()
}

#[test]
fn corrupt_and_newer_databases_fail_without_resetting_existing_bytes() {
    let directory = TempDir::new();
    let corrupt = directory.database();
    let bytes = b"not a sqlite database";
    fs::write(&corrupt, bytes).expect("write corrupt fixture");
    assert!(SqliteJobRepository::open(&corrupt).is_err());
    assert_eq!(fs::read(&corrupt).unwrap(), bytes);

    let newer = directory.0.join("newer.sqlite3");
    let connection = Connection::open(&newer).expect("create future fixture");
    connection
        .execute_batch("CREATE TABLE marker (value TEXT); INSERT INTO marker VALUES ('keep'); PRAGMA user_version = 77;")
        .expect("write future schema fixture");
    drop(connection);
    assert!(matches!(
        SqliteJobRepository::open(&newer),
        Err(RepositoryOpenError::UnsupportedSchemaVersion(77))
    ));
    let connection = Connection::open(newer).expect("reopen future fixture");
    let marker: String = connection
        .query_row("SELECT value FROM marker", [], |row| row.get(0))
        .expect("future data remains intact");
    assert_eq!(marker, "keep");
}

#[test]
fn concurrent_registration_has_one_winner_and_deterministic_conflicts() {
    const WRITERS: usize = 8;
    let directory = TempDir::new();
    let manager = Arc::new(service(&directory));
    let barrier = Arc::new(Barrier::new(WRITERS));
    let writers = (0..WRITERS)
        .map(|_| {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                manager.register(job("concurrent"))
            })
        })
        .collect::<Vec<_>>();
    let results = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(ManagerError::DuplicateJob))
            .count(),
        WRITERS - 1
    );
    assert_eq!(all_jobs(&manager).len(), 1);
}

#[test]
fn active_job_limit_is_enforced_and_deletion_releases_a_slot() {
    let directory = TempDir::new();
    let manager = service(&directory);
    for index in 0..MAX_JOBS {
        manager
            .register(job(&format!("job-{index}")))
            .expect("register within active job limit");
    }
    assert_eq!(all_jobs(&manager).len(), MAX_JOBS);
    assert_eq!(
        manager.register(job("over-limit")),
        Err(ManagerError::JobLimitReached)
    );
    assert!(manager.delete(&JobId::new("job-0").unwrap()).unwrap());
    manager
        .register(job("replacement"))
        .expect("deleted job frees active capacity");
    assert_eq!(all_jobs(&manager).len(), MAX_JOBS);
}

#[test]
fn sqlite_busy_errors_are_bounded() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let blocker = Connection::open(directory.database()).expect("open busy-lock connection");
    blocker
        .execute_batch("BEGIN IMMEDIATE")
        .expect("hold SQLite write lock");

    let started = Instant::now();
    assert_eq!(
        manager.register(job("busy")),
        Err(ManagerError::StorageUnavailable)
    );
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(200), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    blocker
        .execute_batch("ROLLBACK")
        .expect("release SQLite lock");
    assert!(manager.register(job("after-busy")).is_ok());
}

#[test]
fn manager_admission_reserves_job_and_worker_capacity_transactionally() {
    let directory = TempDir::new();
    let repository = SqliteJobRepository::open(directory.database()).unwrap();
    repository.insert(job("first")).unwrap();
    repository.insert(job("second")).unwrap();
    let worker = WorkerId::new("local").unwrap();
    let first = repository
        .create_run(&JobId::new("first").unwrap(), &worker, 1, None)
        .unwrap();
    assert_eq!(first.id().as_str(), "run-1");
    assert_eq!(
        repository.get_run(first.id()).unwrap().unwrap().state(),
        RunState::Requested
    );
    assert_eq!(
        repository.delete(first.job().id()),
        Err(conductor_manager::JobStoreError::ActiveRunConflict)
    );
    assert_eq!(
        repository.create_run(&JobId::new("second").unwrap(), &worker, 1, None),
        Err(RunStoreError::CapacityReached)
    );

    let current = repository.get_run(first.id()).unwrap().unwrap();
    let completed = current
        .next_revision(
            RunState::Failed,
            Some(current.requested_at_ms()),
            Some(current.requested_at_ms() + 1),
            Some(17),
            None,
            Some(RunFailureReason::NonZeroExit),
        )
        .unwrap();
    repository.record_observation(&completed).unwrap();
    assert_eq!(repository.get_run(first.id()).unwrap().unwrap(), completed);

    let second = repository
        .create_run(&JobId::new("second").unwrap(), &worker, 1, None)
        .unwrap();
    assert_eq!(second.id().as_str(), "run-2");
    assert_eq!(
        repository
            .list_runs(
                Some(&JobId::new("second").unwrap()),
                Some(RunState::Requested),
                10
            )
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn idempotency_keys_replay_across_capacity_completion_and_database_reopen() {
    let directory = TempDir::new();
    let repository = SqliteJobRepository::open(directory.database()).unwrap();
    repository.insert(job("first")).unwrap();
    repository.insert(job("other")).unwrap();
    let worker = WorkerId::new("local").unwrap();
    let first = repository
        .create_run(&JobId::new("first").unwrap(), &worker, 1, Some("retry-1"))
        .unwrap();
    assert!(first.replay.is_none());
    let replay = repository
        .create_run(&JobId::new("first").unwrap(), &worker, 1, Some("retry-1"))
        .unwrap();
    assert_eq!(replay.spec.id(), first.spec.id());
    assert_eq!(replay.replay.unwrap().state(), RunState::Requested);
    assert_eq!(
        repository.create_run(&JobId::new("other").unwrap(), &worker, 1, Some("retry-1")),
        Err(RunStoreError::IdempotencyConflict)
    );

    let requested = repository.get_run(first.spec.id()).unwrap().unwrap();
    let terminal = requested
        .next_revision(
            RunState::Succeeded,
            Some(requested.requested_at_ms()),
            Some(requested.requested_at_ms() + 1),
            Some(0),
            None,
            None,
        )
        .unwrap();
    repository.record_observation(&terminal).unwrap();
    drop(repository);

    let reopened = SqliteJobRepository::open(directory.database()).unwrap();
    let replay = reopened
        .create_run(&JobId::new("first").unwrap(), &worker, 1, Some("retry-1"))
        .unwrap();
    assert_eq!(replay.spec.id(), first.spec.id());
    assert_eq!(replay.replay.unwrap(), terminal);
}

#[test]
fn version_one_manager_database_migrates_runs_and_accepts_timeout_specs() {
    let directory = TempDir::new();
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE jobs (
            id TEXT PRIMARY KEY NOT NULL,
            spec_json TEXT NOT NULL,
            created_at_ms INTEGER NOT NULL,
            deleted_at_ms INTEGER
        );
        PRAGMA user_version = 1;",
        )
        .unwrap();
    drop(connection);

    let repository = SqliteJobRepository::open(directory.database()).unwrap();
    let timed = JobSpec::new(
        JobId::new("timed").unwrap(),
        vec!["/usr/bin/true".into()],
        "/tmp".into(),
        BTreeMap::new(),
        false,
        Some(1_000),
        5_000,
    )
    .unwrap();
    repository.insert(timed).unwrap();
    assert_eq!(
        repository
            .create_run(
                &JobId::new("timed").unwrap(),
                &WorkerId::new("worker").unwrap(),
                1,
                None
            )
            .unwrap()
            .job()
            .timeout_ms(),
        Some(1_000)
    );
    let connection = Connection::open(directory.database()).unwrap();
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 11);
}

struct FullWorker(WorkerId);

impl WorkerPort for FullWorker {
    fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.0.clone(), 1, true)
    }

    fn start(&self, _command: StartRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::CapacityReached)
    }

    fn inspect(&self, _command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        Ok(None)
    }

    fn stop(&self, _command: conductor_core::StopRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn read_logs(
        &self,
        _command: conductor_core::ReadRunLogs,
    ) -> Result<conductor_core::RunLogChunk, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }
}

struct UncertainWorker(WorkerId);

impl WorkerPort for UncertainWorker {
    fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.0.clone(), 1, true)
    }

    fn start(&self, _command: StartRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn inspect(&self, _command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        Ok(None)
    }

    fn stop(&self, _command: conductor_core::StopRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn read_logs(
        &self,
        _command: conductor_core::ReadRunLogs,
    ) -> Result<conductor_core::RunLogChunk, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn status(&self) -> Result<conductor_core::WorkerStatus, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }
}

struct StopRecoveryWorker {
    worker_id: WorkerId,
    stopping: RunObservation,
    delivered: std::sync::Mutex<Vec<CommandId>>,
}

impl WorkerPort for StopRecoveryWorker {
    fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.worker_id.clone(), 1, true)
    }

    fn start(&self, _command: StartRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn dispatch_command(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<RunObservation, WorkerPortError> {
        self.delivered
            .lock()
            .unwrap()
            .push(envelope.command_id.clone());
        match envelope.command {
            WorkerCommand::StopRun(command) if command.run_id() == self.stopping.run_id() => self
                .stopping
                .next_revision(
                    RunState::Cancelled,
                    self.stopping.started_at_ms(),
                    Some(self.stopping.requested_at_ms() + 1),
                    None,
                    Some(15),
                    None,
                )
                .map(|observation| {
                    observation
                        .with_termination_reason(conductor_core::RunTerminationReason::Cancelled)
                })
                .ok_or(WorkerPortError::RunConflict),
            _ => Err(WorkerPortError::RunConflict),
        }
    }

    fn inspect(&self, _command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        Ok(None)
    }

    fn stop(&self, _command: StopRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn read_logs(
        &self,
        _command: ReadRunLogs,
    ) -> Result<conductor_core::RunLogChunk, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }
}

struct RetryWorker {
    worker_id: WorkerId,
    deliveries: AtomicUsize,
    attempts: AtomicUsize,
    commands: std::sync::Mutex<Vec<CommandId>>,
    runs: std::sync::Mutex<BTreeMap<RunId, RunObservation>>,
}

impl WorkerPort for RetryWorker {
    fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.worker_id.clone(), 1, true).with_incarnation(5)
    }

    fn start(&self, command: StartRun) -> Result<RunObservation, WorkerPortError> {
        let spec = command.spec();
        let requested = RunObservation::requested(spec, 10).with_worker_incarnation(5);
        let starting = requested
            .next_revision(RunState::Starting, Some(11), None, None, None, None)
            .unwrap();
        let running = starting
            .next_revision(RunState::Running, Some(11), None, None, None, None)
            .unwrap();
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.runs
            .lock()
            .unwrap()
            .insert(spec.id().clone(), running.clone());
        Ok(running)
    }

    fn dispatch_command(
        &self,
        envelope: CommandEnvelope,
    ) -> Result<RunObservation, WorkerPortError> {
        self.commands
            .lock()
            .unwrap()
            .push(envelope.command_id.clone());
        let observation = match envelope.command {
            WorkerCommand::StartRun(command) => {
                let run_id = command.spec().id().clone();
                if let Some(observation) = self.runs.lock().unwrap().get(&run_id).cloned() {
                    observation
                } else {
                    self.start(*command)?
                }
            }
            _ => return Err(WorkerPortError::Unavailable),
        };
        if self.deliveries.fetch_add(1, Ordering::SeqCst) == 0 {
            // The worker accepted the command, but the manager lost the response.
            return Err(WorkerPortError::Unavailable);
        }
        Ok(observation)
    }

    fn inspect(&self, command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        Ok(self.runs.lock().unwrap().get(command.run_id()).cloned())
    }

    fn stop(&self, _command: StopRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn read_logs(&self, _command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }
}

#[test]
fn retry_after_lost_start_response_reuses_command_without_repeating_attempt() {
    let directory = TempDir::new();
    let manager = service(&directory);
    manager.register(job("retry-start")).unwrap();
    let worker_id = WorkerId::new("retry-worker").unwrap();
    let worker = RetryWorker {
        worker_id: worker_id.clone(),
        deliveries: AtomicUsize::new(0),
        attempts: AtomicUsize::new(0),
        commands: std::sync::Mutex::new(Vec::new()),
        runs: std::sync::Mutex::new(BTreeMap::new()),
    };

    assert_eq!(
        manager.start_run_with_key(
            &JobId::new("retry-start").unwrap(),
            &worker_id,
            1,
            Some("retry-key"),
            &worker,
        ),
        Err(ManagerError::WorkerUnavailable)
    );
    drop(manager);
    let manager = service(&directory);
    let started = manager
        .start_run_with_key(
            &JobId::new("retry-start").unwrap(),
            &worker_id,
            1,
            Some("retry-key"),
            &worker,
        )
        .unwrap();

    assert_eq!(started.state(), RunState::Running);
    assert_eq!(worker.attempts.load(Ordering::SeqCst), 1);
    let commands = worker.commands.lock().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0], commands[1]);
    assert!(
        SqliteJobRepository::open(directory.database())
            .unwrap()
            .pending_commands(&worker_id, 5, 32)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn startup_reconciliation_resumes_pending_start_without_repeating_attempt() {
    let directory = TempDir::new();
    let manager = service(&directory);
    manager.register(job("recover-pending-start")).unwrap();
    let worker_id = WorkerId::new("retry-worker").unwrap();
    let worker = RetryWorker {
        worker_id: worker_id.clone(),
        deliveries: AtomicUsize::new(0),
        attempts: AtomicUsize::new(0),
        commands: std::sync::Mutex::new(Vec::new()),
        runs: std::sync::Mutex::new(BTreeMap::new()),
    };

    assert_eq!(
        manager.start_run_with_key(
            &JobId::new("recover-pending-start").unwrap(),
            &worker_id,
            1,
            Some("recover-key"),
            &worker,
        ),
        Err(ManagerError::WorkerUnavailable)
    );
    drop(manager);

    let manager = service(&directory);
    manager.register(job("recovery-capacity-probe")).unwrap();
    manager.reconcile(&worker).unwrap();
    let run = manager
        .list_runs(Some(&JobId::new("recover-pending-start").unwrap()), None, 1)
        .unwrap()
        .pop()
        .expect("recovered run");
    assert_eq!(run.state(), RunState::Running);
    assert_eq!(worker.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        manager.start_run(
            &JobId::new("recovery-capacity-probe").unwrap(),
            &worker_id,
            1,
            &worker,
        ),
        Err(ManagerError::CapacityReached)
    );
    assert_eq!(worker.attempts.load(Ordering::SeqCst), 1);
    let commands = worker.commands.lock().unwrap();
    assert_eq!(commands.len(), 2);
    assert_eq!(commands[0], commands[1]);
    assert!(
        SqliteJobRepository::open(directory.database())
            .unwrap()
            .pending_commands(&worker_id, 5, 32)
            .unwrap()
            .is_empty()
    );
}

struct LogOnlyWorker(WorkerId);

impl WorkerPort for LogOnlyWorker {
    fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.0.clone(), 1, true)
    }

    fn start(&self, _command: StartRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn inspect(&self, _command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        Ok(None)
    }

    fn stop(&self, _command: conductor_core::StopRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn read_logs(&self, command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError> {
        Ok(RunLogChunk::new(
            command.run_id().clone(),
            command.stream(),
            command.offset(),
            command.offset() + 2,
            vec![0, 255],
            RunLogStatus::new(false, false, false),
        ))
    }
}

struct RecoveryWorker {
    worker_id: WorkerId,
    incarnation: u64,
    observation: Option<RunObservation>,
}

impl WorkerPort for RecoveryWorker {
    fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor::new(self.worker_id.clone(), 1, true).with_incarnation(self.incarnation)
    }

    fn start(&self, command: StartRun) -> Result<RunObservation, WorkerPortError> {
        Ok(RunObservation::requested(command.spec(), 10))
    }

    fn inspect(&self, _command: InspectRun) -> Result<Option<RunObservation>, WorkerPortError> {
        Ok(self.observation.clone())
    }

    fn stop(&self, _command: conductor_core::StopRun) -> Result<RunObservation, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }

    fn read_logs(&self, _command: ReadRunLogs) -> Result<RunLogChunk, WorkerPortError> {
        Err(WorkerPortError::Unavailable)
    }
}

#[test]
fn startup_reconciliation_repairs_terminal_state_and_marks_missing_worker_runs_unknown() {
    for worker_has_run in [true, false] {
        let directory = TempDir::new();
        let manager = service(&directory);
        manager.register(job("recover-me")).unwrap();
        let worker_id = WorkerId::new("local").unwrap();
        let admission_worker = RecoveryWorker {
            worker_id: worker_id.clone(),
            incarnation: 0,
            observation: None,
        };
        let requested = manager
            .start_run(
                &JobId::new("recover-me").unwrap(),
                &worker_id,
                1,
                &admission_worker,
            )
            .unwrap();
        let terminal = requested
            .next_revision(RunState::Succeeded, Some(11), Some(12), Some(0), None, None)
            .unwrap();
        let recovery_worker = RecoveryWorker {
            worker_id: worker_id.clone(),
            incarnation: 0,
            observation: worker_has_run.then_some(terminal),
        };

        manager.reconcile(&recovery_worker).unwrap();
        let recovered = manager
            .get_run(requested.run_id(), &recovery_worker)
            .unwrap()
            .unwrap();
        assert_eq!(
            recovered.state(),
            if worker_has_run {
                RunState::Succeeded
            } else {
                RunState::Unknown
            }
        );
    }

    let directory = TempDir::new();
    let manager = service(&directory);
    manager.register(job("stale-worker")).unwrap();
    let worker_id = WorkerId::new("local").unwrap();
    let original = RecoveryWorker {
        worker_id: worker_id.clone(),
        incarnation: 0,
        observation: None,
    };
    let requested = manager
        .start_run(
            &JobId::new("stale-worker").unwrap(),
            &worker_id,
            1,
            &original,
        )
        .unwrap();
    let stale_running = requested
        .next_revision(RunState::Running, Some(11), None, None, None, None)
        .unwrap()
        .with_worker_incarnation(1);
    let restarted = RecoveryWorker {
        worker_id,
        incarnation: 2,
        observation: Some(stale_running),
    };
    manager.reconcile(&restarted).unwrap();
    assert_eq!(
        manager
            .get_run(requested.run_id(), &restarted)
            .unwrap()
            .unwrap()
            .state(),
        RunState::Unknown
    );
}

#[test]
fn manager_reads_logs_only_through_worker_port() {
    let directory = TempDir::new();
    let manager = JobService::open_sqlite(directory.database()).unwrap();
    let worker = LogOnlyWorker(WorkerId::new("worker").unwrap());
    let command = ReadRunLogs::new(
        conductor_core::RunId::new("opaque-run").unwrap(),
        LogStream::Stderr,
        12,
        2,
    );

    let chunk = manager.read_logs(command, &worker).unwrap();

    assert_eq!(chunk.stream(), LogStream::Stderr);
    assert_eq!(chunk.offset(), 12);
    assert_eq!(chunk.data(), [0, 255]);
    assert!(!chunk.stream_closed());
}

#[test]
fn manager_reports_unavailable_worker_status_without_opening_worker_storage() {
    let directory = TempDir::new();
    let manager = service(&directory);
    let worker = UncertainWorker(WorkerId::new("local").unwrap());
    assert_eq!(
        manager.worker_status(&worker),
        Err(ManagerError::WorkerUnavailable)
    );
}

#[test]
fn rejected_worker_admission_does_not_leave_a_stale_manager_reservation() {
    let directory = TempDir::new();
    let manager = JobService::open_sqlite(directory.database()).unwrap();
    manager.register(job("retryable")).unwrap();
    let worker_id = WorkerId::new("local").unwrap();
    let worker = FullWorker(worker_id.clone());
    for _ in 0..2 {
        assert_eq!(
            manager.start_run(&JobId::new("retryable").unwrap(), &worker_id, 1, &worker),
            Err(ManagerError::CapacityReached)
        );
    }
    let runs = manager
        .list_runs(
            Some(&JobId::new("retryable").unwrap()),
            Some(RunState::Failed),
            10,
        )
        .unwrap();
    assert!(
        runs.is_empty(),
        "rejected admission must not create a run record"
    );
    assert!(
        SqliteJobRepository::open(directory.database())
            .unwrap()
            .pending_commands(&worker_id, 0, 32)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn ambiguous_worker_dispatch_is_marked_unknown_and_keeps_its_reservation() {
    let directory = TempDir::new();
    let manager = JobService::open_sqlite(directory.database()).unwrap();
    manager.register(job("ambiguous")).unwrap();
    let worker_id = WorkerId::new("local").unwrap();
    let worker = UncertainWorker(worker_id.clone());
    assert_eq!(
        manager.start_run(&JobId::new("ambiguous").unwrap(), &worker_id, 1, &worker),
        Err(ManagerError::WorkerUnavailable)
    );
    let runs = manager
        .list_runs(
            Some(&JobId::new("ambiguous").unwrap()),
            Some(RunState::Unknown),
            10,
        )
        .unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(
        SqliteJobRepository::open(directory.database())
            .unwrap()
            .pending_commands(&worker_id, 0, 32)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        manager.start_run(&JobId::new("ambiguous").unwrap(), &worker_id, 1, &worker),
        Err(ManagerError::JobConflict)
    );
}

#[test]
fn stop_intent_is_durable_and_terminal_runs_are_unchanged() {
    let directory = TempDir::new();
    let repository = Arc::new(SqliteJobRepository::open(directory.database()).unwrap());
    repository.insert(job("stoppable")).unwrap();
    let worker_id = WorkerId::new("worker").unwrap();
    let spec = repository
        .create_run(&JobId::new("stoppable").unwrap(), &worker_id, 1, None)
        .unwrap();
    let manager = JobService::new(repository.clone());
    let worker = UncertainWorker(worker_id.clone());

    let stopping = manager.stop_run(spec.id(), &worker).unwrap();
    assert_eq!(stopping.state(), RunState::Stopping);
    assert_eq!(
        stopping.termination_reason(),
        Some(conductor_core::RunTerminationReason::Cancelled)
    );
    assert_eq!(
        repository.get_run(spec.id()).unwrap(),
        Some(stopping.clone())
    );
    assert_eq!(manager.stop_run(spec.id(), &worker).unwrap(), stopping);
    let pending = repository.pending_commands(&worker_id, 0, 32).unwrap();
    assert_eq!(
        pending
            .iter()
            .filter(|envelope| matches!(envelope.command, WorkerCommand::StopRun(_)))
            .count(),
        1
    );
    let stop_command_id = pending
        .iter()
        .find(|envelope| matches!(envelope.command, WorkerCommand::StopRun(_)))
        .unwrap()
        .command_id
        .clone();
    repository
        .command_for_run(
            spec.id(),
            &worker_id,
            0,
            WorkerCommand::InspectRun(InspectRun::new(spec.id().clone())),
        )
        .unwrap();
    drop(manager);
    drop(repository);

    let repository = Arc::new(SqliteJobRepository::open(directory.database()).unwrap());
    let manager = JobService::new(repository.clone());
    assert_eq!(manager.stop_run(spec.id(), &worker).unwrap(), stopping);
    let replayed_stop = repository
        .pending_commands(&worker_id, 0, 32)
        .unwrap()
        .into_iter()
        .find(|envelope| matches!(envelope.command, WorkerCommand::StopRun(_)))
        .unwrap();
    assert_eq!(replayed_stop.command_id, stop_command_id);

    let terminal = stopping
        .next_revision(
            RunState::Cancelled,
            stopping.started_at_ms(),
            Some(stopping.requested_at_ms() + 10),
            None,
            Some(15),
            None,
        )
        .unwrap()
        .with_termination_reason(conductor_core::RunTerminationReason::Cancelled);
    repository.record_observation(&terminal).unwrap();
    assert_eq!(manager.stop_run(spec.id(), &worker).unwrap(), terminal);
}

#[test]
fn stop_command_insert_failure_rolls_back_the_stopping_observation() {
    let directory = TempDir::new();
    let repository = Arc::new(SqliteJobRepository::open(directory.database()).unwrap());
    repository.insert(job("atomic-stop-failure")).unwrap();
    let worker_id = WorkerId::new("worker").unwrap();
    let admission = repository
        .create_run(
            &JobId::new("atomic-stop-failure").unwrap(),
            &worker_id,
            1,
            None,
        )
        .unwrap();
    let manager = JobService::new(repository.clone());
    let worker = UncertainWorker(worker_id.clone());
    let before = repository.get_run(admission.spec.id()).unwrap().unwrap();
    let connection = Connection::open(directory.database()).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_stop_command BEFORE INSERT ON manager_commands
             WHEN instr(NEW.envelope_json, '\"kind\":\"stop_run\"') > 0
             BEGIN SELECT RAISE(ABORT, 'injected stop outbox failure'); END;",
        )
        .unwrap();

    assert_eq!(
        manager.stop_run(admission.spec.id(), &worker),
        Err(ManagerError::StorageUnavailable)
    );
    assert_eq!(
        repository.get_run(admission.spec.id()).unwrap(),
        Some(before)
    );
    assert!(
        repository
            .pending_commands(&worker_id, 0, 32)
            .unwrap()
            .iter()
            .all(|envelope| !matches!(envelope.command, WorkerCommand::StopRun(_)))
    );
}

#[test]
fn reconciliation_delivers_a_committed_stop_after_manager_restart() {
    let directory = TempDir::new();
    let repository = SqliteJobRepository::open(directory.database()).unwrap();
    repository.insert(job("restart-stop")).unwrap();
    let worker_id = WorkerId::new("worker").unwrap();
    let admission = repository
        .create_run(&JobId::new("restart-stop").unwrap(), &worker_id, 1, None)
        .unwrap();
    let start = repository
        .pending_commands(&worker_id, 0, 32)
        .unwrap()
        .into_iter()
        .find(|envelope| matches!(envelope.command, WorkerCommand::StartRun(_)))
        .unwrap();
    repository
        .acknowledge_command(&start.command_id, &worker_id, 0, r#"{"accepted":true}"#)
        .unwrap();

    // Simulate manager exit after the atomic commit and before dispatch.
    let (stopping, stop) = repository
        .persist_stop_intent(admission.spec.id(), 0)
        .unwrap();
    let stop = stop.unwrap();
    let expected_command_id = stop.command_id.clone();
    drop(repository);

    let repository = Arc::new(SqliteJobRepository::open(directory.database()).unwrap());
    let manager = JobService::new(repository.clone());
    let worker = StopRecoveryWorker {
        worker_id: worker_id.clone(),
        stopping,
        delivered: std::sync::Mutex::new(Vec::new()),
    };
    manager.reconcile(&worker).unwrap();

    assert_eq!(*worker.delivered.lock().unwrap(), vec![expected_command_id]);
    assert_eq!(
        repository
            .get_run(admission.spec.id())
            .unwrap()
            .unwrap()
            .state(),
        RunState::Cancelled
    );
    assert!(
        repository
            .pending_commands(&worker_id, 0, 32)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn start_saturation_reserves_bounded_stop_and_resolution_capacity() {
    let directory = TempDir::new();
    let repository = Arc::new(SqliteJobRepository::open(directory.database()).unwrap());
    let worker_id = WorkerId::new("worker").unwrap();
    let mut run_ids = Vec::new();
    for index in 0..24 {
        let job_id = JobId::new(format!("reserved-control-{index}")).unwrap();
        repository.insert(job(job_id.as_str())).unwrap();
        let admission = repository
            .create_run(&job_id, &worker_id, 64, None)
            .unwrap();
        run_ids.push(admission.spec.id().clone());
    }
    let overflow_job_id = JobId::new("reserved-control-overflow").unwrap();
    repository.insert(job(overflow_job_id.as_str())).unwrap();
    assert_eq!(
        repository.create_run(&overflow_job_id, &worker_id, 64, None),
        Err(RunStoreError::PendingDeliveryLimit)
    );
    let manager = JobService::new(repository.clone());
    let worker = UncertainWorker(worker_id.clone());
    assert_eq!(
        manager.stop_run(&run_ids[0], &worker).unwrap().state(),
        RunState::Stopping
    );
    for index in 0..7 {
        repository
            .command_for_run(
                &run_ids[0],
                &worker_id,
                0,
                WorkerCommand::ResolveRun(ResolveRun::new(
                    run_ids[0].clone(),
                    format!("reserved-control-{index}"),
                )),
            )
            .unwrap();
    }
    assert_eq!(
        repository
            .pending_commands(&worker_id, 0, 32)
            .unwrap()
            .len(),
        32
    );
    let before = repository.get_run(&run_ids[1]).unwrap().unwrap();
    assert_eq!(
        manager.stop_run(&run_ids[1], &worker),
        Err(ManagerError::WorkerUnavailable)
    );
    assert_eq!(repository.get_run(&run_ids[1]).unwrap(), Some(before));
}

#[test]
fn committed_start_command_survives_reopen_and_duplicate_acknowledgement() {
    let directory = TempDir::new();
    let repository = SqliteJobRepository::open(directory.database()).unwrap();
    repository.insert(job("durable-start")).unwrap();
    let worker_id = WorkerId::new("worker").unwrap();
    let first = repository
        .create_run_for_worker(
            &JobId::new("durable-start").unwrap(),
            &worker_id,
            7,
            1,
            Some("request-1"),
        )
        .unwrap();
    let envelope = first.command.unwrap();
    assert_eq!(envelope.target_worker_incarnation, 7);
    assert_eq!(
        repository.pending_commands(&worker_id, 7, 1).unwrap(),
        vec![envelope.clone()]
    );
    drop(repository);

    let reopened = SqliteJobRepository::open(directory.database()).unwrap();
    let replay = reopened
        .create_run_for_worker(
            &JobId::new("durable-start").unwrap(),
            &worker_id,
            7,
            1,
            Some("request-1"),
        )
        .unwrap();
    assert_eq!(replay.replay.unwrap().run_id(), first.spec.id());
    assert_eq!(replay.command, Some(envelope.clone()));

    let acknowledgement = r#"{"accepted":true}"#;
    reopened
        .acknowledge_command(&envelope.command_id, &worker_id, 7, acknowledgement)
        .unwrap();
    reopened
        .acknowledge_command(&envelope.command_id, &worker_id, 7, acknowledgement)
        .unwrap();
    assert!(
        reopened
            .pending_commands(&worker_id, 7, 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reopened.acknowledge_command(
            &envelope.command_id,
            &WorkerId::new("other-worker").unwrap(),
            7,
            acknowledgement,
        ),
        Err(CommandStoreError::Conflict)
    );
    assert_eq!(
        reopened.acknowledge_command(&envelope.command_id, &worker_id, 7, r#"{"accepted":false}"#,),
        Err(CommandStoreError::Conflict)
    );
}

#[test]
fn pending_command_delivery_is_bounded_per_worker() {
    let directory = TempDir::new();
    let repository = SqliteJobRepository::open(directory.database()).unwrap();
    repository.insert(job("bounded-commands")).unwrap();
    let job_id = JobId::new("bounded-commands").unwrap();
    let worker_id = WorkerId::new("worker").unwrap();
    let admission = repository
        .create_run_for_worker(&job_id, &worker_id, 2, 1, None)
        .unwrap();
    for index in 0..31 {
        repository
            .command_for_run(
                admission.spec.id(),
                &worker_id,
                2,
                WorkerCommand::ResolveRun(ResolveRun::new(
                    admission.spec.id().clone(),
                    format!("reason-{index}"),
                )),
            )
            .unwrap();
    }
    assert_eq!(
        repository
            .pending_commands(&worker_id, 2, 32)
            .unwrap()
            .len(),
        32
    );
    assert_eq!(
        repository.command_for_run(
            admission.spec.id(),
            &worker_id,
            2,
            WorkerCommand::ResolveRun(ResolveRun::new(
                admission.spec.id().clone(),
                "one-more-reason".to_owned(),
            )),
        ),
        Err(CommandStoreError::PendingLimit)
    );
}
