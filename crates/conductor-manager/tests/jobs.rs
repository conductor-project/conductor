use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use conductor_core::{
    CommandEnvelope, CommandId, DeploymentMutation, DeploymentPendingReason, DeploymentStatus,
    InspectRun, JobId, JobRevisionDraft, JobSpec, LogStream, ReadRunLogs, ResolveRun,
    RunFailureReason, RunId, RunLogChunk, RunLogStatus, RunObservation, RunState, StartRun,
    StopRun, WorkerCommand, WorkerDescriptor, WorkerId,
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
            },
            "deploy-one",
        )
        .expect("persist initial deployment");
    assert_eq!(initial.generation(), 1);
    assert_eq!(initial.status(), DeploymentStatus::Pending);
    assert_eq!(
        initial.pending_reason(),
        Some(DeploymentPendingReason::PlacementPending)
    );
    assert_eq!(
        manager
            .mutate_deployment(
                revision_one.id().job_id(),
                None,
                &DeploymentMutation::Deploy {
                    revision: 1,
                    replicas: 3,
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
    assert_eq!(version, 6);
    assert!(table_exists(&connection, "job_revisions"));
    assert!(table_exists(&connection, "job_deployments"));
    assert!(table_exists(&connection, "execution_idempotency"));
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
    assert_eq!(version, 6);
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
    assert_eq!(version, 6);
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
    assert_eq!(version, 6);
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
    assert_eq!(version, 6);
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
    assert_eq!(version, 6);
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
