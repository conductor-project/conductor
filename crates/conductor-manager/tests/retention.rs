use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use conductor_manager::{MAX_HISTORY_CLEANUP_BATCH, SqliteJobRepository};
use rusqlite::{Connection, params};

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let sequence = NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "conductor-manager-retention-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create temporary manager database directory");
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

fn insert_run(connection: &Connection, id: &str, order: i64, state: &str, key: Option<&str>) {
    connection
        .execute(
            "INSERT INTO runs (run_id, job_id, worker_id, job_spec_json, observation_json,
                 revision, state, idempotency_key, request_fingerprint, run_order)
             VALUES (?1, 'job-a', 'worker-a', '{}', '{}', 1, ?2, ?3, NULL, ?4)",
            params![id, state, key, order],
        )
        .expect("insert manager run");
}

#[test]
fn cleanup_removes_only_old_unreferenced_unkeyed_terminal_runs() {
    let directory = TempDir::new();
    drop(
        SqliteJobRepository::open_with_retention(directory.database(), 1)
            .expect("create manager database"),
    );
    let connection = Connection::open(directory.database()).expect("open fixture database");
    for (id, order, state, key) in [
        ("run-1", 1, "succeeded", None),
        ("run-2", 2, "failed", None),
        ("run-3", 3, "succeeded", Some("retry-key")),
        ("run-4", 4, "running", None),
        ("run-5", 5, "succeeded", None),
        ("run-6", 6, "succeeded", None),
        ("run-7", 7, "succeeded", None),
        ("run-8", 8, "succeeded", None),
    ] {
        insert_run(&connection, id, order, state, key);
    }
    connection
        .execute(
            "INSERT INTO jobs (id, spec_json, created_at_ms, deleted_at_ms)
             VALUES ('job-a', '{}', 0, NULL)",
            [],
        )
        .expect("insert service job");
    connection
        .execute(
            "INSERT INTO service_instances (job_id, replica_index, instance_id, current_allocation_id)
             VALUES ('job-a', 0, 'instance-a', NULL)",
            [],
        )
        .expect("insert service instance");
    connection
        .execute(
            "INSERT INTO service_attempts (service_instance_id, current_run_id)
             VALUES ('instance-a', 'run-6')",
            [],
        )
        .expect("protect run referenced by service attempt");
    connection
        .execute("UPDATE run_sequence SET value = 8 WHERE id = 1", [])
        .expect("reserve monotonically assigned run IDs");
    connection
        .execute(
            "INSERT INTO manager_commands (command_id, worker_id, worker_incarnation, run_id,
                 envelope_json, state, acknowledgement_json, created_at_ms, acknowledged_at_ms)
             VALUES ('command-1', 'worker-a', 1, 'run-1', '{}', 'acknowledged', '{}', 1, 2),
                    ('command-2', 'worker-a', 1, 'run-2', '{}', 'pending', NULL, 1, NULL)",
            [],
        )
        .expect("insert acknowledged and pending commands");
    connection
        .execute(
            "INSERT INTO service_port_leases
                 (network_domain_id, protocol, port, allocation_id, run_id, name, advertised_address)
             VALUES ('domain-a', 'tcp', 12000, 'allocation-a', 'run-5', 'http', '127.0.0.1')",
            [],
        )
        .expect("protect run referenced by a port lease");
    drop(connection);

    let repository = SqliteJobRepository::open_with_retention(directory.database(), 1)
        .expect("open manager repository");
    let progress = repository
        .prune_completed_run_history()
        .expect("prune eligible run history");
    assert_eq!(progress.eligible_runs, 3);
    assert_eq!(progress.removed_runs, 2);
    assert!(!progress.more_eligible);

    let connection = Connection::open(directory.database()).expect("read manager database");
    let ids = connection
        .prepare("SELECT run_id FROM runs ORDER BY run_order")
        .expect("prepare run IDs")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query run IDs")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect run IDs");
    assert_eq!(ids, ["run-2", "run-3", "run-4", "run-5", "run-6", "run-8"]);
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM manager_commands", [], |row| row
                .get::<_, i64>(0))
            .expect("count preserved pending commands"),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT value FROM run_sequence WHERE id = 1", [], |row| row
                .get::<_, i64>(0))
            .expect("read monotonic run sequence"),
        8
    );
    drop(connection);

    let status = repository
        .history_retention_status()
        .expect("inspect history retention status");
    assert_eq!(status.completed_runs, 5);
    assert_eq!(status.active_or_unresolved_runs, 1);
    assert_eq!(status.eligible_runs, 1);
    assert_eq!(status.idempotency_protected_runs, 1);
    assert_eq!(status.pending_commands, 1);
    assert_eq!(status.current_service_attempts, 1);
    assert_eq!(status.service_port_leases, 1);
    assert_eq!(status.manager_commands, 1);
    assert_eq!(status.completed_run_retention, 1);
    assert_eq!(status.cleanup_batch_limit, MAX_HISTORY_CLEANUP_BATCH);
    assert!(status.sqlite_page_count > 0);
    assert!(status.sqlite_page_size_bytes > 0);
}

#[test]
fn sustained_history_cleanup_stays_within_batch_and_reports_freed_pages() {
    let directory = TempDir::new();
    drop(
        SqliteJobRepository::open_with_retention(directory.database(), 1)
            .expect("create manager database"),
    );
    let connection = Connection::open(directory.database()).expect("open fixture database");
    let payload = "x".repeat(4_096);
    for order in 1..=300_i64 {
        let id = format!("run-{order}");
        connection
            .execute(
                "INSERT INTO runs (run_id, job_id, worker_id, job_spec_json, observation_json,
                     revision, state, idempotency_key, request_fingerprint, run_order)
                 VALUES (?1, 'job-a', 'worker-a', ?2, ?2, 1, 'succeeded', NULL, NULL, ?3)",
                params![id, payload, order],
            )
            .expect("insert sustained completed history");
    }
    connection
        .execute("UPDATE run_sequence SET value = 300 WHERE id = 1", [])
        .expect("reserve run IDs through the fixture range");
    drop(connection);

    let repository = SqliteJobRepository::open_with_retention(directory.database(), 1)
        .expect("open manager repository");
    let before = repository
        .history_retention_status()
        .expect("measure database before cleanup");
    assert_eq!(before.eligible_runs, 300);
    let mut total_removed = 0;
    let mut progress = repository
        .prune_completed_run_history()
        .expect("first bounded cleanup batch");
    assert_eq!(progress.removed_runs, MAX_HISTORY_CLEANUP_BATCH);
    assert!(progress.more_eligible);
    total_removed += progress.removed_runs;
    while progress.more_eligible {
        progress = repository
            .prune_completed_run_history()
            .expect("resume bounded cleanup");
        assert!(progress.removed_runs <= MAX_HISTORY_CLEANUP_BATCH);
        total_removed += progress.removed_runs;
    }

    let after = repository
        .history_retention_status()
        .expect("measure database after cleanup");
    assert_eq!(total_removed, 299);
    assert_eq!(after.eligible_runs, 1);
    assert_eq!(after.eligible_runs_to_prune, 0);
    assert_eq!(after.sqlite_page_count, before.sqlite_page_count);
    assert!(after.sqlite_freelist_pages > before.sqlite_freelist_pages);
}
