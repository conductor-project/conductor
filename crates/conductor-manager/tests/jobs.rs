use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use conductor_core::{JobId, JobSpec};
use conductor_manager::{
    JobService, MAX_JOBS, ManagerError, RepositoryOpenError, SqliteJobRepository,
};
use rusqlite::Connection;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("conductor-manager-{}-{nonce}", std::process::id()));
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

fn service(path: &TempDir) -> JobService {
    JobService::open_sqlite(path.database()).expect("open manager database")
}

#[test]
fn jobs_and_timestamps_survive_reopen_and_deleted_ids_remain_reserved() {
    let directory = TempDir::new();
    let manager = service(&directory);
    manager.register(job("zeta")).expect("register zeta");
    manager.register(job("alpha")).expect("register alpha");
    assert_eq!(
        manager
            .list()
            .unwrap()
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
    assert_eq!(reopened.list().unwrap().len(), 1);

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
    assert_eq!(reopened.list().unwrap(), vec![job("legacy")]);
    drop(reopened);
    let connection = Connection::open(directory.database()).expect("inspect migrated database");
    let (version, created_at, deleted_at): (i64, i64, Option<i64>) = connection
        .query_row(
            "SELECT (SELECT user_version FROM pragma_user_version), created_at_ms, deleted_at_ms FROM jobs WHERE id = 'legacy'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read migrated schema and fixture");
    assert_eq!(version, 1);
    assert_eq!(created_at, 1234);
    assert_eq!(deleted_at, None);
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
    assert_eq!(manager.list().unwrap().len(), 1);
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
    assert_eq!(manager.list().unwrap().len(), MAX_JOBS);
    assert_eq!(
        manager.register(job("over-limit")),
        Err(ManagerError::JobLimitReached)
    );
    assert!(manager.delete(&JobId::new("job-0").unwrap()).unwrap());
    manager
        .register(job("replacement"))
        .expect("deleted job frees active capacity");
    assert_eq!(manager.list().unwrap().len(), MAX_JOBS);
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
