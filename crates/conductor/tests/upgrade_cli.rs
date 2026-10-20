use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde_json::Value;

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "conductor-upgrade-cli-{}-{nonce}-{}",
            std::process::id(),
            NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir_all(&path).expect("create isolated directory");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn make_database(directory: &Path, name: &str, schema_version: i64) -> PathBuf {
    fs::create_dir_all(directory).expect("create database directory");
    let path = directory.join(name);
    let connection = Connection::open(&path).expect("create database fixture");
    connection
        .pragma_update(None, "user_version", schema_version)
        .expect("set fixture schema version");
    path
}

fn preflight(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["upgrade", "preflight"])
        .args(arguments)
        .output()
        .expect("run upgrade preflight CLI")
}

#[test]
fn preflight_reports_older_schemas_without_migrating_them() {
    let temp = TempDir::new();
    let manager = make_database(&temp.0.join("manager"), "manager.sqlite3", 11);
    let worker = make_database(&temp.0.join("worker"), "worker.sqlite3", 3);
    let output = preflight(&[
        "--manager-data-dir",
        manager.parent().unwrap().to_str().unwrap(),
        "--worker-data-dir",
        worker.parent().unwrap().to_str().unwrap(),
        "--output",
        "json",
    ]);

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).expect("JSON preflight report");
    assert_eq!(report["schemaMarkersSupported"], true);
    assert_eq!(report["databases"][0]["schemaVersion"], 11);
    assert_eq!(report["databases"][0]["state"], "migration_required");
    assert_eq!(report["databases"][1]["schemaVersion"], 3);
    assert_eq!(report["databases"][1]["state"], "migration_required");
    assert_eq!(
        report["requiredMaintenanceActions"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(
        report["requiredMaintenanceActions"][0]
            .as_str()
            .unwrap()
            .contains("verified backup")
    );
    for (path, expected) in [(manager, 11), (worker, 3)] {
        let connection = Connection::open(path).expect("reopen inspected database");
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("read schema marker after preflight");
        assert_eq!(version, expected, "preflight must not migrate databases");
    }
}

#[test]
fn preflight_blocks_a_newer_schema_and_allows_missing_databases() {
    let temp = TempDir::new();
    make_database(
        &temp.0.join("newer"),
        "manager.sqlite3",
        conductor_manager::CURRENT_SCHEMA_VERSION + 1,
    );
    let blocked = preflight(&[
        "--manager-data-dir",
        temp.0.join("newer").to_str().unwrap(),
        "--output",
        "json",
    ]);
    assert!(!blocked.status.success());
    let blocked_report: Value = serde_json::from_slice(&blocked.stdout).expect("blocked report");
    assert_eq!(blocked_report["schemaMarkersSupported"], false);
    assert_eq!(blocked_report["databases"][0]["state"], "unsupported");
    assert_eq!(
        blocked_report["requiredMaintenanceActions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(
        blocked_report["requiredMaintenanceActions"][0]
            .as_str()
            .unwrap()
            .contains("Do not start the candidate")
    );

    let missing = preflight(&[
        "--worker-data-dir",
        temp.0.join("not-created").to_str().unwrap(),
        "--output",
        "json",
    ]);
    assert!(missing.status.success());
    let missing_report: Value = serde_json::from_slice(&missing.stdout).expect("missing report");
    assert_eq!(missing_report["databases"][0]["state"], "not_initialized");
}

#[test]
fn preflight_requires_at_least_one_database_directory() {
    let output = preflight(&["--output", "json"]);
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).expect("JSON error");
    assert_eq!(error["error"]["code"], "upgrade_preflight_failed");
}

#[cfg(unix)]
#[test]
fn preflight_rejects_database_symlinks() {
    use std::os::unix::fs::symlink;

    let temp = TempDir::new();
    let real = make_database(&temp.0.join("real"), "manager.sqlite3", 12);
    let linked_dir = temp.0.join("linked");
    fs::create_dir_all(&linked_dir).expect("create linked database directory");
    symlink(real, linked_dir.join("manager.sqlite3")).expect("link manager database fixture");

    let output = preflight(&[
        "--manager-data-dir",
        linked_dir.to_str().unwrap(),
        "--output",
        "json",
    ]);
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).expect("JSON error");
    assert_eq!(error["error"]["code"], "upgrade_preflight_failed");
}
