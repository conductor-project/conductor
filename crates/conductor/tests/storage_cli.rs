use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use conductor_manager::SqliteJobRepository;

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
            "conductor-storage-cli-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create temporary storage directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn storage_inspection_reports_json_without_changing_the_database() {
    let directory = TempDir::new();
    let database_path = directory.path().join("manager.sqlite3");
    drop(SqliteJobRepository::open(&database_path).expect("create manager database"));
    let database_before = fs::read(&database_path).expect("read manager database");

    let output = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args([
            "storage",
            "inspect",
            "--manager-data-dir",
            directory.path().to_str().expect("UTF-8 temporary path"),
            "--completed-run-retention",
            "5",
            "--output",
            "json",
        ])
        .output()
        .expect("run storage inspection command");

    assert!(
        output.status.success(),
        "storage inspection failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse storage inspection JSON");
    assert_eq!(report["manager"]["completedRunRetention"], 5);
    assert_eq!(report["manager"]["cleanupBatchLimit"], 128);
    assert!(
        report["manager"]["sqlitePageCount"]
            .as_u64()
            .unwrap_or_default()
            > 0
    );
    assert_eq!(
        fs::read(&database_path).expect("re-read manager database"),
        database_before
    );
}
