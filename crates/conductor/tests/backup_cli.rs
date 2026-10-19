use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use sha2::{Digest, Sha256};
use tar::{Builder, EntryType, Header};

static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after Unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "conductor-backup-cli-{}-{nonce}-{}",
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

fn make_database(directory: &Path, name: &str, version: i64) -> PathBuf {
    fs::create_dir_all(directory).expect("create data directory");
    let path = directory.join(name);
    let connection = Connection::open(&path).expect("create database");
    connection
        .execute_batch("CREATE TABLE retained (id TEXT PRIMARY KEY, value TEXT);")
        .expect("create retained table");
    connection
        .execute("INSERT INTO retained VALUES ('kept', 'value')", [])
        .expect("insert retained record");
    connection
        .pragma_update(None, "user_version", version)
        .expect("set schema version");
    path
}

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(arguments)
        .output()
        .expect("run conductor CLI")
}

fn assert_success(output: Output) {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn append_file(archive: &mut Builder<File>, path: &str, bytes: &[u8]) {
    let mut header = Header::new_gnu();
    header.set_path(path).expect("set archive path");
    header.set_size(bytes.len() as u64);
    header.set_mode(0o600);
    header.set_entry_type(EntryType::Regular);
    header.set_cksum();
    archive
        .append_data(&mut header, path, bytes)
        .expect("append archive file");
}

fn write_tar_path_traversal_fixture(path: &Path) {
    let mut header = [0_u8; 512];
    let archive_path = b"../outside";
    header[..archive_path.len()].copy_from_slice(archive_path);
    header[100..108].copy_from_slice(b"0000600\0");
    header[108..116].copy_from_slice(b"0000000\0");
    header[116..124].copy_from_slice(b"0000000\0");
    header[124..136].copy_from_slice(b"00000000000\0");
    header[136..148].copy_from_slice(b"00000000000\0");
    header[148..156].fill(b' ');
    header[156] = b'0';
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum = header.iter().map(|byte| u64::from(*byte)).sum::<u64>();
    let checksum_field = format!("{checksum:06o}\0 ");
    header[148..156].copy_from_slice(checksum_field.as_bytes());
    let mut file = File::create(path).expect("create path-traversal archive");
    file.write_all(&header).expect("write tar header");
    file.write_all(&[0_u8; 1024])
        .expect("write tar end markers");
}

#[test]
fn backup_round_trip_preserves_databases_logs_included_identity_and_recovery_hold() {
    let temp = TempDir::new();
    let manager = temp.0.join("source-manager");
    let worker = temp.0.join("source-worker");
    let manager_db = make_database(&manager, "manager.sqlite3", 12);
    let worker_db = make_database(&worker, "worker.sqlite3", 4);
    fs::write(manager.join("manager.sqlite3-journal"), []).expect("write manager journal fixture");
    fs::write(worker_db.with_file_name("worker.sqlite3-journal"), [])
        .expect("write worker journal fixture");
    let run_logs = worker.join("runs");
    fs::create_dir_all(&run_logs).expect("create worker log directory");
    fs::write(run_logs.join("run-1.stdout"), b"retained output\n").expect("write worker log");
    let identity = temp.0.join("manager-identity");
    fs::create_dir(&identity).expect("create identity directory");
    fs::write(identity.join("worker-ca-key.pem"), b"private-key-fixture").expect("write identity");
    let config = temp.0.join("conductor.json");
    fs::write(&config, br#"{"role":"all"}"#).expect("write config fixture");
    let archive = temp.0.join("maintenance.tar");
    let archive_arg = archive.to_str().unwrap();
    let manager_arg = manager.to_str().unwrap();
    let worker_arg = worker.to_str().unwrap();
    let identity_arg = identity.to_str().unwrap();
    let config_arg = config.to_str().unwrap();

    assert_success(run(&[
        "backup",
        "create",
        "--manager-data-dir",
        manager_arg,
        "--worker-data-dir",
        worker_arg,
        "--include-path",
        identity_arg,
        "--include-path",
        config_arg,
        "--output",
        archive_arg,
    ]));
    assert_success(run(&["backup", "inspect", "--input", archive_arg]));
    Connection::open(worker.join("worker.sqlite3"))
        .expect("open source worker database")
        .execute(
            "INSERT INTO retained VALUES ('executed-later', 'unknown-to-backup')",
            [],
        )
        .expect("record post-backup state change");
    fs::write(identity.join("worker-ca-key.pem"), b"rotated-key-fixture")
        .expect("rotate identity after backup");

    let restored = temp.0.join("restored");
    let restored_arg = restored.to_str().unwrap();
    assert_success(run(&[
        "backup",
        "restore",
        "--input",
        archive_arg,
        "--destination",
        restored_arg,
    ]));
    let restored_db = Connection::open(restored.join("manager/manager.sqlite3"))
        .expect("open restored manager database");
    let retained: String = restored_db
        .query_row("SELECT value FROM retained WHERE id = 'kept'", [], |row| {
            row.get(0)
        })
        .expect("read restored manager record");
    assert_eq!(retained, "value");
    assert_eq!(
        fs::read(restored.join("worker/runs/run-1.stdout")).expect("read restored worker log"),
        b"retained output\n"
    );
    assert!(
        !restored.join("manager/manager.sqlite3-journal").exists(),
        "a SQLite rollback journal must not be paired with the database snapshot"
    );
    assert!(
        !restored.join("worker/worker.sqlite3-journal").exists(),
        "a SQLite rollback journal must not be paired with the database snapshot"
    );
    assert_eq!(
        fs::read(restored.join("included/0000/worker-ca-key.pem")).expect("read restored identity"),
        b"private-key-fixture"
    );
    assert_eq!(
        fs::read(restored.join("included/0001/conductor.json")).expect("read restored config"),
        br#"{"role":"all"}"#
    );
    assert!(
        restored
            .join("manager/.conductor-recovery-required")
            .is_file()
    );
    assert!(
        restored
            .join("worker/.conductor-recovery-required")
            .is_file()
    );
    let restored_worker = Connection::open(restored.join("worker/worker.sqlite3"))
        .expect("open restored worker database");
    let later_record: i64 = restored_worker
        .query_row(
            "SELECT COUNT(*) FROM retained WHERE id = 'executed-later'",
            [],
            |row| row.get(0),
        )
        .expect("query post-backup worker record");
    assert_eq!(
        later_record, 0,
        "restore must reflect the selected backup point"
    );

    let restored_config = temp.0.join("restored-agent.json");
    let config_json = serde_json::json!({
        "role": "manager",
        "listen": "127.0.0.1:0",
        "manager_data_dir": restored.join("manager"),
        "worker_transport_listen": "127.0.0.1:0",
        "worker_id": "restore-test-worker",
        "worker_transport_capacity": 1,
        "api_access": {"mode": "local"},
        "manager_identity_dir": temp.0.join("agent-identity"),
        "manager_server_name": "localhost"
    });
    fs::write(
        &restored_config,
        serde_json::to_vec(&config_json).expect("serialize agent configuration"),
    )
    .expect("write restore startup configuration");
    let blocked_start = run(&["agent", "--config", restored_config.to_str().unwrap()]);
    assert!(!blocked_start.status.success());
    assert!(
        String::from_utf8_lossy(&blocked_start.stderr).contains("recovery clearance"),
        "stderr: {}",
        String::from_utf8_lossy(&blocked_start.stderr)
    );

    assert_success(run(&[
        "backup",
        "recovery-resolve",
        "--manager-data-dir",
        restored.join("manager").to_str().unwrap(),
        "--worker-data-dir",
        restored.join("worker").to_str().unwrap(),
        "--confirm-workers-fenced",
        "--confirm-unknown-outcomes-reviewed",
    ]));
    assert!(
        !restored
            .join("manager/.conductor-recovery-required")
            .exists()
    );
    assert!(
        !restored
            .join("worker/.conductor-recovery-required")
            .exists()
    );

    let source_version: i64 = Connection::open(manager_db)
        .expect("reopen source manager database")
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("read source version");
    assert_eq!(source_version, 12, "backup must not migrate source state");
}

#[test]
fn backup_rejects_symlinks_and_busy_state_locks() {
    let temp = TempDir::new();
    let manager = temp.0.join("manager");
    make_database(&manager, "manager.sqlite3", 12);
    let output = temp.0.join("backup.tar");
    let manager_arg = manager.to_str().unwrap();
    let output_arg = output.to_str().unwrap();

    let lock_path = manager.join(".conductor.lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .expect("open manager lock file");
    lock.try_lock().expect("hold manager state lock");
    let busy = run(&[
        "backup",
        "create",
        "--manager-data-dir",
        manager_arg,
        "--output",
        output_arg,
    ]);
    assert!(!busy.status.success());
    drop(lock);

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        symlink(temp.0.join("missing-target"), manager.join("linked"))
            .expect("create symlink fixture");
        let symlink_result = run(&[
            "backup",
            "create",
            "--manager-data-dir",
            manager_arg,
            "--output",
            output_arg,
        ]);
        assert!(!symlink_result.status.success());
    }
}

#[test]
fn inspect_rejects_tampered_archive_and_restore_never_replaces_destination() {
    let temp = TempDir::new();
    let manager = temp.0.join("manager");
    make_database(&manager, "manager.sqlite3", 12);
    let archive = temp.0.join("backup.tar");
    assert_success(run(&[
        "backup",
        "create",
        "--manager-data-dir",
        manager.to_str().unwrap(),
        "--output",
        archive.to_str().unwrap(),
    ]));

    let destination = temp.0.join("existing");
    fs::create_dir(&destination).expect("create existing restore destination");
    fs::write(destination.join("sentinel"), b"preserve").expect("write sentinel");
    let restore = run(&[
        "backup",
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--destination",
        destination.to_str().unwrap(),
    ]);
    assert!(!restore.status.success());
    assert_eq!(fs::read(destination.join("sentinel")).unwrap(), b"preserve");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&archive)
        .expect("open archive for tampering");
    file.seek(SeekFrom::Start(1024)).expect("seek to payload");
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte).expect("read payload byte");
    byte[0] ^= 0x01;
    file.seek(SeekFrom::Start(1024)).expect("seek to payload");
    file.write_all(&byte).expect("tamper payload");
    file.sync_all().expect("sync tampered archive");
    let inspect = run(&["backup", "inspect", "--input", archive.to_str().unwrap()]);
    assert!(!inspect.status.success());
}

#[cfg(unix)]
#[test]
fn backup_files_are_private() {
    use std::os::unix::fs::PermissionsExt;

    let temp = TempDir::new();
    let manager = temp.0.join("manager");
    make_database(&manager, "manager.sqlite3", 12);
    let archive = temp.0.join("backup.tar");
    assert_success(run(&[
        "backup",
        "create",
        "--manager-data-dir",
        manager.to_str().unwrap(),
        "--output",
        archive.to_str().unwrap(),
    ]));
    assert_eq!(
        fs::metadata(&archive).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let restored = temp.0.join("restored");
    assert_success(run(&[
        "backup",
        "restore",
        "--input",
        archive.to_str().unwrap(),
        "--destination",
        restored.to_str().unwrap(),
    ]));
    assert_eq!(
        fs::metadata(&restored).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(restored.join("manager/manager.sqlite3"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[test]
fn restore_rejects_unsafe_archive_paths_links_and_invalid_sqlite_without_publishing() {
    let temp = TempDir::new();

    let traversal = temp.0.join("traversal.tar");
    write_tar_path_traversal_fixture(&traversal);
    let inspect = run(&["backup", "inspect", "--input", traversal.to_str().unwrap()]);
    assert!(!inspect.status.success());
    let restore_destination = temp.0.join("traversal-restore");
    let restore = run(&[
        "backup",
        "restore",
        "--input",
        traversal.to_str().unwrap(),
        "--destination",
        restore_destination.to_str().unwrap(),
    ]);
    assert!(!restore.status.success());
    assert!(!restore_destination.exists());
    assert!(!temp.0.join("outside").exists());

    let linked = temp.0.join("linked.tar");
    let mut archive = Builder::new(File::create(&linked).expect("create linked archive"));
    let mut header = Header::new_gnu();
    header.set_path("manager/link").expect("set link path");
    header
        .set_link_name("../../outside")
        .expect("set link target");
    header.set_size(0);
    header.set_entry_type(EntryType::Symlink);
    header.set_cksum();
    archive
        .append_data(&mut header, "manager/link", io::empty())
        .expect("append symbolic link");
    archive.into_inner().expect("finish linked archive");
    let inspect = run(&["backup", "inspect", "--input", linked.to_str().unwrap()]);
    assert!(!inspect.status.success());

    let invalid_db = b"not a SQLite database";
    let digest = Sha256::digest(invalid_db);
    let sha256 = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let manifest = serde_json::json!({
        "formatVersion": 1,
        "createdAtUnixSeconds": 1,
        "binaryVersion": "0.1.0",
        "managerIncluded": true,
        "managerSchemaVersion": 12,
        "workerIncluded": false,
        "workerSchemaVersion": null,
        "files": [{"path":"manager/manager.sqlite3","size":invalid_db.len(),"sha256":sha256}],
        "includedSources": []
    });
    let invalid_sqlite = temp.0.join("invalid-sqlite.tar");
    let mut archive =
        Builder::new(File::create(&invalid_sqlite).expect("create invalid database archive"));
    append_file(&mut archive, "manager/manager.sqlite3", invalid_db);
    append_file(
        &mut archive,
        "manifest.json",
        &serde_json::to_vec(&manifest).expect("encode test manifest"),
    );
    archive
        .into_inner()
        .expect("finish invalid database archive");
    let inspect = run(&[
        "backup",
        "inspect",
        "--input",
        invalid_sqlite.to_str().unwrap(),
    ]);
    assert!(
        !inspect.status.success(),
        "inspection must reject invalid SQLite even when its manifest checksum matches"
    );
    let destination = temp.0.join("invalid-sqlite-restore");
    let restore = run(&[
        "backup",
        "restore",
        "--input",
        invalid_sqlite.to_str().unwrap(),
        "--destination",
        destination.to_str().unwrap(),
    ]);
    assert!(!restore.status.success());
    assert!(!destination.exists(), "invalid state is never published");
    assert!(
        fs::read_dir(&temp.0)
            .expect("read temp directory")
            .all(|entry| !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".conductor-restore-")),
        "failed restore staging directory is cleaned up"
    );

    let schema_source = temp.0.join("schema-source");
    let schema_database = make_database(&schema_source, "manager.sqlite3", 11);
    let schema_database_bytes = fs::read(schema_database).expect("read schema fixture");
    let schema_digest = Sha256::digest(&schema_database_bytes);
    let schema_sha256 = schema_digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let schema_manifest = serde_json::json!({
        "formatVersion": 1,
        "createdAtUnixSeconds": 1,
        "binaryVersion": "0.1.0",
        "managerIncluded": true,
        "managerSchemaVersion": 12,
        "workerIncluded": false,
        "workerSchemaVersion": null,
        "files": [{"path":"manager/manager.sqlite3","size":schema_database_bytes.len(),"sha256":schema_sha256}],
        "includedSources": []
    });
    let schema_mismatch = temp.0.join("schema-mismatch.tar");
    let mut archive = Builder::new(File::create(&schema_mismatch).expect("create schema archive"));
    append_file(
        &mut archive,
        "manager/manager.sqlite3",
        &schema_database_bytes,
    );
    append_file(
        &mut archive,
        "manifest.json",
        &serde_json::to_vec(&schema_manifest).expect("encode schema manifest"),
    );
    archive.into_inner().expect("finish schema archive");
    let inspect = run(&[
        "backup",
        "inspect",
        "--input",
        schema_mismatch.to_str().unwrap(),
    ]);
    assert!(
        !inspect.status.success(),
        "inspection must reject a schema marker that disagrees with the SQLite database"
    );
}
