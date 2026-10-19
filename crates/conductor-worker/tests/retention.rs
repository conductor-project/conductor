use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use conductor_core::{InspectRun, JobId, JobSpec, RunId, RunSpec, RunState, StartRun, WorkerId};
use conductor_worker::{
    DEFAULT_LOG_MAX_BYTES_PER_STREAM, ExecutionBackend, LogPolicy, WorkerService,
};
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
            "conductor-worker-retention-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create temporary worker directory");
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

#[derive(Default)]
struct TestBackend;

impl ExecutionBackend for TestBackend {
    fn spawn(&self, _run_id: &str, job: &JobSpec) -> io::Result<Child> {
        let mut command = Command::new(&job.command()[0]);
        command
            .args(&job.command()[1..])
            .current_dir(job.cwd())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !job.inherit_env() {
            command.env_clear();
        }
        command.envs(job.env());
        command.spawn()
    }

    fn terminate_process_group(&self, _group_id: u32) -> io::Result<()> {
        Ok(())
    }

    fn kill_process_group(&self, _group_id: u32) -> io::Result<()> {
        Ok(())
    }
}

fn open_worker(directory: &TempDir, retention_runs: usize) -> WorkerService {
    WorkerService::open_with_log_policy(
        directory.path(),
        WorkerId::new("worker-1").expect("valid worker ID"),
        1,
        LogPolicy {
            max_bytes_per_stream: DEFAULT_LOG_MAX_BYTES_PER_STREAM,
            retention_runs,
        },
        Arc::new(TestBackend),
    )
    .expect("open worker with retention policy")
}

fn seed_completed_runs(directory: &TempDir, count: usize) {
    drop(open_worker(directory, 1));
    let database = directory.path().join("worker.sqlite3");
    let connection = Connection::open(&database).expect("open worker database fixture");
    let log_directory = directory.path().join("runs");
    for index in 0..count {
        let id = format!("run-{index:03}");
        let spec = RunSpec::new(
            RunId::new(id.clone()).expect("valid run ID"),
            WorkerId::new("worker-1").expect("valid worker ID"),
            JobSpec::new(
                JobId::new("job-a").expect("valid job ID"),
                vec!["/usr/bin/true".to_owned()],
                PathBuf::from("/tmp"),
                BTreeMap::new(),
                false,
                None,
                100,
            )
            .expect("valid job spec"),
        );
        let requested = conductor_core::RunObservation::requested(&spec, 1);
        let completed = requested
            .next_revision(RunState::Succeeded, Some(2), Some(3), Some(0), None, None)
            .expect("valid terminal observation");
        let spec_json = serde_json::to_string(&spec).expect("serialize run spec");
        let observation_json =
            serde_json::to_string(&completed).expect("serialize terminal observation");
        connection
            .execute(
                "INSERT INTO runs (run_id, spec_json, observation_json, state, revision)
                 VALUES (?1, ?2, ?3, 'succeeded', 2)",
                params![id, spec_json, observation_json],
            )
            .expect("insert completed worker run");
        connection
            .execute(
                "INSERT INTO run_logs
                     (run_id, stdout_closed, stderr_closed, stdout_truncated, stderr_truncated, pruned, completed_at_ms)
                 VALUES (?1, 1, 1, 0, 0, 0, ?2)",
                params![id, index as i64 + 1],
            )
            .expect("insert completed log state");
        for stream in ["stdout", "stderr"] {
            fs::write(log_directory.join(format!("{id}.{stream}")), b"log")
                .expect("write completed run log");
        }
    }
}

fn pruned_count(directory: &TempDir) -> i64 {
    Connection::open(directory.path().join("worker.sqlite3"))
        .expect("open worker database")
        .query_row(
            "SELECT COUNT(*) FROM run_logs WHERE pruned = 1",
            [],
            |row| row.get(0),
        )
        .expect("count pruned run logs")
}

#[test]
fn read_only_storage_inspection_reports_runs_delivery_and_log_limits() {
    let directory = TempDir::new();
    seed_completed_runs(&directory, 3);
    let database_path = directory.path().join("worker.sqlite3");
    let connection = Connection::open(&database_path).expect("open worker fixture database");
    connection
        .execute(
            "INSERT INTO worker_commands
                (command_id, worker_id, worker_incarnation, command_json, state, result_json, report_sequence, accepted_at_ms)
             VALUES ('accepted', 'worker-1', 1, '{}', 'accepted', NULL, NULL, 1),
                    ('completed', 'worker-1', 1, '{}', 'completed', '{}', 1, 2),
                    ('acknowledged', 'worker-1', 1, '{}', 'acknowledged', '{}', 2, 3)",
            [],
        )
        .expect("insert command delivery fixtures");
    connection
        .execute(
            "UPDATE worker_report_state SET sequence = 2 WHERE incarnation = 1",
            [],
        )
        .expect("advance report sequence");
    drop(connection);
    let database_before = fs::read(&database_path).expect("read worker database before inspect");

    let status = WorkerService::inspect_storage_read_only(
        directory.path(),
        LogPolicy {
            max_bytes_per_stream: 1_024,
            retention_runs: 2,
        },
    )
    .expect("inspect worker storage read-only");

    assert_eq!(status.runs, 3);
    assert_eq!(status.completed_runs, 3);
    assert_eq!(status.completed_unpruned_log_runs, 3);
    assert_eq!(status.worker_commands, 3);
    assert_eq!(status.accepted_commands, 1);
    assert_eq!(status.reports_pending_acknowledgement, 1);
    assert_eq!(status.acknowledged_commands, 1);
    assert_eq!(status.report_sequence_rows, 2);
    assert_eq!(status.report_sequence_high_water, 2);
    assert_eq!(status.estimated_unpruned_log_bytes_upper_bound, 6_144);
    assert_eq!(status.log_retention_runs, 2);
    assert_eq!(status.log_max_bytes_per_stream, 1_024);
    assert_eq!(
        fs::read(&database_path).expect("re-read worker database"),
        database_before
    );
}

#[test]
fn completed_log_cleanup_is_batched_and_resumes_after_worker_restart() {
    let directory = TempDir::new();
    seed_completed_runs(&directory, 140);

    drop(open_worker(&directory, 1));
    assert_eq!(pruned_count(&directory), 128);
    assert_eq!(
        fs::read_dir(directory.path().join("runs")).unwrap().count(),
        24
    );

    drop(open_worker(&directory, 1));
    assert_eq!(pruned_count(&directory), 139);
    assert_eq!(
        fs::read_dir(directory.path().join("runs")).unwrap().count(),
        2
    );
}

#[test]
fn interrupted_file_removal_is_safe_when_the_next_run_retries_pruning() {
    let directory = TempDir::new();
    seed_completed_runs(&directory, 2);
    let worker = open_worker(&directory, 1);
    let database_path = directory.path().join("worker.sqlite3");
    let connection = Connection::open(&database_path).expect("open worker database");
    connection
        .execute(
            "UPDATE run_logs SET pruned = 0 WHERE run_id = 'run-000'",
            [],
        )
        .expect("simulate interruption before marker persistence");
    assert!(!directory.path().join("runs/run-000.stdout").exists());
    fs::write(
        directory.path().join("runs/run-000.stderr"),
        b"remaining stream",
    )
    .expect("restore remaining stream for retry");
    drop(connection);

    let spec = RunSpec::new(
        RunId::new("new-run").expect("valid run ID"),
        WorkerId::new("worker-1").expect("valid worker ID"),
        JobSpec::new(
            JobId::new("job-new").expect("valid job ID"),
            vec!["/usr/bin/true".to_owned()],
            PathBuf::from("/tmp"),
            BTreeMap::new(),
            false,
            None,
            100,
        )
        .expect("valid job spec"),
    );
    worker
        .start(StartRun::new(spec))
        .expect("start a run that triggers the next retention pass");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let observation = worker
            .inspect(InspectRun::new(RunId::new("new-run").unwrap()))
            .expect("inspect run")
            .expect("run exists");
        if observation.state().is_terminal() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "run did not complete");
        thread::sleep(Duration::from_millis(5));
    }
    let retention_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while pruned_count(&directory) < 1 || directory.path().join("runs/run-000.stderr").exists() {
        assert!(
            std::time::Instant::now() < retention_deadline,
            "retention pass did not finish"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert!(!directory.path().join("runs/run-000.stdout").exists());
}
