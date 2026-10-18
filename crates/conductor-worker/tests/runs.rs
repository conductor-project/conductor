use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use conductor_core::{
    CommandEnvelope, CommandId, JobId, JobSpec, RunFailureReason, RunId, RunSpec, RunState,
    StartRun, WorkerCommand, WorkerId,
};
use conductor_worker::{ExecutionBackend, WorkerError, WorkerService};
use rusqlite::Connection;

struct TempDir(PathBuf);

static TEMP_DIR_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = TEMP_DIR_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "conductor-worker-{}-{nonce}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct TestBackend {
    spawns: AtomicUsize,
    kill_failures: AtomicUsize,
    ready_file: Mutex<Option<PathBuf>>,
}

impl ExecutionBackend for TestBackend {
    fn spawn(&self, _run_id: &str, job: &JobSpec) -> io::Result<Child> {
        self.spawns.fetch_add(1, Ordering::SeqCst);
        let mut command = Command::new(&job.command()[0]);
        command
            .args(&job.command()[1..])
            .current_dir(job.cwd())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        if !job.inherit_env() {
            command.env_clear();
        }
        command.envs(job.env());
        let mut child = command.spawn()?;
        if let Some(ready_file) = self.ready_file.lock().unwrap().as_ref() {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while !ready_file.exists() && std::time::Instant::now() < deadline {
                thread::sleep(Duration::from_millis(5));
            }
            if !ready_file.exists() {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "test child did not reach its ready point",
                ));
            }
        }
        Ok(child)
    }

    fn kill_process_group(&self, group_id: u32) -> io::Result<()> {
        if self
            .kill_failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |failures| {
                failures.checked_sub(1)
            })
            .is_ok()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected process-group cleanup failure",
            ));
        }
        #[cfg(unix)]
        {
            use nix::errno::Errno;
            use nix::sys::signal::{Signal, killpg};
            use nix::unistd::Pid;
            match killpg(Pid::from_raw(group_id as i32), Signal::SIGKILL) {
                Ok(()) | Err(Errno::ESRCH) => Ok(()),
                Err(error) => Err(io::Error::from_raw_os_error(error as i32)),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = group_id;
            Ok(())
        }
    }

    fn terminate_process_group(&self, _group_id: u32) -> io::Result<()> {
        Ok(())
    }
}

fn job(id: &str, command: Vec<String>) -> JobSpec {
    JobSpec::new(
        JobId::new(id).unwrap(),
        command,
        PathBuf::from("/tmp"),
        BTreeMap::new(),
        false,
        None,
        100,
    )
    .unwrap()
}

fn run(id: &str, job: JobSpec) -> RunSpec {
    RunSpec::new(
        RunId::new(id).unwrap(),
        WorkerId::new("worker-1").unwrap(),
        job,
    )
}

fn open_worker(directory: &TempDir, capacity: usize, backend: Arc<TestBackend>) -> WorkerService {
    WorkerService::open(
        &directory.0,
        WorkerId::new("worker-1").unwrap(),
        capacity,
        backend,
    )
    .unwrap()
}

#[test]
fn version_one_schema_migration_preserves_run_history_and_log_metadata() {
    let directory = TempDir::new();
    let spec = run("legacy", job("legacy", vec!["/usr/bin/true".into()]));
    let spec_json = serde_json::to_string(&spec).unwrap();
    let observation_json = r#"{"runId":"legacy","jobId":"legacy","workerId":"worker-1","revision":2,"state":"succeeded","requestedAtMs":1234,"startedAtMs":1235,"finishedAtMs":1236,"exitCode":0,"terminationSignal":null,"failureReason":null,"terminationReason":null,"resolutionReason":null,"workerIncarnation":0}"#.to_owned();
    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE runs (
                run_id TEXT PRIMARY KEY NOT NULL,
                spec_json TEXT NOT NULL,
                observation_json TEXT NOT NULL,
                state TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK (revision > 0)
             );
             PRAGMA user_version = 1;",
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO runs (run_id, spec_json, observation_json, state, revision) VALUES (?1, ?2, ?3, 'succeeded', 2)",
            (&"legacy", &spec_json, &observation_json),
        )
        .unwrap();
    drop(connection);

    let service = open_worker(&directory, 1, Arc::new(TestBackend::default()));
    drop(service);

    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    let (version, migrated_spec, migrated_observation, state, revision):
        (i64, String, String, String, i64) = connection
        .query_row(
            "SELECT (SELECT user_version FROM pragma_user_version), spec_json, observation_json, state, revision FROM runs WHERE run_id = 'legacy'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .unwrap();
    assert_eq!(version, 4);
    assert_eq!(migrated_spec, spec_json);
    assert_eq!(migrated_observation, observation_json);
    assert_eq!((state.as_str(), revision), ("succeeded", 2));
    let (stdout_closed, stderr_closed): (i64, i64) = connection
        .query_row(
            "SELECT stdout_closed, stderr_closed FROM run_logs WHERE run_id = 'legacy'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!((stdout_closed, stderr_closed), (1, 1));
}

#[test]
fn future_worker_schema_fails_with_version_and_keeps_existing_data() {
    let directory = TempDir::new();
    let database = directory.0.join("worker.sqlite3");
    let connection = Connection::open(&database).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE marker (value TEXT);
             INSERT INTO marker VALUES ('preserve');
             PRAGMA user_version = 77;",
        )
        .unwrap();
    drop(connection);

    let error = WorkerService::open(
        &directory.0,
        WorkerId::new("worker-1").unwrap(),
        1,
        Arc::new(TestBackend::default()),
    )
    .err()
    .unwrap();
    assert_eq!(
        error.to_string(),
        "worker database schema version 77 is newer than supported version 4"
    );
    let connection = Connection::open(database).unwrap();
    let value: String = connection
        .query_row("SELECT value FROM marker", [], |row| row.get(0))
        .unwrap();
    assert_eq!(value, "preserve");
}

fn wait_for(service: &WorkerService, id: &str) -> conductor_core::RunObservation {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(observation) = service
            .inspect(conductor_core::InspectRun::new(RunId::new(id).unwrap()))
            .unwrap()
            && observation.state().is_terminal()
        {
            return observation;
        }
        assert!(std::time::Instant::now() < deadline, "run did not finish");
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_pending_persistence(service: &WorkerService, id: &str) -> conductor_core::WorkerStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = service.status().unwrap();
        if status.pending_persistence() == 1 {
            assert!(service.is_degraded());
            assert!(!status.available());
            assert_eq!(status.used_slots(), 1);
            assert!(
                service
                    .inspect(conductor_core::InspectRun::new(RunId::new(id).unwrap()))
                    .unwrap()
                    .unwrap()
                    .state()
                    .is_active_or_unresolved()
            );
            return status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "terminal persistence did not become pending"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_pending_count(service: &WorkerService, count: usize) -> conductor_core::WorkerStatus {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status = service.status().unwrap();
        if status.pending_persistence() == count {
            assert!(service.is_degraded());
            return status;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "pending persistence did not reach {count} entries"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn escaped_pipe_fixture() {
    let Ok(mode) = std::env::var("CONDUCTOR_PIPE_FIXTURE_MODE") else {
        return;
    };
    let marker = PathBuf::from(std::env::var_os("CONDUCTOR_PIPE_FIXTURE_MARKER").unwrap());
    if mode == "descendant" {
        #[cfg(unix)]
        nix::unistd::setsid().unwrap();
        fs::write(marker, std::process::id().to_string()).unwrap();
        thread::sleep(Duration::from_secs(30));
        return;
    }
    let executable = std::env::current_exe().unwrap();
    let child = Command::new(executable)
        .args(["--exact", "escaped_pipe_fixture", "--nocapture"])
        .env("CONDUCTOR_PIPE_FIXTURE_MODE", "descendant")
        .env("CONDUCTOR_PIPE_FIXTURE_MARKER", &marker)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !marker.exists() && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(marker.exists(), "escaped descendant did not start");
    // The descendant has detached into its own session and inherited both
    // output descriptors. Let the direct child exit without waiting for it.
    drop(child);
}

#[cfg(unix)]
struct EscapedChild(i32);

#[cfg(unix)]
impl Drop for EscapedChild {
    fn drop(&mut self) {
        use nix::sys::signal::{Signal, kill};
        use nix::unistd::Pid;
        let _ = kill(Pid::from_raw(self.0), Signal::SIGKILL);
    }
}

#[cfg(unix)]
fn escaped_pipe_job(id: &str, marker: PathBuf) -> JobSpec {
    let mut env = BTreeMap::new();
    env.insert("CONDUCTOR_PIPE_FIXTURE_MODE".into(), "parent".into());
    env.insert(
        "CONDUCTOR_PIPE_FIXTURE_MARKER".into(),
        marker.display().to_string(),
    );
    JobSpec::new(
        JobId::new(id).unwrap(),
        vec![
            std::env::current_exe().unwrap().display().to_string(),
            "--exact".into(),
            "escaped_pipe_fixture".into(),
            "--nocapture".into(),
        ],
        PathBuf::from("/tmp"),
        env,
        false,
        None,
        100,
    )
    .unwrap()
}

#[test]
fn worker_acceptance_is_deduplicated_and_terminal_observations_survive_reopen() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 2, Arc::clone(&backend));
    let first_incarnation = service.descriptor().incarnation();
    let marker = directory.0.join("launch-count");
    let spec = run(
        "once",
        job(
            "job",
            vec![
                "/bin/sh".into(),
                "-c".into(),
                format!("printf x >> {}; sleep 0.05", marker.display()),
            ],
        ),
    );
    let first = service.start(StartRun::new(spec.clone())).unwrap();
    assert_eq!(first.state(), RunState::Running);
    let duplicate = service.start(StartRun::new(spec.clone())).unwrap();
    assert_eq!(duplicate.run_id(), first.run_id());
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);
    let terminal = wait_for(&service, "once");
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert_eq!(fs::read_to_string(marker).unwrap(), "x");

    let changed = run("once", job("job", vec!["/bin/true".into()]));
    assert_eq!(
        service.start(StartRun::new(changed)),
        Err(WorkerError::RunConflict)
    );
    drop(service);
    let reopened = open_worker(&directory, 2, backend);
    assert!(reopened.descriptor().incarnation() > first_incarnation);
    let persisted = reopened
        .inspect(conductor_core::InspectRun::new(RunId::new("once").unwrap()))
        .unwrap()
        .unwrap();
    assert_eq!(persisted, terminal);
    assert_eq!(persisted.worker_incarnation(), first_incarnation);
    assert_eq!(persisted.revision(), 4);
}

#[test]
fn command_ids_persist_acceptance_results_report_order_and_acknowledgements() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 2, Arc::clone(&backend));
    let spec = run("journaled", job("journaled", vec!["/bin/true".into()]));
    let envelope = CommandEnvelope {
        command_id: CommandId::new("command-journaled").unwrap(),
        target_worker_id: service.descriptor().worker_id().clone(),
        target_worker_incarnation: service.descriptor().incarnation(),
        command: WorkerCommand::StartRun(Box::new(StartRun::new(spec.clone()))),
    };

    assert_eq!(service.accept_command(&envelope), Ok(None));
    assert_eq!(service.accept_command(&envelope), Ok(None));
    let observation = service.start(StartRun::new(spec)).unwrap();
    let result_json = serde_json::to_string(&observation).unwrap();
    let report = service.complete_command(&envelope, &result_json).unwrap();
    assert_eq!(report.report_sequence, 1);
    assert_eq!(
        service.complete_command(&envelope, &result_json),
        Ok(report.clone())
    );
    assert_eq!(service.accept_command(&envelope), Ok(Some(report.clone())));
    let inspection = CommandEnvelope {
        command_id: CommandId::new("command-inspection").unwrap(),
        target_worker_id: service.descriptor().worker_id().clone(),
        target_worker_incarnation: service.descriptor().incarnation(),
        command: WorkerCommand::InspectRun(conductor_core::InspectRun::new(
            RunId::new("journaled").unwrap(),
        )),
    };
    service.accept_command(&inspection).unwrap();
    let second_report = service
        .complete_command(&inspection, r#"{"kind":"inspection","payload":null}"#)
        .unwrap();
    assert_eq!(second_report.report_sequence, 2);
    assert_eq!(
        service.pending_reports(),
        Ok(vec![report.clone(), second_report.clone()])
    );
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);

    let changed = CommandEnvelope {
        command: WorkerCommand::StartRun(Box::new(StartRun::new(run(
            "different-run",
            job("different-run", vec!["/bin/true".into()]),
        )))),
        ..envelope.clone()
    };
    assert_eq!(
        service.accept_command(&changed),
        Err(WorkerError::RunConflict)
    );
    service
        .acknowledge_report(
            &report.command_id,
            report.worker_incarnation,
            report.report_sequence,
        )
        .unwrap();
    assert_eq!(
        service.pending_reports().unwrap(),
        vec![second_report.clone()]
    );
    service
        .acknowledge_report(
            &second_report.command_id,
            second_report.worker_incarnation,
            second_report.report_sequence,
        )
        .unwrap();
    assert!(service.pending_reports().unwrap().is_empty());
    drop(service);

    let reopened = open_worker(&directory, 2, backend);
    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    let (state, sequence): (String, i64) = connection
        .query_row(
            "SELECT state, report_sequence FROM worker_commands WHERE command_id = 'command-journaled'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "acknowledged");
    assert_eq!(sequence, 1);
    let second_state: String = connection
        .query_row(
            "SELECT state FROM worker_commands WHERE command_id = 'command-inspection'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(second_state, "acknowledged");
    assert!(reopened.pending_reports().unwrap().is_empty());
}

#[test]
fn accepted_command_without_a_run_is_not_replayed_after_worker_restart() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 1, Arc::clone(&backend));
    let spec = run(
        "not-launched",
        job("not-launched", vec!["/bin/true".into()]),
    );
    let envelope = CommandEnvelope {
        command_id: CommandId::new("command-before-launch").unwrap(),
        target_worker_id: service.descriptor().worker_id().clone(),
        target_worker_incarnation: service.descriptor().incarnation(),
        command: WorkerCommand::StartRun(Box::new(StartRun::new(spec))),
    };
    service.accept_command(&envelope).unwrap();
    drop(service);

    let restarted = open_worker(&directory, 1, Arc::clone(&backend));
    assert_ne!(
        restarted.descriptor().incarnation(),
        envelope.target_worker_incarnation
    );
    assert_eq!(
        restarted.accept_command(&envelope),
        Err(WorkerError::WorkerMismatch)
    );
    assert_eq!(restarted.accepted_commands().unwrap(), vec![envelope]);
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 0);
    let run_count: i64 = Connection::open(directory.0.join("worker.sqlite3"))
        .unwrap()
        .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get(0))
        .unwrap();
    assert_eq!(run_count, 0);
}

#[test]
fn unacknowledged_reports_survive_a_worker_restart_with_their_original_incarnation() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 1, Arc::clone(&backend));
    let envelope = CommandEnvelope {
        command_id: CommandId::new("command-unacknowledged").unwrap(),
        target_worker_id: service.descriptor().worker_id().clone(),
        target_worker_incarnation: service.descriptor().incarnation(),
        command: WorkerCommand::InspectRun(conductor_core::InspectRun::new(
            RunId::new("missing-run").unwrap(),
        )),
    };
    service.accept_command(&envelope).unwrap();
    let report = service
        .complete_command(&envelope, r#"{"kind":"inspection","payload":null}"#)
        .unwrap();
    drop(service);

    let restarted = open_worker(&directory, 1, backend);
    assert_ne!(
        restarted.descriptor().incarnation(),
        report.worker_incarnation
    );
    assert_eq!(restarted.pending_reports().unwrap(), vec![report.clone()]);
    restarted
        .acknowledge_report(
            &report.command_id,
            report.worker_incarnation,
            report.report_sequence,
        )
        .unwrap();
    assert!(restarted.pending_reports().unwrap().is_empty());
}

#[test]
fn executable_digest_mismatch_fails_without_spawning_the_workload() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 1, Arc::clone(&backend));
    let marker = directory.0.join("must-not-exist");
    let job = job(
        "digest-mismatch",
        vec![
            "/bin/sh".into(),
            "-c".into(),
            format!("printf launched > {}", marker.display()),
        ],
    )
    .with_executable_digest(Some(
        conductor_core::ExecutableDigest::sha256("0".repeat(64)).unwrap(),
    ));
    let observation = service
        .start(StartRun::new(run("digest-mismatch", job)))
        .unwrap();
    assert_eq!(observation.state(), RunState::Failed);
    assert_eq!(
        observation.failure_reason(),
        Some(RunFailureReason::ExecutableDigestMismatch)
    );
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 0);
    assert!(!marker.exists());
}

#[test]
fn incompatible_executable_format_fails_before_backend_spawn() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 1, Arc::clone(&backend));
    let executable = directory.0.join("wrong-format");
    fs::write(&executable, b"not a native executable").unwrap();

    let observation = service
        .start(StartRun::new(run(
            "wrong-format",
            job(
                "wrong-format",
                vec![executable.to_string_lossy().into_owned()],
            ),
        )))
        .unwrap();

    assert_eq!(observation.state(), RunState::Failed);
    assert_eq!(
        observation.failure_reason(),
        Some(RunFailureReason::IncompatibleExecutable)
    );
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 0);
}

#[test]
fn worker_reports_spawn_exit_signal_and_capacity_outcomes() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 1, backend);
    let spawn_failure = service
        .start(StartRun::new(run(
            "missing",
            job("missing", vec!["/no/such/conductor-executable".into()]),
        )))
        .unwrap();
    assert_eq!(spawn_failure.state(), RunState::Failed);
    assert_eq!(
        spawn_failure.failure_reason(),
        Some(RunFailureReason::SpawnFailed)
    );

    let active = service
        .start(StartRun::new(run(
            "active",
            job(
                "active",
                vec!["/bin/sh".into(), "-c".into(), "sleep 0.25".into()],
            ),
        )))
        .unwrap();
    assert_eq!(active.state(), RunState::Running);
    assert_eq!(
        service.start(StartRun::new(run(
            "overflow",
            job("overflow", vec!["/bin/true".into()])
        ))),
        Err(WorkerError::CapacityReached)
    );
    assert_eq!(wait_for(&service, "active").state(), RunState::Succeeded);

    let start_deadline = std::time::Instant::now() + Duration::from_secs(5);
    let failed = loop {
        match service.start(StartRun::new(run(
            "nonzero",
            job(
                "nonzero",
                vec!["/bin/sh".into(), "-c".into(), "exit 17".into()],
            ),
        ))) {
            Err(WorkerError::CapacityReached) => {
                assert!(
                    std::time::Instant::now() < start_deadline,
                    "cleanup permit did not become available"
                );
                thread::sleep(Duration::from_millis(10));
            }
            result => break result.unwrap(),
        }
    };
    assert_eq!(failed.state(), RunState::Running);
    let failed = wait_for(&service, "nonzero");
    assert_eq!(failed.state(), RunState::Failed);
    assert_eq!(failed.exit_code(), Some(17));
    assert_eq!(failed.failure_reason(), Some(RunFailureReason::NonZeroExit));

    let start_deadline = std::time::Instant::now() + Duration::from_secs(5);
    let signaled = loop {
        match service.start(StartRun::new(run(
            "signal",
            job(
                "signal",
                vec!["/bin/sh".into(), "-c".into(), "kill -TERM $$".into()],
            ),
        ))) {
            Err(WorkerError::CapacityReached) => {
                assert!(
                    std::time::Instant::now() < start_deadline,
                    "cleanup permit did not become available before signal test"
                );
                thread::sleep(Duration::from_millis(10));
            }
            result => break result.unwrap(),
        }
    };
    assert_eq!(signaled.state(), RunState::Running);
    let signaled = wait_for(&service, "signal");
    assert_eq!(signaled.state(), RunState::Failed);
    assert!(signaled.termination_signal().is_some());
    assert_eq!(
        signaled.failure_reason(),
        Some(RunFailureReason::SignalTerminated)
    );
}

#[test]
fn process_is_cleaned_up_when_running_observation_cannot_be_persisted() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let marker = directory.0.join("should-not-run");
    let descendant_pid = directory.0.join("descendant.pid");
    *backend.ready_file.lock().unwrap() = Some(descendant_pid.clone());
    let service = open_worker(&directory, 1, Arc::clone(&backend));
    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_running BEFORE UPDATE OF state ON runs
             WHEN NEW.state = 'running'
             BEGIN SELECT RAISE(FAIL, 'injected running persistence failure'); END;",
        )
        .unwrap();
    drop(connection);
    let spec = run(
        "persist-failure",
        job(
            "persist-failure",
            vec![
                "/bin/sh".into(),
                "-c".into(),
                format!(
                    "sleep 30 & echo $! > {}; wait; touch {}",
                    descendant_pid.display(),
                    marker.display()
                ),
            ],
        ),
    );
    assert_eq!(
        service.start(StartRun::new(spec.clone())),
        Err(WorkerError::StorageUnavailable)
    );
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);
    assert!(!marker.exists());
    let pid = fs::read_to_string(&descendant_pid).unwrap();
    #[cfg(unix)]
    {
        let pid = pid.trim().parse::<i32>().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
            && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err());
    }
    let observed = service
        .inspect(conductor_core::InspectRun::new(spec.id().clone()))
        .unwrap()
        .unwrap();
    assert_eq!(observed.state(), RunState::Failed);

    // An idempotent retry returns the recorded result and never starts a
    // second process for the run whose first launch was cleaned up.
    assert_eq!(service.start(StartRun::new(spec)), Ok(observed));
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);
}

#[test]
fn uncertain_process_group_cleanup_keeps_run_unknown_and_cancels_readers() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    backend.kill_failures.store(1, Ordering::SeqCst);
    let service = open_worker(&directory, 1, Arc::clone(&backend));
    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_running BEFORE UPDATE OF state ON runs
             WHEN NEW.state = 'running'
             BEGIN SELECT RAISE(FAIL, 'injected running persistence failure'); END;",
        )
        .unwrap();
    drop(connection);
    let spec = run(
        "cleanup-uncertain",
        job("cleanup-uncertain", vec!["/bin/sleep".into(), "30".into()]),
    );

    let cleanup_started = std::time::Instant::now();
    assert_eq!(
        service.start(StartRun::new(spec.clone())),
        Err(WorkerError::StorageUnavailable)
    );
    assert!(cleanup_started.elapsed() < Duration::from_secs(2));
    let observed = service
        .inspect(conductor_core::InspectRun::new(spec.id().clone()))
        .unwrap()
        .unwrap();
    assert_eq!(observed.state(), RunState::Unknown);
    assert_eq!(
        observed.failure_reason(),
        Some(RunFailureReason::OutcomeUnknown)
    );
    assert_eq!(service.start(StartRun::new(spec)), Ok(observed));
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);
}

#[test]
fn terminal_persistence_outage_is_bounded_and_recovers_without_relaunch() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 1, Arc::clone(&backend));
    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_terminal BEFORE UPDATE OF state ON runs
             WHEN NEW.state IN ('succeeded', 'failed', 'cancelled', 'timed_out')
             BEGIN SELECT RAISE(FAIL, 'injected terminal persistence failure'); END;",
        )
        .unwrap();
    let spec = run(
        "terminal-retry",
        job(
            "terminal-retry",
            vec!["/bin/sh".into(), "-c".into(), "sleep 0.05; exit 17".into()],
        ),
    );
    assert_eq!(
        service.start(StartRun::new(spec.clone())).unwrap().state(),
        RunState::Running
    );

    let pending_status = wait_for_pending_persistence(&service, "terminal-retry");
    assert_eq!(pending_status.pending_persistence(), 1);
    assert!(pending_status.degraded());
    assert_eq!(pending_status.available_slots(), 0);
    let pending_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while service.status().unwrap().live_processes() != 0 {
        assert!(
            std::time::Instant::now() < pending_deadline,
            "process did not exit while terminal persistence was unavailable"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let pending_status = service.status().unwrap();
    assert_eq!(pending_status.pending_persistence(), 1);
    assert_eq!(pending_status.used_slots(), 1);
    assert_eq!(pending_status.live_processes(), 0);
    assert!(pending_status.degraded());
    assert_eq!(
        service.start(StartRun::new(run(
            "blocked-during-persistence",
            job("blocked-during-persistence", vec!["/bin/true".into()]),
        ))),
        Err(WorkerError::StorageUnavailable)
    );
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);

    connection
        .execute_batch("DROP TRIGGER reject_terminal;")
        .unwrap();
    let terminal = wait_for(&service, "terminal-retry");
    assert_eq!(terminal.state(), RunState::Failed);
    assert_eq!(terminal.exit_code(), Some(17));
    assert_eq!(
        terminal.failure_reason(),
        Some(RunFailureReason::NonZeroExit)
    );
    let status_deadline = std::time::Instant::now() + Duration::from_secs(5);
    let status = loop {
        let status = service.status().unwrap();
        if status.pending_persistence() == 0 {
            break status;
        }
        assert!(
            std::time::Instant::now() < status_deadline,
            "terminal persistence did not recover"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(status.pending_persistence(), 0);
    assert!(!status.degraded());
    assert!(status.available());
    assert_eq!(status.used_slots(), 0);
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);
}

#[test]
fn terminal_retry_backoff_yields_to_other_run_deadlines() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 3, Arc::clone(&backend));
    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_selected_terminal BEFORE UPDATE OF state ON runs
             WHEN NEW.run_id IN ('pending-a', 'pending-b')
               AND NEW.state IN ('succeeded', 'failed', 'cancelled', 'timed_out')
             BEGIN SELECT RAISE(FAIL, 'injected terminal persistence failure'); END;",
        )
        .unwrap();
    for id in ["pending-a", "pending-b"] {
        service
            .start(StartRun::new(run(
                id,
                job(id, vec!["/bin/sh".into(), "-c".into(), "sleep 0.05".into()]),
            )))
            .unwrap();
    }
    let deadline_job = JobSpec::new(
        JobId::new("deadline-job").unwrap(),
        vec!["/bin/sleep".into(), "30".into()],
        PathBuf::from("/tmp"),
        BTreeMap::new(),
        false,
        Some(150),
        50,
    )
    .unwrap();
    service
        .start(StartRun::new(run("deadline-run", deadline_job)))
        .unwrap();
    let terminal = wait_for(&service, "deadline-run");
    assert_eq!(terminal.state(), RunState::TimedOut);
    assert_eq!(
        terminal.termination_reason(),
        Some(conductor_core::RunTerminationReason::DeadlineExceeded)
    );
    let pending = service.status().unwrap();
    assert_eq!(pending.pending_persistence(), 2);
    assert_eq!(pending.used_slots(), 2);
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 3);
}

#[test]
fn shutdown_bounds_pending_terminal_retries_and_runtime_teardown() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 1, Arc::clone(&backend));
    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_terminal BEFORE UPDATE OF state ON runs
             WHEN NEW.state IN ('succeeded', 'failed', 'cancelled', 'timed_out')
             BEGIN SELECT RAISE(FAIL, 'injected terminal persistence failure'); END;",
        )
        .unwrap();
    service
        .start(StartRun::new(run(
            "shutdown-pending",
            job(
                "shutdown-pending",
                vec!["/bin/sh".into(), "-c".into(), "sleep 0.5".into()],
            ),
        )))
        .unwrap();
    wait_for_pending_count(&service, 1);

    let started = std::time::Instant::now();
    let result = service.shutdown();
    drop(service);
    assert!(started.elapsed() < Duration::from_secs(4));
    assert_eq!(
        result.unwrap_err().kind(),
        io::ErrorKind::TimedOut,
        "shutdown reports that the terminal result remains unresolved"
    );
}

#[cfg(unix)]
#[test]
fn pending_output_readers_do_not_block_other_run_deadlines() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 3, backend);
    let mut escaped = Vec::new();
    for id in ["escaped-a", "escaped-b"] {
        let marker = directory.0.join(format!("{id}.pid"));
        service
            .start(StartRun::new(run(id, escaped_pipe_job(id, marker.clone()))))
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let pid = loop {
            if let Ok(value) = fs::read_to_string(&marker)
                && let Ok(pid) = value.trim().parse()
            {
                break pid;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "escaped pipe holder did not write its process id"
            );
            thread::sleep(Duration::from_millis(5));
        };
        escaped.push(EscapedChild(pid));
    }

    let deadline_job = JobSpec::new(
        JobId::new("reader-deadline").unwrap(),
        vec!["/bin/sleep".into(), "30".into()],
        PathBuf::from("/tmp"),
        BTreeMap::new(),
        false,
        Some(120),
        50,
    )
    .unwrap();
    service
        .start(StartRun::new(run("reader-deadline", deadline_job)))
        .unwrap();
    let terminal = wait_for(&service, "reader-deadline");
    assert_eq!(terminal.state(), RunState::TimedOut);
    assert_eq!(wait_for(&service, "escaped-a").state(), RunState::Succeeded);
    assert_eq!(wait_for(&service, "escaped-b").state(), RunState::Succeeded);

    // Shutdown must account for pending readers and cancel their drain even
    // though the escaped descendants still hold the output descriptors.
    service.shutdown().unwrap();
    // Cleanup is guaranteed even when an assertion above fails.
    drop(escaped);
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        let completed: i64 = Connection::open(directory.0.join("worker.sqlite3"))
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM run_logs WHERE run_id IN ('escaped-a', 'escaped-b') AND stdout_closed = 1 AND stderr_closed = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        if completed == 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "reader cleanup did not finish"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn shutdown_announcement_is_lock_free_and_start_races_stay_owned() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let marker = directory.0.join("startup-release");
    *backend.ready_file.lock().unwrap() = Some(marker.clone());
    let service = Arc::new(open_worker(&directory, 2, Arc::clone(&backend)));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let starting_service = Arc::clone(&service);
    let starting = thread::spawn(move || {
        let result = starting_service.start(StartRun::new(run(
            "shutdown-race",
            job("shutdown-race", vec!["/bin/sleep".into(), "30".into()]),
        )));
        started_tx.send(result).unwrap();
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while backend.spawns.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);

    let readiness_started = std::time::Instant::now();
    assert!(!service.is_draining());
    assert!(readiness_started.elapsed() < Duration::from_millis(50));
    let shutdown_started = std::time::Instant::now();
    service.begin_shutdown();
    assert!(shutdown_started.elapsed() < Duration::from_millis(50));
    assert!(service.is_draining());

    fs::write(marker, "continue").unwrap();
    starting.join().unwrap();
    assert_eq!(
        started_rx.recv().unwrap().unwrap().state(),
        RunState::Running
    );
    let rejected = service.start(StartRun::new(run(
        "after-shutdown",
        job("after-shutdown", vec!["/bin/true".into()]),
    )));
    assert_eq!(rejected, Err(WorkerError::ShuttingDown));
    service.shutdown().unwrap();
    assert_eq!(
        wait_for(&service, "shutdown-race").state(),
        RunState::Cancelled
    );
    assert_eq!(backend.spawns.load(Ordering::SeqCst), 1);
}

#[cfg(unix)]
#[test]
fn log_persistence_failure_does_not_change_the_process_exit_outcome() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 1, backend);
    let connection = Connection::open(directory.0.join("worker.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_log_close BEFORE UPDATE OF stdout_closed ON run_logs
             BEGIN SELECT RAISE(FAIL, 'injected log persistence failure'); END;",
        )
        .unwrap();
    drop(connection);
    let observation = service
        .start(StartRun::new(run(
            "log-failure",
            job(
                "log-failure",
                vec!["/bin/sh".into(), "-c".into(), "exit 17".into()],
            ),
        )))
        .unwrap();
    assert_eq!(observation.state(), RunState::Running);
    let terminal = wait_for(&service, "log-failure");
    assert_eq!(terminal.state(), RunState::Failed);
    assert_eq!(terminal.exit_code(), Some(17));
    assert_eq!(
        terminal.failure_reason(),
        Some(RunFailureReason::NonZeroExit)
    );
}
