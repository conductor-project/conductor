use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use conductor_core::{
    JobId, JobSpec, RunFailureReason, RunId, RunSpec, RunState, StartRun, WorkerId,
};
use conductor_worker::{ExecutionBackend, WorkerError, WorkerService};

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("conductor-worker-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
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
}

impl ExecutionBackend for TestBackend {
    fn spawn(&self, job: &JobSpec) -> io::Result<Child> {
        self.spawns.fetch_add(1, Ordering::SeqCst);
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

    fn kill_process_group(&self, _group_id: u32) -> io::Result<()> {
        Ok(())
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

#[test]
fn worker_acceptance_is_deduplicated_and_terminal_observations_survive_reopen() {
    let directory = TempDir::new();
    let backend = Arc::new(TestBackend::default());
    let service = open_worker(&directory, 2, Arc::clone(&backend));
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
    let persisted = reopened
        .inspect(conductor_core::InspectRun::new(RunId::new("once").unwrap()))
        .unwrap()
        .unwrap();
    assert_eq!(persisted, terminal);
    assert_eq!(persisted.revision(), 4);
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

    let failed = service
        .start(StartRun::new(run(
            "nonzero",
            job(
                "nonzero",
                vec!["/bin/sh".into(), "-c".into(), "exit 17".into()],
            ),
        )))
        .unwrap();
    assert_eq!(failed.state(), RunState::Running);
    let failed = wait_for(&service, "nonzero");
    assert_eq!(failed.state(), RunState::Failed);
    assert_eq!(failed.exit_code(), Some(17));
    assert_eq!(failed.failure_reason(), Some(RunFailureReason::NonZeroExit));

    let signaled = service
        .start(StartRun::new(run(
            "signal",
            job(
                "signal",
                vec!["/bin/sh".into(), "-c".into(), "kill -TERM $$".into()],
            ),
        )))
        .unwrap();
    assert_eq!(signaled.state(), RunState::Running);
    let signaled = wait_for(&service, "signal");
    assert_eq!(signaled.state(), RunState::Failed);
    assert!(signaled.termination_signal().is_some());
    assert_eq!(
        signaled.failure_reason(),
        Some(RunFailureReason::SignalTerminated)
    );
}
