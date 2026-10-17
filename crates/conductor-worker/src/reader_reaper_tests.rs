#![cfg(unix)]

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
    InspectRun, JobId, JobSpec, MAX_WORKER_CAPACITY, RunId, RunSpec, RunState, StartRun, StopRun,
    WorkerId,
};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use super::{ExecutionBackend, SpawnGuard, WorkerError, WorkerService};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "conductor-reader-reaper-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create test directory");
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct TestBackend(AtomicUsize);

impl ExecutionBackend for TestBackend {
    fn spawn(&self, _run_id: &str, job: &JobSpec) -> io::Result<Child> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let mut command = Command::new(&job.command()[0]);
        command
            .args(&job.command()[1..])
            .current_dir(job.cwd())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        use std::os::unix::process::CommandExt;
        command.process_group(0);
        if !job.inherit_env() {
            command.env_clear();
        }
        command.envs(job.env());
        command.spawn()
    }

    fn terminate_process_group(&self, group_id: u32) -> io::Result<()> {
        kill(Pid::from_raw(-(group_id as i32)), Signal::SIGTERM).map_err(io::Error::from)
    }

    fn kill_process_group(&self, group_id: u32) -> io::Result<()> {
        kill(Pid::from_raw(-(group_id as i32)), Signal::SIGKILL).map_err(io::Error::from)
    }
}

fn job(id: &str, command: &[&str], timeout_ms: Option<u64>) -> JobSpec {
    JobSpec::new(
        JobId::new(id).expect("job id"),
        command.iter().map(|part| (*part).to_owned()).collect(),
        PathBuf::from("/tmp"),
        BTreeMap::new(),
        false,
        timeout_ms,
        50,
    )
    .expect("job spec")
}

fn wait_for_terminal(service: &WorkerService, id: &str) -> conductor_core::RunObservation {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(run) = service
            .inspect(InspectRun::new(RunId::new(id).expect("run id")))
            .expect("inspect run")
            && run.state().is_terminal()
        {
            return run;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "run {id} did not reach a terminal state"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn cleanup_admission_bounds_reaper_backlog_without_blocking_supervision() {
    let directory = TestDirectory::new();
    let backend = Arc::new(TestBackend::default());
    let service = WorkerService::open(
        &directory.0,
        WorkerId::new("worker-test").unwrap(),
        MAX_WORKER_CAPACITY,
        Arc::clone(&backend) as Arc<dyn ExecutionBackend>,
    )
    .unwrap();
    service.reader_reaper.pause_for_test();

    // Each completed process releases its execution slot, but cleanup capacity
    // remains reserved while the reaper is held.
    for index in 0..MAX_WORKER_CAPACITY - 2 {
        let id = format!("completed-{index}");
        let spec = RunSpec::new(
            RunId::new(&id).unwrap(),
            WorkerId::new("worker-test").unwrap(),
            job(&id, &["/usr/bin/true"], None),
        );
        service.start(StartRun::new(spec)).unwrap();
        let terminal = wait_for_terminal(&service, &id);
        assert_eq!(terminal.state(), RunState::Succeeded, "{terminal:?}");
    }

    let deadline_spec = RunSpec::new(
        RunId::new("deadline-while-reaper-held").unwrap(),
        WorkerId::new("worker-test").unwrap(),
        job(
            "deadline-while-reaper-held",
            &["/bin/sleep", "30"],
            Some(150),
        ),
    );
    service.start(StartRun::new(deadline_spec)).unwrap();
    let stopped_spec = RunSpec::new(
        RunId::new("stop-while-reaper-held").unwrap(),
        WorkerId::new("worker-test").unwrap(),
        job("stop-while-reaper-held", &["/bin/sleep", "30"], None),
    );
    service.start(StartRun::new(stopped_spec)).unwrap();

    assert_eq!(
        service.start(StartRun::new(RunSpec::new(
            RunId::new("beyond-cleanup-capacity").unwrap(),
            WorkerId::new("worker-test").unwrap(),
            job("beyond-cleanup-capacity", &["/usr/bin/true"], None),
        ))),
        Err(WorkerError::CapacityReached)
    );
    assert_eq!(backend.0.load(Ordering::SeqCst), MAX_WORKER_CAPACITY);
    assert_eq!(service.cleanup_admission.available_permits(), 0);
    assert!(!service.status().unwrap().available());

    service
        .stop(StopRun::new(RunId::new("stop-while-reaper-held").unwrap()))
        .unwrap();
    assert_eq!(
        wait_for_terminal(&service, "stop-while-reaper-held").state(),
        RunState::Cancelled
    );
    assert_eq!(
        wait_for_terminal(&service, "deadline-while-reaper-held").state(),
        RunState::TimedOut
    );
    assert_eq!(service.reader_reaper.pending(), MAX_WORKER_CAPACITY);

    service.reader_reaper.resume_for_test();
    let cleanup_deadline = std::time::Instant::now() + Duration::from_secs(10);
    while service.reader_reaper.pending() != 0 {
        assert!(
            std::time::Instant::now() < cleanup_deadline,
            "reader reaper did not drain its bounded queue"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        service.cleanup_admission.available_permits(),
        MAX_WORKER_CAPACITY
    );

    let failed_start = service
        .start(StartRun::new(RunSpec::new(
            RunId::new("spawn-failure-releases-reservation").unwrap(),
            WorkerId::new("worker-test").unwrap(),
            job(
                "spawn-failure-releases-reservation",
                &["/path/that/does/not/exist"],
                None,
            ),
        )))
        .unwrap();
    assert_eq!(failed_start.state(), RunState::Failed);
    assert_eq!(
        service.cleanup_admission.available_permits(),
        MAX_WORKER_CAPACITY
    );

    service.begin_shutdown();
    assert!(service.is_draining());
    service.shutdown().unwrap();
    assert_eq!(service.reader_reaper.pending(), 0);
    assert_eq!(
        service.cleanup_admission.available_permits(),
        MAX_WORKER_CAPACITY
    );
}

#[test]
fn spawn_guard_drop_enqueues_cleanup_without_waiting_for_reaper() {
    let directory = TestDirectory::new();
    let backend: Arc<dyn ExecutionBackend> = Arc::new(TestBackend::default());
    let service = WorkerService::open(
        &directory.0,
        WorkerId::new("worker-test").unwrap(),
        1,
        Arc::clone(&backend),
    )
    .unwrap();
    service.reader_reaper.pause_for_test();
    let permit = Arc::clone(&service.cleanup_admission)
        .try_acquire_owned()
        .expect("reserve cleanup capacity");
    let child = Command::new("/bin/sleep")
        .arg("30")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cleanup fixture");
    let group_id = child.id();
    let guard = SpawnGuard {
        backend,
        reader_reaper: service.reader_reaper.clone(),
        group_id,
        child: Some(child),
        readers: Vec::new(),
        cleanup_permit: Some(permit),
        cancel_readers: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        cleaned: false,
    };
    let started = std::time::Instant::now();
    drop(guard);
    assert!(started.elapsed() < Duration::from_millis(250));
    assert_eq!(service.reader_reaper.pending(), 1);
    service.reader_reaper.resume_for_test();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while service.reader_reaper.pending() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "cleanup did not finish"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn reaper_runs_backend_cleanup_when_child_and_readers_are_already_gone() {
    let directory = TestDirectory::new();
    let backend: Arc<dyn ExecutionBackend> = Arc::new(TestBackend::default());
    let service = WorkerService::open(
        &directory.0,
        WorkerId::new("worker-test").unwrap(),
        1,
        backend,
    )
    .unwrap();
    service.reader_reaper.pause_for_test();
    let permit = Arc::clone(&service.cleanup_admission)
        .try_acquire_owned()
        .expect("reserve cleanup capacity");
    let cleaned = Arc::new(AtomicUsize::new(0));
    let callback_state = Arc::clone(&cleaned);
    service
        .reader_reaper
        .enqueue(
            None,
            Vec::new(),
            permit,
            Some(Box::new(move || {
                callback_state.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })),
        )
        .expect("enqueue backend cleanup");

    assert_eq!(service.reader_reaper.pending(), 1);
    service.reader_reaper.resume_for_test();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while service.reader_reaper.pending() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "reaper did not run cleanup"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
}
