use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Deref;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn lock_tests() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("conductor-run-cli-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&path).expect("create isolated directory");
        Self(path)
    }
}

impl Deref for TempDir {
    type Target = Path;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Agent(Child);

impl Drop for Agent {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn start_agent(root: &Path, capacity: usize, log_cap: Option<u64>) -> (Agent, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral port");
    let address = listener.local_addr().expect("read listener address");
    drop(listener);
    let config_path = root.join("agent.json");
    let mut config = serde_json::json!({
        "role": "all",
        "listen": address.to_string(),
        "manager_data_dir": root.join("manager"),
        "worker_data_dir": root.join("worker"),
        "worker_id": "run-cli-worker",
        "worker_concurrency": capacity,
        "api_access": { "mode": "local" },
    });
    if let Some(log_cap) = log_cap {
        config["log_max_bytes_per_stream"] = serde_json::json!(log_cap);
    }
    fs::write(
        &config_path,
        serde_json::to_vec(&config).expect("encode config"),
    )
    .expect("write config");
    let child = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["agent", "--config"])
        .arg(config_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start debug agent");
    let agent = Agent(child);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            return (agent, address);
        }
        assert!(
            Instant::now() < deadline,
            "agent did not become ready at {address}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn cli(address: SocketAddr, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["run", "--server"])
        .arg(format!("http://{address}"))
        .args(["--output", "json"])
        .args(args)
        .output()
        .expect("run CLI")
}

fn job_cli(address: SocketAddr, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["job", "--server"])
        .arg(format!("http://{address}"))
        .args(["--output", "json"])
        .args(args)
        .output()
        .expect("run job CLI")
}

fn write_job(root: &Path, id: &str, command: &[&str]) -> PathBuf {
    let path = root.join(format!("{id}.json"));
    let value = serde_json::json!({
        "id": id,
        "command": command,
        "cwd": "/tmp",
        "terminationGraceMs": 100,
    });
    fs::write(&path, serde_json::to_vec(&value).expect("encode job")).expect("write job");
    path
}

fn json_stdout(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout was not JSON (status {}): {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
    })
}

fn json_stderr(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stderr).unwrap_or_else(|_| {
        panic!(
            "stderr was not JSON: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn api_json(address: SocketAddr, method: &str, path: &str) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(address).expect("connect to local agent");
    write!(stream, "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n").expect("write request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    let response = String::from_utf8(response).expect("HTTP response is UTF-8");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("HTTP response headers");
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("HTTP status");
    let body = serde_json::from_str(body).expect("JSON response");
    (status, body)
}

fn wait_for_state(address: SocketAddr, run_id: &str, expected: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, run) = api_json(address, "GET", &format!("/v1/runs/{run_id}"));
        assert_eq!(status, 200, "{run}");
        if run["state"] == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "run {run_id} did not reach {expected}: {run}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn checked_in_first_job_example_submits_waits_and_exposes_logs() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (agent, address) = start_agent(&temp, 2, None);
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/cli-job.json");
    let submitted = job_cli(
        address,
        &[
            "submit",
            example.to_str().unwrap(),
            "--idempotency-key",
            "readme-first-job",
        ],
    );
    assert!(
        submitted.status.success(),
        "{}",
        String::from_utf8_lossy(&submitted.stderr)
    );
    let result = json_stdout(&submitted);
    let run_id = result["runId"].as_str().expect("run ID");
    let waited = cli(address, &["wait", run_id, "--timeout-seconds", "5"]);
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    assert_eq!(json_stdout(&waited)["state"], "succeeded");

    let logs = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["run", "--server"])
        .arg(format!("http://{address}"))
        .args([
            "logs", run_id, "--stream", "stdout", "--offset", "0", "--limit", "4096",
        ])
        .output()
        .expect("read raw run logs");
    assert!(logs.status.success());
    assert_eq!(logs.stdout, b"hello conductor\n");

    let inspected = cli(address, &["inspect", run_id]);
    assert!(inspected.status.success());
    assert_eq!(json_stdout(&inspected)["state"], "succeeded");
    drop(agent);
}

#[test]
fn successful_and_failed_runs_are_listed_inspected_and_waited() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (mut agent, address) = start_agent(&temp, 2, None);
    let success_job = write_job(&temp, "run-success", &["/bin/echo", "done"]);
    assert!(
        job_cli(address, &["register", success_job.to_str().unwrap()])
            .status
            .success()
    );
    let started = cli(
        address,
        &["start", "run-success", "--idempotency-key", "success-key"],
    );
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    let success_id = json_stdout(&started)["runId"].as_str().unwrap().to_owned();
    let waited = cli(address, &["wait", &success_id, "--timeout-seconds", "5"]);
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    assert_eq!(json_stdout(&waited)["state"], "succeeded");
    let inspect = cli(address, &["inspect", &success_id]);
    assert!(inspect.status.success());

    let failed_job = write_job(&temp, "run-failed", &["/bin/sh", "-c", "exit 7"]);
    assert!(
        job_cli(address, &["register", failed_job.to_str().unwrap()])
            .status
            .success()
    );
    let failed = cli(
        address,
        &["start", "run-failed", "--idempotency-key", "failed-key"],
    );
    assert!(failed.status.success());
    let failed_id = json_stdout(&failed)["runId"].as_str().unwrap().to_owned();
    let failed_wait = cli(address, &["wait", &failed_id, "--timeout-seconds", "5"]);
    assert!(!failed_wait.status.success());
    assert_eq!(json_stderr(&failed_wait)["error"]["code"], "run_failed");
    assert_eq!(
        json_stderr(&failed_wait)["error"]["partial"]["run"]["state"],
        "failed"
    );

    let list = cli(
        address,
        &[
            "list",
            "--job-id",
            "run-success",
            "--state",
            "succeeded",
            "--limit",
            "1",
        ],
    );
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    assert_eq!(json_stdout(&list)["runs"].as_array().unwrap().len(), 1);
    let empty = cli(address, &["list", "--job-id", "no-runs"]);
    assert!(empty.status.success());
    assert!(json_stdout(&empty)["runs"].as_array().unwrap().is_empty());
    let missing = cli(address, &["inspect", "missing-run"]);
    assert!(!missing.status.success());
    assert_eq!(json_stderr(&missing)["error"]["code"], "run_not_found");

    agent.0.kill().expect("stop agent for connection-loss case");
    let _ = agent.0.wait();
    let disconnected = cli(address, &["list"]);
    assert!(!disconnected.status.success());
    assert_eq!(json_stderr(&disconnected)["error"]["code"], "api_error");
}

#[test]
fn timeout_and_interrupt_do_not_stop_remote_run_but_explicit_stop_does() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent(&temp, 1, None);
    let job = write_job(&temp, "long-run", &["/bin/sleep", "30"]);
    assert!(
        job_cli(address, &["register", job.to_str().unwrap()])
            .status
            .success()
    );
    let started = cli(
        address,
        &["start", "long-run", "--idempotency-key", "long-key"],
    );
    assert!(started.status.success());
    let run_id = json_stdout(&started)["runId"].as_str().unwrap().to_owned();
    wait_for_state(address, &run_id, "running");

    let timeout = cli(address, &["wait", &run_id, "--timeout-seconds", "1"]);
    assert!(!timeout.status.success());
    assert_eq!(json_stderr(&timeout)["error"]["code"], "wait_timeout");
    assert_eq!(json_stderr(&timeout)["error"]["partial"]["runId"], run_id);
    assert_eq!(
        json_stderr(&timeout)["error"]["partial"]["remoteRunStopped"],
        false
    );
    let still_running = cli(address, &["inspect", &run_id]);
    assert_eq!(json_stdout(&still_running)["state"], "running");

    let mut interrupted = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["run", "--server"])
        .arg(format!("http://{address}"))
        .args(["wait", &run_id, "--timeout-seconds", "30"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start wait command");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(interrupted.id() as i32),
        nix::sys::signal::Signal::SIGINT,
    )
    .expect("interrupt local wait");
    let status = interrupted.wait().expect("wait for interrupted CLI");
    assert_eq!(status.signal(), Some(nix::libc::SIGINT));
    let still_running = cli(address, &["inspect", &run_id]);
    assert_eq!(json_stdout(&still_running)["state"], "running");

    let unresolved = cli(address, &["resolve", &run_id, "--reason", "test decision"]);
    assert!(!unresolved.status.success());
    assert_eq!(
        json_stderr(&unresolved)["error"]["code"],
        "resolution_conflict"
    );
    let stop = cli(address, &["stop", &run_id]);
    assert!(
        stop.status.success(),
        "{}",
        String::from_utf8_lossy(&stop.stderr)
    );
    let cancelled = cli(address, &["wait", &run_id, "--timeout-seconds", "5"]);
    assert!(!cancelled.status.success());
    assert_eq!(json_stderr(&cancelled)["error"]["code"], "run_cancelled");
}

#[test]
fn unknown_runs_require_and_accept_an_explicit_abandonment_decision() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (mut agent, address) = start_agent(&temp, 1, None);
    let pid_file = temp.join("workload.pid");
    let command = format!("echo $$ > {}; exec sleep 60", pid_file.display());
    let job = write_job(&temp, "unknown-run", &["/bin/sh", "-c", &command]);
    assert!(
        job_cli(address, &["register", job.to_str().unwrap()])
            .status
            .success()
    );
    let started = cli(
        address,
        &["start", "unknown-run", "--idempotency-key", "unknown-key"],
    );
    assert!(started.status.success());
    let run_id = json_stdout(&started)["runId"].as_str().unwrap().to_owned();
    wait_for_state(address, &run_id, "running");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pid_file.exists() {
        assert!(Instant::now() < deadline, "workload did not write its PID");
        thread::sleep(Duration::from_millis(10));
    }

    agent
        .0
        .kill()
        .expect("kill agent to simulate abrupt daemon loss");
    let _ = agent.0.wait();
    let workload_pid = fs::read_to_string(&pid_file)
        .expect("read workload PID")
        .trim()
        .parse::<i32>()
        .expect("parse workload PID");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(workload_pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("externally stop ambiguous workload before resolving it");

    let (_restarted, address) = start_agent(&temp, 1, None);
    let unknown = cli(address, &["wait", &run_id, "--timeout-seconds", "2"]);
    assert!(!unknown.status.success());
    assert_eq!(
        json_stderr(&unknown)["error"]["code"],
        "run_outcome_unknown"
    );
    let unresolved = cli(address, &["inspect", &run_id]);
    assert_eq!(json_stdout(&unresolved)["state"], "unknown");

    let reason = "Externally verified workload cleanup";
    let resolved = cli(address, &["resolve", &run_id, "--reason", reason]);
    assert!(
        resolved.status.success(),
        "{}",
        String::from_utf8_lossy(&resolved.stderr)
    );
    assert_eq!(json_stdout(&resolved)["state"], "abandoned");
    assert_eq!(json_stdout(&resolved)["resolutionReason"], reason);
    let retry = cli(address, &["resolve", &run_id, "--reason", reason]);
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let conflict = cli(
        address,
        &["resolve", &run_id, "--reason", "different decision"],
    );
    assert!(!conflict.status.success());
    assert_eq!(
        json_stderr(&conflict)["error"]["code"],
        "resolution_conflict"
    );
}

#[test]
fn logs_follow_final_bytes_and_preserve_binary_data_and_offsets() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent(&temp, 1, Some(2));
    let job = write_job(
        &temp,
        "binary-logs",
        &["/bin/sh", "-c", "printf '\\377a\\000b'"],
    );
    assert!(
        job_cli(address, &["register", job.to_str().unwrap()])
            .status
            .success()
    );
    let started = cli(
        address,
        &[
            "start",
            "binary-logs",
            "--idempotency-key",
            "binary-log-key",
        ],
    );
    assert!(started.status.success());
    let run_id = json_stdout(&started)["runId"].as_str().unwrap().to_owned();
    let waited = cli(address, &["wait", &run_id, "--timeout-seconds", "5"]);
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );

    let followed = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["run", "--server"])
        .arg(format!("http://{address}"))
        .args([
            "logs",
            &run_id,
            "--stream",
            "stdout",
            "--limit",
            "1",
            "--follow",
            "--poll-interval-ms",
            "50",
        ])
        .output()
        .expect("follow logs");
    assert!(
        followed.status.success(),
        "{}",
        String::from_utf8_lossy(&followed.stderr)
    );
    assert_eq!(followed.stdout, vec![0xff, b'a']);

    let json_logs = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["run", "--server"])
        .arg(format!("http://{address}"))
        .args([
            "--output", "json", "logs", &run_id, "--offset", "0", "--limit", "2",
        ])
        .output()
        .expect("read JSON logs");
    assert!(
        json_logs.status.success(),
        "{}",
        String::from_utf8_lossy(&json_logs.stderr)
    );
    let frame: serde_json::Value =
        serde_json::from_slice(&json_logs.stdout).expect("JSON log frame");
    assert_eq!(frame["truncated"], true);
    assert_eq!(frame["eof"], true);
    assert_eq!(frame["offset"], 0);
    assert_eq!(frame["nextOffset"], 2);

    let beyond_end = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["run", "--server"])
        .arg(format!("http://{address}"))
        .args(["logs", &run_id, "--offset", "99", "--limit", "2"])
        .output()
        .expect("read beyond retained end");
    assert!(beyond_end.status.success());
    assert!(beyond_end.stdout.is_empty());
}

#[test]
fn checked_in_sleep_fixture_registers_through_the_debug_cli() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent(&temp, 1, None);
    let fixture = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/cli-sleep-job.json");
    let registered = job_cli(address, &["register", fixture.to_str().unwrap()]);
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );
    assert_eq!(json_stdout(&registered)["job"]["id"], "mvp3-sleep-job");
    let inspected = job_cli(address, &["inspect", "mvp3-sleep-job"]);
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    assert_eq!(json_stdout(&inspected)["job"]["command"][0], "/bin/sleep");
}
