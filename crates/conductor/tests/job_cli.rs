use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Deref;
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
            std::env::temp_dir().join(format!("conductor-job-cli-{}-{nonce}", std::process::id()));
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

fn start_agent(root: &Path) -> (Agent, SocketAddr) {
    start_agent_with_capacity(root, 2)
}

fn start_agent_with_capacity(root: &Path, capacity: usize) -> (Agent, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral port");
    let address = listener.local_addr().expect("read listener address");
    drop(listener);
    let config_path = root.join("agent.json");
    let config = serde_json::json!({
        "role": "all",
        "listen": address.to_string(),
        "manager_data_dir": root.join("manager"),
        "worker_data_dir": root.join("worker"),
        "worker_id": "job-cli-worker",
        "worker_concurrency": capacity,
        "api_access": { "mode": "local" },
    });
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

fn api_json(address: SocketAddr, method: &str, path: &str) -> (u16, serde_json::Value) {
    let mut stream = TcpStream::connect(address).expect("connect to local agent");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    )
    .expect("write local API request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read local API response");
    let response = String::from_utf8(response).expect("HTTP response is UTF-8");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("HTTP response headers");
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .expect("HTTP status");
    let body = serde_json::from_str(body).expect("JSON API response");
    (status, body)
}

fn cli(address: SocketAddr, args: &[&str], input: Option<&[u8]>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_conductor"));
    command
        .args(["job", "--server"])
        .arg(format!("http://{address}"))
        .args(["--output", "json"])
        .args(args);
    if let Some(input) = input {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("run job CLI");
        child
            .stdin
            .take()
            .expect("stdin pipe")
            .write_all(input)
            .expect("write stdin JSON");
        child.wait_with_output().expect("wait for job CLI")
    } else {
        command.output().expect("run job CLI")
    }
}

fn job_json(id: &str, command: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": id,
        "command": ["/bin/echo", command],
        "cwd": "/tmp",
        "terminationGraceMs": 100,
    }))
    .expect("encode job")
}

fn sleep_job_json(id: &str, seconds: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": id,
        "command": ["/bin/sleep", seconds.to_string()],
        "cwd": "/tmp",
        "terminationGraceMs": 100,
    }))
    .expect("encode sleeping job")
}

fn wait_for_state(address: SocketAddr, run_id: &str, expected: &str) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, value) = api_json(address, "GET", &format!("/v1/runs/{run_id}"));
        assert_eq!(status, 200, "{value}");
        if value["state"] == expected {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "run {run_id} did not reach {expected}: {value}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn json_output(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout was not JSON (status {}): {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
    })
}

#[test]
fn submit_file_and_stdin_are_idempotent_and_wait_is_bounded() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent(&temp);
    let file = temp.join("job.json");
    let spec = job_json("file-submit", "file-run");
    fs::write(&file, &spec).expect("write job file");

    let first = cli(
        address,
        &[
            "submit",
            file.to_str().expect("UTF-8 path"),
            "--idempotency-key",
            "file-submit-key",
            "--wait",
            "--wait-timeout-seconds",
            "10",
        ],
        None,
    );
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_json = json_output(&first);
    assert_eq!(first_json["jobId"], "file-submit");
    assert_eq!(first_json["run"]["state"], "succeeded");

    let retry = cli(
        address,
        &[
            "submit",
            file.to_str().expect("UTF-8 path"),
            "--idempotency-key",
            "file-submit-key",
        ],
        None,
    );
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    let retry_json = json_output(&retry);
    assert_eq!(retry_json["runId"], first_json["runId"]);
    assert_eq!(retry_json["reusedRegistration"], true);

    let stdin_spec = job_json("stdin-submit", "stdin-run");
    let stdin_submit = cli(
        address,
        &["submit", "-", "--idempotency-key", "stdin-submit-key"],
        Some(&stdin_spec),
    );
    assert!(
        stdin_submit.status.success(),
        "{}",
        String::from_utf8_lossy(&stdin_submit.stderr)
    );
    assert_eq!(json_output(&stdin_submit)["jobId"], "stdin-submit");

    let stdin_retry = cli(
        address,
        &["submit", "-", "--idempotency-key", "stdin-submit-key"],
        Some(&stdin_spec),
    );
    assert!(
        stdin_retry.status.success(),
        "{}",
        String::from_utf8_lossy(&stdin_retry.stderr)
    );
    assert_eq!(
        json_output(&stdin_retry)["runId"],
        json_output(&stdin_submit)["runId"]
    );
}

#[test]
fn invalid_job_input_is_reported_before_api_submission() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent(&temp);
    let invalid = cli(
        address,
        &["register", "-"],
        Some(br#"{"id":"invalid","command":[],"unknown":"field"}"#),
    );
    assert!(!invalid.status.success());
    let error = json_output_stderr(&invalid)["error"].clone();
    assert_eq!(error["code"], "invalid_job");
    assert!(error["message"].as_str().unwrap().contains("unknown field"));

    let service_revision_draft = serde_json::json!({
        "definition": { "id": "service", "command": ["/bin/true"], "cwd": "/tmp", "terminationGraceMs": 100 },
        "workload": { "kind": "service" },
        "executableDigest": { "algorithm": "sha256", "value": "0000000000000000000000000000000000000000000000000000000000000000" },
        "executionRequirements": {},
        "resourceRequest": {},
    });
    let service_submit = cli(
        address,
        &["submit", "-", "--idempotency-key", "service-key"],
        Some(&serde_json::to_vec(&service_revision_draft).unwrap()),
    );
    assert!(!service_submit.status.success());
    let service_error = json_output_stderr(&service_submit)["error"].clone();
    assert_eq!(service_error["code"], "service_requires_deployment");
    assert!(
        service_error["message"]
            .as_str()
            .unwrap()
            .contains("revision create")
    );
}

#[test]
fn wait_timeout_preserves_ids_and_leaves_remote_run_active() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent_with_capacity(&temp, 1);
    let file = temp.join("long-job.json");
    fs::write(&file, sleep_job_json("wait-timeout-job", 5)).expect("write sleeping job");

    let timed_out = cli(
        address,
        &[
            "submit",
            file.to_str().unwrap(),
            "--idempotency-key",
            "wait-timeout-key",
            "--wait",
            "--wait-timeout-seconds",
            "1",
        ],
        None,
    );
    assert!(!timed_out.status.success());
    let partial = json_output_stderr(&timed_out)["error"]["partial"].clone();
    assert_eq!(
        json_output_stderr(&timed_out)["error"]["code"],
        "wait_timeout"
    );
    assert_eq!(partial["jobId"], "wait-timeout-job");
    assert!(partial["runId"].is_string());
    assert_eq!(partial["remoteRunStopped"], false);

    let active = wait_for_state(address, partial["runId"].as_str().unwrap(), "running");
    assert_eq!(active["runId"], partial["runId"]);
    let (status, _) = api_json(
        address,
        "POST",
        &format!("/v1/runs/{}/stop", partial["runId"].as_str().unwrap()),
    );
    assert_eq!(status, 202);
    wait_for_state(address, partial["runId"].as_str().unwrap(), "cancelled");
}

#[test]
fn capacity_failure_after_registration_returns_recovery_guidance() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent_with_capacity(&temp, 1);
    let occupying_file = temp.join("occupying.json");
    fs::write(&occupying_file, sleep_job_json("occupying-job", 8)).expect("write occupying job");
    let occupying = cli(
        address,
        &[
            "submit",
            occupying_file.to_str().unwrap(),
            "--idempotency-key",
            "occupying-key",
        ],
        None,
    );
    assert!(
        occupying.status.success(),
        "{}",
        String::from_utf8_lossy(&occupying.stderr)
    );
    let occupying_run_id = json_output(&occupying)["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    wait_for_state(address, &occupying_run_id, "running");

    let waiting_file = temp.join("waiting.json");
    fs::write(&waiting_file, job_json("waiting-job", "after-capacity")).expect("write waiting job");
    let rejected = cli(
        address,
        &[
            "submit",
            waiting_file.to_str().unwrap(),
            "--idempotency-key",
            "waiting-key",
        ],
        None,
    );
    assert!(!rejected.status.success());
    let error = json_output_stderr(&rejected)["error"].clone();
    assert_eq!(error["code"], "worker_capacity_reached");
    assert_eq!(error["partial"]["jobId"], "waiting-job");
    assert_eq!(error["partial"]["runStart"], "failed_or_unknown");
    let recovery = error["partial"]["recovery"].as_str().unwrap();
    assert!(recovery.contains("same input"));
    assert!(recovery.contains("same idempotency key"));

    let inspect = cli(address, &["inspect", "waiting-job"], None);
    assert!(
        inspect.status.success(),
        "{}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    assert_eq!(json_output(&inspect)["job"]["id"], "waiting-job");

    let (stop_status, _) = api_json(
        address,
        "POST",
        &format!("/v1/runs/{occupying_run_id}/stop"),
    );
    assert_eq!(stop_status, 202);
    wait_for_state(address, &occupying_run_id, "cancelled");
    let retry = cli(
        address,
        &[
            "submit",
            waiting_file.to_str().unwrap(),
            "--idempotency-key",
            "waiting-key",
        ],
        None,
    );
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert_eq!(json_output(&retry)["jobId"], "waiting-job");
    assert_eq!(json_output(&retry)["reusedRegistration"], true);
}

#[test]
fn job_lifecycle_reports_conflicts_and_keeps_deleted_ids_reserved() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent(&temp);
    let first = job_json("managed-job", "original");
    let file = temp.join("job.json");
    fs::write(&file, &first).expect("write job");

    let registered = cli(
        address,
        &["register", file.to_str().expect("UTF-8 path")],
        None,
    );
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );
    assert_eq!(json_output(&registered)["job"]["id"], "managed-job");

    let list = cli(address, &["list", "--limit", "10"], None);
    assert!(
        list.status.success(),
        "{}",
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(
        json_output(&list)["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|job| job["id"] == "managed-job")
    );

    let inspect = cli(address, &["inspect", "managed-job"], None);
    assert!(
        inspect.status.success(),
        "{}",
        String::from_utf8_lossy(&inspect.stderr)
    );

    let different = job_json("managed-job", "different");
    let conflict = cli(
        address,
        &["submit", "-", "--idempotency-key", "different-key"],
        Some(&different),
    );
    assert!(!conflict.status.success());
    assert_eq!(
        json_output_stderr(&conflict)["error"]["code"],
        "duplicate_job"
    );

    let deleted = cli(address, &["delete", "managed-job"], None);
    assert!(
        deleted.status.success(),
        "{}",
        String::from_utf8_lossy(&deleted.stderr)
    );
    assert_eq!(json_output(&deleted)["idReserved"], true);

    let reserved = cli(
        address,
        &["submit", "-", "--idempotency-key", "reserved-key"],
        Some(&first),
    );
    assert!(!reserved.status.success());
    assert_eq!(
        json_output_stderr(&reserved)["error"]["code"],
        "reserved_job_id"
    );

    let missing = cli(address, &["inspect", "managed-job"], None);
    assert!(!missing.status.success());
    assert_eq!(
        json_output_stderr(&missing)["error"]["code"],
        "job_not_found"
    );
}

fn json_output_stderr(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stderr).unwrap_or_else(|_| {
        panic!(
            "stderr was not JSON: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn revision_create_and_inspect_use_stable_job_and_revision_ids() {
    let _guard = lock_tests();
    let temp = TempDir::new();
    let (_agent, address) = start_agent(&temp);
    let job = job_json("revision-job", "revision");
    let job_file = temp.join("job.json");
    fs::write(&job_file, &job).expect("write job");
    let registered = cli(address, &["register", job_file.to_str().unwrap()], None);
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );

    let draft = serde_json::json!({
        "definition": {
            "id": "revision-job",
            "command": ["/bin/echo", "revision"],
            "cwd": "/tmp",
            "terminationGraceMs": 100,
        },
        "workload": { "kind": "one_shot" },
        "executableDigest": { "algorithm": "sha256", "value": "0000000000000000000000000000000000000000000000000000000000000000" },
        "executionRequirements": {},
        "resourceRequest": {},
    });
    let revision_file = temp.join("revision.json");
    fs::write(&revision_file, serde_json::to_vec(&draft).unwrap()).expect("write revision");
    let created = cli(
        address,
        &[
            "revision",
            "create",
            "revision-job",
            revision_file.to_str().unwrap(),
            "--idempotency-key",
            "revision-key",
        ],
        None,
    );
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert_eq!(json_output(&created)["revisionId"]["revision"], 1);

    let inspected = cli(address, &["revision", "inspect", "revision-job", "1"], None);
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    assert_eq!(json_output(&inspected)["revision"]["id"]["revision"], 1);
}
