use std::fs;
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
        let path = std::env::temp_dir().join(format!(
            "conductor-deployment-cli-{}-{nonce}",
            std::process::id()
        ));
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

fn start_agent(root: &Path, role: &str) -> (Agent, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral port");
    let address = listener.local_addr().expect("read listener address");
    drop(listener);
    let config_path = root.join("agent.json");
    let mut config = serde_json::json!({
        "role": role,
        "listen": address.to_string(),
        "manager_data_dir": root.join("manager"),
        "api_access": { "mode": "local" },
    });
    if role == "all" {
        config["worker_data_dir"] = serde_json::json!(root.join("worker"));
        config["worker_id"] = serde_json::json!("deployment-cli-worker");
        config["worker_concurrency"] = serde_json::json!(2);
    } else {
        let transport_listener = TcpListener::bind("127.0.0.1:0").expect("reserve transport port");
        let transport_address = transport_listener.local_addr().expect("transport address");
        drop(transport_listener);
        config["worker_transport_listen"] = serde_json::json!(transport_address.to_string());
        config["manager_identity_dir"] = serde_json::json!(root.join("identity"));
        config["manager_server_name"] = serde_json::json!("localhost");
        config["worker_id"] = serde_json::json!("disconnected-worker");
        config["worker_transport_capacity"] = serde_json::json!(1);
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

fn run(address: SocketAddr, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["deployment", "--server"])
        .arg(format!("http://{address}"))
        .args(["--output", "json"])
        .args(args)
        .output()
        .expect("run deployment CLI")
}

fn job(address: SocketAddr, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["job", "--server"])
        .arg(format!("http://{address}"))
        .args(["--output", "json"])
        .args(args)
        .output()
        .expect("run job CLI")
}

fn json(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "stdout was not JSON (status {}): {}; stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
    })
}

fn create_service_revisions(root: &Path, address: SocketAddr, count: u64) {
    create_service_revisions_with_command(root, address, count, &["/bin/sleep", "30"]);
}

fn create_service_revisions_with_command(
    root: &Path,
    address: SocketAddr,
    count: u64,
    command: &[&str],
) {
    let base = serde_json::json!({
        "id": "deployment-cli-service",
        "command": command,
        "cwd": "/tmp",
        "terminationGraceMs": 100,
    });
    let job_path = root.join("service-job.json");
    fs::write(&job_path, serde_json::to_vec(&base).unwrap()).expect("write job");
    let registered = job(address, &["register", job_path.to_str().unwrap()]);
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );

    for revision in 1..=count {
        let draft = serde_json::json!({
            "definition": {
                "id": "deployment-cli-service",
                "command": command,
                "cwd": "/tmp",
                "terminationGraceMs": 100,
            },
            "workload": {
                "kind": "service",
                "identity": { "namespace": "default", "name": "deployment-cli-service" },
            },
            "executableDigest": { "algorithm": "sha256", "value": "0000000000000000000000000000000000000000000000000000000000000000" },
            "executionRequirements": {},
            "resourceRequest": { "cpuMillis": 250, "memoryBytes": 268435456 },
        });
        let draft_path = root.join(format!("revision-{revision}.json"));
        fs::write(&draft_path, serde_json::to_vec(&draft).unwrap()).expect("write revision draft");
        let key = format!("deployment-cli-revision-{revision}");
        let created = job(
            address,
            &[
                "revision",
                "create",
                "deployment-cli-service",
                draft_path.to_str().unwrap(),
                "--idempotency-key",
                &key,
            ],
        );
        assert!(
            created.status.success(),
            "{}",
            String::from_utf8_lossy(&created.stderr)
        );
        assert_eq!(json(&created)["revisionId"]["revision"], revision);
    }
}

fn apply_input(root: &Path, revision: u64, replicas: u32) -> PathBuf {
    let path = root.join(format!("deploy-{revision}-{replicas}.json"));
    fs::write(
        &path,
        serde_json::to_vec(&serde_json::json!({"revision":revision,"replicas":replicas})).unwrap(),
    )
    .expect("write deploy request");
    path
}

fn generation_arg(value: &serde_json::Value) -> String {
    value["generation"]
        .as_u64()
        .expect("generation")
        .to_string()
}

fn example(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples")
        .join(name)
}

fn revision_with_runtime_digest(root: &Path, fixture: &str) -> PathBuf {
    let digest = Command::new("shasum")
        .args(["-a", "256", "/bin/sleep"])
        .output()
        .expect("hash the service executable");
    assert!(digest.status.success(), "shasum could not hash /bin/sleep");
    let digest = String::from_utf8(digest.stdout)
        .expect("digest output is UTF-8")
        .split_ascii_whitespace()
        .next()
        .expect("digest value")
        .to_owned();
    let mut draft: serde_json::Value = serde_json::from_slice(
        &fs::read(example(fixture)).expect("read checked-in revision fixture"),
    )
    .expect("checked-in revision fixture is JSON");
    draft["executableDigest"]["value"] = serde_json::Value::String(digest);
    let output = root.join(format!("runtime-{fixture}"));
    fs::write(
        &output,
        serde_json::to_vec(&draft).expect("encode runtime revision fixture"),
    )
    .expect("write runtime revision fixture");
    output
}

#[test]
fn checked_in_deployment_examples_follow_documented_generation_workflow() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "all");
    let service_job = example("cli-service-job.json");
    let registered = job(address, &["register", service_job.to_str().unwrap()]);
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );

    for (number, fixture) in [
        (1, "cli-service-revision.json"),
        (2, "cli-service-revision-v2.json"),
    ] {
        let key = format!("example-revision-{number}");
        let revision_file = revision_with_runtime_digest(&root, fixture);
        let created = job(
            address,
            &[
                "revision",
                "create",
                "mvp3-demo-service",
                revision_file.to_str().unwrap(),
                "--idempotency-key",
                &key,
            ],
        );
        assert!(
            created.status.success(),
            "{}",
            String::from_utf8_lossy(&created.stderr)
        );
        assert_eq!(json(&created)["revisionId"]["revision"], number);
    }

    let applied = run(
        address,
        &[
            "apply",
            "mvp3-demo-service",
            example("cli-deployment.json").to_str().unwrap(),
            "--idempotency-key",
            "example-deployment-create",
            "--wait",
            "--wait-timeout-seconds",
            "15",
        ],
    );
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    assert_eq!(json(&applied)["generation"], 1);
    assert_eq!(json(&applied)["revision"], 1);
    assert_eq!(json(&applied)["status"], "ready");

    let inspected = run(address, &["inspect", "mvp3-demo-service"]);
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    assert_eq!(json(&inspected)["generation"], 1);
    assert_eq!(json(&inspected)["desiredReplicas"], 2);

    let updated = run(
        address,
        &[
            "apply",
            "mvp3-demo-service",
            example("cli-deployment-v2.json").to_str().unwrap(),
            "--generation",
            "1",
            "--idempotency-key",
            "example-deployment-update",
            "--wait",
            "--wait-timeout-seconds",
            "15",
        ],
    );
    assert!(
        updated.status.success(),
        "{}",
        String::from_utf8_lossy(&updated.stderr)
    );
    assert_eq!(json(&updated)["generation"], 2);
    assert_eq!(json(&updated)["revision"], 2);

    let rolled_back = run(
        address,
        &[
            "rollback",
            "mvp3-demo-service",
            "--revision",
            "1",
            "--generation",
            "2",
            "--idempotency-key",
            "example-deployment-rollback",
            "--wait",
            "--wait-timeout-seconds",
            "15",
        ],
    );
    assert!(
        rolled_back.status.success(),
        "{}",
        String::from_utf8_lossy(&rolled_back.stderr)
    );
    assert_eq!(json(&rolled_back)["generation"], 3);
    assert_eq!(json(&rolled_back)["revision"], 1);
    assert_eq!(json(&rolled_back)["desiredReplicas"], 2);

    let scaled = run(
        address,
        &[
            "scale",
            "mvp3-demo-service",
            "--replicas",
            "1",
            "--generation",
            "3",
            "--idempotency-key",
            "example-deployment-scale",
            "--wait",
            "--wait-timeout-seconds",
            "15",
        ],
    );
    assert!(
        scaled.status.success(),
        "{}",
        String::from_utf8_lossy(&scaled.stderr)
    );
    assert_eq!(json(&scaled)["generation"], 4);
    assert_eq!(json(&scaled)["desiredReplicas"], 1);
}

#[test]
fn deployment_commands_preserve_generations_and_support_all_lifecycle_operations() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "all");
    create_service_revisions(&root, address, 2);

    let input = apply_input(&root, 1, 0);
    let applied = run(
        address,
        &[
            "apply",
            "deployment-cli-service",
            input.to_str().unwrap(),
            "--idempotency-key",
            "apply-one",
            "--wait",
            "--wait-timeout-seconds",
            "5",
        ],
    );
    assert!(
        applied.status.success(),
        "{}",
        String::from_utf8_lossy(&applied.stderr)
    );
    let mut current = json(&applied);
    assert_eq!(current["status"], "ready");
    assert_eq!(current["rolloutComplete"], true);
    assert_eq!(current["generation"], 1);
    assert_eq!(current["etag"], "\"1\"");

    let retry = run(
        address,
        &[
            "apply",
            "deployment-cli-service",
            input.to_str().unwrap(),
            "--idempotency-key",
            "apply-one",
        ],
    );
    assert!(
        retry.status.success(),
        "{}",
        String::from_utf8_lossy(&retry.stderr)
    );
    assert_eq!(json(&retry)["generation"], 1);

    let inspect = run(address, &["inspect", "deployment-cli-service"]);
    assert!(
        inspect.status.success(),
        "{}",
        String::from_utf8_lossy(&inspect.stderr)
    );
    assert_eq!(json(&inspect)["generation"], 1);

    let update_input = apply_input(&root, 2, 0);
    let updated = run(
        address,
        &[
            "apply",
            "deployment-cli-service",
            update_input.to_str().unwrap(),
            "--generation",
            "1",
            "--idempotency-key",
            "apply-two",
            "--wait",
            "--wait-timeout-seconds",
            "5",
        ],
    );
    assert!(
        updated.status.success(),
        "{}",
        String::from_utf8_lossy(&updated.stderr)
    );
    current = json(&updated);
    assert_eq!(current["revision"], 2);
    assert_eq!(current["generation"], 2);

    let stale = run(
        address,
        &[
            "scale",
            "deployment-cli-service",
            "--replicas",
            "0",
            "--generation",
            "1",
            "--idempotency-key",
            "stale-scale",
        ],
    );
    assert!(!stale.status.success());
    assert_eq!(json_stderr(&stale)["error"]["code"], "precondition_failed");
    let generation = generation_arg(&current);
    let scaled = run(
        address,
        &[
            "scale",
            "deployment-cli-service",
            "--replicas",
            "0",
            "--generation",
            &generation,
            "--idempotency-key",
            "scale-zero",
            "--wait",
            "--wait-timeout-seconds",
            "5",
        ],
    );
    assert!(
        scaled.status.success(),
        "{}",
        String::from_utf8_lossy(&scaled.stderr)
    );
    current = json(&scaled);

    let generation = generation_arg(&current);
    let suspended = run(
        address,
        &[
            "suspend",
            "deployment-cli-service",
            "--generation",
            &generation,
            "--idempotency-key",
            "suspend-one",
            "--wait",
            "--wait-timeout-seconds",
            "5",
        ],
    );
    assert!(
        suspended.status.success(),
        "{}",
        String::from_utf8_lossy(&suspended.stderr)
    );
    current = json(&suspended);
    assert_eq!(current["status"], "suspended");

    let generation = generation_arg(&current);
    let resumed = run(
        address,
        &[
            "resume",
            "deployment-cli-service",
            "--generation",
            &generation,
            "--idempotency-key",
            "resume-one",
            "--wait",
            "--wait-timeout-seconds",
            "5",
        ],
    );
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stderr)
    );
    current = json(&resumed);
    assert_eq!(current["status"], "ready");

    let generation = generation_arg(&current);
    let rolled_back = run(
        address,
        &[
            "rollback",
            "deployment-cli-service",
            "--revision",
            "1",
            "--generation",
            &generation,
            "--idempotency-key",
            "rollback-one",
            "--wait",
            "--wait-timeout-seconds",
            "5",
        ],
    );
    assert!(
        rolled_back.status.success(),
        "{}",
        String::from_utf8_lossy(&rolled_back.stderr)
    );
    current = json(&rolled_back);
    assert_eq!(current["revision"], 1);

    let generation = generation_arg(&current);
    let deleted = run(
        address,
        &[
            "delete",
            "deployment-cli-service",
            "--generation",
            &generation,
            "--idempotency-key",
            "delete-one",
            "--wait",
            "--wait-timeout-seconds",
            "5",
        ],
    );
    assert!(
        deleted.status.success(),
        "{}",
        String::from_utf8_lossy(&deleted.stderr)
    );
    assert_eq!(json(&deleted)["status"], "deleted");
}

#[test]
fn invalid_or_unknown_deployment_input_is_rejected_before_mutation() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "all");
    let input = root.join("invalid-deployment.json");
    fs::write(&input, br#"{"revision":1,"replicas":0,"unexpected":true}"#)
        .expect("write invalid deployment input");
    let result = run(
        address,
        &[
            "apply",
            "not-registered",
            input.to_str().unwrap(),
            "--idempotency-key",
            "invalid-input",
        ],
    );
    assert!(!result.status.success());
    let error = json_stderr(&result)["error"].clone();
    assert_eq!(error["code"], "invalid_deployment");
    assert!(error["message"].as_str().unwrap().contains("unknown field"));
}

#[test]
fn rollout_timeout_preserves_deployment_identity_and_does_not_cancel_remote_desired_state() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "manager");
    create_service_revisions(&root, address, 1);
    let input = apply_input(&root, 1, 1);
    let timed_out = run(
        address,
        &[
            "apply",
            "deployment-cli-service",
            input.to_str().unwrap(),
            "--idempotency-key",
            "pending-apply",
            "--wait",
            "--wait-timeout-seconds",
            "1",
        ],
    );
    assert!(!timed_out.status.success());
    let error = json_stderr(&timed_out)["error"].clone();
    assert_eq!(error["code"], "rollout_timeout");
    assert_eq!(error["partial"]["jobId"], "deployment-cli-service");
    assert_eq!(error["partial"]["generation"], 1);
    assert_eq!(error["partial"]["remoteMutationCancelled"], false);

    let inspected = run(address, &["inspect", "deployment-cli-service"]);
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    let deployment = json(&inspected);
    assert_eq!(deployment["generation"], 1);
    assert_eq!(deployment["desiredReplicas"], 1);
    assert_eq!(deployment["status"], "pending");

    let generation = generation_arg(&deployment);
    let waiting = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["deployment", "--server"])
        .arg(format!("http://{address}"))
        .args([
            "--output",
            "json",
            "scale",
            "deployment-cli-service",
            "--replicas",
            "2",
            "--generation",
            &generation,
            "--idempotency-key",
            "interrupted-scale",
            "--wait",
            "--wait-timeout-seconds",
            "20",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start interruptible deployment wait");
    let wait_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let observed = run(address, &["inspect", "deployment-cli-service"]);
        if observed.status.success() && json(&observed)["desiredReplicas"] == 2 {
            break;
        }
        assert!(
            Instant::now() < wait_deadline,
            "interruptible scale command did not commit its desired replica count"
        );
        thread::sleep(Duration::from_millis(50));
    }
    // Allow the CLI to enter its bounded wait after the manager has accepted
    // the mutation before delivering SIGINT.
    thread::sleep(Duration::from_millis(100));
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(waiting.id() as i32),
        nix::sys::signal::Signal::SIGINT,
    )
    .expect("interrupt local deployment wait");
    let interrupted = waiting
        .wait_with_output()
        .expect("wait for interrupted CLI");
    assert!(interrupted.status.code().is_some());
    assert!(!interrupted.status.success());
    let error = json_stderr(&interrupted)["error"].clone();
    assert_eq!(error["code"], "rollout_interrupted");
    assert_eq!(error["partial"]["generation"], 2);
    assert_eq!(error["partial"]["remoteMutationCancelled"], false);
    let inspected = run(address, &["inspect", "deployment-cli-service"]);
    assert!(
        inspected.status.success(),
        "{}",
        String::from_utf8_lossy(&inspected.stderr)
    );
    assert_eq!(json(&inspected)["desiredReplicas"], 2);
}

#[test]
fn failed_rollout_returns_terminal_progress_reason_and_deployment_identity() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "all");
    create_service_revisions_with_command(&root, address, 1, &["/bin/false"]);
    let input = apply_input(&root, 1, 1);
    let failed = run(
        address,
        &[
            "apply",
            "deployment-cli-service",
            input.to_str().unwrap(),
            "--idempotency-key",
            "failed-apply",
            "--wait",
            "--wait-timeout-seconds",
            "30",
        ],
    );
    assert!(!failed.status.success());
    let error = json_stderr(&failed)["error"].clone();
    assert_eq!(error["code"], "rollout_failed");
    assert_eq!(error["partial"]["jobId"], "deployment-cli-service");
    assert_eq!(error["partial"]["generation"], 1);
    assert_eq!(error["partial"]["deployment"]["status"], "pending");
}

fn json_stderr(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stderr).unwrap_or_else(|_| {
        panic!(
            "stderr was not JSON: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}
