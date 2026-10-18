use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::routing::get;

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
            "conductor-worker-cli-{}-{nonce}",
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

struct EmptyInventoryServer {
    address: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl EmptyInventoryServer {
    fn start() -> Self {
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let thread = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build empty-inventory API runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind empty-inventory API");
                address_tx
                    .send(listener.local_addr().expect("read API address"))
                    .expect("send API address");
                axum::serve(
                    listener,
                    Router::new().route(
                        "/v1/workers",
                        get(|| async { axum::Json(serde_json::json!([])) }),
                    ),
                )
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("serve empty-inventory API");
            });
        });
        Self {
            address: address_rx.recv().expect("receive API address"),
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        }
    }
}

impl Drop for EmptyInventoryServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn start_agent(root: &Path, role: &str) -> (Agent, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral API port");
    let address = listener.local_addr().expect("read API address");
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
        config["worker_id"] = serde_json::json!("worker-cli-test");
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
        serde_json::to_vec(&config).expect("encode agent config"),
    )
    .expect("write agent config");
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
        .args(["worker", "--server"])
        .arg(format!("http://{address}"))
        .args(["--output", "json"])
        .args(args)
        .output()
        .expect("run worker CLI")
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

fn json_stderr(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stderr).unwrap_or_else(|_| {
        panic!(
            "stderr was not JSON: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn worker_list_handles_empty_and_populated_inventory_and_drain_lifecycle() {
    let _guard = lock_tests();
    let empty_server = EmptyInventoryServer::start();
    let empty = run(empty_server.address, &["list"]);
    assert!(empty.status.success());
    assert_eq!(json(&empty)["count"], 0);
    assert_eq!(json(&empty)["workers"], serde_json::json!([]));

    let disconnected_root = TempDir::new();
    let (_manager, disconnected_address) = start_agent(&disconnected_root, "manager");
    let disconnected = run(disconnected_address, &["list"]);
    assert!(disconnected.status.success());
    assert_eq!(
        json(&disconnected)["workers"][0]["workerId"],
        "disconnected-worker"
    );
    assert_eq!(json(&disconnected)["workers"][0]["available"], false);
    assert_eq!(json(&disconnected)["workers"][0]["fresh"], false);

    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "all");
    let listed = run(address, &["list"]);
    assert!(
        listed.status.success(),
        "{}",
        String::from_utf8_lossy(&listed.stderr)
    );
    assert_eq!(json(&listed)["count"], 1);
    assert_eq!(json(&listed)["workers"][0]["workerId"], "worker-cli-test");

    let inspected = run(address, &["inspect", "worker-cli-test"]);
    assert!(inspected.status.success());
    assert_eq!(json(&inspected)["capacity"], 2);
    assert_eq!(json(&inspected)["availableSlots"], 2);

    let missing = run(address, &["inspect", "missing-worker"]);
    assert!(!missing.status.success());
    assert_eq!(json_stderr(&missing)["error"]["code"], "worker_not_found");

    let drained = run(
        address,
        &[
            "drain",
            "worker-cli-test",
            "--grace-period-seconds",
            "10",
            "--wait",
            "--wait-timeout-seconds",
            "5",
        ],
    );
    assert!(
        drained.status.success(),
        "{}",
        String::from_utf8_lossy(&drained.stderr)
    );
    assert_eq!(json(&drained)["state"], "completed");
    assert_eq!(json(&drained)["drainComplete"], true);

    let still_drained = run(address, &["inspect", "worker-cli-test"]);
    assert!(still_drained.status.success());
    assert_eq!(json(&still_drained)["draining"], true);
    assert_eq!(json(&still_drained)["available"], false);

    let undrained = run(address, &["undrain", "worker-cli-test"]);
    assert!(
        undrained.status.success(),
        "{}",
        String::from_utf8_lossy(&undrained.stderr)
    );
    assert_eq!(json(&undrained)["state"], "accepting_placements");
    let available = run(address, &["inspect", "worker-cli-test"]);
    assert_eq!(json(&available)["draining"], false);
    assert_eq!(json(&available)["available"], true);
}

#[test]
fn drain_timeout_keeps_accepted_intent_and_a_later_wait_observes_completion() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "all");
    let job_file = root.join("sleep-job.json");
    fs::write(
        &job_file,
        br#"{"id":"worker-drain-sleep","command":["/bin/sleep","30"],"cwd":"/tmp","terminationGraceMs":5000}"#,
    )
    .expect("write sleep job");
    let registered = job(address, &["register", job_file.to_str().unwrap()]);
    assert!(
        registered.status.success(),
        "{}",
        String::from_utf8_lossy(&registered.stderr)
    );
    let started = job(
        address,
        &[
            "submit",
            job_file.to_str().unwrap(),
            "--idempotency-key",
            "worker-drain-run",
        ],
    );
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );

    let timed_out = run(
        address,
        &[
            "drain",
            "worker-cli-test",
            "--grace-period-seconds",
            "30",
            "--wait",
            "--wait-timeout-seconds",
            "1",
        ],
    );
    assert!(!timed_out.status.success());
    let error = json_stderr(&timed_out)["error"].clone();
    assert_eq!(error["code"], "drain_timeout");
    assert_eq!(error["partial"]["desiredStateAccepted"], true);
    assert_eq!(error["partial"]["remoteMutationCancelled"], false);
    assert_eq!(error["partial"]["drain"]["state"], "draining");

    let status = run(address, &["drain-status", "worker-cli-test"]);
    assert!(status.status.success());
    assert_eq!(json(&status)["state"], "draining");

    let run_id = json(&started)["runId"].as_str().expect("run ID").to_owned();
    let stopped = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["run", "--server"])
        .arg(format!("http://{address}"))
        .args(["stop", &run_id])
        .output()
        .expect("stop test run");
    assert!(
        stopped.status.success(),
        "{}",
        String::from_utf8_lossy(&stopped.stderr)
    );
    let run_finished = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["run", "--server"])
        .arg(format!("http://{address}"))
        .args([
            "--output",
            "json",
            "wait",
            &run_id,
            "--timeout-seconds",
            "10",
        ])
        .output()
        .expect("wait for stopped test run");
    assert!(!run_finished.status.success());
    assert_eq!(json_stderr(&run_finished)["error"]["code"], "run_cancelled");
    assert_eq!(
        json_stderr(&run_finished)["error"]["partial"]["run"]["state"],
        "cancelled"
    );
    let waited = run(
        address,
        &["drain-wait", "worker-cli-test", "--timeout-seconds", "5"],
    );
    assert!(
        waited.status.success(),
        "{}",
        String::from_utf8_lossy(&waited.stderr)
    );
    assert_eq!(json(&waited)["state"], "completed");
}
