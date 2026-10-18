use std::fs;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{Json, Router, extract::State, http::StatusCode, routing::get};

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
            "conductor-service-cli-{}-{nonce}",
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
        config["worker_id"] = serde_json::json!("service-cli-worker");
        config["worker_concurrency"] = serde_json::json!(2);
        config["worker_service_network"] = serde_json::json!({
            "networkDomainId":"service-cli-network",
            "portRangeStart":31000,
            "portRangeEnd":31010,
            "advertisedAddress":"192.0.2.10"
        });
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

fn command(address: SocketAddr, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(args)
        .args(["--server", &format!("http://{address}")])
        .output()
        .expect("run conductor CLI")
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

struct MockDiscoveryServer {
    address: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockDiscoveryServer {
    fn start() -> Self {
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let watch_count = Arc::new(AtomicUsize::new(0));
        let thread = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build mock discovery runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind mock discovery API");
                address_tx
                    .send(listener.local_addr().expect("read API address"))
                    .expect("send API address");
                let router = Router::new()
                    .route(
                        "/v1/jobs/mock-job/endpoints",
                        get(|| async { Json(mock_endpoints()) }),
                    )
                    .route(
                        "/v1/services/default/web/endpoints",
                        get(|| async { Json(mock_endpoints()) }),
                    )
                    .route(
                        "/v1/services/default/web/watch",
                        get(mock_watch).with_state(watch_count),
                    );
                axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .expect("serve mock discovery API");
            });
        });
        Self {
            address: address_rx.recv().expect("receive API address"),
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        }
    }
}

impl Drop for MockDiscoveryServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn mock_endpoints() -> serde_json::Value {
    serde_json::json!({
        "endpoints":[{
            "jobId":"mock-job",
            "namespace":"default",
            "serviceName":"web",
            "serviceInstanceId":"service-instance-1",
            "allocationId":"allocation-1",
            "runId":"run-1",
            "workerId":"worker-1",
            "name":"http",
            "protocol":"tcp",
            "advertisedAddress":"192.0.2.10",
            "port":31000,
            "ready":true,
            "workerFresh":true,
            "workerDraining":false
        }]
    })
}

async fn mock_watch(
    State(calls): State<Arc<AtomicUsize>>,
) -> (StatusCode, Json<serde_json::Value>) {
    if calls.fetch_add(1, Ordering::Relaxed) == 0 {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error":{"code":"temporarily_unavailable","message":"retry"}})),
        )
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({"endpoints":[],"changed":true})),
        )
    }
}

#[test]
fn endpoint_lookup_and_watch_follow_service_catalog_changes() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "all");

    let missing_job = command(
        address,
        &["service", "--output", "json", "endpoints", "missing-job"],
    );
    assert!(missing_job.status.success());
    assert_eq!(json(&missing_job)["count"], 0);
    let missing_service = command(
        address,
        &[
            "service", "--output", "json", "lookup", "default", "missing",
        ],
    );
    assert!(missing_service.status.success());
    assert_eq!(json(&missing_service)["count"], 0);

    let invalid = command(address, &["service", "lookup", "unsupported", "name"]);
    assert!(!invalid.status.success());
    assert_eq!(
        json_stderr(&invalid)["error"]["code"],
        "invalid_service_identity"
    );

    let timed_out = command(
        address,
        &[
            "service",
            "--output",
            "json",
            "watch",
            "default",
            "missing",
            "--timeout-seconds",
            "1",
        ],
    );
    assert!(!timed_out.status.success());
    assert_eq!(json_stderr(&timed_out)["error"]["code"], "watch_timeout");
    assert_eq!(
        json_stderr(&timed_out)["error"]["partial"]["cursorSupported"],
        false
    );
}

#[test]
fn endpoint_lookup_and_watch_decode_contracts_and_retry_transient_errors() {
    let _guard = lock_tests();
    let server = MockDiscoveryServer::start();

    let by_job = command(
        server.address,
        &["service", "--output", "json", "endpoints", "mock-job"],
    );
    assert!(
        by_job.status.success(),
        "{}",
        String::from_utf8_lossy(&by_job.stderr)
    );
    assert_eq!(json(&by_job)["count"], 1);
    assert_eq!(json(&by_job)["endpoints"][0]["name"], "http");
    assert_eq!(json(&by_job)["endpoints"][0]["ready"], true);

    let by_name = command(
        server.address,
        &["service", "--output", "json", "lookup", "default", "web"],
    );
    assert!(by_name.status.success());
    assert_eq!(json(&by_name)["count"], 1);

    let changed = command(
        server.address,
        &[
            "service",
            "--output",
            "json",
            "watch",
            "default",
            "web",
            "--timeout-seconds",
            "5",
        ],
    );
    assert!(
        changed.status.success(),
        "{}",
        String::from_utf8_lossy(&changed.stderr)
    );
    assert_eq!(json(&changed)["changed"], true);
    assert_eq!(json(&changed)["endpoints"], serde_json::json!([]));

    assert_eq!(json(&changed)["cursorSupported"], false);
    assert_eq!(json(&changed)["versionSupported"], false);
}

#[test]
fn diagnostics_separate_health_readiness_and_metrics() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let (_agent, address) = start_agent(&root, "all");

    let health = command(address, &["diagnostics", "--output", "json", "health"]);
    assert!(
        health.status.success(),
        "{}",
        String::from_utf8_lossy(&health.stderr)
    );
    assert_eq!(json(&health)["reachable"], true);
    assert_eq!(json(&health)["healthy"], true);
    assert_eq!(json(&health)["status"], "ok");

    let readiness = command(address, &["diagnostics", "--output", "json", "ready"]);
    assert!(
        readiness.status.success(),
        "{}",
        String::from_utf8_lossy(&readiness.stderr)
    );
    assert_eq!(json(&readiness)["ready"], true);

    let metrics = command(address, &["diagnostics", "metrics"]);
    assert!(metrics.status.success());
    let body = String::from_utf8(metrics.stdout).expect("metrics UTF-8");
    assert!(body.starts_with("# HELP conductor_deployments"));
    assert!(!body.starts_with('{'));

    let metrics_json = command(address, &["diagnostics", "--output", "json", "metrics"]);
    assert!(metrics_json.status.success());
    assert!(
        json(&metrics_json)["metrics"]
            .as_str()
            .unwrap()
            .contains("conductor_deployments")
    );

    let manager_root = TempDir::new();
    let (_manager, manager_address) = start_agent(&manager_root, "manager");
    let not_ready = command(
        manager_address,
        &["diagnostics", "--output", "json", "ready"],
    );
    assert!(!not_ready.status.success());
    assert_eq!(
        json_stderr(&not_ready)["error"]["code"],
        "manager_not_ready"
    );
    assert_eq!(
        json_stderr(&not_ready)["error"]["partial"]["reachable"],
        true
    );
}
