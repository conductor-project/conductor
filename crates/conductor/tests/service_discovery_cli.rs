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
    start_agent_with_api_access(root, role, serde_json::json!({"mode":"local"}))
}

fn start_agent_with_api_access(
    root: &Path,
    role: &str,
    api_access: serde_json::Value,
) -> (Agent, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral API port");
    let address = listener.local_addr().expect("read API address");
    drop(listener);
    let config_path = root.join("agent.json");
    let mut config = serde_json::json!({
        "role": role,
        "listen": address.to_string(),
        "manager_data_dir": root.join("manager"),
        "api_access": api_access,
    });
    if role == "all" {
        config["worker_data_dir"] = serde_json::json!(root.join("worker"));
        config["worker_id"] = serde_json::json!("service-cli-worker");
        config["worker_concurrency"] = serde_json::json!(2);
        config["worker_labels"] = serde_json::json!({
            "diagnostics_test_label": "do-not-export-this-secret"
        });
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

struct MockDiagnosticsServer {
    address: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl MockDiagnosticsServer {
    fn start(workers: Vec<serde_json::Value>, metrics: String, worker_delay: Duration) -> Self {
        let (address_tx, address_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let thread = thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build mock diagnostics runtime");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind mock diagnostics API");
                address_tx
                    .send(listener.local_addr().expect("read mock API address"))
                    .expect("send mock API address");
                let router = Router::new()
                    .route(
                        "/healthz",
                        get(|| async {
                            Json(serde_json::json!({
                                "status":"ok",
                                "role":"manager",
                                "capabilities":["jobs-v1","diagnostics-v1"]
                            }))
                        }),
                    )
                    .route(
                        "/readyz",
                        get(|| async {
                            (
                                StatusCode::SERVICE_UNAVAILABLE,
                                Json(serde_json::json!({
                                    "status":"degraded",
                                    "role":"manager",
                                    "capabilities":["jobs-v1"]
                                })),
                            )
                        }),
                    )
                    .route(
                        "/v1/workers",
                        get(move || {
                            let workers = workers.clone();
                            async move {
                                tokio::time::sleep(worker_delay).await;
                                Json(workers)
                            }
                        }),
                    )
                    .route("/v1/metrics", get(move || async move { metrics.clone() }));
                axum::serve(listener, router)
                    .with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                    })
                    .await
                    .expect("serve mock diagnostics API");
            });
        });
        Self {
            address: address_rx.recv().expect("receive mock diagnostics address"),
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        }
    }
}

impl Drop for MockDiagnosticsServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn mock_worker_status(worker_id: &str, overrides: serde_json::Value) -> serde_json::Value {
    let mut worker = serde_json::json!({
        "workerId":worker_id,
        "incarnation":1,
        "processExecution":true,
        "capacity":1,
        "usedSlots":1,
        "liveProcesses":1,
        "unknownRuns":1,
        "pendingPersistence":1,
        "availableSlots":0,
        "available":false,
        "degraded":false,
        "draining":false,
        "observedAtMs":1000,
        "inventory":null,
        "resourceDomainId":null,
        "resourceDomainCapacity":null,
        "labels":{"credential":"planted-private-label"},
        "fresh":false,
        "lastSeenAtMs":1000,
        "serviceNetwork":null
    });
    if let (Some(target), Some(source)) = (worker.as_object_mut(), overrides.as_object()) {
        target.extend(source.clone());
    }
    worker
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

    let export = command(
        address,
        &[
            "diagnostics",
            "--output",
            "json",
            "export",
            "--deadline-seconds",
            "3",
        ],
    );
    assert!(
        export.status.success(),
        "{}",
        String::from_utf8_lossy(&export.stderr)
    );
    let export_json = json(&export);
    assert_eq!(export_json["schemaVersion"], 1);
    assert_eq!(export_json["versions"]["binary"], env!("CARGO_PKG_VERSION"));
    assert_eq!(export_json["versions"]["httpApi"], 1);
    assert_eq!(export_json["collectionStatus"], "complete");
    assert_eq!(export_json["sections"]["health"]["status"], "collected");
    assert_eq!(export_json["sections"]["workers"]["status"], "collected");
    assert_eq!(
        export_json["sections"]["storage"]["data"]["remoteCollection"],
        "not_supported"
    );
    let export_text = String::from_utf8(export.stdout).expect("diagnostics export UTF-8");
    assert!(!export_text.contains("do-not-export-this-secret"));
    assert!(!export_text.contains("diagnostics_test_label"));

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

#[test]
fn diagnostics_export_returns_explicit_partial_result_when_manager_is_unreachable() {
    let _guard = lock_tests();
    let output = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args([
            "diagnostics",
            "--server",
            "http://127.0.0.1:1",
            "--output",
            "json",
            "export",
            "--deadline-seconds",
            "1",
        ])
        .output()
        .expect("run partial diagnostics export");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = json(&output);
    assert_eq!(report["collectionStatus"], "partial");
    for section in ["health", "readiness", "workers", "metrics"] {
        assert_eq!(report["sections"][section]["status"], "unavailable");
        assert!(report["sections"][section]["errorCode"].is_string());
    }
    assert!(
        report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|finding| { finding["code"] == "diagnostics_partial" })
    );
}

#[test]
fn diagnostics_export_uses_operator_authorization_for_protected_observations() {
    let _guard = lock_tests();
    let root = TempDir::new();
    let token_path = root.join("operator-token");
    let token_output = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["create-operator-token", "--output"])
        .arg(&token_path)
        .output()
        .expect("create operator token fixture");
    assert!(token_output.status.success());
    let token = fs::read_to_string(&token_path)
        .expect("read operator token fixture")
        .trim()
        .to_owned();
    let (_agent, address) = start_agent_with_api_access(
        &root,
        "all",
        serde_json::json!({
            "mode":"operator_token_file",
            "token_file":token_path
        }),
    );

    let unauthorized = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args([
            "diagnostics",
            "--server",
            &format!("http://{address}"),
            "--output",
            "json",
            "export",
            "--deadline-seconds",
            "2",
        ])
        .output()
        .expect("run unauthenticated diagnostics export");
    assert!(unauthorized.status.success());
    let denied = json(&unauthorized);
    assert_eq!(denied["collectionStatus"], "partial");
    assert_eq!(denied["sections"]["workers"]["status"], "unavailable");
    assert_eq!(
        denied["sections"]["workers"]["errorCode"],
        "operator_authentication_required"
    );

    let authorized = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args([
            "diagnostics",
            "--server",
            &format!("http://{address}"),
            "--token-file",
        ])
        .arg(&token_path)
        .args(["--output", "json", "export", "--deadline-seconds", "2"])
        .output()
        .expect("run authenticated diagnostics export");
    assert!(
        authorized.status.success(),
        "{}",
        String::from_utf8_lossy(&authorized.stderr)
    );
    let report = json(&authorized);
    assert_eq!(report["sections"]["workers"]["status"], "collected");
    let encoded = String::from_utf8(authorized.stdout).expect("export UTF-8");
    assert!(!encoded.contains(&token));
}

#[test]
fn diagnostics_export_explains_worker_and_placement_blockers_without_exporting_labels() {
    let _guard = lock_tests();
    let metrics = concat!(
        "conductor_pending_work{kind=\"allocations\"} 2\n",
        "conductor_pending_work{kind=\"deployment\",reason=\"no_eligible_worker\"} 1\n",
        "conductor_pending_work{kind=\"deployment\",reason=\"worker_capacity_exhausted\"} 1\n",
    );
    let server = MockDiagnosticsServer::start(
        vec![
            mock_worker_status("stale-worker", serde_json::json!({})),
            mock_worker_status(
                "unknown-worker",
                serde_json::json!({
                    "unknownRuns":0,
                    "pendingPersistence":0,
                    "availableSlots":1,
                    "usedSlots":0,
                    "liveProcesses":0,
                    "available":true,
                    "lastSeenAtMs":null
                }),
            ),
        ],
        metrics.to_owned(),
        Duration::ZERO,
    );
    let output = command(
        server.address,
        &[
            "diagnostics",
            "--output",
            "json",
            "export",
            "--deadline-seconds",
            "2",
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = json(&output);
    assert_eq!(report["collectionStatus"], "complete");
    assert_eq!(report["sections"]["readiness"]["data"]["ready"], false);
    let worker = &report["sections"]["workers"]["data"]["workers"][0];
    assert_eq!(worker["freshness"], "stale");
    assert!(worker["observationAgeMs"].as_u64().unwrap() > 0);
    assert_eq!(worker["availableSlots"], 0);
    assert_eq!(worker["unknownRuns"], 1);
    assert_eq!(worker["pendingPersistence"], 1);
    assert_eq!(
        report["sections"]["workers"]["data"]["workers"][1]["freshness"],
        "unknown"
    );
    let findings = report["findings"].as_array().expect("diagnostic findings");
    for expected in [
        "manager_not_ready",
        "worker_observation_stale",
        "worker_no_available_slots",
        "worker_unknown_runs",
        "worker_pending_persistence",
        "placement_pending",
        "deployment_no_eligible_worker",
        "deployment_worker_capacity_exhausted",
    ] {
        assert!(
            findings.iter().any(|finding| finding["code"] == expected),
            "missing finding {}: {:?}",
            expected,
            findings
        );
    }
    let encoded = String::from_utf8(output.stdout).expect("export UTF-8");
    assert!(!encoded.contains("credential"));
    assert!(!encoded.contains("planted-private-label"));

    let human = command(
        server.address,
        &["diagnostics", "export", "--deadline-seconds", "2"],
    );
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout).expect("human diagnostics UTF-8");
    assert!(human.contains("readiness: collected (degraded)"));
    assert!(human.contains("workers: collected"));
    assert!(human.contains("storage: unavailable"));
    assert!(human.contains("The worker reports unresolved runs"));
}

#[test]
fn diagnostics_export_caps_items_and_returns_before_slow_sections_exceed_deadline() {
    let _guard = lock_tests();
    let workers = (0..130)
        .map(|index| {
            mock_worker_status(
                &format!("worker-{index}"),
                serde_json::json!({"unknownRuns":0,"pendingPersistence":0}),
            )
        })
        .collect();
    let server = MockDiagnosticsServer::start(workers, String::new(), Duration::from_secs(3));
    let started = Instant::now();
    let output = command(
        server.address,
        &[
            "diagnostics",
            "--output",
            "json",
            "export",
            "--deadline-seconds",
            "1",
        ],
    );
    let elapsed = started.elapsed();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = json(&output);
    assert_eq!(report["collectionStatus"], "partial");
    assert_eq!(report["sections"]["workers"]["status"], "unavailable");
    assert_eq!(
        report["sections"]["workers"]["errorCode"],
        "deadline_exceeded"
    );
    assert!(
        elapsed < Duration::from_millis(2_500),
        "collection took {:?}",
        elapsed
    );

    let fast = MockDiagnosticsServer::start(
        (0..130)
            .map(|index| mock_worker_status(&format!("worker-{index}"), serde_json::json!({})))
            .collect(),
        String::new(),
        Duration::ZERO,
    );
    let output = command(
        fast.address,
        &[
            "diagnostics",
            "--output",
            "json",
            "export",
            "--deadline-seconds",
            "2",
        ],
    );
    assert!(output.status.success());
    let report = json(&output);
    assert_eq!(
        report["sections"]["workers"]["data"]["workers"]
            .as_array()
            .unwrap()
            .len(),
        128
    );
    assert_eq!(report["sections"]["workers"]["data"]["omittedWorkers"], 2);
}
