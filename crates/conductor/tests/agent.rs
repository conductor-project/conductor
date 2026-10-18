use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine;
use rusqlite::Connection;

const ASYNC_TEST_WATCHDOG: Duration = Duration::from_secs(30);

struct TempDir(PathBuf);

impl TempDir {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("conductor-{label}-{}-{nonce}", std::process::id()));
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

struct ProcessGuard(PathBuf);

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        if let Ok(pid) = fs::read_to_string(&self.0)
            && let Ok(pid) = pid.trim().parse::<u32>()
        {
            let _ = Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }
}

fn write_config(path: &Path, role: &str, listen: &str, extra: &str) -> PathBuf {
    write_config_dirs(
        path,
        role,
        listen,
        &path.join("manager"),
        &path.join("worker"),
        extra,
    )
}

fn write_config_dirs(
    path: &Path,
    role: &str,
    listen: &str,
    manager_dir: &Path,
    worker_dir: &Path,
    extra: &str,
) -> PathBuf {
    fs::create_dir_all(path).expect("create config directory");
    let config_path = path.join("agent.json");
    let content = format!(
        r#"{{"role":"{role}","listen":"{listen}","manager_data_dir":"{}","worker_data_dir":"{}","worker_id":"test-worker","worker_concurrency":1,"api_access":{{"mode":"local"}}{extra}}}"#,
        manager_dir.display(),
        worker_dir.display(),
    );
    fs::write(&config_path, content).expect("write config");
    config_path
}

fn command(config: &Path, role: Option<&str>) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_conductor"));
    command.args(["agent", "--config"]).arg(config);
    if let Some(role) = role {
        command.args(["--role", role]);
    }
    command
}

fn operator_command(address: SocketAddr, token_file: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["operator", "--server"])
        .arg(format!("http://{address}"))
        .args(["--token-file"])
        .arg(token_file)
        .args(args)
        .output()
        .expect("run operator CLI")
}

fn create_operator_token(path: &Path) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(["create-operator-token", "--output"])
        .arg(path)
        .output()
        .expect("create operator token");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::read_to_string(path)
        .expect("read operator token")
        .trim()
        .to_owned()
}

fn wait_for_operator_run(address: SocketAddr, token: &str, run_id: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, _, body) = http_with_custom_headers(
            address,
            "GET",
            &format!("/v1/runs/{run_id}"),
            None,
            &[("Authorization", &format!("Bearer {token}"))],
            &[],
        );
        assert_eq!(status, 200, "{body}");
        let observation: serde_json::Value = serde_json::from_str(&body).expect("run observation");
        if observation["state"] == "succeeded" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "run did not succeed: {observation}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn start(path: &Path, config_role: &str, role_override: Option<&str>) -> (Agent, SocketAddr) {
    start_with_capacity(path, config_role, role_override, 1)
}

fn start_with_capacity(
    path: &Path,
    config_role: &str,
    role_override: Option<&str>,
    capacity: usize,
) -> (Agent, SocketAddr) {
    let config = write_config(path, config_role, "127.0.0.1:0", "");
    if capacity != 1 {
        let content = fs::read_to_string(&config)
            .expect("read generated config")
            .replace(
                "\"worker_concurrency\":1",
                &format!("\"worker_concurrency\":{capacity}"),
            );
        fs::write(&config, content).expect("write worker capacity");
    }
    start_config(config, role_override)
}

fn start_with_request_timeout(path: &Path, timeout_ms: u64) -> (Agent, SocketAddr) {
    let config = write_config(
        path,
        "all",
        "127.0.0.1:0",
        &format!(",\"request_timeout_ms\":{timeout_ms}"),
    );
    start_config(config, None)
}

fn start_config(config: PathBuf, role_override: Option<&str>) -> (Agent, SocketAddr) {
    let child = command(&config, role_override)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start agent");
    let mut child = Agent(child);
    let stderr = child.0.stderr.take().expect("stderr");
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = sender.send(line.clone());
        // Keep draining diagnostics after the startup event. Closing this
        // pipe early can make later eprintln! calls panic in the child.
        let _ = std::io::copy(&mut reader, &mut std::io::sink());
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("startup event before readiness deadline");
    let event: serde_json::Value = serde_json::from_str(&line).expect("structured startup event");
    assert_eq!(event["event"], "agent_started", "{line}");
    assert_eq!(event["severity"], "info", "{line}");
    assert_eq!(event["component"], "agent", "{line}");
    assert!(event["timestampMs"].as_u64().is_some(), "{line}");
    let address: SocketAddr = event["address"]
        .as_str()
        .expect("address")
        .parse()
        .expect("socket address");
    wait_for_ready(address);
    (child, address)
}

fn start_without_api_readiness(config: &Path) -> (Agent, serde_json::Value) {
    let child = command(config, None)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start manager agent");
    let mut child = Agent(child);
    let stderr = child.0.stderr.take().expect("stderr");
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = sender.send(line.clone());
        loop {
            line.clear();
            if reader
                .read_line(&mut line)
                .ok()
                .filter(|read| *read > 0)
                .is_none()
            {
                break;
            }
            if line.contains("manager_recovery_retry") {
                eprintln!("{line}");
            }
        }
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("manager startup event before readiness deadline");
    let event: serde_json::Value = serde_json::from_str(&line).expect("startup event JSON");
    assert_eq!(event["event"], "agent_started", "{line}");
    assert_eq!(event["role"], "manager", "{line}");
    (child, event)
}

fn start_worker_process(config: &Path) -> Agent {
    let child = command(config, None)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start worker agent");
    let mut child = Agent(child);
    let stderr = child.0.stderr.take().expect("stderr");
    thread::spawn(move || {
        let _ = std::io::copy(&mut BufReader::new(stderr), &mut std::io::sink());
    });
    child
}

fn wait_for_ready(address: SocketAddr) {
    wait_for_ready_named(address, "agent");
}

fn wait_for_ready_named(address: SocketAddr, description: &str) {
    wait_for_ready_with_timeout(address, description, Duration::from_secs(5));
}

fn wait_for_ready_with_timeout(address: SocketAddr, description: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let mut last_response = String::new();
    loop {
        if let Ok(mut stream) = TcpStream::connect(address) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
            let _ = stream
                .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            let mut response = String::new();
            if stream.read_to_string(&mut response).is_ok() {
                if response.starts_with("HTTP/1.1 200") {
                    return;
                }
                last_response = response;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{description} did not become ready before deadline; last response: {last_response}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_http_status(address: SocketAddr, path: &str, expected: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(mut stream) = TcpStream::connect(address) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(100)));
            let request =
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
            let _ = stream.write_all(request.as_bytes());
            let mut response = String::new();
            if stream.read_to_string(&mut response).is_ok()
                && response.starts_with(&format!("HTTP/1.1 {expected}"))
            {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "HTTP {path} did not return status {expected} before deadline"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_exit(child: &mut Agent) {
    wait_for_exit_with_timeout(child, Duration::from_secs(5));
}

fn wait_for_exit_with_timeout(child: &mut Agent, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.0.try_wait().expect("poll child") {
            assert!(status.success(), "agent exited with {status}");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "agent did not exit before deadline"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn wait_for_failure_exit(child: &mut Agent, timeout: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.0.try_wait().expect("poll child") {
            assert!(
                !status.success(),
                "agent unexpectedly reported clean shutdown"
            );
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "agent did not exit before deadline"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn signal(child: &Agent, name: &str) {
    let status = Command::new("kill")
        .args([format!("-{name}"), child.0.id().to_string()])
        .status()
        .expect("send signal");
    assert!(status.success(), "kill -{name} failed");
}

fn failed(config: &Path, role: Option<&str>) -> Output {
    command(config, role)
        .output()
        .expect("run agent with invalid settings")
}

#[test]
fn actual_binary_serves_health_and_shuts_down_on_sigint() {
    let path = TempDir::new("sigint");
    let (mut child, address) = start_with_capacity(&path, "all", None, 2);
    let mut stream = TcpStream::connect(address).expect("connect to agent");
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    let (_, body) = response.split_once("\r\n\r\n").expect("HTTP body");
    let health: serde_json::Value = serde_json::from_str(body).expect("health response JSON");
    assert_eq!(health["status"], "ok");
    assert_eq!(health["role"], "all");
    assert_eq!(health["capabilities"][0], "local_process_execution");
    let mut run_ids = Vec::new();
    let mut pid_files = Vec::new();
    let mut process_guards = Vec::new();
    for id in ["shutdown-a", "shutdown-b"] {
        let pid_file = path.join(format!("{id}.pid"));
        process_guards.push(ProcessGuard(pid_file.clone()));
        let job = serde_json::json!({
            "id":id,
            "command":["/bin/sh", "-c", format!("echo $$ > {}; exec /bin/sleep 60", pid_file.display())],
            "cwd":"/tmp",
            "terminationGraceMs":200,
        });
        assert_eq!(
            http(
                address,
                "POST",
                "/v1/jobs",
                Some("application/json"),
                &serde_json::to_vec(&job).unwrap()
            )
            .0,
            201
        );
        let (status, accepted) = http(address, "POST", &format!("/v1/jobs/{id}/runs"), None, &[]);
        assert_eq!(status, 202, "{accepted}");
        let accepted: serde_json::Value = serde_json::from_str(&accepted).unwrap();
        run_ids.push(accepted["runId"].as_str().unwrap().to_owned());
        pid_files.push(pid_file);
    }
    for pid_file in &pid_files {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !pid_file.exists() {
            assert!(Instant::now() < deadline, "workload did not start");
            thread::sleep(Duration::from_millis(10));
        }
    }
    signal(&child, "INT");
    wait_for_exit(&mut child);
    let (mut restarted, address) = start_with_capacity(&path, "all", None, 2);
    for run_id in run_ids {
        let (status, result) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
        assert_eq!(status, 200, "{result}");
        let result: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(result["state"], "cancelled");
        assert_eq!(result["terminationReason"], "shutdown");
    }
    signal(&restarted, "INT");
    wait_for_exit(&mut restarted);
    assert!(
        TcpStream::connect(address).is_err(),
        "listener remained bound"
    );
}

#[test]
fn actual_binary_runs_service_dns_until_graceful_shutdown() {
    let directory = TempDir::new("service-dns-agent");
    let config = write_config(
        &directory,
        "all",
        "127.0.0.1:0",
        r#", "service_dns":{"listen":"127.0.0.1:0"}"#,
    );
    let (mut agent, address) = start_config(config, None);
    let (status, _) = http(address, "GET", "/healthz", None, &[]);
    assert_eq!(status, 200);
    assert!(agent.0.try_wait().expect("check daemon status").is_none());
    signal(&agent, "INT");
    wait_for_exit(&mut agent);
}

#[test]
fn worker_diagnostics_report_capacity_identity_and_restart_incarnation() {
    let path = TempDir::new("worker-status");
    let (mut child, address) = start_with_capacity(&path, "all", None, 2);
    assert_eq!(http(address, "GET", "/healthz", None, &[]).0, 200);
    assert_eq!(http(address, "GET", "/readyz", None, &[]).0, 200);
    let (status, body) = http(address, "GET", "/v1/workers", None, &[]);
    assert_eq!(status, 200, "{body}");
    let workers: serde_json::Value = serde_json::from_str(&body).unwrap();
    let initial = &workers[0];
    assert_eq!(initial["workerId"], "test-worker");
    assert_eq!(initial["capacity"], 2);
    assert_eq!(initial["processExecution"], true);
    assert_eq!(initial["usedSlots"], 0);
    assert_eq!(initial["availableSlots"], 2);
    assert_eq!(initial["unknownRuns"], 0);
    assert_eq!(initial["available"], true);
    assert_eq!(initial["draining"], false);
    assert!(initial["observedAtMs"].as_i64().unwrap() > 0);
    let first_incarnation = initial["incarnation"].as_u64().unwrap();
    let (status, single) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
    assert_eq!(status, 200, "{single}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&single).unwrap()["incarnation"],
        first_incarnation
    );
    assert_eq!(
        http(address, "GET", "/v1/workers/missing", None, &[]).0,
        404
    );

    let job = job_json("status-probe", &["/bin/sh", "-c", "exec /bin/sleep 60"]);
    assert_eq!(
        http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/status-probe/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let run_id = serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    let second_job = job_json(
        "status-probe-second",
        &["/bin/sh", "-c", "exec /bin/sleep 60"],
    );
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &second_job
        )
        .0,
        201
    );
    let (status, second_accepted) = http(
        address,
        "POST",
        "/v1/jobs/status-probe-second/runs",
        None,
        &[],
    );
    assert_eq!(status, 202, "{second_accepted}");
    let second_run_id =
        serde_json::from_str::<serde_json::Value>(&second_accepted).unwrap()["runId"]
            .as_str()
            .unwrap()
            .to_owned();
    let deadline = Instant::now() + Duration::from_secs(2);
    let active = loop {
        let (status, body) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
        assert_eq!(status, 200, "{body}");
        let status: serde_json::Value = serde_json::from_str(&body).unwrap();
        if status["usedSlots"] == 2 {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "worker slot use was not reported: {status}"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(active["availableSlots"], 0);
    assert_eq!(active["incarnation"], first_incarnation);
    assert_eq!(http(address, "GET", "/readyz", None, &[]).0, 200);
    assert_eq!(
        http(
            address,
            "POST",
            &format!("/v1/runs/{run_id}/stop"),
            None,
            &[]
        )
        .0,
        202
    );
    wait_for_run_state(address, &run_id, &["cancelled"], Duration::from_secs(3));
    assert_eq!(
        http(
            address,
            "POST",
            &format!("/v1/runs/{second_run_id}/stop"),
            None,
            &[]
        )
        .0,
        202
    );
    wait_for_run_state(
        address,
        &second_run_id,
        &["cancelled"],
        Duration::from_secs(3),
    );
    let (status, body) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["usedSlots"],
        0
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);

    let (mut restarted, address) = start(&path, "all", None);
    let (status, body) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
    assert_eq!(status, 200, "{body}");
    let next = serde_json::from_str::<serde_json::Value>(&body).unwrap();
    assert!(next["incarnation"].as_u64().unwrap() > first_incarnation);
    signal(&restarted, "INT");
    wait_for_exit(&mut restarted);
}

#[test]
fn actual_binary_drains_worker_runs_with_deadline_and_allows_reuse_after_clear() {
    let path = TempDir::new("worker-drain");
    let (mut child, address) = start_with_capacity(&path, "all", None, 1);
    let job = job_json("drain-process", &["/bin/sh", "-c", "exec /bin/sleep 60"]);
    assert_eq!(
        http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/drain-process/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let run_id = serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();

    let (status, body) = http(
        address,
        "POST",
        "/v1/workers/test-worker/drain",
        Some("application/json"),
        br#"{"gracePeriodSeconds":1}"#,
    );
    assert_eq!(status, 202, "{body}");
    let draining: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(draining["state"], "draining");
    assert_eq!(draining["activeRuns"], 1);
    assert_eq!(
        http(address, "POST", "/v1/jobs/drain-process/runs", None, &[]).0,
        503,
        "new work is rejected while the manager-owned drain is active"
    );

    wait_for_run_state(address, &run_id, &["cancelled"], Duration::from_secs(5));
    let deadline = Instant::now() + Duration::from_secs(2);
    let completed = loop {
        let (status, body) = http(address, "GET", "/v1/workers/test-worker/drain", None, &[]);
        assert_eq!(status, 200, "{body}");
        let progress: serde_json::Value = serde_json::from_str(&body).unwrap();
        if progress["state"] == "completed" {
            break progress;
        }
        assert!(
            Instant::now() < deadline,
            "worker drain did not complete: {progress}"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(completed["activeRuns"], 0);
    let (status, worker) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
    assert_eq!(status, 200, "{worker}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&worker).unwrap()["draining"],
        true
    );

    assert_eq!(
        http(
            address,
            "DELETE",
            "/v1/workers/test-worker/drain",
            None,
            &[]
        )
        .0,
        204
    );
    let (status, worker) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
    assert_eq!(status, 200, "{worker}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&worker).unwrap()["draining"],
        false
    );

    let (status, accepted) = http(address, "POST", "/v1/jobs/drain-process/runs", None, &[]);
    assert_eq!(
        status, 202,
        "completed worker can be reused after clearing drain"
    );
    let resumed_run_id = serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        http(
            address,
            "POST",
            &format!("/v1/runs/{resumed_run_id}/stop"),
            None,
            &[]
        )
        .0,
        202
    );
    wait_for_run_state(
        address,
        &resumed_run_id,
        &["cancelled"],
        Duration::from_secs(3),
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn request_header_and_processing_time_bounds_are_enforced() {
    let path = TempDir::new("request-bounds");
    let config = write_config(&path, "all", "127.0.0.1:0", ",\"request_timeout_ms\":100");
    let child = command(&config, None)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start agent");
    let mut child = Agent(child);
    let stderr = child.0.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let mut reader = BufReader::new(stderr);
        let _ = reader.read_line(&mut line);
        let _ = sender.send(line);
        let _ = std::io::copy(&mut reader, &mut std::io::sink());
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("startup event");
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    let address: SocketAddr = event["address"].as_str().unwrap().parse().unwrap();
    wait_for_ready(address);

    let headers = vec![("X-Filler", "x"); 65];
    assert_eq!(
        http_with_custom_headers(address, "GET", "/healthz", None, &headers, &[]).0,
        431
    );

    let mut stream = TcpStream::connect(address).expect("connect for bounded body request");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream.write_all(b"POST /v1/jobs HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"id\":").unwrap();
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read timed request response");
    assert!(response.starts_with("HTTP/1.1 408"), "{response}");
    assert_eq!(http(address, "GET", "/healthz", None, &[]).0, 200);
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn blocking_storage_does_not_block_liveness_and_timed_out_work_keeps_its_permit() {
    let path = TempDir::new("blocking-storage");
    // Keep the deadline below SQLite's 250 ms busy timeout, with headroom for
    // the liveness probes when this test runs alongside the release suite.
    let (mut child, address) = start_with_request_timeout(&path, 200);
    let job = job_json("stop-me", &["/bin/sh", "-c", "exec /bin/sleep 60"]);
    assert_eq!(
        http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/stop-me/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let run_id = serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    let database = path.join("manager/manager.sqlite3");
    // Prove one known operation remains alive after the HTTP request times out.
    // Keep the lock only until that response arrives, before SQLite's own busy
    // timeout can expire, then observe the job through the API.
    let lock = Connection::open(&database).expect("open lock connection");
    lock.execute_batch("BEGIN EXCLUSIVE;")
        .expect("hold manager write lock");
    let (status, body) = thread::spawn(move || {
        let body = job_json("blocked-survives", &["/usr/bin/true"]);
        http(address, "POST", "/v1/jobs", Some("application/json"), &body)
    })
    .join()
    .expect("timed-out write request");
    assert_eq!(status, 408, "{body}");
    lock.execute_batch("ROLLBACK;")
        .expect("release manager write lock");
    let deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    loop {
        if http(address, "GET", "/v1/jobs/blocked-survives", None, &[]).0 == 200 {
            break;
        }
        assert!(Instant::now() < deadline, "timed-out write did not finish");
        thread::sleep(Duration::from_millis(20));
    }

    // Independently saturate general blocking work while storage is locked.
    lock.execute_batch("BEGIN EXCLUSIVE;")
        .expect("hold manager write lock for saturation check");
    let requests = 64;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(requests));
    let handles = (0..requests)
        .map(|index| {
            let barrier = std::sync::Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let body = job_json(&format!("blocked-{index}"), &["/usr/bin/true"]);
                http(address, "POST", "/v1/jobs", Some("application/json"), &body)
            })
        })
        .collect::<Vec<_>>();
    // Observe admission saturation directly instead of guessing how long the
    // parallel request threads need to reach the server.
    let saturation_deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    loop {
        let body = job_json("saturated", &["/usr/bin/true"]);
        let (status, response) = http(address, "POST", "/v1/jobs", Some("application/json"), &body);
        if status == 429 {
            assert!(response.contains("blocking_admission_saturated"));
            break;
        }
        assert_eq!(status, 408, "{response}");
        assert!(
            Instant::now() < saturation_deadline,
            "blocking admission did not reach its configured capacity"
        );
    }
    assert_eq!(http(address, "GET", "/healthz", None, &[]).0, 200);
    assert_eq!(http(address, "GET", "/readyz", None, &[]).0, 200);
    let (status, response) = http(
        address,
        "POST",
        &format!("/v1/runs/{run_id}/stop"),
        None,
        &[],
    );
    assert_eq!(
        status, 408,
        "reserved control operation was rejected: {response}"
    );

    lock.execute_batch("ROLLBACK;")
        .expect("release manager write lock");
    let mut timed_out_requests = 0;
    let mut rejected_requests = 0;
    for handle in handles {
        let (status, body) = handle.join().expect("request thread");
        match status {
            408 => timed_out_requests += 1,
            429 => {
                assert!(body.contains("blocking_admission_saturated"), "{body}");
                rejected_requests += 1;
            }
            _ => panic!("unexpected response while storage remained locked: {status} {body}"),
        }
    }
    assert!(timed_out_requests > 0, "no admitted request timed out");
    assert!(
        rejected_requests > 0,
        "bounded admission did not reject excess work"
    );
    let deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    loop {
        let (status, body) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
        if matches!(status, 408 | 429) {
            assert!(
                Instant::now() < deadline,
                "timed-out or saturated run inspection did not recover"
            );
            thread::sleep(Duration::from_millis(20));
            continue;
        }
        assert_eq!(status, 200, "{body}");
        let observation: serde_json::Value = serde_json::from_str(&body).unwrap();
        if observation["state"] == "cancelled" {
            break;
        }
        assert!(Instant::now() < deadline, "stop operation did not finish");
        thread::sleep(Duration::from_millis(20));
    }
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn timed_out_start_can_be_retried_with_the_same_idempotency_key() {
    let path = TempDir::new("timed-out-idempotent-start");
    let marker = path.join("started-once");
    let command = [
        "/bin/sh",
        "-c",
        &format!("printf x >> '{}'", marker.display()),
    ];
    let job = job_json("timeout-start", &command);

    // Register the job with the normal request budget so setup latency on a
    // busy CI runner cannot consume the intentionally short timeout used by
    // the idempotency retry portion of this test.
    let config = write_config(&path, "all", "127.0.0.1:0", "");
    let (mut setup, address) = start_config(config, None);
    assert_eq!(
        http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
        201
    );
    signal(&setup, "INT");
    wait_for_exit(&mut setup);

    // Keep this deadline below SQLite's 250 ms busy timeout so the first run
    // request can time out while the test holds the manager database lock.
    let (mut child, address) = start_with_request_timeout(&path, 200);

    let lock = Connection::open(path.join("manager/manager.sqlite3")).unwrap();
    lock.execute_batch("BEGIN EXCLUSIVE;").unwrap();
    let headers = [("Idempotency-Key", "timeout-start-once")];
    let (status, _, _) = http_with_custom_headers(
        address,
        "POST",
        "/v1/jobs/timeout-start/runs",
        None,
        &headers,
        &[],
    );
    assert_eq!(status, 408);
    lock.execute_batch("ROLLBACK;").unwrap();

    let retry_deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    let (status, response) = loop {
        let (status, _, response) = http_with_custom_headers(
            address,
            "POST",
            "/v1/jobs/timeout-start/runs",
            None,
            &headers,
            &[],
        );
        if status == 202 {
            break (status, response);
        }
        assert!(matches!(status, 408 | 429), "{status}: {response}");
        assert!(
            Instant::now() < retry_deadline,
            "idempotent start did not become available after its first timeout"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(status, 202, "{response}");
    let run_id = serde_json::from_str::<serde_json::Value>(&response).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (status, body) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
        assert_eq!(status, 200, "{body}");
        let observation: serde_json::Value = serde_json::from_str(&body).unwrap();
        if observation["state"] == "succeeded" {
            break;
        }
        assert!(Instant::now() < deadline, "timed-out start did not finish");
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(fs::read_to_string(marker).unwrap(), "x");
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_shuts_down_on_sigterm() {
    let path = TempDir::new("sigterm");
    let (mut child, address) = start_with_capacity(&path, "all", None, 2);
    let mut run_ids = Vec::new();
    for id in ["term-a", "term-b"] {
        let job = job_json(id, &["/bin/sh", "-c", "exec /bin/sleep 60"]);
        assert_eq!(
            http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
            201
        );
        let (status, accepted) = http(address, "POST", &format!("/v1/jobs/{id}/runs"), None, &[]);
        assert_eq!(status, 202, "{accepted}");
        run_ids.push(
            serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    signal(&child, "TERM");
    // Server graceful shutdown and worker process cleanup each have an
    // independent bounded window; keep the test limit aligned with that
    // documented combined ceiling under a busy test runner.
    wait_for_exit_with_timeout(&mut child, Duration::from_secs(12));
    assert!(
        TcpStream::connect(address).is_err(),
        "listener remained bound"
    );
    let (mut restarted, address) = start_with_capacity(&path, "all", None, 2);
    for run_id in run_ids {
        let (status, result) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
        assert_eq!(status, 200, "{result}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&result).unwrap()["terminationReason"],
            "shutdown"
        );
    }
    signal(&restarted, "INT");
    wait_for_exit(&mut restarted);
}

#[test]
fn readiness_and_sigterm_remain_responsive_during_stalled_worker_startup() {
    let path = TempDir::new("worker-start-stall");
    let (mut child, address) = start_with_request_timeout(&path, 100);
    let job = job_json("startup-stall", &["/bin/sleep", "30"]);
    assert_eq!(
        http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
        201
    );

    // Hold the worker database write lock so start owns its lifecycle gate
    // while waiting in the worker's bounded SQLite busy timeout.
    let worker_lock = Connection::open(path.join("worker/worker.sqlite3")).unwrap();
    worker_lock.execute_batch("BEGIN IMMEDIATE;").unwrap();
    let (sender, receiver) = mpsc::channel();
    let run_address = address;
    let run_request = thread::spawn(move || {
        let response = http(
            run_address,
            "POST",
            "/v1/jobs/startup-stall/runs",
            None,
            &[],
        );
        sender.send(response).unwrap();
    });
    thread::sleep(Duration::from_millis(30));

    let ready_started = Instant::now();
    let (ready_status, ready_body) = http(address, "GET", "/readyz", None, &[]);
    assert_eq!(ready_status, 200, "{ready_body}");
    assert!(ready_started.elapsed() < Duration::from_millis(100));
    let health_started = Instant::now();
    let (health_status, health_body) = http(address, "GET", "/healthz", None, &[]);
    assert_eq!(health_status, 200, "{health_body}");
    assert!(health_started.elapsed() < Duration::from_millis(100));

    // Status uses bounded blocking storage work; its configured request
    // deadline must still be serviced by the HTTP executor.
    let workers_started = Instant::now();
    let (workers_status, workers_body) = http(address, "GET", "/v1/workers", None, &[]);
    assert_eq!(workers_status, 408, "{workers_body}");
    assert!(workers_started.elapsed() < Duration::from_millis(500));
    let (start_status, start_body) = receiver.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(start_status, 408, "{start_body}");

    signal(&child, "TERM");
    worker_lock.execute_batch("ROLLBACK;").unwrap();
    run_request.join().unwrap();
    let manager = Connection::open(path.join("manager/manager.sqlite3")).unwrap();
    let run_id = manager
        .query_row(
            "SELECT run_id FROM runs WHERE job_id = 'startup-stall'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    wait_for_exit(&mut child);

    let (mut restarted, address) = start_with_capacity(&path, "all", None, 1);
    let (status, observation) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
    assert_eq!(status, 200, "{observation}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&observation).unwrap()["terminationReason"],
        "shutdown"
    );
    signal(&restarted, "INT");
    wait_for_exit(&mut restarted);
}

#[test]
fn actual_binary_bounds_sigterm_shutdown_when_terminal_writes_fail() {
    let path = TempDir::new("sigterm-persistence-outage");
    let (mut child, address) = start(&path, "all", None);
    let body = job_json("shutdown-outage", &["/bin/sleep", "30"]);
    assert_eq!(
        http(address, "POST", "/v1/jobs", Some("application/json"), &body).0,
        201
    );
    let (status, response) = http(address, "POST", "/v1/jobs/shutdown-outage/runs", None, &[]);
    assert_eq!(status, 202, "{response}");
    let run_id = serde_json::from_str::<serde_json::Value>(&response).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    let database = Connection::open(path.join("worker/worker.sqlite3")).unwrap();
    database
        .execute_batch(
            "CREATE TRIGGER reject_terminal BEFORE UPDATE OF state ON runs
             WHEN NEW.state IN ('succeeded', 'failed', 'cancelled', 'timed_out')
             BEGIN SELECT RAISE(FAIL, 'injected terminal persistence failure'); END;",
        )
        .unwrap();
    drop(database);

    let started = Instant::now();
    signal(&child, "TERM");
    let status = wait_for_failure_exit(&mut child, Duration::from_secs(11));
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(started.elapsed() < Duration::from_secs(11));
    let persisted = Connection::open(path.join("worker/worker.sqlite3"))
        .unwrap()
        .query_row(
            "SELECT state FROM runs WHERE run_id = ?1",
            [run_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert!(matches!(persisted.as_str(), "running" | "stopping"));
    assert!(!status.success());
}

#[test]
fn actual_binary_keeps_serving_after_shutdown_grace_period() {
    let path = TempDir::new("daemon-lifetime");
    let (mut child, address) = start(&path, "all", None);
    let deadline = Instant::now() + Duration::from_millis(5_500);
    while Instant::now() < deadline {
        assert_eq!(
            child.0.try_wait().expect("poll daemon"),
            None,
            "daemon exited without a shutdown signal"
        );
        assert_eq!(http(address, "GET", "/healthz", None, &[]).0, 200);
        thread::sleep(Duration::from_millis(100));
    }
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn role_override_can_enable_combined_mode_and_standalone_roles_report_configuration_errors() {
    let path = TempDir::new("roles");
    let (mut child, _) = start(&path, "manager", Some("all"));
    signal(&child, "INT");
    wait_for_exit(&mut child);

    let manager_config = path.join("manager-invalid.json");
    fs::write(
        &manager_config,
        serde_json::to_vec(&serde_json::json!({
            "role":"manager",
            "listen":"127.0.0.1:0",
            "manager_data_dir":path.join("manager-state"),
            "api_access":{"mode":"local"},
        }))
        .unwrap(),
    )
    .unwrap();
    let result = failed(&manager_config, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("worker_transport_listen"));

    let worker_config = path.join("worker-invalid.json");
    fs::write(
        &worker_config,
        serde_json::to_vec(&serde_json::json!({
            "role":"worker",
            "worker_data_dir":path.join("worker-state"),
            "worker_id":"worker-a",
            "worker_concurrency":1,
            "worker_identity_file":path.join("worker-state/identity.pem"),
            "manager_ca_file":path.join("manager-ca.pem"),
        }))
        .unwrap(),
    )
    .unwrap();
    let result = failed(&worker_config, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("manager_endpoint"));
}

#[test]
fn standalone_manager_and_worker_run_an_authenticated_remote_one_shot() {
    let path = TempDir::new("standalone-remote-job");
    let manager_data_dir = path.join("manager-state");
    let identity_dir = path.join("manager-identity");
    let api_config = path.join("manager.json");
    let reserved_transport_listener =
        TcpListener::bind("127.0.0.1:0").expect("reserve transport port");
    let configured_transport_address = reserved_transport_listener.local_addr().unwrap();
    drop(reserved_transport_listener);
    fs::write(
        &api_config,
        serde_json::to_vec(&serde_json::json!({
            "role":"manager",
            "listen":"127.0.0.1:0",
            "worker_transport_listen":configured_transport_address.to_string(),
            "manager_data_dir":"manager-state",
            "manager_identity_dir":"manager-identity",
            "manager_server_name":"localhost",
            "worker_id":"remote-worker",
            "worker_transport_capacity":2,
            "worker_resource_domain_id":"shared-host",
            "worker_labels":{"pool":"integration"},
            "host_resource_domains":[{"id":"shared-host","cpu_millis":1000,"memory_bytes":1073741824}],
            "additional_workers":[{"worker_id":"remote-worker-2","capacity":2,"resource_domain_id":"shared-host","labels":{"pool":"integration"}}],
            "api_access":{"mode":"local"},
        }))
        .unwrap(),
    )
    .unwrap();
    let (mut manager, event) = start_without_api_readiness(&api_config);
    let mut api_address: SocketAddr = event["address"]
        .as_str()
        .expect("manager API address")
        .parse()
        .expect("valid manager API address");
    let transport_address: SocketAddr = event["workerTransportAddress"]
        .as_str()
        .expect("worker transport address")
        .parse()
        .expect("valid worker transport address");
    assert_eq!(
        transport_address.port(),
        configured_transport_address.port()
    );

    wait_for_http_status(api_address, "/healthz", 200, Duration::from_secs(5));
    let (status, readiness) = http(api_address, "GET", "/readyz", None, &[]);
    assert_eq!(
        status, 503,
        "manager should expose degraded readiness before workers connect: {readiness}"
    );
    let readiness: serde_json::Value = serde_json::from_str(&readiness).unwrap();
    assert_eq!(readiness["role"], "manager");
    assert_eq!(readiness["status"], "degraded");

    let mut workers = Vec::new();
    let wrong_host_id = "wrong-host-worker";
    let wrong_host_credential = path.join("wrong-host-credential");
    let enrollment = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args([
            "enroll-worker",
            "--manager-data-dir",
            manager_data_dir.to_str().expect("manager path"),
            "--identity-dir",
            identity_dir.to_str().expect("identity path"),
            "--worker-id",
            wrong_host_id,
            "--output",
            wrong_host_credential.to_str().expect("credential path"),
        ])
        .output()
        .expect("create wrong-host worker enrollment credential");
    assert!(
        enrollment.status.success(),
        "{}",
        String::from_utf8_lossy(&enrollment.stderr)
    );
    let wrong_host_identity = path.join("wrong-host-state/identity.pem");
    let wrong_host_config = path.join("wrong-host.json");
    fs::write(
        &wrong_host_config,
        serde_json::to_vec(&serde_json::json!({
            "role":"worker",
            "worker_data_dir":"wrong-host-state",
            "worker_id":wrong_host_id,
            "worker_concurrency":1,
            "manager_endpoint":format!("https://127.0.0.1:{}/", transport_address.port()),
            "worker_identity_file":"wrong-host-state/identity.pem",
            "worker_enrollment_credential_file":"wrong-host-credential",
            "manager_ca_file":"manager-identity/worker-ca.pem",
        }))
        .unwrap(),
    )
    .unwrap();
    let mut wrong_host_worker = start_worker_process(&wrong_host_config);
    wait_for_failure_exit(&mut wrong_host_worker, Duration::from_secs(12));
    assert!(
        !wrong_host_identity.exists(),
        "worker must not persist an identity when manager hostname verification fails"
    );
    assert!(
        wrong_host_credential.exists(),
        "worker must retain the unconsumed enrollment credential after TLS failure"
    );

    let wrong_ca_id = "wrong-ca-worker";
    let wrong_ca_credential = path.join("wrong-ca-credential");
    let enrollment = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args([
            "enroll-worker",
            "--manager-data-dir",
            manager_data_dir.to_str().expect("manager path"),
            "--identity-dir",
            identity_dir.to_str().expect("identity path"),
            "--worker-id",
            wrong_ca_id,
            "--output",
            wrong_ca_credential.to_str().expect("credential path"),
        ])
        .output()
        .expect("create wrong-CA worker enrollment credential");
    assert!(
        enrollment.status.success(),
        "{}",
        String::from_utf8_lossy(&enrollment.stderr)
    );
    let untrusted_ca_dir = path.join("untrusted-ca");
    let untrusted_ca = conductor_transport::CertificateAuthority::open_or_create(&untrusted_ca_dir)
        .expect("create unrelated test CA");
    let wrong_ca_file = path.join("wrong-manager-ca.pem");
    fs::write(&wrong_ca_file, untrusted_ca.certificate_pem()).expect("write unrelated CA");
    let wrong_ca_identity = path.join("wrong-ca-state/identity.pem");
    let wrong_ca_config = path.join("wrong-ca.json");
    fs::write(
        &wrong_ca_config,
        serde_json::to_vec(&serde_json::json!({
            "role":"worker",
            "worker_data_dir":"wrong-ca-state",
            "worker_id":wrong_ca_id,
            "worker_concurrency":1,
            "manager_endpoint":format!("https://localhost:{}/", transport_address.port()),
            "worker_identity_file":"wrong-ca-state/identity.pem",
            "worker_enrollment_credential_file":"wrong-ca-credential",
            "manager_ca_file":"wrong-manager-ca.pem",
        }))
        .unwrap(),
    )
    .unwrap();
    let mut wrong_ca_worker = start_worker_process(&wrong_ca_config);
    wait_for_failure_exit(&mut wrong_ca_worker, Duration::from_secs(12));
    assert!(
        !wrong_ca_identity.exists(),
        "worker must not persist an identity when the manager CA is untrusted"
    );
    assert!(
        wrong_ca_credential.exists(),
        "worker must retain the unconsumed enrollment credential after CA validation fails"
    );

    for worker_id in ["remote-worker", "remote-worker-2"] {
        let credential_file = path.join(format!("{worker_id}-credential"));
        let enrollment = Command::new(env!("CARGO_BIN_EXE_conductor"))
            .args([
                "enroll-worker",
                "--manager-data-dir",
                manager_data_dir.to_str().expect("manager path"),
                "--identity-dir",
                identity_dir.to_str().expect("identity path"),
                "--worker-id",
                worker_id,
                "--output",
                credential_file.to_str().expect("credential path"),
            ])
            .output()
            .expect("create one-time worker enrollment credential");
        assert!(
            enrollment.status.success(),
            "{}",
            String::from_utf8_lossy(&enrollment.stderr)
        );
        let worker_data_dir = path.join(format!("{worker_id}-state"));
        let worker_identity = worker_data_dir.join("identity.pem");
        let worker_config = path.join(format!("{worker_id}.json"));
        fs::write(
            &worker_config,
            serde_json::to_vec(&serde_json::json!({
                "role":"worker",
            "worker_data_dir":format!("{worker_id}-state"),
                "worker_id":worker_id,
                "worker_concurrency":2,
                "manager_endpoint":format!("https://localhost:{}/", transport_address.port()),
            "worker_identity_file":format!("{worker_id}-state/identity.pem"),
            "worker_enrollment_credential_file":format!("{worker_id}-credential"),
            "manager_ca_file":"manager-identity/worker-ca.pem",
            }))
            .unwrap(),
        )
        .unwrap();
        let worker = start_worker_process(&worker_config);
        workers.push((worker, worker_identity, credential_file, worker_config));
        if worker_id == "remote-worker" {
            let deadline = Instant::now() + Duration::from_secs(5);
            let body = loop {
                let (status, body) = http(api_address, "GET", "/v1/workers", None, &[]);
                assert_eq!(status, 200, "{body}");
                let observed: serde_json::Value = serde_json::from_str(&body).unwrap();
                if observed[0]["fresh"] == true {
                    assert_eq!(observed[0]["available"], true, "{body}");
                    break body;
                }
                assert!(
                    Instant::now() < deadline,
                    "worker did not report fresh status: {body}"
                );
                thread::sleep(Duration::from_millis(20));
            };
            let observed: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert!(observed[0]["inventory"].is_object(), "{body}");
            let (status, readiness) = http(api_address, "GET", "/readyz", None, &[]);
            assert_eq!(
                status, 503,
                "second configured worker is still offline: {readiness}"
            );
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&readiness).unwrap()["status"],
                "degraded"
            );
        }
    }
    wait_for_ready_named(api_address, "manager after first worker enrollment");
    let (status, body) = http(api_address, "GET", "/v1/workers", None, &[]);
    assert_eq!(status, 200, "{body}");
    let workers_status: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(workers_status[0]["resourceDomainId"], "shared-host");
    assert_eq!(workers_status[1]["resourceDomainId"], "shared-host");
    assert_eq!(
        workers_status[0]["resourceDomainCapacity"]["cpuMillis"],
        1000
    );
    assert_eq!(
        workers_status[1]["resourceDomainCapacity"]["cpuMillis"],
        1000
    );
    assert_eq!(workers_status[0]["fresh"], true);
    assert_eq!(workers_status[1]["fresh"], true);
    assert_eq!(workers_status[0]["labels"]["pool"], "integration");
    assert_eq!(workers_status[1]["labels"]["pool"], "integration");
    assert_eq!(workers_status[0]["inventory"]["executionSlots"], 2);
    assert_eq!(workers_status[1]["inventory"]["executionSlots"], 2);
    assert_eq!(
        workers_status[0]["inventory"]["backends"][0],
        "native_process"
    );
    let (status, health) = http(api_address, "GET", "/healthz", None, &[]);
    assert_eq!(status, 200, "{health}");
    let health: serde_json::Value = serde_json::from_str(&health).unwrap();
    assert_eq!(health["role"], "manager");
    assert_eq!(health["capabilities"][0], "remote_worker_transport");
    let (status, readiness) = http(api_address, "GET", "/readyz", None, &[]);
    assert_eq!(status, 200, "{readiness}");
    let readiness: serde_json::Value = serde_json::from_str(&readiness).unwrap();
    assert_eq!(readiness["role"], "manager");
    assert_eq!(readiness["status"], "ready");
    for (_, identity, credential, _) in &workers {
        assert!(
            identity.is_file(),
            "worker should persist its private identity"
        );
        assert!(
            !credential.exists(),
            "single-use credential should be removed"
        );
    }

    let job = serde_json::json!({
        "id":"remote-one-shot",
        "command":["/bin/sh", "-c", "printf remote-worker-output"],
        "cwd":"/tmp",
    });
    let (status, body) = http(
        api_address,
        "POST",
        "/v1/jobs",
        Some("application/json"),
        &serde_json::to_vec(&job).unwrap(),
    );
    assert_eq!(status, 201, "{body}");
    let (status, accepted) = http(
        api_address,
        "POST",
        "/v1/jobs/remote-one-shot/runs",
        None,
        &[],
    );
    assert_eq!(status, 202, "{accepted}");
    let run_id = serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
        .as_str()
        .expect("accepted run ID")
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let (status, body) = http(api_address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
        assert_eq!(status, 200, "{body}");
        let observation: serde_json::Value = serde_json::from_str(&body).unwrap();
        if observation["state"] == "succeeded" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "remote run did not complete: {body}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let (status, logs) = http(
        api_address,
        "GET",
        &format!("/v1/runs/{run_id}/logs?stream=stdout"),
        None,
        &[],
    );
    assert_eq!(status, 200, "{logs}");
    let logs: serde_json::Value = serde_json::from_str(&logs).unwrap();
    let encoded = logs["data"].as_str().expect("base64 run log");
    let mut output = [0_u8; 64];
    let length = base64::engine::general_purpose::STANDARD
        .decode_slice(encoded, &mut output)
        .expect("decode remote worker output");
    assert_eq!(&output[..length], b"remote-worker-output");
    let active_job = serde_json::json!({
        "id":"active-on-manager-restart",
        "command":["/bin/sleep", "30"],
        "cwd":"/tmp",
    });
    let (status, body) = http(
        api_address,
        "POST",
        "/v1/jobs",
        Some("application/json"),
        &serde_json::to_vec(&active_job).unwrap(),
    );
    assert_eq!(status, 201, "{body}");
    let (status, accepted) = http(
        api_address,
        "POST",
        "/v1/jobs/active-on-manager-restart/runs",
        None,
        &[],
    );
    assert_eq!(status, 202, "{accepted}");
    let active_run_id = serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
        .as_str()
        .expect("accepted active run ID")
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let (status, body) = http(
            api_address,
            "GET",
            &format!("/v1/runs/{active_run_id}"),
            None,
            &[],
        );
        assert_eq!(status, 200, "{body}");
        if serde_json::from_str::<serde_json::Value>(&body).unwrap()["state"] == "running" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "run did not become active before manager restart: {body}"
        );
        thread::sleep(Duration::from_millis(20));
    }

    signal(&manager, "INT");
    wait_for_exit_with_timeout(&mut manager, Duration::from_secs(12));
    let (restarted_manager, restart_event) = start_without_api_readiness(&api_config);
    manager = restarted_manager;
    api_address = restart_event["address"]
        .as_str()
        .expect("restarted manager API address")
        .parse()
        .expect("valid restarted API address");
    assert_eq!(
        restart_event["workerTransportAddress"],
        configured_transport_address.to_string()
    );
    wait_for_http_status(api_address, "/healthz", 200, Duration::from_secs(5));
    let (status, readiness) = http(api_address, "GET", "/readyz", None, &[]);
    assert_eq!(
        status, 503,
        "restarted manager should initially be degraded: {readiness}"
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&readiness).unwrap()["status"],
        "degraded"
    );
    wait_for_ready_with_timeout(
        api_address,
        "manager after late worker reconnect",
        Duration::from_secs(15),
    );
    for (worker, _, _, _) in &mut workers {
        assert_eq!(worker.0.try_wait().expect("check reconnected worker"), None);
    }
    let (status, recovered) = http(
        api_address,
        "GET",
        &format!("/v1/runs/{active_run_id}"),
        None,
        &[],
    );
    assert_eq!(status, 200, "{recovered}");
    let (status, stopped) = http(
        api_address,
        "POST",
        &format!("/v1/runs/{active_run_id}/stop"),
        None,
        &[],
    );
    assert!(status == 200 || status == 202, "{stopped}");

    for (worker, _, _, _) in &workers {
        signal(worker, "INT");
    }
    for (worker, _, _, _) in &mut workers {
        wait_for_exit(worker);
    }
    signal(&manager, "INT");
    wait_for_exit(&mut manager);
}

#[test]
fn strict_json_rejects_unknown_fields_and_invalid_concurrency() {
    let path = TempDir::new("invalid-config");
    let unknown = write_config(&path, "all", "127.0.0.1:0", ",\"unexpected\":true");
    let result = failed(&unknown, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("unknown field"));

    let zero_concurrency = write_config(&path, "all", "127.0.0.1:0", "");
    let content = fs::read_to_string(&zero_concurrency)
        .expect("read generated config")
        .replace("\"worker_concurrency\":1", "\"worker_concurrency\":0");
    fs::write(&zero_concurrency, content).expect("write zero concurrency");
    let result = failed(&zero_concurrency, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("worker_concurrency"));

    let invalid_timeout = write_config(&path, "all", "127.0.0.1:0", ",\"request_timeout_ms\":99");
    let result = failed(&invalid_timeout, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("request_timeout_ms"));

    let config = path.join("malformed.json");
    fs::write(&config, b"{ this is not JSON").expect("write malformed JSON");
    let result = failed(&config, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("startup_failed"));
}

#[test]
fn corrupt_manager_state_fails_closed_without_resetting_database_bytes() {
    let path = TempDir::new("corrupt-manager-state");
    let manager_dir = path.join("manager");
    fs::create_dir_all(&manager_dir).unwrap();
    let database = manager_dir.join("manager.sqlite3");
    let original = b"not a sqlite database";
    fs::write(&database, original).unwrap();
    let config = write_config(&path, "all", "127.0.0.1:0", "");

    let result = failed(&config, None);
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        !result.status.success(),
        "corrupt state unexpectedly started"
    );
    assert!(stderr.contains("startup_failed"), "{stderr}");
    assert_eq!(fs::read(database).unwrap(), original);
}

#[test]
fn non_loopback_listener_and_occupied_port_fail_before_readiness() {
    let path = TempDir::new("listener-errors");
    let non_loopback = write_config(&path, "all", "0.0.0.0:0", "");
    let result = failed(&non_loopback, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("loopback"));

    let occupied = TcpListener::bind("127.0.0.1:0").expect("reserve listener");
    let address = occupied.local_addr().expect("reserved address");
    let config = write_config(&path, "all", &address.to_string(), "");
    let result = failed(&config, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("startup_failed"));
}

#[test]
fn clap_help_and_invalid_role_are_clear() {
    let help = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .arg("--help")
        .output()
        .expect("help");
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("agent"));

    let path = TempDir::new("cli-role");
    let config = write_config(&path, "all", "127.0.0.1:0", "");
    let result = failed(&config, Some("invalid"));
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("possible values"));
}

#[test]
fn enroll_worker_cli_writes_a_private_one_time_credential_without_logging_it() {
    let path = TempDir::new("enrollment-cli");
    let credential_path = path.join("credential");
    let output = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args([
            "enroll-worker",
            "--manager-data-dir",
            path.join("manager").to_str().expect("manager path"),
            "--identity-dir",
            path.join("identity").to_str().expect("identity path"),
            "--worker-id",
            "cli-worker",
            "--output",
            credential_path.to_str().expect("credential path"),
        ])
        .output()
        .expect("run enrollment credential command");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let credential = fs::read_to_string(&credential_path).expect("read protected credential file");
    assert_eq!(credential.trim().len(), 43);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("enrollment_credential_created"));
    assert!(!stderr.contains(credential.trim()));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(credential_path)
                .expect("credential metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn operator_token_cli_config_and_routes_enforce_operator_access() {
    let path = TempDir::new("operator-access");
    let token_path = path.join("operator-token");
    let output = Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args([
            "create-operator-token",
            "--output",
            token_path.to_str().expect("token path"),
        ])
        .output()
        .expect("create operator token");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let token = fs::read_to_string(&token_path).expect("read operator token file");
    let token = token.trim();
    assert_eq!(token.len(), 43);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("operator_token_created"));
    assert!(!stderr.contains(token));
    assert!(!String::from_utf8_lossy(&output.stdout).contains(token));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&token_path)
                .expect("token metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        use std::os::unix::fs::symlink;
        let symlink_path = path.join("operator-token-link");
        symlink(&token_path, &symlink_path).expect("create token symlink");
        let symlink_config = write_config(&path, "all", "127.0.0.1:0", "");
        let content = fs::read_to_string(&symlink_config)
            .expect("read symlink config")
            .replace(
                "\"api_access\":{\"mode\":\"local\"}",
                &format!(
                    "\"api_access\":{{\"mode\":\"operator_token_file\",\"token_file\":{}}}",
                    serde_json::to_string(symlink_path.to_str().expect("utf8 symlink path"))
                        .expect("encode symlink path")
                ),
            );
        fs::write(&symlink_config, content).expect("write symlink config");
        let result = failed(&symlink_config, None);
        assert!(
            String::from_utf8_lossy(&result.stderr)
                .contains("operator token file could not be accessed securely")
        );
    }

    let config = write_config(&path, "all", "127.0.0.1:0", "");
    let content = fs::read_to_string(&config)
        .expect("read generated config")
        .replace(
            "\"api_access\":{\"mode\":\"local\"}",
            &format!(
                "\"api_access\":{{\"mode\":\"operator_token_file\",\"token_file\":{}}}",
                serde_json::to_string(token_path.to_str().expect("utf8 token path"))
                    .expect("encode token path")
            ),
        );
    fs::write(&config, content).expect("write operator config");
    let (mut child, address) = start_config(config, None);

    assert_eq!(http(address, "GET", "/healthz", None, &[]).0, 200);
    let (status, headers, _) = http_with_custom_headers(
        address,
        "GET",
        "/v1/workers",
        None,
        &[("X-Worker-Id", "test-worker")],
        &[],
    );
    assert_eq!(status, 401);
    assert!(
        headers
            .to_ascii_lowercase()
            .contains("www-authenticate: bearer")
    );
    assert_eq!(http(address, "GET", "/v1/jobs", None, &[]).0, 401);

    let job = job_json("operator-job", &["/bin/true"]);
    assert_eq!(
        http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
        401
    );
    let (status, _, body) = http_with_custom_headers(
        address,
        "POST",
        "/v1/jobs",
        Some("application/json"),
        &[("Authorization", &format!("Bearer {token}"))],
        &job,
    );
    assert_eq!(status, 201, "{body}");
    let (status, _, _) = http_with_custom_headers(
        address,
        "GET",
        "/v1/workers",
        None,
        &[("Authorization", &format!("Bearer {token}"))],
        &[],
    );
    assert_eq!(status, 200);
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn operator_cli_authenticates_inspects_paginates_and_reads_bounded_logs() {
    let path = TempDir::new("operator-cli");
    let token_path = path.join("operator-token");
    let token = create_operator_token(&token_path);
    let wrong_token_path = path.join("wrong-operator-token");
    let wrong_token = create_operator_token(&wrong_token_path);
    assert_ne!(token, wrong_token);

    let config = write_config(
        &path,
        "all",
        "127.0.0.1:0",
        ",\"log_max_bytes_per_stream\":128",
    );
    let content = fs::read_to_string(&config)
        .expect("read generated config")
        .replace(
            "\"api_access\":{\"mode\":\"local\"}",
            &format!(
                "\"api_access\":{{\"mode\":\"operator_token_file\",\"token_file\":{}}}",
                serde_json::to_string(token_path.to_str().expect("utf8 token path"))
                    .expect("encode token path")
            ),
        );
    fs::write(&config, content).expect("write operator config");
    let (mut agent, address) = start_config(config, None);

    let unauthorized = operator_command(address, &wrong_token_path, &["workers", "list"]);
    let unauthorized_stderr = String::from_utf8_lossy(&unauthorized.stderr);
    assert!(!unauthorized.status.success());
    assert!(
        unauthorized_stderr.contains("HTTP 401"),
        "{unauthorized_stderr}"
    );
    assert!(!unauthorized_stderr.contains(&wrong_token));

    let workers = operator_command(address, &token_path, &["workers", "list"]);
    assert!(
        workers.status.success(),
        "{}",
        String::from_utf8_lossy(&workers.stderr)
    );
    let workers: serde_json::Value = serde_json::from_slice(&workers.stdout).unwrap();
    assert_eq!(workers[0]["workerId"], "test-worker");

    let worker = operator_command(address, &token_path, &["workers", "inspect", "test-worker"]);
    assert!(
        worker.status.success(),
        "{}",
        String::from_utf8_lossy(&worker.stderr)
    );
    let worker: serde_json::Value = serde_json::from_slice(&worker.stdout).unwrap();
    assert_eq!(worker["workerId"], "test-worker");

    let body = job_json("operator-cli-logs", &["/usr/bin/printf", "%0500d", "0"]);
    let (status, _, response) = http_with_custom_headers(
        address,
        "POST",
        "/v1/jobs",
        Some("application/json"),
        &[("Authorization", &format!("Bearer {token}"))],
        &body,
    );
    assert_eq!(status, 201, "{response}");
    let mut run_ids = Vec::new();
    for key in ["operator-cli-run-one", "operator-cli-run-two"] {
        let (status, _, response) = http_with_custom_headers(
            address,
            "POST",
            "/v1/jobs/operator-cli-logs/runs",
            None,
            &[
                ("Authorization", &format!("Bearer {token}")),
                ("Idempotency-Key", key),
            ],
            &[],
        );
        assert_eq!(status, 202, "{response}");
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        let run_id = response["runId"].as_str().unwrap().to_owned();
        wait_for_operator_run(address, &token, &run_id);
        run_ids.push(run_id);
    }

    let first_page = operator_command(
        address,
        &token_path,
        &["runs", "--job-id", "operator-cli-logs", "--limit", "1"],
    );
    assert!(
        first_page.status.success(),
        "{}",
        String::from_utf8_lossy(&first_page.stderr)
    );
    let first_page: serde_json::Value = serde_json::from_slice(&first_page.stdout).unwrap();
    assert_eq!(first_page["runs"].as_array().unwrap().len(), 1);
    let cursor = first_page["nextCursor"].as_str().expect("next page cursor");
    let second_page = operator_command(
        address,
        &token_path,
        &[
            "runs",
            "--job-id",
            "operator-cli-logs",
            "--limit",
            "1",
            "--cursor",
            cursor,
        ],
    );
    assert!(
        second_page.status.success(),
        "{}",
        String::from_utf8_lossy(&second_page.stderr)
    );
    let second_page: serde_json::Value = serde_json::from_slice(&second_page.stdout).unwrap();
    assert_eq!(second_page["runs"].as_array().unwrap().len(), 1);
    assert_ne!(
        first_page["runs"][0]["runId"], second_page["runs"][0]["runId"],
        "cursor pagination must return distinct stable run IDs"
    );

    let log = operator_command(
        address,
        &token_path,
        &[
            "logs",
            &run_ids[1],
            "--stream",
            "stdout",
            "--limit",
            "65536",
        ],
    );
    assert!(
        log.status.success(),
        "{}",
        String::from_utf8_lossy(&log.stderr)
    );
    assert_eq!(
        log.stdout.len(),
        128,
        "configured log storage cap is respected"
    );
    let metadata: serde_json::Value = serde_json::from_slice(&log.stderr).unwrap();
    assert_eq!(metadata["runId"], run_ids[1]);
    assert_eq!(metadata["offset"], 0);
    assert_eq!(metadata["nextOffset"], 128);
    assert_eq!(metadata["truncated"], true);

    let missing_run = operator_command(
        address,
        &token_path,
        &["logs", "missing-run", "--stream", "stdout"],
    );
    let missing_stderr = String::from_utf8_lossy(&missing_run.stderr);
    assert!(!missing_run.status.success());
    assert!(missing_stderr.contains("HTTP 404"), "{missing_stderr}");
    assert!(!missing_stderr.contains(&token));

    signal(&agent, "INT");
    wait_for_exit(&mut agent);
}

#[test]
fn operator_cli_preserves_disconnected_worker_status() {
    let path = TempDir::new("operator-cli-disconnected-worker");
    let token_path = path.join("operator-token");
    create_operator_token(&token_path);
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock manager API");
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept operator request");
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .expect("bound operator request read");
        let mut request = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).expect("read operator request");
            assert_ne!(read, 0, "operator client must send request headers");
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
            assert!(request.len() < 8192, "operator headers stay bounded");
        }
        let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
        assert!(request.contains("authorization: bearer "));
        let body = br#"[{"workerId":"worker-offline","capacity":2,"usedSlots":1,"availableSlots":1,"available":false,"degraded":false,"draining":false,"fresh":false,"unknownRuns":1}]"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .expect("write mock API headers");
        stream.write_all(body).expect("write mock API response");
    });
    let output = operator_command(address, &token_path, &["workers", "list"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let workers: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(workers[0]["workerId"], "worker-offline");
    assert_eq!(workers[0]["available"], false);
    assert_eq!(workers[0]["fresh"], false);
    assert_eq!(workers[0]["unknownRuns"], 1);
    server.join().expect("mock manager request completed");
}

#[test]
fn operator_cli_deploys_scales_rolls_back_and_drains_with_generation_guards() {
    let path = TempDir::new("operator-cli-deployment");
    let token_path = path.join("operator-token");
    let token = create_operator_token(&token_path);
    let config = write_config(&path, "all", "127.0.0.1:0", "");
    let content = fs::read_to_string(&config)
        .expect("read generated config")
        .replace("\"worker_concurrency\":1", "\"worker_concurrency\":2")
        .replace(
            "\"api_access\":{\"mode\":\"local\"}",
            &format!(
                "\"api_access\":{{\"mode\":\"operator_token_file\",\"token_file\":{}}}",
                serde_json::to_string(token_path.to_str().expect("utf8 token path"))
                    .expect("encode token path")
            ),
        );
    fs::write(&config, content).expect("write operator config");
    let (mut agent, address) = start_config(config, None);
    let authorization = format!("Bearer {token}");
    let digest_output = Command::new("shasum")
        .args(["-a", "256", "/bin/sleep"])
        .output()
        .expect("hash service executable");
    assert!(digest_output.status.success(), "hash service executable");
    let executable_digest = String::from_utf8(digest_output.stdout)
        .expect("digest output is UTF-8")
        .split_ascii_whitespace()
        .next()
        .expect("digest value")
        .to_owned();

    let create_revision = |argument: &str, key: &str| {
        let body = serde_json::to_vec(&serde_json::json!({
            "definition": {
                "id": "operator-cli-service",
                "command": ["/bin/sleep", argument],
                "cwd": "/tmp",
                "terminationGraceMs": 1000,
            },
            "workload": {
                "kind": "service",
                "identity": { "namespace": "default", "name": "operator-cli-service" },
            },
            "executableDigest": {
                "algorithm": "sha256",
                "value": executable_digest,
            },
            "executionRequirements": {},
            "resourceRequest": { "cpuMillis": 250, "memoryBytes": 268435456 },
        }))
        .unwrap();
        let (status, _, response) = http_with_custom_headers(
            address,
            "POST",
            "/v1/jobs/operator-cli-service/revisions",
            Some("application/json"),
            &[("Authorization", &authorization), ("Idempotency-Key", key)],
            &body,
        );
        assert_eq!(status, 201, "{response}");
    };

    create_revision("30", "operator-cli-revision-one");
    let deployed = operator_command(
        address,
        &token_path,
        &[
            "deploy",
            "operator-cli-service",
            "--create",
            "--revision",
            "1",
            "--replicas",
            "1",
            "--idempotency-key",
            "operator-cli-deploy-one",
        ],
    );
    assert!(
        deployed.status.success(),
        "{}",
        String::from_utf8_lossy(&deployed.stderr)
    );
    let deployed: serde_json::Value = serde_json::from_slice(&deployed.stdout).unwrap();
    assert_eq!(deployed["revisionId"]["revision"], 1);

    let inspect_ready = |replicas: usize| {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let output = operator_command(
                address,
                &token_path,
                &["service", "inspect", "operator-cli-service"],
            );
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
            let attempts = value["serviceAttempts"].as_array().unwrap();
            if value["deployment"]["status"] == "ready"
                && attempts.len() == replicas
                && attempts
                    .iter()
                    .all(|attempt| attempt["currentRun"]["state"] == "running")
            {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "service did not become ready: {value}"
            );
            thread::sleep(Duration::from_millis(20));
        }
    };
    let one_replica = inspect_ready(1);
    let stable_instance = one_replica["serviceAttempts"][0]["serviceInstanceId"]
        .as_str()
        .unwrap()
        .to_owned();
    let generation = one_replica["deployment"]["generation"].as_u64().unwrap();
    let generation_arg = generation.to_string();
    let scaled = operator_command(
        address,
        &token_path,
        &[
            "scale",
            "operator-cli-service",
            "--replicas",
            "2",
            "--if-match-generation",
            &generation_arg,
            "--idempotency-key",
            "operator-cli-scale-two",
        ],
    );
    assert!(
        scaled.status.success(),
        "{}",
        String::from_utf8_lossy(&scaled.stderr)
    );
    let two_replicas = inspect_ready(2);
    assert!(
        two_replicas["serviceAttempts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|attempt| {
                attempt["serviceInstanceId"] == stable_instance
                    && attempt["allocation"]["allocation"]["jobRevisionId"]["revision"] == 1
            })
    );
    assert_eq!(
        two_replicas["deployment"]["pendingReason"],
        serde_json::Value::Null
    );
    assert_eq!(
        two_replicas["etag"],
        format!(
            "\"{}\"",
            two_replicas["deployment"]["generation"].as_u64().unwrap()
        )
    );

    create_revision("31", "operator-cli-revision-two");
    let generation = two_replicas["deployment"]["generation"].as_u64().unwrap();
    let generation_arg = generation.to_string();
    let updated = operator_command(
        address,
        &token_path,
        &[
            "deploy",
            "operator-cli-service",
            "--revision",
            "2",
            "--replicas",
            "2",
            "--if-match-generation",
            &generation_arg,
            "--idempotency-key",
            "operator-cli-deploy-two",
        ],
    );
    assert!(
        updated.status.success(),
        "{}",
        String::from_utf8_lossy(&updated.stderr)
    );
    let updated: serde_json::Value = serde_json::from_slice(&updated.stdout).unwrap();
    assert_eq!(updated["revisionId"]["revision"], 2);
    let two_updated = inspect_ready(2);
    assert_eq!(two_updated["deployment"]["revisionId"]["revision"], 2);
    assert!(
        two_updated["serviceAttempts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|attempt| {
                attempt["serviceInstanceId"] == stable_instance
                    && attempt["allocation"]["allocation"]["jobRevisionId"]["revision"] == 2
            })
    );

    let generation = two_updated["deployment"]["generation"].as_u64().unwrap();
    let generation_arg = generation.to_string();
    let rollback = operator_command(
        address,
        &token_path,
        &[
            "rollback",
            "operator-cli-service",
            "--revision",
            "1",
            "--if-match-generation",
            &generation_arg,
            "--idempotency-key",
            "operator-cli-rollback-one",
        ],
    );
    assert!(
        rollback.status.success(),
        "{}",
        String::from_utf8_lossy(&rollback.stderr)
    );
    let rolled_back = inspect_ready(2);
    assert_eq!(rolled_back["deployment"]["revisionId"]["revision"], 1);

    let generation = rolled_back["deployment"]["generation"].as_u64().unwrap();
    let generation_arg = generation.to_string();
    let zero = operator_command(
        address,
        &token_path,
        &[
            "scale",
            "operator-cli-service",
            "--replicas",
            "0",
            "--if-match-generation",
            &generation_arg,
            "--idempotency-key",
            "operator-cli-scale-zero",
        ],
    );
    assert!(
        zero.status.success(),
        "{}",
        String::from_utf8_lossy(&zero.stderr)
    );
    let drained = operator_command(
        address,
        &token_path,
        &[
            "drain",
            "start",
            "test-worker",
            "--grace-period-seconds",
            "30",
        ],
    );
    assert!(
        drained.status.success(),
        "{}",
        String::from_utf8_lossy(&drained.stderr)
    );
    let drained: serde_json::Value = serde_json::from_slice(&drained.stdout).unwrap();
    assert_eq!(drained["workerId"], "test-worker");
    let drain_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let output = operator_command(address, &token_path, &["drain", "status", "test-worker"]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let status: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        if status["state"] == "completed" {
            break;
        }
        assert!(
            Instant::now() < drain_deadline,
            "worker drain did not complete: {status}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let cleared = operator_command(address, &token_path, &["drain", "clear", "test-worker"]);
    assert!(
        cleared.status.success(),
        "{}",
        String::from_utf8_lossy(&cleared.stderr)
    );
    let cleared: serde_json::Value = serde_json::from_slice(&cleared.stdout).unwrap();
    assert_eq!(cleared["state"], "cleared");

    signal(&agent, "INT");
    wait_for_exit(&mut agent);
}

fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> (u16, String) {
    let (status, _, body) = http_with_headers(address, method, path, content_type, body);
    (status, body)
}

fn http_with_headers(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> (u16, String, String) {
    http_with_custom_headers(address, method, path, content_type, &[], body)
}

fn http_with_custom_headers(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    extra_headers: &[(&str, &str)],
    body: &[u8],
) -> (u16, String, String) {
    let mut stream = TcpStream::connect(address).expect("connect to agent");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("set bounded read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(3)))
        .expect("set bounded write timeout");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    )
    .expect("write request headers");
    if let Some(content_type) = content_type {
        write!(stream, "Content-Type: {content_type}\r\n").expect("write content type");
    }
    for (name, value) in extra_headers {
        write!(stream, "{name}: {value}\r\n").expect("write custom header");
    }
    stream.write_all(b"\r\n").expect("finish request headers");
    stream.write_all(body).expect("write request body");

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .unwrap_or_else(|error| panic!("read response for {method} {path}: {error}"));
    let (headers, body) = response.split_once("\r\n\r\n").expect("HTTP response");
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .expect("HTTP status")
        .parse()
        .expect("numeric status");
    (status, headers.to_owned(), body.to_owned())
}

fn job_json(id: &str, command: &[&str]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": id,
        "command": command,
        "cwd": "/tmp",
    }))
    .expect("encode job")
}

fn decode_log_chunk(encoded: &str) -> [u8; 4] {
    use base64::Engine;

    let mut bytes = [0; 4];
    let length = base64::engine::general_purpose::STANDARD
        .decode_slice(encoded, &mut bytes)
        .unwrap();
    assert!(length <= bytes.len());
    bytes
}

fn job_with(id: &str, command: &[String], cwd: &str, env: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": id,
        "command": command,
        "cwd": cwd,
        "env": env,
    }))
    .expect("encode job")
}

fn start_and_poll(address: SocketAddr, job_id: &str) -> serde_json::Value {
    let (status, headers, body) = http_with_headers(
        address,
        "POST",
        &format!("/v1/jobs/{job_id}/runs"),
        None,
        &[],
    );
    assert_eq!(status, 202, "{body}");
    let run: serde_json::Value = serde_json::from_str(&body).expect("accepted run JSON");
    let run_id = run["runId"].as_str().expect("run ID");
    assert!(
        headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case(&format!("Location: /v1/runs/{run_id}"))),
        "missing Location header: {headers}"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, body) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
        assert_eq!(status, 200, "{body}");
        let observation: serde_json::Value = serde_json::from_str(&body).expect("run observation");
        if ["succeeded", "failed", "cancelled", "timed_out"]
            .contains(&observation["state"].as_str().unwrap_or(""))
        {
            assert!(observation["finishedAtMs"].is_number());
            assert!(observation.get("pid").is_none());
            return observation;
        }
        assert!(
            Instant::now() < deadline,
            "run did not reach terminal state: {observation}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_run_state(
    address: SocketAddr,
    run_id: &str,
    expected: &[&str],
    timeout: Duration,
) -> serde_json::Value {
    let deadline = Instant::now() + timeout;
    loop {
        let (status, body) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
        assert_eq!(status, 200, "{body}");
        let observation: serde_json::Value = serde_json::from_str(&body).expect("run observation");
        if expected.contains(&observation["state"].as_str().unwrap_or("")) {
            return observation;
        }
        assert!(
            Instant::now() < deadline,
            "run state did not match: {observation}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_closed_logs(address: SocketAddr, run_id: &str) {
    let deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    loop {
        let mut all_closed = true;
        for stream in ["stdout", "stderr"] {
            let (status, body) = http(
                address,
                "GET",
                &format!("/v1/runs/{run_id}/logs?stream={stream}"),
                None,
                &[],
            );
            if status == 404 {
                all_closed = false;
                continue;
            }
            assert_eq!(status, 200, "{body}");
            let chunk: serde_json::Value = serde_json::from_str(&body).unwrap();
            all_closed &= chunk["streamClosed"] == true;
        }
        if all_closed {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "output readers did not close both log streams before the watchdog"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn actual_binary_stops_runs_and_enforces_deadlines() {
    let path = TempDir::new("stop-and-deadline");
    let (mut child, address) = start(&path, "all", None);
    let cooperative_ready = path.join("cooperative-ready");
    let cooperative = serde_json::json!({
        "id":"cooperative-stop",
        "command":["/bin/sh", "-c", format!("trap 'exit 0' TERM; : > '{}'; while :; do /bin/sleep 1; done", cooperative_ready.display())],
        "cwd":"/tmp",
        "timeoutMs":null,
        "terminationGraceMs":500,
    });
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &serde_json::to_vec(&cooperative).unwrap()
        )
        .0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/cooperative-stop/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let accepted: serde_json::Value = serde_json::from_str(&accepted).unwrap();
    let run_id = accepted["runId"].as_str().unwrap();
    wait_for_run_state(address, run_id, &["running"], Duration::from_secs(3));
    let ready_deadline = Instant::now() + Duration::from_secs(3);
    while !cooperative_ready.exists() {
        assert!(
            Instant::now() < ready_deadline,
            "cooperative process did not initialize"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let (status, stopping) = http(
        address,
        "POST",
        &format!("/v1/runs/{run_id}/stop"),
        None,
        &[],
    );
    assert_eq!(status, 202, "{stopping}");
    let stopping: serde_json::Value = serde_json::from_str(&stopping).unwrap();
    assert_eq!(stopping["state"], "stopping");
    let cancelled = wait_for_run_state(address, run_id, &["cancelled"], Duration::from_secs(3));
    assert_eq!(cancelled["terminationReason"], "cancelled");
    assert_eq!(cancelled["exitCode"], 0, "{cancelled}");
    let (status, repeated) = http(
        address,
        "POST",
        &format!("/v1/runs/{run_id}/stop"),
        None,
        &[],
    );
    assert_eq!(status, 200, "{repeated}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&repeated).unwrap(),
        cancelled
    );

    let stubborn_ready = path.join("stubborn-ready");
    let stubborn = serde_json::json!({
        "id":"stubborn-stop",
        "command":["/bin/sh", "-c", format!("trap '' TERM; : > '{}'; while :; do /bin/sleep 1; done", stubborn_ready.display())],
        "cwd":"/tmp",
        "timeoutMs":null,
        "terminationGraceMs":100,
    });
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &serde_json::to_vec(&stubborn).unwrap()
        )
        .0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/stubborn-stop/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let accepted: serde_json::Value = serde_json::from_str(&accepted).unwrap();
    let run_id = accepted["runId"].as_str().unwrap();
    wait_for_run_state(address, run_id, &["running"], Duration::from_secs(3));
    let ready_deadline = Instant::now() + Duration::from_secs(3);
    while !stubborn_ready.exists() {
        assert!(
            Instant::now() < ready_deadline,
            "stubborn process did not initialize"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let started = Instant::now();
    assert_eq!(
        http(
            address,
            "POST",
            &format!("/v1/runs/{run_id}/stop"),
            None,
            &[]
        )
        .0,
        202
    );
    let stopped = wait_for_run_state(address, run_id, &["cancelled"], Duration::from_secs(3));
    assert!(started.elapsed() >= Duration::from_millis(80));
    assert_eq!(stopped["terminationReason"], "cancelled");
    assert!(stopped["terminationSignal"].is_number());

    let deadline_job = serde_json::json!({
        "id":"deadline",
        "command":["/bin/sh", "-c", "exec /bin/sleep 60"],
        "cwd":"/tmp",
        "timeoutMs":100,
        "terminationGraceMs":0,
    });
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &serde_json::to_vec(&deadline_job).unwrap()
        )
        .0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/deadline/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let accepted: serde_json::Value = serde_json::from_str(&accepted).unwrap();
    let run_id = accepted["runId"].as_str().unwrap();
    let timed_out = wait_for_run_state(address, run_id, &["timed_out"], Duration::from_secs(3));
    assert_eq!(timed_out["terminationReason"], "deadline_exceeded");
    assert!(timed_out["terminationSignal"].is_number());
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_registers_lists_reads_and_deletes_immutable_jobs() {
    let path = TempDir::new("jobs");
    let (mut child, address) = start(&path, "all", None);
    let marker = path.join("must-not-be-created");
    let marker_arg = marker.to_str().expect("UTF-8 temp path");
    let zeta = job_json("zeta", &["/usr/bin/touch", marker_arg]);
    let alpha = job_json("alpha", &["/usr/bin/touch", marker_arg]);

    let (status, created) = http(address, "POST", "/v1/jobs", Some("application/json"), &zeta);
    assert_eq!(status, 201, "{created}");
    let created: serde_json::Value = serde_json::from_str(&created).expect("job response JSON");
    assert_eq!(created["id"], "zeta");
    assert_eq!(created["inheritEnv"], false);
    assert_eq!(created["env"], serde_json::json!({}));
    assert_eq!(created["terminationGraceMs"], 5_000);
    assert_eq!(created["timeoutMs"], serde_json::Value::Null);

    let (status, _) = http(
        address,
        "POST",
        "/v1/jobs",
        Some("application/json"),
        &alpha,
    );
    assert_eq!(status, 201);
    let (status, fetched) = http(address, "GET", "/v1/jobs/zeta", None, &[]);
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&fetched).unwrap(),
        created
    );
    let (status, listed) = http(address, "GET", "/v1/jobs", None, &[]);
    assert_eq!(status, 200);
    let listed: serde_json::Value = serde_json::from_str(&listed).expect("collection JSON");
    assert_eq!(listed["jobs"][0]["id"], "alpha");
    assert_eq!(listed["jobs"][1]["id"], "zeta");

    let (status, duplicate) = http(address, "POST", "/v1/jobs", Some("application/json"), &zeta);
    assert_eq!(status, 409, "{duplicate}");
    assert!(duplicate.contains("duplicate_job"));
    assert!(!marker.exists(), "registering a job must not execute it");

    let (status, _) = http(address, "DELETE", "/v1/jobs/zeta", None, &[]);
    assert_eq!(status, 204);
    let (status, missing) = http(address, "GET", "/v1/jobs/zeta", None, &[]);
    assert_eq!(status, 404, "{missing}");
    assert!(missing.contains("job_not_found"));
    let (status, missing_delete) = http(address, "DELETE", "/v1/jobs/zeta", None, &[]);
    assert_eq!(status, 404, "{missing_delete}");
    let (status, reused) = http(address, "POST", "/v1/jobs", Some("application/json"), &zeta);
    assert_eq!(status, 409, "{reused}");

    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_rejects_invalid_jobs_bodies_and_media_types_without_poisoning_service() {
    let path = TempDir::new("invalid-jobs");
    let (mut child, address) = start(&path, "all", None);
    let base = r#"{"id":"valid","command":["/usr/bin/true"],"cwd":"/tmp"}"#;
    let invalid = [
        ("{ invalid", 400, "invalid_json"),
        (
            r#"{"id":"valid","command":["/usr/bin/true"],"cwd":"/tmp","unexpected":1}"#,
            422,
            "invalid_job",
        ),
        (r#"{"id":"valid","cwd":"/tmp"}"#, 422, "invalid_job"),
        (
            r#"{"id":"valid","command":[],"cwd":"/tmp"}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"valid","command":["true"],"cwd":"/tmp"}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"valid","command":["/usr/bin/true"],"cwd":"relative"}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"bad id","command":["/usr/bin/true"],"cwd":"/tmp"}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"valid","command":["/usr/bin/true","x\u0000y"],"cwd":"/tmp"}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"valid","command":["/usr/bin/true"],"cwd":"/tmp","env":{"BAD-NAME":"SECRET_VALUE"}}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"valid","command":["/usr/bin/true"],"cwd":"/tmp","env":{"GOOD":"bad\u0000value"}}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"valid","command":["/usr/bin/true"],"cwd":"/tmp","timeoutMs":0}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"valid","command":["/usr/bin/true"],"cwd":"/tmp","timeoutMs":604800001}"#,
            422,
            "invalid_job",
        ),
        (
            r#"{"id":"valid","command":["/usr/bin/true"],"cwd":"/tmp","terminationGraceMs":300001}"#,
            422,
            "invalid_job",
        ),
    ];
    for (body, expected_status, expected_code) in invalid {
        let (status, response) = http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            body.as_bytes(),
        );
        assert_eq!(status, expected_status, "{body}: {response}");
        assert!(response.contains(expected_code), "{body}: {response}");
        assert!(
            !response.contains("SECRET_VALUE"),
            "sensitive input echoed: {response}"
        );
    }

    let (status, unsupported) = http(
        address,
        "POST",
        "/v1/jobs",
        Some("text/plain"),
        base.as_bytes(),
    );
    assert_eq!(status, 415, "{unsupported}");
    assert!(unsupported.contains("unsupported_media_type"));

    let oversized = vec![b'x'; 65_537];
    let (status, too_large) = http(
        address,
        "POST",
        "/v1/jobs",
        Some("application/json"),
        &oversized,
    );
    assert_eq!(status, 413, "{too_large}");
    assert!(too_large.contains("body_too_large"));

    let (status, _) = http(
        address,
        "POST",
        "/v1/jobs",
        Some("application/json"),
        base.as_bytes(),
    );
    assert_eq!(status, 201);
    let (status, health) = http(address, "GET", "/healthz", None, &[]);
    assert_eq!(status, 200, "{health}");
    let (status, unknown) = http(
        address,
        "POST",
        "/v1/unknown",
        Some("application/json"),
        &[],
    );
    assert_eq!(status, 404, "{unknown}");

    signal(&child, "TERM");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_preserves_jobs_after_normal_and_abrupt_restart() {
    let path = TempDir::new("durable-restart");
    let (mut child, address) = start(&path, "all", None);
    let durable = job_json("durable", &["/usr/bin/true"]);
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &durable
        )
        .0,
        201
    );
    let deleted = job_json("deleted", &["/usr/bin/true"]);
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &deleted
        )
        .0,
        201
    );
    assert_eq!(
        http(address, "DELETE", "/v1/jobs/deleted", None, &[]).0,
        204
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);

    let (mut child, address) = start(&path, "all", None);
    let (status, body) = http(address, "GET", "/v1/jobs/durable", None, &[]);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &deleted
        )
        .0,
        409
    );
    signal(&child, "KILL");
    let status = child.0.wait().expect("wait for killed agent");
    assert!(!status.success());

    let (mut child, address) = start(&path, "all", None);
    let (status, body) = http(address, "GET", "/v1/jobs/durable", None, &[]);
    assert_eq!(status, 200, "{body}");
    signal(&child, "TERM");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_rejects_duplicate_manager_and_worker_state_ownership() {
    let path = TempDir::new("state-ownership");
    let manager = path.join("shared-manager");
    let worker_a = path.join("worker-a");
    let worker_b = path.join("worker-b");
    let config_a = write_config_dirs(
        &path.join("a"),
        "all",
        "127.0.0.1:0",
        &manager,
        &worker_a,
        "",
    );
    let mut owner = Agent(
        command(&config_a, None)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start owner"),
    );
    let stderr = owner.0.stderr.take().expect("stderr");
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let _ = BufReader::new(stderr).read_line(&mut line);
        let _ = sender.send(line);
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("owner readiness");
    let event: serde_json::Value = serde_json::from_str(&line).expect("owner startup event");
    assert_eq!(event["event"], "agent_started", "{line}");
    let address: SocketAddr = event["address"].as_str().unwrap().parse().unwrap();
    wait_for_ready(address);

    let config_manager_conflict = write_config_dirs(
        &path.join("b"),
        "all",
        "127.0.0.1:0",
        &manager,
        &worker_b,
        "",
    );
    let result = failed(&config_manager_conflict, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("already in use"));

    let config_worker_conflict = write_config_dirs(
        &path.join("c"),
        "all",
        "127.0.0.1:0",
        &path.join("manager-c"),
        &worker_a,
        "",
    );
    let result = failed(&config_worker_conflict, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("already in use"));
    assert_eq!(http(address, "GET", "/healthz", None, &[]).0, 200);
    signal(&owner, "INT");
    let mut owner = owner;
    wait_for_exit(&mut owner);
}

#[cfg(unix)]
#[test]
fn actual_binary_rejects_aliased_state_directories_and_sets_private_permissions() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let path = TempDir::new("state-paths");
    let manager = path.join("manager");
    fs::create_dir_all(&manager).unwrap();
    symlink(&manager, path.join("manager-alias")).expect("create manager alias");
    let config = write_config_dirs(
        &path.join("alias"),
        "all",
        "127.0.0.1:0",
        &manager,
        &path.join("manager-alias"),
        "",
    );
    let result = failed(&config, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("same directory"));

    let (mut child, _) = start(&path, "all", None);
    for directory in [&path.join("manager"), &path.join("worker")] {
        assert_eq!(
            fs::metadata(directory).unwrap().permissions().mode() & 0o077,
            0
        );
        assert_eq!(
            fs::metadata(directory.join(".conductor.lock"))
                .unwrap()
                .permissions()
                .mode()
                & 0o077,
            0
        );
    }
    let database = path.join("manager/manager.sqlite3");
    assert_eq!(
        fs::metadata(database).unwrap().permissions().mode() & 0o077,
        0
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_executes_jobs_and_reports_distinct_process_outcomes() {
    let path = TempDir::new("run-outcomes");
    let (mut child, address) = start(&path, "all", None);
    let cases = [
        (
            "success",
            vec!["/usr/bin/true".to_owned()],
            "/tmp",
            serde_json::json!({}),
            "succeeded",
            None,
            None,
        ),
        (
            "nonzero",
            vec!["/bin/sh".into(), "-c".into(), "exit 17".into()],
            "/tmp",
            serde_json::json!({}),
            "failed",
            Some(17),
            Some("non_zero_exit"),
        ),
        (
            "launch",
            vec!["/missing/conductor-run-executable".into()],
            "/tmp",
            serde_json::json!({}),
            "failed",
            None,
            Some("spawn_failed"),
        ),
        (
            "cwd",
            vec!["/usr/bin/true".into()],
            "/tmp/no-such-conductor-cwd",
            serde_json::json!({}),
            "failed",
            None,
            Some("spawn_failed"),
        ),
        (
            "signal",
            vec!["/bin/sh".into(), "-c".into(), "kill -TERM $$".into()],
            "/tmp",
            serde_json::json!({}),
            "failed",
            None,
            Some("signal_terminated"),
        ),
    ];
    for (id, command, cwd, env, state, exit_code, failure) in cases {
        let payload = job_with(id, &command, cwd, env);
        let (status, created) = http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &payload,
        );
        assert_eq!(status, 201, "{created}");
        let observation = start_and_poll(address, id);
        assert_eq!(observation["state"], state);
        if let Some(code) = exit_code {
            assert_eq!(observation["exitCode"], code);
        }
        if let Some(reason) = failure {
            assert_eq!(observation["failureReason"], reason);
        }
        if id == "signal" {
            assert!(observation["terminationSignal"].is_number());
        }
    }

    let marker = path.join("must-not-expand");
    let literal = format!("argument with spaces; $(touch {})", marker.display());
    let command = vec!["/usr/bin/printf".into(), "%s".into(), literal.clone()];
    let payload = job_with("arguments", &command, "/tmp", serde_json::json!({}));
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &payload
        )
        .0,
        201
    );
    let observation = start_and_poll(address, "arguments");
    let run_id = observation["runId"].as_str().unwrap();
    assert_eq!(
        fs::read_to_string(path.join("worker/runs").join(format!("{run_id}.stdout"))).unwrap(),
        literal
    );
    assert!(!marker.exists(), "shell metacharacters must remain literal");

    let env_payload = job_with(
        "environment",
        &["/usr/bin/env".into()],
        "/tmp",
        serde_json::json!({"CONDUCTOR_TEST_VALUE":"expected"}),
    );
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &env_payload
        )
        .0,
        201
    );
    let observation = start_and_poll(address, "environment");
    let run_id = observation["runId"].as_str().unwrap();
    let env_output =
        fs::read_to_string(path.join("worker/runs").join(format!("{run_id}.stdout"))).unwrap();
    assert_eq!(env_output.trim(), "CONDUCTOR_TEST_VALUE=expected");

    let canonical_cwd = fs::canonicalize(&*path).unwrap();
    let cwd = canonical_cwd.to_str().unwrap();
    let cwd_payload = job_with(
        "working-directory",
        &["/bin/pwd".into()],
        cwd,
        serde_json::json!({}),
    );
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &cwd_payload
        )
        .0,
        201
    );
    let observation = start_and_poll(address, "working-directory");
    let run_id = observation["runId"].as_str().unwrap();
    let output =
        fs::read_to_string(path.join("worker/runs").join(format!("{run_id}.stdout"))).unwrap();
    assert_eq!(output.trim(), cwd);

    let (status, collection) = http(
        address,
        "GET",
        "/v1/runs?jobId=nonzero&state=failed&limit=1",
        None,
        &[],
    );
    assert_eq!(status, 200, "{collection}");
    let collection: serde_json::Value = serde_json::from_str(&collection).unwrap();
    assert_eq!(collection["runs"].as_array().unwrap().len(), 1);
    signal(&child, "TERM");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_inherits_environment_only_when_job_opts_in() {
    let path = TempDir::new("inherit-environment");
    let config = write_config(&path, "all", "127.0.0.1:0", "");
    let mut child = Agent(
        command(&config, None)
            .env("CONDUCTOR_PARENT_TEST_VALUE", "controlled-value")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start agent with controlled environment"),
    );
    let stderr = child.0.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let _ = BufReader::new(stderr).read_line(&mut line);
        let _ = sender.send(line);
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("agent readiness");
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["event"], "agent_started", "{line}");
    let address: SocketAddr = event["address"].as_str().unwrap().parse().unwrap();
    wait_for_ready(address);
    let payload = serde_json::to_vec(&serde_json::json!({
        "id":"inherit-environment",
        "command":["/usr/bin/env"],
        "cwd":"/tmp",
        "inheritEnv":true,
    }))
    .unwrap();
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &payload
        )
        .0,
        201
    );
    let observation = start_and_poll(address, "inherit-environment");
    let run_id = observation["runId"].as_str().unwrap();
    let output =
        fs::read_to_string(path.join("worker/runs").join(format!("{run_id}.stdout"))).unwrap();
    assert!(
        output
            .lines()
            .any(|line| line == "CONDUCTOR_PARENT_TEST_VALUE=controlled-value")
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_enforces_one_run_per_job_and_worker_capacity() {
    let path = TempDir::new("run-capacity");
    let (mut child, address) = start(&path, "all", None);
    for id in ["active", "other"] {
        let payload = job_json(id, &["/bin/sh", "-c", "sleep 5"]);
        assert_eq!(
            http(
                address,
                "POST",
                "/v1/jobs",
                Some("application/json"),
                &payload
            )
            .0,
            201
        );
    }
    let (status, accepted) = http(address, "POST", "/v1/jobs/active/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let (status, conflict) = http(address, "POST", "/v1/jobs/active/runs", None, &[]);
    assert_eq!(status, 409, "{conflict}");
    assert!(conflict.contains("job_conflict"));
    let (status, conflict) = http(address, "DELETE", "/v1/jobs/active", None, &[]);
    assert_eq!(status, 409, "{conflict}");
    let (status, full) = http(address, "POST", "/v1/jobs/other/runs", None, &[]);
    assert_eq!(status, 429, "{full}");
    signal(&child, "TERM");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_turns_interrupted_runs_unknown_without_replaying_them() {
    let path = TempDir::new("run-crash");
    let pid_file = path.join("workload.pid");
    let marker = path.join("workload-started");
    let job_command = vec![
        "/bin/sh".to_owned(),
        "-c".to_owned(),
        format!(
            "echo $$ > {}; echo started >> {}; exec sleep 60",
            pid_file.display(),
            marker.display()
        ),
    ];
    let payload = job_with("ambiguous", &job_command, "/tmp", serde_json::json!({}));
    let config = write_config(&path, "all", "127.0.0.1:0", "");
    let mut child = Agent(
        command(&config, None)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start agent"),
    );
    let stderr = child.0.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let _ = BufReader::new(stderr).read_line(&mut line);
        let _ = sender.send(line);
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("agent readiness");
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    let address: SocketAddr = event["address"].as_str().unwrap().parse().unwrap();
    wait_for_ready(address);
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &payload
        )
        .0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/ambiguous/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let run: serde_json::Value = serde_json::from_str(&accepted).unwrap();
    let initial_incarnation = run["workerIncarnation"].as_u64().unwrap();
    let run_id = run["runId"].as_str().unwrap().to_owned();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !pid_file.exists() {
        assert!(Instant::now() < deadline, "workload did not start");
        thread::sleep(Duration::from_millis(10));
    }
    signal(&child, "KILL");
    let _ = child.0.wait().expect("wait for killed agent");

    let (mut restarted, address) = start(&path, "all", None);
    let (status, body) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
    assert_eq!(status, 200, "{body}");
    let observation: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(observation["state"], "unknown");
    assert!(observation["workerIncarnation"].as_u64().unwrap() > initial_incarnation);
    assert_eq!(fs::read_to_string(&marker).unwrap().lines().count(), 1);
    let (status, status_body) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
    assert_eq!(status, 200, "{status_body}");
    let worker_status: serde_json::Value = serde_json::from_str(&status_body).unwrap();
    assert_eq!(worker_status["usedSlots"], 1);
    assert_eq!(worker_status["availableSlots"], 0);
    assert_eq!(worker_status["unknownRuns"], 1);
    assert_eq!(http(address, "GET", "/readyz", None, &[]).0, 200);
    assert_eq!(
        http(address, "POST", "/v1/jobs/ambiguous/runs", None, &[]).0,
        409
    );
    let pid: u32 = fs::read_to_string(pid_file)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let status = Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let resolution = br#"{"resolution":"abandon","reason":"Externally verified workload cleanup"}"#;
    let (status, body) = http(
        address,
        "POST",
        &format!("/v1/runs/{run_id}/resolve"),
        Some("application/json"),
        resolution,
    );
    assert_eq!(status, 200, "{body}");
    let abandoned: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(abandoned["state"], "abandoned");
    assert_eq!(
        abandoned["resolutionReason"],
        "Externally verified workload cleanup"
    );
    assert_eq!(
        http(
            address,
            "POST",
            &format!("/v1/runs/{run_id}/resolve"),
            Some("application/json"),
            resolution
        )
        .0,
        200,
        "same resolution is idempotent",
    );
    let conflicting = br#"{"resolution":"abandon","reason":"different decision"}"#;
    assert_eq!(
        http(
            address,
            "POST",
            &format!("/v1/runs/{run_id}/resolve"),
            Some("application/json"),
            conflicting
        )
        .0,
        409,
    );
    signal(&restarted, "INT");
    wait_for_exit(&mut restarted);

    let (mut recovered, address) = start(&path, "all", None);
    let (status, body) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["state"],
        "abandoned"
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/ambiguous/runs", None, &[]);
    assert_eq!(
        status, 202,
        "resolution releases the worker slot: {accepted}"
    );
    signal(&recovered, "INT");
    wait_for_exit(&mut recovered);
}

#[test]
fn actual_binary_recovers_pending_terminal_persistence_as_unknown_without_replay() {
    let path = TempDir::new("terminal-persistence-crash");
    let marker = path.join("workload-started");
    let payload = job_with(
        "terminal-pending",
        &[
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            format!("echo started >> {}; sleep 0.1; exit 9", marker.display()),
        ],
        "/tmp",
        serde_json::json!({}),
    );
    let config = write_config(&path, "all", "127.0.0.1:0", "");
    let mut child = Agent(
        command(&config, None)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start agent"),
    );
    let stderr = child.0.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut line = String::new();
        let _ = BufReader::new(stderr).read_line(&mut line);
        let _ = sender.send(line);
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("agent readiness");
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["event"], "agent_started", "{line}");
    let address: SocketAddr = event["address"].as_str().unwrap().parse().unwrap();
    wait_for_ready(address);

    let connection = Connection::open(path.join("worker/worker.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_terminal BEFORE UPDATE OF state ON runs
             WHEN NEW.state IN ('succeeded', 'failed', 'cancelled', 'timed_out')
             BEGIN SELECT RAISE(FAIL, 'injected terminal persistence failure'); END;",
        )
        .unwrap();
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &payload
        )
        .0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/terminal-pending/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let run: serde_json::Value = serde_json::from_str(&accepted).unwrap();
    let run_id = run["runId"].as_str().unwrap().to_owned();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, body) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
        assert_eq!(status, 200, "{body}");
        let worker_status: serde_json::Value = serde_json::from_str(&body).unwrap();
        if worker_status["pendingPersistence"] == 1 && worker_status["liveProcesses"] == 0 {
            assert_eq!(worker_status["degraded"], true);
            assert_eq!(worker_status["usedSlots"], 1);
            assert_eq!(worker_status["available"], false);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "terminal persistence did not become pending"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let (status, readiness) = http(address, "GET", "/readyz", None, &[]);
    assert_eq!(status, 503, "{readiness}");
    assert!(readiness.contains("degraded"));
    assert_eq!(fs::read_to_string(&marker).unwrap().lines().count(), 1);

    signal(&child, "KILL");
    let _ = child.0.wait().expect("kill agent with pending persistence");
    drop(connection);

    let connection = Connection::open(path.join("worker/worker.sqlite3")).unwrap();
    connection
        .execute_batch("DROP TRIGGER reject_terminal;")
        .unwrap();
    drop(connection);
    let (mut restarted, address) = start(&path, "all", None);
    let (status, body) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
    assert_eq!(status, 200, "{body}");
    let observation: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(observation["state"], "unknown");
    assert_eq!(fs::read_to_string(&marker).unwrap().lines().count(), 1);
    let (status, body) = http(address, "GET", "/v1/workers/test-worker", None, &[]);
    assert_eq!(status, 200, "{body}");
    let worker_status: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(worker_status["pendingPersistence"], 0);
    assert_eq!(worker_status["unknownRuns"], 1);
    assert_eq!(worker_status["degraded"], false);
    signal(&restarted, "TERM");
    wait_for_exit(&mut restarted);
}

#[test]
fn actual_binary_cleans_process_group_members_and_keeps_terminal_history() {
    let path = TempDir::new("process-group");
    let orphan_pid_file = path.join("orphan.pid");
    let _orphan_guard = ProcessGuard(orphan_pid_file.clone());
    let (mut child, address) = start(&path, "all", None);

    let leader_exits = job_with(
        "leader-exits",
        &[
            "/bin/sh".into(),
            "-c".into(),
            format!("sleep 60 & echo $! > {}; exit 0", orphan_pid_file.display()),
        ],
        "/tmp",
        serde_json::json!({}),
    );
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &leader_exits
        )
        .0,
        201
    );
    let terminal = start_and_poll(address, "leader-exits");
    assert_eq!(terminal["state"], "succeeded");
    let orphan_pid = fs::read_to_string(&orphan_pid_file)
        .unwrap()
        .trim()
        .to_owned();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let status = Command::new("ps")
            .args(["-o", "stat=", "-p", &orphan_pid])
            .output()
            .unwrap();
        let process_state = String::from_utf8_lossy(&status.stdout);
        if process_state.trim().is_empty() || process_state.trim_start().starts_with('Z') {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "remaining process group member is alive: {process_state}"
        );
        thread::sleep(Duration::from_millis(10));
    }

    let (mut restarted, address) = {
        signal(&child, "INT");
        wait_for_exit(&mut child);
        start(&path, "all", None)
    };
    let run_id = terminal["runId"].as_str().unwrap();
    let (status, persisted) = http(address, "GET", &format!("/v1/runs/{run_id}"), None, &[]);
    assert_eq!(status, 200, "{persisted}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&persisted).unwrap(),
        terminal
    );

    let active_pid_file = path.join("active.pid");
    let _active_guard = ProcessGuard(active_pid_file.clone());
    let active = job_with(
        "shutdown-active",
        &[
            "/bin/sh".into(),
            "-c".into(),
            format!("echo $$ > {}; exec sleep 60", active_pid_file.display()),
        ],
        "/tmp",
        serde_json::json!({}),
    );
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &active
        )
        .0,
        201
    );
    assert_eq!(
        http(address, "POST", "/v1/jobs/shutdown-active/runs", None, &[]).0,
        202
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    while !active_pid_file.exists() {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(10));
    }
    let active_pid = fs::read_to_string(&active_pid_file)
        .unwrap()
        .trim()
        .to_owned();
    signal(&restarted, "TERM");
    wait_for_exit(&mut restarted);
    let status = Command::new("ps")
        .args(["-o", "stat=", "-p", &active_pid])
        .output()
        .unwrap();
    let process_state = String::from_utf8_lossy(&status.stdout);
    assert!(
        process_state.trim().is_empty() || process_state.trim_start().starts_with('Z'),
        "workload remained alive after shutdown: {process_state}"
    );
}

#[test]
fn actual_binary_serves_bounded_binary_logs_with_offsets_and_retention_metadata() {
    let path = TempDir::new("logs");
    let config = write_config(
        &path,
        "all",
        "127.0.0.1:0",
        ",\"log_max_bytes_per_stream\":4,\"log_retention_runs\":1",
    );
    let config_contents = fs::read_to_string(&config)
        .unwrap()
        .replace("\"worker_concurrency\":1", "\"worker_concurrency\":2");
    fs::write(&config, config_contents).unwrap();
    let child = command(&config, None)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start agent");
    let mut child = Agent(child);
    let stderr = child.0.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        let _ = reader.read_line(&mut line);
        let _ = sender.send(line);
        let _ = std::io::copy(&mut reader, &mut std::io::sink());
    });
    let line = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    let event: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(event["event"], "agent_started", "{line}");
    let address: SocketAddr = event["address"].as_str().unwrap().parse().unwrap();
    wait_for_ready(address);

    let job = serde_json::json!({
        "id":"binary-logs",
        "command":["/bin/sh", "-c", "printf '\\000\\377\\303\\251'; printf '\\000\\376\\014' >&2; (head -c 1048576 /dev/zero) & (head -c 1048576 /dev/zero >&2) & wait; sleep 0.3"],
        "cwd":"/tmp"
    });
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &serde_json::to_vec(&job).unwrap()
        )
        .0,
        201
    );
    let (status, accepted) = http(address, "POST", "/v1/jobs/binary-logs/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let run_id = serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    wait_for_run_state(address, &run_id, &["running"], Duration::from_secs(2));

    let deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    let chunk = loop {
        let (status, body) = http(
            address,
            "GET",
            &format!("/v1/runs/{run_id}/logs?stream=stdout&offset=0&limit=2"),
            None,
            &[],
        );
        assert_eq!(status, 200, "{body}");
        let chunk: serde_json::Value = serde_json::from_str(&body).unwrap();
        if chunk["nextOffset"].as_u64().unwrap_or_default() >= 2 {
            break chunk;
        }
        assert!(
            Instant::now() < deadline,
            "log reader did not observe output: {chunk}"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(chunk["dataEncoding"], "base64");
    assert_eq!(
        decode_log_chunk(chunk["data"].as_str().unwrap())[..2],
        [0, 255]
    );
    assert_eq!(chunk["offset"], 0);
    assert_eq!(chunk["nextOffset"], 2);
    assert_eq!(chunk["streamClosed"], false);

    let deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    let chunk = loop {
        let (status, body) = http(
            address,
            "GET",
            &format!("/v1/runs/{run_id}/logs?stream=stdout&offset=2&limit=2"),
            None,
            &[],
        );
        assert_eq!(status, 200, "{body}");
        let chunk: serde_json::Value = serde_json::from_str(&body).unwrap();
        if chunk["truncated"] == true {
            break chunk;
        }
        assert!(
            Instant::now() < deadline,
            "worker did not report capped output: {chunk}"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert_eq!(
        decode_log_chunk(chunk["data"].as_str().unwrap())[..2],
        [195, 169]
    );
    assert_eq!(chunk["truncated"], true);

    let finished = wait_for_run_state(address, &run_id, &["succeeded"], Duration::from_secs(3));
    assert_eq!(finished["state"], "succeeded");
    wait_for_closed_logs(address, &run_id);
    let (status, chunk) = http(
        address,
        "GET",
        &format!("/v1/runs/{run_id}/logs?stream=stderr&offset=0&limit=8"),
        None,
        &[],
    );
    assert_eq!(status, 200, "{chunk}");
    let chunk: serde_json::Value = serde_json::from_str(&chunk).unwrap();
    assert_eq!(
        decode_log_chunk(chunk["data"].as_str().unwrap())[..3],
        [0, 254, 12]
    );
    assert_eq!(chunk["streamClosed"], true);
    assert_eq!(chunk["eof"], true);
    assert_eq!(chunk["truncated"], true);
    let (status, empty) = http(
        address,
        "GET",
        &format!("/v1/runs/{run_id}/logs?stream=stdout&offset=99"),
        None,
        &[],
    );
    assert_eq!(status, 200);
    let empty: serde_json::Value = serde_json::from_str(&empty).unwrap();
    assert_eq!(empty["data"], "");
    assert_eq!(empty["eof"], true);
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs/{run_id}/logs?stream=stdout&limit=0"),
        None,
        &[],
    );
    assert_eq!(status, 400, "{body}");
    let active_job = job_json("retention-active", &["/bin/sleep", "60"]);
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &active_job
        )
        .0,
        201
    );
    let (status, active_accepted) =
        http(address, "POST", "/v1/jobs/retention-active/runs", None, &[]);
    assert_eq!(status, 202, "{active_accepted}");
    let active_id = serde_json::from_str::<serde_json::Value>(&active_accepted).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    wait_for_run_state(address, &active_id, &["running"], Duration::from_secs(2));
    let active_log_deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    loop {
        let (status, body) = http(
            address,
            "GET",
            &format!("/v1/runs/{active_id}/logs?stream=stdout"),
            None,
            &[],
        );
        if status == 200 {
            let chunk: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(chunk["streamClosed"], false, "{body}");
            break;
        }
        assert_eq!(status, 404, "active log request failed: {body}");
        assert!(
            Instant::now() < active_log_deadline,
            "active stream was not created: {body}"
        );
        thread::sleep(Duration::from_millis(10));
    }
    let second_job = job_json("retention-followup", &["/bin/true"]);
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &second_job
        )
        .0,
        201
    );
    start_and_poll(address, "retention-followup");
    // Pruning runs after the asynchronous output readers finish. Keep polling
    // for that state transition; this is only a deadlock guard, not a latency
    // expectation for the reader/reaper thread.
    let retention_deadline = Instant::now() + ASYNC_TEST_WATCHDOG;
    loop {
        let (status, body) = http(
            address,
            "GET",
            &format!("/v1/runs/{run_id}/logs?stream=stdout"),
            None,
            &[],
        );
        if status == 410 {
            break;
        }
        assert!(
            status == 200 || status == 404,
            "log retention poll failed: {body}"
        );
        assert!(
            Instant::now() < retention_deadline,
            "completed log did not reach the pruned state after asynchronous capture finished: {body}"
        );
        thread::sleep(Duration::from_millis(10));
    }
    wait_for_run_state(address, &active_id, &["running"], Duration::from_secs(2));
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs/{active_id}/logs?stream=stdout"),
        None,
        &[],
    );
    assert_eq!(status, 200, "active logs were pruned: {body}");
    let (status, body) = http(
        address,
        "GET",
        "/v1/runs/unknown-run/logs?stream=stdout",
        None,
        &[],
    );
    assert_eq!(status, 404, "{body}");
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs/{run_id}/logs?stream=stdout&limit=65537"),
        None,
        &[],
    );
    assert_eq!(status, 400, "{body}");
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs/{run_id}/logs?stream=other"),
        None,
        &[],
    );
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "invalid_log_query"
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);
    let (mut restarted, address) = start(&path, "all", None);
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs/{active_id}/logs?stream=stdout"),
        None,
        &[],
    );
    assert_eq!(status, 200, "{body}");
    let logs: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(logs["streamClosed"], true);
    assert_eq!(logs["eof"], true);
    fs::remove_file(path.join("worker/runs").join(format!("{active_id}.stdout"))).unwrap();
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs/{active_id}/logs?stream=stdout"),
        None,
        &[],
    );
    assert_eq!(status, 404, "{body}");
    signal(&restarted, "INT");
    wait_for_exit(&mut restarted);
}

#[cfg(unix)]
#[test]
fn log_storage_symlink_failure_returns_redacted_internal_error() {
    use std::os::unix::fs::symlink;

    let path = TempDir::new("log-storage-fault");
    let (mut child, address) = start(&path, "all", None);
    let job = job_json("log-storage", &["/bin/sh", "-c", "printf safe-output"]);
    assert_eq!(
        http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
        201
    );
    let result = start_and_poll(address, "log-storage");
    let run_id = result["runId"].as_str().unwrap();
    let logs_dir = path.join("worker/runs");
    let log_path = logs_dir.join(format!("{run_id}.stdout"));
    let target = path.join("secret-target");
    fs::write(&target, b"must-not-be-read-through-a-link").unwrap();
    fs::remove_file(&log_path).unwrap();
    symlink(&target, &log_path).unwrap();

    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs/{run_id}/logs?stream=stdout"),
        None,
        &[],
    );
    assert_eq!(status, 503, "{body}");
    assert!(body.contains("worker_unavailable"), "{body}");
    assert!(
        !body.contains("secret-target"),
        "storage path leaked: {body}"
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn actual_binary_deduplicates_concurrent_and_restarted_run_submissions() {
    let path = TempDir::new("idempotency");
    let (mut child, address) = start(&path, "all", None);
    let marker = path.join("launches");
    let job = serde_json::json!({
        "id":"idempotent",
        "command":["/bin/sh", "-c", format!("printf x >> {}; sleep 0.2", marker.display())],
        "cwd":"/tmp"
    });
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &serde_json::to_vec(&job).unwrap()
        )
        .0,
        201
    );
    let (status, _, body) = http_with_custom_headers(
        address,
        "POST",
        "/v1/jobs/idempotent/runs",
        None,
        &[("Idempotency-Key", "contains space")],
        &[],
    );
    assert_eq!(status, 400, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "invalid_idempotency_key"
    );

    let callers = (0..12)
        .map(|_| {
            thread::spawn(move || {
                http_with_custom_headers(
                    address,
                    "POST",
                    "/v1/jobs/idempotent/runs",
                    None,
                    &[("Idempotency-Key", "same-operation")],
                    &[],
                )
            })
        })
        .collect::<Vec<_>>();
    let responses = callers
        .into_iter()
        .map(|caller| caller.join().unwrap())
        .collect::<Vec<_>>();
    let mut run_ids = Vec::new();
    for (status, headers, body) in responses {
        assert_eq!(status, 202, "{headers}\n{body}");
        let run: serde_json::Value = serde_json::from_str(&body)
            .unwrap_or_else(|error| panic!("invalid run response {headers:?} {body:?}: {error}"));
        run_ids.push(run["runId"].as_str().unwrap().to_owned());
    }
    assert!(run_ids.iter().all(|id| id == &run_ids[0]));
    wait_for_run_state(address, &run_ids[0], &["succeeded"], Duration::from_secs(3));
    assert_eq!(fs::read_to_string(&marker).unwrap(), "x");

    let other_job = job_json("other-idempotent", &["/bin/true"]);
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &other_job
        )
        .0,
        201
    );
    let (status, _, body) = http_with_custom_headers(
        address,
        "POST",
        "/v1/jobs/other-idempotent/runs",
        None,
        &[("Idempotency-Key", "same-operation")],
        &[],
    );
    assert_eq!(status, 409, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["error"]["code"],
        "idempotency_key_conflict"
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);

    let (mut restarted, address) = start(&path, "all", None);
    let (status, _, body) = http_with_custom_headers(
        address,
        "POST",
        "/v1/jobs/idempotent/runs",
        None,
        &[("Idempotency-Key", "same-operation")],
        &[],
    );
    assert_eq!(status, 202, "{body}");
    let replay: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(replay["runId"], run_ids[0]);
    assert_eq!(fs::read_to_string(&marker).unwrap(), "x");
    signal(&restarted, "INT");
    wait_for_exit(&mut restarted);
}

#[test]
fn actual_binary_paginates_jobs_and_runs_with_filter_bound_cursors() {
    let path = TempDir::new("pagination");
    let (mut child, address) = start(&path, "all", None);
    for id in ["page-a", "page-b", "page-c", "page-d"] {
        let job = job_json(id, &["/bin/true"]);
        assert_eq!(
            http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
            201
        );
        start_and_poll(address, id);
    }

    let (status, body) = http(address, "GET", "/v1/jobs?limit=2", None, &[]);
    assert_eq!(status, 200, "{body}");
    let first: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(first["jobs"].as_array().unwrap().len(), 2);
    let cursor = first["nextCursor"].as_str().unwrap();
    let concurrent_job = job_json("page-bb", &["/bin/true"]);
    assert_eq!(
        http(
            address,
            "POST",
            "/v1/jobs",
            Some("application/json"),
            &concurrent_job
        )
        .0,
        201
    );
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/jobs?limit=2&cursor={cursor}"),
        None,
        &[],
    );
    assert_eq!(status, 200, "{body}");
    let second: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(second["jobs"].as_array().unwrap().len(), 2);
    assert_eq!(second["jobs"][0]["id"], "page-bb");
    assert_eq!(second["jobs"][1]["id"], "page-c");
    let cursor = second["nextCursor"].as_str().unwrap();
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/jobs?limit=2&cursor={cursor}"),
        None,
        &[],
    );
    assert_eq!(status, 200, "{body}");
    let third: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(third["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(third["jobs"][0]["id"], "page-d");

    let (status, body) = http(address, "GET", "/v1/runs?limit=2", None, &[]);
    assert_eq!(status, 200, "{body}");
    let first: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(first["runs"].as_array().unwrap().len(), 2);
    let cursor = first["nextCursor"].as_str().unwrap();
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs?limit=2&jobId=page-a&cursor={cursor}"),
        None,
        &[],
    );
    assert_eq!(status, 400, "cursor must be bound to its filters: {body}");
    let (status, body) = http(
        address,
        "GET",
        &format!("/v1/runs?limit=2&cursor={cursor}"),
        None,
        &[],
    );
    assert_eq!(status, 200, "{body}");
    let second: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(second["runs"].as_array().unwrap().len(), 2);
    assert!(second["nextCursor"].is_null());

    let (status, body) = http(address, "GET", "/v1/jobs?limit=101", None, &[]);
    assert_eq!(status, 400, "{body}");
    let (status, body) = http(address, "GET", "/v1/jobs?cursor=broken", None, &[]);
    assert_eq!(status, 400, "{body}");
    signal(&child, "INT");
    wait_for_exit(&mut child);
}

#[test]
fn capacity_rejection_does_not_consume_an_idempotency_key() {
    let path = TempDir::new("capacity-key");
    let (mut child, address) = start(&path, "all", None);
    for (id, command) in [
        ("occupying", vec!["/bin/sh", "-c", "sleep 5"]),
        ("retry-after-capacity", vec!["/bin/sh", "-c", "exit 0"]),
    ] {
        let job = job_json(id, &command);
        assert_eq!(
            http(address, "POST", "/v1/jobs", Some("application/json"), &job).0,
            201
        );
    }
    let (status, accepted) = http(address, "POST", "/v1/jobs/occupying/runs", None, &[]);
    assert_eq!(status, 202, "{accepted}");
    let run_id = serde_json::from_str::<serde_json::Value>(&accepted).unwrap()["runId"]
        .as_str()
        .unwrap()
        .to_owned();
    wait_for_run_state(address, &run_id, &["running"], Duration::from_secs(2));

    let (status, headers, body) = http_with_custom_headers(
        address,
        "POST",
        "/v1/jobs/retry-after-capacity/runs",
        None,
        &[("Idempotency-Key", "reuse-after-429")],
        &[],
    );
    assert_eq!(status, 429, "{body}");
    assert!(
        headers
            .lines()
            .any(|line| line.eq_ignore_ascii_case("Retry-After: 1"))
    );
    let (status, _) = http(
        address,
        "POST",
        &format!("/v1/runs/{run_id}/stop"),
        None,
        &[],
    );
    assert_eq!(status, 202);
    wait_for_run_state(address, &run_id, &["cancelled"], Duration::from_secs(3));

    let (status, _, body) = http_with_custom_headers(
        address,
        "POST",
        "/v1/jobs/retry-after-capacity/runs",
        None,
        &[("Idempotency-Key", "reuse-after-429")],
        &[],
    );
    assert_eq!(status, 202, "{body}");
    let run: serde_json::Value = serde_json::from_str(&body).unwrap();
    wait_for_run_state(
        address,
        run["runId"].as_str().unwrap(),
        &["succeeded"],
        Duration::from_secs(3),
    );
    signal(&child, "INT");
    wait_for_exit(&mut child);
}
