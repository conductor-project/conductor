use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

fn write_config(path: &Path, role: &str, listen: &str, extra: &str) -> PathBuf {
    let config_path = path.join("agent.json");
    let content = format!(
        r#"{{"role":"{role}","listen":"{listen}","manager_data_dir":"{}","worker_data_dir":"{}","worker_id":"test-worker","worker_concurrency":1{extra}}}"#,
        path.join("manager").display(),
        path.join("worker").display(),
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

fn start(path: &Path, config_role: &str, role_override: Option<&str>) -> (Agent, SocketAddr) {
    let config = write_config(path, config_role, "127.0.0.1:0", "");
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
        let _ = sender.send(line);
    });
    let line = receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("startup event before readiness deadline");
    let event: serde_json::Value = serde_json::from_str(&line).expect("structured startup event");
    assert_eq!(event["event"], "agent_started", "{line}");
    let address: SocketAddr = event["address"]
        .as_str()
        .expect("address")
        .parse()
        .expect("socket address");
    (child, address)
}

fn wait_for_exit(child: &mut Agent) {
    let deadline = Instant::now() + Duration::from_secs(5);
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
    let (mut child, address) = start(&path, "all", None);
    let mut stream = TcpStream::connect(address).expect("connect to agent");
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("response");
    assert!(response.starts_with("HTTP/1.1 200 OK"));
    assert!(response.ends_with(r#"{"status":"ok"}"#));
    signal(&child, "INT");
    wait_for_exit(&mut child);
    assert!(
        TcpStream::connect(address).is_err(),
        "listener remained bound"
    );
}

#[test]
fn actual_binary_shuts_down_on_sigterm() {
    let path = TempDir::new("sigterm");
    let (mut child, address) = start(&path, "all", None);
    signal(&child, "TERM");
    wait_for_exit(&mut child);
    assert!(
        TcpStream::connect(address).is_err(),
        "listener remained bound"
    );
}

#[test]
fn role_override_can_enable_combined_mode_and_standalone_roles_fail() {
    let path = TempDir::new("roles");
    let (mut child, _) = start(&path, "manager", Some("all"));
    signal(&child, "INT");
    wait_for_exit(&mut child);

    for role in ["manager", "worker"] {
        let config = write_config(&path, "all", "127.0.0.1:0", "");
        let result = failed(&config, Some(role));
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("unsupported"));
    }
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

    let config = path.join("malformed.json");
    fs::write(&config, b"{ this is not JSON").expect("write malformed JSON");
    let result = failed(&config, None);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("startup_failed"));
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

fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> (u16, String) {
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
    stream.write_all(b"\r\n").expect("finish request headers");
    stream.write_all(body).expect("write request body");

    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    let (headers, body) = response.split_once("\r\n\r\n").expect("HTTP response");
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .expect("HTTP status")
        .parse()
        .expect("numeric status");
    (status, body.to_owned())
}

fn job_json(id: &str, command: &[&str]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "id": id,
        "command": command,
        "cwd": "/tmp",
    }))
    .expect("encode job")
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
        base.as_bytes(),
    );
    assert_eq!(status, 404, "{unknown}");

    signal(&child, "TERM");
    wait_for_exit(&mut child);
}
