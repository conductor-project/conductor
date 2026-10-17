use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
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
    let mut reader = BufReader::new(stderr);
    let mut line = String::new();
    reader.read_line(&mut line).expect("startup event");
    let event: serde_json::Value = serde_json::from_str(&line).expect("structured startup event");
    assert_eq!(event["event"], "agent_started", "{line}");
    let address: SocketAddr = event["address"]
        .as_str()
        .expect("address")
        .parse()
        .expect("socket address");
    drop(reader);
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
