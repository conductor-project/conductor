use std::fs;
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_TEMP_CONFIG_HOME: AtomicU64 = AtomicU64::new(0);

fn temp_config_home() -> std::path::PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after Unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "conductor-context-test-{}-{nonce}-{}",
        std::process::id(),
        NEXT_TEMP_CONFIG_HOME.fetch_add(1, Ordering::Relaxed),
    ));
    fs::create_dir_all(&path).expect("create isolated config home");
    path
}

fn run(config_home: &std::path::Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor"))
        .args(arguments)
        .env("XDG_CONFIG_HOME", config_home)
        .env_remove("HOME")
        .output()
        .expect("run conductor CLI")
}

#[test]
fn context_crud_keeps_token_references_out_of_operator_output() {
    let config_home = temp_config_home();
    let context_dir = config_home.join("conductor");
    let token_path = "/tmp/secret/operator-token-reference";

    let created = run(
        &config_home,
        &[
            "context",
            "create",
            "local",
            "--server",
            "http://127.0.0.1:8080",
            "--token-file",
            token_path,
        ],
    );
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    assert!(context_dir.join("contexts.json").is_file());

    let used = run(&config_home, &["context", "use", "local"]);
    assert!(
        used.status.success(),
        "{}",
        String::from_utf8_lossy(&used.stderr)
    );

    for command in [vec!["context", "list"], vec!["context", "show", "local"]] {
        let output = run(&config_home, &command);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).expect("UTF-8 context output");
        assert!(!text.trim_start().starts_with('{'));
        assert!(text.contains("local"));
        assert!(text.contains("http://127.0.0.1:8080"));
        assert!(!text.contains(token_path));
    }

    for command in [
        vec!["context", "list", "--output", "json"],
        vec!["context", "--output", "json", "list"],
        vec!["context", "show", "local", "--output", "json"],
    ] {
        let output = run(&config_home, &command);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON output");
        assert!(!output.stdout.is_empty());
        let text = String::from_utf8(output.stdout).expect("UTF-8 context output");
        assert!(!text.contains(token_path));
        assert!(value.is_object());
    }

    let created_json = run(
        &config_home,
        &[
            "context",
            "create",
            "production",
            "--server",
            "https://manager.example.test",
            "--output",
            "json",
        ],
    );
    assert!(created_json.status.success());
    let created_value: serde_json::Value =
        serde_json::from_slice(&created_json.stdout).expect("create JSON output");
    assert_eq!(created_value["created"], true);
    assert_eq!(created_value["name"], "production");

    let used_json = run(
        &config_home,
        &["context", "use", "production", "--output", "json"],
    );
    assert!(used_json.status.success());
    let used_value: serde_json::Value =
        serde_json::from_slice(&used_json.stdout).expect("use JSON output");
    assert_eq!(used_value["selected"], true);

    let deleted = run(
        &config_home,
        &["context", "delete", "local", "--output", "json"],
    );
    assert!(
        deleted.status.success(),
        "{}",
        String::from_utf8_lossy(&deleted.stderr)
    );
    let missing = run(&config_home, &["context", "show", "local"]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("context does not exist"));
    let deleted_json = run(
        &config_home,
        &["context", "delete", "production", "--output", "json"],
    );
    assert!(deleted_json.status.success());
    let deleted_value: serde_json::Value =
        serde_json::from_slice(&deleted_json.stdout).expect("delete JSON output");
    assert_eq!(deleted_value["deleted"], true);
    fs::remove_dir_all(config_home).expect("remove isolated context store");
}

#[test]
fn context_creation_rejects_insecure_remote_http_and_unknown_config_fields() {
    let config_home = temp_config_home();
    let rejected = run(
        &config_home,
        &[
            "context",
            "create",
            "remote",
            "--server",
            "http://manager.example.test",
        ],
    );
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("loopback"));

    let context_dir = config_home.join("conductor");
    fs::create_dir_all(&context_dir).expect("create context directory");
    fs::write(
        context_dir.join("contexts.json"),
        r#"{"unexpected":true,"contexts":{}}"#,
    )
    .expect("write malformed context store");
    let listed = run(&config_home, &["context", "list"]);
    assert!(
        !listed.status.success(),
        "unexpected success: {}",
        String::from_utf8_lossy(&listed.stdout)
    );
    assert!(String::from_utf8_lossy(&listed.stderr).contains("invalid or unsupported JSON"));
    fs::remove_dir_all(config_home).expect("remove isolated context store");
}

#[test]
fn completions_follow_the_implemented_command_tree() {
    let config_home = temp_config_home();
    let output = run(&config_home, &["completions", "bash"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let script = String::from_utf8(output.stdout).expect("UTF-8 completion script");
    assert!(script.contains("conductor"));
    assert!(script.contains("context"));
    assert!(script.contains("operator"));
    assert!(script.contains("conductor,deployment"));
    assert!(script.contains("conductor,run"));
    assert!(script.contains("conductor,service"));
    assert!(script.contains("conductor,diagnostics"));
    fs::remove_dir_all(config_home).expect("remove isolated context store");
}
