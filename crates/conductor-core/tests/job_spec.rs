use std::collections::BTreeMap;
use std::path::PathBuf;

use conductor_core::{JobId, JobSpec, RunFailureReason, RunId, RunSpec, RunState, WorkerId};

#[test]
fn identifiers_enforce_stable_ascii_constraints() {
    assert!(JobId::new("a_1.test-2").is_ok());
    assert!(JobId::new("").is_err());
    assert!(JobId::new("contains space").is_err());
    assert!(JobId::new("snowman-☃").is_err());
    assert!(JobId::new("x".repeat(129)).is_err());
    assert!(serde_json::from_str::<RunId>("\"bad id\"").is_err());
    assert!(serde_json::from_str::<WorkerId>("\"\"").is_err());
}

#[test]
fn job_spec_applies_defaults_and_serializes_canonically() {
    let json = r#"{"id":"hello","command":["/usr/bin/printf","hello world","$HOME"],"cwd":"/tmp"}"#;
    let job: JobSpec = serde_json::from_str(json).expect("valid job");
    assert!(!job.inherit_env());
    assert!(job.env().is_empty());
    assert_eq!(job.timeout_ms(), None);
    assert_eq!(job.termination_grace_ms(), 5_000);
    assert_eq!(job.command()[1], "hello world");
    assert_eq!(job.command()[2], "$HOME");

    let encoded = serde_json::to_string(&job).expect("serialize job");
    assert_eq!(
        encoded,
        r#"{"id":"hello","command":["/usr/bin/printf","hello world","$HOME"],"cwd":"/tmp","env":{},"inheritEnv":false,"timeoutMs":null,"terminationGraceMs":5000}"#
    );
    let decoded: JobSpec = serde_json::from_str(&encoded).expect("round trip");
    assert_eq!(decoded, job);
}

#[test]
fn job_spec_validates_all_process_inputs_and_numeric_bounds() {
    let valid = r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp"}"#;
    let cases = [
        (r#"{"id":"job","command":[],"cwd":"/tmp"}"#, "command must"),
        (
            r#"{"id":"job","command":["true"],"cwd":"/tmp"}"#,
            "absolute path",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true"],"cwd":"relative"}"#,
            "absolute path",
        ),
        (
            r#"{"id":"bad id","command":["/usr/bin/true"],"cwd":"/tmp"}"#,
            "identifier",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true","bad\u0000arg"],"cwd":"/tmp"}"#,
            "NUL",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp\u0000bad"}"#,
            "NUL",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp","env":{"BAD-NAME":"secret"}}"#,
            "environment names",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp","env":{"GOOD":"bad\u0000value"}}"#,
            "environment values",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp","timeoutMs":0}"#,
            "timeoutMs",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp","timeoutMs":604800001}"#,
            "timeoutMs",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp","terminationGraceMs":300001}"#,
            "terminationGraceMs",
        ),
        (
            r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp","unknown":1}"#,
            "unknown field",
        ),
    ];
    for (input, expected) in cases {
        let error = serde_json::from_str::<JobSpec>(input).expect_err(input);
        assert!(error.to_string().contains(expected), "{input}: {error}");
    }
    let _: JobSpec = serde_json::from_str(valid).expect("base case remains valid");
    assert!(serde_json::from_str::<JobSpec>(
        r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp","timeoutMs":18446744073709551616}"#
    )
    .is_err());
}

#[test]
fn constructor_accepts_explicit_environment_and_timeout() {
    let env = BTreeMap::from([("MODE".to_owned(), "safe".to_owned())]);
    let job = JobSpec::new(
        JobId::new("worker-task").expect("valid ID"),
        vec!["/usr/bin/printf".to_owned(), "a b; $HOME".to_owned()],
        PathBuf::from("/tmp"),
        env,
        true,
        Some(30_000),
        1_000,
    )
    .expect("valid spec");
    assert!(job.inherit_env());
    assert_eq!(job.timeout_ms(), Some(30_000));
}

#[test]
fn run_contract_serializes_snapshots_and_rejects_stale_or_terminal_regressions() {
    let job: JobSpec =
        serde_json::from_str(r#"{"id":"job","command":["/usr/bin/true"],"cwd":"/tmp"}"#).unwrap();
    let spec = RunSpec::new(
        RunId::new("run-1").unwrap(),
        WorkerId::new("worker-1").unwrap(),
        job.clone(),
    );
    let serialized = serde_json::to_string(&spec).unwrap();
    let decoded: RunSpec = serde_json::from_str(&serialized).unwrap();
    assert_eq!(decoded, spec);
    assert_eq!(decoded.job(), &job);

    let requested = conductor_core::RunObservation::requested(&spec, 100);
    let running = requested
        .next_revision(RunState::Running, Some(110), None, None, None, None)
        .unwrap();
    let mut current = requested.clone();
    assert!(current.apply_newer(running.clone()));
    assert!(!current.apply_newer(requested));
    let terminal = running
        .next_revision(
            RunState::Failed,
            Some(110),
            Some(120),
            Some(2),
            None,
            Some(RunFailureReason::NonZeroExit),
        )
        .unwrap();
    assert!(current.apply_newer(terminal.clone()));
    let regressed = terminal
        .next_revision(RunState::Running, Some(110), None, None, None, None)
        .unwrap();
    assert!(!current.apply_newer(regressed));
    assert_eq!(current.state(), RunState::Failed);
}
