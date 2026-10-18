---
type: Verification Record
title: Local MVP3 CLI workflow evidence
description: Debug CLI workflows are exercised against real local agents on Apple silicon macOS, with portable integration coverage in the conductor crate.
status: stable
source_revision: bff6a3d635a1d9599e7185f0a7f484fd48749a0c
source_reviewed_at: 2026-10-19
sources:
  - resource: ../README.md
    title: User CLI walkthrough and operator behavior reference
  - resource: ../crates/conductor/tests/run_cli.rs
    title: First-job, JSON run ID, wait, logs, and inspect integration test
  - resource: ../crates/conductor/tests/job_cli.rs
    title: Job submission, validation, conflict, and timeout integration tests
  - resource: ../crates/conductor/tests/deployment_cli.rs
    title: Checked-in service deployment, rollback, scale, and wait integration tests
  - resource: ../crates/conductor/tests/worker_cli.rs
    title: Worker inventory and drain integration tests
  - resource: ../crates/conductor/tests/service_discovery_cli.rs
    title: Service endpoint and diagnostics integration tests
---

# Environment

- Host: macOS 27.0.0, Darwin `arm64` (Apple silicon).
- Toolchain: `rustc 1.99.1 (58cf8f9c3 2026-10-13)` and Cargo 1.99.1.
- Profile: debug only. No release build or test was run.
- Integration fixtures use unique temporary state and ephemeral loopback
  listeners. Tests bound readiness polling and clean up child agents on exit.
- No privileged fixtures or GitHub workflow changes are part of this evidence.

# Commands and observations

The README walkthrough uses the checked-in `examples/local.json` configuration
and checked-in job/service JSON files. Its first-job flow submits the JSON job,
copies the JSON `runId`, waits for the run, reads the raw stdout log bytes, and
inspects the terminal run state. The observed log payload is
`hello conductor\n`.

The integration test
`checked_in_first_job_example_submits_waits_and_exposes_logs` was run with:

```sh
cargo test -p conductor --test run_cli checked_in_first_job_example_submits_waits_and_exposes_logs -- --exact
```

It passed against a real debug `conductor` agent using temporary manager and
worker state. The test consumes the JSON submission response to obtain the
run ID and verifies the separate wait, raw log, and inspect commands.

Other portable integration coverage in the same crate exercises failed and
timed-out runs, interruption, invalid strict JSON, duplicate job conflicts,
server loss, pagination, deployment generation conflicts, deployment waits,
worker drains, service discovery, readiness, and metrics. The focused checks
for these were included in the full locked workspace test run. Final local
debug checks were:

```sh
cargo +1.99.1 build --locked -p conductor
python3 scripts/check-dependency-policy.py
cargo fmt --all -- --check
cargo clippy --workspace --locked --all-targets --all-features -- -D warnings
cargo test --workspace --locked --all-targets --all-features -- --test-threads=1
```

The build, dependency policy, formatting, and Clippy checks passed. The full
workspace test command passed 247 tests across 22 suites in 65.74 seconds.
Linux and macOS debug CI passed for the service and diagnostics CLI changes in
PR #145. That CI does not perform release builds or privileged multi-host
qualification.

# Scope and limits

This record describes local debug execution on Apple silicon macOS plus the
repository's Linux/macOS debug CI. It does not establish remote-host reachability
of advertised service addresses, production proxy configuration, or multi-host
service routing. The current service watch API has no cursor or version
semantics; the CLI can retry transient errors only within its bounded deadline.
