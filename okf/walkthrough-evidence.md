---
type: Verification Record
title: Local README walkthrough evidence
description: The documented first-job, log-readback, cancellation, and shutdown flow completed against the debug daemon on Apple silicon macOS.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../README.md
    title: User walkthrough
  - resource: ../crates/conductor/tests/agent.rs
    title: Binary integration tests
  - resource: ../crates/conductor-api/src/lib.rs
    title: HTTP route implementation
---

# Environment

- Repository source revision exercised: `f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a`.
- Host: macOS 27.0.1, Darwin `arm64` (Apple silicon).
- Toolchain: `rustc 1.99.1 (58cf8f9c3 2026-10-13)` and Cargo 1.99.1.
- Build profile: debug (`target/debug/conductor`). No release build or test ran.
- State: unique temporary directory for configuration, manager DB, worker DB,
  job payloads, and daemon output; removed after validation.
- Listener: ephemeral port `57177` bound to `127.0.0.1`; no external network
  access.

# Commands and observations

Build command:

```sh
cargo +1.99.1 build --locked -p conductor
```

The debug daemon was started with `agent --config <temporary-file>` and an
`all`-role JSON config using separate temporary manager/worker directories,
local API access, and a loopback listener. Readiness requests were polled every
100 ms for at most 5 seconds, with a 1-second per-request timeout. The job was
registered and started through the documented HTTP API; the initial run start
included a saved `Idempotency-Key`, and its returned `runId` was polled every
100 ms for at most 30 seconds with a 1-second per-request timeout. The terminal
state was `succeeded`. The stdout API returned base64 output which decoded
exactly to `hello conductor\n`.

A distinct `/bin/sleep 30` job was registered and started. The HTTP stop route
was called and the run was polled every 100 ms for at most 10 seconds; the
terminal state was `cancelled`. The cancelled definition was deleted before
daemon shutdown. Sending SIGINT to the daemon then produced a successful
process exit. The observed health and readiness endpoints returned success
before job submission. Submission, stop, log, and deletion requests used
bounded client timeouts.

# Scope

This is one local runtime walkthrough on Apple silicon macOS, not a Linux
qualification or independent-host deployment test. It does not exercise
operator-token proxying, service deployment, cgroup v2, Landlock, or routed
multi-host DNS. The README commands use POSIX shell syntax, `curl`, `jq`, and
Python base64 support, but this walkthrough was not run on Linux. Linux CI
covers repository builds and tests, not execution of this exact README
sequence. The demo's fixed example port is 8080; this evidence run used an
ephemeral loopback port to avoid conflicts.
