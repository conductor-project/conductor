---
type: Component
title: conductor crate overview
description: The executable exposes daemon, job, run, deployment, worker, service discovery, diagnostics, operator context, and bootstrap commands.
status: stable
source_revision: bff6a3d635a1d9599e7185f0a7f484fd48749a0c
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/main.rs
    title: CLI, configuration, and composition
  - resource: ../src/operator_cli.rs
    title: Authenticated operator client
  - resource: ../src/contexts.rs
    title: Named operator contexts
  - resource: ../src/job_cli.rs
    title: Job registration and submission CLI
  - resource: ../src/run_cli.rs
    title: Run lifecycle, wait, and log CLI
  - resource: ../src/deployment_cli.rs
    title: Service deployment lifecycle CLI
  - resource: ../src/worker_cli.rs
    title: Worker inventory and drain CLI
  - resource: ../src/service_cli.rs
    title: Service endpoint lookup and watch CLI
  - resource: ../src/diagnostics_cli.rs
    title: Health, readiness, and metrics CLI
  - resource: ../tests/service_discovery_cli.rs
    title: Service discovery and diagnostics CLI integration evidence
  - resource: ../tests/job_cli.rs
    title: Job CLI integration evidence
  - resource: ../tests/run_cli.rs
    title: Run CLI and documented first-job workflow evidence
  - resource: ../tests/deployment_cli.rs
    title: Deployment lifecycle integration evidence
  - resource: ../tests/worker_cli.rs
    title: Worker CLI integration evidence
  - resource: ../tests/agent.rs
    title: Binary lifecycle integration tests
---

# Responsibility

The `conductor` binary validates configuration, opens durable state, composes
manager/worker/API/transport adapters, and coordinates daemon shutdown. It also
contains operator and bootstrap commands. Policy and persistence remain in
their owning crates.

## Commands

The public commands are `agent`, `context`, `operator`, `job`, `run`,
`deployment`, `worker`, `service`, `diagnostics`, `enroll-worker`,
`revoke-worker`, and `create-operator-token`.
The hidden `__internal-launch` command is used by
the process backend to enter configured enforcement before executing a job.
The repository [README](../../../README.md) contains the copyable CLI workflows
and operator behavior reference.

## Source and tests

Configuration structs, validation, role selection, runtime composition, and
enrollment are in `src/main.rs`. Named context management is in
`src/contexts.rs`; the operator request client and its command surface are in
`src/operator_cli.rs`. Service endpoint lookup and bounded watch behavior are
in `src/service_cli.rs`; health, readiness, and metrics are in
`src/diagnostics_cli.rs`. Real binary CLI behavior is exercised in the linked
integration tests with temporary state and loopback listeners.
