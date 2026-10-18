---
type: Component
title: conductor crate overview
description: The executable exposes agent, operator context, operator, service discovery, diagnostics, enrollment, and token commands and composes the other workspace crates.
status: stable
source_revision: df48fc16415e79fcb9514047fa0ff477587da1dd
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/main.rs
    title: CLI, configuration, and composition
  - resource: ../src/operator_cli.rs
    title: Authenticated operator client
  - resource: ../src/contexts.rs
    title: Named operator contexts
  - resource: ../src/service_cli.rs
    title: Service endpoint lookup and watch CLI
  - resource: ../src/diagnostics_cli.rs
    title: Health, readiness, and metrics CLI
  - resource: ../tests/service_discovery_cli.rs
    title: Service discovery and diagnostics CLI integration evidence
  - resource: ../tests/agent.rs
    title: Binary lifecycle integration tests
---

# Responsibility

The `conductor` binary validates configuration, opens durable state, composes
manager/worker/API/transport adapters, and coordinates daemon shutdown. It also
contains operator and bootstrap commands. Policy and persistence remain in
their owning crates.

## Commands

The public commands are `agent`, `context`, `operator`, `service`,
`diagnostics`, `enroll-worker`, `revoke-worker`, and `create-operator-token`.
The hidden `__internal-launch` command is used by
the process backend to enter configured enforcement before executing a job.
There is no CLI command for registering or starting a one-shot job; use the
HTTP job API described in the repository [README](../../../README.md).

## Source and tests

Configuration structs, validation, role selection, runtime composition, and
enrollment are in `src/main.rs`. Named context management is in
`src/contexts.rs`; the operator request client and its command surface are in
`src/operator_cli.rs`. Service endpoint lookup and bounded watch behavior are
in `src/service_cli.rs`; health, readiness, and metrics are in
`src/diagnostics_cli.rs`. Real binary lifecycle behavior is
exercised in the repository-level
[`conductor/tests/agent.rs`](../tests/agent.rs).
