---
type: Component
title: conductor crate overview
description: The executable exposes the agent, operator, enrollment, and token commands and composes the other workspace crates.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/main.rs
    title: CLI, configuration, and composition
  - resource: ../src/operator_cli.rs
    title: Authenticated operator client
  - resource: ../tests/agent.rs
    title: Binary lifecycle integration tests
---

# Responsibility

The `conductor` binary validates configuration, opens durable state, composes
manager/worker/API/transport adapters, and coordinates daemon shutdown. It also
contains operator and bootstrap commands. Policy and persistence remain in
their owning crates.

## Commands

The public commands are `agent`, `operator`, `enroll-worker`, `revoke-worker`,
and `create-operator-token`. The hidden `__internal-launch` command is used by
the process backend to enter configured enforcement before executing a job.
There is no CLI command for registering or starting a one-shot job; use the
HTTP job API described in the repository [README](../../../README.md).

## Source and tests

Configuration structs, validation, role selection, runtime composition, and
enrollment are in `src/main.rs`. The operator request client and its command
surface are in `src/operator_cli.rs`. Real binary lifecycle behavior is
exercised in the repository-level
[`conductor/tests/agent.rs`](../tests/agent.rs).
