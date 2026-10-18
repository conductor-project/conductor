---
type: Component
title: conductor-core overview
description: Stable identifiers and transport-neutral contracts define the seams between manager, worker, process, API, and transport crates.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Shared types and invariants
  - resource: ../src/execution.rs
    title: Job, revision, and deployment models
  - resource: ../src/protocol.rs
    title: Protocol version types
  - resource: ../tests/job_spec.rs
    title: Job contract tests
  - resource: ../tests/execution_model.rs
    title: Execution-model tests
  - resource: ../tests/protocol.rs
    title: Protocol tests
---

# Responsibility

`conductor-core` contains shared IDs, validated models, commands, reports,
worker status, and protocol envelopes. It does not own HTTP, persistence,
process handles, or scheduling side effects. Other crates depend inward on
these contracts to avoid sharing implementation state.

# Compatibility

Stable IDs identify logical jobs, runs, workers, and commands. Worker
incarnations and protocol ranges are separate compatibility/freshness values;
they must not be confused with logical IDs. Resource and readiness models
describe accepted requirements and observations, not proof that a host kernel
will enforce them.

See the [repository architecture](../../../okf/architecture.md) for crate
ownership and the following concepts for field-level invariants.
