---
type: Reference
title: Core execution, resources, and protocol
description: Service and run contracts carry immutable requirements, versioned worker command envelopes, and observations across trust boundaries.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/execution.rs
    title: Revision, deployment, resources, and readiness contracts
  - resource: ../src/inventory.rs
    title: Worker inventory contracts
  - resource: ../src/protocol.rs
    title: Protocol offer and version range
  - resource: ../src/structs/command_envelope.rs
    title: Durable worker command identity
  - resource: ../tests/execution_model.rs
    title: Execution-model invariants
  - resource: ../tests/protocol.rs
    title: Protocol compatibility tests
---

# Immutable execution requirements

Service execution revisions carry the command, resources, platform
requirements, readiness behavior, and executable integrity settings used by
attempts. Deployment state separately expresses mutable replica and lifecycle
intent. The two objects have distinct generations and transition rules.

Resource requirements and host inventory support manager placement decisions.
Worker capacity and host resource domains are scheduling data, not kernel
quotas. A platform requirement can reject incompatible workers before launch;
the native OS loader remains authoritative on binary format and CPU slice.

# Commands and reports

Worker commands have stable command IDs and carry worker/attempt context.
Observations report current run or service-attempt state. Protocol offers and
version ranges negotiate a compatible wire version; they do not imply all
future optional semantics are supported. Managers and workers must reject
incompatible protocol offers and stale worker incarnations rather than silently
interpreting an envelope under a different contract.

# Invariants and evidence

Tests in `tests/execution_model.rs`, `tests/worker_inventory.rs`, and
`tests/protocol.rs` exercise the contract-level invariants. Runtime durability
and delivery properties belong to manager, worker, and transport bundles.
