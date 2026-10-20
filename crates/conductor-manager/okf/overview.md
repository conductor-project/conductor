---
type: Component
title: conductor-manager overview
description: Manager operations own desired state, durable scheduling decisions, worker-facing commands, and reconciliation.
status: stable
source_revision: de95885
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Manager contracts and service facade
  - resource: ../src/structs/job_service.rs
    title: Job, run, and service operations
  - resource: ../src/structs/sqlite_job_repository.rs
    title: SQLite implementation
  - resource: ../tests/jobs.rs
    title: Manager behavior tests
---

# Responsibility

The manager owns job definitions, immutable revisions, deployment intent,
worker identity and scheduling state, durable run reservations, command
delivery, service attempts, allocations, audit, and reconciliation. It
communicates through `WorkerPort`; it does not depend on worker or process
implementation crates.

# Persistence boundary

SQLite repository methods serialize manager transactions and persist the
manager's view of state. Worker observations are received through the port and
reconciled; worker-local accepted commands, run logs, and process state remain
owned by the worker. The manager DB schema version in this reviewed source is
14. See [persistence and migrations](desired-state-and-persistence.md).

# Evidence

`tests/jobs.rs` covers manager state transitions, persistence, scheduling,
reconciliation, and services through worker-port doubles. The repository-level
binary tests cover composition with actual local processes. These sources
should be used together; a manager test double does not prove remote transport
or operating-system behavior.
