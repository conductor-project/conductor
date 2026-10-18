---
type: Reference
title: Manager scheduling, reconciliation, and services
description: The manager maps desired runs and service replicas to worker ports, records allocations, reconciles observations, and maintains service endpoints.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/structs/job_service.rs
    title: Assignment, runs, services, rollouts, and worker drains
  - resource: ../src/structs/sqlite_job_repository.rs
    title: Durable placement and catalog state
  - resource: ../src/structs/allocation_record.rs
    title: Allocation record
  - resource: ../src/structs/service_attempt_record.rs
    title: Attempt record
  - resource: ../tests/jobs.rs
    title: Scheduling and rollout tests
---

# One-shot scheduling

The manager validates job registration and run admission, selects a worker
port, and persists a run reservation before dispatch. Capacity limits and
resource-domain budgets constrain assignment. A run in unresolved state keeps
its reservation; the manager does not automatically replay work after an
ambiguous restart.

# Service desired state

An immutable revision captures execution requirements. A deployment stores
mutable desired replica count and lifecycle/rollout state. Reconciliation
compares desired replicas with current attempts, schedules missing attempts,
requests stops for obsolete attempts, and records progress. Stop completion
and readiness are asynchronous worker observations, so rollout accounting
must include in-progress operations, not only immediate terminal responses.

For revision updates, the deployment retains the previous revision in durable
rollout state. `maxSurge` is currently zero; `maxUnavailable` bounds the
readiness-gated replacement wave, and a running process passes the gate when
no readiness probe is declared. Progress-deadline expiry records a visible
rollout failure and recovery decision instead of silently claiming
convergence. Stable service-instance IDs are retained for replica slots that
remain desired across scaling and revision changes.

Each assignment records an allocation and scheduler reservation. Resource
requests are distinct from execution limits: shared-host resource-domain
budgets account CPU and memory for placement but are not kernel enforcement.
Declared TCP service ports receive leases from the configured network-domain
range. A logical lease can survive restart and uncertain run state; the manager
does not own the socket. A workload can still lose an external bind race and
must report failure if it cannot bind its assigned port. The worker injects
`CONDUCTOR_PORT_<NAME>` variables for declared ports.

The manager also maintains port leases, service endpoint ownership, and a
catalog view. Catalog updates describe observed ready endpoints and can lag
state changes. API and DNS behavior is documented in the independent API
bundle. Do not infer routed multi-host qualification from local adapter tests.

# Draining and audit

Worker drains suppress new assignments and track existing work until
completion or operator action. Manager audit records bounded operation
outcomes and stable resource identifiers; it does not store secrets or request
bodies. See `tests/jobs.rs` for manager-level tests and the API bundle for
authorization and HTTP contracts.
