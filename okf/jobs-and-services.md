---
type: Reference
title: Job submission and managed services
description: Job definitions produce explicit runs; service revisions and deployments track desired replicas separately.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../crates/conductor-api/src/lib.rs
    title: HTTP routes and mappings
  - resource: ../crates/conductor-manager/src/lib.rs
    title: Manager operations and persistence
  - resource: ../crates/conductor-core/src/execution.rs
    title: Execution and service models
  - resource: ../examples/job.json
    title: One-shot job example
  - resource: ../crates/conductor/tests/agent.rs
    title: Real daemon integration tests
---

# One-shot jobs

Register a validated immutable definition with `POST /v1/jobs`. Start creates
a distinct run with `POST /v1/jobs/{id}/runs`; capture the returned `runId` and
inspect `GET /v1/runs/{id}` until it reaches a terminal state. Read output from
`GET /v1/runs/{id}/logs?stream=stdout` or `stderr`. The [README walkthrough](../README.md)
shows the complete first-run request sequence.

A command is an executable plus an argument array. No shell is invoked
implicitly. The executable and `cwd` must exist on the worker. `env` is
explicit; `inheritEnv: false` avoids inheriting the daemon environment. A
definition may have a monotonic execution timeout and termination grace period.

IDs are durable: deleting a job reserves its ID, and it cannot be registered
again as a different definition. Choose a new job ID for each registration.
An active or unresolved run blocks a concurrent run for the same one-shot job.
`Idempotency-Key` can recover an accepted start after an ambiguous client
timeout, but it does not guarantee exactly-once external side effects.

## Run lifecycle

The run can be `requested`, `starting`, `running`, `stopping`, or terminal:
`succeeded`, `failed`, `cancelled`, `timed_out`, or `abandoned`. `unknown` is
unresolved, not terminal: durable recovery cannot determine whether execution
completed, so it retains its reservation until an operator resolves it and is
never automatically replayed. `abandoned` is an explicit administrative
resolution, not success.

# Managed services

The service API separates immutable execution revisions from mutable desired
deployment state. Create a revision under `/v1/jobs/{id}/revisions`, then
create or mutate `/v1/jobs/{id}/deployment`. Scaling, suspend/resume, rollback,
and deletion use generation conditions and idempotency controls as documented
by the handlers. Service requests include positive CPU and memory reservations;
resource requirements and readiness probes belong to the revision contract.

The manager assigns service attempts, tracks their observations, reconciles
desired replicas, and publishes catalog endpoints. Worker readiness probes
gate endpoint availability. Endpoint data and status are observations with a
convergence delay; a response is not proof that every client cache has
converged. See the [manager service bundle](../crates/conductor-manager/okf/scheduling-and-services.md),
[worker probe bundle](../crates/conductor-worker/okf/deadlines-readiness-and-logs.md),
and [API route bundle](../crates/conductor-api/okf/routes-and-contracts.md).

# Limits and references

The local manager accepts at most 1,000 active job definitions. API pagination,
request bounds, ETags, detailed errors, and service state transitions are
specified in the API bundle and exercised by crate tests. Historical MVP2 docs
are retained as compatibility pointers in `docs/`; see the
[migration map](migration-map.md).
