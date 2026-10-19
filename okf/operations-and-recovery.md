---
type: Runbook
title: Operations and recovery
description: Use health and readiness correctly, plan graceful shutdown, and resolve uncertain work after restart.
status: stable
source_revision: e7e17917291459f16a2d9a53fcd03a7f29ae3271
source_reviewed_at: 2026-10-19
sources:
  - resource: ../crates/conductor-api/src/lib.rs
    title: Readiness and metrics routes
  - resource: ../crates/conductor-manager/src/lib.rs
    title: Durable run and command state
  - resource: ../crates/conductor-worker/src/lib.rs
    title: Worker recovery and shutdown
  - resource: ../crates/conductor/tests/agent.rs
    title: Lifecycle integration coverage
  - resource: ../crates/conductor/src/upgrade_cli.rs
    title: Read-only upgrade preflight
  - resource: ../crates/conductor-manager/src/lib.rs
    title: Manager schema inspection and migrations
  - resource: ../crates/conductor-worker/src/lib.rs
    title: Worker schema inspection and migrations
  - resource: ../scripts/demo-agent.py
    title: Isolated real-binary lifecycle demo
---

# Health and readiness

`GET /healthz` reports whether the HTTP process responds. `GET /readyz` reports
whether recovery and configured worker sessions permit new work. A combined
worker can be ready while at capacity; run admission performs its own capacity
check. In a manager role, readiness can remain degraded until required workers
have fresh authenticated sessions.

Poll readiness with a bounded deadline and treat failure to become ready as an
operational error. Do not use a fixed sleep as readiness evidence. The README
contains a bounded local polling example.

The repository also has a longer, isolated real-binary demo in
[`scripts/demo-agent.py`](../scripts/demo-agent.py). It allocates temporary
manager/worker state and an ephemeral loopback port, then checks successful
and concurrent runs, idempotent start retry, stdout/stderr, output truncation
and retention, stop, command and launch failures, timeout, capacity rejection,
graceful shutdown, restart persistence, and logs. An opt-in `--include-crash`
scenario terminates its own recorded fixture process, verifies cleanup, and
resolves the recovered run as `abandoned`. This describes the script's source,
not a run performed for this documentation change. For local use, point it at
an already-built debug binary; release builds and tests run post-merge in CI.

# Graceful shutdown

Ctrl-C and SIGTERM stop new work, request bounded worker/process cleanup, and
close listeners. A job stop is separate from daemon shutdown. The process
backend sends SIGTERM to the process group, waits its configured grace period,
then force-kills remaining ordinary group members. A process can deliberately
leave its group; this is lifecycle cleanup, not containment. systemd's example
adds its own control-group cleanup boundary.

# Persistence and uncertain outcomes

Manager state and worker observations are stored separately. On restart,
Conductor reconciles them before accepting runs. An active process whose result
cannot be confirmed becomes `unknown`; its job/capacity reservation remains,
and the process is not automatically launched again. Operators must determine
whether external cleanup is needed before using the authenticated resolution
API. After independently verifying cleanup, submit
`{"resolution":"abandon","reason":"..."}` to
`POST /v1/runs/{id}/resolve`. The reason is required and at most 512 bytes; an
ambiguous retry must use the same reason. The manager records that operator
statement but does not verify cleanup itself. Abandonment is administrative
closure, not success; it releases the held reservation. Do not report an
`unknown` or `abandoned` run as successful. See the [API route guide](../crates/conductor-api/okf/routes-and-contracts.md#resolving-an-uncertain-run)
for the request and conflict behavior.

Back up manager SQLite state with the daemon stopped. Startup fails closed on
corrupt, inaccessible, or newer-than-supported database schemas. The current
source declares manager schema version 12 and worker schema version 4; see the
[schema migration pages](../crates/conductor-manager/okf/desired-state-and-persistence.md)
and [worker persistence page](../crates/conductor-worker/okf/supervision-and-recovery.md).

## Upgrade compatibility policy

The current source reports Conductor `0.1.0`, HTTP API `v1`, manager-worker
protocol `3`, manager database schema `12`, and worker database schema `4`.
Manager and worker databases have independent schema sequences. During a
planned upgrade, stop all manager and worker processes, make a consistent
backup, and upgrade their binaries together to the same Conductor version.
Mixed-version operation is unsupported; no zero-downtime or automatic rollback
is promised. Database migrations run only when a role starts. A downgrade is
not supported after migration; restore the matching pre-upgrade backup before
starting an older binary.

| Compatibility boundary | Current source value | Policy |
| --- | --- | --- |
| CLI binary | `0.1.0` | Upgrade manager and worker binaries together; no prior release is supported yet. |
| HTTP API | `v1` | The operator client uses versioned `/v1` routes; no cross-release compatibility window is published. |
| Manager-worker transport | Protocol `3` | Negotiation checks protocol overlap, but supported mixed binary versions are not established; use matching releases. |
| Manager database | Schema `12` | Forward migrations are transactional; no downgrade is supported. |
| Worker database | Schema `4` | Independently versioned and migrated on worker startup; no downgrade is supported. |

There are no published version tags yet, so the project has no previous
supported release baseline or validated upgrade window. Pre-release snapshots
are not a supported upgrade source. The first supported release must establish
the baseline and provide real prior-release fixtures before a later release can
claim upgrade compatibility. Until then, schema migration code coverage is not
evidence of a supported release-to-release upgrade.

Run `conductor upgrade preflight` with the manager and worker data directories
after stopping the daemon. It opens existing SQLite files read-only and reports
whether their schema markers are understood by the current binary. It does not
run migrations, inspect active remote peers, validate workload compatibility,
or establish package/distribution support. The report's API and transport
versions describe this binary only.

# Audit and metrics

Operator mutations record bounded outcomes and resource identifiers without
recording request contents or secrets. Metrics are bounded by label cardinality
and aggregate manager and worker observations. Their presence is operational
telemetry; it is not a service-level objective or a claim that a deployment
meets a particular capacity target. See the [API operations bundle](../crates/conductor-api/okf/authentication-and-bounds.md)
and [manager audit bundle](../crates/conductor-manager/okf/scheduling-and-services.md).
