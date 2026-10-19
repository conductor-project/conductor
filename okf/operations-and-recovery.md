---
type: Runbook
title: Operations and recovery
description: Use health and readiness correctly, plan graceful shutdown, and resolve uncertain work after restart.
status: stable
source_revision: 636059dbca9a89b34a6133c701a9df5a121c8a4b
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
Manager and worker databases have independent schema sequences. The release
policy intends `0.1.x` as the first stable line, then support for the latest
and immediately preceding minor lines for 12 months from each line's first
stable release. No release tag has been published, so there is not yet a
previous supported release to use as a real upgrade baseline. Untagged
development builds are not a supported upgrade source.

| Compatibility boundary | Current source value | Policy |
| --- | --- | --- |
| CLI binary | `0.1.0` | Upgrade all manager and worker binaries together to the same stable release. Untagged builds are unsupported upgrade baselines. |
| HTTP API | `v1` | Routes are versioned under `/v1`. No cross-release client/server window has been validated; use matching release versions. |
| Manager-worker transport | Protocol `3` | Protocol negotiation checks version overlap and required capabilities. Mixed Conductor release versions are not a supported operating mode until separately qualified. |
| Manager database | Schema `12` | Each source migration step is transactional and the current code migrates known earlier markers. This does not prove a real previous-release upgrade. No downgrade is supported after migration. |
| Worker database | Schema `4` | Independently versioned and migrated during worker startup. This does not prove a real previous-release upgrade. No downgrade is supported after migration. |

The release policy and the upgrade evidence are separate: the policy defines
which stable lines are intended to receive support, while a supported upgrade
path still requires a published source release and exercised fixtures from
that release. Once `0.1.0` is published as the first stable release, it becomes
the baseline for later `0.1.x` patch upgrades. A later minor release must
exercise the preceding supported line before claiming upgrade compatibility.

Run `conductor upgrade preflight` with the manager and worker data directories
using the candidate binary, after draining work and stopping the daemons. It
opens existing SQLite files read-only and reports whether their schema markers
are understood by that binary. An older marker is reported as
`migration_required`, not as proof that the database is valid or that a release
upgrade is supported. It does not run migrations, inspect active remote peers,
validate workload compatibility, or establish package/distribution support.
The report's API and transport versions describe this binary only.

### Planned upgrade and rollback sequence

1. Read the release notes and confirm the source and target are inside the
   published support window. Do not upgrade from an untagged development build.
2. Drain each worker through the manager and wait for drain completion. Inspect
   unresolved runs and service deployments; do not treat an unknown outcome as
   safe to replay.
3. Stop workers, then stop the manager after it has stopped assigning work.
   Include manager and worker databases, logs, configuration, and identity
   material in one consistent backup set. Verify the backup before proceeding.
4. Run the candidate binary's `upgrade preflight` against both stopped data
   directories. Resolve an unsupported marker or inspection error before
   replacing binaries. A `migration_required` result means only that the
   current source has a migration path for that marker.
5. Install matching manager and worker binaries without starting them
   implicitly. Start the manager first, confirm readiness, then start workers.
   Inspect run, deployment, and recovery state before resuming normal work.

Before any migration starts, rollback may use the previous binary with the
unchanged databases. Once migration has started, do not downgrade the binary or
database. Stop the processes, restore the matching manager/worker backup set
and its configuration/identity, then fence surviving workers and workloads and
resolve unknown outcomes before scheduling resumes. Restoring a backup can
discard writes made after it; follow the offline backup and restore procedure
from #133, including its fencing and recovery-hold requirements. Package
scripts do not restart services, so the operator controls the point where a
migration can begin. Package lifecycle guidance must keep this boundary
explicit.

# Audit and metrics

Operator mutations record bounded outcomes and resource identifiers without
recording request contents or secrets. Metrics are bounded by label cardinality
and aggregate manager and worker observations. Their presence is operational
telemetry; it is not a service-level objective or a claim that a deployment
meets a particular capacity target. See the [API operations bundle](../crates/conductor-api/okf/authentication-and-bounds.md)
and [manager audit bundle](../crates/conductor-manager/okf/scheduling-and-services.md).
