---
type: Runbook
title: Operations and recovery
description: Use health and readiness correctly, plan graceful shutdown, and resolve uncertain work after restart.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
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
  - resource: ../crates/conductor/src/backup_cli.rs
    title: Offline backup and restore commands
  - resource: ../crates/conductor/tests/backup_cli.rs
    title: Backup integrity and recovery-hold coverage
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

The manager schema is version 12 and worker schema version 4 in the reviewed
source. Their migration sequences are independent; see the [manager schema
page](../crates/conductor-manager/okf/desired-state-and-persistence.md) and
[worker persistence page](../crates/conductor-worker/okf/supervision-and-recovery.md).
Startup fails closed on corrupt, inaccessible, or newer-than-supported
schemas.

# Offline backup and restore

The supported backup unit is an offline Conductor snapshot: one or both role
state directories, including the manager/worker SQLite database and, for
workers, retained `runs/` logs. SQLite's online backup API produces a
consistent database image while Conductor's OS-backed state lock proves that
the corresponding daemon role is stopped. Backup creation rejects a busy
manager or worker lock. It copies other regular files below each selected
state directory, but excludes the lock file and SQLite WAL/SHM sidecars because
the database snapshot already contains committed state.

Configuration and identity material may live outside state directories and is
not discovered automatically. Pass each required configuration file,
certificate, private key, CA directory, and long-lived credential with one or
more `--include-path` arguments. Review the manifest's source-to-archive
mapping. Included paths must not overlap a selected manager/worker state
directory. Links, sockets, devices, and other non-regular files are rejected.
The archive stores included items under `included/NNNN`, so a restored
configuration must be updated to point at the restored state and identity
paths.

The `.tar` bundle has a versioned JSON manifest containing the producing
binary version, manager and worker schema markers, every file path, byte size,
and SHA-256 digest. `backup inspect` validates the complete archive without
extracting it. The digest detects accidental corruption but is not a signature
and does not authenticate the archive's origin. Treat the bundle as secret
material: it may contain private identity keys and credentials. Store it with
owner-only access and use an independently approved encryption and off-host
retention process when required by the deployment.

`backup restore` validates paths, entry types, checksums, SQLite integrity, and
schema compatibility before publishing into a new destination directory. It
never overwrites an existing destination. The restored manager and worker
state live under `DESTINATION/manager` and `DESTINATION/worker`; included files
live under `DESTINATION/included/NNNN`. An interrupted or invalid restore is
discarded from its private staging directory and does not replace a usable
installation.

Every restored component creates `.conductor-recovery-required` in its state
directory. Manager, worker, and combined `all` processes refuse startup while
their marker exists. This prevents restored state from silently accepting
stale commands, replaying work, or treating stale authority as current while
workers or workloads may have survived. Before clearing it, stop or fence all
surviving workers and workload processes, compare the restored manager and
worker histories, inspect active and `unknown` runs, check identity/certificate
changes, and resolve external effects using the deployment's recovery process.
Then run `conductor backup recovery-resolve` for every restored state directory
with both explicit confirmation flags. The command records an operator assertion; it does not inspect remote
hosts or verify that processes stopped. Backup age defines the potential data
loss window, not whether execution occurred. Unknown outcomes remain uncertain
until investigated; do not claim they did not run based only on an older
backup.

Example sequence:

```sh
conductor backup create --manager-data-dir /var/lib/conductor/manager \
  --worker-data-dir /var/lib/conductor/worker \
  --include-path /etc/conductor/agent.json \
  --include-path /var/lib/conductor/manager-identity \
  --include-path /etc/conductor/worker-identity.pem \
  --output /var/backups/conductor-maintenance.tar
conductor backup inspect --input /var/backups/conductor-maintenance.tar
conductor backup restore --input /var/backups/conductor-maintenance.tar \
  --destination /var/lib/conductor/restored
conductor backup recovery-resolve \
  --manager-data-dir /var/lib/conductor/restored/manager \
  --worker-data-dir /var/lib/conductor/restored/worker \
  --confirm-workers-fenced --confirm-unknown-outcomes-reviewed
```

The CLI does not automatically rewrite configuration paths, fence remote
workers, terminate workloads, or roll databases backward in place. Keep the
original state untouched until restored state, included credentials, and the
recovery decision have been verified.

# Audit and metrics

Operator mutations record bounded outcomes and resource identifiers without
recording request contents or secrets. Metrics are bounded by label cardinality
and aggregate manager and worker observations. Their presence is operational
telemetry; it is not a service-level objective or a claim that a deployment
meets a particular capacity target. See the [API operations bundle](../crates/conductor-api/okf/authentication-and-bounds.md)
and [manager audit bundle](../crates/conductor-manager/okf/scheduling-and-services.md).
