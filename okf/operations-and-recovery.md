---
type: Runbook
title: Operations and recovery
description: Use health and readiness correctly, plan graceful shutdown, and resolve uncertain work after restart.
status: stable
source_revision: bafe31e2fc936b3290dca62fe5f68258e1c42244
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
  - resource: ../crates/conductor-manager/tests/jobs.rs
    title: Manager migration preservation and failure coverage
  - resource: ../crates/conductor-worker/src/lib.rs
    title: Worker schema inspection and migrations
  - resource: ../crates/conductor-worker/tests/runs.rs
    title: Worker migration preservation and failure coverage
  - resource: ../crates/conductor/src/backup_cli.rs
    title: Offline backup and restore commands
  - resource: ../crates/conductor/src/diagnostics_cli.rs
    title: Bounded remote diagnostics export
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
state directory, but excludes the lock file and SQLite WAL/SHM/rollback-journal
sidecars because the database snapshot already contains a consistent state.

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
extracting it, including SQLite integrity and agreement between each database's
schema marker and the manifest. The digest detects accidental corruption but
is not a signature and does not authenticate the archive's origin. Treat the
bundle as secret material: it may contain private identity keys and credentials.
Store it with owner-only access and use an independently approved encryption
and off-host retention process when required by the deployment.

`backup restore` validates paths, entry types, checksums, SQLite integrity, and
schema compatibility before publishing into a new destination directory. It
never overwrites an existing destination. The restored manager and worker
state live under `DESTINATION/manager` and `DESTINATION/worker`; included files
live under `DESTINATION/included/NNNN`. An interrupted or invalid restore is
discarded from its private staging directory and does not replace a usable
installation. Restored directories use mode `0700` and files use mode `0600`,
owned by the account that ran the restore. If the daemon uses a different
service account, assign the restored state directories to that account before
startup. Set ownership and permissions for included configuration and identity
files separately; do not recursively change the entire destination without
reviewing its contents.

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

## Upgrade compatibility policy

The current source reports Conductor `0.0.1`, HTTP API `v1`, manager-worker
protocol `3`, manager database schema `12`, and worker database schema `4`.
Manager and worker databases have independent schema sequences. The release
policy intends `0.0.x` as the first stable line, then support for the latest
and immediately preceding minor lines for 12 months from each line's first
stable release. No release tag has been published, so there is not yet a
previous supported release to use as a real upgrade baseline. Untagged
development builds are not a supported upgrade source.

| Compatibility boundary | Current source value | Policy |
| --- | --- | --- |
| CLI binary | `0.0.1` | Upgrade all manager and worker binaries together to the same stable release. Untagged builds are unsupported upgrade baselines. |
| HTTP API | `v1` | Routes are versioned under `/v1`. No cross-release client/server window has been validated; use matching release versions. |
| Manager-worker transport | Protocol `3` | Negotiation requires an overlapping version and all required capabilities. An incompatible worker poll returns HTTP `426` with a compatibility diagnostic and the worker exits; install matching Conductor versions. Mixed release versions are not a supported operating mode until separately qualified. |
| Manager database | Schema `12` | Each source migration step is transactional and the current code migrates known earlier markers. This does not prove a real previous-release upgrade. No downgrade is supported after migration. |
| Worker database | Schema `4` | Independently versioned and migrated during worker startup. This does not prove a real previous-release upgrade. No downgrade is supported after migration. |

The migration tests verify preservation for supported older schema fixtures and
rollback of partial schema changes when manager schema `2` or worker schema `1`
migration fails. These synthetic debug-test fixtures establish transaction
behavior only; they are not fixtures from a published release and do not qualify
a release-to-release upgrade.

The binary-level test
`failed_upgrade_restores_matching_backup_and_keeps_recovery_hold_until_confirmed`
exercises a failed candidate preflight, restore from the matching pre-upgrade
manager/worker backup, and startup blocking until the operator confirms worker
fencing and unknown-outcome review. It verifies that state written after the
backup is absent after restore.

`actual_binary_recovery_hold_blocks_restore_while_a_workload_survives_failure`
starts the real combined daemon, records an offline backup, runs a long-lived
workload, and kills the daemon while the workload remains alive. It then plants
a conflicting audit-table schema at marker 11 and starts the real daemon again;
the migration fails on the missing `occurred_at_ms` column and leaves marker 11
unchanged. Preflight still reports that known marker as requiring migration.
The test restores the matching backup and verifies startup remains blocked while
the workload survives. It then explicitly fences the workload, clears the hold,
and confirms the restored job definition is available. The post-backup run is
absent, as expected for this restore point. This test exercises a real
transactional migration failure, workload survival, and the operator fencing
gate; it does not test a separately running remote worker or an upgrade from a
published prior release. The recovery command records operator confirmation and
does not independently inspect external processes.

Local debug verification on source revision
`afcacf3b2c0f277e6b9b2c75f01592a9b1d536a9`:

- `python3 scripts/check-dependency-policy.py` — passed.
- `cargo fmt --all -- --check` — passed.
- `cargo clippy --workspace --locked --all-targets --all-features -- -D warnings` — passed.
- `cargo test --workspace --locked --all-targets --all-features` — 272 tests passed across 27 suites, including the live-workload recovery drill.

This is local debug evidence. No release build/test, stable previous-release
upgrade, package qualification, or recovery drill with a separately running
remote worker is claimed here.

The release policy and the upgrade evidence are separate: the policy defines
which stable lines are intended to receive support, while a supported upgrade
path still requires a published source release and exercised fixtures from
that release. Once `0.0.1` is published as the first stable release, it becomes
the baseline for later `0.0.x` patch upgrades. A later minor release must
exercise the preceding supported line before claiming upgrade compatibility.

Run `conductor upgrade preflight` with the manager and worker data directories
using the candidate binary, after draining work and stopping the daemons. It
opens existing SQLite files read-only and reports whether their schema markers
are understood by that binary. An older marker is reported as
`migration_required`, not as proof that the database is valid or that a release
upgrade is supported. JSON and human output include required maintenance
actions: unsupported markers must be resolved with a compatible binary or a
matching verified backup; supported older markers require a verified joint
backup and an explicit manager/worker restart to apply migration. Preflight
does not run migrations, inspect active remote peers, validate workload
compatibility, or establish package/distribution support. The report's API and
transport versions describe this binary only.

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

# Bounded diagnostics export

`conductor diagnostics --output json export` collects `/healthz`, `/readyz`,
the authorized `/v1/workers` response, and `/v1/metrics` concurrently through
the typed API client. `--deadline-seconds` sets each request's bound from 1 to
300 seconds (default 15). The client also enforces its configured response-byte
cap. Export schema version 1 has per-section `collected` or `unavailable`
status, safe error codes, and an overall `complete` or `partial` result. The
overall status summarizes the four remote API collectors; the storage section
remains explicitly unavailable because the remote API does not expose local
database details.
The top-level `versions` field records the binary, HTTP API, and
manager-worker protocol versions; capability lists remain in the health and
readiness sections.
Worker summaries are limited to 128 items, capability lists to 32 entries,
metric aggregates to 64 samples, findings to 128, and serialized output to
256 KiB. Omitted worker and metric counts are reported.

The worker allowlist contains stable worker IDs, freshness, availability,
degraded/draining state, numeric slot counts, unresolved-run and persistence
counts, and observation timestamps. Labels, inventory, resource-domain data,
service addresses, commands, logs, environment values, credentials, and
request contents are not serialized. Prometheus labels are converted only for
explicitly allowlisted allocation and deployment-blocker metrics. Findings
describe observed states and do not claim an unverified cause. Stale means the
manager has a last-seen timestamp but currently considers the observation
stale; missing freshness metadata is reported as `unknown`.

Remote storage sizes and database paths are intentionally unavailable through
this export. Stop the relevant daemon and use the local `conductor storage
inspect` command for offline storage inspection. This is separate from remote
diagnostics and does not collect or export database contents.

# Audit and metrics

Operator mutations record bounded outcomes and resource identifiers without
recording request contents or secrets. Metrics are bounded by label cardinality
and aggregate manager and worker observations. Their presence is operational
telemetry; it is not a service-level objective or a claim that a deployment
meets a particular capacity target. See the [API operations bundle](../crates/conductor-api/okf/authentication-and-bounds.md)
and [manager audit bundle](../crates/conductor-manager/okf/scheduling-and-services.md).
