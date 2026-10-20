---
type: Reference
title: Manager desired state and persistence
description: SQLite stores immutable job/revision records, mutable deployment state, run reservations, worker records, commands, allocations, audit, and overlay membership.
status: stable
source_revision: 28dd056a30bf9ce7ab11165548ae17b66c473713
source_reviewed_at: 2026-10-20
sources:
  - resource: ../src/lib.rs
    title: Schema version and transaction setup
  - resource: ../src/structs/sqlite_job_repository.rs
    title: Manager repository operations
  - resource: ../tests/jobs.rs
    title: Persistence and restart tests
  - resource: ../../conductor/src/upgrade_cli.rs
    title: Read-only schema marker preflight
  - resource: ../../conductor/src/backup_cli.rs
    title: Offline SQLite snapshot and restore validation
  - resource: ../../conductor/src/storage_cli.rs
    title: Read-only storage inspection
  - resource: ../../conductor/tests/backup_cli.rs
    title: Backup round-trip and recovery-hold tests
---

# Durable state

The manager schema includes job definitions and reserved IDs, run observations
and idempotency records, worker identities/inventory, command outbox rows,
execution revisions, deployments, scheduling sequence, service instances and
allocations, service attempts/catalog ownership, worker drains, and audit
events. The exact tables evolve through transactional migrations. Current
`SCHEMA_VERSION` is **14** in `src/lib.rs`; this number describes the manager DB
only and must not be reused for worker state.

SQLite uses WAL mode and full synchronization. Startup migrates supported older
schemas transactionally, and fails closed when a database is corrupt,
inaccessible, or newer than the supported schema. Use
`conductor backup create --manager-data-dir ...` with the daemon stopped to
create a consistent SQLite snapshot. The command takes the manager state lock,
uses SQLite's online backup API, and checks snapshot integrity and schema
version. It does not change the source database.

## Overlay peer identity groundwork

Schema 14 adds manager-owned overlay network domains, worker endpoint IDs,
durable IPv4 assignments, retired endpoint tombstones, and bounded latest
worker observations. Domain creation accepts only canonical private IPv4
CIDRs with disjoint worker and service pools; ranges cannot overlap pools in
other domains. Worker allocations are serialized in an immediate SQLite
transaction and capped at 128 peers per domain. The manager never stores an
Iroh private key.

Each endpoint ID is bound to one actively enrolled worker in a domain. Key
replacement increments the worker peer generation while preserving its
address. Revocation and lease expiry retire the endpoint ID and quarantine its
address until the old lease plus 30 seconds of clock-skew allowance elapses.
Retired endpoint IDs remain tombstoned after address reuse. A worker certificate
revocation also quarantines all of that worker's overlay memberships. Config
snapshots include a schema version and only active, unexpired peers; each
snapshot is bounded to the domain's 128-peer limit.

Schema 14 persists each worker's bounded direct UDP candidate advertisement.
Workers submit candidate updates over mTLS with their current peer generation;
the manager rejects stale generations and invalid or duplicate addresses.
Changed candidates increment the domain's peer-map generation, and the
acknowledgement returns that generation and candidate expiry. Candidates are
reachability hints, not identity or authorization, and expire no later than the
peer authorization lease. Peer snapshots distribute the candidates to other
authorized workers.

These rows are in the normal manager SQLite snapshot and restore unit. No
network-domain deletion operation is exposed; address pools and replay
tombstones therefore remain reserved. The peer-list diagnostic joins that
desired state with the latest worker report and marks it fresh only when its
timestamp is nonnegative, no later than manager time, and at most 60 seconds
old, and its network generation matches desired state. The diagnostic includes
the report generation so older applies remain visible as stale. A bounded
runtime payload also records the helper's interface address/MTU, installed
Conductor routes, active direct Iroh links, handshake timestamps, and transport
UDP byte counters (including protocol overhead, not application payload).
Runtime payloads are capped at 64 KiB, validated against the
authenticated worker assignment, and stored with the report. They are
worker-reported state, not a route probe or proof of peer/application
reachability. The separate latest directional application probe is submitted
over worker mTLS and stored independently. It is fresh only for the current
source peer and network generations, an active target assignment with the same
address, and an observation no more than 60 seconds old. Any fresh HTTP status
verifies that an application response returned; the separate outcome records
2xx success versus non-2xx or transport failure. Local `conductor overlay
probe` results stay local unless `--config` is supplied, in which case the CLI
reports the bounded response through the worker's authenticated manager
transport. An unavailable manager does not turn a local result into manager
evidence.

## Run-history retention

The manager keeps the newest `completed_run_retention` eligible terminal runs
(10,000 by default; configurable range 1 through 1,000,000). A cleanup pass
deletes at most 128 runs. Cleanup runs once at manager startup and after a
terminal observation is persisted; `moreEligible` progress is written as a
structured manager event. It removes acknowledged command-outbox rows only
with their deleted run.

Eligibility requires a terminal run with no idempotency key, pending command,
current service-attempt reference, or service-port lease. Active and unknown
runs, idempotency-keyed runs, and runs with those references remain stored.
Run IDs use a monotonic persisted sequence that cleanup never decrements, so
removed IDs stay reserved. Audit events and execution-idempotency records are
permanent; this policy does not globally bound those tables. Worker command and
report history remains worker-owned and is not deleted by manager cleanup.
Acknowledged manager commands are deleted only with their eligible run;
pending commands stay protected. Job ID reservations, job revisions, and
current deployment state are retained by their existing contracts. Service
attempts and port leases stay while referenced by current state. This change
does not set expiry windows for any idempotency or replay evidence.

`conductor storage inspect --manager-data-dir ...` reports bounded manager counts,
eligible rows beyond the configured target, permanent metadata counts, SQLite
page count/freelist, and manager database/WAL/shared-memory file sizes. The
inspection does not migrate or change database contents. SQLite may create its
WAL coordination sidecar files when opening a read-only connection. Deleting
rows places pages on SQLite's freelist but does not shrink the database file;
SQLite reuses those pages on later writes. Physical compaction is not performed
automatically. Add `--worker-data-dir ...` to inspect worker run/log/command/
report-sequence counts and SQLite file/page measurements. The worker command
and report records currently have no expiry or compaction policy; worker log
files have the separate bounded policy in the worker knowledge page. The log
byte figure is an upper bound derived from unpruned log pairs, including active
runs, and the configured per-stream cap; it does not scan log files or report
their exact aggregate byte size.

The `conductor upgrade preflight` command reads only `PRAGMA user_version` via
a read-only connection. It does not validate table structure or prove that a
database is recoverable; successful marker inspection is not a substitute for
a backup or a migration test.

Before upgrading, stop the daemon and take a consistent backup of manager
state and worker state/logs. Start only binaries whose worker transport
protocols overlap. There is no automatic schema downgrade: after a database
migrates, an older binary that cannot read that schema must not be pointed at
it. Restore the matching pre-upgrade database backup before rolling binaries
back; doing so discards writes made after that backup. Confirm jobs, runs, and
retained logs before resuming traffic.

Restoring manager state places the database in a new destination and creates a
recovery hold. Manager and `all` startup remain blocked until an operator has
fenced surviving workers/workloads, reviewed active and unknown runs, checked
identity changes, and explicitly runs `conductor backup recovery-resolve`.
When worker state was also restored, clear both recovery holds together. The
marker prevents automatic replay from a stale manager snapshot; the CLI's
confirmation is an operator assertion, not remote process verification.

# Transactions and ownership

Manager operations make durable intent and reservations before dispatching
commands through `WorkerPort`. A delivery failure does not mean the persisted
intent vanished; a later reconciliation may retry or repair it according to
the operation. The manager stores stable identities and observations, not
worker PIDs or log file paths.

The per-worker command outbox is bounded: at most 32 pending commands total,
with at most 24 non-control commands. This retains capacity for stop or
resolution control commands when ordinary delivery is saturated. Command IDs
remain stable through delivery/replay and acknowledgement advances durable
delivery state; the transport and worker bundles describe the other side of
that boundary.

## Evidence

`tests/jobs.rs` exercises reopening the database, version checks, API-facing
manager transactions, and recovery. Source review of schema initialization
and repository methods does not itself constitute a migration test for every
historical user database.
