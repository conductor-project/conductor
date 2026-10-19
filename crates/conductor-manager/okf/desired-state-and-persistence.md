---
type: Reference
title: Manager desired state and persistence
description: SQLite stores immutable job/revision records, mutable deployment state, run reservations, worker records, commands, allocations, and audit.
status: stable
source_revision: e7e17917291459f16a2d9a53fcd03a7f29ae3271
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Schema version and transaction setup
  - resource: ../src/structs/sqlite_job_repository.rs
    title: Manager repository operations
  - resource: ../tests/jobs.rs
    title: Persistence and restart tests
  - resource: ../../conductor/src/upgrade_cli.rs
    title: Read-only schema marker preflight
---

# Durable state

The manager schema includes job definitions and reserved IDs, run observations
and idempotency records, worker identities/inventory, command outbox rows,
execution revisions, deployments, scheduling sequence, service instances and
allocations, service attempts/catalog ownership, worker drains, and audit
events. The exact tables evolve through transactional migrations. Current
`SCHEMA_VERSION` is **12** in `src/lib.rs`; this number describes the manager DB
only and must not be reused for worker state.

SQLite uses WAL mode and full synchronization. Startup migrates supported older
schemas transactionally, and fails closed when a database is corrupt,
inaccessible, or newer than the supported schema. Back up a consistent DB/WAL
state with the daemon stopped.

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
