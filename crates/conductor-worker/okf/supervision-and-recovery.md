---
type: Reference
title: Worker supervision and recovery
description: The worker persists command acceptance and observations around run supervision, then reconciles uncertain work on restart.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Startup recovery and supervision
  - resource: ../src/structs/worker_service.rs
    title: Worker database and command handlers
  - resource: ../src/structs/run_supervision.rs
    title: Per-run supervision state
  - resource: ../tests/runs.rs
    title: Recovery and idempotency tests
  - resource: ../../conductor/src/backup_cli.rs
    title: Worker database and retained-log backup
  - resource: ../../conductor/tests/backup_cli.rs
    title: Worker state and log restore coverage
---

# Durable acceptance

The worker persists a command identity and accepted run specification before
launch. Repeating a command with the same identity/specification returns its
existing observation; reusing that ID for a different specification conflicts.
Worker reports are retained until their acknowledgement/delivery state permits
cleanup.

# Restart and incarnation

Each process startup advances the durable worker incarnation. Startup
reconciles persisted records before accepting new starts. If a run was not
durably terminal, its outcome can be `unknown`; Conductor does not claim to
recover the exact side effects of a process launch interrupted by a crash. The
manager keeps the corresponding reservation until an operator resolves it.

## Schema

The worker database has its own migration series; current source sets
`SCHEMA_VERSION` to **4** in `src/lib.rs`. It is independent of manager schema
version 12. Worker records include runs/logs, metadata/incarnation, accepted
commands, and report state.

Offline backup takes the worker state lock and snapshots `worker.sqlite3`
through SQLite's backup API. It also includes retained files under `runs/`.
WAL/SHM sidecars are not copied separately. Restore validates the database
integrity and schema marker and installs the worker state into a new destination
without replacing existing files. Include worker identity and CA files
explicitly when they are needed to reconnect; they may be outside the worker
state directory. A restored worker remains held from startup until the
operator has fenced surviving workload processes, reviewed unknown outcomes,
checked identity changes, and clears its recovery hold with
`conductor backup recovery-resolve --worker-data-dir ...`.

## Failure boundary

Durable worker state records what the worker accepted and observed. It cannot
prove that an external system saw exactly one side effect or that an escaped
process is gone. Investigate host state before resolving an `unknown` run.
