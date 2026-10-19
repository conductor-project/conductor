---
type: Reference
title: Core identifiers and validated contracts
description: Newtypes and constructors enforce stable IDs and validated job inputs before manager persistence or worker execution.
status: stable
source_revision: 5a56d84
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: ID types, states, commands, and errors
  - resource: ../src/structs/job_id.rs
    title: Job ID validation
  - resource: ../src/structs/run_id.rs
    title: Run ID validation
  - resource: ../src/structs/job_spec.rs
    title: Validated job definition
  - resource: ../src/structs/raw_job_spec.rs
    title: Untrusted job input
  - resource: ../tests/job_spec.rs
    title: Validation tests
---

# Identifiers

Job, run, worker, and command IDs are distinct newtypes rather than
interchangeable strings. Constructors validate externally supplied values
before they reach storage or transport. A run ID is not an operating-system
process ID. A worker ID is stable across connections; its incarnation
distinguishes a worker restart and fences stale sessions.

Overlay contracts add a stable network-domain ID and a canonical lowercase
hexadecimal Iroh endpoint public ID. Peer assignments use IPv4 addresses and
monotonic peer/network generations. Configuration snapshots contain public
peer identities only; secret endpoint keys remain worker-local. These shared
types describe control-plane state and do not implement packet transport.

# Job contracts

Raw request data is converted to validated job data before execution. A job
includes an explicit executable-and-arguments vector, working directory,
environment policy, optional timeout, termination grace, and optional resource
or filesystem policy. Commands are not implicitly interpreted by a shell.
Executable and working-directory existence is a host/worker concern.

Job and service definitions use explicit domain types and validation errors.
Consumers should use the constructors and preserve errors at API boundaries;
they should not bypass validated constructors by assembling unchecked internal
state. Tests in `tests/job_spec.rs` cover accepted/rejected definitions and
identity invariants.

# State vocabulary

Run states distinguish requested/starting/running/stopping from terminal
outcomes. `unknown` means the outcome is not established; `abandoned` is an
operator resolution and not a successful exit. Worker readiness states and
availability/draining statuses are also distinct. See the state and execution
contracts in [`execution-resources-and-protocol.md`](execution-resources-and-protocol.md).
