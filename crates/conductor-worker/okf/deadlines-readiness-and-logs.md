---
type: Reference
title: Worker deadlines, readiness, logs, and inventory
description: Supervisors enforce execution deadlines, collect bounded streams, report readiness, and advertise host capabilities.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Run supervision and service probes
  - resource: ../src/structs/log_policy.rs
    title: Log retention policy
  - resource: ../src/structs/worker_service.rs
    title: Persistence and report state
  - resource: ../../conductor-core/src/inventory.rs
    title: Inventory contract
  - resource: ../tests/runs.rs
    title: Deadline, logs, and probe tests
---

# Deadlines and cancellation

Job timeouts begin when process execution launches and use a monotonic clock;
queue time does not consume the deadline. A stop request triggers bounded
graceful termination followed by backend cleanup. Natural exit can race with
stop; the worker reports the terminal observation it establishes from the
supervisor's ordering rules.

# Readiness

Service readiness probes execute according to the revision contract and
produce persisted observations. A service endpoint is eligible only after
readiness succeeds and the manager reconciles that observation. Probe success
does not prove client-side DNS or network convergence.

# Logs

Stdout and stderr are drained continuously to private per-run files. Each
stream stores at most `log_max_bytes_per_stream` bytes (8 MiB by default;
configured range 1 byte to 1 GiB). Once the cap is reached, additional bytes
are discarded while draining continues, and that stream is marked truncated.
The worker retains logs for the newest `log_retention_runs` completed runs
(100 by default; configured range 0 to 10,000). Pruning waits for both streams
to close and preserves run history. Each pass removes at most 128 eligible run
log pairs. A structured `log_retention_progress` event reports the number of
runs pruned and whether another batch remains; the next cleanup pass resumes
from the durable `run_logs.pruned` markers. If interrupted after removing a
file but before recording the marker, a later pass treats the missing file as
already removed and records progress. Active runs' logs remain subject to the
per-stream cap; SQLite run and retention metadata are managed separately and
can continue to grow.

`GET /v1/runs/{runId}/logs` requires `stream=stdout` or `stderr`; `offset`
defaults to 0 and `limit` defaults to 4,096 bytes, with a range of 1 to 65,536.
The JSON response includes base64 `data`, requested `offset`, `nextOffset`,
`streamClosed`, `eof`, and `truncated`. Continue at `nextOffset`. `eof` is true
only when the stream is closed and the returned range reaches its end; an
empty read from a live stream is not EOF. A pruned log returns `410 Gone`; an
unknown run returns `404`. Worker filesystem paths are not exposed as API
values.

# Host inventory

The worker reports OS/architecture, process and enforcement capabilities,
resource totals/allocatable capacity, labels, and slot information through
bounded inventory messages. Inventory is a scheduling snapshot rather than
utilization telemetry or kernel enforcement. Source and contract tests are in
`src/lib.rs`, `conductor-core/src/inventory.rs`, and `tests/runs.rs`.
