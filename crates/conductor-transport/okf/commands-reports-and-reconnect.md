---
type: Reference
title: Worker commands, reports, and reconnect
description: Workers poll bounded command batches and send durable reports with acknowledgements; session freshness and incarnation fence stale traffic.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Authentication, poll, and report routes
  - resource: ../src/structs/worker_session.rs
    title: Poll/report ordering and inventory
  - resource: ../src/structs/worker_transport_hub.rs
    title: Manager transport hub
  - resource: ../src/structs/worker_transport_client.rs
    title: Worker polling client
  - resource: ../src/structs/worker_report.rs
    title: Report envelope
  - resource: ../src/lib.rs
    title: Transport tests
---

# Poll and delivery

Workers establish an authenticated session and poll for bounded command
batches. Manager commands have durable IDs and remain available for replay
until accepted/report acknowledgement advances delivery state. A reconnect can
therefore return the same command identity; the worker's idempotent command
handler makes duplicate delivery safe for its durable run state.

Before dispatch, the manager persists assignment and command identity. Its
outbox retains at most 32 unacknowledged commands per worker, with at most 24
non-control commands so up to eight remaining slots can carry stop or
resolution control commands. Saturation fails explicitly; committed commands
are not silently dropped. A retry of one command keeps its ID, target worker
incarnation, assignment, and contents. The worker durably journals command
acceptance and its result; duplicate delivery with identical contents returns
the prior recorded decision, while reuse of an ID with changed contents is a
conflict. Acknowledged command rows are currently retained indefinitely.

An `accepted` response means the worker durably recorded the command; it does
not prove that a process was spawned or completed. Those outcomes arrive in a
later report. The transport does not claim exactly-once external side effects.

# Reports and ordering

The worker sends bounded reports with identity and sequence context. The
manager validates the active certificate and worker identity, then checks the
report sequence and its durable command target before applying observations.
Duplicate reports with identical contents are acknowledged; conflicting
duplicates and sequence gaps are rejected. Inventory synchronization has its
own bounded page and acknowledgement flow.

Reports include the stable command ID, worker ID, worker incarnation, a
strictly increasing report sequence within that incarnation, and the stored
worker result. The worker commits a report sequence with the result before
sending it and keeps the report in the pending resend set until the manager
acknowledges it. The manager accepts the next expected sequence for that
incarnation, acknowledges an identical duplicate, and rejects changed
contents, a sequence gap, or a report that does not match its original durable
command target and result. A pending report from an older incarnation can be
replayed after restart when it matches that original durable command; it is not
rejected only because a newer session has since connected. The manager
persists the command acknowledgement before returning the report
acknowledgement. Acknowledgement removes a report from the worker's pending
resend set but marks the command row acknowledged; command/result history is
not deleted or compacted by this acknowledgement.

The next worker process advances its durable incarnation. Polls and commands
targeted to an older incarnation are fenced rather than redirected to the new
process, while a matching report for an old durable command may still complete
its original delivery as described above.

# Reconnect and freshness

Workers reconnect with bounded retry/backoff after transport failures. The
manager marks a worker stale after 45 seconds without a successful poll or
report. It retains the last inventory snapshot for inspection, but stale worker
state is not current availability evidence. Durable commands are not dropped
just because a connection closed.

An authenticated poll with no compatible protocol version or required
capability returns HTTP `426 Upgrade Required` with a compatibility diagnostic.
The worker treats that response as permanent and exits with an actionable error
instead of retrying indefinitely; operators should align the manager and worker
Conductor versions.

# Limits and tests

The transport caps JSON request bodies at 128 KiB in this reviewed source.
Payload schemas and bounds are enforced before manager/worker dispatch. Tests
in the crate cover protocol negotiation, identity/incarnation rejection,
replay, inventory synchronization, report ordering, and reconnect. Local
multi-process integration tests on one host do not establish routed
multi-host failure behavior.

The current wire protocol is version 3 with explicit capabilities for
assignment generations, command deduplication, ordered reports, worker
incarnations, and worker resource inventory. It is independent of the manager and worker SQLite schema
versions. Use the current source and tests for the message contract; historical
design text describing acknowledgements, gap buffering, or compaction is not
an additional current protocol guarantee.

`incompatible_protocol_poll_returns_actionable_upgrade_diagnostic` verifies
the manager's HTTP `426` response text; the worker client status-mapping test
verifies that the response becomes a permanent `IncompatibleProtocol` error,
while transient HTTP failures remain retryable.
