# MVP2 command and report semantics

This document specifies transport-neutral command ownership and report
ordering. It builds on the identities and allocation states in
[the MVP2 execution model](mvp2-execution-model.md). It does not define a
network transport or claim exactly-once execution.

## Fencing identities

An assignment is identified by `(allocationId, assignmentGeneration,
workerId, workerIncarnation)`. The manager increments the generation whenever
it assigns a pending allocation and binds the assignment to the worker's
current incarnation. The worker accepts a command only for its current
incarnation and the manager-authorized generation for that allocation; it
rejects lower generations as stale and higher, unrecognized generations until
manager reconciliation authorizes them. The manager accepts a report only
when its allocation, generation, assigned worker, and assigned incarnation
match the ownership record for that command. A newer generation makes
messages from earlier assignments stale.

A worker incarnation identifies one process lifetime for a stable `workerId`.
The worker durably advances its incarnation before accepting commands after a
restart. Commands target one exact incarnation. A replacement process rejects
commands addressed to the previous incarnation, then reconciles its durable
allocation and command journal with the manager. Incarnations prevent a
restarted worker from acting on delayed messages; they do not prove that a
process started by the previous incarnation has stopped.

Each command has a manager-generated stable `commandId`. A retry of one
logical command reuses that ID and the exact same command contents. A new
logical command, including a replacement attempt after a proven non-acceptance,
uses a new ID. Reusing an ID with different contents is a protocol conflict.

## Command envelope

The following JSON is illustrative. Fields are required and unknown fields
are rejected. Protocol version negotiation is specified by issue #57.

```json
{
  "commandId": "command-104",
  "targetWorkerId": "worker-a",
  "targetWorkerIncarnation": 17,
  "allocationId": "allocation-9",
  "assignmentGeneration": 2,
  "attemptId": "attempt-3",
  "kind": "start_attempt",
  "jobRevisionId": {
    "jobId": "thumbnailer",
    "revision": 4
  }
}
```

The tuple `(targetWorkerId, targetWorkerIncarnation, allocationId,
assignmentGeneration)` binds the command to its owner. `attemptId` and
`jobRevisionId` identify immutable execution inputs. A stop command uses the
same ownership fields and attempt ID with `kind: "stop_attempt"`; it cannot
silently target a later attempt.

## Acknowledgement meaning

An acknowledgement reports delivery progress; it is not an execution result.

| Acknowledgement | Required evidence | Does not prove |
| --- | --- | --- |
| `received` | The envelope passed validation in memory. | Durable acceptance or execution. |
| `accepted` | The worker durably recorded the command ID, contents, and acceptance decision before acknowledging it. | That a process was spawned or completed. |
| `rejected` | The worker durably recorded a terminal rejection reason for the command ID. | That another command or assignment is safe without manager reconciliation. |

Duplicate delivery of an accepted or rejected command returns the recorded
decision and does not repeat its effect. If the worker crashes before the
acceptance transaction commits, it must not have acknowledged `accepted`. If
the transaction commits but the acknowledgement is lost, redelivery returns
the committed decision. Acceptance is the durable ownership boundary; process
launch and completion are separate reports.

## Report envelope and ordering

Reports are immutable records. `reportSequence` increases strictly within a
worker incarnation and is committed durably with the report before delivery.
The sequence restarts only when the worker advances to a new incarnation.
Reports refer to the original command and assignment so delayed reports can be
recognized without relying on arrival order.

```json
{
  "workerId": "worker-a",
  "workerIncarnation": 17,
  "reportSequence": 208,
  "commandId": "command-104",
  "allocationId": "allocation-9",
  "assignmentGeneration": 2,
  "attemptId": "attempt-3",
  "kind": "attempt_observed",
  "observationRevision": 3,
  "state": "running"
}
```

The manager deduplicates reports by `(workerId, workerIncarnation,
reportSequence)`. The same sequence and same contents is a duplicate; the same
sequence with different contents is a protocol conflict. Reports arriving
ahead of a missing sequence are held in a bounded reorder buffer and are not
applied until the gap is filled or reconciliation resolves it. A lower
sequence already applied is ignored as a duplicate. Within an attempt, only a
strictly newer `observationRevision` can advance its observed state. Terminal
attempt states cannot be replaced by a later nonterminal observation.

After restart, a worker may replay unacknowledged reports from its previous
incarnation. The manager records and acknowledges those reports against that
incarnation's sequence watermark. They may resolve the outcome of the same
allocation generation and attempt, but they cannot authorize new commands or
change ownership. If a newer generation or attempt has superseded them, they
are retained as history and do not change current state.

The manager persists reports before acknowledging them. A worker retains
unacknowledged reports and replays them after reconnect. Acknowledgement of
report sequence `n` means the manager durably recorded all reports through
`n` for that worker incarnation; it is a compaction watermark, not proof that
the execution side effect is reversible.

## Failure semantics

| Failure | Required behavior |
| --- | --- |
| Identical command delivered more than once | Return the durable prior decision for its `commandId`; do not start or stop twice. |
| Same command ID arrives with changed contents | Reject as a protocol conflict; never reinterpret the existing ID. |
| Acceptance acknowledgement is lost | Retry the exact command ID and contents. The journal returns its prior decision; `accepted` alone does not imply that execution started. |
| Worker restarts before acceptance is committed | Advance incarnation before accepting more commands. Reconcile whether the prior command was durably accepted; do not blindly replay an ambiguous start. |
| Worker restarts after acceptance is committed | Reject commands targeting the old incarnation, reconcile the accepted command and attempt, and report `unknown` when start/completion cannot be established. Do not create a second attempt automatically. |
| Old generation command or report arrives | Reject the command; retain a report as history, but do not let it change current ownership or a superseding attempt. |
| Reports arrive out of order | Buffer later sequences within a fixed bound; request/replay the gap or reconcile before applying later records. |
| Report acknowledgement is lost | Worker replays the same report sequence and contents; the manager acknowledges the durable duplicate without applying it twice. |

`assignmentGeneration` fences manager ownership and stale protocol messages
only. It cannot stop an already-running process, fence writes to an external
service, or undo an external side effect. Those guarantees require
cooperation from the execution target or an external fencing mechanism. If
worker state does not establish whether an attempt started or stopped, the
allocation remains uncertain and its reservation is retained until explicit
reconciliation or operator resolution.

## Deduplication and retention boundary

The worker retains each command ID, canonical command contents, decision, and
associated attempt outcome for at least the lifetime of its allocation,
including while that allocation is uncertain. It must not use a time-based
expiry that could make a delayed retry execute again. The manager retains
report sequence contents until acknowledged and retains the highest
acknowledged sequence per worker incarnation as the compaction watermark.

Command records may be compacted only after the allocation is released, its
terminal or unresolved outcome is recorded by the manager, and the manager has
advanced a durable command-retention watermark that excludes future replay.
Report records through an acknowledged sequence may be compacted while keeping
the watermark. Command IDs and report sequence values are never reused after
compaction. Until a versioned persistence and compaction protocol is defined,
implementations keep these records; no fixed retention duration is implied.
