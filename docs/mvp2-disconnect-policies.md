# MVP2 disconnect and uncertain-allocation policies

This document defines manager behavior when worker connectivity or execution
outcomes are uncertain. It builds on the allocation contract in
[the MVP2 execution model](mvp2-execution-model.md) and the command/report
contract in [MVP2 command semantics](mvp2-command-semantics.md). It specifies
policy; it does not claim that a heartbeat or process-group signal proves
process termination.

## Independent state dimensions

Connectivity, execution, and health describe different facts. A manager must
not infer one from another.

| Dimension | Example states | What the state establishes |
| --- | --- | --- |
| Connectivity | `connected`, `suspected`, `disconnected` | Whether the manager can currently exchange messages with the worker. |
| Execution | `queued`, `starting`, `running`, terminal, `unknown` | The latest durable evidence about one attempt. `unknown` means the outcome is unresolved. |
| Health | `healthy`, `unhealthy`, `unknown` | The latest workload or worker health observation. Missing observations produce `unknown`, not `unhealthy`. |

Heartbeats update connectivity. They do not report whether an attempt is
running, whether a process has stopped, or whether the application is healthy.
Application health checks update health; an unhealthy workload can still be
running and consuming capacity. Execution reports update attempt state; a
terminal attempt can belong to a worker that is currently disconnected.

## Timeout and partition decisions

The failure-detection interval is a configurable liveness threshold, not a
lease that transfers ownership. Crossing it marks a worker suspected or
disconnected according to local policy. It does not prove worker failure.

| Event or evidence | Connectivity | Execution and health | Allocation and reservation | Manager action |
| --- | --- | --- | --- | --- |
| One heartbeat is missed | `suspected` | Keep last execution report; health is unchanged or becomes `unknown` when its freshness bound expires. | Preserve ownership and reservation. | Retry communication with bounded backoff. Do not replace work. |
| Liveness threshold expires | `disconnected` | Keep the last confirmed attempt state; report health as `unknown` if stale. Do not turn `running` into `failed`. | Mark an assigned allocation `uncertain` if command or process outcome cannot be established; retain its reservation. | Stop relying on stale status. Reconcile when the worker returns. |
| Command acknowledgement deadline expires | `suspected` or `disconnected` | Command acceptance and execution are unresolved; use `unknown` when neither can be established. | Retain the assignment and reservation. | Redeliver the same command ID and contents only to the same worker incarnation, or wait for reconciliation. Never invent a new command ID to make an ambiguous start happen again. |
| Workload execution deadline expires while connected | `connected` | Send a stop request. Keep the attempt unresolved until a terminal report or reliable backend evidence arrives. Health remains an independent observation. | Keep the allocation and reservation until termination is established. | Record the deadline event; do not equate a stop request or response with process termination. |
| Stop deadline expires or worker disconnects during stop | `suspected` or `disconnected` | Execution becomes `unknown` unless a terminal result is durably reported. | Mark uncertain and retain reservation. | Reconcile; do not automatically retry as a new attempt. |
| Worker is declared lost or misses many heartbeats | `disconnected` | Last execution evidence remains last known; current health is `unknown`. | Retain uncertain ownership and reservation. | Missed heartbeats alone never authorize singleton replacement or release. |

An RPC timeout describes the caller's inability to observe a response. It does
not cancel the remote operation. A workload deadline requests termination; it
does not establish that termination completed.

## Replacement and duplicate tolerance

Replacement during a partition can overlap with an execution that is still
running. The default duplicate-tolerance policy is `forbidden`. Under that
default, a singleton allocation is not replaced because of missed heartbeats,
stale health, or an elapsed command timeout.

An operator may explicitly configure duplicate execution as `allowed` for a
workload. During a partition, the manager may then create a distinct
replacement allocation for the desired service instance or one-shot request,
provided capacity can be reserved for the replacement. The original
allocation remains uncertain with its reservation held; the manager records
that both attempts may execute. It does not transfer or erase the original
allocation's identity, assignment generation, or attempt history. The policy
permits overlapping execution; it does not fence external writes, guarantee
the old process stopped, or promise that duplicates are harmless.

Only an explicit duplicate-tolerance setting authorizes that behavior. If
the setting is absent, invalid, or unknown to a manager, it is treated as
`forbidden`. A health failure while the worker remains connected may trigger
workload policy, but it does not by itself resolve an uncertain prior attempt
or release its reservation.

## Reconnect and reconciliation

On reconnect, the manager authenticates the worker identity and compares the
worker incarnation, assignment generation, command journal, report sequence
watermarks, and attempt observations with its own durable state. Commands for
an older worker incarnation are not replayed to a new incarnation. Unacknowledged
reports from a known prior incarnation may be recorded as history and may
resolve that same attempt; they do not authorize commands or change a newer
assignment.

| Reconciliation evidence | Execution result | Allocation and reservation | Next action |
| --- | --- | --- | --- |
| Worker proves the command was never durably accepted and no attempt exists | Not started | Resolve uncertainty to `pending`; release the old assignment reservation before assigning again. | A new assignment advances the generation and uses a new command ID. |
| Durable report confirms the assigned attempt is still running | `running` | Preserve the current assignment and reservation. | Resume observation and health reporting. |
| Durable terminal report or reliable backend evidence confirms completion/stop | Terminal or stopped | Persist the outcome, then release the allocation reservation according to job policy. | Continue normal scheduling or service reconciliation. |
| Command was accepted but start or stop outcome cannot be established | `unknown` | Keep allocation uncertain and retain reservation. | Wait for more evidence or operator resolution; do not automatically replay a start. |
| Worker identity/incarnation conflicts with assignment, or records conflict | `unknown` | Keep the prior allocation uncertain and retain its reservation. | Reject stale commands/reports as current state; surface the conflict for operator review. |
| Worker never returns and no independent evidence establishes termination | `unknown` | Keep allocation uncertain and reservation held. | Require explicit operator resolution; a heartbeat timeout is insufficient. |

“Reliable backend evidence” must be specific to an enabled backend capability
and its documented limits. For example, macOS process-group cleanup does not
prove that descendants which escaped the process group have stopped. Linux
cgroup evidence may resolve a process's placement only when the relevant cgroup
was actually enabled and inspected; the policy does not assume Linux-only
facilities on another platform. Resource reservations remain scheduler
accounting and are not kernel enforcement.

## Operator resolution

Operator actions resolve manager ownership explicitly and must record the
principal, target allocation and attempt, chosen outcome, evidence or reason,
and whether duplicate execution was accepted. Resolution does not rewrite
prior reports or attempt history.

| Resolution choice | Preconditions | Result |
| --- | --- | --- |
| Confirm not accepted/not started | Durable worker reconciliation establishes no command acceptance or attempt. | Return the allocation to `pending`; release its old reservation and allow reassignment with an advanced generation. |
| Confirm terminal/stopped | A terminal report or backend evidence meets the enabled capability's documented guarantee. | Persist the final outcome and release the reservation. |
| Keep uncertain | Evidence is absent or conflicting, and duplicate execution is forbidden. | Preserve uncertain ownership and reservation; do not replace the singleton. |
| Authorize duplicate replacement | Duplicate execution is explicitly allowed and the operator accepts overlap risk. | Keep the original allocation uncertain; reserve separately for a new allocation and mark both as potentially active. |
| Force administrative closure | The operator supplies a reason but cannot prove execution stopped. | Record outcome as unresolved/unknown. Any reservation release is an explicit risk acceptance and does not imply termination; replacement remains forbidden unless duplicate execution is allowed. |

The manager must not silently convert an unresolved attempt to `failed`,
free its reservation, or launch a replacement after a connectivity timeout.
When resolution cannot prove whether side effects occurred, preserve that
uncertainty in status and audit history.
