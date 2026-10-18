# MVP2 execution model

This document defines the transport-neutral desired-state and execution
contracts in `conductor-core`. It does not add distributed scheduling or
worker transport. MVP1 `JobSpec`, `RunSpec`, and `RunObservation` remain valid
for one-shot execution.

## Identity and revision invariants

- A logical `JobId` identifies a job across edits. Each immutable
  `JobRevisionId` is the pair `(jobId, revision)`, with a positive revision
  number. A revision's `JobSpec`, workload mode, executable digest, platform
  requirements, and resource request do not change after construction.
- `ExecutableDigest` records an operator-provisioned SHA-256 digest as 64
  lowercase hexadecimal characters. The contract does not calculate the
  digest; the worker must verify it before execution. MVP1 `JobSpec` values
  remain representable without being rewritten as MVP2 revisions.
- A `Workload::OneShot` revision preserves one-shot completion semantics. A
  `Workload::Service` revision names a service; mutable desired replica counts
  live in `JobDeployment`, not in the immutable revision. Service identity has
  an explicit namespace; MVP2 accepts only `default`.
- Each desired replica slot has a stable `ServiceInstanceId` and zero-based
  replica index. Its identity survives allocation and attempt replacement.
- Each `AllocationId` identifies durable ownership of one one-shot execution
  or service slot. An assignment generation increases each time a pending
  allocation is assigned. The manager uses `(allocationId, generation)` to
  reject stale assignment reports; this does not fence external side effects.
- Every retry is a new `ExecutionAttemptId` and increasing attempt number.
  Attempts are evidence for one allocation and are not overwritten by retries.

## Resources and execution capabilities

`ResourceRequest` describes scheduling demand; `ResourceReservation` records
capacity held for an allocation. Both are separate from MVP1 `ResourceLimits`,
which describe backend-enforced process limits. A reservation is accounting,
not enforcement.

`WorkerCapabilities` reports operating system, CPU architecture, available
execution backends, and actual enforcement capabilities. A job's
`ExecutionRequirements` matches each explicit platform/backend constraint and
requires every listed enforcement capability. Assignment is invalid when the
worker does not satisfy those requirements; unsupported required protections
must not be silently downgraded. Optional `workerLabels` constraints use
exact key/value matches against manager-controlled worker labels. MVP2
currently models Linux and macOS native process execution, with cgroup v2 and
Landlock as distinct enforcement capabilities. Listing a platform does not
imply that it implements either capability.

Service scheduling persists one allocation reservation per desired replica.
Placement checks worker capacity and the aggregate capacity of each shared
resource domain in one SQLite immediate transaction. Assigned and uncertain
allocations both consume capacity; a lost worker makes its assignments
uncertain and does not free the reservation. Candidate workers are selected
deterministically by current reserved slots and then worker ID. Omitted
one-shot resource requests retain the no-reservation behavior; service requests
must specify positive CPU and memory dimensions. The manager starts each
assigned service slot independently of one-shot admission, so replicas of one
job can run concurrently. It polls durable run observations and restarts a
service after any terminal exit with exponential backoff starting at 250 ms,
capped at 30 seconds, for at most five retries. After the retry budget is
exhausted, it retains the terminal run as failure evidence and stops relaunching
that slot. MVP1 one-shot completion never enters this restart loop.

An unresolved or disconnected service run remains attached to its durable slot;
the manager does not relaunch it until reconciliation establishes a terminal
outcome. This avoids duplicate processes when delivery or worker state is
uncertain. The restart timer and budget survive manager restart. This MVP2
policy is fixed per service and does not yet expose per-revision policy knobs.

## Allocation transitions

| Current state | Allowed transition | Meaning |
| --- | --- | --- |
| `pending` | `assigned` | Reserve capacity and assign a worker; advance generation. |
| `assigned` | `uncertain` | Delivery or execution outcome cannot currently be established. Keep ownership and reservation. |
| `uncertain` | `pending` | Explicit reconciliation proved the worker never accepted or started the allocation; clear worker assignment. |
| `pending`, `assigned`, `uncertain` | `released` | Explicitly release ownership and reservation after the caller has resolved the allocation. |

An uncertain allocation is not implicitly reassigned or released. Assignment
from any state other than `pending`, repeated release, and clearing uncertainty
without reconciliation are rejected. Retry attempts belong to the same
allocation unless manager policy explicitly creates a replacement allocation
for the same service instance.

## Attempt transitions

| Current state | Allowed next states |
| --- | --- |
| `queued` | `starting`, `cancelled`, `unknown` |
| `starting` | `running`, `failed`, `cancelled`, `unknown` |
| `running` | `succeeded`, `failed`, `cancelled`, `unknown` |
| `unknown` | `running`, `succeeded`, `failed`, `cancelled` after a later observation resolves the outcome |
| terminal (`succeeded`, `failed`, `cancelled`) | none |

Every accepted transition advances the attempt observation revision. Stale
observations must not replace newer state. `unknown` is unresolved, not a
terminal failure or proof that the process stopped. In particular, process
group cleanup does not prove that descendants escaped from the group have
stopped.

## Compatibility and scope

These types define the durable identities and state vocabulary used by
protocol, persistence, reconciliation, and scheduler work. They make no
guarantee about health checks, port ownership, external-resource fencing, or
manager high availability. Existing MVP1 one-shot JSON contracts and combined
manager/worker mode remain unchanged.

See [MVP2 command and report semantics](mvp2-command-semantics.md) for the
worker-incarnation, assignment-generation, command-deduplication, and report
ordering contract used by distributed execution.
