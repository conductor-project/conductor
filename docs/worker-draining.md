# Worker draining

Worker drains are manager-owned and persisted in the manager database. A drain
request immediately excludes the worker from new placements and marks its
service-catalog endpoints ineligible. This intent survives manager restarts.

## API

Request a drain:

```http
POST /v1/workers/{workerId}/drain
Content-Type: application/json

{"gracePeriodSeconds":30}
```

`gracePeriodSeconds` is optional, defaults to 30, and must be between 1 and
300. The response is `202 Accepted` and includes `state`, `requestedAtMs`,
`deadlineAtMs`, `activeRuns`, and `blockedReason`.

Read progress with `GET /v1/workers/{workerId}/drain`. States are `draining`,
`blocked`, and `completed`. `replacement_capacity` means a service still runs
on the draining worker but no eligible worker currently has enough reserved
capacity for its replacement. Its catalog endpoint is already withdrawn. The
service run remains active while the drain is within its grace period. Once a
replacement allocation is assigned, the manager stops the old run through the
existing worker stop command and then starts the replacement. At the deadline,
the manager also stops remaining non-terminal runs. The worker applies each
run's configured `terminationGraceMs` before killing and reaping its process.

`termination_pending` means the deadline passed but the manager has not yet
confirmed termination. Unknown or unreachable runs remain visible and block
completion; the manager does not release their allocations on a timeout.

After the drain reaches `completed`, the worker remains excluded from placement.
`DELETE /v1/workers/{workerId}/drain` clears that completed intent so the worker
can be scheduled again. An in-progress drain cannot be cleared.

Service replacements keep their stable replica slot and allocation history.
The current schema permits one service run per replica slot, so the old service
process is stopped after replacement capacity is reserved and before the new
run is started. Clients should expect a service interruption while the new run
starts and becomes ready.
