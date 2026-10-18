# MVP2 submission and deployment contract

This document defines the public manager contract for immutable execution
revisions and mutable service deployment intent. JSON uses camel-case field
names, rejects unknown fields, and uses the existing bounded request body and
stable error envelope.

## MVP1 compatibility

`POST /v1/jobs` continues to accept and store the existing `JobSpec`. Existing
one-shot submission, run idempotency, and `POST /v1/runs/{runId}/stop` semantics
remain unchanged. Stopping one run stops that attempt only; it does not change
the desired state of a service deployment. MVP2 service submissions use the
revision and deployment routes below.

## Immutable revisions

`POST /v1/jobs/{jobId}/revisions` creates the next immutable revision. The
request contains `definition`, `workload`, `executableDigest`, and
`executionRequirements`. The server assigns the positive monotonically
increasing revision number; clients do not choose it. The path ID must equal
`definition.id`. A revision cannot be edited or reused after creation.

`workload` is either `{"kind":"one_shot"}` or a service with a stable
identity and an optional list of named TCP ports, for example
`{"kind":"service","identity":{"namespace":"default","name":"api"},"ports":[{"name":"http","protocol":"tcp"}]}`.
The service identity is a stable DNS label in the only supported namespace,
`default`. Replica count is not revision data; deployment replica counts are
limited to 10,000.

Each named port is assigned from a manager-configured range for the worker's
network domain. Configure `worker_service_network` for the primary worker and
`service_network` for each additional worker with a routable
`advertised_address`, a shared `network_domain_id` for workers on the same host
network, and non-overlapping `port_range_start` and `port_range_end` values.
SQLite holds a logical lease for every active run, so simultaneous Conductor
reservations in a network domain cannot collide. The manager does not keep a
socket open on behalf of the workload; the service process binds the assigned
port itself. Leases survive manager restart and uncertain run outcomes; the
manager releases one only after confirmed terminal cleanup or allocation
release. Workers inject the assignment as
`CONDUCTOR_PORT_<NAME>` (upper-case, hyphens changed to underscores). The
service must exit nonzero when it cannot bind. If another process wins an
external bind race, the failed attempt remains in run history and the retry
advances its starting port candidate with the next run sequence, then scans
for an unleased port. External races can recur because the manager cannot
atomically reserve a socket used by the workload. Ranges must be large enough
for concurrent services and any retries.

Service revisions require positive `resourceRequest.cpuMillis` and
`resourceRequest.memoryBytes`. The manager rejects a missing dimension instead
of silently reserving zero. For one-shot revisions these request fields may be
omitted, which means no scheduler reservation is requested. Omitted
`definition.resourceLimits` means no requested kernel limit; supplied limits
remain part of the immutable `definition`. Resource requests are scheduler
accounting, never a claim of kernel enforcement.

For services, the CPU request must cover the configured cgroup CPU ceiling,
rounded up to millicores (`ceil(cpuQuotaUs * 1000 / cpuPeriodUs)`), and the
memory request must be at least `memoryMaxBytes` when set. Swap is not included
in the memory reservation. One-shot revisions keep the MVP1 no-reservation
semantics even when they request process limits.

The returned revision includes its assigned ID, the validated definition,
effective resource request, `resourceRequestProvenance`, and the union of
explicit execution requirements with requirements derived from the definition.
CPU or memory limits require Linux cgroup v2; a non-empty filesystem policy
requires Landlock. Contradictory or unsupported requirements are rejected.
The request has no administrator defaults, so accepted values remain stable
across retries and manager restarts. A service request is marked `explicit`; an
omitted one-shot request is marked `unspecified`.

`executionRequirements.workerLabels` is an optional map of exact-match worker
label constraints. Omission means no label constraints; an empty map has the
same meaning. The manager selects only fresh, available workers whose platform,
backend, enforcement capabilities, and labels satisfy the revision. Missing
required protection is reported as `required_protection_unavailable`; the
manager does not weaken the requirement to place the service.

Revision keys are scoped to the revision-creation operation and job ID.
Deployment mutation keys are scoped to the deployment operation and job ID.
The revision idempotency key is required. Repeating the same key and canonical
request returns the original revision; using the key for different contents
returns `409 idempotency_conflict`. Validation failures do not consume a key.

## Mutable deployment intent

`PUT /v1/jobs/{jobId}/deployment` selects a previously created service revision
and sets `replicas` from zero through 10,000. The first
deployment requires `If-None-Match: *`. Replacing an existing deployment
requires its current strong ETag in `If-Match`. An optional `strategy` configures
revision updates; it defaults to `{"maxSurge":0,"maxUnavailable":1,
"progressDeadlineSeconds":300}`. `maxSurge` must currently be zero because a
service slot persists one execution attempt at a time. `maxUnavailable` must be
positive and is the maximum number of replica slots the manager replaces in one
readiness-gated wave. The progress deadline is from 1 through 86,400 seconds.
The response includes the deployment generation as its ETag and a durable
`pending` or `suspended` state.
An accepted deployment is durable before placement begins; placement failures
remain visible with an actionable `pendingReason`.

`GET /v1/jobs/{jobId}/deployment` returns the complete desired state and its
ETag. `POST .../scale` changes only the replica count, including scale-to-zero.
`POST .../suspend` retains the replica count but requests zero active
allocations. `POST .../resume` restores the retained replica count. Scaling a
suspended deployment changes the retained count but does not resume it.

Each mutation requires `If-Match`, except initial creation, and a required
`Idempotency-Key`. The manager compares the generation and commits the new
generation and idempotency result in one SQLite transaction. A retry with the
same key and request returns the original response even if the caller's ETag is
now stale. A key reused with different contents returns `409
idempotency_conflict`; a stale ETag returns `412 precondition_failed`. A
successful mutation increments the generation exactly once. Replayed requests
after restart return the stored result.

Scaling, suspension, resumption, and revision deployment do not change service
instance IDs for retained replica slots. A slot keeps its ID through a
revision update and its replacement allocation uses the newly selected
revision. Scale-down retires highest numbered slots first; scale-up reactivates
retained slot identities before allocating new identities. Scale-to-zero keeps
the deployment and revision so a later scale operation can resume it.

`POST .../rollback` selects an existing earlier revision. Rollback changes the
execution revision only; it preserves the current replica count and suspension
state. Every rollback is conditional on the current generation, so concurrent
scale, update, suspend, resume, and rollback requests cannot overwrite one
another silently.

For a revision update, the manager retains the previous revision in durable
rollout state and replaces at most `maxUnavailable` slots at once. If a
readiness probe is declared, the next wave waits until every replacement in the
current wave reports `ready`; without a probe, a running process passes the
rollout gate. When the progress deadline expires before the target revision
converges, the manager advances the deployment generation and sets the previous
revision as desired. This rollback is itself readiness gated, preserves the
replica count, and is visible in `GET .../deployment` as the desired revision
plus `rollout.rollingBack`. Scale-to-zero cancels rollout progress and retains
the selected revision; suspension retains the rollout for resume.

`GET /v1/jobs/{jobId}/revisions/{revision}` returns immutable revision history.
`pending` means the requested replica state has not converged. This includes
unplaced replicas, assigned replicas that have not started, retries waiting for
backoff, exhausted retry budgets, and unresolved worker outcomes. `ready` means
every desired replica has a running process, or the desired replica count is
zero. Outside a revision rollout, service health probes remain separate from
deployment convergence: an unready probe does not stop a running process from
satisfying deployment convergence. During a rollout, readiness gates only the
advance to the next replacement wave.

`GET /v1/jobs/{jobId}/service-attempts` returns each known service slot's
current allocation and run, retry count, next eligible start time, and
`terminalFailure` flag. This distinguishes a service whose retry budget is
exhausted from a deployment that is merely waiting for placement. The terminal
run remains available through the run history endpoint.

`GET /v1/jobs/{jobId}/endpoints` returns only currently eligible endpoints for
that job. `GET /v1/services/{namespace}/{name}/endpoints` looks up the same
catalog by logical service identity. Both return the job, service identity,
instance, allocation, run, worker, port, readiness, and worker freshness/draining
state. An endpoint is eligible only when its current allocation and run match
the active deployment, its run is running and ready, and its worker is fresh,
available, non-degraded, and not draining. Endpoints without a readiness probe
are not published. Worker liveness is bounded by the transport's 45-second
freshness timeout; a worker reporting failure, becoming stale, or draining
withdraws its endpoints. The catalog is reconstructed from durable manager state
after restart.

Service identity is `namespace/name`; only `default` is accepted in MVP2.
Replicas of one job share its identity. Only one non-deleted job deployment may
own an identity, including while suspended or scaled to zero; deletion releases
ownership after allocation cleanup is complete. Both catalog paths use the
manager routes and are protected by the configured operator authorization
policy. Endpoint addresses are manager-configured routable addresses. Loopback
addresses are rejected so test listener addresses are not published as
production endpoints.

`DELETE /v1/jobs/{jobId}/deployment` is conditional and idempotent. It records a
durable deleting tombstone and returns `202` while active or uncertain
allocations still need reconciliation. It does not delete revision history or
release an uncertain allocation. Once cleanup is confirmed, the deployment is
deleted and the job ID remains reserved under existing MVP1 deletion rules.
Until scheduling records allocations, deletion is immediate because there is
no known worker ownership to reconcile.

## Errors and pending state

Errors use `{"error":{"code":"...","message":"..."}}`. Malformed JSON,
unknown fields, invalid service names, missing service resource requests, and
invalid transitions return `400`. Unknown jobs or revisions return `404`.
Duplicate revision identity, idempotency key reuse with different contents, or
an incompatible existing deployment returns `409`. Stale `If-Match` returns
`412`; a missing conditional header returns `428`. The active job limit returns
`507`; storage failures return `500`; request admission saturation returns
`429` with `Retry-After`.

Pending placement reasons are stable machine-readable values:
`placement_pending`, `no_eligible_worker`,
`worker_capacity_exhausted`, `worker_disconnected`,
`required_protection_unavailable`, and `shared_host_capacity_exhausted`. The
response also includes a bounded human-readable explanation. Pending and
uncertain allocations retain their resource reservations until reconciliation
proves they were never accepted or confirms terminal cleanup.

Each service replica has a durable allocation and reservation. Placement is
serialized with deployment changes, checks per-worker limits and shared-host
resource-domain budgets, and spreads replicas across workers with the fewest
current reservations (worker ID breaks ties). A worker's resource domain is
manager-provided metadata; workers sharing a physical host must use the same
domain ID and aggregate capacity. Reservations in `assigned` or `uncertain`
state count against both worker and domain budgets, including while a worker is
disconnected. An allocation whose worker becomes stale is marked uncertain and
its reservation remains held while a replacement is considered. The manager
does not assume that a lost worker stopped its process.

## HTTP examples

Create a service revision (the service reserves 250 millicores and 256 MiB per
replica):

```http
POST /v1/jobs/api/revisions
Idempotency-Key: revision-api-1
Content-Type: application/json

{
  "definition": {
    "id": "api",
    "command": ["/usr/local/bin/api"],
    "cwd": "/srv/api",
    "env": {},
    "inheritEnv": false,
    "terminationGraceMs": 5000
  },
  "workload": {
    "kind": "service",
    "identity": {"namespace": "default", "name": "api"}
  },
  "executableDigest": {"algorithm": "sha256", "value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
  "executionRequirements": {},
  "resourceRequest": {"cpuMillis": 250, "memoryBytes": 268435456}
}
```

Create and inspect a deployment:

```http
PUT /v1/jobs/api/deployment
If-None-Match: *
Idempotency-Key: deployment-api-1
Content-Type: application/json

{"revision": 1, "replicas": 2}
```

```http
GET /v1/jobs/api/deployment
```

Scale to zero and then resume capacity:

```http
POST /v1/jobs/api/deployment/scale
If-Match: "1"
Idempotency-Key: scale-api-2
Content-Type: application/json

{"replicas": 0}
```

```http
POST /v1/jobs/api/deployment/scale
If-Match: "2"
Idempotency-Key: scale-api-3
Content-Type: application/json

{"replicas": 2}
```

Suspend and resume the current replica intent:

```http
POST /v1/jobs/api/deployment/suspend
If-Match: "3"
Idempotency-Key: suspend-api-1
```

```http
POST /v1/jobs/api/deployment/resume
If-Match: "4"
Idempotency-Key: resume-api-1
```

Update and roll back the execution revision while retaining deployment intent:

```http
PUT /v1/jobs/api/deployment
If-Match: "5"
Idempotency-Key: deploy-api-revision-2
Content-Type: application/json

{"revision": 2, "replicas": 2}
```

```http
POST /v1/jobs/api/deployment/rollback
If-Match: "6"
Idempotency-Key: rollback-api-1
Content-Type: application/json

{"revision": 1}
```

Stopping an individual attempt continues to use
`POST /v1/runs/{runId}/stop`; it leaves deployment intent unchanged.

```http
POST /v1/runs/01J-RUN-ID/stop
```

Delete deployment intent with its current generation:

```http
DELETE /v1/jobs/api/deployment
If-Match: "7"
Idempotency-Key: delete-api-1
```
