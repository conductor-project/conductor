---
type: Reference
title: HTTP routes and contracts
description: Routes expose jobs, runs, service revisions/deployments, workers, drains, metrics, and readiness; the feature-gated typed client calls these contracts with bounded JSON responses.
status: stable
source_revision: 663bd0f
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/server.rs
    title: Route registration and handlers
  - resource: ../src/client.rs
    title: Typed client and response bounds
  - resource: ../src/structs/error_envelope.rs
    title: Error envelope
  - resource: ../src/structs/run_logs_response.rs
    title: Run log response
  - resource: ../../conductor/tests/agent.rs
    title: HTTP integration tests
---

# Typed client

Enable the `client` crate feature without defaults for client-only consumers.
The client accepts an HTTP(S) base URL, optional bearer token, optional PEM CA
certificate, request timeout, and maximum response size. A bearer token over
plain HTTP is accepted only for a loopback host. `get`, `post`, `put`,
`post_conditional`, `post_create`, `put_create`, and `delete` accept serde request/response types.
`get_bytes` and `get_text`
support bounded non-JSON endpoints such as metrics, and `delete_empty` handles
empty successful responses. `post` and `put` accept an
idempotency key; conditional POST/PUT/DELETE calls accept an `If-Match` value. The client
does not automatically retry mutations. It disables redirects, enforces the
response limit on bytes actually read (including error responses), and reports
typed errors without including response bodies or credentials.
`get_response` preserves the HTTP status and ETag alongside a decoded body,
including readiness states returned with HTTP 503.

Common job/run pages, deployment request bodies, service endpoints/watch,
worker drain and service attempt responses, and run log response contracts live
in `conductor_api::contracts` and are shared by the client and server.

Example read and mutation:

```rust,ignore
let mut config = conductor_api::client::ClientConfig::new("https://127.0.0.1:8080/");
config.bearer_token = Some(token);
config.ca_certificate_pem = Some(ca_pem);
let api = conductor_api::client::Client::new(config)?;
let job: conductor_core::JobSpec = api.get("v1/jobs/example").await?;
let run: conductor_core::RunObservation = api
    .post("v1/jobs/example/runs", &serde_json::Value::Null, Some("start-example-once"))
    .await?;
```

# Route groups

| Operation | Routes |
| --- | --- |
| Process status | `GET /healthz`, `GET /readyz` |
| One-shot jobs | `POST/GET /v1/jobs`, `GET/DELETE /v1/jobs/{id}` |
| Runs | `POST /v1/jobs/{id}/runs`, `GET /v1/runs`, `GET /v1/runs/{id}`, `POST /v1/runs/{id}/stop`, `POST /v1/runs/{id}/resolve`, `GET /v1/runs/{id}/logs` |
| Service revisions and deployments | `POST /v1/jobs/{id}/revisions`, `GET /v1/jobs/{id}/revisions/{revision}`, `GET/PUT/DELETE /v1/jobs/{id}/deployment`, deployment scale/suspend/resume/rollback |
| Workers and operations | worker status, drain/cancel drain, metrics, service attempt and endpoint/catalog reads/watch |
| Overlay identity and allocation | `POST /v1/network-domains`, `GET /v1/network-domains/{id}`, `GET /v1/network-domains/{id}/peers`, `DELETE /v1/network-domains/{id}/peers/{workerId}` |

The exact methods, paths, body types, query parameters, and conditions are
registered in `src/server.rs`. API tests are closest to the canonical behavior;
the [README](../../../README.md) is a shorter successful one-shot flow.

Overlay domain creation and peer revocation are audited operator mutations.
Peer status contains public endpoint IDs, assigned addresses, generations,
expiry, and quarantine state; it contains no Iroh private key. Worker identity
enrollment, configuration delivery, and observations use the separate mTLS
worker transport endpoints described in the transport bundle. These routes
manage membership and desired state only; they do not configure a tunnel or
host route.

# Important run semantics

Creating a job definition returns the validated stored definition. Starting a
run returns `202 Accepted`, a `Location: /v1/runs/{runId}` header, and an
observation containing the run ID. Poll the run resource rather than treating
acceptance as completion. Run output is fetched in bounded base64 chunks; the
response includes offset and closure/truncation metadata.

Run submission may include an `Idempotency-Key`. Replaying the same key for
the same job recovers the existing run; using it for another job conflicts.
This protects request retry semantics, not external process side effects.
Service creation requires `If-None-Match: *`; updates and destructive/mutating
operations require the current quoted generation in `If-Match`. Mutations
require an idempotency key. A deployment response includes its generation as
an `ETag`; stale generations fail rather than silently overwriting newer
intent. The service transition contract is exercised in `src/server.rs` tests.

## Service revision example

The following is a shape example, not a ready-to-run command: replace the
digest with the lower-case SHA-256 of the executable that will run on the
worker. The manager assigns the next revision number; the path ID must match
`definition.id`.

```json
{
  "definition": {
    "id": "web",
    "command": ["/opt/web/bin/server"],
    "cwd": "/opt/web",
    "env": {},
    "inheritEnv": false,
    "timeoutMs": null,
    "terminationGraceMs": 5000
  },
  "workload": {
    "kind": "service",
    "identity": {"namespace": "default", "name": "web"},
    "ports": [{"name": "http", "protocol": "tcp"}]
  },
  "executableDigest": {"algorithm": "sha256", "value": "<64 lower-case hex digits>"},
  "executionRequirements": {"workerLabels": {"pool": "web"}},
  "resourceRequest": {"cpuMillis": 250, "memoryBytes": 268435456}
}
```

Conductor injects the allocated port as `CONDUCTOR_PORT_<PORT_NAME>` (upper-case,
hyphens replaced by underscores) for declared service ports. The service must
read that environment variable and bind the port itself; Conductor does not
interpolate shell variables in command arguments. A workload can also declare
`{"kind":"one_shot"}`; one-shot resource requests may be omitted,
while service revisions require positive CPU and memory requests. Requests are
scheduling reservations, not enforcement; if a revision requests cgroup
limits, its CPU request must cover the configured CPU ceiling and memory must
cover configured `memoryMaxBytes`.

Revision creation requires `Idempotency-Key`. The response includes the server
assigned revision, validated definition, effective resource request and its
provenance, and execution requirements. Worker-label constraints are
exact-match constraints and only fresh, available workers satisfying platform,
backend, enforcement, labels, and resource capacity can be selected.

## Initial deployment example

```http
PUT /v1/jobs/web/deployment
If-None-Match: *
Idempotency-Key: deploy-web-1
Content-Type: application/json

{"revision":1,"replicas":2}
```

The response carries an `ETag` generation and may be `202 Accepted` while the
durable desired state is pending placement. The default update strategy is
`maxSurge: 0`, `maxUnavailable: 1`, `progressDeadlineSeconds: 300`; max surge
must remain zero because each stable service slot has one execution attempt.
Rollout replacements are readiness-gated when a probe is configured. A
`pendingReason` explains placement or convergence delays.

## Service mutation and worker drain routes

The mutation endpoints are `POST /v1/jobs/{id}/deployment/scale`, `/suspend`,
`/resume`, and `/rollback`. Each existing-deployment mutation needs the current
quoted generation in `If-Match` and a fresh `Idempotency-Key`; reuse the same
key only when retrying that same request. Refresh the ETag after each accepted
mutation. A suspended deployment retains its desired replica count; scaling
while suspended changes that count without resuming it. Rollback changes the
revision while preserving replica and suspension intent.

```http
POST /v1/jobs/web/deployment/scale
If-Match: "1"
Idempotency-Key: scale-web-1
Content-Type: application/json

{"replicas":3}
```

```http
POST /v1/jobs/web/deployment/suspend
If-Match: "2"
Idempotency-Key: suspend-web-1
```

```http
POST /v1/jobs/web/deployment/resume
If-Match: "3"
Idempotency-Key: resume-web-1
```

```http
POST /v1/jobs/web/deployment/rollback
If-Match: "4"
Idempotency-Key: rollback-web-1
Content-Type: application/json

{"revision":1}
```

`POST /v1/workers/{id}/drain` takes optional `{"gracePeriodSeconds":30}`;
the default is 30 seconds and allowed values are 1 through 300. It returns
`202` with the drain state, timestamps, active-run count, and blocking reason.
`GET` on the same route returns drain progress with `draining`, `blocked`, or
`completed` state; `DELETE` clears only a completed drain and returns `204`. A
drain immediately excludes the worker
from new placements and withdraws its service endpoints. `replacement_capacity`
means no eligible worker has enough reserved capacity for a service
replacement; `termination_pending` means the deadline passed without confirmed
termination. Unreachable or unknown runs remain visible and block completion;
the manager does not release their allocations on timeout. Clear a completed
drain before scheduling the worker again. Service replacement preserves its
replica slot, but the current schema permits one run per slot, so the old run
stops after replacement capacity is reserved and before the new run starts.

```http
POST /v1/workers/worker-east/drain
Content-Type: application/json

{"gracePeriodSeconds":30}
```

Only clear the drain after `GET /v1/workers/{id}/drain` reports `completed`:

```http
DELETE /v1/workers/worker-east/drain
```

## Resolving an uncertain run

After independently verifying that an `unknown` workload has been cleaned up,
a trusted operator may abandon it with `POST /v1/runs/{id}/resolve`. The
resolution must be `abandon` and the reason must be non-empty and at most 512
bytes. Retry an ambiguous resolution with the same reason; a different reason
conflicts. The endpoint records operator-supplied evidence but does not perform
or verify external process cleanup. Abandonment releases held job and worker
capacity; it is not a successful run result.

```http
POST /v1/runs/01J-RUN-ID/resolve
Content-Type: application/json

{"resolution":"abandon","reason":"Externally verified workload cleanup"}
```

## Service observation and metrics routes

The API exposes `GET /v1/jobs/{id}/service-attempts`,
`GET /v1/jobs/{id}/endpoints`,
`GET /v1/services/{namespace}/{name}/endpoints`, and
`GET /v1/services/{namespace}/{name}/watch?waitSeconds=30`. Watch bounds are 1
to 30 seconds, default 30; at most 16 watches are active per daemon. Catalog
endpoints require a running, ready attempt on a fresh, available, non-draining
worker. `GET /v1/metrics` returns bounded-label Prometheus-style process and
manager observations. Metrics are not utilization or SLA guarantees.

# Pagination and payload sizes

Job and run lists default to 50 results and accept at most 100 per page. Their
opaque `cursor` is scoped to endpoint filters; pages are not a snapshot while
state changes concurrently. Run filters accept `jobId` and `state`. Log reads
default to 4,096 bytes and accept at most 65,536 bytes per chunk, with an
explicit `offset` and `stream`.

`GET /v1/runs/{runId}/logs?stream=stdout&offset=0&limit=4096` returns base64
`data`, `dataEncoding`, `offset`, `nextOffset`, `streamClosed`, `eof`, and
`truncated`. Continue with the returned `nextOffset`. `eof` is set only when the
stream is closed and that range reaches its end; an empty live read is not
EOF. Query values must select `stdout` or `stderr`, use a nonnegative offset,
and set `limit` from 1 to 65,536. The response is `404` for an unknown run and
`410 Gone` when its retained log has been pruned. The worker bundle documents
the per-stream cap and completed-run retention policy.

JSON request bodies are capped at 65,536 bytes. Requests with more than 64
header fields or more than 16,384 aggregate header-name/value bytes receive
`431`. The configurable `request_timeout_ms` defaults to 10,000 ms and ranges
from 100 to 120,000 ms. A timed-out handler does not cancel work already running
in a blocking thread; a mutation or run start can finish after the client gets
a timeout. Retry run start with the same idempotency key to recover the result.
The API has separate bounded admission for starts, general operations, control
operations, and service watches; saturation returns an overload response
rather than queuing unbounded work.

# Errors

Errors use a structured envelope with stable machine-readable code and
redacted description. A `404` indicates missing identity/resource; `409` a
state or idempotency conflict; `429` bounded admission/capacity; `503`
readiness/recovery or unavailable operations; malformed input is rejected
before manager mutation. Consult handler tests for the exact condition/status
pair rather than assuming all errors in a category share one code.
