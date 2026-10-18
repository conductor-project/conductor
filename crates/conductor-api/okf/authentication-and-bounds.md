---
type: Reference
title: API authentication, request bounds, and operations
description: Operator routes can require bearer authentication, while request size, handler concurrency, service watches, and output are bounded.
status: stable
source_revision: 663bd0f
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/server.rs
    title: Auth middleware, bounds, metrics, and readiness
  - resource: ../src/client.rs
    title: Client authentication and response bounds
  - resource: ../src/structs/bearer_token_authenticator.rs
    title: Token authentication
  - resource: ../src/structs/api_readiness.rs
    title: Readiness state
  - resource: ../../conductor/tests/agent.rs
    title: Auth and bounds tests
---

# Authentication

Local mode is unauthenticated and should bind only to loopback. Operator token
mode authenticates manager operator routes using a token file; health and
readiness remain public so a local supervisor can check process state. Store
the token with restrictive permissions. For remote operators, terminate TLS at
a trusted loopback proxy rather than exposing the local cleartext API.

Create the token with the CLI, which writes it once to a new owner-only file
and refuses to overwrite an existing path:

```sh
conductor create-operator-token --output /etc/conductor/operator-token
```

Configure `api_access.mode` as `operator_token_file` and point `token_file` at
the protected file. Relative token paths resolve from the configuration file's
directory:

```json
{
  "role": "all",
  "listen": "127.0.0.1:8080",
  "manager_data_dir": "/var/lib/conductor/manager",
  "worker_data_dir": "/var/lib/conductor/worker",
  "worker_id": "local-worker",
  "worker_concurrency": 4,
  "api_access": {
    "mode": "operator_token_file",
    "token_file": "/etc/conductor/operator-token"
  }
}
```

Every `/v1` request must send `Authorization: Bearer <token>`. The CLI
requires a regular non-symlink token file with no group or other permissions
on Unix. Protect the token and its parent directory; never put token contents
in logs, command-line arguments, or configuration files. Invalid or missing
credentials return `401` with a `WWW-Authenticate` challenge. `/healthz` and
`/readyz` stay public and expose only basic health/readiness.

To rotate a token, create a replacement at a new protected path, update
`token_file`, and restart the daemon. It reads one token at startup, so the
restart invalidates the old token. Distribute the replacement through the
existing protected operator channel, then remove the old file securely after
clients have switched. Operator credentials remain distinct from worker
certificates and enrollment tokens.

# Bounds and concurrency

Request JSON is limited to 65,536 bytes; header count is limited to 64 and
aggregate header names/values to 16,384 bytes. Log chunks default to 4,096
bytes and cap at 65,536. Request processing timeout defaults to 10 seconds and
is configurable from 100 ms to 120 seconds. The deadline does not cancel
blocking work already in progress; a mutation may commit after the response
times out. Retry run start with the same idempotency key to recover the accepted
run.

The API bounds concurrent run starts (32), general blocking operations (64),
separate control operations (8), and active service watches (16). Service
watches time out at 30 seconds. Saturated operations return bounded overload
responses instead of growing an unbounded queue. See
[route contracts](routes-and-contracts.md) for pagination, cursors, and ETags.

# Readiness and metrics

`/healthz` is liveness. `/readyz` reflects startup recovery, worker freshness,
draining, and degraded state. A full worker can be ready; capacity is checked
when work is admitted. Prometheus-style metrics have bounded labels and
aggregate observations; they are not SLA claims. Audit events omit raw request
contents, secrets, and full commands.

`GET /v1/metrics` follows the configured API access policy. It exports aggregate
series only: job, run, worker, command, service, token, and other user-controlled
values are not labels. Current gauges count deployments by fixed status,
pending work by fixed kind/reason, retried or terminal service attempts,
workers by connectivity/health/freshness state, and configured service owners.

| Metric | Type | Meaning |
| --- | --- | --- |
| `conductor_deployments{status}` | Gauge | Non-deleted deployments by `pending`, `ready`, `suspended`, or `deleting`. |
| `conductor_pending_work{kind,reason}` | Gauge | Pending allocations and deployments by the fixed pending-reason vocabulary. |
| `conductor_service_attempts{state}` | Gauge | Instances that retried or reached terminal failure. |
| `conductor_workers{state}` | Gauge | Worker connectivity, health, and inventory freshness groups. |
| `conductor_discovery_service_owners` | Gauge | Number of configured logical service owners, not endpoint count. |
| `conductor_manager_reconcile_total` | Counter | Service reconciliation passes since this manager process started. |
| `conductor_manager_reconcile_failures_total` | Counter | Failed reconciliation passes since this manager process started. |
| `conductor_manager_reconcile_duration_seconds` | Histogram | Complete service reconciliation duration. |

Manager reconciliation exports process-lifetime pass and failure counters plus
a duration histogram with fixed bounds 0.005, 0.025, 0.1, 0.5, 1, and 5
seconds. Those counters reset on restart; durable gauges are reconstructed from
current manager state. Worker status-query failures count as unavailable, and
freshness is `unknown` before manager-enriched inventory is available. The
bounded label vocabulary keeps the current series count below 40; this is an
implementation bound, not a general Prometheus cardinality guarantee.

Manager audit events are append-only rows in schema version 12, ordered by a
database sequence. HTTP mutation outcomes record principal, fixed action name,
resource type/ID, success or failure, HTTP status, and timestamp. They omit
request bodies, query strings, headers, credentials, commands, and environment
values. Rejected `429` requests are not mutation events; a daemon failure
between an operation and its audit write can leave no result event. Enrollment
issuance, worker enrollment, certificate renewal, and revocation are recorded
in the same SQLite transaction as identity changes. Principals are bounded
(`operator`, `local_operator`, `worker_enrollment`); a shared bearer token does
not identify individual people. Direct in-process manager calls and CLI
bootstrap commands that do not call the HTTP API do not create operator API
mutation events; `conductor operator` mutations go through the audited HTTP
routes. Worker identity lifecycle changes are recorded at the repository
transaction boundary. There is no remote audit read endpoint or built-in
retention/export/tamper evidence across database replacement.

Audited operator API actions include job create/delete, revision creation,
deploy/scale/suspend/resume/rollback, run start/stop/resolve, and worker
drain/clear. The job ID for creation is carried as validated response metadata;
the audit layer does not inspect the request body. HTTP mutations that time out
at the client may continue in a blocking worker and are audited when their
handler eventually returns; a manager failure before the audit write can leave
the outcome without an event.

# Security limits

Bearer authentication does not encrypt network traffic. It depends on
filesystem protection for the token and a trusted TLS proxy for remote use.
The job process backend runs with daemon-account permissions; API auth does not
sandbox submitted jobs. See repository [self-hosting guidance](../../../okf/self-hosting.md).
