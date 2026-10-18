# Metrics and audit events

## Metrics

`GET /v1/metrics` returns Prometheus text format and follows the configured API
access policy. In authenticated mode it requires the operator bearer token.
The endpoint exports aggregates only: no job, run, worker, command, service
name, token, or other user-controlled value appears in a label.

Metric meanings:

| Metric | Type | Meaning |
| --- | --- | --- |
| `conductor_deployments{status}` | Gauge | Current non-deleted deployments grouped as `pending`, `ready`, `suspended`, or `deleting`. |
| `conductor_pending_work{kind,reason}` | Gauge | Pending allocations and deployments grouped by the fixed pending-reason vocabulary. |
| `conductor_service_attempts{state}` | Gauge | Current service instances that have retried at least once or reached terminal failure. |
| `conductor_workers{state}` | Gauge | Current worker counts grouped by connectivity (`available`, `unavailable`), health (`degraded`, `draining`), and inventory freshness (`fresh`, `stale`, `freshness_unknown`). |
| `conductor_discovery_service_owners` | Gauge | Number of configured service identity owners in the manager catalog. This is a count of registered names, not a count of endpoints. |
| `conductor_manager_reconcile_total` | Counter | Service reconciliation passes, including placement and scheduling work, since this manager process started. |
| `conductor_manager_reconcile_failures_total` | Counter | Reconciliation passes that returned an error since this manager process started. |
| `conductor_manager_reconcile_duration_seconds` | Histogram | Wall time for each complete service reconciliation pass. Bounds are 0.005, 0.025, 0.1, 0.5, 1, and 5 seconds, plus `+Inf`. |

Labels use only fixed enums: at most 4 deployment statuses, 13 pending-work
combinations, 2 service-attempt states, and 7 worker states. The reconciliation histogram has 7 fixed
buckets and no labels. The endpoint therefore has a fixed series ceiling of
fewer than 40 series, independent of the number or names of workloads. The
reconciliation counters reset on process restart; durable pending, deployment,
retry, catalog-owner, and worker state is read from current manager state on
each scrape. Worker freshness is `unknown` when a worker has no manager-enriched
inventory snapshot. A failed worker status query contributes to `unavailable`.

## Audit events

Audit events are stored in the manager's SQLite database in schema version 12.
They are append-only and ordered by a database sequence. After an operator
mutation handler returns, the API appends a `success` or `failure` event with
the HTTP status. The handler continues in the background if the HTTP processing
deadline expires, preserving the API's existing timed-out-work behavior; its
event records the handler's eventual outcome. Requests rejected by the bounded
blocking-admission gate (`429`) are not recorded as mutations. If the manager
stops between a mutation and its audit write, that action may have no result
event.

Audited API actions include job creation/deletion, revision creation, deploy,
scale, suspend, resume, rollback, run start/stop/resolve, and worker drain or
drain cancellation. Enrollment credential issuance, successful worker
enrollment, certificate renewal, and worker revocation are written in the same
SQLite transaction as their corresponding identity changes. Principals are bounded role identifiers:
`operator`, `local_operator`, and `worker_enrollment`. The current bearer-token
configuration represents one shared operator principal; it does not identify
individual human users.

Events contain the principal, fixed action name, resource type, stable resource
ID, outcome, status code, and timestamp. For job creation, the API carries the
validated ID to the audit layer as response metadata; it does not inspect or
copy the request body. Request bodies, queries, headers, credentials,
commands, and environment values are not copied into audit records. The
operator CLI and other direct in-process manager calls are not HTTP mutations
and do not create operator audit events; worker identity lifecycle changes are
audited at the repository boundary.

The API does not expose a remote audit log endpoint yet. Audit retention,
export, and tamper evidence across manager-database replacement are not
provided by this schema.
