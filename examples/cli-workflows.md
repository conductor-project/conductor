# CLI workflow fixtures

These JSON files are inputs for the MVP3 CLI workflows. The one-shot job
fixture is [`job.json`](job.json); the long-lived service example is split
between [`cli-service-revision.json`](cli-service-revision.json) and
[`cli-service-deployment.json`](cli-service-deployment.json).

The service fixture starts `/bin/sleep` so it can demonstrate desired replicas,
rollout readiness, scale, and rollback without relying on an external service
binary. It does not publish a network endpoint. Before creating the revision,
replace the all-zero SHA-256 placeholder with the lowercase SHA-256 digest of
`/bin/sleep` on the worker host. The process must be available at that path on
every worker that may run the service. The deployment fixture selects revision
1 and requests one replica; use a fresh job ID when repeating the workflow
because deleted job IDs remain reserved.

These are fixture inputs, not a substitute for the CLI commands. The root
README should show commands only as their corresponding MVP3 operations land.
