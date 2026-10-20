---
type: Runbook
title: Operate Conductor with the CLI
description: Select named manager connection contexts, submit and manage jobs and revisions, observe and control runs, follow bounded logs, inspect workers and services, mutate desired state safely, drain workers, and page history.
status: stable
source_revision: 28dd056a30bf9ce7ab11165548ae17b66c473713
source_reviewed_at: 2026-10-20
sources:
  - resource: ../src/operator_cli.rs
    title: Operator command parsing and requests
  - resource: ../src/job_cli.rs
    title: Job and revision commands
  - resource: ../src/run_cli.rs
    title: Run lifecycle and log commands
  - resource: ../src/deployment_cli.rs
    title: Conditional deployment mutations and rollout waiting
  - resource: ../src/worker_cli.rs
    title: Worker inspection and drain lifecycle
  - resource: ../src/contexts.rs
    title: Named context configuration
  - resource: ../../conductor-api/src/lib.rs
    title: Routes invoked by the CLI
  - resource: ../../conductor/tests/agent.rs
    title: Operator CLI integration coverage
  - resource: ../../conductor/tests/run_cli.rs
    title: Run lifecycle and log CLI integration coverage
  - resource: ../../conductor/tests/deployment_cli.rs
    title: Deployment CLI integration coverage
  - resource: ../../conductor/tests/worker_cli.rs
    title: Worker CLI integration coverage
---

# Connect and inspect

The CLI uses named contexts or explicit connection flags. Contexts store a
server URL, timeout, and optional token-file and CA-file references. Token
contents are not stored in the context JSON, and context list/show omit file
paths. Context creation makes relative file references absolute. Use
`conductor context use NAME` to select a default, or `operator --context NAME`
to select one for a command. Explicit server, token-file, CA-file, and timeout
flags override the corresponding context settings.

Without a context, operator commands default to `http://127.0.0.1:8080` and do
not require a dummy token. Token files must be regular non-symlink files and
owner-only on Unix; token contents are not printed in diagnostics. Plain HTTP
is accepted only for loopback, and remote manager use requires a token file.
Use a trusted TLS proxy or a configured CA file for a remote manager.

# Submit and manage jobs

Use `conductor job register FILE` to store an immutable one-shot job without
starting it. `conductor job submit FILE --idempotency-key KEY` registers the
definition and starts one run. Both commands accept `-` in place of a file to
read JSON from stdin. `job submit` handles an identical existing definition by
reusing it; an ID with a different definition conflicts, and a deleted ID
remains reserved. Reuse the same idempotency key after an ambiguous submit
result to recover the run instead of creating a second one.

Add `--wait --wait-timeout-seconds N` to submit when the run should be observed
to completion. Timing out the local wait does not stop the remote run. Unknown
execution results are reported as unresolved and need an explicit operator
decision. JSON errors can include a `partial` object with known job and run IDs
and a safe recovery command when registration succeeded but run start did not.

```sh
conductor job register examples/cli-job.json
conductor job submit examples/cli-job.json --idempotency-key first-job --wait
conductor job list --output json
conductor job inspect mvp3-first-job --output json
conductor job revision create mvp3-first-job examples/cli-job-revision.json \
  --idempotency-key first-job-revision
conductor job revision inspect mvp3-first-job 1 --output json
conductor job delete mvp3-first-job
```

Service workload input belongs in a revision and deployment workflow. Create a
service revision and use the deployment commands to select it; do not submit a
service workload as a one-shot job. Job deletion keeps the ID reserved and
fails while a run is active or unresolved.

# Observe and control runs

`conductor run start JOB_ID --idempotency-key KEY` starts one run of a
registered job. Reuse the key only when retrying an ambiguous request. `run
list` supports job and state filters with `--limit` and `--cursor`; `run
inspect` returns one run. `run stop` explicitly requests cancellation.

`run wait RUN_ID --timeout-seconds N` polls until a terminal result or the
bounded deadline. Success exits zero; failure, cancellation, timeout, and
unknown outcomes exit nonzero with a machine-readable error code in JSON mode.
Timeout or Ctrl-C only ends local observation and never stops the remote run.
An unknown outcome requires operator judgment; `run resolve RUN_ID --reason
TEXT` records an explicit decision to abandon it. Resolution is not a retry.
`job submit --wait` uses this same polling and terminal-state behavior.

```sh
conductor run start mvp3-first-job --idempotency-key first-job-run
conductor run list --job-id mvp3-first-job --state failed --limit 25
conductor run inspect RUN_ID --output json
conductor run wait RUN_ID --timeout-seconds 30
conductor run stop RUN_ID
conductor run resolve RUN_ID --reason "verified the process cannot continue"
```

The default output is concise human text; use `--output json` for structured
output. Errors are written to stderr with a nonzero exit code. In human mode,
`run logs` writes raw bytes to stdout and retention warnings to stderr; in JSON
mode, base64 log data and its metadata are written together to stdout.

```sh
conductor operator --server https://manager.example.net \
  --token-file /etc/conductor/operator-token workers list
conductor operator --server https://manager.example.net \
  --token-file /etc/conductor/operator-token workers inspect worker-east
conductor operator --server https://manager.example.net \
  --token-file /etc/conductor/operator-token service inspect frontend
```

`service inspect` displays deployment generation and ETag, current status,
pending placement reason, rollout progress, and per-replica allocation and run
state.

# Manage deployment intent

`conductor deployment apply JOB_ID FILE` creates a deployment when no
`--generation` is supplied. To update, provide the current generation reported
by `deployment inspect`; the CLI sends it as a conditional ETag and reports a
conflict if another operator changed the deployment. The JSON file contains a
revision number, desired replica count, and optional rollout strategy. The
strategy defaults to zero surge, one unavailable replica, and a 300-second
progress deadline.

Every mutation requires a fresh idempotency key. Reuse the same key only when
retrying an ambiguous request. `--wait` observes rollout status with a bounded
deadline; acceptance of desired state is reported separately from rollout
completion. A timeout or Ctrl-C stops local observation but does not cancel the
manager's desired state. Inspect again or resume waiting to see progress. A
failed rollout returns its latest deployment state and pending reason.

Rollback requires an explicit immutable revision number. Suspension retains the
desired replica count while stopping service placements; resume schedules that
count again. Delete stops active instances and preserves deleted deployment
history. None of these commands implies cross-host traffic migration or client
cache convergence.

```sh
conductor deployment apply frontend examples/cli-deployment.json \
  --idempotency-key frontend-deploy-1 --wait
conductor deployment inspect frontend --output json
conductor deployment apply frontend examples/cli-deployment-v2.json \
  --generation 1 --idempotency-key frontend-deploy-2 --wait
conductor deployment scale frontend --replicas 3 --generation 2 \
  --idempotency-key frontend-scale-3 --wait
conductor deployment rollback frontend --revision 1 --generation 3 \
  --idempotency-key frontend-rollback-1 --wait
conductor deployment suspend frontend --generation 4 \
  --idempotency-key frontend-suspend-1 --wait
conductor deployment resume frontend --generation 5 \
  --idempotency-key frontend-resume-1 --wait
conductor deployment delete frontend --generation 6 \
  --idempotency-key frontend-delete-1 --wait
```

# Inspect workers and manage drains

`worker list` and `worker inspect WORKER_ID` report capacity, available slots,
connectivity freshness, readiness and drain state. A disconnected worker stays
in the inventory but is marked unavailable or stale. An empty worker inventory
is a successful empty result.

Draining excludes the worker from new placements and withdraws its service
catalog endpoints. Existing runs are not guaranteed to migrate or terminate;
service replacements may be pending if no eligible capacity is available.
`drain` acceptance is distinct from completion. Waits are bounded, and timeout
or Ctrl-C stops observation while leaving the accepted drain in place. A
completed drain must be explicitly cleared before the worker can be scheduled
again.

```sh
conductor worker --token-file ./operator-token list
conductor worker --token-file ./operator-token inspect worker-east
conductor worker --token-file ./operator-token drain worker-east \
  --grace-period-seconds 30 --wait --wait-timeout-seconds 300
conductor worker --token-file ./operator-token drain-status worker-east
conductor worker --token-file ./operator-token drain-wait worker-east \
  --timeout-seconds 300
conductor worker --token-file ./operator-token undrain worker-east
conductor run list --job-id frontend --state unknown --limit 25
```

Run-list pages expose `nextCursor`; pass it with `--cursor` for the next page.
Use `worker undrain` only after drain status reports `completed`.

# Configure and inspect worker overlays

Overlay commands create and inspect manager-owned network domains, list desired
peer assignments with lease state and the latest worker apply report, and revoke
a worker peer. Use the operator API; do not edit manager SQLite state directly.
A report is fresh only when it matches the desired generation and its timestamp
is within the last 60 seconds. It separates the helper's last successful
configuration apply from reported TUN address/MTU, installed routes, active
direct Iroh links, handshake times, and transport UDP byte counters. Those
counters include protocol overhead, not application payload. Worker reports
do not prove application reachability; `reachability` remains `not_verified`
until an operator runs an explicit probe. On an overlay-enabled worker,
`conductor overlay probe` performs a bounded HTTP GET over the local worker's
overlay route to an assigned private IPv4 address and workload port. Supply a
safe health path explicitly. The probe disables proxies and redirects, limits
the total request to five seconds and the response body to 65,536 bytes. It
reports probe success only after receiving a 2xx response; any complete HTTP
response is separately marked `application_response_received`, including
non-2xx status codes. It tests application traffic from that worker; it is not
a manager-side or cross-host qualification and does not measure throughput.
Revoke output continues to mark reachability `not_verified`.

```sh
conductor operator --context production overlay create workers-east \
  --worker-address-pool 10.90.0.0/24 \
  --service-address-pool 10.90.1.0/24
conductor operator --context production overlay inspect workers-east
conductor operator --context production --output json overlay peers workers-east
conductor operator --context production overlay revoke workers-east worker-17
conductor overlay probe --target-ip 10.90.0.17 --port 8080 --path /health
```

# Retrieve logs

Each `run logs` request fetches one chunk between 1 and 65,536 bytes. Human
mode writes exact decoded bytes to stdout, including non-UTF-8 data. JSON mode
writes one base64 JSON record per fetched chunk with its offset, `nextOffset`,
`eof`, and `truncated` metadata. `--follow` resumes from each returned byte
offset until the stream reaches EOF; it does not duplicate bytes. Retention
truncation is reported on stderr in human mode and in every JSON record. Log
content can contain workload-generated data; save it to a protected destination.

```sh
conductor run logs run-42 --stream stderr --offset 0 --limit 8192 \
  > run-42.stderr
conductor run logs run-42 --stream stderr --offset 0 --limit 8192 --follow
conductor run logs run-42 --stream stderr --offset 0 --limit 8192 \
  --follow --output json
```
