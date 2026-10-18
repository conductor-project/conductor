---
type: Runbook
title: Operate Conductor with the CLI
description: Select named manager connection contexts, submit and manage jobs and revisions, inspect workers and services, mutate desired state safely, drain workers, page run history, and retrieve bounded logs.
status: stable
source_revision: c696ef83dd136b8317c3c440ad4e4e2401c54a8c
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/operator_cli.rs
    title: Operator command parsing and requests
  - resource: ../src/job_cli.rs
    title: Job and revision commands
  - resource: ../src/contexts.rs
    title: Named context configuration
  - resource: ../../conductor-api/src/lib.rs
    title: Routes invoked by the CLI
  - resource: ../../conductor/tests/agent.rs
    title: Operator CLI integration coverage
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

The default output is concise human text; use `--output json` for structured
output. Errors are written to stderr with a nonzero exit code. Log bytes are
written to stdout and log metadata to stderr so binary log data stays intact.

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

# Mutate service intent

Initial deployment uses `--create`. Later updates require the current
generation from `service inspect`. Supply a fresh idempotency key for each
mutation and reuse that same key if retrying after an ambiguous client result.
Refresh the generation after each accepted mutation; stale generations are
rejected instead of overwriting newer desired state.

```sh
conductor operator --token-file ./operator-token deploy frontend \
  --create --revision 1 --replicas 2 --idempotency-key frontend-deploy-1
conductor operator --token-file ./operator-token scale frontend \
  --replicas 3 --if-match-generation 1 --idempotency-key frontend-scale-3
conductor operator --token-file ./operator-token deploy frontend \
  --revision 2 --replicas 3 --if-match-generation 2 \
  --idempotency-key frontend-deploy-2
conductor operator --token-file ./operator-token rollback frontend \
  --revision 1 --if-match-generation 3 --idempotency-key frontend-rollback-1
```

# Drain a worker and inspect runs

Draining excludes the worker from new placements and withdraws its service
catalog endpoints. Completion can be blocked while a service replacement lacks
capacity or an old run's termination remains unconfirmed. A completed drain
must be explicitly cleared before the worker can be scheduled again.

```sh
conductor operator --token-file ./operator-token drain start worker-east \
  --grace-period-seconds 30
conductor operator --token-file ./operator-token drain status worker-east
conductor operator --token-file ./operator-token drain clear worker-east
conductor operator --token-file ./operator-token runs \
  --job-id frontend --state unknown --limit 25
```

Run-list pages expose `nextCursor`; pass it with `--cursor` for the next page.
Use `drain clear` only after status reports `completed`.

# Retrieve logs

Each `logs` request fetches one chunk between 1 and 65,536 bytes. It writes
decoded bytes to stdout and `nextOffset`, `eof`, and `truncated` metadata to
stderr. Request another chunk using the returned offset. Log content can
contain workload-generated data; save it to a protected destination.

```sh
conductor operator --token-file ./operator-token logs run-42 \
  --stream stderr --offset 0 --limit 8192 > run-42.stderr
```
