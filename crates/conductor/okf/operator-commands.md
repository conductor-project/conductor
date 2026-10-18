---
type: Runbook
title: Operate Conductor with the CLI
description: Inspect workers and services, mutate desired state safely, drain workers, page run history, and retrieve bounded logs.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/operator_cli.rs
    title: Operator command parsing and requests
  - resource: ../../conductor-api/src/lib.rs
    title: Routes invoked by the CLI
  - resource: ../../conductor/tests/agent.rs
    title: Operator CLI integration coverage
---

# Connect and inspect

The CLI uses the protected operator bearer token. Token files must be regular
non-symlink files and owner-only on Unix; token contents are not printed in
diagnostics. Plain HTTP is accepted only for loopback. Use a trusted TLS proxy
for a remote manager.

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
