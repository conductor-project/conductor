# Operator CLI

The `conductor operator` commands call the manager's authenticated HTTP API.
They use stable worker, job, service-instance, allocation, and run IDs. The
token file must be a regular file with owner-only permissions on Unix. The CLI
does not follow token-file symlinks, include token contents in diagnostics, or
follow HTTP redirects. Plain HTTP is accepted only for loopback; use HTTPS
through a trusted TLS proxy for remote access.

```sh
conductor operator --server https://manager.example.net \
  --token-file /etc/conductor/operator-token workers list

conductor operator --server https://manager.example.net \
  --token-file /etc/conductor/operator-token workers inspect worker-east

conductor operator --server https://manager.example.net \
  --token-file /etc/conductor/operator-token service inspect frontend
```

`service inspect` returns deployment generation and ETag, status, pending
placement reason, rollout progress, and per-replica allocation and run state.
The per-replica records let an operator identify unavailable workers without
guessing from replica order.

Create an initial deployment with `--create`. Later updates require the current
generation from `service inspect`. Supply a fresh idempotency key for each
mutation; keep the same key when retrying an ambiguous request.

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

The CLI exposes bounded worker drains and run pagination. `logs` requests one
chunk of 1 to 65,536 bytes, writes decoded log bytes to stdout, and writes
`nextOffset`, `eof`, and `truncated` metadata to stderr. Request later chunks
with the returned offset.

```sh
conductor operator --token-file ./operator-token drain start worker-east \
  --grace-period-seconds 30
conductor operator --token-file ./operator-token drain status worker-east
conductor operator --token-file ./operator-token runs \
  --job-id frontend --state unknown --limit 25
conductor operator --token-file ./operator-token logs run-42 \
  --stream stderr --offset 0 --limit 8192 > run-42.stderr
```

Run-list pages contain `nextCursor`; pass it as `--cursor` for the next page.
Log output may contain workload-generated data. Direct it to an appropriately
protected destination.
