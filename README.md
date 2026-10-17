# Conductor

Conductor is an early-stage Linux daemon for managing and running jobs. MVP1 targets one `conductor` binary that combines manager and worker responsibilities behind explicit interfaces. It validates and stores immutable job definitions, starts local host processes, and records run observations through a loopback HTTP API. Job definitions and run history survive agent restarts; a process whose outcome becomes ambiguous across a crash is marked `unknown` and is not automatically replayed.

The host process backend runs with the daemon user's operating-system permissions and network access. It is not a security sandbox. Each process starts in a dedicated process group; ordinary group members are force-cleaned after the leader exits and during daemon shutdown. A process can deliberately leave its group, so this mechanism is not containment.

## Requirements

- Linux for the supported daemon deployment.
- Rust 1.99.1 with `rustfmt` and `clippy` (installed by `rust-toolchain.toml`).
- `curl` and `jq` for the HTTP walkthrough.

## Build and check

```sh
python3 scripts/check-dependency-policy.py
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --locked --all-targets --all-features -- -D warnings
cargo test --workspace --locked --all-targets --all-features
cargo test --release --workspace --locked --all-targets --all-features
cargo build --workspace --locked --release
```

Release builds use fat LTO and one codegen unit, which increase compile time and may reduce binary size or improve optimization opportunities. They do not promise the fastest performance for every workload. Overflow checks, debug assertions, unwind behavior, and line tables remain enabled to retain correctness checks and useful diagnostics. Panics indicate bugs; cleanup must be implemented explicitly.

## Run the agent

```sh
cargo build --workspace --locked
./target/debug/conductor agent --config examples/local.json
curl --fail http://127.0.0.1:8080/healthz
```

The agent prints a structured startup event with its bound address to stderr. Stop it with Ctrl-C or SIGTERM and verify it exits successfully. `--role all` runs the combined baseline; `--role manager` and `--role worker` are recognized but fail as unsupported until remote transport is available. A CLI role overrides the JSON `role` value.

## Job definition API

The API listens only on loopback. Register a job using the checked-in [JSON example](examples/job.json):

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @examples/job.json http://127.0.0.1:8080/v1/jobs
curl --fail http://127.0.0.1:8080/v1/jobs
curl --fail http://127.0.0.1:8080/v1/jobs/hello
curl --fail -X DELETE http://127.0.0.1:8080/v1/jobs/hello
```

`POST /v1/jobs` returns 201 with the validated immutable definition; duplicate and previously deleted IDs return 409. `GET /v1/jobs` returns `{"jobs":[...],"nextCursor":...}` in ascending ID order. `GET /v1/jobs/{id}` returns 200 or 404, and `DELETE /v1/jobs/{id}` returns 204 or 404. Deletion reserves the ID so it cannot refer to a different definition later. The manager accepts up to 1,000 active jobs; deleted-ID reservations remain durable. Request bodies are limited to 65,536 bytes.

Start and inspect a run using the checked-in [success example](examples/job.json), [failure example](examples/job-failure.json), [cancellable example](examples/job-cancellable.json), and [deadline example](examples/job-timeout.json):

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @examples/job.json http://127.0.0.1:8080/v1/jobs
curl --fail-with-body -X POST -D - http://127.0.0.1:8080/v1/jobs/hello/runs
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @examples/job-failure.json http://127.0.0.1:8080/v1/jobs
curl --fail-with-body -X POST -D - http://127.0.0.1:8080/v1/jobs/failure/runs
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @examples/job-cancellable.json http://127.0.0.1:8080/v1/jobs
run_id=$(curl --fail-with-body -X POST http://127.0.0.1:8080/v1/jobs/cancellable/runs | jq -r .runId)
curl --fail-with-body -X POST "http://127.0.0.1:8080/v1/runs/$run_id/stop"
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @examples/job-timeout.json http://127.0.0.1:8080/v1/jobs
curl --fail-with-body -X POST http://127.0.0.1:8080/v1/jobs/deadline/runs
curl --fail http://127.0.0.1:8080/v1/runs
```

Run start returns 202 and a relative `Location: /v1/runs/{runId}` header. Poll that URL for `requested`, `starting`, `running`, then a terminal state. `GET /v1/runs` accepts `jobId`, `state`, `limit`, and `cursor`; both list endpoints default to 50 entries and allow at most 100. Responses provide an opaque `nextCursor` when another page exists. Job pages are ordered by ID; run pages use descending insertion order. Cursors are bound to endpoint filters. They keep a scan from repeating earlier rows while concurrent inserts arrive; run state changes or job deletion between pages can affect which filtered rows remain visible, so pages are not a cross-request snapshot. Results include requested/start/finish timestamps, exit code or termination signal, a structured `failureReason`, and an optional `terminationReason`. Run IDs are not process IDs. Starting a job that already has an active or unresolved run, or deleting it while that run is held, returns 409. A full local worker returns 429 with `Retry-After: 1`. Job `timeoutMs` starts when the process is launched and is measured with a monotonic clock; queue/admission time does not count.

Run submission accepts an optional `Idempotency-Key` header containing 1–128 ASCII letters, digits, `.`, `_`, `~`, or `-`. Keys are global to this manager database and are retained with their run history. Repeating the same key for the same immutable job returns the original run, including after completion and restart. Reusing it for another job returns 409; validation and capacity rejection do not consume a key. This deduplicates the request and process launch, but it cannot make external process side effects exactly once. Up to 32 start requests are admitted concurrently; excess new starts return 429 with `Retry-After: 1`. Health, stop, result, and log requests do not use these start slots.

```sh
curl --fail-with-body -X POST -H 'Idempotency-Key: demo-start-1' \
  http://127.0.0.1:8080/v1/jobs/hello/runs
curl --fail http://127.0.0.1:8080/v1/jobs?limit=50
curl --fail 'http://127.0.0.1:8080/v1/runs?jobId=hello&limit=50'
```

Stop a run with `POST /v1/runs/{runId}/stop`. The manager records `stopping` before dispatch, and the endpoint returns 202 while cleanup is in progress. Repeating the request is safe; stopping an already terminal run returns its unchanged result with 200. The worker sends SIGTERM to the owned process group, waits the job's `terminationGraceMs`, then sends SIGKILL to remaining ordinary group members. A natural exit wins if the supervisor observes it before a stop/deadline request; after termination intent is recorded, the terminal state is `cancelled` or `timed_out`, with the actual exit code/signal preserved. `terminationReason` is `cancelled`, `deadline_exceeded`, or `shutdown`. Cancellation does not roll back external side effects.

On SIGINT/SIGTERM, the worker immediately stops admission and requests graceful termination for all active runs concurrently. After five seconds it force-kills remaining owned groups and allows up to one additional second for reaping and output flush. Existing API connections receive Axum graceful shutdown during a five-second server window. These windows bound shutdown to at most eleven seconds under ordinary local process behavior; a workload that escapes its process group is outside this cleanup guarantee. `unknown` and `abandoned` remain reserved for recovery/operator-resolution work; `unknown` is used when launch or outcome becomes ambiguous after restart.

Runs are committed to manager storage before dispatch. The worker has its own SQLite database under `worker_data_dir`; accepted run specifications and observations are committed before a process launches. Repeating the same worker `StartRun` command returns the existing observation, while reusing its run ID with a different specification is a conflict. After a daemon restart, any worker run that was not known terminal becomes `unknown` and keeps its job and capacity reservation. It is not automatically replayed; this avoids claiming exactly-once execution when a crash leaves process launch ambiguous. `unknown` and `abandoned` states reserve their future lifecycle semantics. Standard output and error are continuously drained into private per-run files. Each stream stores at most `log_max_bytes_per_stream` bytes (8 MiB by default); excess bytes are discarded while draining continues, and the response marks the stream as truncated. The worker retains logs for the newest 100 completed runs by default. Pruning waits until both streams close and keeps run history intact. The configured bounds apply to log files: retained completed runs plus active runs, each with two capped streams. SQLite run and retention metadata remain until separately managed and can grow over time.

Retrieve output in bounded byte ranges with `GET /v1/runs/{runId}/logs?stream=stdout&offset=0&limit=4096`. `stream` is required (`stdout` or `stderr`); `offset` defaults to 0 and `limit` defaults to 4096, with an allowed range of 1 to 65,536 bytes. The JSON response contains Base64 `data`, the requested and next byte offsets, `streamClosed`, `eof`, and `truncated`. Use `nextOffset` for the next range. `eof` becomes true only after the stream closes and the requested range reaches its end; an empty live read is not EOF. Invalid query values return 400, unknown runs return 404, and logs removed by retention return 410.

Example:

```sh
run_id=$(curl --fail-with-body -X POST http://127.0.0.1:8080/v1/jobs/hello/runs | jq -r .runId)
curl --fail-with-body "http://127.0.0.1:8080/v1/runs/$run_id/logs?stream=stdout&offset=0&limit=4096" \
  | jq -r .data | base64 -d
```

Definitions require an absolute executable path and absolute working directory. Arguments are passed directly to the executable without an implicit shell. Environment inheritance defaults to false; only the explicit `env` map is added unless `inheritEnv` is set true. Unknown fields and invalid paths, identifiers, environment entries, and duration bounds are rejected with a stable JSON error envelope. Registration only stores the definition; it never launches the command.

Configuration is strict JSON. The listener must bind to IPv4 or IPv6 loopback. Manager and worker state directories must be separate, worker ID must be stable and valid, and concurrency must be between 1 and 1,000. `log_max_bytes_per_stream` sets the per-run stdout and stderr cap (1 byte to 1 GiB); `log_retention_runs` sets the number of newest completed runs whose logs are retained (0 to 10,000). The daemon canonicalizes both state paths and holds OS file locks on them for its lifetime; concurrent processes using either directory, including through a symlink alias, fail startup. Lock files left by a crashed process do not block a later start because the OS releases the lock when the process exits. On Unix, state directories use mode `0700`, and the database and lock files use `0600`.

The manager database is `manager.sqlite3` in `manager_data_dir`. SQLite uses WAL journaling with `synchronous=FULL`; database schema changes run as versioned transactions. Startup fails closed if the database is corrupt, inaccessible, or has a schema version newer than the daemon supports. Back up the manager state directory while the daemon is stopped so the database and WAL state remain consistent. `127.0.0.1:0` is supported for integration tests.

## Architecture

HTTP handlers in `conductor-api` call manager operations. `conductor-manager` owns desired state and assignment. `conductor-worker` owns process supervision, worker persistence, deadlines, logs, and observed execution state. `conductor-process` contains host process mechanics. `conductor-core` holds shared contracts only. The `conductor` binary composes the pieces through explicit interfaces. See [AGENTS.md](AGENTS.md) for boundaries and contribution rules.

## Contribution

Read [AGENTS.md](AGENTS.md) and [dependency-decisions.md](dependency-decisions.md) before changing architecture or dependencies. All repository work uses the primary checkout, never a Git worktree. Start each issue branch from the latest default branch and deliver changes through a pull request; never push directly to `main`.
