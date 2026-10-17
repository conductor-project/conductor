# Conductor

Conductor is an early-stage Linux daemon for managing and running jobs. MVP1 targets one `conductor` binary that combines manager and worker responsibilities behind explicit interfaces. It validates and stores immutable job definitions, starts local host processes, and records run observations through a loopback HTTP API. Job definitions and run history survive agent restarts; a process whose outcome becomes ambiguous across a crash is marked `unknown` and is not automatically replayed.

The host process backend runs with the daemon user's operating-system permissions and network access. It is not a security sandbox. Each process starts in a dedicated process group; ordinary group members are force-cleaned after the leader exits and during daemon shutdown. A process can deliberately leave its group, so process groups alone are not containment. Optional Linux cgroup v2 enforcement places the launcher in a per-run cgroup before it executes the job, applies CPU, memory, swap, and process-count limits, and cleans up descendants that leave the process group. It does not provide filesystem or network isolation, and workloads sharing the daemon UID may interfere with cgroup files if the administrator has not protected the delegated hierarchy.

## Requirements

- Linux for the supported daemon deployment.
- Rust 1.99.1 with `rustfmt` and `clippy` (installed by `rust-toolchain.toml`).
- `curl` and `jq` for the HTTP walkthrough.
- Python 3.9 or newer for the reproducible agent demo and release probe.

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
curl --fail http://127.0.0.1:8080/readyz
```

The agent prints a structured startup event with its bound address to stderr. `/healthz` reports liveness; `/readyz` reports whether recovery permits new work. Stop it with Ctrl-C or SIGTERM and verify it exits successfully. `--role all` runs the combined baseline; `--role manager` and `--role worker` are recognized but fail as unsupported until remote transport is available. A CLI role overrides the JSON `role` value.

### Reproducible lifecycle demo

After building the release binary, run the checked-in client demo:

```sh
cargo build --workspace --locked --release
python3 scripts/demo-agent.py --binary target/release/conductor
```

The demo creates isolated manager/worker state and an ephemeral loopback listener, waits for `/readyz`, then exercises job registration, concurrent runs within capacity, concurrent idempotency-key replay, stdout/stderr retrieval, bounded large-output retention, stop, process failure, launch failure, timeout, capacity rejection, graceful shutdown, restart, persisted results, and retained logs. It prints a success line only after these observations pass. `--include-crash` adds a separate crash scenario: the fixture records its own child PID, the demo terminates that exact PID, verifies cleanup, then explicitly resolves the recovered `unknown` run as `abandoned`. The script cleans up its agent and fixture processes on failure or interruption. Failures print recent run states and bounded daemon diagnostics. CI runs this demo against the release binary.

## Job definition API

The API listens only on loopback. `/healthz` is liveness and `/readyz` is readiness; startup recovery and shutdown return 503 from `/readyz`. `GET /v1/workers` returns one bounded snapshot for the local worker, and `GET /v1/workers/{id}` returns that worker or 404. Snapshots include stable worker ID, incarnation, process capability, configured/used/available slots, unknown-run reservations, availability/draining state, and observation time. These are local operational readings, not distributed heartbeats or leases. A full worker can still be ready; run admission enforces capacity separately. Register a job using the checked-in [JSON example](examples/job.json):

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @examples/job.json http://127.0.0.1:8080/v1/jobs
curl --fail http://127.0.0.1:8080/v1/jobs
curl --fail http://127.0.0.1:8080/v1/jobs/hello
curl --fail -X DELETE http://127.0.0.1:8080/v1/jobs/hello
```

`POST /v1/jobs` returns 201 with the validated immutable definition; duplicate and previously deleted IDs return 409. `GET /v1/jobs` returns `{"jobs":[...],"nextCursor":...}` in ascending ID order. `GET /v1/jobs/{id}` returns 200 or 404, and `DELETE /v1/jobs/{id}` returns 204 or 404. Deletion reserves the ID so it cannot refer to a different definition later. The manager accepts up to 1,000 active jobs; deleted-ID reservations remain durable. Request bodies are limited to 65,536 bytes. Requests with more than 64 header fields or over 16,384 aggregate header-name/value bytes receive 431. HTTP header reads use Hyper's bounded parser defaults; the handler processing deadline is configurable with `request_timeout_ms` (100–120,000 ms, default 10,000). Execution deadlines are independent. Synchronous storage and worker API operations run outside the async HTTP executor, with at most 64 general operations and 8 separately reserved stop or resolution operations in flight. Worker process concurrency is capped at 64 pending Linux qualification; saturated admission returns 429 with `Retry-After: 1`. A request timeout does not cancel an operation already running in a blocking thread; it can still finish and commit afterward, and its admission slot remains occupied until completion. SQLite or filesystem locks can delay the operation itself. A timed-out start may already have been accepted; retry it with the same `Idempotency-Key` to recover its run identity.

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

Run outcomes are observable through `GET /v1/runs/{runId}`:

| Outcome | Meaning |
| --- | --- |
| `succeeded` | The process exited with status 0. |
| `failed` | The process exited nonzero or could not be launched; inspect `failureReason` and `exitCode`. |
| `cancelled` | An operator stop or graceful daemon shutdown terminated the process group; inspect `terminationReason`. |
| `timed_out` | The configured execution deadline expired; `terminationReason` is `deadline_exceeded`. |
| `unknown` | Restart recovery cannot determine whether the process ran or finished; capacity remains reserved pending operator action. |
| `abandoned` | An operator explicitly resolved an `unknown` run after external cleanup; this is not a successful execution result. |

Run submission accepts an optional `Idempotency-Key` header containing 1–128 ASCII letters, digits, `.`, `_`, `~`, or `-`. Keys are global to this manager database and are retained with their run history. Repeating the same key for the same immutable job returns the original run, including after completion and restart. Reusing it for another job returns 409; validation and capacity rejection do not consume a key. This deduplicates the request and process launch, but it cannot make external process side effects exactly once. Up to 32 start requests are admitted concurrently; excess new starts return 429 with `Retry-After: 1`. Health, stop, result, and log requests do not use these start slots.

```sh
curl --fail-with-body -X POST -H 'Idempotency-Key: demo-start-1' \
  http://127.0.0.1:8080/v1/jobs/hello/runs
curl --fail http://127.0.0.1:8080/v1/jobs?limit=50
curl --fail 'http://127.0.0.1:8080/v1/runs?jobId=hello&limit=50'
```

Stop a run with `POST /v1/runs/{runId}/stop`. The manager records `stopping` before dispatch, and the endpoint returns 202 while cleanup is in progress. Repeating the request is safe; stopping an already terminal run returns its unchanged result with 200. The worker sends SIGTERM to the owned process group, waits the job's `terminationGraceMs`, then sends SIGKILL to remaining ordinary group members. A natural exit wins if the supervisor observes it before a stop/deadline request; after termination intent is recorded, the terminal state is `cancelled` or `timed_out`, with the actual exit code/signal preserved. `terminationReason` is `cancelled`, `deadline_exceeded`, or `shutdown`. Cancellation does not roll back external side effects.

On SIGINT/SIGTERM, the worker immediately stops admission and requests graceful termination for all active runs concurrently. After five seconds it force-kills remaining owned groups and allows up to one additional second for reaping and output flush. Terminal database writes retry with cooperative backoff; shutdown gives unresolved terminal writes a two-second retry budget and reports a non-success exit if any remain unresolved. A restart conservatively marks those runs `unknown` instead of replaying them. The bounded persistence executor limits active storage operations to two; as with other OS filesystem calls, a genuinely stalled filesystem operation cannot be cancelled and may outlive the cooperative retry budget. Output readers have a 500 ms drain grace after process-group cleanup; joins and child reaping run on one bounded reaper thread, and overdue readers are cancelled and reported as incomplete. A blocked filesystem call inside a reader cannot be cancelled and shutdown reports if its reaper has not finished. Existing API connections receive Axum graceful shutdown during a five-second server window. These windows bound ordinary local process shutdown to at most eleven seconds; a workload that escapes its process group is outside this cleanup guarantee. `unknown` is used when launch or outcome becomes ambiguous after restart. Only a trusted local operator may move an unknown run to `abandoned`, after independently verifying that the workload has been cleaned up:

```sh
curl --fail-with-body -X POST -H 'Content-Type: application/json' \
  --data '{"resolution":"abandon","reason":"Externally verified workload cleanup"}' \
  "http://127.0.0.1:8080/v1/runs/$run_id/resolve"
```

The resolution is durable and idempotent for the same reason; a conflicting reason or any state other than `unknown` returns 409. The daemon records the reason but does not claim it discovered or performed the external cleanup. Abandonment releases the run's job and worker capacity reservation.

Runs are committed to manager storage before dispatch. The worker has its own SQLite database under `worker_data_dir`; accepted run specifications and observations are committed before a process launches. Repeating the same worker `StartRun` command returns the existing observation, while reusing its run ID with a different specification is a conflict. Each worker startup advances its durable incarnation. Before accepting starts, the HTTP server reconciles manager reservations against worker observations: newer durable terminal observations repair stale manager history, and active work whose execution state cannot be confirmed becomes `unknown`. During reconciliation, `/healthz` remains available while run admission returns 503. After a daemon restart, any worker run that was not known terminal becomes `unknown` and keeps its job and capacity reservation. It is not automatically replayed; this avoids claiming exactly-once execution when a crash leaves process launch ambiguous. Standard output and error are continuously drained into private per-run files. Each stream stores at most `log_max_bytes_per_stream` bytes (8 MiB by default); excess bytes are discarded while draining continues, and the response marks the stream as truncated. The worker retains logs for the newest 100 completed runs by default. Pruning waits until both streams close and keeps run history intact. The configured bounds apply to log files: retained completed runs plus active runs, each with two capped streams. SQLite run and retention metadata remain until separately managed and can grow over time.

Retrieve output in bounded byte ranges with `GET /v1/runs/{runId}/logs?stream=stdout&offset=0&limit=4096`. `stream` is required (`stdout` or `stderr`); `offset` defaults to 0 and `limit` defaults to 4096, with an allowed range of 1 to 65,536 bytes. The JSON response contains Base64 `data`, the requested and next byte offsets, `streamClosed`, `eof`, and `truncated`. Use `nextOffset` for the next range. `eof` becomes true only after the stream closes and the requested range reaches its end; an empty live read is not EOF. Invalid query values return 400, unknown runs return 404, and logs removed by retention return 410.

Example:

```sh
run_id=$(curl --fail-with-body -X POST http://127.0.0.1:8080/v1/jobs/hello/runs | jq -r .runId)
curl --fail-with-body "http://127.0.0.1:8080/v1/runs/$run_id/logs?stream=stdout&offset=0&limit=4096" \
  | jq -r .data | base64 -d
```

Definitions require an absolute executable path and absolute working directory. Arguments are passed directly to the executable without an implicit shell. Environment inheritance defaults to false; only the explicit `env` map is added unless `inheritEnv` is set true. Unknown fields and invalid paths, identifiers, environment entries, and duration bounds are rejected with a stable JSON error envelope. Registration only stores the definition; it never launches the command.

Configuration is strict JSON. The listener must bind to IPv4 or IPv6 loopback. Manager and worker state directories must be separate, worker ID must be stable and valid, and concurrency must be between 1 and 64. `log_max_bytes_per_stream` sets the per-run stdout and stderr cap (1 byte to 1 GiB); `log_retention_runs` sets the number of newest completed runs whose logs are retained (0 to 10,000). The daemon canonicalizes both state paths and holds OS file locks on them for its lifetime; concurrent processes using either directory, including through a symlink alias, fail startup. Lock files left by a crashed process do not block a later start because the OS releases the lock when the process exits. On Unix, state directories use mode `0700`, and the database and lock files use `0600`.

`cgroup_v2.enabled` defaults to `false` for compatibility. When enabled, `delegated_root` must identify an administrator-delegated, non-root cgroup v2 directory with the `cpu`, `memory`, and `pids` controllers enabled for child groups. Conductor creates an exclusive `conductor-<worker-id-hash>` subtree there; reserve it for this worker because startup removes only stale `run-*` groups inside that subtree. The daemon never configures systemd delegation or changes controllers on ancestors. The checked-in systemd unit requests `Delegate=cpu memory pids`; after enabling delegation, set `delegated_root` to that unit's cgroup directory. Startup fails if required controls or permissions are missing.

The optional job field `resourceLimits` accepts `cpuQuotaUs` with `cpuPeriodUs` (quota and period in microseconds), `memoryMaxBytes`, `memorySwapMaxBytes`, and `pidsMax`. Swap is separately limited; `memorySwapMaxBytes: 0` disables swap for the run. Omitted per-job values inherit the matching administrator `cgroup_v2.maximums` value. Job requests above an administrator maximum are rejected before workload execution. For example:

```json
{
  "cgroup_v2": {
    "enabled": true,
    "delegatedRoot": "/sys/fs/cgroup/system.slice/conductor.service",
    "maximums": {
      "cpuQuotaUs": 200000,
      "cpuPeriodUs": 100000,
      "memoryMaxBytes": 536870912,
      "memorySwapMaxBytes": 0,
      "pidsMax": 128
    }
  }
}
```

Keep the workload UID from writing cgroup control files or migrating itself outside the delegated subtree. A delegated root owned by the same UID as the workload is not itself a security boundary. Cgroups provide resource limits and descendant cleanup only; they do not hide host metadata, restrict files or network, or prevent all same-user interference. When cgroup enforcement is disabled, jobs that request resource limits fail before their executable is launched.

### systemd operation

The checked-in [systemd unit example](examples/conductor.service) runs as the dedicated `conductor` user, uses an explicit working directory, and sets `Restart=no`. Install the binary at `/usr/local/bin/conductor`, install a loopback-only config at `/etc/conductor/agent.json`, create `/var/lib/conductor` owned by `conductor`, and adjust the config's manager/worker paths to separate directories beneath it. Then install the unit under `/etc/systemd/system/`, run `systemctl daemon-reload`, enable and start `conductor.service`, and inspect logs with `journalctl -u conductor.service`.

The example sets `KillMode=control-group`: systemd also cleans up processes remaining in the unit's cgroup when stopping the service, beyond Conductor's own process-group supervision. Running the binary directly has no cgroup cleanup, so a crash can leave an externally owned workload whose outcome becomes `unknown`. Conductor never automatically restarts after a daemon failure in this unit; operators must inspect and clean up uncertain workloads before resolving them.

### Local release probe

Build the release binary, then run `python3 scripts/benchmark-agent.py --iterations 100`. The harness uses isolated temporary state, loopback on an ephemeral port, 100 `/healthz`, worker-status, run-start, and terminal-run inspection samples by default, and `/bin/true` jobs. It reports p50/p95/p99 latency and process RSS before/after, along with the CPU architecture/count, Rust compiler, platform, and binary SHA-256. Increase iterations only up to 500. Results describe that machine, OS, filesystem, and build; they are not service-level targets or portable performance claims. This probe is separate from correctness tests and has no timing pass/fail threshold.

One recorded probe on macOS 27.0.1 arm64 (8 logical CPUs), Rust 1.99.1, 100 iterations, and the repository release profile (optimization level 3, fat LTO, one codegen unit, overflow checks and debug assertions enabled) measured RSS from 9,328 to 10,016 KiB. p50/p95/p99 were 0.224/0.257/0.619 ms for `/healthz`, 0.247/0.294/0.326 ms for worker inspection, 1.120/1.451/1.731 ms for run start, and 0.242/0.283/0.308 ms for terminal-run inspection. This measurement is from a macOS development host and does not qualify Linux deployment performance.

The manager database is `manager.sqlite3` in `manager_data_dir`. SQLite uses WAL journaling with `synchronous=FULL`; database schema changes run as versioned transactions. Startup fails closed if the database is corrupt, inaccessible, or has a schema version newer than the daemon supports. Back up the manager state directory while the daemon is stopped so the database and WAL state remain consistent. `127.0.0.1:0` is supported for integration tests.

The Linux CI run also executes [`scripts/qualify-linux-capacity.py`](scripts/qualify-linux-capacity.py) against the release binary. It starts 64 concurrent runs, including continuous output jobs, samples `/proc` RSS/thread/file-descriptor counts and health latency, then cancels the workload in a burst. Acceptance limits are 512 MiB RSS, 256 threads, 1,024 file descriptors, 250 ms health p95, 2 s run-start p95, and 5 s stop-to-terminal p95. The report records the runner and binary details. CI uses temporary local storage, so it does not qualify slow or degraded storage behavior. This 64-run target does not claim support for the previous 1,000-run configuration.

## Architecture

HTTP handlers in `conductor-api` call manager operations. `conductor-manager` owns desired state and assignment. `conductor-worker` owns process supervision, worker persistence, deadlines, logs, and observed execution state; its two-thread Tokio runtime schedules supervisors without creating a supervisor OS thread per run. Output capture uses one polling reader thread per stream, while a single bounded reaper thread joins readers and reaps child processes without blocking supervisor tasks. `conductor-process` contains host process mechanics. `conductor-core` holds shared contracts only. The `conductor` binary composes the pieces through explicit interfaces. See [AGENTS.md](AGENTS.md) for boundaries and contribution rules.

MVP1 runs Linux host processes with the daemon user's permissions and network access in combined local mode. The loopback API has no remote authentication. It does not provide distributed placement, automatic retries, automatic daemon restart/recovery of workloads, or process isolation. Use OS service controls and permissions according to the trust level of the jobs; process-group cleanup is lifecycle management, not containment.

## Contribution

Read [AGENTS.md](AGENTS.md) and [dependency-decisions.md](dependency-decisions.md) before changing architecture or dependencies. All repository work uses the primary checkout, never a Git worktree. Start each issue branch from the latest default branch and deliver changes through a pull request; never push directly to `main`.
