# Conductor

Conductor is an early-stage Linux daemon for managing jobs and runs. MVP1 targets one `conductor` binary that combines manager and worker responsibilities behind explicit interfaces. The current runnable milestone validates and manages immutable job definitions over a loopback HTTP API. Job registration does not execute processes; process execution is a later milestone. Job definitions currently live in memory and are lost when the agent restarts.

The host process backend runs with the daemon user's operating-system permissions and network access. It is not a security sandbox.

## Requirements

- Linux for the supported daemon deployment.
- Rust 1.99.1 with `rustfmt` and `clippy` (installed by `rust-toolchain.toml`).

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

`POST /v1/jobs` returns 201 with the validated immutable definition; duplicate and previously deleted IDs return 409. `GET /v1/jobs` returns `{"jobs":[...]}` ordered by ID. `GET /v1/jobs/{id}` returns 200 or 404, and `DELETE /v1/jobs/{id}` returns 204 or 404. Deletion reserves the ID so it cannot refer to a different definition later. The manager currently accepts up to 1,000 active jobs and reserves up to 10,000 IDs; request bodies are limited to 65,536 bytes. These limits are temporary until pagination and durable storage are implemented.

Definitions require an absolute executable path and absolute working directory. Arguments are passed directly to the executable without an implicit shell. Environment inheritance defaults to false; only the explicit `env` map is added unless `inheritEnv` is set true. Unknown fields and invalid paths, identifiers, environment entries, and duration bounds are rejected with a stable JSON error envelope. Registration only stores the definition; it never launches the command.

Configuration is strict JSON. The listener must bind to IPv4 or IPv6 loopback. Manager and worker state directories must be separate, worker ID must be stable and valid, and concurrency must be positive. `127.0.0.1:0` is supported for integration tests.

## Architecture

HTTP handlers in `conductor-api` call manager operations. `conductor-manager` owns desired state and assignment. `conductor-worker` owns process supervision, worker persistence, deadlines, logs, and observed execution state. `conductor-process` contains host process mechanics. `conductor-core` holds shared contracts only. The `conductor` binary composes the pieces through explicit interfaces. See [AGENTS.md](AGENTS.md) for boundaries and contribution rules.

## Contribution

Read [AGENTS.md](AGENTS.md) and [dependency-decisions.md](dependency-decisions.md) before changing architecture or dependencies. All repository work uses the primary checkout, never a Git worktree. Start each issue branch from the latest default branch and deliver changes through a pull request; never push directly to `main`.
