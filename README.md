# Conductor

Conductor is an early-stage Linux daemon for managing jobs and runs. MVP1 targets one `conductor` binary that combines manager and worker responsibilities behind explicit interfaces. The agent currently provides only configuration validation and a loopback liveness endpoint; job management and process execution are planned for later issues.

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

Configuration is strict JSON. The listener must bind to IPv4 or IPv6 loopback. Manager and worker state directories must be separate, worker ID must be stable and valid, and concurrency must be positive. `127.0.0.1:0` is supported for integration tests.

## Architecture

HTTP handlers in `conductor-api` call manager operations. `conductor-manager` owns desired state and assignment. `conductor-worker` owns process supervision, worker persistence, deadlines, logs, and observed execution state. `conductor-process` contains host process mechanics. `conductor-core` holds shared contracts only. The `conductor` binary composes the pieces through explicit interfaces. See [AGENTS.md](AGENTS.md) for boundaries and contribution rules.

## Contribution

Read [AGENTS.md](AGENTS.md) and [dependency-decisions.md](dependency-decisions.md) before changing architecture or dependencies. All repository work uses the primary checkout, never a Git worktree. Start each issue branch from the latest default branch and deliver changes through a pull request; never push directly to `main`.
