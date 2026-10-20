# Conductor contributor guidance

Conductor is a daemon for submitting one-shot jobs and managing service deployments on hosts you operate. It supports a combined manager/worker process and separate manager and worker roles connected by authenticated transport. The process backend runs with the daemon user's host permissions and network access; it is not a security sandbox.

## Crates and dependency directions

- `conductor-core`: stable IDs and transport-neutral manager/worker contracts only. No HTTP, storage, process handles, or execution behavior.
- `conductor-manager`: desired state, assignment, manager-owned persistence, and reconciliation. It depends on core, never on worker or process code.
- `conductor-worker`: worker-owned supervision, persistence, deadlines, logs, and an execution-backend interface. It depends on core.
- `conductor-process`: host-specific process execution implementing the worker interface.
- `conductor-api`: HTTP entrypoint and request/response mapping; handlers call manager operations.
- `conductor-transport`: authenticated manager/worker transport implementing explicit command and report adapters.
- `conductor`: CLI, configuration, composition, local manager/worker adapter, and shutdown.

Communication between manager and worker goes through explicit command/report interfaces. Keep PIDs and worker filesystem paths internal to the worker. Use stable job, run, and worker IDs at boundaries. The supported role selections are manager, worker, and all. The `all` role composes manager and worker in one process; standalone roles use authenticated transport. Keep this description aligned with the CLI and transport implementation.

## Knowledge bundles

`README.md` is the user entry point. `OKF.md` links the cross-crate repository knowledge bundle in `okf/` and independent technical bundles under each `crates/<crate>/okf/`. Keep these pages descriptive and source-linked: they do not override this file. When behavior, configuration, API contracts, platform support, or verification evidence changes, update affected knowledge pages and any relevant README walkthrough in the same change. Record the reviewed source revision and distinguish source review, tests, local runtime validation, CI, and platform qualification. Do not claim verification that was not performed. See `okf/migration-map.md` for old documentation paths and their canonical destinations.

## Required workflow

- Do all repository work in the primary checkout. Do not create or use Git worktrees.
- For each GitHub issue, fetch the latest default branch and create an issue-specific branch from it. Do not base a new issue branch on another feature branch.
- Never push directly to `main`; deliver work through a pull request. Push incremental commits so CI checks each stage. Inspect and fix CI failures as work proceeds.
- Complete required checks and resolve review feedback. Obtain an eligible approval and merge only when repository protections permit; never bypass required reviews or branch protection.
- Keep changes focused on the issue and preserve a runnable agent at each milestone.

## Rust and dependency policy

- Use Rust 1.99.1, edition 2024, and the root `Cargo.lock`. Every crate inherits workspace package settings and lints.
- Zero unsafe code is allowed in workspace production code, tests, examples, benchmarks, and build scripts. `unsafe_code = "forbid"` checks this workspace only; it does not audit dependencies, Rust standard library internals, or platform code.
- Prefer `std` and already approved dependencies. Add a library only when required for correctness or reliable platform behavior. Record its capability, alternatives, maintenance/license posture, enabled features, transitive footprint, and safety boundary in `dependency-decisions.md`.
- Do not hand-roll fragile HTTP parsers, cryptography, or storage engines to avoid a dependency. Keep dependency versions/features centralized and lockfile updates reviewable.
- Use JSON for configuration and HTTP examples. Reject unknown configuration fields. Never print environment values, secrets, full commands, or sensitive request contents in diagnostics.
- Treat panics as bugs, not lifecycle control flow. Avoid unsupported claims about isolation or crash recovery.

## Local checks

```sh
python3 scripts/check-dependency-policy.py
cargo fmt --all -- --check
cargo clippy --workspace --locked --all-targets --all-features -- -D warnings
cargo test --workspace --locked --all-targets --all-features
```

Never run release tests or release builds locally. Run them only after merge in
CI, except when reproducing a specific CI issue.

Integration tests must use isolated temporary state, an ephemeral loopback listener, bounded readiness polling, and cleanup on failure. Test real binary behavior for lifecycle changes. The release test suite must exercise the release profile's overflow and debug assertions.
