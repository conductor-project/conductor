# Conductor

Conductor runs one-shot jobs and manages long-running service deployments on
machines you operate. In combined mode, one daemon runs the manager and a local
worker. Separate manager and worker roles are available when you need workers
on other hosts. Jobs run as the daemon account with its host permissions and
network access; they are not sandboxed by default.

## Platforms and prerequisites

The current CI exercises Linux x86_64 and Apple silicon macOS. Linux arm64 has
not been qualified; Intel macOS is outside the current CI scope. See the
[platform and evidence guide](okf/platforms-and-evidence.md) for the limits of
those claims. This repository does not publish downloadable binaries or
package-manager installs. [Install Rustup](https://rustup.rs/) for your OS,
then install the pinned toolchain with
`rustup toolchain install 1.99.1 --profile minimal`. Build the binary from
source with that toolchain. Operator tasks use the `conductor` CLI.

## Build and start a local daemon

From the repository root, build the debug binary:

```sh
cargo +1.99.1 build --locked -p conductor
```

Create a user configuration and persistent state directories. The API binds to
loopback and uses local access without a bearer token:

```sh
config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/conductor"
data_dir="${XDG_DATA_HOME:-$HOME/.local/share}/conductor"
mkdir -p "$config_dir" "$data_dir/manager" "$data_dir/worker"
jq -n --arg manager "$data_dir/manager" --arg worker "$data_dir/worker" \
  '{role:"all",listen:"127.0.0.1:8080",manager_data_dir:$manager,
    worker_data_dir:$worker,worker_id:"local-worker",worker_concurrency:2,
    api_access:{mode:"local"}}' > "$config_dir/agent.json"
```

**Terminal A:** from the repository root, start the foreground daemon:

```sh
./target/debug/conductor agent \
  --config "${XDG_CONFIG_HOME:-$HOME/.config}/conductor/agent.json"
```

Keep this terminal open. **Terminal B:** confirm that the manager responds and
list the configured worker. A full worker can still be ready; run admission
checks capacity separately.

```sh
./target/debug/conductor operator workers list
```

The config and state directories above persist across restarts. See
[self-hosted operation](okf/self-hosting.md) for service management, separate
roles, remote access, and backups.

## Submit and inspect your first job

Registering a job stores its definition. Submitting it starts a separate run.
The checked-in JSON file runs the system `printf` executable directly; a job
command is an argument array and Conductor does not implicitly invoke a shell.

```sh
./target/debug/conductor job --output json submit examples/cli-job.json \
  --idempotency-key mvp3-first-job --wait --wait-timeout-seconds 30
```

The JSON result includes stable `jobId` and `runId` values. Retry the same
command with the same input and idempotency key after an ambiguous timeout; the
same accepted run is returned instead of starting another. A successful
`--wait` ends in `succeeded`; other terminal states include `failed`,
`cancelled`, and `timed_out`. A wait timeout does not stop the remote run.

```sh
./target/debug/conductor job list --output json
./target/debug/conductor job inspect mvp3-first-job --output json
./target/debug/conductor run list --job-id mvp3-first-job --output json
./target/debug/conductor run logs RUN_ID --stream stdout --offset 0 --limit 4096
```

The final command prints `hello conductor`. The executable and working
directory must exist on the worker that executes the job. Set `inheritEnv` to
`false` and provide only the needed variables in `env`. Use `conductor job
register FILE` to store a definition without starting a run; input `-` reads
JSON from stdin. See [job and service behavior](okf/jobs-and-services.md) for
revision, deployment, retries, IDs, and API contracts.

If the daemon is unavailable, confirm Terminal A is still running; the
`operator workers list` command below confirms the API is reachable. Registering
a different definition under an existing ID returns `409`; registering the
same definition is safe. Deleted IDs stay reserved, so choose a new ID. If run
start returns `429`, the worker has no
available slot; wait for a run to finish or increase configured concurrency
within the supported limit. A `failed` run caused by a missing executable
means that path does not exist on the executing worker. For an ambiguous client
timeout while submitting, retry the same `job submit` command with the same
`--idempotency-key` to recover its run ID. This does not make the job's
external side effects exactly once.

## Inspect and stop a run

The checked-in long-running example can be registered and started directly.
Choose a fresh idempotency key when starting a new run; reuse that key only to
recover the same run after an ambiguous response.

```sh
./target/debug/conductor job register examples/cli-sleep-job.json
./target/debug/conductor run start mvp3-sleep-job \
  --idempotency-key mvp3-sleep-run-1 --output json
./target/debug/conductor run inspect RUN_ID --output json
./target/debug/conductor run stop RUN_ID --output json
./target/debug/conductor run wait RUN_ID
./target/debug/conductor run logs RUN_ID --stream stdout --follow
./target/debug/conductor job delete mvp3-sleep-job
```

`run wait` returns a nonzero exit status when the run fails, is cancelled,
times out, or has an unknown outcome. A wait timeout or Ctrl-C leaves the
remote run alone; use `run stop` to request cancellation. Resolve an unknown
run only after an operator determines that it should be abandoned. Job
deletion requires no active or unresolved runs and keeps the ID reserved.
`run logs --follow` writes exact log bytes to stdout. Add `--output json` to
emit base64 JSON lines with offsets and truncation metadata instead.

## Deploy and manage a service

Register a job definition, create immutable service revisions, then apply a
deployment request. The checked-in example uses `/bin/sleep` to keep each
replica alive; it does not serve application traffic. A rollout wait confirms
the manager reports the requested state, but does not guarantee that every
external client has observed the change. Before creating either checked-in
service revision, set its `executableDigest.value` to the SHA-256 digest of
`/bin/sleep` on the target worker (for example, get it with
`shasum -a 256 /bin/sleep`).

```sh
./target/debug/conductor job register examples/cli-service-job.json
./target/debug/conductor job revision create mvp3-demo-service \
  examples/cli-service-revision.json --idempotency-key service-revision-1
./target/debug/conductor job revision create mvp3-demo-service \
  examples/cli-service-revision-v2.json --idempotency-key service-revision-2
./target/debug/conductor deployment apply mvp3-demo-service \
  examples/cli-deployment.json --idempotency-key service-deploy-1 \
  --wait --wait-timeout-seconds 120 --output json
./target/debug/conductor deployment inspect mvp3-demo-service --output json
./target/debug/conductor deployment apply mvp3-demo-service \
  examples/cli-deployment-v2.json --generation 1 \
  --idempotency-key service-deploy-2 --wait --wait-timeout-seconds 120
./target/debug/conductor deployment rollback mvp3-demo-service \
  --revision 1 --generation 2 --idempotency-key service-rollback-1 \
  --wait --wait-timeout-seconds 120
./target/debug/conductor deployment scale mvp3-demo-service \
  --replicas 1 --generation 3 --idempotency-key service-scale-1 --wait
./target/debug/conductor deployment delete mvp3-demo-service \
  --generation 4 --idempotency-key service-delete-1 --wait
```

Each mutation requires a fresh idempotency key and an expected generation for
updates. Read the generation returned by the preceding command or inspection
before the next mutation; stale generations return a conflict and require
operator review. A rollout timeout or Ctrl-C leaves the accepted desired state
in place. Repeat the wait or inspect status to continue observing it. Rollback
selects a revision number explicitly; suspension retains the desired replica
count, and delete keeps deployment history after stopping instances.

Deletion preserves an ID reservation and does not delete persistent manager or
worker state. Do not delete the state directories as a routine cleanup step.

Stopping a run is different from stopping the daemon. To stop the daemon, press
Ctrl-C in Terminal A and wait for its shutdown to finish. Job definitions and
run history persist. A daemon restart can leave an ambiguous run in `unknown`;
Conductor does not automatically replay it. See
[operations and recovery](okf/operations-and-recovery.md).

## Configure operator connections

The operator CLI supports named connection contexts. Contexts store the manager
URL and references to optional token and CA files; token contents are never
copied into the context store. Context files live below
`$XDG_CONFIG_HOME/conductor` or `$HOME/.config/conductor` and are restricted to
the current user.

For local loopback access, create and select a context without a token:

```sh
./target/debug/conductor context create local --server http://127.0.0.1:8080
./target/debug/conductor context use local
./target/debug/conductor context list
```

Context commands use concise human output by default. Add `--output json` to
any context subcommand to get machine-readable results, for example:

```sh
./target/debug/conductor context list --output json
./target/debug/conductor context show local --output json
```

For a remote TLS endpoint, provide a protected token file and, when needed, a
CA certificate file:

```sh
./target/debug/conductor context create production \
  --server https://manager.example.net \
  --token-file /etc/conductor/operator-token \
  --ca-file /etc/conductor/manager-ca.pem \
  --timeout-seconds 10
./target/debug/conductor context show production
./target/debug/conductor operator --context production workers list
./target/debug/conductor operator --context production --output json workers list
./target/debug/conductor completions zsh
```

The `--context` selection can be overridden field by field with explicit
`--server`, `--token-file`, `--ca-file`, and `--timeout-seconds` options.
Without an explicit context, Conductor uses the selected default; without a
stored default, `operator` uses local loopback and does not require a dummy
token. Plain HTTP to non-loopback hosts is rejected. Use
`conductor context delete NAME` to remove a saved context. The existing
`operator` command remains available while resource-oriented CLI commands are
added incrementally. Operator responses default to concise human text; use
`--output json` for JSON output. `conductor completions SHELL` generates
completion scripts for Bash, Elvish, Fish, PowerShell, and Zsh.

## Next steps

- [Keep Conductor running and configure separate roles](okf/self-hosting.md)
- [Configure authenticated operator access](okf/configuration.md#operator-access)
- [Submit jobs and manage services](okf/jobs-and-services.md)
- [Explore the repository and crate knowledge bundles](OKF.md)
- [Contribute](AGENTS.md) · [Apache 2.0 license](LICENSE.md)
- [Support and issue tracker](https://github.com/conductor-project/conductor/issues)
