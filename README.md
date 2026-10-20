# Conductor

Conductor runs one-shot jobs and manages long-running service deployments on
machines you operate. In combined mode, one daemon runs the manager and a local
worker. Separate manager and worker roles are available when you need workers
on other hosts. Jobs run as the daemon account with its host permissions and
network access; they are not sandboxed by default.

## Platforms and prerequisites

The current CI exercises Linux x86_64, Linux arm64, and Apple silicon macOS;
Intel macOS is outside the current CI scope. Package files are checked on the
native Linux runners, but target-distribution installation and runtime
qualification are not established by those jobs. See the
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

The checked-in [`examples/local.json`](examples/local.json) configures an
all-role daemon with a loopback API and local access. It stores state under
`./state`; run the following commands from the repository root. The daemon
creates the state directories when it starts.

**Terminal A:** from the repository root, start the foreground daemon:

```sh
./target/debug/conductor agent \
  --config examples/local.json
```

Keep this terminal open. **Terminal B:** confirm that the manager responds and
list the configured worker. A full worker can still be ready; run admission
checks capacity separately.

```sh
./target/debug/conductor worker list
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
  --idempotency-key mvp3-first-job
```

The JSON response includes stable `jobId` and `runId` values. Copy the returned
`runId` into the next commands. Retry the same command with the same input and
idempotency key after an ambiguous timeout; the same accepted run is returned
instead of starting another. A successful `run wait` ends in `succeeded`; other
terminal states include `failed`, `cancelled`, and `timed_out`. A wait timeout
does not stop the remote run.

```sh
./target/debug/conductor run wait RUN_ID --timeout-seconds 30
./target/debug/conductor run logs RUN_ID --stream stdout --offset 0 --limit 4096
./target/debug/conductor run inspect RUN_ID --output json
./target/debug/conductor job list --output json
./target/debug/conductor job inspect mvp3-first-job --output json
./target/debug/conductor run list --job-id mvp3-first-job --output json
```

The final command prints `hello conductor`. The executable and working
directory must exist on the worker that executes the job. Set `inheritEnv` to
`false` and provide only the needed variables in `env`. Use `conductor job
register FILE` to store a definition without starting a run; input `-` reads
JSON from stdin. See [job and service behavior](okf/jobs-and-services.md) for
revision, deployment, retries, IDs, and API contracts.

If the daemon is unavailable, confirm Terminal A is still running; the
`worker list` command below confirms the API is reachable. Registering
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
`shasum -a 256 /bin/sleep` on macOS or `sha256sum /bin/sleep` on Linux), then
replace the example's all-zero digest with that value before creating it.

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
./target/debug/conductor deployment suspend mvp3-demo-service \
  --generation 4 --idempotency-key service-suspend-1 --wait --output json
./target/debug/conductor deployment resume mvp3-demo-service \
  --generation 5 --idempotency-key service-resume-1 --wait --output json
./target/debug/conductor deployment delete mvp3-demo-service \
  --generation 6 --idempotency-key service-delete-1 --wait
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

## Inspect and drain workers

`worker list` reports the manager's inventory, including capacity, available
slots, connectivity freshness, readiness, and drain state. `worker inspect`
shows the same details for one worker. A disconnected worker remains visible
but is reported unavailable or stale; an empty inventory is a successful empty
list.

Draining prevents new placements on that worker and withdraws its service
endpoints from manager discovery. Existing runs continue; Conductor does not
promise to migrate them or terminate them when a drain is requested. Use a
bounded wait to observe completion. If the wait times out or you press Ctrl-C,
the accepted drain remains active. Clear it only after the manager reports
completion.

```sh
./target/debug/conductor worker list --output json
./target/debug/conductor worker inspect local-worker --output json
./target/debug/conductor worker drain local-worker \
  --grace-period-seconds 30 --wait --wait-timeout-seconds 120
./target/debug/conductor worker drain-status local-worker --output json
./target/debug/conductor worker drain-wait local-worker --timeout-seconds 120
./target/debug/conductor worker undrain local-worker
```

## Discover service endpoints and check manager health

Endpoint commands report only endpoints currently eligible in the manager's
service catalog. An empty result is successful. Returned addresses are the
workers' advertised network addresses; lookup does not configure host DNS or
prove that the address is reachable from the caller. Use `watch` to long-poll
for a catalog change. The current API does not provide watch cursors or
versions, so the CLI retries transient connection and capacity failures within
the requested deadline; Ctrl-C stops observation without changing deployment
state. The optional manager Hickory listener can expose these services through
a separate overlay DNS suffix, resolving active workers to their assigned
overlay addresses while preserving the existing host-network suffix. Overlay
TCP SRV records preserve each assigned service port; see the [service
discovery reference](crates/conductor-api/okf/service-discovery.md) for
configuration and resolver limits.

```sh
./target/debug/conductor service endpoints mvp3-demo-service --output json
./target/debug/conductor service lookup default mvp3-demo-service --output json
./target/debug/conductor service watch default mvp3-demo-service \
  --timeout-seconds 300 --output json
./target/debug/conductor diagnostics health --output json
./target/debug/conductor diagnostics ready --output json
./target/debug/conductor diagnostics metrics
./target/debug/conductor diagnostics --output json export --deadline-seconds 15
```

Health reports whether the API responds and the manager process is healthy.
Readiness reports whether the manager can currently accept work; a reachable
manager can be not ready while draining, degraded, or recovering. Metrics are
Prometheus text by default; use `--output json` to wrap them in a JSON response.
The versioned `diagnostics export` collects health, readiness, worker summaries,
and allowlisted scheduling metrics through the authenticated typed API. Each
remote section has a bounded request deadline and reports `collected` or
`unavailable`; the overall result can be `partial`. Worker/item and output
sizes are capped. The export omits worker labels, inventory, network details,
commands, logs, and request contents. Remote storage sizes are not available;
use the explicitly offline `conductor storage inspect` command against local
data directories when the daemon is stopped.

## MVP5 Iroh peer identity groundwork

An authenticated operator can create a network domain with separate private
IPv4 pools for workers and future service addresses, then inspect its bounded
peer allocation status through the manager API. Workers enroll their public
Iroh endpoint ID over the existing mTLS control channel; the manager binds it
to the authenticated worker identity and assigns a leased address. The Iroh
private key stays on the worker.

An enrolled worker can advertise up to eight direct UDP underlay candidates
over mTLS, fenced by its peer generation. The manager validates and distributes
those reachability hints in versioned peer maps; candidates do not authenticate
or authorize a peer. The initial Iroh topology requires directly reachable
UDP and disables relays, public/default address discovery, and NAT traversal.

Operators can create and inspect network domains with
`conductor operator overlay create|inspect|peers|revoke`; peer diagnostics
distinguish desired membership, helper-reported TUN/routes, and active direct
Iroh paths with their handshake time and transport counters. Those observations
are bounded and generation/freshness tagged; they do not prove application
reachability. From an overlay-enabled worker, run `conductor overlay probe`
with an assigned private overlay IP, service port, and explicit safe HTTP GET
path to verify that an application response returns through that worker's
local route. The probe is capped at five seconds and 64 KiB; it does not run
through the manager or change service state. Add `--config /path/to/worker.json`
to report a completed HTTP response over worker mTLS; diagnostics retain the
directional result for 60 seconds only while its peer and network generations
and target assignment remain current. See the [operator command
guide](crates/conductor/okf/operator-commands.md).

The worker overlay crate validates IPv4 packets, routes them by the manager's
peer map, and carries them in authenticated Iroh datagrams with relays and
address lookup disabled. On Linux, a separate helper owns the tun-rs device
and Conductor-managed routes; ordinary worker mode starts one endpoint for its
local services. This implementation has local tests, but no independent-host
traffic qualification yet. See the [overlay architecture](okf/overlay-networking-architecture.md)
and [manager/API contracts](crates/conductor-manager/okf/desired-state-and-persistence.md).

## Command output and recovery

The manager and combined `all` role expose the management API; a standalone
`worker` role does not. The API listens on loopback by default. Remote operator
access is supported through a trusted TLS proxy in front of the loopback API;
the proxy terminates TLS and forwards to the manager. Do not expose the local
API directly on an untrusted network. Configure manager/all API access with an
operator token and protect the token and proxy configuration. Use a token file
and CA file in the operator context for the proxy's authenticated HTTPS
endpoint. See [operator access configuration](okf/configuration.md#operator-access).

Successful commands exit with status `0`. Application or API errors exit with
status `1`; command-line usage errors exit with status `2`. With `--output json`,
structured command errors are written to stderr as an error object with `code`
and `message` fields and an optional `partial` result when a command has useful
accepted state to report. Human errors also go to stderr. Run
`conductor COMMAND --help` for flags and defaults.

| Situation | Result |
| --- | --- |
| Invalid job JSON or unknown fields | `invalid_job`; no request is submitted |
| Authentication required or rejected | API error `operator_authentication_required` or an HTTP status code |
| Duplicate job ID with different contents | `duplicate_job`; the existing job is unchanged |
| Stale deployment generation | Conflict error; inspect the deployment and retry with its current generation and a new idempotency key |
| Server unavailable | `api_error`; retry after checking the manager or proxy |
| Job wait times out or is interrupted | `wait_timeout` or `wait_interrupted`; the run continues until explicitly stopped |
| Deployment wait times out or is interrupted | `rollout_timeout` or `rollout_interrupted`; accepted desired state remains active |

List commands accept `--limit` and an opaque `--cursor`; pass the returned
`nextCursor` to the next request. Cursors are for pagination only. For a
submission or mutation with an ambiguous response, retry the same command with
the same idempotency key. For a new operation, choose a fresh key. Read current
deployment generations before mutations; a conflict is not safe to retry with
the stale generation. These commands are documented in the job, run, service,
deployment, and worker sections above.
The resource commands complement the existing `operator` API command; they do
not change the HTTP API or require migrating stored jobs, runs, or deployments.

Before a planned upgrade, stop every Conductor process and make a consistent
backup of manager and worker state. Then inspect the local database schemas
without starting a daemon or applying migrations:

```sh
./target/debug/conductor upgrade preflight \
  --manager-data-dir /var/lib/conductor/manager \
  --worker-data-dir /var/lib/conductor/worker
```

The command reports the binary, HTTP API, and manager-worker protocol versions,
plus each selected database's schema state. It is read-only; an older schema
with a migration path is reported as requiring migration, and a newer or
invalid schema blocks the upgrade. Migrations run only when the corresponding
role is explicitly started. The preflight reads local files only: it does not
verify versions of running remote workers or package/distribution
compatibility. Keep manager and worker binaries on the same Conductor version
during an upgrade. Drain workers, stop workers before the manager, back up
both roles and their identity/configuration as one set, and run the candidate
binary's preflight before installing or starting it. Start the manager before
workers. If migration begins, do not downgrade; restore the matching backup
and reconcile surviving workloads. See the [upgrade compatibility policy and
rollback sequence](okf/operations-and-recovery.md#upgrade-compatibility-policy).
If a worker's protocol version or required capabilities are incompatible with
the manager, the poll returns HTTP `426` and the worker exits with a diagnostic
to install matching Conductor versions rather than retrying indefinitely.
Preflight JSON includes `requiredMaintenanceActions`; human output prints the
same guidance for unsupported schemas or required migrations.

Stopping a run is different from stopping the daemon. To stop the daemon, press
Ctrl-C in Terminal A and wait for its shutdown to finish. Job definitions and
run history persist. A daemon restart can leave an ambiguous run in `unknown`;
Conductor does not automatically replay it. See
[operations and recovery](okf/operations-and-recovery.md).

## Manager history and storage inspection

Inspect manager and/or worker retained history, delivery counts, SQLite
freelist pages, and database/WAL file sizes without migrating or changing
database contents:

```sh
./target/debug/conductor storage inspect \
  --manager-data-dir /var/lib/conductor/manager \
  --worker-data-dir /var/lib/conductor/worker \
  --completed-run-retention 10000
```

`completed_run_retention` defaults to 10,000 and accepts values from 1 through
1,000,000. Cleanup removes eligible terminal runs in batches of at most 128.
Idempotency-keyed runs, runs with pending commands, service references, or port
leases are retained. Run identifiers stay reserved by the monotonic run
sequence. Audit events and execution idempotency records are permanent. The
inspection reports SQLite free pages, but does not shrink the database file;
SQLite reuses those pages for later writes. A read-only SQLite connection may
create WAL coordination sidecar files when they do not already exist. The
completed-run target applies to eligible run rows, not permanent or protected
metadata. Add `--worker-data-dir` to inspect worker run/command/report counts,
SQLite page and file sizes, and the estimated upper bound for unpruned log
pairs, including active runs. Set `--log-retention-runs` and
`--log-max-bytes-per-stream` to
match the worker configuration when calculating that estimate. Worker run
observations, command results, and report sequence state are retained for replay
and reconnect safety and currently have no expiry or compaction policy. Worker
log files use their separate `log_retention_runs` setting.

## Offline backup and restore

Stop Conductor and every worker before planned maintenance. Create and verify a
backup with explicit manager/worker state paths, plus every configuration and
identity file or directory needed to reconnect the installation:

```sh
./target/debug/conductor backup create \
  --manager-data-dir /var/lib/conductor/manager \
  --worker-data-dir /var/lib/conductor/worker \
  --include-path /etc/conductor/agent.json \
  --include-path /var/lib/conductor/manager-identity \
  --include-path /etc/conductor/worker-identity.pem \
  --output /var/backups/conductor-maintenance.tar
./target/debug/conductor backup inspect \
  --input /var/backups/conductor-maintenance.tar
```

The archive contains consistent SQLite snapshots, worker run logs, and the
explicitly included files. It is created with owner-only permissions and
includes a versioned manifest with file sizes and SHA-256 checksums. The
manifest detects accidental corruption; inspection also checks SQLite
integrity and confirms each database schema marker matches the manifest. These
checks do not authenticate who created the archive. Protect and encrypt backup
storage according to your deployment's credential-handling policy. Symlinks,
special files, invalid paths, unsupported schemas, and checksum mismatches are
rejected. Manager/worker state paths may not also be passed as included paths.

Restore only into a new directory; the command never replaces an existing
destination. It validates the archive before publishing the restored tree:

```sh
./target/debug/conductor backup restore \
  --input /var/backups/conductor-maintenance.tar \
  --destination /var/lib/conductor/restored
```

The restored state is at `restored/manager` and `restored/worker`; explicit
included files are under `restored/included/NNNN`. Update the restored
configuration to use those state paths and the restored identity files. A
restore creates a recovery hold in every restored component, so manager,
worker, and `all` roles refuse to start until surviving workers and workloads
are fenced and uncertain outcomes have been reviewed. After those checks,
clear each hold explicitly (pass both paths when both roles were restored):

Restored directories are mode `0700` and files are mode `0600`, owned by the
account that ran the restore. If the daemon runs under a different service
account, assign the restored state directories to that account before startup;
set ownership and permissions for included configuration and identity files
separately. For the packaged `conductor` account, for example:

```sh
sudo chown -R conductor:conductor \
  /var/lib/conductor/restored/manager \
  /var/lib/conductor/restored/worker
```

Do this only after checking the destination paths and which account should own
each item.

```sh
./target/debug/conductor backup recovery-resolve \
  --manager-data-dir /var/lib/conductor/restored/manager \
  --worker-data-dir /var/lib/conductor/restored/worker \
  --confirm-workers-fenced \
  --confirm-unknown-outcomes-reviewed
```

Restoring an older snapshot discards writes after its backup time. The recovery
hold does not prove that external processes stopped or undo their effects; do
not resume scheduling until the operator has checked those conditions. See the
[backup and recovery runbook](okf/operations-and-recovery.md#offline-backup-and-restore)
for the complete procedure.

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

For the trusted TLS proxy's remote HTTPS endpoint, provide a protected token
file and, when needed, a CA certificate file:

```sh
./target/debug/conductor context create production \
  --server https://manager.example.net \
  --token-file /etc/conductor/operator-token \
  --ca-file /etc/conductor/manager-ca.pem \
  --timeout-seconds 10
./target/debug/conductor context show production
./target/debug/conductor worker --context production list
./target/debug/conductor worker --context production --output json list
./target/debug/conductor completions zsh
```

The `--context` selection can be overridden field by field with explicit
`--server`, `--token-file`, `--ca-file`, and `--timeout-seconds` options.
Without an explicit context, Conductor uses the selected default; without a
stored default, `operator` uses local loopback and does not require a dummy
token. Plain HTTP to non-loopback hosts is rejected. Use
`conductor context delete NAME` to remove a saved context. The existing
`operator` command remains available for lower-level API operations.
Resource-oriented CLI commands use concise human output by default; use
`--output json` for JSON output. `conductor completions SHELL` generates
completion scripts for Bash, Elvish, Fish, PowerShell, and Zsh.

## Linux packages

The published Debian/RPM packages target Debian 13 and Rocky Linux 10 on
x86_64 and arm64. The first packages are available in release
[`v0.0.1`](https://github.com/conductor-project/conductor/releases/tag/v0.0.1).
Packages preserve operator configuration and persistent state, and require an
explicit configuration and manual service start. See the [package installation and
verification guide](docs/package-install.md) for the install, upgrade,
removal, checksum, signature, and provenance procedures.

## Next steps

- [Keep Conductor running and configure separate roles](okf/self-hosting.md)
- [Configure authenticated operator access](okf/configuration.md#operator-access)
- [Submit jobs and manage services](okf/jobs-and-services.md)
- [Explore the repository and crate knowledge bundles](OKF.md)
- [Release support and security backport policy](docs/release-policy.md) (first supported release: `v0.0.1`; tagged releases use exact-run verification and protected publication)
- [Contribute](AGENTS.md) · [Apache 2.0 license](LICENSE.md)
- [Support and issue tracker](https://github.com/conductor-project/conductor/issues)
