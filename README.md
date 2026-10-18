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
source with that toolchain. The first-job example uses `curl`,
`jq`, and Python 3 to inspect and decode HTTP responses; these are walkthrough
client tools, not daemon runtime dependencies.

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

Keep this terminal open. **Terminal B:** wait up to five seconds for readiness
and confirm the daemon responds. A full worker can still be ready; run admission
checks capacity separately.

```sh
ready=0
deadline=$(( $(date +%s) + 5 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if curl --silent --fail --max-time 1 \
    http://127.0.0.1:8080/readyz >/dev/null; then
    ready=1
    break
  fi
  sleep 0.1
done
[ "$ready" -eq 1 ] || { echo "Conductor did not become ready" >&2; exit 1; }
curl --fail --max-time 1 http://127.0.0.1:8080/healthz
```

The config and state directories above persist across restarts. See
[self-hosted operation](okf/self-hosting.md) for service management, separate
roles, remote access, and backups.

## Submit and inspect your first job

Registering a job stores its definition. Starting it creates a separate run.
The command is an argument array, not shell text: this example runs the system
`printf` executable directly and does not expand shell syntax.

```sh
JOB_ID="hello-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
jq -n --arg id "$JOB_ID" \
  '{id:$id,command:["/usr/bin/printf","hello conductor\\n"],cwd:"/tmp",
    env:{},inheritEnv:false,timeoutMs:null,terminationGraceMs:5000}' \
  > /tmp/conductor-first-job.json

curl --fail-with-body --max-time 5 -H 'Content-Type: application/json' \
  --data-binary @/tmp/conductor-first-job.json \
  http://127.0.0.1:8080/v1/jobs
IDEMPOTENCY_KEY="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
run_response=$(curl --fail-with-body --max-time 5 -X POST \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  "http://127.0.0.1:8080/v1/jobs/$JOB_ID/runs")
RUN_ID=$(printf '%s' "$run_response" | jq -r .runId)
```

Poll the returned run ID for up to 30 seconds. A successful run ends in
`succeeded`; other terminal states include `failed`, `cancelled`, and
`timed_out`.

```sh
finished=0
deadline=$(( $(date +%s) + 30 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  run=$(curl --fail-with-body --max-time 1 \
    "http://127.0.0.1:8080/v1/runs/$RUN_ID")
  state=$(printf '%s' "$run" | jq -r .state)
  case "$state" in
    succeeded|failed|cancelled|timed_out|abandoned)
      finished=1
      break
      ;;
    unknown)
      echo "Run outcome is unknown; operator resolution is required" >&2
      exit 1
      ;;
  esac
  sleep 0.1
done
[ "$finished" -eq 1 ] || { echo "Run did not finish within 30 seconds" >&2; exit 1; }
printf '%s\n' "$run" | jq '{runId,state,exitCode,failureReason}'
[ "$state" = succeeded ] || { echo "Job did not succeed" >&2; exit 1; }

curl --fail-with-body --max-time 5 \
  "http://127.0.0.1:8080/v1/runs/$RUN_ID/logs?stream=stdout&offset=0&limit=4096" \
  | jq -r .data \
  | python3 -c 'import base64,sys; sys.stdout.buffer.write(base64.b64decode(sys.stdin.read()))'
```

The final command prints `hello conductor`. The executable and working
directory must exist on the worker that executes the job. Set `inheritEnv` to
`false` and provide only the needed variables in `env`; Conductor does not
implicitly invoke a shell. See [job and service behavior](okf/jobs-and-services.md)
for definitions, deployments, retries, IDs, and API contracts.

If the daemon is unavailable, confirm Terminal A is still running and check
`/readyz`. Registering an already-used ID returns `409`; deleted IDs stay
reserved, so choose a new ID. If run start returns `429`, the worker has no
available slot; wait for a run to finish or increase configured concurrency
within the supported limit. A `failed` run caused by a missing executable
means that path does not exist on the executing worker. For an ambiguous client
timeout while starting a run, retry the same request with the `Idempotency-Key`
saved before the original request to recover its run ID; adding a key only on
the retry cannot recover an unkeyed start. This does not make the job's
external side effects exactly once.

## Stop a job and shut down

Use a separate long-running definition to try cancellation. This creates a
fresh ID each time, starts `/bin/sleep`, asks Conductor to stop the returned
run, and polls for a terminal result:

```sh
JOB_ID="sleep-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
jq -n --arg id "$JOB_ID" \
  '{id:$id,command:["/bin/sleep","30"],cwd:"/tmp",env:{},inheritEnv:false,
    timeoutMs:null,terminationGraceMs:1000}' > /tmp/conductor-sleep-job.json
curl --fail-with-body --max-time 5 -H 'Content-Type: application/json' \
  --data-binary @/tmp/conductor-sleep-job.json http://127.0.0.1:8080/v1/jobs
IDEMPOTENCY_KEY="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
run_response=$(curl --fail-with-body --max-time 5 -X POST \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  "http://127.0.0.1:8080/v1/jobs/$JOB_ID/runs")
RUN_ID=$(printf '%s' "$run_response" | jq -r .runId)
curl --fail-with-body --max-time 5 -X POST \
  "http://127.0.0.1:8080/v1/runs/$RUN_ID/stop"
deadline=$(( $(date +%s) + 10 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  run=$(curl --fail-with-body --max-time 1 \
    "http://127.0.0.1:8080/v1/runs/$RUN_ID")
  state=$(printf '%s' "$run" | jq -r .state)
  [ "$state" = cancelled ] && break
  sleep 0.1
done
[ "$state" = cancelled ] || { echo "Run did not cancel within 10 seconds" >&2; exit 1; }
printf '%s\n' "$run" | jq '{runId,state,terminationReason}'
```

Delete the definition only after its active run has finished or been resolved.
This does not free its ID for reuse:

```sh
curl --fail --max-time 5 -X DELETE \
  "http://127.0.0.1:8080/v1/jobs/$JOB_ID"
```

Deletion preserves an ID reservation and does not delete persistent manager or
worker state. Do not delete the state directories as a routine cleanup step.

Stopping a run is different from stopping the daemon. To stop the daemon, press
Ctrl-C in Terminal A and wait for its shutdown to finish. Job definitions and
run history persist. A daemon restart can leave an ambiguous run in `unknown`;
Conductor does not automatically replay it. See
[operations and recovery](okf/operations-and-recovery.md).

## Next steps

- [Keep Conductor running and configure separate roles](okf/self-hosting.md)
- [Configure authenticated operator access](okf/configuration.md#operator-access)
- [Submit jobs and manage services](okf/jobs-and-services.md)
- [Explore the repository and crate knowledge bundles](OKF.md)
- [Contribute](AGENTS.md) · [Apache 2.0 license](LICENSE.md)
- [Support and issue tracker](https://github.com/conductor-project/conductor/issues)
