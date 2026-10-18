# MVP2 multi-node qualification

## One-host qualification harness

The repository retains `scripts/qualify-mvp2-services.py` as a one-host
qualification harness. It starts one standalone manager binary and two
separately enrolled standalone worker binaries. Each worker has its own state
directory, identity, process, and non-overlapping host-port range. The suite
creates two native service processes, verifies that placement puts them on
different workers, and sends application requests in both directions. CI does
not invoke this harness; CI runs release builds and tests only after merge.

The service processes use the sole `default` namespace with a non-default
`qualification.test` DNS suffix, then query fully qualified names and short
names with the configured search domain. They query the named-port SRV record,
resolve its target, and connect to the returned dynamic port. They do not use
peer addresses from the manager API.

For a local macOS run that must not touch the developer's resolver, build and
run the debug binary in direct mode:

```sh
cargo build --locked -p conductor
python3 scripts/qualify-mvp2-services.py \
  --binary target/debug/conductor \
  --profile debug \
  --resolver-mode direct
```

Direct mode binds Conductor DNS to an ephemeral loopback port and sends
explicit FQDN, search-domain, and SRV queries to it. It verifies real
cross-service requests and lifecycle scenarios but does not test the operating
system's resolver integration. The default `host` mode tests system-resolver
lookups by temporarily configuring the runner for
`default.service.qualification.test`; it restores the previous Linux
`/etc/resolv.conf` or macOS supplemental resolver and search-domain settings
during cleanup. To protect developer machines, the script allows this mode
only on a root-owned GitHub-hosted runner.

When the harness is run on an eligible disposable GitHub-hosted runner, its step
summary records the executable SHA-256, profile, OS, architecture,
kernel/platform string, topology, worker placement, and completed scenarios.
The suite also exercises a real listener port conflict and retry, a failed
readiness probe, shared-host resource pressure, authoritative negative DNS
answers, host resolver lookups, service revision replacement, a manager DNS outage and
restart with worker reconnect, and a worker drain that moves a service while
preserving its name. It then terminates the remaining worker and verifies that
its endpoints disappear after the documented 45-second freshness window and
that DNS returns NODATA with a 1-second negative TTL. The report includes
measured readiness, resolver, application-request, restart, drain, and stale
endpoint withdrawal timings for the run.

The workspace test suite separately exercises command/report replay,
duplicate acknowledgements, report sequencing, worker restart recovery,
placement pressure, readiness, and rollout behavior. These tests validate the
manager, worker, and transport state machines; they are not a substitute for
the real-process service-name traffic in the hosted qualification.

## Routed independent-host scenario

The hosted CI scenario is one manager and two workers on one machine. It does
not prove independent-host failure isolation, firewall configuration, or
routed cross-host communication. A routed qualification must be run against
three independently addressed machines (manager M, worker W1, worker W2) on a
deployment-owned network. Use one Linux and one macOS worker when claiming a
mixed-OS topology. Do not use loopback addresses in the worker service network
for this scenario.

1. Record the exact Conductor binary SHA-256, build profile, OS, architecture,
   kernel, host identities, network routes, firewall policy, and resolver
   configuration on all three machines.
2. Start a standalone manager on M with a routable worker-transport listener,
   the service DNS listener, and a host resource domain sized for the tested
   workload. Configure its certificate name and CA before enrolling W1 and W2.
3. Start one standalone worker on W1 and one on W2 with separate state and
   enrollment identity files. Give each worker a distinct, non-overlapping
   service port range and its reachable host address as `advertised_address`.
   Permit only the required worker-transport, DNS, and service-port traffic in
   the test network firewall.
4. Configure split DNS and the `default.service.<suffix>` search domain on
   W1 and W2 following [service discovery resolver setup](service-discovery.md#host-resolver-setup).
   Preserve the existing resolver settings and prepare a rollback before
   changing either host.
5. Deploy `frontend` and `backend` with named TCP port `http`. Confirm they
   land on distinct workers. On each workload host, verify both the service
   FQDN and short name with the system resolver; then use an SRV-aware client
   to call the other service in both directions. Do not copy worker addresses
   or assigned ports into either application configuration.
6. Replace an instance, change its allocated endpoint, drain its worker, stop
   and restart the manager, disconnect and reconnect a worker, and exercise a
   readiness failure. For each event, record the time of injection, endpoint
   withdrawal/publication, resolver result, client retry, and recovered
   application request. Record behavior for already-open connections
   separately from new connections.
7. Restore the recorded resolver and firewall state. Attach the signed test
   report and relevant redacted logs to the release qualification record.

The documented DNS bounds are a positive TTL of 2 seconds, a negative TTL of
1 second, and withdrawal of a disconnected worker after it has been stale for
45 seconds. Readiness withdrawal also includes the configured probe failure
threshold and report delivery. These are catalog and cache bounds under a
responsive manager; they do not promise application recovery time, established
connection migration, or manager failover. Record measured end-to-end results
for the routed topology before claiming that topology is qualified.
