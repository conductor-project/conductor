---
type: Qualification procedure
title: MVP5 worker overlay cross-host qualification
description: Reproducible deployment-owned Linux procedure and evidence record for Conductor's Iroh worker overlay.
status: partial direct TUN/TCP test passed; TCP/UDP rerun and deployment qualification pending
source_revision: 270b2e7f6e2aa3b9debdb34310f0ec7f506a5d2f
source_reviewed_at: 2026-10-20
sources:
  - resource: ../okf/overlay-networking-architecture.md
    title: Accepted overlay architecture and boundaries
  - resource: ../crates/conductor-overlay/tests/linux_overlay.rs
    title: Local Linux interface lifecycle tests
  - resource: ../crates/conductor-overlay/tests/transport.rs
    title: Direct-only transport and cross-host qualification tests
  - resource: ../examples/overlay-worker.json
    title: Standalone Linux worker overlay configuration
  - resource: ../scripts/qualify-overlay-traffic.py
    title: Bounded randomized application traffic probe
  - resource: ../crates/conductor/okf/operator-commands.md
    title: Operator overlay commands and report semantics
---

# Qualification status

Partial direct application-traffic evidence is recorded below: the Linux TUN
and Iroh adapter exchanged TCP data between two independently booted Debian
systems. The second system was a QEMU VM hosted on the development Mac, so the
run does not prove resilience across two independent physical hosts. The
deployment-level qualification remains pending; keep acceptance rows open.

On 2026-10-20, authenticated SSH access to the available Debian 13 x86_64
Linux host was confirmed. The host reports kernel
`6.12.113+deb13-amd64`; `/dev/net/tun` and `tcpdump` are present. This host was
used as worker A in the partial direct-traffic run below. Worker B was a Debian
13 ARM64 QEMU VM on the development Mac. The two systems had separate kernels
and exchanged traffic over direct UDP, but shared one physical Mac/underlay
environment and did not use a manager process.

The code under review supports a Linux TUN with a 1,000-byte IPv4 packet
limit and a direct-only Iroh topology. Iroh relay use, public/default address
discovery, and NAT traversal are disabled. Managers distribute authenticated
peer identities and direct UDP candidates; they do not carry workload packets.
Each worker has one Iroh Endpoint for its local services. There is no
per-service sidecar. Apple silicon macOS has not been qualified for overlay
mode; overlay-enabled workers are currently rejected outside Linux.

# Testbed record

Complete this section for every run. Record exact versions and addresses in a
protected operator record; do not put credentials, private keys, bearer tokens,
or raw environment values in this document.

| Field | Value |
| --- | --- |
| Run ID and UTC interval | Pending |
| Conductor source commit and branch | Pending |
| SHA-256 of manager, worker, and helper binaries | Pending |
| Manager OS, kernel, architecture | Pending |
| Worker A OS, kernel, architecture | Pending |
| Worker B OS, kernel, architecture | Pending |
| Underlay topology and firewall rules | Pending |
| CPU model, governor, and load during run | Pending |
| Manager, worker, and helper CPU/RSS measurement method | Pending |
| Application transfer size, iterations, latency percentiles, throughput | Pending |
| Overlay MTU and effective workload packet size | Pending |
| Worker overlay addresses and domain ID | Pending |
| Address pools and overlapping host routes checked | Pending |
| Helper privilege and service-manager configuration | Pending |
| Evidence directory and checksums | Pending |

Use at least two independently booted Linux worker hosts on an underlay that
permits direct UDP between their advertised candidates. Record the candidate
addresses, firewall rules, and observed direct-path result. This implementation
provides no relay fallback or public/default address lookup; a blocked direct
path is expected to fail within the documented bound. Current operator
diagnostics distinguish desired membership, helper-reported TUN/address/MTU
and installed routes, active direct Iroh links, selected direct IP path, last
handshake time, and transport byte counters. Reports are generation- and
freshness-tagged; counters measure QUIC UDP traffic including protocol overhead,
not application payload. `conductor overlay probe` reports locally by default.
With the source worker's `--config`, a completed HTTP response is submitted
over mTLS and appears as a directional application probe in manager diagnostics.
The manager marks reachability `verified` only while the report is no more than
60 seconds old, its source generations are current, and the same target address
belongs to an active peer. Retain the separate payload/hash check as evidence of
content integrity.

# DNS-discovered application traffic

This test exercises the manager-hosted Hickory listener from #155 through the
worker host resolver. Before deploying the test service, configure the manager's
`service_dns` listener with `overlayNetworkDomainId`, `overlaySuffix`, and an
`allowedClients` range containing the worker hosts. The network domain can be
created after the manager starts. Configure the host resolver to forward
`service.<overlaySuffix>` to that listener; retain the host-network suffix as a
separate zone. The root [service discovery runbook](../okf/service-discovery.md)
contains resolver setup and rollback steps.

Deploy one service replica to worker A with a named TCP port `http`. The
workload command should start the managed probe on worker A's assigned overlay
IPv4 address and read the port Conductor injects:

```json
{
  "command": [
    "/usr/bin/python3",
    "/opt/conductor/scripts/qualify-overlay-traffic.py",
    "serve-managed",
    "--bind",
    "WORKER_A_OVERLAY_IP",
    "--port-env",
    "CONDUCTOR_PORT_HTTP",
    "--bytes",
    "65536",
    "--timeout-seconds",
    "600"
  ],
  "workload": {
    "kind": "service",
    "identity": {"namespace": "default", "name": "dns-probe"},
    "ports": [{"name": "http", "protocol": "tcp"}]
  }
}
```

Use the normal service revision API fields for the executable digest, resource
request, and worker placement. Bind to the worker overlay address (or a wildcard
address) so the assigned TUN address accepts the service port. Save the probe's
`ready` log record with its nonce, payload size, and SHA-256.

From worker B, query the configured host resolver rather than supplying the
underlay or overlay address directly:

```sh
dig +short SRV _http._tcp.dns-probe.default.service.overlay.internal.
```

For an SRV row, retain its target and port, then confirm the target's A record
is worker A's assigned overlay address:

```sh
dig +short A SRV_TARGET.
conductor overlay --config /etc/conductor/worker-b.json probe \
  --target-ip WORKER_A_OVERLAY_IP --port SRV_PORT \
  --path /probe/READY_NONCE --timeout-ms 5000
python3 scripts/qualify-overlay-traffic.py request \
  --host SRV_TARGET --port SRV_PORT \
  --nonce READY_NONCE --sha256 READY_SHA256 --bytes 65536 \
  --timeout-seconds 30 --iterations 10
```

Save the SRV answer, target A answer, `probe_complete` record, and server's
`request_complete` log. The probe uses the host resolver to look up the SRV
Target's A record and verifies the returned bytes. Also query the matching
host-network suffix and confirm it still returns the worker's configured
advertised address. Overlay addresses are IPv4 in this implementation, so an
AAAA query in the overlay namespace is expected to return NODATA. A DNS answer
alone does not prove that the direct Iroh path or application traffic works.
The bounded CLI probe reports locally and, with the worker config above, sends
the HTTP result to the manager. Retain its accepted manager diagnostic and the
payload-verifying request as separate evidence.

# Basic traffic procedure

1. Install the same reviewed Conductor and helper binaries on both workers.
   Use `examples/overlay-worker.json`, with unique worker IDs, private state
   directories, manager HTTPS identity, and the network-domain ID created by
   an operator. Install the helper with only the network-administration
   capability it needs. Keep workers' existing host routes and firewall state
   captured before startup.
2. Create the network domain using `conductor operator overlay create`, then
   inspect it and wait for both active peer assignments. Confirm each worker's
   `observationFresh` is true and its reported network generation matches the
   desired generation. Reachability starts as `not_verified`; a manager-accepted
   bounded HTTP response can mark its direction `verified` for 60 seconds,
   while the payload-verifying request remains a separate qualification step.
3. On worker A, bind a native HTTP server to its assigned overlay address and
   serve a unique random marker file of at least 64 KiB:

   ```sh
   python3 scripts/qualify-overlay-traffic.py serve \
     --bind WORKER_A_OVERLAY_IP --port 18080 --bytes 65536 \
     --timeout-seconds 120 --max-requests 10
   ```

   Save the single `ready` JSON line; it contains a random request nonce, the
   expected SHA-256, byte count, and a random capture marker. The server exits
   after one request or the bounded timeout.

4. From worker B, fetch the file by worker A's assigned private address. Save
   the response and compare its SHA-256 with the source file on worker A:

   ```sh
   python3 scripts/qualify-overlay-traffic.py request \
     --host WORKER_A_OVERLAY_IP --port 18080 \
     --nonce READY_NONCE --sha256 READY_SHA256 --bytes 65536 \
     --timeout-seconds 30 --iterations 10
   ```

   Save the `probe_complete` JSON record on worker B. It reports a verified
   payload hash, byte count, per-transfer latency and throughput, and min,
   median, p95, and max summaries. Each iteration resolves the service name
   and opens a fresh TCP connection; the system resolver may answer from cache.
   These are application end-to-end measurements rather than isolated link
   capacity. Repeat with worker roles reversed and a new nonce/payload.

   A successful peer handshake alone does not satisfy this step. Record HTTP
   status, byte count, hashes, elapsed time, and the source/destination
   addresses observed by the application.
5. Capture the underlay interface on each worker during a repeated transfer.
   Record the packet-capture command, interface, filters, and checksum. Search
   the capture bytes for the random `captureMarker` from the server's `ready`
   record and save the result. The capture must show direct UDP underlay traffic;
   marker absence alone is not proof of encryption. Keep
   the packet capture protected because it exposes network metadata.
6. Compare `ip -brief address` and relevant `ip route` output before worker
   startup, with the overlay active, after orderly shutdown, after forced
   helper termination, and after daemon restart. Verify only the Conductor
   interface and tagged routes were changed and that unrelated routes remain
   byte-for-byte equivalent in the saved normalized output.
7. Record the CPU model, governor, background load, transfer size, iteration
   count, MTU, and effective packet size for each run. Measure the probe
   processes with `/usr/bin/time -v` and record user/system CPU time and maximum
   resident set size. Measure the manager, worker, and helper separately with
   a documented process or service-cgroup sampler; include its version and
   sampling interval. Keep resource samples synchronized with the traffic
   interval. The Python probe's throughput includes DNS lookup and connection
   setup when given a service name, so retain these figures as end-to-end
   workload measurements and do not label them as Iroh link capacity.

# Failure and recovery matrix

Record the exact injected fault, start/end UTC timestamps, expected bound,
observed recovery, and cleanup result for every row. Do not infer a pass from a
worker status, Iroh handshake, ping, or timeout alone.

| Scenario | Required observation | Result |
| --- | --- | --- |
| Worker process restart | Same stable endpoint identity/address; helper, interface, and routes converge without duplicates | Not run |
| Host reboot | Worker rejoins, assigned address is stable, host routes outside the overlay survive | Not run |
| Helper killed during setup | Worker fails closed; partial owned interface/routes are cleaned on restart | Not run |
| Existing conflicting route | Startup reports the conflict and preserves the pre-existing route | Not run |
| Peer process/network loss | Traffic fails within a measured bound; reconnect resumes application traffic | Not run |
| Manager unreachable after configuration | Existing traffic behavior is measured through the assigned lease expiry; stale state is visible | Not run |
| Peer revoked while worker is disconnected from manager | Revocation is delivered on reconnect or the old lease expires; no indefinite stale authorization | Not run |
| Oversized IPv4 packet | Adapter rejects packets over 1,000 bytes without panic or unbounded growth | Not run on hosts; packet unit tests cover local validation |
| Direct UDP blocked | Connection fails within the documented bound and never invokes relay or public address lookup | Not run |
| Overlay DNS discovery | Host resolver returns the assigned worker overlay A record and SRV port; an SRV-aware application transfers the expected payload | Not run on independent hosts |
| Orderly and forced shutdown | Owned routes/interface are removed; unrelated network state is unchanged | Not run on independent hosts |

For each failure, save the manager peer listing before and after, including
desired peer generation, observation generation, application-probe outcome and
freshness, lease expiry, and state. A failed or stale application probe must
not be treated as verified reachability.

# Existing local evidence

## Repeat direct-host run and blocked-candidate probe

On 2026-10-20, the exact source revision
`e1003ad016084731ac334b5063959a91bebded7b` passed the ignored
`independent_linux_hosts_exchange_tcp_over_iroh_tun` test on Fortress and the
separately booted Debian 13 ARM64 QEMU VM. Each role compiled the test target
with Rust 1.99.1. Both roles exchanged and compared a 65,536-byte TCP payload
and its echo through owned TUN interfaces and relay-disabled Iroh endpoints.
Worker B bound its test listener before waiting for the peer path; Worker A
waited for the peer test endpoint to close after it sent the application ACK.
The role-specific test processes passed in 5.77 seconds (A) and 3.97 seconds
(B). Test binary SHA-256 values were
`fc6246e80317220c0470edccb752d37bf6291a8009e56ec53a6fc217b9efb9e1` on
Fortress and
`1d64253bc530b2c27e544762754534b57afbde1d54e0a8cbf62b44d83597ef44` on the
VM. The run still used a manually injected, short-lived peer configuration;
the VM shared the development Mac as its physical host. It does not qualify
manager bootstrap, Hickory/host-resolver discovery, two independent physical
failure domains, restart, revocation, MTU, queue pressure, or performance.

The same source revision passed the ignored
`unavailable_direct_candidate_reports_failure_within_bound` test on Fortress.
With relay mode disabled and the explicit documentation-only candidate
`192.0.2.1:65000`, the test reported a non-connected attempt outcome inside its
12-second bound; the full test completed in 13.04 seconds. Its binary was the
Fortress transport test binary above. This exercises one local unreachable
candidate; it is not a two-host firewall-blockage run and does not replace the
remaining #156 failure matrix.

The worker-routing slice adds an isolated Linux route-overlap test. On
Fortress, overlay setup was rejected with `AddrInUse` when its peer route
overlapped an unrelated static host route, and the pre-existing route remained
installed.

## TCP and UDP cross-host rerun

On 2026-10-20, exact source revision
`e9eae8522e123dea79371715fa5d820673e335a6` passed the ignored
`independent_linux_hosts_exchange_tcp_and_udp_over_iroh_tun` test on Fortress
and the separately booted Debian 13 ARM64 QEMU VM. Both role binaries were
compiled from that source with Rust 1.99.1. Relay mode was disabled, each
process ran with the privileges required to create its owned TUN and route,
and each used the explicit direct UDP candidate supplied for the other role.
The test exchanged and compared a 65,536-byte TCP payload and echo, echoed a
24-byte UDP application payload, exchanged completion acknowledgments, and
completed a bounded TCP half-close. Both role processes passed: A in 1.32
seconds and B in 17.21 seconds. Test binary SHA-256 values were
`7e8d0f315daf5dc003f4e3f5209a4cba672497b678b5f6ece07fc1156bf6d3d3` on
Fortress and
`084ab026f614893489ca1c86c4b2c8cdd9d412d71a6b06c22896d9013306d0d9` on the
VM.

Fortress was Debian 13, x86_64, kernel `6.12.113+deb13-amd64`; the VM was
Debian 13, aarch64, kernel `6.12.111+deb13-cloud-arm64`. The VM shared the
development Mac as its physical host and used QEMU user-mode networking with a
static UDP forward. This proves traffic between two Linux kernels on separate
hosts, but not between independent physical failure domains. A Conductor
manager was not running; the test injected short-lived peer configuration.
There was no underlay packet capture in this rerun, so encryption at the
underlay boundary is not newly demonstrated by this result. Manager bootstrap
and outage, host-resolver DNS/Hickory application discovery, independent
physical failure domains, performance limits, restart, revocation, expiry,
MTU, queue pressure, route preservation, and blocked direct UDP remain
unqualified. Keep those #156 acceptance criteria open.

## Partial direct-host application test

On 2026-10-20, exact source revision
`dcbfa67fd959c9113bfcfa2a9782b687424e2bb5` passed the ignored
`independent_linux_hosts_exchange_tcp_over_iroh_tun` test on both systems.
Worker A was Fortress: Debian 13.7, x86_64, kernel
`6.12.113+deb13-amd64`, Rust 1.99.1, AMD Ryzen 3 3200G. Worker B was an
independently booted Debian 13 ARM64 QEMU VM: kernel
`6.12.111+deb13-cloud-arm64`, Rust 1.99.1. The VM was hosted on the development
Mac and used QEMU user-mode networking with a static UDP port forward. This is
two separate Linux kernels, but one physical host plus one VM; it is not a
two-machine failure-domain qualification.

Each process created a Linux TUN with a /32 overlay address, installed the
test-owned peer route, and ran a relay-disabled Iroh endpoint using the test's
explicit direct candidate. A native TCP socket transferred 65,536 bytes from A
to B, returned the same payload, and completed application acknowledgments in
both directions. Both test processes passed; their reported durations were
6.27 seconds on A and 6.71 seconds on B. Iroh selected a direct IP path. The
manager was not running: the test injected a short-lived peer configuration
directly, so this does not qualify manager-distributed bootstrap or cached
operation during manager loss.

The TUN test initially exposed host-generated LLMNR multicast reaching the
interface. The dataplane previously terminated on that unroutable packet; it
now drops malformed or unmapped TUN packets while preserving packet-validation
errors for callers. The final bidirectional exchange passed with that fix.

Fortress captured the underlay on `enp3s0` using a UDP filter for test ports
40000 and 40101–40103. The capture contains 419 packets in both directions;
the randomized application marker was absent from the capture bytes. Iroh
relay mode was disabled and the test supplied the direct peer candidate. The
capture SHA-256 is
`b291dd5dce80d8a929c204075406c0cb6682b101d443cad2caae4ed8ac3ea022`.
The protected capture is retained outside the repository because it includes
underlay metadata. Test binary SHA-256 values are
`3b26c9789d15a27b81ac213021797ee0affaf955f14e0fa436c00f2b353e43a8` on
Fortress and
`2abfd533d15a5a04d47a520d48cf504ffe65e38226f0f7ca5b93d45ee4245982` on the
VM. Both were compiled from the same test source and toolchain.

This partial run did not use a Conductor manager, Hickory DNS, the host
resolver, or a deployed service process. It did not measure throughput,
latency percentiles, CPU/RSS, daemon/host restart, revocation, lease expiry,
manager outage, route preservation across reboot, queue pressure, MTU failure,
or blocked direct UDP. The VM's same-Mac placement and QEMU port forwarding
also limit what the underlay capture proves. Keep the corresponding #156
acceptance criteria open until those scenarios are run on the declared
deployment topology.

On 2026-10-19, the Linux overlay integration target was compiled and run in a
Rust 1.99.1 Docker container on an Apple silicon host (`aarch64`). Both ignored
Linux integration tests passed: owned interface/route reconciliation across
restart, and helper ownership of TUN plus the overlay packet loop. The bounded
HTTP probe also passed a 64 KiB randomized request over `127.0.0.1`; that checks
the probe's hash and output handling only. These are single-host results; they
did not exercise two-host Iroh traffic, underlay encryption, deployment
privileges, host reboot, or direct traffic on independent hosts.

On 2026-10-20, qualification-tool source commit
`2146dda3fb990386aaf7b1b74cc186f7d9ffafed` passed Python compilation and a
four-iteration, 64 KiB-per-transfer loopback smoke check. The output contained
the expected 262,144 verified bytes and latency/throughput summaries. This
validates the bounded probe and its reporting only; loopback measurements are
not overlay performance evidence.

On 2026-10-20, source commit `9752cfc376a66f31808b2ddf900ed41d12c1dfe2`
passed a Rust 1.99.1 Linux arm64 workspace `cargo check --offline --workspace
--locked --all-targets --all-features`. This compiles the Linux helper, TUN,
route, and worker startup code at that revision; it does not run the helper or
qualify host networking. The later manager-persisted probe report change has
not yet been rechecked in the Linux container.

On 2026-10-20, exact integrated source commit
`74a577bec06a7ad817fadff5833dae41c56acf93` was archived
(`d299dc1d70e76d6bbcc22cd53891f871df9914662cd25ffa4cec1a279786df62`) and
tested on the available Fortress host: Debian 13 x86_64, kernel
`6.12.113+deb13-amd64`, Rust 1.99.1. The command
`CARGO_TARGET_DIR=/home/batman/conductor-mvp5-src/target cargo test --locked -p
conductor-overlay --test linux_overlay --no-run` built the Linux integration
target. Running it with
`sudo -n unshare --net -- env RUST_TEST_THREADS=1 timeout 90s
/home/batman/conductor-mvp5-src/target/debug/deps/linux_overlay-31f5885afd604320
--include-ignored --test-threads=1` passed both ignored tests: helper
TUN/dataplane ownership and managed interface/route reconciliation across
restart (2 passed, 0 failed, 0.10 seconds).

The same source commit's transport target was built with
`CARGO_TARGET_DIR=/home/batman/conductor-mvp5-src/target cargo test --locked -p
conductor-overlay --test transport --no-run`. Running
`timeout 30s env RUST_TEST_THREADS=1
/home/batman/conductor-mvp5-src/target/debug/deps/transport-056288c93679feac
--include-ignored --test-threads=1 --nocapture` passed one ignored simulation
(1 passed, 0 failed, 0.09 seconds). Two local relay-disabled Iroh endpoints
used non-loopback IPv4 candidates, selected an IP path matching the advertised
candidate, exchanged IPv4 datagrams in both directions, and rejected a packet
whose claimed source address was not assigned to its authenticated endpoint.
The test and helper binary SHA-256 values are
`435503154f889be142122026f9b97431c274053b82a218268a64c337f5234a65`,
`fd74c8a56482a91fab1532c3eb5245da61bd8336efed2d4829a0f1a93823ad6b`, and
`374d4df85e0f6c3145350e9adc2d6b4783668be5e8ac6933edabea5ef79462b8`, for the
Linux lifecycle test, transport simulation, and helper respectively. A
normalized host address and route snapshot matched before and after both runs
(SHA-256
`8953527becd0405da91e9cf463f5b3902203fbc16f7f8ec1b92afe9c7dc4040b`).

The first run exposed that the diagnostics client rejected the helper's valid
terminal `stopped` response, which omits runtime telemetry. PR #189 now accepts
that terminal shape while continuing to require validated runtime telemetry
for ready, applied, and status responses; the reruns above passed with the fix.
The local transport simulation uses actual UDP between two Iroh endpoints on
one host. It is not separate-host workload traffic, packet-capture evidence of
encryption, or qualification of host-resolver DNS, independent-host relay
absence, restart recovery, or the complete failure matrix.

On 2026-10-20, the exact source revision at the open #190 head,
`ebaf367e206a620c9f01194403ca15d276047b2e`, was archived with SHA-256
`106d291ae07ea4f870b92da822c1d6a268fd9e18644a269b9ca438176c59ee32` and
compiled on Fortress using Rust 1.99.1. The focused command
`CARGO_TARGET_DIR=/home/batman/conductor-mvp5-src/target
/home/batman/.cargo/bin/cargo test --locked -p conductor-overlay --test
linux_overlay --test transport --no-run` completed successfully. The ignored
Linux lifecycle target passed both tests under `sudo -n unshare --net`; the
ignored transport simulation passed one test. Test/helper binary SHA-256
values were `69e0ccb94f7a24ad44008579ebfa89acc4708273172c3dff00d172d1771149cf`,
`79714debdca87e94a517772e2096158c9a5d4cb4598d576968260b46ac370064`, and
`d53ba4172f5b681e5c7abfffa6670b8ca9fc6545d1d007d4d6556622a50382f5` for the
Linux lifecycle test, transport simulation, and helper. The transport test
selected the advertised direct candidate `192.168.0.238`, exchanged authorized
IPv4 packets between two local Iroh endpoints, and rejected a spoofed source.
This remains single-host simulation evidence: it does not establish separate-
host traffic, underlay packet-capture encryption, or host-resolver DNS
qualification. The current host has not yet been paired with a second
independent Linux worker.

A historical workspace debug suite passed 291 tests on the local macOS host for
source commit `5e23aea68b4a99e424e96842a04ce756fa79795f`; that revision predates
the current direct-only rework and is not evidence for this snapshot. For the
current integrated source, the dependency policy, formatting, workspace
Clippy, workspace check, and full workspace test suite passed on the local
macOS host; the new bounded overlay probe's successful response and oversized
body tests passed over an ephemeral loopback listener. The focused core
contract test and Linux arm64 all-target check also passed after the final
Linux worker ownership fix. The managed-port probe
passed a 64 KiB loopback smoke transfer on 2026-10-20. These checks do not qualify
two-host overlay behavior, DNS-discovered traffic through an installed host
resolver, packet-capture encryption, or the platform privilege boundary.
The current source snapshot also passed `python3
scripts/check-dependency-policy.py`, `cargo fmt --all -- --check`, workspace
Clippy, and `cargo test --workspace --locked --all-targets --all-features`
(301 passed, 1 ignored, 33 suites; 40.50 seconds). The two-host TCP+UDP test is
compiled but remains pending a fresh two-role run. GitHub Actions were not
awaited because of the reported outage. Replace this evidence record when
independent-host runs are performed.
