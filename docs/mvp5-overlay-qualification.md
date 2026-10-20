---
type: Qualification procedure
title: MVP5 worker overlay cross-host qualification
description: Reproducible deployment-owned Linux procedure and evidence record for Conductor's Iroh worker overlay.
status: pending external testbed
source_revision: c3584f77b0f39252822acc03743b10397077ab1e
source_reviewed_at: 2026-10-20
sources:
  - resource: ../okf/overlay-networking-architecture.md
    title: Accepted overlay architecture and boundaries
  - resource: ../crates/conductor-overlay/tests/linux_overlay.rs
    title: Local Linux interface lifecycle tests
  - resource: ../examples/overlay-worker.json
    title: Standalone Linux worker overlay configuration
  - resource: ../scripts/qualify-overlay-traffic.py
    title: Bounded randomized application traffic probe
  - resource: ../crates/conductor/okf/operator-commands.md
    title: Operator overlay commands and report semantics
---

# Qualification status

No independent-host traffic result is recorded yet. One separate Linux host is
available; a second independently booted Linux worker host is still needed.
Do not treat local tests as cross-host evidence or mark acceptance rows as
passed until the run is performed on both hosts.

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
  --timeout-seconds 30
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
     --timeout-seconds 120
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
     --timeout-seconds 30
   ```

   Save the `probe_complete` JSON record on worker B. It reports the result,
   connected peer, status, byte count, hash, and elapsed time. Repeat with
   worker roles reversed and a new nonce/payload.

   Repeat with worker roles reversed. A successful peer handshake alone does
   not satisfy this step. Record HTTP status, byte count, hashes, elapsed time,
   and the source/destination addresses observed by the application.
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

On 2026-10-19, the Linux overlay integration target was compiled and run in a
Rust 1.99.1 Docker container on an Apple silicon host (`aarch64`). Both ignored
Linux integration tests passed: owned interface/route reconciliation across
restart, and helper ownership of TUN plus the overlay packet loop. The bounded
HTTP probe also passed a 64 KiB randomized request over `127.0.0.1`; that checks
the probe's hash and output handling only. These are single-host results; they
did not exercise two-host Iroh traffic, underlay encryption, deployment
privileges, host reboot, or direct traffic on independent hosts.

On 2026-10-20, source commit `9752cfc376a66f31808b2ddf900ed41d12c1dfe2`
passed a Rust 1.99.1 Linux arm64 workspace `cargo check --offline --workspace
--locked --all-targets --all-features`. This compiles the Linux helper, TUN,
route, and worker startup code at that revision; it does not run the helper or
qualify host networking. The later manager-persisted probe report change has
not yet been rechecked in the Linux container.

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
GitHub Actions were not awaited because of the reported outage. Replace this
evidence record when independent-host runs are performed.
