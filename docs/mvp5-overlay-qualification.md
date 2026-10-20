---
type: Qualification procedure
title: MVP5 worker overlay cross-host qualification
description: Reproducible deployment-owned Linux procedure and evidence record for Conductor's Iroh worker overlay.
status: pending external testbed
source_revision: 36b84f6c94acd728a47a83516d1d575bb989c5ce
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

No independent-host traffic result is recorded yet. The user is arranging
two independent Linux hosts. Do not treat local tests as cross-host evidence or
mark acceptance rows as passed until the run is performed on those hosts.

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
diagnostics distinguish desired membership and helper-applied generation, but
do not report Iroh connection state, selected path, or counters.

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
   desired generation. `reachability` must remain `not_verified` until traffic
   below succeeds.
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
| Overlay DNS discovery | A/AAAA and SRV preserve endpoint address/port pairing and an SRV-aware client exchanges application traffic | Blocked pending #155 overlay-aware Hickory publication |
| Orderly and forced shutdown | Owned routes/interface are removed; unrelated network state is unchanged | Not run on independent hosts |

For each failure, save the manager peer listing before and after, including
desired peer generation, observation generation, observation timestamp,
freshness, lease expiry, and state. Keep `reachability: not_verified` in the
record unless an application-level probe succeeded.

# Existing local evidence

On 2026-10-19, the Linux overlay integration target was compiled and run in a
Rust 1.99.1 Docker container on an Apple silicon host (`aarch64`). Both ignored
Linux integration tests passed: owned interface/route reconciliation across
restart, and helper ownership of TUN plus the overlay packet loop. The bounded
HTTP probe also passed a 64 KiB randomized request over `127.0.0.1`; that checks
the probe's hash and output handling only. These are single-host results; they
did not exercise two-host Iroh traffic, underlay encryption, deployment
privileges, host reboot, or direct traffic on independent hosts.

A historical workspace debug suite passed 291 tests on the local macOS host for
source commit `5e23aea68b4a99e424e96842a04ce756fa79795f`; that revision predates
the current direct-only rework and is not evidence for this snapshot. For source
revision `36b84f6c94acd728a47a83516d1d575bb989c5ce`, the dependency policy,
formatting, workspace Clippy, workspace check, and full workspace test suite
passed locally (292 tests across 32 suites). The probe also passed a 64 KiB
loopback smoke transfer on 2026-10-20. These checks do not qualify two-host
overlay behavior, DNS-discovered traffic, packet-capture encryption, or the
platform privilege boundary. GitHub Actions were not awaited because of the
reported outage. Replace this evidence record when independent-host runs are
performed.
