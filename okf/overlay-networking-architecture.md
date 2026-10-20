---
type: Architecture decision
title: Overlay networking architecture
description: Selected Rust-native direct-only host overlay contracts for encrypted worker-to-worker workload traffic.
status: selected
source_revision: 7e6beee8177b0fe7fe119191eb075efbf15a9f3f
source_reviewed_at: 2026-10-20
sources:
  - resource: ../crates/conductor-core/src/lib.rs
    title: Transport-neutral worker contracts
  - resource: ../crates/conductor-transport/src/lib.rs
    title: Authenticated manager-worker transport
  - resource: platforms-and-evidence.md
    title: Current platform and qualification scope
  - resource: ../dependency-decisions.md
    title: Selected overlay dependency versions and boundaries
  - resource: https://github.com/conductor-project/conductor/pull/188
    title: Worker packet adapter and route lifecycle implementation reference
  - resource: https://github.com/n0-computer/iroh
    title: Iroh networking library
  - resource: https://github.com/tun-rs/tun-rs
    title: tun-rs TUN device library
  - resource: https://docs.rs/iroh/latest/iroh/
    title: Iroh API and transport model
  - resource: https://github.com/slackhq/nebula
    title: Nebula overlay implementation
  - resource: https://github.com/zerotier/ZeroTierOne
    title: ZeroTier overlay implementation
  - resource: https://github.com/openziti/ziti
    title: OpenZiti application overlay
---

# Decision status

This is the architecture decision for MVP5 issue #152. Rust-native components
are a requirement. The selected stack is one Iroh endpoint per worker, the
`tun-rs` OS TUN interface, a Conductor-owned layer-3 packet adapter, and
manager-distributed direct underlay candidates. Relays, public/default address
discovery, and automatic NAT traversal are disabled for the initial topology.
The implementation PRs add the worker packet path and privilege boundary, but
cross-host behavior has not been qualified. Issue #156 must prove the direct
packet path before support claims change.

## Goal and boundary

MVP5 needs native workloads on separate supported worker hosts to reach one
another by manager-assigned private IP address and port. This is a host-level
network service. It is not a job sandbox and does not grant per-service
authorization. The existing manager-worker TLS connection remains the
independent authenticated control path; it must work before overlay bootstrap
and during overlay failure.

Iroh is a Rust library for authenticated QUIC connections, direct connectivity,
streams, and datagrams; it is not a ready-made virtual-IP network. Conductor
uses one Iroh endpoint per worker for all local services. It connects the
`tun-rs` TUN interface to authorized Iroh peers, preserves IP packet
boundaries, routes packets by manager-assigned address, and reports transport
and interface state separately. Iroh relay mode and public/default address
discovery are explicitly disabled. Managers distribute EndpointIds and
validated direct IP:port candidates over the authenticated control plane;
peers must be directly reachable on the declared LAN/VPC or configured
topology. Conductor owns packet adaptation, policy, routes, and lifecycle; it
does not implement cryptography or NAT traversal.

Pin published Iroh and `tun-rs` releases from crates.io, not moving Git
branches. PR #187 pins Iroh 1.3.0. Verify the selected releases and their
transitive dependency graph against Conductor's pinned Rust 1.99.1 toolchain.
A docs.rs build using a newer compiler is not evidence of minimum compiler
compatibility.

## Alternatives considered

| Option | Capability and operational shape | Assessment for MVP5 |
| --- | --- | --- |
| Iroh library plus Conductor L3 adapter | Rust library for authenticated QUIC peer connections, with direct addresses supplied by Conductor. MIT/Apache-2.0. Needs a TUN adapter, manager-owned membership/address contracts, and a privileged networking boundary. | Selected. It meets the Rust-native constraint and supplies QUIC transport and endpoint identity. Relays and public/default address discovery are disabled; direct UDP reachability is a deployment prerequisite. The packet adapter remains the main qualification risk. |
| Nebula | Complete host overlay with certificates, assigned IPs, firewall rules, lighthouse discovery, hole punching, and relay support. MIT. Runs as a separate Go daemon and has its own certificate/configuration lifecycle. | Not selected: the network daemon is Go, contrary to the Rust-native requirement. It remains a useful feature benchmark for the operator surface and failure cases. |
| ZeroTier One | Complete virtual networking daemon with peer-to-peer encrypted links, virtual Ethernet, controller-managed membership, and relaying. The main daemon is C++; the repository identifies MPL-2.0 and non-free/source-available code. | Not selected: it is not Rust-native and introduces a separate daemon/controller plus licensing review. |
| OpenZiti | Self-hostable identity-based service overlay with controllers, routers, and endpoint SDKs/tunnelers; Apache-2.0. | Not selected: it focuses on identity-authorized service access, not manager-assigned host IP networking, and its core components are not Rust-native. |
| New transport/crypto stack | Conductor implements discovery, handshake, encryption, retransmission, roaming, and relay behavior itself. | Rejected. It creates too much protocol and security surface. Reusing Iroh still leaves only the IP packet adapter and route lifecycle to build. |

Upstream references: [Iroh project](https://github.com/n0-computer/iroh),
[Iroh API](https://docs.rs/iroh/latest/iroh/), [Iroh 1.0 stability
policy](https://www.iroh.computer/blog/v1), [Iroh relay hosting
options](https://www.iroh.computer/services/hosting), [Nebula project and
technical overview](https://github.com/slackhq/nebula), [ZeroTier One
licensing and architecture](https://github.com/zerotier/ZeroTierOne), and
[OpenZiti architecture](https://github.com/openziti/ziti).

## Existing Iroh-to-IP implementations

Several Rust projects already connect Iroh to a virtual IP interface. They
reduce uncertainty around packet framing and TUN integration, but none matches
Conductor's manager-owned identity, durable allocation, and worker lifecycle
contracts as a drop-in component.

| Project | Reusable capability | Gap for Conductor |
| --- | --- | --- |
| [iroh-lan](https://github.com/rustonbsd/iroh-lan) | MIT-licensed Rust library and TUN-backed layer-3 network over Iroh; the repository includes a library example and virtual IPv4 subnet. | Designed for ephemeral LANs discovered by shared name/password. It has no manager-enrolled WorkerId binding, durable address reservations, lease expiry, or revocation contract; peers must also use compatible protocol versions. Inspect as the first packet-path prototype/reference, not as the membership or allocation authority. |
| [iron](https://github.com/lucascherzer/iron) | Rust TUN interface routing packets over Iroh, with persistent endpoint keys and source-address checks. | IPv6-only address mapping, root-run daemon, and no manager-controlled durable membership/address lifecycle. Useful for packet-path comparison, not a direct MVP5 fit. |
| [Tunnet](https://github.com/tunnetio/Tunnet) | Rust-native Iroh mesh product with managed IP allocation, a full agent/control plane, and an embeddable Rust SDK. | The published SDK documents peer streams and managed enrollment; adopting the complete managed network also introduces its external identity/control-plane model. Its components have different licenses, including AGPL-3.0-only for control-plane and persistence components. Assess only if Conductor is willing to replace its own overlay control plane. |
| [ezvpn](https://github.com/flexaccessdev/ezvpn) | Rust TUN-over-Iroh implementation with assigned client addresses and direct/relay paths. | Server-centered client access; it explicitly drops client-to-client packets and does not provide a durable peer mesh. It can inform framing and MTU choices, not supply the needed topology. |

These projects are prior art rather than evidence of Conductor compatibility or
production qualification. Review their exact source revisions, protocol and
license before reusing code. `tun-rs` supplies OS TUN device access but does
not supply peer mapping, manager authorization, allocation, packet
encapsulation, or route lifecycle behavior. Its selected release and enabled
features must remain recorded in the dependency decision.

## Protocol and ownership contracts

### Worker addresses, DNS, Service VIPs, and bootstrap

These are separate control-plane concerns. The manager durably assigns each
worker one overlay address and distributes a worker peer map from that address
to the stable `workerId`, authorized Iroh `EndpointId`, and validated direct
underlay candidates. That peer map is the dataplane routing authority; DNS
records are not consulted per packet or used to authenticate a peer.

The existing manager-hosted Hickory service owns service-name discovery.
Overlay-aware service records may publish a worker's overlay address and its
allocated workload port through A/AAAA and SRV records, while the existing
host-network discovery mode remains explicit. DNS names and SRV ports identify
workload endpoints; they do not allocate worker addresses or carry routing
state. MVP6's Service virtual IP range is reserved separately and is not
allocated or forwarded by this MVP5 worker overlay.

Manager bootstrap and worker enrollment use the existing authenticated
manager-worker transport, with operator-provisioned trust, before overlay
configuration is applied. This path is independent of Hickory service DNS and
the workload overlay, so workers can enroll, refresh authorization, and report
overlay failure when workload networking or service DNS is unavailable.

### Worker dataplane contract

The worker endpoint registers the versioned Iroh ALPN
`conductor/overlay/1`. A connection using another ALPN is rejected. The
dataplane sends one complete IPv4 packet per Iroh QUIC datagram; it adds no
second packet envelope and does not use Iroh streams for workload IP traffic.
This preserves the IP packet boundary and the workload's TCP or UDP semantics.
QUIC datagrams are unreliable and unordered: an inner TCP flow can recover
loss through TCP, while inner UDP applications may observe loss or reordering.

The initial packet contract is IPv4 only, with a 1,000-byte TUN MTU and a
1,000-byte maximum complete IPv4 packet. The adapter rejects malformed packets,
packets above that limit, and packets above the negotiated QUIC datagram size;
it does not silently fragment or reassemble them. IPv6 and stream-based packet
fallback are unsupported.

For an outbound TUN packet, Conductor validates its IPv4 header and maps the
destination address through the current manager-authorized peer map to one
stable worker identity, EndpointId, and direct candidate set. It sends the
whole packet only on that authenticated Iroh connection. On receive, Conductor
checks the peer EndpointId against the active authorization and requires the
packet's source IPv4 address to equal that peer's assigned worker address; the
destination must be the local overlay address before the packet is written to
the TUN. Underlay candidates are reachability hints, not identities or source
authorization.

Packet processing uses a bounded queue of 128 packets. Producers apply
backpressure while the queue is full; packet processing must not create
unbounded secondary buffers. A direct connection attempt has a ten-second
deadline; reconnect delay starts at 250 ms and grows to a 30-second cap. No
connection attempt starts after the peer authorization expires, and expiry
closes an established connection and stops packet acceptance. Unsupported
datagrams, oversize packets, and unreachable candidates fail explicitly; none
trigger relay fallback.

The initial network is an explicitly configured IPv4 overlay. IPv6 is not
claimed until addressing, forwarding, MTU, and recovery behavior are qualified
for it. Each network domain has a stable manager-created ID. Managers require
two explicit, non-overlapping CIDR pools per domain: one for worker addresses
and one reserved for future Service virtual IPs. Pools must not overlap one
another or manager-configured host routes. The operator owns choosing ranges
that do not overlap networks outside Conductor's view.

| Field | Owner and rule |
| --- | --- |
| `networkDomainId` | Manager; stable identity for one routed overlay and its address pools. |
| `workerId` | Existing enrolled worker identity; never derived from an address or Iroh key. |
| `endpointId` | Iroh public-key identity bound to one active enrolled `workerId`. Its private key stays on that worker in protected local state. |
| `workerAddress` | Manager allocation from the worker pool, initially one IPv4 `/32` per worker. Reserved durably before delivery. |
| `serviceAddress` | Future manager allocation from the separate Service pool; issue #152 defines the range boundary but does not implement Service forwarding. |
| `networkGeneration` | Manager-monotonic version of the complete desired peer and route snapshot for a domain. |
| `peerGeneration` | Manager-monotonic worker incarnation/key generation. Re-enrollment and key replacement increment it. |
| `issuedAt` / `expiresAt` | Manager-issued configuration lease. Workers reject stale snapshots and fail closed after lease expiry. Initial maximum lease: five minutes; this is a design bound to validate, not measured evidence. |

Control messages are versioned JSON contracts carried through the existing
authenticated worker transport. A snapshot names the domain, both generations,
the worker's assigned address, allowed peer public identities and direct
underlay candidates, lease timestamps, and config schema version. Peer
candidates are reachability hints, never identity or authorization. Workers
advertise bounded direct candidates through the authenticated worker session;
the manager validates and distributes them with the peer map. Unknown fields
are rejected. No Iroh secret key or workload payload crosses the manager API.
Reports separately identify desired generation, applied generation, local
interface/route state, authenticated Iroh connection state, selected direct
path, and bounded probe results. A connection or handshake is not evidence
that an application listener is reachable.

Workers persist the highest accepted generation per domain and peer
incarnation. Lower generations and retired peer identities are rejected,
including after restart. The manager retains allocations and retirement
tombstones across restart and ambiguous delivery. An address is quarantined
until every old lease has expired plus the configured maximum clock-skew
allowance; it is not reassigned merely because a worker is unreachable.

The manager remains authoritative for desired membership and address
allocation. Iroh endpoint keys authenticate transport peers but do not replace
Conductor worker enrollment or manager authorization. A manager leader term
can be added to the desired snapshot when leadership fencing is introduced;
stale commands must be rejected. This proposal does not introduce consensus
or manager peer traffic over the workload overlay.

## Expiry, restart, and failure behavior

The five-minute lease bounds how long disconnected workers may retain a
previous peer configuration. A connected worker receives a newer snapshot on
the authenticated control channel. A worker that cannot refresh before its
lease expires withdraws only Conductor-owned addresses and routes and stops
accepting overlay packets. Existing host-network workloads and manager
control-plane connectivity remain available. The worker does not silently
extend a lease during restart; if persisted expiry cannot be checked safely,
it starts in host-network-only mode and waits for fresh manager state.

Every peer association expires with the same lease. A reachable worker removes
revoked peer identities when applying a newer generation; a partitioned peer
can use an old association only until that lease expires. With the five-minute
maximum lease, the stated revocation bound is at most five minutes from the
last configuration delivery that included the peer, plus the configured clock
skew allowance. This bound is a policy proposal, not a measurement. MVP5 must
measure delivery and expiry timing, including restart and an offline peer.

The initial overlay uses only directly reachable UDP candidates on declared
LAN/VPC or explicitly configured underlay topologies. Iroh relay mode and
public/default address discovery are disabled; no dedicated Conductor relay
role or worker relay server is used. A peer pair with blocked or unavailable
direct UDP reachability fails explicitly after bounded retries. Managers do
not carry workload packets. Host-network-only deployments remain an explicit
supported mode.

## Platform and privilege boundary

The initial candidate target is Linux x86_64 and arm64, matching the current
Linux CI targets. Apple silicon macOS remains host-network-only for the first
MVP5 implementation until its TUN, route, restart, and cleanup behavior has
separate qualification. This is a proposed overlay target matrix, not a
change to general Conductor platform support.

The ordinary worker process must not receive `CAP_NET_ADMIN`, root, or
equivalent network-administration authority because it launches user
workloads. The TUN device, address/route installation, and cleanup must be
owned by a separate, narrowly scoped privileged helper or service. Its local
protocol accepts typed operations for Conductor-owned resources only; it must
not accept arbitrary commands, paths, or firewall rules. Authenticate the
local caller by OS peer credentials and bind each operation to the worker's
stable local identity and current domain generation. The Iroh packet engine
must not share the workload process boundary. The helper's exact crate/service
boundary and safe API require implementation review in #154.

## Threat and failure analysis

| Risk | Contract and limit |
| --- | --- |
| Underlay observer | Iroh authenticates/encrypts peer QUIC connections. Underlay addresses and traffic volume remain observable. Verify the selected release's security properties and configuration in implementation review. |
| Stolen worker endpoint key | Manager binds the key to the enrolled worker identity, supports replacement/revocation, and expires peer snapshots within the configured lease. Offline peers can remain effective only to the lease bound. |
| Stale or replayed manager configuration | Authenticated existing transport, strict schema/version checks, monotonic durable generations, expiry, and retired identity tombstones reject stale state. |
| Compromised workload on an overlay-enabled worker | Overlay membership is host-level reachability, not service authorization or workload isolation. Workloads execute with daemon-user network access; MVP5 makes no claim that same-host workloads cannot reach overlay peers. |
| Manager partition or outage | Existing sessions continue only while their current lease is valid. After expiry, the worker fails closed for overlay traffic and preserves host-network workloads and independent control-plane recovery. |
| Route, address, or MTU conflict | Reject overlaps with configured host routes and other pools; stage owned resources and reconcile idempotently. Return explicit unsupported/conflict diagnostics. Real route, packet-size, and cleanup tests are required before platform claims. |
| Helper compromise or incorrect cleanup | Keep its API narrow, verify OS peer credentials, track exact owned resource IDs, and remove only those resources. Do not run workloads with helper privileges. |

Peer-link encryption does not isolate services from one another. Any
per-service policy or virtual-IP forwarding semantics remain future work and
must be enforced separately from peer encryption.

## Cross-host qualification plan

Issue #156 executes this plan; passing library or single-host tests does not
qualify the Conductor overlay. Use a manager and at least two independently
booted Linux worker hosts on a declared LAN/VPC or explicitly configured
underlay that permits direct UDP between their advertised candidates. The
initial support matrix is Linux x86_64 and arm64. Disable Iroh relays, public
address discovery, and NAT traversal on every worker. A blocked direct path
must fail within the bounded retry interval without looking up or connecting
to a relay.

| Qualification area | Procedure and evidence |
| --- | --- |
| Topology and privilege | Record OS/kernel/architecture, underlay interfaces and firewall rules, manager and worker versions, source and binary hashes, TUN/helper setup, and pre-run host routes. Verify only the helper receives the required network-administration authority and workload processes do not receive root or `CAP_NET_ADMIN`. |
| Application traffic | Send real TCP request/reply and UDP payloads between services bound to assigned overlay addresses and allocated ports in both directions. Verify byte counts and payload hashes; record latency distributions and throughput. A handshake or ping alone is not a pass. |
| Direct encrypted transport | Record each advertised direct candidate and selected path. Capture underlay UDP on both workers during a transfer, retain capture hashes, and check that a unique application marker is absent from the encrypted transport. Correlate the capture with the selected direct Iroh path and the source-reviewed relay-disabled configuration; marker absence alone is not encryption proof. |
| CPU, memory, and throughput | Measure manager, worker, helper, and workload CPU/RSS during synchronized repeated transfers. Record CPU model/governor, background load, payload size, iteration count, throughput, and latency percentiles; include host-network baseline measurements and report observed values rather than extrapolating from loopback. |
| MTU and packet bounds | Verify an effective 1,000-byte TUN MTU, successful delivery up to the configured packet limit, and bounded rejection above it or above the negotiated QUIC datagram size. Record outcomes for both TCP and UDP workloads and confirm no silent adapter fragmentation. |
| Restart and cleanup | Exercise orderly and forced worker/helper restart, daemon restart, and host reboot. Verify the same authorized identity/address returns without duplicate interfaces/routes, expired state fails closed, owned resources are removed, and normalized unrelated host routes remain unchanged. |
| Failure and recovery | Block direct UDP, interrupt a peer, partition a worker from the manager, and exercise revocation/expiry. Measure the failure and recovery bounds, confirm cached authorized peers work only through their lease, and verify there is no relay or public-discovery fallback. |

Use unique run and payload markers, isolated state, bounded readiness polling,
and cleanup even on failure. Retain exact commands, candidate/path observations,
configuration generations and expiries, payload hashes, packet captures,
resource samples, normalized before/after routes, checksums, and limitations.
Record unrun scenarios as unrun. A single-host simulation, a failed attempt
between unsupported platforms, or a peer handshake cannot satisfy the
independent Linux host acceptance criterion.

## Verification and next steps

Source review at `source_revision` confirms that `conductor-core` owns
transport-neutral contracts, `conductor-manager` owns desired state, the
worker owns local supervision, and `conductor-transport` already provides an
authenticated manager-worker path. The current platform page records Linux
x86_64, Linux arm64, and Apple silicon CI, but no independent-host overlay
qualification.

PR #186 records the selected dependency versions, dataplane contract, and
cross-host qualification plan. PR #187 implements the manager identity/address
contracts and adds Iroh dependencies; PR #188 adds direct candidate
advertisements, `tun-rs`, the Conductor packet adapter, and Linux helper route
lifecycle. Later implementation PRs must keep the selected versions and
features aligned with `dependency-decisions.md` and the resolved `Cargo.lock`
graph. Local unit and workspace tests cover packet validation, peer
authorization, candidate fencing, and manager migrations. They do not
demonstrate independent-host traffic or exercise the privileged Linux helper.
Issue #156 must still execute the plan above and qualify manager-enrolled
identities, direct-only packet forwarding, application traffic, MTU, restart,
throughput, and packet loss on separate hosts with relays and public discovery
unavailable. This plan is not evidence that qualification has passed.

This source review did not run tests, construct an overlay, validate a
privileged helper, or access a two-host testbed. No network behavior or
platform qualification is claimed.
