---
type: Architecture decision
title: Overlay networking architecture
description: Selected Rust-native direct-only host overlay contracts for encrypted worker-to-worker workload traffic.
status: selected
source_revision: e10f326
source_reviewed_at: 2026-10-20
sources:
  - resource: ../crates/conductor-core/src/lib.rs
    title: Transport-neutral worker contracts
  - resource: ../crates/conductor-transport/src/lib.rs
    title: Authenticated manager-worker transport
  - resource: platforms-and-evidence.md
    title: Current platform and qualification scope
  - resource: https://github.com/n0-computer/iroh
    title: Iroh networking library
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
Issue #153 implements manager-owned identity and address allocation. Issue
#154 implements the worker packet path and privilege boundary. Cross-host
behavior has not been qualified; issue #156 must prove the direct packet path
before support claims change.

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

Workers advertise at most eight distinct direct UDP `IP:port` candidates over
the mTLS `POST /v1/worker/network-domains/{id}/candidates` endpoint, including
their current `peerGeneration`. The manager rejects stale generations,
unspecified, loopback, multicast, and link-local addresses, and non-usable
ports. A changed candidate set advances `networkGeneration`; the acknowledgement
does not extend the peer authorization lease. Candidates expire with that
lease and are distributed only inside authorized peer snapshots.

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

## Verification and next steps

Source review at `source_revision` confirms that `conductor-core` owns
transport-neutral contracts, `conductor-manager` owns desired state, the
worker owns local supervision, and `conductor-transport` already provides an
authenticated manager-worker path. The current platform page records Linux
x86_64, Linux arm64, and Apple silicon CI, but no independent-host overlay
qualification.

PR #187 adds the pinned Iroh release and manager identity/address contracts;
PR #188 adds direct candidate advertisements, `tun-rs`, the Conductor packet
adapter, and the Linux helper route lifecycle. Local unit and workspace tests
cover packet validation, peer authorization, candidate fencing, and manager
migrations. They do not demonstrate independent-host traffic or exercise the
privileged Linux helper. Issue #156 must still qualify manager-enrolled
identities, direct-only packet forwarding, an application request/reply, MTU,
reconnect, throughput, and packet loss on separate hosts with relays and public
discovery unavailable. Keep versions, enabled features, transitive footprint,
and third-party safety boundaries in the dependency decision.
