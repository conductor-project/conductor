---
type: Architecture decision
title: Overlay networking architecture
description: Proposed Rust-native host overlay contracts for encrypted worker-to-worker workload traffic.
status: proposed
source_revision: 1e7516055fd9572c724df85d00c3111883c58ed7
source_reviewed_at: 2026-10-19
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

This is the architecture proposal for MVP5 issue #152. Rust-native components
are a requirement. The proposal selects Iroh as the encrypted peer transport,
with a small Conductor-owned layer-3 packet adapter to connect ordinary
workload IP traffic. No backend is implemented, no dependency is added, and no
cross-host network behavior has been tested or qualified. Issue #154 must
prove the packet path before support claims change.

## Goal and boundary

MVP5 needs native workloads on separate supported worker hosts to reach one
another by manager-assigned private IP address and port. This is a host-level
network service. It is not a job sandbox and does not grant per-service
authorization. The existing manager-worker TLS connection remains the
independent authenticated control path; it must work before overlay bootstrap
and during overlay failure.

Iroh is a Rust library for authenticated QUIC connections, direct path
discovery/hole punching, and relay fallback. It exposes QUIC streams and
datagrams; it is not a ready-made virtual-IP network. Conductor must connect a
TUN interface to authorized Iroh peers, preserve IP packet boundaries, route
packets by manager-assigned address, and report transport and interface state
separately. This reuses Iroh for transport, peer authentication, and path
management without implementing a crypto or NAT-traversal protocol in the
workspace. The TUN packet adapter and host route lifecycle remain Conductor
code and must stay small, bounded, and independently testable.

Pin a stable Iroh release from crates.io, not a moving Git branch. Upstream
announced wire/API stability for Iroh 1.0 in June 2026; docs.rs lists 1.3.0
as the latest release at this review. Verify the selected release and its
transitive dependency graph against Conductor's pinned Rust 1.99.1 toolchain
before adding it. A docs.rs build using a newer compiler is not evidence of
minimum compiler compatibility.

## Alternatives considered

| Option | Capability and operational shape | Assessment for MVP5 |
| --- | --- | --- |
| Iroh library plus Conductor L3 adapter | Rust library for authenticated QUIC peer connections, direct path discovery/hole punching, and relay fallback. MIT/Apache-2.0. Self-hostable relay. Needs a TUN adapter, manager-owned membership/address contracts, and a privileged networking boundary. | Selected proposal. It meets the Rust-native constraint and supplies mature transport/path management. The packet adapter is the main implementation risk and must pass a two-host prototype before broad implementation. |
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
license before reusing code. A `tun-rs`-style device crate can supply OS TUN
plumbing but does not supply peer mapping, manager authorization, allocation,
or packet lifecycle behavior.

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

Control messages are versioned JSON contracts carried through the existing
authenticated worker transport. A snapshot names the domain, both generations,
the worker's assigned address, allowed peer public identities and endpoint
addresses, relay configuration, lease timestamps, and config schema version.
Unknown fields are rejected. No Iroh secret key or workload payload crosses
the manager API. Reports separately identify desired generation, applied
generation, local interface/route state, peer path (direct or relayed), and
bounded probe results. A connection or handshake is not evidence that an
application listener is reachable.

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

Direct connectivity is attempted where possible. Relay fallback uses an
operator-configured Iroh relay; production does not silently depend on the
public shared relay, which upstream describes as rate-limited and without an
uptime guarantee. An operator must permit the configured outbound QUIC/relay
traffic. MVP5 does not promise connectivity where both peers and the relay
are unreachable. Host-network-only deployments remain an explicit supported
mode.

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
| Underlay observer or untrusted relay | Iroh authenticates/encrypts peer QUIC connections; relays forward encrypted data. Relay metadata and traffic volume remain visible. Verify the selected release's security properties and configuration in implementation review. |
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

Before adding Iroh as a dependency, inspect the `iroh-lan` packet path and
prototype the minimum Conductor-owned path on two Linux hosts: manager-enrolled
endpoint identities, TUN packet forwarding, Iroh QUIC datagrams or a bounded
packet framing scheme, a self-hosted relay fallback, and a request/reply
application probe. Measure direct and relayed MTU, reconnect, throughput, and
packet loss. Verify compile compatibility with Rust 1.99.1, dependency
footprint, feature flags, and third-party safety boundary. Treat the adapter
and privileged route lifecycle as substantial implementation work, not as
functionality supplied by Iroh. If the packet path fails the prototype, revise
the Rust-native backend or the milestone's addressed-IP acceptance before
implementation proceeds; Iroh peer connectivity alone does not provide the
required host overlay.

This source review did not run tests, construct an overlay, validate a
privileged helper, or access a two-host testbed. No network behavior or
platform qualification is claimed.
