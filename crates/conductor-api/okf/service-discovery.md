---
type: Reference
title: Service catalog and DNS discovery
description: The API exposes observed service endpoints over HTTP and can serve DNS records from the manager catalog.
status: stable
source_revision: 929d001fa66a983ac4ef5b07d0bdd142e6bab7be
source_reviewed_at: 2026-10-20
sources:
  - resource: ../src/server.rs
    title: Catalog, endpoint, and watch routes
  - resource: ../src/dns.rs
    title: DNS records and listeners
  - resource: ../../conductor/tests/agent.rs
    title: Catalog and DNS integration tests
  - resource: ../../../docs/service-discovery.md
    title: Prior discovery guide, audited for this page
---

# HTTP catalog

The API reads observed ready endpoints and service attempts from the manager.
Watch calls are bounded and endpoints may change as workers report readiness,
failure, drain, or replacement. A catalog response is a manager observation,
not a promise that client caches or network routes have converged.

# DNS service

The optional DNS listener maps service names to catalog records and is
configured separately from the operator API. In the `conductor` configuration,
omitting `allowedClients` uses the loopback-only default when the listener
binds to loopback. A non-loopback listener requires explicit client CIDRs; an
empty list is rejected by the DNS listener configuration. This address filter
is not authentication. Resolver installation is host-specific and should be
performed by an administrator; see the [service discovery operations guide](../../../okf/service-discovery.md)
for host setup and rollback procedures.

The existing `suffix` publishes host-network addresses. To publish overlay
addresses as a separate namespace, configure both `overlayNetworkDomainId`
and `overlaySuffix` inside `service_dns`. The DNS listener validates the domain
ID and suffix at startup; the network domain may be created later through the
operator API. Overlay lookups fail with `SERVFAIL` until the domain exists,
then publish an endpoint only while its service catalog entry is eligible and
its worker has an active, unexpired assignment in that domain. The overlay
namespace uses the manager-assigned worker IPv4 address
with the catalog's allocated service port; the host-network suffix keeps its
existing advertised-address behavior. Separate suffixes avoid returning a mix
of host-only and overlay-only addresses to the same name. The current overlay
address pools are IPv4, so overlay lookups return A and SRV data; AAAA queries
produce NODATA. A workload published through the overlay suffix must bind its
allocated service port on the worker overlay address or a wildcard address;
binding only to a host-only interface makes the DNS result unreachable.

```json
{
  "service_dns": {
    "listen": "10.90.0.1:53",
    "suffix": "conductor.internal",
    "overlayNetworkDomainId": "workers-east",
    "overlaySuffix": "overlay.internal",
    "allowedClients": ["10.90.0.0/24"]
  }
}
```

Use `backend.default.service.conductor.internal` for host-network discovery
and `backend.default.service.overlay.internal` for overlay discovery. A TCP
SRV lookup under either suffix preserves the address/port pair in its target;
ordinary A records do not communicate dynamic service ports. Overlay DNS
reflects current manager state and has a 2-second positive TTL. An expired or
revoked peer is withdrawn from the overlay namespace even if its host-network
catalog entry remains eligible. DNS publication does not prove direct-path
reachability or application connectivity.

## DNS response and freshness semantics

Successful A, AAAA, and TCP SRV records have a 2-second TTL. The manager
includes only catalog endpoints whose run is running and ready, whose
allocation and worker observations match, and whose worker is valid, fresh,
available, not degraded, and not draining. Worker transport considers a worker
stale after 45 seconds without a successful poll or report. A stale worker's
last inventory remains inspectable, but its endpoints are excluded from DNS.
DNS answers are snapshots; they do not establish that a client route has
converged or that the endpoint will remain ready for the record's lifetime.

The listener currently responds as follows:

| Query condition | DNS response | Cache behavior |
| --- | --- | --- |
| Eligible endpoint and supported A, AAAA, or TCP SRV record | `NOERROR` with answer | Answer TTL 2 seconds |
| Existing service but no eligible endpoint or no matching record type/port | `NOERROR` with no answer (NODATA) and SOA | Negative SOA TTL 1 second |
| Unknown service identity or invalid name inside the service zone | `NXDOMAIN` with SOA | Negative SOA TTL 1 second |
| Query outside the configured service zone | `REFUSED` | No SOA negative cache record |
| Lookup admission is full or catalog storage lookup fails | `SERVFAIL` | Retry after a short delay; no negative cache record |
| Malformed request or non-IN query class | `FORMERR` | No negative cache record |

These short TTLs bound stale DNS cache exposure; they do not make endpoint
replacement instantaneous. Clients should resolve on connection attempts and
handle failed connections by looking up the service again.

# Service-wide names and SRV targets

The service FQDN `<service>.<namespace>.service.<suffix>.` keeps its
service-wide A/AAAA behavior and returns the advertised addresses of all
currently eligible replicas. Each TCP SRV answer instead names a deterministic
endpoint target containing that replica's advertised IP and port tuple. A or
AAAA lookup of that target returns only the matching endpoint, and only while
the tuple remains in the current eligible catalog. If a replica is withdrawn
or replaced with a different address or port, its former target no longer
resolves after the catalog changes. These lookups use fresh worker status and
catalog observations; DNS caches may retain answers for the configured TTL.

Issue [#118](https://github.com/conductor-project/conductor/issues/118) is
implemented at the reviewed source revision. Debug tests cover two replicas
with distinct IPv4/IPv6 addresses and ports, follow each SRV target to its
matching address, and verify withdrawn-target and replacement behavior. The
full workspace debug suite passed (204 tests across 16 suites), along with
workspace Clippy, formatting, and dependency-policy checks. These tests verify
record construction and current-catalog filtering; they do not qualify routed
multi-host behavior, resolver installation, or network reachability. Issues
#77 and #78 were cancelled, and no deployment-owned routed testbed is
available.
