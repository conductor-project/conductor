---
type: Runbook
title: Configure host service discovery
description: Route Conductor service DNS through host resolvers and roll resolver changes back safely.
status: stable
source_revision: 929d001fa66a983ac4ef5b07d0bdd142e6bab7be
source_reviewed_at: 2026-10-20
sources:
  - resource: ../crates/conductor-api/src/dns.rs
    title: DNS listener validation, access, and records
  - resource: ../crates/conductor/src/main.rs
    title: Loopback defaults and daemon configuration
  - resource: ../docs/service-discovery.md
    title: Previous resolver setup, audited for this guide
---

# DNS listener

Conductor can expose service catalog records through an optional UDP and TCP DNS listener. The listener is disabled when `service_dns` is omitted. A loopback listener gets a loopback-only client range by default. A non-loopback listener must list explicit `allowedClients` CIDRs; the daemon rejects a non-loopback listener without them. The lower-level DNS listener rejects an empty client allowlist. The allowlist is an address filter, not authentication. Restrict it to workload hosts that need discovery.

The default DNS suffix is `conductor.internal`. The stable FQDN is
`<service>.default.service.<suffix>.`; `default` is the only supported
namespace. Overlay DNS can use `overlayNetworkDomainId` and `overlaySuffix`
together to publish active peers under a separate name suffix. For example,
`backend.default.service.overlay.internal` resolves to the worker's assigned
overlay IPv4 address; SRV records preserve the allocated service port.
Workloads must bind their service port on the worker overlay address or a
wildcard address. Existing host-network suffix answers are unchanged. The
referenced overlay domain can be created after manager startup; overlay
lookups return `SERVFAIL` until it exists. Expired or revoked peer assignments
are withdrawn. Configure host resolver forwarding
for each desired suffix separately; see the [API service discovery
reference](../crates/conductor-api/okf/service-discovery.md). Host-network A
records contain advertised IPv4 addresses, AAAA records contain advertised
IPv6 addresses, and named TCP ports are returned as SRV records. SRV is needed
for dynamically assigned ports. Each SRV target identifies its endpoint using
the advertised IP and port; querying that target returns only the matching
eligible address. Service-wide A/AAAA queries continue to aggregate eligible
replicas.

For a single-host daemon, a loopback listener can use the configured default client range:

```json
{
  "service_dns": {
    "listen": "127.0.0.1:53",
    "suffix": "conductor.internal"
  }
}
```

For a manager reachable by remote workload hosts, use its reachable address and explicit client CIDRs:

```json
{
  "service_dns": {
    "listen": "192.0.2.53:53",
    "suffix": "conductor.internal",
    "allowedClients": ["192.0.2.0/24", "2001:db8:10::/64"]
  }
}
```

Replace example addresses with deployment addresses. The service network also needs a non-overlapping host-port range and a worker `advertisedAddress` reachable by other workload hosts. DNS does not establish routes or open host firewalls; configure those separately. See the [manager service bundle](../crates/conductor-manager/okf/scheduling-and-services.md).

# Host resolver setup

Resolver changes are host-wide for native processes. Route only the Conductor service zone to its DNS server, preserve unrelated DNS servers and search domains, and record their existing values before changing them. The examples use `192.0.2.53` as the Conductor DNS address. Replace it with the manager's reachable address. Apply changes only on hosts that need service lookup.

## Linux with systemd-resolved

`systemd-resolved` chooses DNS servers by link scope, not by individual server within a link. All DNS servers configured on one link must answer equivalently for the domains routed there. Do not put an ordinary resolver and Conductor's authoritative-only DNS listener together on the same link. Use either a dedicated lookup interface/scope that has only the Conductor DNS server, or configure the existing recursive resolver to conditionally forward the Conductor zone to that server. See the [systemd-resolved routing and DNS server equivalence rules](https://www.freedesktop.org/software/systemd/man/latest/systemd-resolved.service.html#Protocols%20and%20Routing). The examples below are operator guidance and have not been exercised on a deployment host.

For the dedicated-interface option, first inspect `resolvectl status <interface>` and confirm the chosen interface is dedicated to this DNS scope. Do not replace settings on a shared interface. This example assumes the dedicated interface is `tun0`:

```sh
sudo resolvectl dns tun0 192.0.2.53
sudo resolvectl domain tun0 '~service.conductor.internal' \
  default.service.conductor.internal
```

The tilde-prefixed domains route only the configured service zones to the dedicated link. For overlay publication, add `~service.overlay.internal` and the optional unprefixed search domain `default.service.overlay.internal` alongside the host-network suffix. Verify both full names and any desired short names, then verify an unrelated name still uses its ordinary resolver. To roll back, restore the exact DNS server and domain lists recorded before setup using `resolvectl dns` and `resolvectl domain`. Do not use `resolvectl revert` if the interface has other per-link DNS settings that must remain. This procedure requires systemd-resolved to be the active resolver. On NetworkManager-managed systems, configure split DNS through the connection settings instead of mixing network managers.

## macOS

Supplemental resolvers in `/etc/resolver` route only matching domains to the configured DNS server and leave unrelated names with existing system resolvers. Install a file for the configured suffix:

```sh
sudo install -d -m 755 /etc/resolver
printf 'nameserver 192.0.2.53\n' \
  | sudo tee /etc/resolver/service.conductor.internal
```

Use `scutil --dns` to confirm the supplemental resolver points at the intended address. Verify the FQDN with `dscacheutil -q host -a name backend.default.service.conductor.internal`, and verify an unrelated name still resolves. Roll back by removing only the file created for this zone:

```sh
sudo rm /etc/resolver/service.conductor.internal
```

For optional short-name lookup, append `default.service.conductor.internal` to the active network service's existing search domains with Network Settings or `networksetup -setsearchdomains`. Record and restore the previous list when rolling back. Search-domain changes apply to host processes generally.

# Connect to a dynamic SRV port

The API returns dynamic ports through SRV records. A client can use `dig` to resolve one record and `curl --connect-to` to connect to its target and port while preserving the service hostname in the HTTP URL and Host header. This example requires Bash process substitution, `dig`, and `curl`:

```sh
service=backend.default.service.conductor.internal
port_name=http
while read -r priority weight port target; do
  [ -n "$port" ] || continue
  curl --fail --connect-timeout 2 \
    --connect-to "$service:80:$target:$port" \
    "http://$service/health"
  break
done < <(dig +short SRV "_$port_name._tcp.$service")
```

Applications should resolve at connection time and retry another SRV answer when a selected endpoint fails. Do not put worker addresses or assigned ports into application configuration.
