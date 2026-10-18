# Service discovery

Conductor can expose eligible service allocation endpoints over the manager API and an optional authoritative DNS listener. Discovery is manager-local: a single-manager outage has no peer to take over catalog queries.

## Stable names and records

MVP2 reserves `default` as its only namespace. The stable fully qualified name is:

```text
<service>.default.service.<dnsSuffix>.
```

The default suffix is `conductor.internal`; configure another deployment-owned suffix with `service_dns.suffix` (at most 104 ASCII bytes so the longest supported SRV name fits DNS limits). A service name is 1–63 lowercase ASCII letters, digits, or hyphens; it starts with a lowercase letter and cannot end with a hyphen. DNS comparisons are case-insensitive. One job owns a service identity at a time; duplicate `namespace/name` ownership is rejected as a conflict. `default` is the sole supported namespace, but the namespace label is part of every FQDN now so a future namespace can be added without changing existing names. Short names are a resolver search convenience and are not the stable identity. Per-namespace policy and per-workload DNS views are not provided.

For a service with multiple ready replicas, A and AAAA queries return the IPv4 and IPv6 addresses advertised by each eligible allocation. They do not include a port. Named TCP ports (names up to 62 characters, so the leading SRV underscore fits the DNS label limit) are published as SRV records at `_<port-name>._tcp.<service-fqdn>`. Each SRV target is the service FQDN and its port is the dynamically assigned host port. Conductor currently advertises TCP service ports. An ordinary HTTP client does not query SRV: fixed-port clients can use the FQDN only when every selected endpoint shares the configured fixed port; dynamic ports require an SRV-aware client to select an SRV answer and connect to its target and port.

The HTTP manager API provides `GET /v1/services/default/<service>/endpoints` for a snapshot and `GET /v1/services/default/<service>/watch?waitSeconds=30` for a bounded long poll. Watch time is 1–30 seconds (default 30), changes are sampled once per second, and at most 16 watch requests are active per daemon. Both routes use the configured manager API access policy.

## DNS behavior and convergence

Enable the DNS listener in a manager or combined-mode config. If omitted, DNS is disabled. The listener serves UDP and TCP on the configured socket. Loopback clients are allowed by default; a non-loopback listener requires explicit `allowedClients` CIDR ranges. Restrict those ranges to workload hosts that need discovery.

```json
{
  "service_dns": {
    "listen": "127.0.0.1:53",
    "suffix": "conductor.internal"
  }
}
```

The positive TTL is 2 seconds. Unknown names inside the configured service zone return NXDOMAIN with a 1-second negative SOA TTL. Known services with no eligible address or requested port return NOERROR/NODATA with the same 1-second SOA TTL. Names outside the zone return REFUSED. A manager storage/lookup failure returns SERVFAIL and is not negatively cached. If the manager process is down, the listener is down too; clients see their resolver's timeout/failure behavior. There is no replicated catalog or discovery failover.

Each native worker that hosts service ports also needs a service network with a non-overlapping host-port range and an `advertisedAddress` reachable by other workload hosts. In manager mode, configure the local worker with `worker_service_network` and additional workers with `additional_workers[].service_network`. The DNS records expose these advertised addresses and assigned host ports; they do not establish cross-host routing or open host firewalls.

Readiness changes are published when the worker observes the configured probe threshold and reports it to the manager. The health-detection part is bounded by up to `failureThreshold × (intervalMs + timeoutMs)` for a failing probe, plus report delivery and one catalog lookup. An explicit drain removes endpoints as soon as the manager's worker status reports draining. A disconnected worker is considered fresh for at most 45 seconds; after that, its endpoints are withdrawn. DNS caches may retain an old positive answer for at most 2 seconds after withdrawal, and a negative answer for at most 1 second after a service gains an endpoint. Clients should re-resolve before opening new connections and retry eligible SRV/A/AAAA results after connection failures. DNS changes do not close established connections. These bounds assume the manager and its storage are responsive; a manager outage returns SERVFAIL while running and resolver failure while stopped.

## Host resolver setup

The resolver listener must be reachable on the standard DNS port from each host that will run workloads. Use a manager address reachable from those hosts and allow only those host CIDRs. For an all-in-one local daemon, loopback can be used. Resolver changes are host-wide for native processes; Conductor does not configure per-workload search domains. Keep the pre-existing DNS servers and search domains when using the split-DNS setup, and record them before applying changes so rollback restores the prior state.

### Linux with systemd-resolved

Use a per-link route-only domain so only the Conductor zone is sent to the manager DNS server. Add the suffix as a search domain only if short-name lookup (`backend`) is desired. Keep the link's existing DNS servers and domains in the command, as shown by `resolvectl status <interface>`; the following example assumes the existing DNS server is `192.0.2.1` and existing search domain is `corp.example`:

```sh
sudo resolvectl dns eth0 192.0.2.1 192.0.2.53
sudo resolvectl domain eth0 corp.example '~service.conductor.internal' default.service.conductor.internal
```

Here `192.0.2.53` is the Conductor DNS address. The tilde-prefixed `service.conductor.internal` domain routes only service queries to that link. The unprefixed `default.service.conductor.internal` search domain makes the short name `backend` expand to the service FQDN in the sole MVP2 namespace. For another configured suffix, substitute `service.<suffix>` and `default.service.<suffix>`. If short names are not wanted, omit the unprefixed search domain. Verify `resolvectl query backend.default.service.conductor.internal` and `resolvectl query backend`, then verify an unrelated name still resolves through the original resolver. To roll back, restore the exact DNS server and domain lists recorded before setup with `resolvectl dns` and `resolvectl domain`. Do not use `resolvectl revert` when the interface has other per-link DNS settings that must remain.

This setup requires systemd-resolved to be the host's active resolver. On NetworkManager-managed systems, configure the same split DNS server and route-only domain through the connection's DNS settings instead of mixing two network managers.

### macOS

macOS supplemental resolvers in `/etc/resolver` route only matching domains to the configured server, leaving unrelated names with the existing system resolvers. Install a file for the configured suffix (replace the example address):

```sh
sudo install -d -m 755 /etc/resolver
printf 'nameserver 192.0.2.53\n' | sudo tee /etc/resolver/service.conductor.internal
```

Use `scutil --dns` to confirm that the supplemental resolver for `service.conductor.internal` points at `192.0.2.53`, then verify the FQDN with `dscacheutil -q host -a name backend.default.service.conductor.internal` and an unrelated name with `dscacheutil`. Remove only the file created for this service zone to roll back:

```sh
sudo rm /etc/resolver/service.conductor.internal
```

For optional short-name lookup, append `default.service.conductor.internal` to the active network service's existing search domains with Network Settings or `networksetup -setsearchdomains`; preserve and restore the previous list when rolling back. Verify with `dscacheutil -q host -a name backend`. Short-name search settings apply to host processes generally.

## SRV-aware HTTP example

This example uses `dig` to resolve the SRV record, then `curl --connect-to` to connect to one returned target and dynamic port while preserving the service hostname in the HTTP URL and Host header. `dig` and `curl` are client tools; no custom DNS packet parsing is involved.

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

The frontend and backend can use the same contract in either direction: configure each service with a `http` TCP service port and resolve the other service's FQDN/SRV name at connection time. Never put a worker's advertised address or assigned host port into the application configuration. A production client should retry another SRV answer when a selected endpoint fails.

## Example configuration for a remote manager

```json
{
  "service_dns": {
    "listen": "192.0.2.53:53",
    "suffix": "conductor.internal",
    "allowedClients": ["192.0.2.0/24", "2001:db8:10::/64"]
  }
}
```

Use routable addresses and real CIDRs in deployment configuration. The listener access list is checked per DNS client address; it is not an authentication mechanism and should not be exposed to untrusted networks.
