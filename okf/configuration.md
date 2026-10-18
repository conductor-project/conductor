---
type: Reference
title: Conductor configuration and paths
description: Strict JSON selects daemon roles, listeners, persistent state, worker identity, and optional host enforcement.
status: stable
source_revision: a739b4c20dd8e81ded656dfa7bb9859f48d73f9e
source_reviewed_at: 2026-10-19
sources:
  - resource: ../crates/conductor/src/main.rs
    title: Configuration types and validation
  - resource: ../examples/local.json
    title: Combined role example
  - resource: ../examples/manager.json
    title: Manager example
  - resource: ../examples/worker-one.json
    title: Worker example
---

# Configuration rules

The daemon reads JSON and rejects unknown fields. The `agent --config` path is
required. `--role` may override the JSON `role`, but the selected role's
required settings are still validated before state or listeners are opened.
See the [`conductor` CLI bundle](../crates/conductor/okf/cli-and-configuration.md)
for all currently accepted keys.

| Role | Main settings | Relative path basis |
| --- | --- | --- |
| `all` | `listen`, manager/worker state dirs, `worker_id`, optional concurrency | Working directory for state, cgroup, and Landlock paths |
| `manager` | Operator API, worker transport listener, manager identity and CA directory | Configuration file directory for role state and identity paths |
| `worker` | Manager endpoint, CA, worker ID/state, enrollment or identity file | Configuration file directory for role state and identity paths |

`api_access.token_file` is relative to the configuration file directory in all
roles. In `all` mode, standalone identity fields are rejected. Absolute paths
are unchanged. When `--role` overrides an `all` config to a standalone role,
relative manager and worker data paths retain their historical working
directory basis; set absolute paths when clarity is important.

# Local combined example

[`examples/local.json`](../examples/local.json) binds the API to
`127.0.0.1:8080`, selects `role: all`, and keeps separate manager and worker
state directories under `./state`. It uses local API access, two worker slots,
and disabled cgroup and Landlock enforcement. Resolve its relative state paths
from the repository root when using it.

# Operator access

Local API mode is unauthenticated and intended for a loopback-only listener.
`operator_token_file` mode reads a token from a protected file and authenticates
operator API routes; `/healthz` and `/readyz` remain public. Remote access
should terminate TLS at a trusted local proxy. The operator token file and
proxy configuration must be protected. More detail lives in the
[API auth bundle](../crates/conductor-api/okf/authentication-and-bounds.md).

# Optional host enforcement

Linux cgroup v2 and Landlock configuration is opt-in. A job requesting a
resource limit or filesystem policy fails before launch if the worker cannot
apply the requested policy. MacOS does not provide these Linux enforcement
mechanisms. Do not interpret successful configuration parsing as proof of
effective enforcement; see [process enforcement boundaries](../crates/conductor-process/okf/cgroups-and-landlock.md).

# Service network and resource domains

Managers require explicit scheduling domains for shared-host capacity and
optional service port publication. A `host_resource_domains` entry gives one
aggregate CPU/memory budget; workers on the same physical host must use the
same `resource_domain_id`, while distinct hosts use distinct domains. The
configured budget must fit the inventory the workers report. This is manager
scheduling accounting, not a kernel quota.

For a worker that hosts service ports, `worker_service_network` supplies a
`networkDomainId`, non-overlapping `portRangeStart` / `portRangeEnd`, and the
worker's routable `advertisedAddress`. These are the accepted JSON key names;
unknown snake_case spellings are rejected. Each `additional_workers` entry
uses its own `service_network` with those same camelCase network keys. Workers
sharing one network use the same network-domain ID and disjoint ranges. The
manager keeps logical port leases; it does not hold the workload's socket
open, so the service process must bind the assigned port itself. See
[`examples/manager.json`](../examples/manager.json). DNS SRV targets identify
their replica by advertised IP and port; service-wide A/AAAA records continue
to aggregate eligible replica addresses. See the [service discovery
guide](service-discovery.md) for record behavior and verification scope.
