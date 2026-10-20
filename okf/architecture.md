---
type: Architecture
title: Conductor crate boundaries and runtime ownership
description: The manager owns desired state, the worker owns execution, and explicit ports connect transport, API, and process layers.
status: stable
source_revision: a622643
source_reviewed_at: 2026-10-20
sources:
  - resource: ../crates/conductor-core/src/lib.rs
    title: Transport-neutral contracts
  - resource: ../crates/conductor-manager/src/lib.rs
    title: Manager operations
  - resource: ../crates/conductor-worker/src/lib.rs
    title: Worker implementation
  - resource: ../crates/conductor-api/src/lib.rs
    title: HTTP API
  - resource: ../crates/conductor-transport/src/lib.rs
    title: Worker transport
  - resource: ../crates/conductor/src/main.rs
    title: Process composition
---

# Purpose

Conductor separates desired state and placement from host execution. Stable
identifiers and explicit command/report contracts cross those boundaries; PIDs
and worker filesystem paths remain internal to the worker.

## Ownership

| Crate | Owns | Does not own |
| --- | --- | --- |
| [`conductor-core`](../crates/conductor-core/okf/index.md) | Stable IDs, job and service models, execution commands and observations | HTTP, databases, process handles, scheduling side effects |
| [`conductor-manager`](../crates/conductor-manager/okf/index.md) | Desired state, scheduling, durable manager records, reconciliation | Worker implementation or process handles |
| [`conductor-worker`](../crates/conductor-worker/okf/index.md) | Accepted command/report durability, supervision, logs, deadlines, execution backend interface | Global desired state or HTTP routing |
| [`conductor-process`](../crates/conductor-process/okf/index.md) | Native process launch and platform enforcement mechanisms | Job policy or placement decisions |
| [`conductor-api`](../crates/conductor-api/okf/index.md) | HTTP route, auth, validation, and response mapping | Direct SQLite access or process lifecycle |
| [`conductor-transport`](../crates/conductor-transport/okf/index.md) | Authenticated manager-worker session and message exchange | Manager policy or worker process execution |
| [`conductor`](../crates/conductor/okf/index.md) | CLI, strict config, locks, composition, shutdown | A second implementation of crate-owned business rules |

## Runtime shapes

The `all` role composes manager, worker, API, and process backend in one daemon
for a local deployment. The `manager` role exposes the operator API and worker
transport. A `worker` connects outbound to a manager using an enrolled mTLS
identity. The manager's current composition includes explicitly configured
worker adapters; do not infer an unconfigured general placement service from
the existence of transport.

The API calls manager operations. Manager-to-worker interaction uses command
and report ports; remote transport implements those boundaries over HTTPS and
mTLS. The `conductor` binary chooses composition and coordinates lifecycle.

The accepted MVP5 Iroh host-network overlay architecture, packet-tunnel
boundary, addressing, leases, and failure behavior are described in the
[overlay networking architecture](overlay-networking-architecture.md). The
manager identity and address-allocation control plane is implemented; the
Iroh packet adapter and network backend are still in progress.

## Source and verification

Read this with each crate's contracts. The ownership statements are source
reviewed at the revision recorded in frontmatter; consult the crate bundles for
tests that exercise each interface. This page does not claim an independent
multi-host qualification.
