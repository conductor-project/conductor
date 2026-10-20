---
type: Reference
title: conductor composition, enrollment, and lifecycle
description: The executable wires local or separated manager/worker roles and controls state locks, startup reconciliation, and graceful shutdown.
status: stable
source_revision: e10f326
source_reviewed_at: 2026-10-20
sources:
  - resource: ../src/main.rs
    title: Runtime assembly, enrollment, shutdown
  - resource: ../src/state_lock.rs
    title: Exclusive daemon state locks
  - resource: ../../conductor-api/src/lib.rs
    title: API readiness and listener serving
  - resource: ../../conductor-transport/src/identity.rs
    title: Worker identity lifecycle
  - resource: ../../conductor/tests/agent.rs
    title: Real-process integration coverage
---

# Composition

In `all` mode the binary assembles one manager, worker, process backend, and
operator API. In `manager` mode it serves the operator API and HTTPS/mTLS worker
transport; in `worker` mode it enrolls or loads a persisted identity and polls
the configured manager over outbound HTTPS. The distinct roles preserve
manager/worker ownership; network transport is an adapter, not a shared
database.

Use [`examples/manager.json`](../../../examples/manager.json) and
[`examples/worker-one.json`](../../../examples/worker-one.json) as complete
role-specific configuration references. Start the manager with
`conductor agent --config /etc/conductor/manager.json`; start each worker with
its own config after provisioning its identity. The manager's operator API is
loopback-only and independent from its worker HTTPS/mTLS listener. Workers
connect outbound and do not need an inbound listener.

# Enrollment and identity

`enroll-worker` creates a single-use manager-authorized enrollment credential.
The worker generates its private key locally, redeems the credential over
verified HTTPS, persists the resulting identity, and removes the consumed
credential. `revoke-worker` revokes the enrolled identity and unused
credentials. Protect manager identity material, CA keys, credentials, and
worker identity files. Exact protocol is in the
[transport identity bundle](../../conductor-transport/okf/enrollment-and-identity.md).

Create one credential per worker from the manager host. The raw credential is
written once to a new owner-only file and expires within 24 hours:

```sh
conductor enroll-worker \
  --manager-data-dir /var/lib/conductor/manager \
  --identity-dir /var/lib/conductor/identity \
  --worker-id worker-east \
  --output /run/conductor/worker-east-enrollment
```

Provision only the one-time credential and public `worker-ca.pem` to that
worker over a trusted channel. Never copy `worker-ca-key.pem`; the private
worker key is created and retained on the worker. Configure the worker with
its unique `worker_id`, `worker_data_dir`, `manager_endpoint`,
`manager_ca_file`, `worker_enrollment_credential_file`, and
`worker_identity_file`. Start it with
`conductor agent --config /etc/conductor/worker.json`. The credential is
consumed and removed after the identity is persisted; later starts reuse the
identity file and do not need the enrollment credential. Each worker needs a
separate state directory, identity file, and credential.

An opt-in Linux worker can join a manager-owned overlay domain by setting
`worker_overlay_network_domain_id` and the absolute path
`worker_overlay_helper_binary`. Install the helper as a root-owned executable
available only to the Conductor service account, and grant only that helper
`CAP_NET_ADMIN`; the main `conductor` process and workload processes must not
receive it. The helper runs the worker's single Iroh endpoint and packet
adapter alongside the TUN and route lifecycle. The worker stores its Iroh secret in
`worker_data_dir/overlay-identity.key` with owner-only permissions, registers
the derived endpoint identity over worker mTLS, and refreshes its peer snapshot
from the manager. The selected topology is direct-only: workers exchange
authenticated EndpointIds and direct UDP candidates through the manager, and
Iroh relays, public/default address discovery, and automatic NAT traversal are
disabled. Peers must be directly reachable on the configured LAN/VPC or
underlay topology. This currently requires Linux; blocked direct paths fail
without relay fallback.

Revoke a worker from the manager host:

```sh
conductor revoke-worker \
  --manager-data-dir /var/lib/conductor/manager \
  --worker-id worker-east
```

Revocation invalidates its active certificates and any unused enrollment
credential. To enroll it again, stop the worker, issue a new credential,
provision the public CA and credential, remove the revoked identity file, set
`worker_enrollment_credential_file` to the new credential path, and restart the
worker. Use the operator's protected filesystem procedure for removing its old
private identity material.

# Locks, readiness, and shutdown

The executable locks its configured state against concurrent daemon owners.
Startup opens state and reconciles durable manager/worker observations before
the API admits runs. `/healthz` can respond while `/readyz` reports startup
recovery or missing/freshness-degraded workers. Ctrl-C and SIGTERM request
orderly shutdown: stop admission, allow bounded worker cleanup, and close
listeners. Shutdown deadlines are reported when bounded cleanup expires; do
not infer all child processes were contained by the daemon.

The binary integration suite starts the actual executable with temporary
manager/worker state and loopback listeners, polls readiness, exercises
requests, then cleans up. Consult the current tests before attributing a
specific lifecycle guarantee to an integration test.
