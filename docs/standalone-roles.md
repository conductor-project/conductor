# Standalone manager and worker roles

Standalone roles compose the existing manager, worker, and authenticated transport
interfaces in separate daemon processes. The combined `all` role remains the
single-process local deployment.

Protect the configuration, manager CA key, operator token, worker identity, and
enrollment credential with filesystem permissions appropriate to the daemon
account. Relative path handling depends on the selected role, as described
below; absolute paths remain unchanged.

## Manager configuration

The manager API remains loopback-only. `worker_transport_listen` is a separate
HTTPS/mTLS listener for workers and may bind a reachable interface. Configure
the name workers use in `manager_server_name`; it must match the manager server
certificate SAN.

```json
{
  "role": "manager",
  "listen": "127.0.0.1:8080",
  "worker_transport_listen": "0.0.0.0:9443",
  "manager_data_dir": "/var/lib/conductor/manager",
  "manager_identity_dir": "/var/lib/conductor/identity",
  "manager_server_name": "manager.example.test",
  "worker_id": "worker-one",
  "worker_transport_capacity": 2,
  "worker_resource_domain_id": "shared-host",
  "worker_labels": { "region": "example", "pool": "general" },
  "host_resource_domains": [
    { "id": "shared-host", "cpu_millis": 1000, "memory_bytes": 1073741824 }
  ],
  "additional_workers": [
    {
      "worker_id": "worker-two",
      "capacity": 2,
      "resource_domain_id": "shared-host",
      "labels": { "region": "example", "pool": "general" }
    }
  ],
  "api_access": { "mode": "operator_token_file", "token_file": "/etc/conductor/operator-token" }
}
```

The operator API starts independently of worker enrollment and remains
available while workers reconnect. `/readyz` reports degraded readiness until
all configured workers have established fresh authenticated sessions. The
current manager adapter dispatches one-shot runs through `worker_id`; additional
workers establish their own transport sessions and are ready for later placement
work. Capacity-aware placement across workers is tracked by issue #67. Manager
data and the CA key must be protected and backed up. The operator API accepts
connections only on loopback; a remote operator reaches it through a trusted
local TLS proxy.

Worker inventory reports OS, architecture, execution and enforcement capabilities,
CPU and memory totals and allocatable capacity, plus execution slots. Allocatable
capacity reserves 20% of the measured host total for the daemon and system. It is
inventory for placement, not utilization telemetry or a kernel-enforced limit.
The manager owns worker labels and resource-domain assignments. Use the same
`resource_domain_id` for workers sharing one physical host and configure one
aggregate budget for that domain; the manager rejects a shared domain without a
budget. The configured budget must fit within each worker's reported allocatable
capacity. For workers on different hosts, leave the domain IDs distinct. `/v1/workers`
and `/v1/workers/{id}` include the last inventory, manager labels, resource domain,
and `fresh`/`lastSeenAtMs` fields so stale reports remain visible during disconnects.

Issue a single-use enrollment credential locally for each configured worker:

```sh
conductor enroll-worker \
  --manager-data-dir /var/lib/conductor/manager \
  --identity-dir /var/lib/conductor/identity \
  --worker-id worker-one \
  --output /run/conductor/worker-one-enrollment
```

Repeat with the second worker ID and a new output file. Transfer the credential
and public `worker-ca.pem` to that worker through a trusted provisioning
channel. Never transfer `worker-ca-key.pem`.

## Worker configuration

Workers need only their own durable state, stable ID, concurrency, manager URL,
manager CA trust file, and credential or previously enrolled identity. On its
first start, a worker generates its private key locally, redeems the
single-use credential over HTTPS, persists the issued certificate and private
key in an owner-only identity file, then removes the consumed credential.
Subsequent starts reuse that identity file. Use a separate state directory,
identity file, enrollment credential, and worker ID for every process.
The worker trusts only the CA in `manager_ca_file` and verifies that the
`manager_endpoint` hostname matches the manager certificate before it sends an
enrollment credential. It does not fall back to operating-system trust roots.

```json
{
  "role": "worker",
  "worker_data_dir": "/var/lib/conductor/worker-one",
  "worker_id": "worker-one",
  "worker_concurrency": 2,
  "manager_endpoint": "https://manager.example.test:9443/",
  "manager_ca_file": "/etc/conductor/worker-ca.pem",
  "worker_enrollment_credential_file": "/run/conductor/worker-one-enrollment",
  "worker_identity_file": "/var/lib/conductor/worker-one/identity.pem"
}
```

Worker connections are outbound HTTPS on the configured manager port. Workers
do not need inbound listeners. Firewall rules should allow only the worker
transport port from enrolled worker hosts. The manager API port and worker
transport port serve different trust policies and should not be conflated.

## Start, readiness, and shutdown

Start the manager and then each worker. The manager logs its loopback `address`
and public `workerTransportAddress`; its operator API starts independently of
worker enrollment. `/healthz` reports that the manager process is serving, while
`/readyz` returns `503` with `status: degraded` until all configured workers have
connected and remain fresh. Readiness degrades again during a worker outage and
recovers after reconnection. Workers log their manager endpoint and remain
running through temporary network outages with bounded reconnect backoff.
Ctrl-C and SIGTERM stop admission, let worker processes perform their bounded
shutdown, and close the manager HTTP and mTLS listeners. Run the same procedure
on macOS or Linux; Linux cgroup v2 and Landlock settings apply only to Linux
workers and are rejected on other platforms when required.

All role objects use strict JSON and reject unknown fields. `--role` overrides
the `role` field, but the selected role's required fields are still validated
before state is opened or listeners are bound.

In a configuration whose role is `manager` or `worker`, relative manager or
worker state paths and role identity, CA, and enrollment paths resolve from the
configuration file's directory. `api_access.token_file` also resolves from that
directory in every role. The `--role` option selects role validation; when it
overrides an `all` configuration, relative manager and worker state paths retain
their historical working-directory location. Combined `all` mode keeps
relative state, cgroup, and Landlock paths working-directory-relative. Its
standalone identity fields are rejected, so those paths apply only to standalone
configurations. Relative cgroup and Landlock paths remain
working-directory-relative.

The process backend runs workloads with the worker daemon account's normal
permissions. On Linux, configured Landlock grants are rejected if they overlap
the worker state, identity, CA, enrollment credential, config, or other protected
control paths. macOS does not provide equivalent filesystem restrictions here;
do not treat native process execution as a security sandbox.
