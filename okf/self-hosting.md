---
type: Runbook
title: Self-hosting Conductor
description: Install from source, configure a daemon account, operate combined or separate roles, and protect persistent state.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../README.md
    title: First-run workflow
  - resource: ../examples/conductor.service
    title: systemd unit example
  - resource: ../docs/standalone-roles.md
    title: Previous role walkthrough, audited for this guide
  - resource: ../crates/conductor/src/main.rs
    title: CLI and daemon composition
---

# Install

The repository currently documents source builds and does not publish a
downloadable binary or package-manager channel. Build with Rust 1.99.1, the
version pinned in `rust-toolchain.toml`:

```sh
cargo +1.99.1 build --locked -p conductor
```

The binary is `target/debug/conductor`. The [README first-run steps](../README.md)
exercise a combined local daemon and submit a job. That flow is a debug build;
release builds and tests are reserved for post-merge CI except when reproducing
a specific CI failure.

# Deployment account and state

Run the daemon as a dedicated OS account with only the filesystem permissions
and network access the workload trust level permits. Job processes inherit
that account's ordinary OS permissions and network access. The process backend
is not a security sandbox. Keep manager state, worker state, API credentials,
identity keys, and configuration protected from workload writes.

Manager and worker state directories are persistent. Back up manager SQLite
state only while the daemon is stopped so database and WAL files remain
consistent. Do not delete these directories during normal shutdown or as a
generic demo cleanup.

# Roles

`role: all` is the straightforward single-host deployment. `role: manager`
starts the loopback operator API and a separate HTTPS/mTLS worker listener.
`role: worker` starts an outbound worker session; it does not require an
inbound listener. Separate roles require explicit manager identity, enrollment,
and endpoint configuration. See the [conductor CLI and composition bundle](../crates/conductor/okf/cli-and-configuration.md)
and [transport identity bundle](../crates/conductor-transport/okf/enrollment-and-identity.md)
for the exact configuration and enrollment behavior.

On Linux, a standalone worker may opt into manager-assigned overlay networking
with `worker_overlay_network_domain_id` and
`worker_overlay_helper_binary`. Only the helper needs `CAP_NET_ADMIN`; do not
grant that capability to the main agent or workload processes. The helper runs
one worker Iroh endpoint with the manager-distributed direct candidate map and
the tun-rs packet adapter. Iroh relays, public/default address discovery, and
automatic NAT traversal are disabled; peers must be directly reachable on the
declared LAN/VPC or configured underlay.

The API listener is loopback-only. To permit remote operator access, use a
trusted local TLS proxy and configure operator-token authentication. The
worker transport listener has a distinct purpose and trust policy; do not
expose the unauthenticated local API as a remote control plane.

# systemd

[`examples/conductor.service`](../examples/conductor.service) is an example,
not a complete installer. It expects the binary at
`/usr/local/bin/conductor`, configuration at `/etc/conductor/agent.json`, and
state below `/var/lib/conductor`. Install those paths with ownership and
permissions appropriate to the dedicated `conductor` account, then copy the
unit into `/etc/systemd/system/conductor.service` and review it for the target
host. Its `UMask=0077` keeps daemon-created files private to that account.

For example, after installing the binary and writing a valid configuration:

```sh
sudo install -d -o root -g conductor -m 0750 /etc/conductor
sudo install -d -o conductor -g conductor -m 0700 /var/lib/conductor/manager
sudo install -d -o conductor -g conductor -m 0700 /var/lib/conductor/worker
sudo install -o root -g root -m 0644 examples/conductor.service \
  /etc/systemd/system/conductor.service
sudo systemctl daemon-reload
sudo systemctl enable --now conductor.service
systemctl status conductor.service
journalctl -u conductor.service
```

Set `manager_data_dir` and `worker_data_dir` in the configuration to the two
state paths above. If cgroup v2 management is enabled, configure its delegated
root and absolute ownership-lock directory, prepare the lock directory for the
daemon account (for example, `sudo install -d -o root -g conductor -m 1770
/var/lib/conductor/cgroup-locks` for a shared group-writable directory), and
retain the unit's `Delegate=cpu memory pids`; the unit
comment calls out that delegation requirement. Leave cgroup management disabled
when the host has not been prepared for it. Review `ExecStart`, paths,
permissions, and delegation before enabling the unit. It sets `Restart=no` and
`KillMode=control-group`; it does not promise automatic workload recovery after
daemon failure. The [operations guide](operations-and-recovery.md) describes
uncertain runs after restart.

# Evidence limits

CI builds and tests Linux x86_64 and Apple silicon macOS. The README walkthrough
is exercised separately on the recorded developer platform. CI evidence does
not qualify every filesystem, kernel configuration, network, or remote host
topology. See [platforms and evidence](platforms-and-evidence.md) for the exact
scope.
