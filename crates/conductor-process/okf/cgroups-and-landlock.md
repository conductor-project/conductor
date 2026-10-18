---
type: Reference
title: Linux cgroup v2 and Landlock enforcement
description: Optional Linux controls apply resource limits and selected filesystem restrictions before execution, subject to kernel and administrator setup.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/cgroup.rs
    title: Cgroup v2 control and cleanup
  - resource: ../src/landlock.rs
    title: Landlock rules and protected paths
  - resource: ../src/structs/cgroup_backend.rs
    title: Cgroup-aware backend
  - resource: ../tests/landlock_paths.rs
    title: Landlock path validation tests
  - resource: ../../../examples/conductor.service
    title: systemd delegation example
---

# Cgroup v2

When enabled on Linux, the backend places a run in an administrator-delegated
cgroup before exec and applies configured CPU, memory, swap, and process-count
limits. It cleans descendants in that subtree. Conductor does not configure
systemd delegation or change controllers on ancestors. A daemon UID that can
also modify control files can undermine the intended policy; protect the
delegated hierarchy and ownership-lock directory.

Cgroups provide resource limits and descendant cleanup. They do not isolate
files or networking, hide host metadata, or prevent every same-user
interference. If a job requests limits while enforcement is disabled or
unavailable, it fails before its executable runs.

The ownership-lock directory is administrator-provisioned persistent control
state. Every daemon that can access the delegated cgroup root must see the same
underlying filesystem directory. Do not place it under `/tmp`, a systemd
`PrivateTmp` path, or another mount-namespace-private location. Keep it and its
lock files in place while a daemon may own the cgroup namespace; do not replace
or clean it during service restarts. For a shared group-writable directory,
use a sticky mode such as `root:conductor` `1770` so another UID cannot remove
an owned lock file. Startup rejects unsafe, missing, or inaccessible paths and
does not fall back to a private directory. The lock directory must not overlap
Landlock grants.

Conductor creates a `conductor-<worker-id-hash>` child under the delegated
root and holds an advisory ownership lock named from that worker namespace and
delegated root. Reserve each child namespace for one worker. On startup, the
backend removes stale `run-*` child groups only inside its owned worker
subtree; it does not configure ancestors or clean another worker's subtree.

Example worker configuration (the delegated root must be provisioned by the
administrator and have `cpu`, `memory`, and `pids` enabled for child groups):

```json
{
  "cgroup_v2": {
    "enabled": true,
    "delegatedRoot": "/sys/fs/cgroup/system.slice/conductor.service",
    "ownershipLockDir": "/var/lib/conductor/cgroup-locks",
    "maximums": {
      "cpuQuotaUs": 200000,
      "cpuPeriodUs": 100000,
      "memoryMaxBytes": 536870912,
      "memorySwapMaxBytes": 0,
      "pidsMax": 128
    }
  }
}
```

`maximums` are administrator ceilings. A job can request values under those
ceilings; omitted job values inherit the corresponding maximum, and a request
above any configured maximum is rejected before launch. `memorySwapMaxBytes: 0`
disables swap for the run. A cgroup resource request requires Linux cgroup v2
enforcement; the manager also requires service CPU/memory reservations to
cover configured CPU and memory ceilings.

# Landlock

Landlock mode is `disabled`, `optional`, or `required`. Jobs may select only
administrator-defined grant IDs; they cannot supply paths or rights. Required
mode rejects missing policy, unsupported ABI, unknown grants, or setup failure
before execution. Ordinary Unix permissions still apply. Grants must account
for executable, loader, libraries, inputs, and outputs. Protected paths prevent
configured grants overlapping daemon state and other sensitive paths.

Landlock restricts selected filesystem operations. It does not isolate network
access or mounts, hide all metadata, revoke already-open handles, or prevent
all interactions between processes sharing a UID. Cgroups and Landlock are
independent controls. Read the implementation and tests before claiming a
specific kernel guarantee.

`mode` is `disabled`, `optional`, or `required`. Disabled is the default and
cannot have grants. Optional mode applies a configured policy where supported;
jobs without a filesystem policy remain unrestricted by Landlock. Required
mode rejects jobs without an administrator-approved policy and fails the launch
if the kernel ABI or policy setup is unavailable. A job selects grant IDs in
`filesystemPolicy`; it cannot submit grant paths or rights. Each grant names
one file or directory and the `read`, `write`, and `execute` rights the admin
allows. Configure executable, loader, libraries, inputs, and outputs as
needed. The checked-in [Landlock example config](../../../examples/landlock-agent.json)
and [job](../../../examples/job-landlock.json) show the shape; paths and loader
locations are host-specific.

```json
{
  "landlock": {
    "mode": "required",
    "protectedPaths": ["/opt/conductor/credentials"],
    "grants": [
      {"id":"program","path":"/opt/app/bin/server","read":true,"execute":true},
      {"id":"data","path":"/var/lib/app","read":true,"write":true}
    ]
  }
}
```

The job opts into these grants by ID:

```json
{"filesystemPolicy":{"grants":["program","data"]}}
```
