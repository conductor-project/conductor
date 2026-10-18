---
type: Reference
title: Process spawn and cleanup
description: The backend executes a configured program directly, tracks its process group, and performs bounded termination and cleanup.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/structs/host_process_backend.rs
    title: Native process backend
  - resource: ../src/lib.rs
    title: Spawn and wait implementation
  - resource: ../../conductor-worker/src/lib.rs
    title: Worker supervision contract
  - resource: ../../conductor/tests/agent.rs
    title: Binary-level process lifecycle tests
---

# Launch

The backend passes the configured executable and argument array to the OS
process API. It does not invoke a shell. The working directory and environment
are configured explicitly. The configured command is launched on the worker
host, so its executable, interpreter, shared libraries, and files must be
available there.

# Termination

Each job starts in a dedicated process group. On stop/deadline, the backend
sends SIGTERM to the owned group, waits the configured grace period, then sends
SIGKILL to remaining ordinary group members. It also cleans up ordinary group
members after the leader exits. A workload can deliberately leave its group;
process groups do not form a containment boundary.

When configured, cgroup cleanup supplements process groups on Linux. A daemon
crash can leave externally owned work, which may become unknown after restart.
The backend reports spawn failures and observed exit status/signal to the
worker; it does not claim exactly-once external side effects.
