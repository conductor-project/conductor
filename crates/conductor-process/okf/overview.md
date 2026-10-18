---
type: Component
title: conductor-process overview
description: The host backend implements worker execution using native operating-system process launch and optional Linux enforcement.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Backend interface and process mechanics
  - resource: ../src/structs/host_process_backend.rs
    title: Host backend implementation
  - resource: ../src/cgroup.rs
    title: Cgroup v2 setup
  - resource: ../src/landlock.rs
    title: Landlock policy setup
  - resource: ../tests/landlock_paths.rs
    title: Landlock path validation tests
---

# Responsibility

`conductor-process` launches and signals host processes for the worker. It
implements the worker backend boundary and owns native process handles,
process groups, cgroup paths, launch descriptors, and Landlock setup. It does
not decide desired replicas or expose OS handles at API boundaries.

# Trust boundary

The daemon account's ordinary OS permissions and network access apply to jobs.
The backend is not a general sandbox. Process-group cleanup is lifecycle
management; optional cgroups and Landlock add specific Linux guarantees only
when enabled, supported, and correctly configured.
