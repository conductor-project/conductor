---
type: Component
title: conductor-worker overview
description: The worker accepts manager commands, supervises runs through a backend interface, persists observations, and returns reports.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Worker service, persistence, and supervisors
  - resource: ../src/structs/worker_service.rs
    title: Command handling and SQLite state
  - resource: ../tests/runs.rs
    title: Worker run behavior tests
---

# Responsibility

The worker owns accepted command durability, local process supervision,
deadlines, readiness probes, run logs, observations, host inventory, and its
execution backend interface. The manager communicates using stable IDs and
observations; PIDs and worker filesystem paths remain local.

# Backend boundary

The worker delegates launch, stop, cleanup, and enforcement to a backend
interface implemented by `conductor-process`. Worker state transitions do not
claim a process is safely contained: native process groups and optional Linux
enforcement have separate limits.

# Evidence

`tests/runs.rs` exercises worker-level command and lifecycle behavior. The
repository binary tests and macOS multi-process tests exercise composition
through real processes on their tested host. See the
[process bundle](../../conductor-process/okf/index.md) for platform boundaries.
