---
okf_version: "0.2"
---

# conductor-worker crate bundle

- [Overview](overview.md) - worker ownership and backend boundary.
- [Supervision and recovery](supervision-and-recovery.md) - durable commands, state, incarnation, and restart behavior.
- [Deadlines, readiness, logs, and inventory](deadlines-readiness-and-logs.md) - lifecycle mechanics and observations.

This bundle describes worker-owned state and behavior. Native process details
live in `conductor-process`.
