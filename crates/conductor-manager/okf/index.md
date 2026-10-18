---
okf_version: "0.2"
---

# conductor-manager crate bundle

- [Overview](overview.md) - manager ownership and ports.
- [Desired state and persistence](desired-state-and-persistence.md) - SQLite schemas and transaction boundaries.
- [Scheduling, reconciliation, and service catalog](scheduling-and-services.md) - assignments, deployments, drains, ports, and audit.

This bundle describes manager-owned behavior. Worker-local persistence is
documented in the independent `conductor-worker` bundle.
