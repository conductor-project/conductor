---
okf_version: "0.2"
---

# conductor-transport crate bundle

- [Overview](overview.md) - authenticated manager/worker transport boundary.
- [Enrollment and identity](enrollment-and-identity.md) - credential redemption and certificate lifecycle.
- [Commands, reports, and reconnect](commands-reports-and-reconnect.md) - polling, ordering, acknowledgement, freshness, and delivery.

This bundle can be read independently. The HTTP API used by operators is
documented in `conductor-api`; this crate serves worker transport.
