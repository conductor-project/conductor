---
type: Component
title: conductor-transport overview
description: HTTPS and mutual TLS connect worker sessions to manager-owned command and report ports.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Transport routes and hub
  - resource: ../src/structs/worker_session.rs
    title: Authenticated session state
  - resource: ../src/structs/worker_transport_client.rs
    title: Worker HTTP client
  - resource: ../src/identity.rs
    title: Identity and enrollment records
---

# Responsibility

The transport crate adapts manager `WorkerPort` and worker command/report
interfaces over authenticated HTTPS/mTLS. It validates a certificate-bound
worker identity, worker incarnation, protocol compatibility, and bounded JSON
messages before invoking manager behavior. It is separate from the loopback
operator API.

# Trust and ownership

The manager controls worker identity enrollment and revocation. The worker
generates and stores its private key locally. Transport messages contain stable
IDs and bounded data; they do not expose worker filesystem paths or PIDs. The
manager remains authoritative for worker labels, placement, and command
assignment.
