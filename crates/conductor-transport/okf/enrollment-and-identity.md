---
type: Reference
title: Worker enrollment and identity
description: Single-use manager credentials authorize a worker-generated key and certificate, which are persisted and used for mutual TLS.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/identity.rs
    title: Enrollment and identity records
  - resource: ../src/structs/worker_session.rs
    title: Certificate identity validation
  - resource: ../../conductor/src/main.rs
    title: CLI bootstrap and worker startup
  - resource: ../src/lib.rs
    title: Enrollment transport routes
---

# Enrollment flow

An operator creates a single-use enrollment credential for a stable worker ID
through the local manager command. The credential and public manager CA
certificate are provisioned to the intended worker over a trusted channel.
The worker creates its private key locally and redeems the credential over
HTTPS after verifying the configured manager CA and hostname. The manager
issues a certificate bound to the authorized worker identity. The worker
persists identity material in a protected file and removes the consumed
credential.

On later starts, the worker reuses its identity file. A revoked certificate or
credential cannot establish an authorized session. Protect private keys,
enrollment files, and manager CA key material; never distribute the CA private
key to workers.

## Overlay identity endpoints

After ordinary worker enrollment, the active mTLS certificate also authorizes
`PUT /v1/worker/network-domains/{id}/identity` with the worker-generated
`endpointId`. The manager binds that public Iroh endpoint identity to the
authenticated WorkerId, issues a five-minute lease, and returns the durable
worker address and generations. Key replacement increments the peer
generation; a revoked or expired endpoint is retired and cannot be replayed.

`GET /v1/worker/network-domains/{id}` returns that worker's bounded desired
peer configuration. `POST /v1/worker/network-domains/{id}/observation` records
the worker's applied generation and status; the manager timestamps the report
and rejects identity mismatches and stale generations. The private Iroh key
and workload packets never cross these endpoints. These control-plane routes
do not create a TUN device or install a host route.

# Validation boundary

Transport binds the authenticated certificate identity to the requested
worker ID and incarnation and rejects mismatches. The manager owns enrollment
and revocation state; the worker owns its private key. See the executable
[bootstrap CLI](../../conductor/okf/cli-and-configuration.md) for command
syntax and the transport tests for rejection cases.
