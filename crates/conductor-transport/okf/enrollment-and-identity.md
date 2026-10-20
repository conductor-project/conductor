---
type: Reference
title: Worker enrollment and identity
description: Single-use manager credentials authorize a worker-generated key and certificate, which are persisted and used for mutual TLS.
status: stable
source_revision: c3fbba5483c79e27e0687e445fde1a89ace9d872
source_reviewed_at: 2026-10-20
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
peer configuration. `POST /v1/worker/network-domains/{id}/candidates` accepts
up to eight direct UDP IP:port candidates from the authenticated worker and
requires its current peer generation. The manager rejects unroutable,
duplicate, oversized, or stale advertisements, advances the peer-map
generation when the set changes, and returns that generation with the
unchanged authorization expiry. Candidate addresses are hints, not identity
or authorization. `POST /v1/worker/network-domains/{id}/observation` records
the worker's applied generation and status; the manager timestamps the report
and rejects identity mismatches and stale generations. A bounded
`POST /v1/worker/network-domains/{id}/probe` records a completed application
HTTP probe from the authenticated worker. The manager checks source generations
and derives the target worker from its active address assignment; it never
trusts a caller-provided target worker ID. Probe results are separate from
helper-reported interface and Iroh telemetry. The private Iroh key
and workload packets never cross these endpoints. These control-plane routes
do not create a TUN device or install a host route.

# Validation boundary

Transport binds the authenticated certificate identity to the requested
worker ID and incarnation and rejects mismatches. The manager owns enrollment
and revocation state; the worker owns its private key. See the executable
[bootstrap CLI](../../conductor/okf/cli-and-configuration.md) for command
syntax and the transport tests for rejection cases.
