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

# Validation boundary

Transport binds the authenticated certificate identity to the requested
worker ID and incarnation and rejects mismatches. The manager owns enrollment
and revocation state; the worker owns its private key. See the executable
[bootstrap CLI](../../conductor/okf/cli-and-configuration.md) for command
syntax and the transport tests for rejection cases.
