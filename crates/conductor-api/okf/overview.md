---
type: Component
title: conductor-api overview
description: The API crate provides an optional bounded HTTP client and optional Axum server; server handlers authenticate operators, call manager operations, and map outcomes to bounded responses.
status: stable
source_revision: 663bd0f
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/server.rs
    title: Routes and request handlers
  - resource: ../src/client.rs
    title: Bounded typed HTTP client
  - resource: ../src/dns.rs
    title: Service DNS implementation
  - resource: ../../conductor/tests/agent.rs
    title: End-to-end HTTP behavior
---

# Responsibility

The API crate provides two opt-in Cargo features. `server` enables the Axum
entrypoint, manager adapter, request validation and bounds, status/error
mapping, readiness, metrics, and service catalog/DNS adapters. `client` enables
a reusable reqwest client with bearer authentication, custom CA trust,
timeouts, typed JSON encode/decode, conditional and idempotency headers, and a
bounded response reader. Defaults enable neither feature. A client-only
consumer does not depend on Axum or conductor-manager.

Server handlers call manager operations rather than querying storage directly.
The executable provides the configured manager and worker ports.

# Listener and modes

The local operator API is bound to loopback. Authenticated operator mode uses a
bearer token read from a protected file; health and readiness endpoints remain
available. The optional service DNS listener is configured separately from the
HTTP listener and has its own allowed-client policy.

# Source evidence

Routes are registered in `src/server.rs`, and DNS record construction is in
`src/dns.rs`. Repository-level tests start the actual agent to exercise HTTP
requests and worker behavior. See the concepts linked from this index for
precise response and limitation details.
