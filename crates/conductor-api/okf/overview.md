---
type: Component
title: conductor-api overview
description: Axum handlers validate HTTP input, authenticate operators, call manager operations, and map outcomes to bounded responses.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/lib.rs
    title: Routes and request handlers
  - resource: ../src/dns.rs
    title: Service DNS implementation
  - resource: ../../conductor/tests/agent.rs
    title: End-to-end HTTP behavior
---

# Responsibility

The API crate owns HTTP routes, auth policy, input validation, request bounds,
status/error mapping, readiness, metrics, and service catalog/DNS adapters. It
calls manager operations rather than querying storage directly. The executable
provides the configured manager and worker ports.

# Listener and modes

The local operator API is bound to loopback. Authenticated operator mode uses a
bearer token read from a protected file; health and readiness endpoints remain
available. The optional service DNS listener is configured separately from the
HTTP listener and has its own allowed-client policy.

# Source evidence

Routes are registered in `src/lib.rs`, and DNS record construction is in
`src/dns.rs`. Repository-level tests start the actual agent to exercise HTTP
requests and worker behavior. See the concepts linked from this index for
precise response and limitation details.
