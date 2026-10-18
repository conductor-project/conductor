---
type: Reference
title: Documentation migration map
description: Every former documentation page and long README reference section maps to canonical repository or crate knowledge pages.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../docs
    title: Previous documentation pages, retained as compatibility links
  - resource: ../README.md
    title: Previous README reference sections
  - resource: ../crates/conductor-api/src/lib.rs
    title: Current API routes
  - resource: ../crates/conductor-manager/src/lib.rs
    title: Current manager schema
  - resource: ../crates/conductor-worker/src/lib.rs
    title: Current worker schema and inventory
---

# Former `docs/*.md` pages

Each former page now contains a short compatibility pointer. The linked bundle
page is canonical; do not restore independent prose contracts at the old path.

| Former page | Canonical destination | Compatibility page |
| --- | --- | --- |
| [`metrics-and-audit.md`](../docs/metrics-and-audit.md) | [API auth, bounds, metrics and audit](../crates/conductor-api/okf/authentication-and-bounds.md); [manager audit ownership](../crates/conductor-manager/okf/scheduling-and-services.md) | Retained |
| [`mvp2-command-outbox.md`](../docs/mvp2-command-outbox.md) | [Manager transactions and outbox bounds](../crates/conductor-manager/okf/desired-state-and-persistence.md); [worker command journal](../crates/conductor-worker/okf/supervision-and-recovery.md); [transport replay](../crates/conductor-transport/okf/commands-reports-and-reconnect.md) | Retained |
| [`mvp2-command-semantics.md`](../docs/mvp2-command-semantics.md) | [Core identities and states](../crates/conductor-core/okf/execution-resources-and-protocol.md); [source-backed command/report delivery](../crates/conductor-transport/okf/commands-reports-and-reconnect.md) | Retained |
| [`mvp2-disconnect-policies.md`](../docs/mvp2-disconnect-policies.md) | [Worker freshness and replay](../crates/conductor-transport/okf/commands-reports-and-reconnect.md); [uncertain manager allocations](../crates/conductor-manager/okf/scheduling-and-services.md); [operator resolution](../crates/conductor-api/okf/routes-and-contracts.md#resolving-an-uncertain-run) | Retained; policy claims audited against current implementation |
| [`mvp2-execution-model.md`](../docs/mvp2-execution-model.md) | [Core execution model](../crates/conductor-core/okf/execution-resources-and-protocol.md); [manager scheduling/services](../crates/conductor-manager/okf/scheduling-and-services.md); [worker readiness](../crates/conductor-worker/okf/deadlines-readiness-and-logs.md); [process enforcement](../crates/conductor-process/okf/cgroups-and-landlock.md) | Retained |
| [`mvp2-multi-node-qualification.md`](../docs/mvp2-multi-node-qualification.md) | [Current platform evidence, retained debug/direct harness invocation, and qualification limits](platforms-and-evidence.md); [same-host multiprocess test](../crates/conductor/tests/macos_multiprocess.rs) | Retained; direct mode is not host-resolver or independent-host evidence |
| [`mvp2-protocol-and-schema-versions.md`](../docs/mvp2-protocol-and-schema-versions.md) | [Protocol v1](../crates/conductor-transport/okf/commands-reports-and-reconnect.md); [manager schema 12 and backup](../crates/conductor-manager/okf/desired-state-and-persistence.md); [worker schema 4](../crates/conductor-worker/okf/supervision-and-recovery.md) | Retained; legacy 5/3 schema numbers were stale |
| [`mvp2-submission-contract.md`](../docs/mvp2-submission-contract.md) | [API methods, bodies, preconditions, and errors](../crates/conductor-api/okf/routes-and-contracts.md); [manager state](../crates/conductor-manager/okf/desired-state-and-persistence.md); [core request models](../crates/conductor-core/okf/identifiers-and-contracts.md) | Retained |
| [`operator-access.md`](../docs/operator-access.md) | [API token setup, complete config, header, checks, and rotation](../crates/conductor-api/okf/authentication-and-bounds.md); [self-hosted proxy boundary](self-hosting.md) | Retained |
| [`operator-cli.md`](../docs/operator-cli.md) | [conductor CLI](../crates/conductor/okf/cli-and-configuration.md); [operator examples](../crates/conductor/okf/operator-commands.md); [API routes](../crates/conductor-api/okf/routes-and-contracts.md) | Retained |
| [`platform-support.md`](../docs/platform-support.md) | [Platform and evidence](platforms-and-evidence.md); [process platform boundary](../crates/conductor-process/okf/platform-boundaries.md); [worker inventory](../crates/conductor-worker/okf/deadlines-readiness-and-logs.md) | Retained |
| [`service-discovery.md`](../docs/service-discovery.md) | [Host resolver and DNS setup](service-discovery.md); [API discovery and DNS](../crates/conductor-api/okf/service-discovery.md); [manager service catalog](../crates/conductor-manager/okf/scheduling-and-services.md) | Retained |
| [`standalone-roles.md`](../docs/standalone-roles.md) | [Role configuration](../crates/conductor/okf/cli-and-configuration.md); [composition/enrollment](../crates/conductor/okf/composition-and-lifecycle.md); [transport identity](../crates/conductor-transport/okf/enrollment-and-identity.md) | Retained |
| [`worker-draining.md`](../docs/worker-draining.md) | [Manager drain lifecycle](../crates/conductor-manager/okf/scheduling-and-services.md); [API body, progress, blockers, and clear behavior](../crates/conductor-api/okf/routes-and-contracts.md); [CLI operations](../crates/conductor/okf/operator-commands.md) | Retained |
| [`worker-identity.md`](../docs/worker-identity.md) | [Transport enrollment and identity](../crates/conductor-transport/okf/enrollment-and-identity.md); [issue/revoke and worker provisioning commands](../crates/conductor/okf/composition-and-lifecycle.md) | Retained |

# Former README reference sections

The README is now an onboarding path. Detailed material from the former
sections maps as follows:

| Former section | Canonical destination |
| --- | --- |
| Requirements and platform matrix | [Platforms and evidence](platforms-and-evidence.md) |
| Build, lint, tests, and release profile | [Contributing policy](contributing.md) and [`AGENTS.md`](../AGENTS.md) |
| Agent roles and configuration | [Self-hosting](self-hosting.md), [configuration](configuration.md), and [CLI/composition](../crates/conductor/okf/index.md) |
| Reproducible lifecycle demo | [Operations/recovery](operations-and-recovery.md) and its source-linked lifecycle checks; the README's first-run path is separately exercised on the recorded debug build |
| Job definition, run API, pagination, idempotency, cancellation, and recovery | [Jobs and services](jobs-and-services.md) and [API contracts](../crates/conductor-api/okf/routes-and-contracts.md) |
| Service revisions and deployment | [Jobs and services](jobs-and-services.md), [core execution model](../crates/conductor-core/okf/execution-resources-and-protocol.md), [manager services](../crates/conductor-manager/okf/scheduling-and-services.md) |
| Request bounds, authentication, metrics, and audit event details | [API authentication and bounds](../crates/conductor-api/okf/authentication-and-bounds.md) |
| Cgroup v2 and Landlock | [Process enforcement](../crates/conductor-process/okf/cgroups-and-landlock.md) and [configuration](configuration.md) |
| systemd operation | [Self-hosting](self-hosting.md) |
| Historical local probe and service qualification harness | [Platforms and evidence](platforms-and-evidence.md), preserving reported benchmark values and the debug/direct harness command while stating unavailable provenance and scope; local release run commands removed |
| Architecture and limitations | [Architecture](architecture.md) and [platform evidence](platforms-and-evidence.md) |
| Contribution | [Contributing](contributing.md) and authoritative [`AGENTS.md`](../AGENTS.md) |

# Factual reconciliation performed

- Manager schema version is 12; worker schema version is 4 in the reviewed
  source. The prior version 5/3 documentation is replaced by those references.
- Host inventory and platform statements are scoped to current code and CI;
  Linux arm64 and Intel macOS are not represented as qualified.
- The CLI has no job-submit command. One-shot submission uses HTTP; the README
  does not invent an authenticated CLI flow.
- `GET /readyz` is readiness, not proof of available capacity. Run admission
  has its own capacity checks.
- Deleted job IDs remain reserved. The README uses fresh per-invocation IDs
  and does not depend on deleting and re-registering the same ID.
- Issues #77 and #78 were cancelled; no routed independent-host qualification
  claim is made. Issue #118's SRV target/address pairing fix is implemented and
  covered by debug tests; the lack of routed independent-host qualification
  remains explicit in the service discovery and platform evidence pages.
- Release builds and tests are post-merge CI only except for reproducing a
  specific CI issue. The local onboarding walkthrough uses debug build output.

## Superseded and intentionally excluded claims

- The previous manager/worker schema numbers (5 and 3) were stale at the
  reviewed source revision; current source declares manager 12 and worker 4.
- Older execution/disconnect prose included desired policies such as
  duplicate-tolerance choices and report-gap buffering. Those are not stated as
  shipped behavior unless present in current source; the current manager keeps
  uncertain allocations reserved and does not automatically replay them.
- Old platform pages linked historical workflow runs and described qualification
  against older matrices. Current support wording follows the checked-in PR and
  post-merge workflows, and records local benchmark numbers as unverified
  historical evidence with the missing provenance called out.
- The service harness's direct DNS mode is retained as a debug command, but its
  host-resolver mode and proposed routed multi-host scenario do not establish
  present qualification. Issues #77 and #78 were cancelled.

## Maintenance

This map and the concepts were source-reviewed at `a739b4c20dd8e81ded656dfa7bb9859f48d73f9e`.
That records the review baseline only; it is not a claim that every topic was
runtime tested. The current README walkthrough is validated separately on the
host and commit recorded in the change review. Update this map when adding,
removing, or relocating legacy documentation.
