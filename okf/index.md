---
okf_version: "0.2"
---

# Repository knowledge bundle

This bundle contains Conductor's cross-crate, user-facing, and operational
knowledge. Technical details owned by one crate live in that crate's
independent bundle; links to those pages leave this bundle and require the
surrounding repository checkout.

## Deployment and operation

- [Self-hosting](self-hosting.md) - start, operate, and secure deployments.
- [Configuration](configuration.md) - JSON configuration, paths, roles, and credentials.
- [Jobs and services](jobs-and-services.md) - HTTP submission, run lifecycle, and service deployments.
- [Service discovery](service-discovery.md) - DNS listener access, host resolver setup, and rollback.
- [Operations and recovery](operations-and-recovery.md) - readiness, shutdown, persistence, diagnosis, and recovery.
- [Package installation and recovery acceptance](package-lifecycle-acceptance.md) - planned local package lifecycle scenarios and evidence requirements.
- [Platforms and evidence](platforms-and-evidence.md) - supported targets, enforcement boundaries, and qualification evidence.
- [Release and support policy](release-and-support-policy.md) - supported release lines, backports, security handling, and publication rules.
- [Linux package installation](../docs/package-install.md) - package verification, first configuration, service start, upgrade, and removal.
- [README walkthrough evidence](walkthrough-evidence.md) - recorded local debug-only first-job and cancellation run.

## Project structure

- [Architecture](architecture.md) - crate boundaries and manager/worker ownership.
- [Overlay networking architecture](overlay-networking-architecture.md) - accepted MVP5 Iroh peer transport, IP addressing, lease, and privilege contracts.
- [Contributing](contributing.md) - local workflow and policy links.
- [Documentation migration map](migration-map.md) - old docs and README sections to canonical pages.

## Independent crate bundles

These links leave the repository bundle and require the containing repository:

- [conductor](../crates/conductor/okf/index.md)
- [conductor-core](../crates/conductor-core/okf/index.md)
- [conductor-manager](../crates/conductor-manager/okf/index.md)
- [conductor-worker](../crates/conductor-worker/okf/index.md)
- [conductor-process](../crates/conductor-process/okf/index.md)
- [conductor-api](../crates/conductor-api/okf/index.md)
- [conductor-transport](../crates/conductor-transport/okf/index.md)

Every concept states its reviewed source revision and links to implementation,
tests, examples, or historical evidence as applicable. `source_reviewed_at`
means source and documentation were compared; it does not by itself claim a
runtime test or platform qualification. Check the individual evidence page
before relying on a runtime claim.
