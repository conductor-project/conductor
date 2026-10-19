---
type: Reference
title: Platform support and evidence
description: Platform claims are scoped to source, tests, workflows, and the exact execution environment that produced evidence.
status: stable
source_revision: 79594cf7977e9703bc6876ce5bbd7719edfb4b3e
source_reviewed_at: 2026-10-19
sources:
  - resource: ../.github/workflows/pr.yml
    title: Pull request debug workflow
  - resource: ../.github/workflows/merge.yml
    title: Post-merge release workflow
  - resource: ../packaging/rpm/conductor.spec
    title: RPM payload construction and lifecycle hooks
  - resource: ../packaging/debian/postinst
    title: Debian package post-install behavior
  - resource: ../packaging/debian/prerm
    title: Debian package pre-removal behavior
  - resource: ../packaging/debian/postrm
    title: Debian package post-removal behavior
  - resource: ../crates/conductor-worker/src/lib.rs
    title: Host inventory collection
  - resource: ../crates/conductor/tests/macos_multiprocess.rs
    title: Native multi-process integration test
  - resource: ../docs/mvp2-multi-node-qualification.md
    title: Historical qualification record, not independent-host qualification
  - resource: ../scripts/qualify-mvp2-services.py
    title: Retained debug/direct service qualification harness
  - resource: ../README.md
    title: Historical benchmark values transcribed from the reviewed predecessor
---

# Current repository evidence

| Target | CI status | Scope and limit |
| --- | --- | --- |
| Linux x86_64 | PR and post-merge jobs | Debug/release builds and tests, plus package construction and binary-identity checks on Ubuntu 26.04. Distribution installation/runtime qualification is separate. |
| Linux arm64 | PR and post-merge jobs | Native debug/release builds and tests, plus package construction and binary-identity checks on Ubuntu 26.04 arm64. Distribution installation/runtime qualification is separate. |
| Apple silicon macOS | PR and post-merge jobs | Debug and release builds/tests on the current macOS runner; Intel macOS is explicitly skipped. |
| Intel macOS | Not in current matrix | Do not infer support from Apple silicon results. |

The PR workflow runs CodeQL, debug checks, and non-published debug packages.
The merge workflow runs CodeQL, release tests/builds, and signed/attested
package artifacts on trusted pushes to `main` and `release/*`. Package checks
compare the binary extracted from each package to the matching workflow
artifact. A passing CI job establishes the specific checks it ran on that
runner and commit; it is not universal hardware, kernel, filesystem, network,
or target-distribution installation evidence. See the [package installation
guide](../docs/package-install.md) for target distributions and operator steps.

The configuration accepts `worker_concurrency` from 1 through 64. This is an
admission ceiling per worker, not a measured 64-run capacity qualification or
a throughput guarantee. The API's separate bounded concurrent start-operation
limit is 32; it is not the worker's execution capacity.

# Host inventory and enforcement

Worker inventory collects operating system and architecture for Linux and
macOS builds on x86_64 and aarch64. It reports process and enforcement
capabilities and available capacity used for scheduling. Inventory is not
utilization telemetry, measured availability over time, or kernel enforcement.
The manager's configured resource domains are scheduling budgets; they are not
host-wide quota controls.

Native process execution uses the daemon user's permissions. Linux cgroup v2
applies configured resource controls and descendant cleanup when correctly
delegated. Landlock can restrict selected filesystem operations under kernel
support and configured grants. Neither mechanism isolates networking or every
same-user interaction. macOS does not apply Linux cgroup or Landlock controls.
The OS loader decides whether a configured executable format can run; Conductor
does not pre-inspect ELF or Mach-O headers.

# Qualification status

The local multi-process tests exercise independent daemon processes on one host
with distinct state directories/listeners. This demonstrates local process and
transport behavior only. Issue #117 was closed before this source revision; its
rollout-unavailable accounting correction is included in the reviewed source.
Issues #77 and #78, which proposed routed multi-host qualification, were
cancelled; no routed deployment-owned Linux/worker/macOS testbed evidence is
claimed here. Issue #118's DNS record association fix is implemented: focused
debug tests verify that each SRV target resolves only to its advertised
address/port tuple, while service-wide A/AAAA remains aggregated. The tests and
full workspace debug suite passed on the current macOS host; this is source and
local-test evidence, not routed multi-host qualification, host resolver
validation, or deployment reachability evidence. Issues #77 and #78 were
cancelled, and no routed deployment-owned testbed evidence is available.

Historical benchmark or qualification reports must retain their exact commit,
OS, architecture, build profile, runner, workload, and command. Such reports
are evidence for that run only; they are not current workflow guarantees or
portable performance claims.

## Historical macOS probe record

The README at source revision `f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a`
reported one probe on macOS 27.0.1 arm64 (8 logical CPUs), Rust 1.99.1,
100 iterations, and the repository release profile (optimization level 3,
fat LTO, one codegen unit, overflow checks and debug assertions enabled). It
reported RSS from 9,328 to 10,016 KiB and p50/p95/p99 latencies:

| Operation | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| `/healthz` | 0.224 ms | 0.257 ms | 0.619 ms |
| Worker inspection | 0.247 ms | 0.294 ms | 0.326 ms |
| Run start | 1.120 ms | 1.451 ms | 1.731 ms |
| Terminal-run inspection | 0.242 ms | 0.283 ms | 0.308 ms |

This is a historical, self-reported note, not a result re-measured for this
change. The source note did not include the probe date, binary hash, exact
command, or raw output, so those provenance fields are unavailable. It was
explicitly described as a macOS development-host measurement and does not
qualify Linux deployment performance or support a current capacity claim.

## Historical debug/direct qualification harness

The repository retains `scripts/qualify-mvp2-services.py`. Its documented
one-host scenario starts one standalone manager and two separately enrolled
workers, each with separate state and identities, and checks service
placement, cross-service traffic, DNS records, replacement, drain, and
reconnect behaviors. The direct resolver mode sends explicit FQDN, search,
and SRV queries to a temporary loopback listener; it does not change or test
the host's resolver configuration. The old invocation was:

```sh
cargo +1.99.1 build --locked -p conductor
python3 scripts/qualify-mvp2-services.py \
  --binary target/debug/conductor \
  --profile debug \
  --resolver-mode direct
```

This invocation is recorded for reproducibility and describes the harness's
debug/direct mode, not a result for this change. It was not run as part of this
documentation work. Issues #77 and #78 were cancelled, no routed
independent-host testbed is available, and the harness is not run by the
current CI workflows. Do not infer host-resolver integration or independent
host qualification from direct mode.
