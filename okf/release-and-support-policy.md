---
type: Reference
title: Release and support policy
description: Supported line policy and release/security procedures, distinguished from configured GitHub protections and completed release evidence.
status: planned
source_revision: 949131c83b86c9492b817d4a364a05d5a90495b0
source_reviewed_at: 2026-10-19
sources:
  - resource: ../docs/release-policy.md
    title: Release support and security backport policy
  - resource: ../.github/workflows/pr.yml
    title: Pull request checks
  - resource: ../.github/workflows/merge.yml
    title: Post-merge release checks
  - resource: ../AGENTS.md
    title: Repository contribution and protection requirements
---

# Policy and current evidence

[`docs/release-policy.md`](../docs/release-policy.md) defines `0.0.1` as the
first intended release on the `0.0.x` line, the current and immediately
preceding minor support window with a 12-month minimum, maintainer ownership,
backport and embargo procedures, and immutable publication. The release
workflow verifies and promotes packages from the exact successful `Merge` run
without rebuilding; tag pushes create drafts, while publishing requires the
configured `release-publication` environment. The workflow includes a no-write
approval rehearsal and fails with an exact-run recovery command when transient
artifacts are missing or expired. No version is supported until its verified
release is published.

Tag `v0.0.1` points to main commit `09f4af5e35b73fc54130c58daea4ae9fe87d7ace`.
Its exact [`Merge` run](https://github.com/conductor-project/conductor/actions/runs/39243996603)
passed CodeQL, release tests, and package jobs; both architecture-specific
package artifacts were present and unexpired. The tag-triggered
[`Publish release` run](https://github.com/conductor-project/conductor/actions/runs/39246314442)
verified the packages,
checksums, signatures, and attestations, then failed while resuming the draft:
both architecture artifacts contained `BUILD-INFO.json` and `SHA256SUMS`.
It left a [draft release](https://github.com/conductor-project/conductor/releases/tag/untagged-f1328f563f1c0633652d)
tagged `v0.0.1` with the complete aarch64 asset set and no x86_64 metadata;
nothing was published. The follow-up change assigns unique names to x86_64
metadata and checksums while preserving the existing aarch64 asset bytes. Its
CI and a successful `prepare` rerun are still pending.

The checked-in pull request workflow runs debug checks; the post-merge workflow
runs release tests and builds on Linux x86_64 and macOS and packages Linux
x86_64 and arm64. The release workflow consumes those signed, attested
artifacts. No maintenance-line release or publication rehearsal has been
recorded, and no version is supported until its verified release is published.

GitHub repository settings were inspected on 2026-10-19. The active `Protect
Main` ruleset applies to `refs/heads/main` and only contains deletion and
non-fast-forward restrictions. It does not require pull requests or CI checks.
The classic branch-protection endpoint reports that `main` is not protected,
and no tag-targeting ruleset is configured. These settings do not satisfy the
release-policy protection requirements; repository administrators must
configure and re-verify pull request, required-check, maintenance-branch, and
tag protections before publishing releases. This observation records current
settings, not a claim that the settings cannot change.

Release-line, backport, embargo, emergency-approval, upgrade, artifact, and
rehearsal procedures are policy requirements. A policy statement is not
evidence that a release was published or a rehearsal was run.
