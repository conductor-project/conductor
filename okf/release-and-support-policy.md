---
type: Reference
title: Release and support policy
description: Supported line policy and release/security procedures, distinguished from configured GitHub protections and completed release evidence.
status: planned
source_revision: e7e17917291459f16a2d9a53fcd03a7f29ae327c
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

[`docs/release-policy.md`](../docs/release-policy.md) defines the intended
first `0.1.x` line, the current and immediately preceding minor support window
with a 12-month minimum, maintainer ownership, backport and embargo procedures,
and immutable publication. No version is supported until its verified release
is published.

The reviewed repository revision has no release tags or maintenance branches.
The checked-in pull request workflow runs debug checks; the post-merge workflow
runs release tests and builds on Linux x86_64 and macOS. Neither workflow
currently produces packages or proves maintenance-line coverage. GitHub
branch/tag rulesets are administrator configuration and are not established by
these documents. See the issue/PR evidence before claiming they are configured
or rehearsed.

Release-line, backport, embargo, emergency-approval, upgrade, artifact, and
rehearsal procedures are policy requirements. A policy statement is not
evidence that a release was published, a rehearsal was run, or a repository
ruleset was enabled.
