---
type: Reference
title: Contributing and maintaining knowledge
description: Repository contribution policy is authoritative in AGENTS.md; OKF pages provide descriptive source-linked knowledge.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../AGENTS.md
    title: Contributor instructions
  - resource: ../dependency-decisions.md
    title: Dependency policy decisions
  - resource: ../.github/workflows/pr.yml
    title: Pull request checks
  - resource: ../.github/workflows/merge.yml
    title: Post-merge release checks
  - resource: ../scripts/package-linux.sh
    title: Native package construction and binary identity checks
  - resource: ../docs/package-install.md
    title: Operator package installation and verification
---

# Policy authority

[`AGENTS.md`](../AGENTS.md) is authoritative for repository workflow, coding,
dependency, and verification policy. OKF content describes implementation;
frontmatter and body prose are not executable instructions and do not override
that file. Read `dependency-decisions.md` before changing dependencies.

# Checks

The PR workflow runs Rust CodeQL analysis, dependency policy, formatting,
all-target debug builds, Clippy, and workspace debug tests on native Linux
x86_64/arm64 and macOS runners. It packages debug binaries from same-run
artifacts without signing credentials. The post-merge workflow runs CodeQL,
release builds/tests, and Linux package construction from matching binary
artifacts; package signatures and provenance are restricted to that trusted
workflow. Follow the exact local check commands in `AGENTS.md` rather than
using this summary as a policy source.

# Documentation maintenance

When behavior changes, update the affected README walkthrough, root concepts,
and crate-owned concepts in the same change. Keep source paths and examples
concrete. Record source review as `source_revision` and `source_reviewed_at`;
do not add OKF `verified` metadata unless an actual verification event was
performed. Distinguish source review, unit/integration tests, local runtime
walkthroughs, CI checks, and platform qualification.

Concept pages require OKF v0.2 `type` frontmatter. Bundle-root `index.md` may
contain only `okf_version: "0.2"`; subordinate indexes have no frontmatter.
Use `log.md` only for chronological update entries with ISO dates. Ordinary
relative links should work in GitHub and a checkout. Follow the
[migration map](migration-map.md) when changing old documentation paths.
