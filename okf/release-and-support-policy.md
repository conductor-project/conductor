---
type: Reference
title: Release and support policy
description: Supported line policy and release/security procedures, distinguished from configured GitHub protections and completed release evidence.
status: planned
source_revision: 848190fed82842c54506dad31a6b6a5e49ce0214
source_reviewed_at: 2026-10-19
sources:
  - resource: ../docs/release-policy.md
    title: Release support and security backport policy
  - resource: ../.github/workflows/pr.yml
    title: Pull request checks
  - resource: ../.github/workflows/merge.yml
    title: Post-merge release checks
  - resource: ../.github/workflows/publish-release.yml
    title: Tagged release promotion and publication gates
  - resource: ../scripts/resolve-release-ref.py
    title: Release tag and authorized-line resolver
  - resource: ../scripts/test-resolve-release-ref.py
    title: Unprivileged release-reference fixtures
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
approval rehearsal with read-only repository contents permission; draft writes
are isolated in a separate job and final publication remains behind the
protected environment. It fails with an exact-run recovery command when
transient artifacts are missing or expired. The intermediate verified-asset
artifact is retained for 90 days while approval is pending; its expiration
guidance revalidates the exact trusted run and refreshes the existing draft.
No version is supported until its verified release is published.

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
metadata and checksums while preserving the existing aarch64 asset bytes.
PR #174's PR workflow run `39247026626` passed CodeQL, debug checks on Linux
x86_64, macOS, and Linux arm64, and both Linux package jobs.

On 2026-10-19, this draft-preparation command ran as GitHub Actions run
[`39250145368`](https://github.com/conductor-project/conductor/actions/runs/39250145368)
from main revision `d345c8f3009fed2e922ef70c62f504b83c776940`. It selected the
successful `Merge` run `39243996603` for the exact tag commit and passed
metadata, checksum, Cosign signature, and GitHub attestation verification for
both architectures. It resumed the existing [draft release](https://github.com/conductor-project/conductor/releases/tag/untagged-f1328f563f1c0633652d).
The draft now has 14 release assets: Debian and RPM packages for x86_64 and
arm64, their four Sigstore bundles, architecture-specific build metadata and
checksum files, source commit, and release provenance. GitHub displays 16
assets total including its two generated source archives. Existing aarch64
asset bytes were reused; the x86_64 names are `BUILD-INFO-x86_64.json` and
`SHA256SUMS-x86_64`.

```sh
gh workflow run publish-release.yml --ref main --field tag=v0.0.1 --field action=prepare
```

The no-write rehearsal used this command and completed successfully as run
[`39250467897`](https://github.com/conductor-project/conductor/actions/runs/39250467897).
It revalidated the exact-run packages, checksums, signatures, and attestations;
the release-writing and protected-publication jobs were skipped. The draft is
still unpublished.

```sh
gh workflow run publish-release.yml --ref main --field tag=v0.0.1 --field action=rehearse
```

After PRs #176 and #177 merged, the no-write rehearsal was repeated against
the merged workflow at `ae824f0a6ed7242e589b340243d46897c3b67877`. GitHub Actions
run [39255361441](https://github.com/conductor-project/conductor/actions/runs/39255361441)
completed successfully. It selected the successful `Merge` run `39243996603`
for tag commit `09f4af5e35b73fc54130c58daea4ae9fe87d7ace`, verified both
architecture package sets, checksums, Cosign signatures, and attestations, and
logged `Candidate verification succeeded; no release or tag was created or
changed.` The protected publication job was skipped. A post-run inspection
confirmed that `v0.0.1` remains a draft with no publication timestamp.

After PR #179 merged, the no-write rehearsal ran against release-workflow
revision `372ba25f7dc941ab779fe792055e0c607e029e4c` on the
`codex/issue-147-rehearsal-evidence` branch as run
[`39259649886`](https://github.com/conductor-project/conductor/actions/runs/39259649886).
It passed the tag resolver, exact-run artifact selection, and package,
checksum, Cosign signature, and attestation verification for both architectures.
This revision checks out the default branch's verifier scripts for tag pushes
and the selected workflow revision for manual dispatches, keeping candidate
source separate from the validation code. Draft preparation and publication
steps were skipped; `v0.0.1` remained a draft.

After PRs #180 and #181 merged, the no-write rehearsal ran against main
revision `f65d8adf7bace42a0ff888c2374e84d05331f11d` as run
[`39268211011`](https://github.com/conductor-project/conductor/actions/runs/39268211011).
It selected the successful `Merge` run for the exact `v0.0.1` tag commit,
downloaded both architecture artifacts, and verified package metadata,
checksums, Cosign signatures, and attestations. The release-asset staging step
and both release-writing jobs were skipped. A post-run inspection confirmed
that `v0.0.1` is still a draft with no publication timestamp. This run verifies
the merged permission split and no-write path; it does not test the protected
approval gate or maintenance-line promotion.

The release-reference resolver is covered by
`python3 scripts/test-resolve-release-ref.py` (5 tests passed). Temporary local
Git fixtures exercise current-line tags, maintenance-line tags, shared commits,
invalid tag syntax, version mismatch, missing tags, and unauthorized branches.
These fixtures do not install packages or require privileged access.

After PR #184 merged, the protected no-write approval rehearsal ran on main
revision `848190fed82842c54506dad31a6b6a5e49ce0214` as run
[`39276031693`](https://github.com/conductor-project/conductor/actions/runs/39276031693).
It selected the successful exact-commit `Merge` run, verified both package
architectures, checksums, Cosign signatures, and GitHub attestations, and staged
the verified assets. The configured `release-publication` environment had one
required reviewer and allowed self-review under the sole-maintainer policy; the
maintainer approved the rehearsal. The environment check and no-publish
rehearsal step passed, while the draft-creation and publish steps were skipped.
A post-run check confirmed `v0.0.1` remains a draft with no publication
timestamp. This validates candidate verification and the approval gate, but
does not qualify maintenance-line promotion or publish a supported release.

The checked-in pull request workflow runs debug checks; the post-merge workflow
runs release tests and builds on Linux x86_64 and macOS and packages Linux
x86_64 and arm64. The release workflow consumes those signed, attested
artifacts. No maintenance-line release or protected approval rehearsal has
been recorded, and no version is supported until its verified release is
published.

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
