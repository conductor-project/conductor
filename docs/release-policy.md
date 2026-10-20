# Release support and security backports

This policy describes the supported release lines, normal backports, embargoed
security fixes, and emergency publication. Repository protections remain
mandatory: maintainers use reviewed pull requests and required checks; this
policy does not authorize direct pushes or protection bypasses.

## Supported lines

The first stable release, `0.0.1`, was published on 2026-10-19 and starts the
`0.0.x` line. The `release/0.0` maintenance branch was cut from the exact
`v0.0.1` commit, `09f4af5e35b73fc54130c58daea4ae9fe87d7ace`.

Conductor supports the latest minor line and the immediately preceding minor
line. Each minor line receives at least 12 months
of support from its first stable release. If that minimum extends beyond the
period when it is one of the two newest minor lines, support continues through
the minimum period. Support applies to the latest patch in each supported line;
operators should upgrade to that patch. Maintainers announce end of support in
the release notes and README and provide upgrade or mitigation guidance.

| Line | Authorized tags | Branch | Status |
| --- | --- | --- | --- |
| First line | `v0.0.Z` | `main` and `release/0.0` | Supported; `v0.0.1` published; `v0.0.2` is a verified unpublished draft candidate |
| Later minor `X.Y` | `vX.Y.Z` | `main`, then `release/X.Y` when supported as the preceding line | Not yet selected |

The table distinguishes published releases from unpublished draft candidates.
Maintainers update the status and support dates when a release line is opened,
published, or retired.

## Branches, tags, and release authority

The repository-maintainer role owns release lines. A maintenance branch is cut
from the exact commit of a verified stable release after that release has
passed its required checks. Release tags use `vX.Y.Z`; a tag series is
authorized only on `main` for the current line and on its matching
`release/X.Y` branch for a supported maintenance line.

Repository administrators must configure protections before publishing a
release: require pull requests and the same required verification checks on
`main` and `release/*`; restrict branch deletion and history rewrites; protect
`v*` tags against unauthorized creation, deletion, and updates; and restrict
release approval/publication to repository maintainers. The release workflow
must independently verify tag syntax, version agreement, commit ancestry on
the authorized line, and successful checks for the exact tagged commit. The
`Publish release` workflow promotes package files from the exact successful
`Merge` run for the tag commit; it performs no Conductor build. A pushed tag
can only create a draft release. Publication is a separate manual action and
requires the `release-publication` environment to have required reviewers and
an eligible approval. Enable self-review prevention when another eligible
maintainer is available. In a sole-maintainer repository, the configured
maintainer may approve their own manually dispatched release job. Administrators
must configure and verify that environment before using publication. Self-
approval does not provide independent review. The workflow verifies detached
Cosign signatures over the `.deb` and `.rpm` bytes and verifies GitHub artifact
attestations for those same files before promotion. Its release assets include
the package signature bundles, build metadata, checksums, source commit, and
the trusted workflow run identifier. Architecture-specific metadata and
checksum assets avoid filename collisions: `BUILD-INFO.json` and `SHA256SUMS`
are for aarch64/arm64, while x86_64 uses `BUILD-INFO-x86_64.json` and
`SHA256SUMS-x86_64`.
Candidate verification and no-write rehearsal run with read-only repository
contents permission. Draft creation and resumption use a separate job with
release write permission; final publication remains isolated behind the
protected environment. The temporary verified-asset artifact waits for
approval for up to 90 days; if it expires, rerun `prepare` to reverify the
exact trusted `Merge` artifacts and refresh the draft, then request publication
again.
On 2026-10-19, repository administrators configured and verified `Protect
Main` for `refs/heads/main`, `Protect Release Lines` for `refs/heads/release/**`,
and `Protect Release Tags` for `refs/tags/v*`. The branch rulesets require
pull requests and the required CodeQL, debug test, and Linux package checks,
and prevent deletion and non-fast-forward updates. The tag ruleset restricts
tag creation, deletion, and updates to repository administrators. The
`release-publication` environment has a required maintainer reviewer and
allows sole-maintainer self-approval, as recorded in the release evidence.
Administrators must re-verify these settings after any governance change;
workflow checks alone do not protect a tag from being moved or deleted.

## Fixes and backports

Fix ordinary issues on `main` first. For every supported line affected by a
fix, open a focused backport pull request from that line, link the originating
change, and record the target line, applicability, adaptations, exclusions,
and reason for any omitted line. Embargoed security work follows the private
path below and is integrated publicly only when coordinated disclosure is
ready.

Patch releases contain fixes needed by that line. Avoid unrelated features,
dependency churn, and database schema migrations. If a compatibility or schema
change is necessary, assess it against the #134 compatibility policy and
document operator impact before approval. Never claim database downgrade
support. Test upgrades from the previous patch using representative
configuration and persistent state, including manager/worker and backup/
recovery behavior where relevant.

Track each affected line with links to the originating fix, backport PR,
validation evidence, planned fixed version, and publication status. For an
unsupported line, provide a mitigation or upgrade path without promising a
backport.

## Embargoed security fixes

Repository maintainers prepare embargoed fixes in a private repository or
other access-restricted environment. Limit access to the maintainers and
security responders who need it. Use private CI and private artifacts; redact
issue titles, branch names, logs, release notes, and artifact metadata that
could expose the vulnerability. Validate the fix and backports privately
against the same build, test, architecture, and provenance requirements as a
normal release.

Coordinate disclosure with affected parties through GitHub private security
advisories and CVE coordination where appropriate. A repository-maintainer
serves as security coordinator; another repository-maintainer is the backup.
The maintainers record the disclosure plan and publication sequence in the
restricted incident record, then integrate fixes into public release lines
and publish the advisory and fixed versions together when disclosure is
authorized. No public workflow or draft release may reveal embargoed details
before that point.

## Approval and expedited releases

Repository maintainers are the release approvers. The security-coordinator
role and its backup coordinate incident handling and keep the restricted
audit record. An emergency may expedite scheduling and review turnaround, but
not eligibility: required reviews, checks, exact-commit artifact verification,
signing, and protected publication approval remain in force. Maintainers
prioritize active exploitation and operator exposure and act as promptly as
they can; this project makes no numeric response-time commitment. Record what
was expedited and why.

## Artifacts, advisories, and immutable releases

Release publication promotes only packages produced and verified for the
exact tag commit by the trusted release workflow. Publish packages, checksums,
signatures, build/source provenance, and release notes covering compatibility,
upgrade steps, known limitations, affected/fixed versions, and mitigations.
Do not rebuild packages inside the promotion job. The signing and
trust-bootstrap mechanism is documented in the package installation guide and
is included with each published package.

Publication is resumable only when an existing asset matches the expected
bytes. A conflicting asset or tag fails; published tags and assets are never
rewritten. Correct a faulty patch with a new version and an advisory update.
The release workflow's `rehearse` dispatch validates the existing tag, branch,
exact-run artifacts, package metadata, checksums, signatures, and attestations
without creating or changing a release. `prepare` creates or resumes a draft;
`approval-rehearsal` sends the candidate through the protected publication
environment and confirms approval without publishing; `publish` publishes the
draft only after that environment approval.

Run a rehearsal and prepare a draft with:

```sh
gh workflow run publish-release.yml --ref main --field tag=v0.0.1 --field action=rehearse
gh workflow run publish-release.yml --ref main --field tag=v0.0.1 --field action=prepare
gh workflow run publish-release.yml --ref main --field tag=v0.0.1 --field action=approval-rehearsal
gh workflow run publish-release.yml --ref main --field tag=v0.0.1 --field action=publish
```

Inspect the draft and artifacts before dispatching `action=publish`; the
protected environment still requires an eligible approval. Repeat the same
sequence for a supported maintenance-line tag after its release branch exists.
Post-merge run artifacts are retained for up to 90 days; the prepared draft
release is the durable candidate. If an artifact has expired, the promotion
workflow fails with the trusted run ID. Recover by rerunning that exact
successful `Merge` run (`gh run rerun RUN_ID`), waiting for the same source
commit's release checks and packages to pass, then dispatching `action=prepare`
again. Promotion still consumes the refreshed run artifacts and never builds
Conductor. This preserves the source commit identity while regenerating the
temporary candidate files.

## Required rehearsals and records

The first current-line release, `v0.0.1`, passed the exact-run verification and
protected publication flow. Its evidence and the maintenance-line rehearsal
are recorded in [`okf/release-and-support-policy.md`](../okf/release-and-support-policy.md).
The `v0.0.2` maintenance candidate passed exact-source release checks and a
protected approval rehearsal; it remains an unpublished draft. Record the
originating fix, adaptations, exact commits, checks, package architectures,
compatibility/upgrade results, and publication status for each maintenance
release. Run an embargo rehearsal
that tests private access, log and artifact redaction, coordinated public
integration, and advisory publication without exposing a real vulnerability.
Run a faulty-release exercise that issues a new patch version while preserving
the earlier bytes. Keep rehearsal artifacts clearly identified as
non-production and do not create a misleading public release.
