# Release support and security backports

This policy describes the supported release lines, normal backports, embargoed
security fixes, and emergency publication. Repository protections remain
mandatory: maintainers use reviewed pull requests and required checks; this
policy does not authorize direct pushes or protection bypasses.

## Supported lines

The first intended stable release is `0.0.1`, the start of the `0.0.x`
line. The repository currently
has no published release tags or maintenance branches, so no version is
supported until its verified release is published.

After releases begin, Conductor supports the latest minor line and the
immediately preceding minor line. Each minor line receives at least 12 months
of support from its first stable release. If that minimum extends beyond the
period when it is one of the two newest minor lines, support continues through
the minimum period. Support applies to the latest patch in each supported line;
operators should upgrade to that patch. Maintainers announce end of support in
the release notes and README and provide upgrade or mitigation guidance.

| Line | Authorized tags | Branch | Status |
| --- | --- | --- | --- |
| First line | `v0.0.Z` | `main`, then `release/0.0` when cut | Planned; not yet published |
| Later minor `X.Y` | `vX.Y.Z` | `main`, then `release/X.Y` when supported as the preceding line | Not yet selected |

The table records policy, not evidence that a release or branch exists.
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
self-review prevention configured. Administrators must configure and verify
that environment before using publication.
Configuration of GitHub rulesets is an administrator action and must be
verified in repository settings; workflow checks alone do not protect a tag
from being moved or deleted.

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
Do not rebuild packages inside the promotion job. The approved signing and
trust-bootstrap mechanism must be documented before the first production
release.

Publication is resumable only when an existing asset matches the expected
bytes. A conflicting asset or tag fails; published tags and assets are never
rewritten. Correct a faulty patch with a new version and an advisory update.
The release workflow's `rehearse` dispatch validates the existing tag, branch,
exact-run artifacts, package metadata, checksums, signatures, and attestations
without creating or changing a release. `prepare` creates or resumes a draft;
`publish` publishes that draft only after the configured environment approval.

## Required rehearsals and records

Before production publication, rehearse a current-line release and a supported
maintenance-line backport through the same verification and artifact gates.
Record the originating fix, adaptations, exact commits, checks, package
architectures, compatibility/upgrade results, and publication status. Run an
embargo rehearsal that tests private access, log and artifact redaction,
coordinated public integration, and advisory publication without exposing a
real vulnerability. Run a faulty-release exercise that issues a new patch
version while preserving the earlier bytes. Keep rehearsal artifacts clearly
identified as non-production and do not create a misleading public release.
