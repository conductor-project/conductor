---
type: Qualification plan
title: Package installation and recovery acceptance
description: Planned local lifecycle qualification for the selected Debian and RPM package targets.
status: planned
source_revision: 86124327e9ac93a6efddcf6583d9e30b95f8ade0
source_reviewed_at: 2026-10-19
sources:
  - resource: ../scripts/demo-agent.py
    title: Existing isolated real-binary lifecycle fixture
  - resource: operations-and-recovery.md
    title: Supported backup, restore, and unknown-outcome behavior
  - resource: self-hosting.md
    title: Service roles, systemd behavior, and current evidence limits
---

# Scope and evidence boundary

This page records the fixture design for issue #137. It is a plan, not a
qualification report. No package lifecycle result is claimed here. Record each
run's source revision, package and binary SHA-256 values, distribution release,
architecture, runtime image digest, exact commands, observed results, and
limitations in a separate evidence record.

The current local qualification target is arm64/aarch64 on Debian 13 and Rocky
Linux 10, using the `.deb` and `.rpm` produced for the same source revision.
This reflects the available Apple silicon host. It does not establish x86_64
package runtime support. CI may build and inspect x86_64 artifacts; privileged
package installation, systemd, and lifecycle fixtures stay out of GitHub
Actions. Use an isolated local VM or container that actually runs systemd and
supports the target package manager. Do not change the host's package database,
service account, or systemd configuration.

Final execution depends on the package workflow and artifacts from #132, the
compatibility and preflight behavior from #134, and the diagnostics coverage
from #136. The offline backup and restore commands are provided by #133. This
plan must be reconciled with the final delivered commands before it is treated
as operator guidance. A supported release-to-release upgrade cannot be
qualified until a prior supported release exists; a temporary version override
or a debug package is useful fixture input but is not release-upgrade evidence.

# Fixture contract

- Accept explicit `.deb`, `.rpm`, and matching binary artifact paths. Verify
  package architecture and compare the extracted executable hash with the
  binary artifact before installation.
- Create a fresh, uniquely named isolated runtime for every scenario. Require
  a bounded readiness deadline, capture exit status and bounded logs, and
  destroy the runtime in a `finally`/trap path on success or failure.
- Use package-manager and `systemctl` operations inside the isolated runtime;
  never invoke privileged package or service commands directly on the host.
- Seed operator configuration and manager/worker state with known values.
  Record hashes and ownership/mode before each upgrade, backup, restore, and
  removal boundary.
- Allocate loopback ports dynamically inside the fixture and wait on explicit
  health/readiness or state predicates. Fixed sleeps are not readiness checks.
- Keep secrets and full config contents out of logs and evidence. Record paths,
  modes, hashes, and redacted command arguments instead.

# Acceptance scenario matrix

| Scenario | Setup and actions | Required observation |
| --- | --- | --- |
| Fresh install | Install each package on its selected distribution with no operator config. | Package files, dedicated account, paths, ownership, and unit match the package contract. The service is not enabled or started before explicit valid configuration is installed. |
| Configure and workload | Install isolated manager/worker configuration and identity; start the packaged service; wait for readiness; submit and inspect a job; deploy a service and verify its endpoints. | Job and service reach the documented states, and persistent state is written only under the configured paths. |
| Drain and stop | Drain the worker, wait for the bounded drain result, stop the service, then inspect status and logs. | No new assignment is accepted after drain; shutdown follows the documented policy; stored run and service state remains inspectable. |
| Upgrade | Seed active and completed records, stop or drain as documented, back up both state and external config/identity, then install the next compatible package. Record whether the baseline is a supported published release or a temporary debug fixture. | Package restart policy is followed. Config and state hashes remain unchanged except for documented migration writes. Preflight and post-upgrade inspection report the expected versions and records. Only a supported published baseline counts as release-upgrade qualification. |
| Interrupted maintenance | Stop between backup, package replacement, and explicit restart; resume using the documented runbook. | No package hook starts the daemon with absent/invalid config or silently crosses an irreversible migration boundary. Each interruption point has a documented recovery action. |
| Manager/worker crash and lost connection | Terminate only the fixture-owned daemon/process; partition or pause the fixture's worker connection; restore connectivity and inspect. | Recovered work is not silently replayed. If execution cannot be established, the run remains `unknown` until an explicit operator resolution; diagnostics identify actionable blockers without claiming success. |
| Migration failure | Present a copied fixture database with a future or deliberately invalid schema marker to the preflight/start path. | Preflight/start blocks as documented, retains the original state, and exposes enough diagnostics to select recovery. No in-place downgrade is attempted. |
| Stale restore | Back up state, make later writes, restore the older snapshot to a new destination, then attempt startup before and after the explicit recovery gate. | Restore preserves snapshot contents and creates the recovery hold. Startup is rejected before clearance; stale state is not described as proof that later work did not execute. |
| Remove and reinstall | Remove the package normally, inspect the state/config/identity paths, reinstall, then explicitly configure and start. | Normal removal preserves operator state and credentials. Reinstall does not enable/start prematurely and reuses preserved configuration only according to documented policy. Purge cleanup requires an explicit destructive operator action. |

Run each package target independently. A passing `.deb` scenario does not
qualify the `.rpm`, and a package payload inspection does not substitute for
runtime/systemd evidence.

# Evidence record template

For each run, capture:

- source commit and workflow run URL;
- target distribution/version, `arm64`/`aarch64` architecture, runtime image
  digest, and systemd version;
- package filenames, package SHA-256 values, package metadata, and matching
  binary SHA-256;
- scenario identifier, redacted commands, bounded timestamps, exit codes, and
  observed state transitions;
- config/state hashes and ownership/mode checks at the relevant boundaries;
- pass, fail, or not-run result for every row above, plus cleanup result;
- deviations, unsupported steps, and uncertainty that remains after recovery.

Do not mark issue #137 complete until each accepted distribution/package row
has the required runtime evidence, documentation is exercised against the
delivered CLI, and all dependency-specific tests remain owned by their source
issues.
