# Platform support

Conductor's native process backend uses the daemon user's permissions. Starting a
job does not create a security sandbox. Platform support means the binary builds
and the portable process lifecycle passes on the listed OS and architecture; it
does not imply equivalent enforcement across operating systems.

## OS and architecture matrix

| OS | Architecture | Status | Qualification evidence |
| --- | --- | --- | --- |
| Linux | x86_64 | Supported | Required Linux CI builds, lints, runs debug and release suites, and exercises real cgroup v2 and Landlock behavior. |
| macOS | arm64 (Apple silicon) | Qualified | PR CI [run 39147861895](https://github.com/conductor-project/conductor/actions/runs/39147861895) passed dependency policy, formatting, all-target build, lint, and the full debug workspace suite. Post-merge CI [run 39148005287](https://github.com/conductor-project/conductor/actions/runs/39148005287) passed the debug suite, release tests and build, and inherited-descriptor check. The multi-process test starts two separate native worker processes, authenticates them over HTTPS, checks their distinct state and listeners, verifies manager-configured shared-host capacity, runs a native job, retrieves its output, and shuts both workers down. |
| macOS | x86_64 (Intel) | Not qualified | Intel macOS CI is outside the current qualification scope. |
| Linux | arm64 | Not yet qualified | No required CI runner currently exercises this target. |

On pull requests, the macOS job checks dependency policy and formatting, builds
all targets, runs lint, and runs the full debug test suite including the
portable multi-process lifecycle test. The post-merge macOS job runs release
tests and builds, then checks inherited-descriptor hygiene. Release work stays
on merged commits. Linux-only cgroup v2 and Landlock integration tests are
compiled only on Linux and remain covered by the separate Linux job. A passing
macOS build does not count as evidence for those Linux kernel guarantees.

## macOS qualification evidence

The Apple silicon claim is limited to the `macos-15` runner and arm64 debug
qualification on this host. The CI jobs and tests map to the audited surfaces
as follows:

| Surface | Qualification evidence | Boundary |
| --- | --- | --- |
| Dependencies and portable build | The macOS CI job checks dependency policy, formatting, all-target builds, lint, and the full debug workspace suite. | This does not qualify Intel macOS or Linux-only cgroup v2 and Landlock behavior. |
| Manager and two workers | `crates/conductor/tests/macos_multiprocess.rs::one_manager_authenticates_and_runs_on_two_native_worker_processes` starts two separate worker processes and an isolated loopback manager transport endpoint. It establishes mTLS worker sessions after enrollment, rejects a mismatched worker identity, verifies distinct worker IDs, PIDs, state directories and listeners, runs a native command, retrieves its output, and stops both workers with a deadline. | The test manager is the isolated manager service running in the test process; the two worker processes use the real worker service and transport client. This is one-host process evidence, not independent-host or routed-network qualification. |
| Signals, shutdown, process groups, and restart | `crates/conductor/tests/agent.rs` covers SIGINT and SIGTERM shutdown, process-group cleanup, active-run shutdown, interrupted-run recovery, and normal and abrupt manager restarts. | Cleanup covers ordinary process-group members. Descendants that leave the process group remain outside the macOS guarantee. |
| Logs | The multi-process test retrieves the worker's native stdout after completion; the agent tests also cover bounded binary logs, offsets, and retention metadata. | Log retrieval proves captured output, not a guarantee that logs survive every storage failure. |
| Inherited descriptors | The post-merge macOS release job runs `scripts/test-inherited-fds.py` against the release binary. | The check verifies that a descriptor inherited by the daemon is unavailable to the spawned job. |
| Resource inventory and enforcement | Worker status and the multi-process test report configured run slots and manager-assigned shared-host capacity. The platform matrix records cgroup v2 and Landlock as unavailable on macOS. | These values are scheduling metadata, not measured host inventory or kernel enforcement. Unsupported required protections are rejected. |
| Executable architecture and digest | The OS loader selects or rejects the native executable; `crates/conductor-worker/tests/runs.rs::executable_digest_mismatch_fails_without_spawning_the_workload` checks digest rejection before spawn. | Conductor does not parse executable headers. The digest test is platform-neutral; macOS slice selection is delegated to the OS loader. |

## Platform capabilities and limits

| Capability | Linux | macOS |
| --- | --- | --- |
| Native process execution | Yes | Yes |
| Inherited descriptor hygiene | Close-on-exec scan before startup threads and after backend initialization; Linux CI checks descriptor visibility under Landlock | Same close-on-exec scan; macOS CI checks that a descriptor passed to the daemon is unavailable to its job |
| Host resource inventory | Current worker status reports configured run slots; no host-wide resource inventory is exposed | Current worker status reports configured run slots; no host-wide resource inventory is exposed |
| Process-group signal and cleanup | Yes; cgroup v2 can clean descendants that leave the process group when configured | Yes for ordinary process-group members; descendants that leave the group are outside Conductor's cleanup guarantee |
| cgroup v2 CPU, memory, swap, and process limits | Optional, when an administrator delegates and configures cgroup v2 | Unavailable |
| Landlock filesystem policy | Optional, when the kernel and administrator configuration meet the requirements | Unavailable |
| Resource requests and reservations | Scheduling metadata; only configured cgroup limits are kernel enforcement | Scheduling metadata only; no kernel resource enforcement |
| Filesystem-policy request | Enforced by Landlock or rejected before launch, depending on policy mode | Rejected before launch |
| Executable architecture and digest | The OS loader decides whether the executable can run on this host; the worker independently verifies an optional operator-provisioned SHA-256 digest before launch | The OS loader decides whether the executable can run on this host, including selecting a compatible universal-binary slice; the worker independently verifies an optional operator-provisioned SHA-256 digest before launch |

Conductor does not silently replace a requested Linux protection with a weaker
macOS behavior. A macOS worker cannot advertise cgroup or Landlock enforcement,
and a job that requires filesystem policy or resource limits fails before its
executable is launched. Resource requests on macOS are not kernel-enforced.

The macOS tests establish local process lifecycle behavior only. They do not
qualify independent-host failure isolation or cross-host networking; those
require the separate MVP2 multi-host qualification in issue #77. The shared
host budget in the two-worker test is manager configured scheduling capacity,
not measured machine capacity or kernel enforcement. Host resource inventory
and refusal to place work on incompatible workers are covered by issues #66
and #67; actual macOS resource enforcement remains unavailable.

Conductor does not inspect ELF or Mach-O headers before launch. It passes the
configured command to the native process backend and lets the OS loader decide
whether the format and CPU architecture can run on that host. This also leaves
universal Mach-O slice selection to macOS. An OS launch rejection is reported
as `spawn_failed`; interpreter scripts are handled by their shebang
interpreter. When a job includes `executableDigest`, the worker independently
hashes the configured executable immediately before launch and fails the run
without spawning it if the digest does not match. The digest confirms file
contents; it does not provide a sandbox or protect against a same-user process
changing the file after verification. A successful OS launch does not guarantee
that dynamic libraries or other runtime requirements are available.

## macOS prerequisites

- Install Rust 1.99.1 with `rustfmt` and `clippy` as pinned by `rust-toolchain.toml`.
- Use a supported macOS release with the matching native Rust target available.
- Use distinct manager and worker state directories and loopback listeners for
  local process-level tests.
- Do not configure Linux-only cgroup v2 or Landlock enforcement on macOS.

For repository-wide commands, see [Build and check](../README.md#build-and-check).
