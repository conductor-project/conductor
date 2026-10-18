# Platform support

Conductor's native process backend uses the daemon user's permissions. Starting a
job does not create a security sandbox. Platform support means the binary builds
and the portable process lifecycle passes on the listed OS and architecture; it
does not imply equivalent enforcement across operating systems.

## OS and architecture matrix

| OS | Architecture | Status | Qualification evidence |
| --- | --- | --- | --- |
| Linux | x86_64 | Supported | Required Linux CI builds, lints, runs debug and release suites, and exercises real cgroup v2 and Landlock behavior. |
| macOS | arm64 (Apple silicon) | In qualification | Local Apple silicon debug and release suites and the inherited-descriptor check have passed; MVP2 issue #79 adds required hosted CI coverage. |
| macOS | x86_64 (Intel) | Not qualified | Intel macOS CI is outside the current qualification scope. |
| Linux | arm64 | Not yet qualified | No required CI runner currently exercises this target. |

The macOS CI jobs run the workspace build, lint, debug and release test suites,
including the real-binary portable process lifecycle tests. Linux-only cgroup v2
and Landlock integration tests are compiled only on Linux and remain covered by
the separate Linux job. A passing macOS build does not count as evidence for
those Linux kernel guarantees.

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
require the separate MVP2 multi-host qualification in issue #77. The manager
plus two worker authenticated-transport demonstration is tracked with runtime
composition in issue #61. Host resource inventory and refusal to place work on
incompatible workers are tracked in issues #66 and #67. These remain explicit
qualification gaps until their implementation and macOS evidence are complete.

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
