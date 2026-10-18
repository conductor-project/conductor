---
type: Reference
title: Process backend platform boundaries
description: Native process launch is supported across tested Linux and macOS targets, while kernel enforcement and executable loading remain platform-specific.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/structs/host_process_backend.rs
    title: Native launch
  - resource: ../src/cgroup.rs
    title: Linux-only cgroup support
  - resource: ../src/landlock.rs
    title: Linux-only Landlock support
  - resource: ../../conductor/tests/macos_multiprocess.rs
    title: Apple silicon multi-process test
  - resource: ../../../.github/workflows/pr.yml
    title: Debug platform matrix
---

# Native launch selection

Conductor does not inspect ELF or Mach-O headers before launch. It gives the
configured command to the OS and reports an OS launch rejection as a spawn
failure. The OS loader handles architecture and universal-binary slice
selection; scripts rely on their shebang interpreter. If `executableDigest` is
configured, the worker hashes the executable immediately before launch and
rejects a mismatch. This check does not protect against a same-user process
changing the file after hashing.

# Platform-specific mechanisms

Linux cgroup v2 and Landlock code is conditionally compiled and is not a macOS
feature. macOS process lifecycle tests do not establish those Linux kernel
guarantees. The exact tested CI matrix is in the repository
[platform evidence](../../../okf/platforms-and-evidence.md). Linux arm64 and
Intel macOS are not qualified by the current workflows.

# Descriptor hygiene

The launcher marks inherited descriptors close-on-exec around backend setup
and tests check that unintended descriptors do not reach workload code. This
is descriptor hygiene, not a general isolation boundary.
