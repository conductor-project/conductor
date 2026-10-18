---
okf_version: "0.2"
---

# conductor-process crate bundle

- [Overview](overview.md) - host process backend boundary.
- [Spawn and cleanup](spawn-and-cleanup.md) - native process groups and launch errors.
- [Cgroups and Landlock](cgroups-and-landlock.md) - optional Linux resource and filesystem enforcement.
- [Platform boundaries](platform-boundaries.md) - OS differences, executable loading, and descriptor hygiene.

This independent bundle describes host execution. Worker policy and desired
state are documented in sibling crate bundles.
