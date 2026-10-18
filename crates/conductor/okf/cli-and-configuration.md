---
type: Reference
title: conductor CLI and configuration
description: Clap commands select daemon operation and strict JSON configuration selects role-specific listeners, state, and identity paths.
status: stable
source_revision: f0d7e23f83ae770450f7b3302e5dd8eb71bf5c9a
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/main.rs
    title: CLI and serde configuration
  - resource: ../src/operator_cli.rs
    title: Operator subcommands and flags
  - resource: ../../../examples/local.json
    title: Local configuration
  - resource: ../../../examples/manager.json
    title: Manager configuration
  - resource: ../../../examples/worker-one.json
    title: Worker configuration
---

# Command surface

`conductor agent --config PATH [--role all|manager|worker]` starts the daemon.
`--role` overrides the role in the JSON file but does not bypass validation.
`conductor operator` requires a protected token file and accepts a manager URL
and request timeout. Its commands are `workers list|inspect`, `service
inspect`, `deploy`, `scale`, `rollback`, `drain start|status|clear`, `runs`,
and `logs`. It can inspect and operate on manager state, but does not create a
one-shot job definition or start a one-shot run. Bootstrap commands create or
revoke worker enrollment identities and create an operator token in a new
protected file. There is no `submit` command; use the HTTP API.

The hidden `__internal-launch` command is reserved for the process backend and
is not a user job API.

# Strict configuration

Configuration uses `serde(deny_unknown_fields)`. Config is rejected before
listeners or state are opened when keys are unknown or role-required values
are missing. The supported roles are combined `all`, standalone `manager`, and
standalone `worker`.

The all-role example configures `listen`, manager and worker state dirs, worker
ID/concurrency, access mode, request and log limits, and optional cgroup and
Landlock settings. Manager configuration adds a separate worker transport
listener, manager identity, resource domains, labels, and additional worker
descriptors. Worker configuration specifies the manager HTTPS endpoint, CA,
worker state/ID, and either an enrollment credential or existing identity.
Use the checked-in examples as complete field-level references rather than
copying an outdated schema description.

# Path resolution

Absolute paths are preserved. In standalone roles, manager/worker data and
role-identity paths resolve relative to the configuration file; the operator
token file does so in every role. In combined `all` mode, state, cgroup, and
Landlock paths are working-directory-relative. If `--role` overrides an `all`
configuration, relative state paths retain the working-directory basis.
Prefer explicit absolute paths in services.

## Failure and security behavior

Malformed config fails startup with an error; secret values and environment
contents are not intended for diagnostics. Token, manager CA private key,
worker identity, and enrollment files need restrictive ownership/mode. The
manager API is loopback-only; expose a trusted TLS proxy for remote operator
access. See repository [configuration guidance](../../../okf/configuration.md).
