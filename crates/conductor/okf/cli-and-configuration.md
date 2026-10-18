---
type: Reference
title: conductor CLI and configuration
description: Clap commands select daemon operation, named operator contexts, and strict JSON configuration selects role-specific listeners, state, and identity paths.
status: stable
source_revision: c696ef83dd136b8317c3c440ad4e4e2401c54a8c
source_reviewed_at: 2026-10-19
sources:
  - resource: ../src/main.rs
    title: CLI and serde configuration
  - resource: ../src/operator_cli.rs
    title: Operator subcommands and flags
  - resource: ../src/contexts.rs
    title: Named connection contexts
  - resource: ../../../examples/local.json
    title: Local configuration
  - resource: ../../../examples/manager.json
    title: Manager configuration
  - resource: ../../../examples/worker-one.json
    title: Worker configuration
---

# Command surface

`conductor completions SHELL` generates completions from the implemented Clap
command tree for Bash, Elvish, Fish, PowerShell, and Zsh.
`conductor agent --config PATH [--role all|manager|worker]` starts the daemon.
`--role` overrides the role in the JSON file but does not bypass validation.
`conductor context create|list|show|use|delete` manages named connection
contexts. The store contains a server URL and references to optional token and
CA files; it never stores token contents. `operator --context NAME` selects a
context explicitly, otherwise the selected default applies. Explicit
`--server`, `--token-file`, `--ca-file`, and `--timeout-seconds` values override
the matching context settings. With no saved context, the operator defaults
to unauthenticated local loopback. Plain HTTP is limited to loopback; remote
HTTP requires HTTPS, and remote operator use requires a token file.

Context commands use concise human output consistently, including create, list,
show, use, and delete. Add `--output json` before or after the subcommand for
stable machine-readable results. Context output never exposes token or CA file
paths. Operator commands use the same human default and accept `--output json`;
log data remains raw bytes on stdout with JSON metadata on stderr.

`conductor operator` accepts a manager URL and request timeout. Its commands
are `workers list|inspect`, `service
inspect`, `deploy`, `scale`, `rollback`, `drain start|status|clear`, `runs`,
and `logs`. It can inspect and operate on manager state, but does not create a
one-shot job definition or start a one-shot run. Bootstrap commands create or
revoke worker enrollment identities and create an operator token in a new
protected file. There is no `submit` command; use the HTTP API.

Failures are written to stderr with a nonzero exit status; current CLI commands
do not prompt interactively.

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

Absolute paths are preserved. Relative context token and CA file references
are made absolute when the context is created. In standalone roles,
manager/worker data and role-identity paths resolve relative to the
configuration file; bootstrap identity paths follow their own command
semantics. In combined `all` mode, state, cgroup, and
Landlock paths are working-directory-relative. If `--role` overrides an `all`
configuration, relative state paths retain the working-directory basis.
Prefer explicit absolute paths in services.

## Failure and security behavior

Malformed config fails startup with an error; secret values and environment
contents are not intended for diagnostics. Token, manager CA private key,
worker identity, and enrollment files need restrictive ownership/mode. The
manager API is loopback-only; expose a trusted TLS proxy for remote operator
access. Context listings and detail output omit credential file paths; token
contents remain only in the protected token file. CLI context configuration
and local tests were reviewed at the source revision recorded above; no
platform qualification is implied. See repository
[configuration guidance](../../../okf/configuration.md).
