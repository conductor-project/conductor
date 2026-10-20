---
type: Reference
title: conductor CLI and configuration
description: Clap commands select daemon operation, named operator contexts, strict JSON selects role-specific listeners, and workers can run bounded local overlay probes.
status: stable
source_revision: c3fbba5483c79e27e0687e445fde1a89ace9d872
source_reviewed_at: 2026-10-20
sources:
  - resource: ../src/main.rs
    title: CLI and serde configuration
  - resource: ../src/operator_cli.rs
    title: Operator subcommands and flags
  - resource: ../src/overlay_probe_cli.rs
    title: Bounded local worker overlay application probe
  - resource: ../src/contexts.rs
    title: Named connection contexts
  - resource: ../src/upgrade_cli.rs
    title: Local upgrade preflight
  - resource: ../src/backup_cli.rs
    title: Offline backup, restore, and recovery clearance
  - resource: ../src/storage_cli.rs
    title: Read-only manager storage inspection
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
`conductor backup create|inspect|restore` creates, checks, and restores an
offline state archive. Inspection checks checksums, SQLite integrity, and
database schema markers against the manifest. Pass every required configuration
and identity path to `backup create` with `--include-path`; restore creates a
new destination with manager and worker state below it and never replaces an
existing path. Each restored component remains held from startup until explicit
recovery clearance.
`conductor storage inspect` accepts `--manager-data-dir PATH`,
`--worker-data-dir PATH`, or both. It reads the selected database(s) without
migration or writes. Manager output includes retained and eligible history
counts, permanent idempotency and audit counts, SQLite page/free-page counts,
and database, WAL, and shared-memory file sizes. Worker output includes run,
log, command, report-sequence counts, SQLite page/free-page counts, file sizes,
and an upper bound for all unpruned log-pair bytes, including active runs. Pass the configured
`--completed-run-retention`, `--log-retention-runs`, and
`--log-max-bytes-per-stream` values when overriding the defaults.
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

`conductor upgrade preflight` reads one or both local SQLite schema markers
without opening the daemon or applying migrations. It requires
`--manager-data-dir`, `--worker-data-dir`, or both; use `--output json` for a
machine-readable report. A report about an existing database is local evidence
only and does not validate active remote peers, installed packages, or
distribution support. JSON reports include `requiredMaintenanceActions`;
human output prints the same guidance for unsupported schemas or required
migrations. See the [upgrade compatibility policy](../../../okf/operations-and-recovery.md#upgrade-compatibility-policy).

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
ID/concurrency, access mode, request and log limits, completed-run retention,
and optional cgroup and Landlock settings. `completed_run_retention` defaults
to 10,000 and accepts 1 through 1,000,000. Manager configuration adds a separate worker transport
listener, manager identity, resource domains, labels, and additional worker
descriptors. Worker configuration specifies the manager HTTPS endpoint, CA,
worker state/ID, and either an enrollment credential or existing identity.
Use the checked-in examples as complete field-level references rather than
copying an outdated schema description.

Linux standalone workers may additionally set both
`worker_overlay_network_domain_id` and the absolute
`worker_overlay_helper_binary` path to join an overlay domain. The helper owns
one worker Iroh endpoint, packet adapter, TUN interface, and overlay routes; it
is a separate process and should be installed with only `CAP_NET_ADMIN`. These
fields are rejected for `all` and `manager` roles. Other platforms reject
overlay-enabled workers.

The manager or combined `all` role can publish host-network service DNS through
`service_dns.suffix`. Overlay publication is opt-in: configure both
`service_dns.overlayNetworkDomainId` and `service_dns.overlaySuffix` to publish
eligible service ports at their workers' assigned overlay IPv4 addresses under
a separate DNS suffix. The network domain can be created after manager startup;
overlay queries fail with `SERVFAIL` until it exists. Expired or revoked peer
leases are withdrawn. Services must bind their port on the overlay address or
a wildcard address. Resolver forwarding remains an administrator task; see the
[service discovery bundle](../../../okf/service-discovery.md).

Run `conductor overlay probe --target-ip <private-ip> --port <port> --path
<safe-health-path>` on an overlay-enabled worker to send a bounded HTTP GET
through its local route. The total deadline is at most five seconds and the
response body is limited to 65,536 bytes. The probe does not use operator API
credentials, does not follow redirects or proxies, and stays local by default.
To persist a completed HTTP response to the manager, add
`--config /path/to/worker.json` before the `probe` subcommand. This uses the
worker's existing protected identity and CA files, requires the target address
to appear in its current manager-issued peer configuration, and reports the
result over worker mTLS.

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
