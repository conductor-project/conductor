# MVP2 protocol and persistence versions

This document defines manager-worker compatibility and the local SQLite
schema upgrade contract. The wire contract remains transport-neutral; it does
not enable remote worker connections by itself.

## Protocol negotiation

Each peer offers an inclusive range of positive protocol versions and lists
the capabilities it supports and requires. The handshake selects the highest
version in the overlap. No overlap fails the connection before commands are
accepted. A peer may require only a capability it also advertises as
supported; negotiation fails if either peer's requirements are not in the
mutual capability set.

Protocol versions are monotonically increasing wire-contract revisions.
Versions inside a peer's advertised range are mutually compatible. A change
that alters command meaning or makes an existing representation incompatible
must use a new version outside the prior compatible range. A rolling upgrade
is allowed only while every connected pair has an overlapping range. Do not
retry with unauthenticated transport or silently fall back after a negotiation
failure.

Capabilities are explicit and strict. Unknown capability names and unknown
JSON fields fail decoding; an unknown mandatory capability therefore cannot
be treated as supported. Capability support is separate from OS, CPU
architecture, execution-backend, and enforcement capabilities advertised by
the worker. Those platform constraints remain explicit in the execution
contract, and reservations remain distinct from enforced resource limits.

The initial contract currently advertises protocol version 1 and the
transport-neutral capabilities for assignment generations, command
deduplication, ordered reports, and worker incarnations. The range and
capability list can expand only when an implementation supports the advertised
behavior and has compatibility tests.

## SQLite schemas

Manager and worker databases have separate SQLite `user_version` values. The
current schema version for both databases is 3. Startup migrates supported
older schemas transactionally and preserves the existing job, run, log, and
worker-incarnation records. A schema newer than the binary supports is a
startup error with the detected version; startup must not reset or rewrite
that database. An unrecognized legacy layout also fails instead of guessing.

The manager migrator handles schemas 0, 1, and 2, and fixtures prove that
version-2 run history survives migration. The worker migrator handles schemas
0, 1, and 2, and a version-1 fixture proves its run history and log metadata
survive migration. New migration steps must be append-only, transactional,
and must extend these fixtures to prove that existing history survives. The
database schema version is independent of the network protocol version.

## Upgrade and rollback

1. Stop manager and workers cleanly and take consistent backups of manager
   state, each worker database, and worker log directories.
2. Install binaries that have an overlapping protocol range with all peers
   that will remain connected. For a coordinated upgrade with no overlap,
   stop remote workers until both sides are upgraded.
3. Start workers and then the manager; each process applies its own supported
   local database migrations before accepting work. Confirm readiness and
   inspect retained jobs, runs, and logs before resuming traffic.
4. Keep backups until the upgraded deployment has been verified.

There is no automatic downgrade migration. Once a database has advanced past
the maximum schema version supported by an older binary, rolling that binary
back against the migrated database is unsupported. Restore the matching
pre-upgrade database and log backup before reverting binaries. Restoring a
backup also discards changes made after that backup, so isolate or account for
those changes before rollback.
