# Linux package installation

The first packages target Debian 13 (amd64, arm64) and Rocky Linux 10
(x86_64, aarch64). Package files are built around the exact binary from the
matching workflow run. The packaging step does not compile Conductor.

There is currently no published package release. When packages are published,
download the package, `SHA256SUMS`, Sigstore signature bundle, and build
provenance from the same GitHub Release. Verify the checksum first, then the
signature and provenance before installation:

```sh
sha256sum --check SHA256SUMS
cosign verify-blob \
  --bundle conductor-<version>-1.<arch>.rpm.sigstore.json \
  --certificate-identity-regexp '^https://github.com/conductor-project/conductor/.github/workflows/merge.yml@refs/heads/(main|release/[0-9]+\.[0-9]+)$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  conductor-<version>-1.<arch>.rpm
gh attestation verify conductor-<version>-1.<arch>.rpm \
  --repo conductor-project/conductor
```

For Debian packages, use the corresponding `.deb` filename and `.sigstore.json`
bundle. Do not install a package whose checksum, signer identity, source
revision, architecture, or build profile does not match the release notes and
attestation. Public release packages are expected to be release-profile builds.

## Configure before starting

Package installation creates the `conductor` system account and installs the
systemd unit, binary, license, installation guidance, and an example at
`/usr/share/doc/conductor/examples/agent.json`. It does not install an active
configuration, enable the unit, start the daemon, or restart a running daemon.
Review the example, choose role-appropriate state and identity paths, and
create `/etc/conductor/agent.json` explicitly before starting the service:

```sh
sudo install -d -o root -g conductor -m 0750 /etc/conductor
sudo install -o root -g conductor -m 0640 \
  /usr/share/doc/conductor/examples/agent.json \
  /etc/conductor/agent.json
sudoedit /etc/conductor/agent.json
```

The example is for local `all` role use. Separate manager and worker hosts need
their own role-specific configuration and authenticated transport setup. The
service runs as the unprivileged `conductor` account with umask `0077`; keep
private keys and enrollment credentials readable only by the service account
and administrators. The default persistent path is `/var/lib/conductor`.

The package does not automatically enable or start the service. After
validating the configuration and arranging any external identity files, start
it explicitly:

```sh
sudo systemctl enable --now conductor.service
sudo systemctl status conductor.service
```

The unit has `Restart=no`. If a run becomes ambiguous after a crash or
maintenance event, inspect and reconcile it using the recovery runbook before
starting the daemon again.

## Upgrade, removal, and reinstall

Before upgrading, stop Conductor deliberately, take a consistent offline
backup, and run the compatibility preflight documented in the upgrade
runbook. Package scripts never start or restart the service. Install the new
package, review the release notes, and start the service manually after
preflight. Do not assume a database downgrade is available.

Normal package removal stops the service and removes package-owned files. It
preserves `/var/lib/conductor`, credentials, operator configuration, the
service account, and backups. Reinstall uses the existing state and leaves
configuration untouched. Package removal and purge do not delete persistent
state or credentials; an administrator must separately perform and verify any
destructive cleanup.

## Evidence boundaries

CI package jobs check metadata and verify that the extracted package binary is
byte-for-byte identical to the matching build artifact. They also reject a
binary that requires glibc newer than 2.39, the Rocky Linux 10 baseline.
Those checks do not prove that a host installed or started the package. Record installation,
upgrade, removal, and runtime results only from a Debian 13 or Rocky Linux 10
system with systemd, including architecture and package hash. Release and
package runtime claims must cite that evidence.

### Local arm64 qualification (2026-10-19)

On Apple silicon with Docker Desktop 29.8.1, the `debian:13` and
`rockylinux/rockylinux:10` images were verified as `linux/arm64`. Disposable
systemd-enabled containers exercised the arm64 PR debug packages from workflow
run 39204575826 (source `dd4a473de781604ab3f7d31f07402138b04e5bbc`):

| Distribution | Package | SHA-256 |
| --- | --- | --- |
| Debian 13 arm64 | `conductor_0.1.0_arm64.deb` | `b6b44413ce6fe35675af68d12eb5dc5fdcaf676ddc50378d2e4410c191335da8` |
| Rocky Linux 10 arm64 | `conductor-0.1.0-1.aarch64.rpm` | `c72d51006bd6d886b96856473a9e11d10bf1ae060ccf2eafd5ca94b863e2170c` |

Both formats passed fresh install, `conductor --version`, service-account and
payload checks, no automatic enable/start, explicit `systemctl start` and
`stop`, removal, and reinstall while preserving seeded operator configuration
and state. A separate debug 0.1.1 binary was built from the same source with a
temporary workspace package-version override and wrapped with
`scripts/package-linux.sh`; its artifacts are a local upgrade fixture, not
release artifacts:

| Package | SHA-256 |
| --- | --- |
| `conductor_0.1.1_arm64.deb` | `d968a0e65803b06430170306506e70ff3b30da53d02be63c0197ede3905d211f` |
| `conductor-0.1.1-1.aarch64.rpm` | `42ccc86d9877a2094698ebbc6eb749a133caab55bc93477c64a6b1103324ef3a` |

The 0.1.0-to-0.1.1 upgrade passed on both distributions. The configured service
remained active with the same main PID through package upgrade, the config and
state-marker hashes remained unchanged, and an explicit stop/start succeeded
afterward. These checks establish package lifecycle behavior for arm64 in these
Docker fixtures; they do not establish x86_64 target-distribution behavior,
release compatibility, database migration safety, or production-host behavior.

### Local Debian 13 x86_64 qualification (2026-10-19)

The `conductor_0.1.0_amd64.deb` package from PR workflow run
[39226570066](https://github.com/conductor-project/conductor/actions/runs/39226570066)
(artifact `conductor-linux-packages-x86_64-debug`, artifact ID `12174860463`)
was installed directly on a native Debian 13 x86_64 host. The artifact was a
debug package built from source merge revision
`6305eb47ce8535a2741c489c1b4db516e5f4e556`, which merged PR head
`ad530e7f6283fb4fc51c94ef3aaf7bff924f6e2e` into `41e3d28322471ca4d195bfa2d402e1cd21775ce5`.
The downloaded workflow artifact digest was
`sha256:675dde74f5decadce2536384f759a3f3a9220609eb5cf900a042b105af88e160`;
the Debian package SHA-256 was
`24a7133c58f23b59b9701d37a06a7e88c8a5a3a7fd78256d7df66cbc97f07ba0`.
Workflow checksum verification passed for the package bundle. Package metadata
reported version `0.1.0` and architecture `amd64`; the matching binary hash in
`BUILD-INFO` was
`6e11f40b0941675899c3ad9340c1d297e7d738a17174e57abe1b38a15233333e`.

Fresh installation created the service account and installed the executable
root-owned with mode 0755. The unit remained inactive and disabled after
installation. With isolated temporary manager/worker state and a dynamically
allocated loopback listener configured, explicit service start returned HTTP
200 from `/healthz`; the daemon ran as `conductor`. After stop, the state marker
and config hashes were unchanged. `dpkg -r`, reinstall, and `dpkg --purge`
removed package-owned executable/unit files while preserving the test config
and state through removal and reinstall. Reinstall also left the unit inactive
and disabled. The fixture reported `cleanup=passed` and removed its temporary
paths, test-created state/config, and service account. A follow-up SSH cleanup
inspection could not resolve the host name, so independent post-run inspection
was not completed. The exact systemd and kernel versions were not recorded.

This is Debian 13 x86_64 `.deb` lifecycle evidence for this debug package and
host only. It does not qualify Rocky Linux 10 x86_64 `.rpm`, an x86_64 package
upgrade, a published release, or production-host behavior. The fixture was run
locally by explicit authorization; privileged package and systemd fixtures
remain excluded from GitHub workflows.

### Local Rocky Linux 10 x86_64 qualification (2026-10-19)

The `conductor-0.1.0-1.x86_64.rpm` package from the same PR workflow run 44 and
artifact `conductor-linux-packages-x86_64-debug` (ID `12174860463`, archive
digest
`sha256:675dde74f5decadce2536384f759a3f3a9220609eb5cf900a042b105af88e160`)
was installed in a disposable Rocky Linux 10.2 x86_64 systemd container on
Fortress. Source merge revision was
`6305eb47ce8535a2741c489c1b4db516e5f4e556`, merging PR head
`ad530e7f6283fb4fc51c94ef3aaf7bff924f6e2e` into
`41e3d28322471ca4d195bfa2d402e1cd21775ce5`. The RPM SHA-256 was
`db85b4c3ad5541743ac91c32671a11e4da2ef550ab80154a746912b5dd318789`; the
matching binary SHA-256 from `BUILD-INFO.json` was
`6e11f40b0941675899c3ad9340c1d297e7d738a17174e57abe1b38a15233333e`. The
workflow artifact checksum and RPM metadata (`0.1.0`, `x86_64`) were verified
before installation.

The container used the cached `docker.io/rockylinux/rockylinux:10` amd64 image
(image ID
`0b5329d806d053ad65ab6b21fc3e49f1025e970cd826547a01a93b3ce9012796`) with
systemd 257 (`257-23.el10_2.2.rocky.0.1-gb237c67`), curl, and procps-ng
installed. Fortress was Debian 13 x86_64 with Podman 5.4.2, cgroup v2, and
kernel `6.12.113+deb13-amd64`. The minimal Rocky image sets `tsflags=nodocs`;
the test cleared that DNF option for package installation so the packaged
example configuration was available for the service test.

The test image installed systemd with `dnf -y install systemd curl procps-ng`.
The lifecycle commands inside the container were:

```sh
dnf -y --setopt=tsflags= install /artifacts/conductor-0.1.0-1.x86_64.rpm
systemctl start conductor.service
curl --silent --show-error --fail http://127.0.0.1:8080/healthz
systemctl stop conductor.service
dnf -y remove conductor
dnf -y --setopt=tsflags= install /artifacts/conductor-0.1.0-1.x86_64.rpm
dnf -y remove conductor
```

Fresh install left the unit inactive and disabled, created the `conductor`
account, installed `/usr/bin/conductor` as `root:root` mode 0755, and the
installed binary hash matched `BUILD-INFO.json`. After configuring the packaged
example, explicit `systemctl start` passed the `/healthz` check and the service
ran as `conductor`; `systemctl stop` succeeded. `dnf remove` preserved the
operator config and a seeded state marker. Reinstall left the service inactive
and disabled and preserved both hashes; final removal also preserved them. The
fixture reported `cleanup=passed`; a separate read-only check found no test
container, temporary image, or staging directory left on Fortress.

This qualifies Rocky Linux 10.2 x86_64 `.rpm` install/runtime/removal and
reinstall behavior for this debug package in a systemd container. It does not
qualify an x86_64 package upgrade, a published release, or production-host
behavior. The fixture was run locally; privileged package and systemd fixtures
remain excluded from GitHub workflows.
