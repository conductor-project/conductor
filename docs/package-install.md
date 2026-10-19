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
