#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: package-linux.sh <deb-arch> <rpm-arch> <version> <binary> <output-dir> <source-revision> <profile>" >&2
  exit 2
}

[[ $# -eq 7 ]] || usage
deb_arch=$1
rpm_arch=$2
version=$3
binary=$4
output_dir=$5
source_revision=$6
profile=$7

[[ -f "$binary" ]] || { echo "binary artifact is missing" >&2; exit 1; }
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9.+~]+)?$ ]] || { echo "invalid package version" >&2; exit 1; }
[[ "$source_revision" =~ ^[0-9a-f]{40}$ ]] || { echo "invalid source revision" >&2; exit 1; }
[[ "$profile" == debug || "$profile" == release ]] || { echo "invalid build profile" >&2; exit 1; }

case "$(uname -m):$deb_arch:$rpm_arch" in
  x86_64:amd64:x86_64|aarch64:arm64:aarch64) ;;
  *) echo "runner architecture does not match requested package architecture" >&2; exit 1 ;;
esac

for tool in dpkg-deb rpmbuild rpm rpm2cpio cpio sha256sum tar readelf sort; do
  command -v "$tool" >/dev/null || { echo "required packaging tool is unavailable" >&2; exit 1; }
done

chmod 0755 "$binary"
binary_version=$("$binary" --version)
[[ "$binary_version" == "conductor $version" ]] || { echo "binary version does not match package version" >&2; exit 1; }
max_glibc_symbol=$(readelf --version-info "$binary" | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' | sort -Vu | tail -n 1 || true)
if [[ -n "$max_glibc_symbol" && "$(printf '%s\n' GLIBC_2.39 "$max_glibc_symbol" | sort -Vu | tail -n 1)" != GLIBC_2.39 ]]; then
  echo "binary requires $max_glibc_symbol; Rocky Linux 10 compatibility requires GLIBC_2.39 or older" >&2
  exit 1
fi

mkdir -p "$output_dir"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
root="$work/root"
install -D -m 0755 "$binary" "$root/usr/bin/conductor"
install -D -m 0644 packaging/systemd/conductor.service "$root/usr/lib/systemd/system/conductor.service"
install -D -m 0644 LICENSE.md "$root/usr/share/doc/conductor/copyright"
install -D -m 0644 docs/package-install.md "$root/usr/share/doc/conductor/INSTALL.md"
install -D -m 0644 packaging/agent.json.example "$root/usr/share/doc/conductor/examples/agent.json"

deb="$output_dir/conductor_${version}_${deb_arch}.deb"
deb_root="$work/deb-root"
mkdir -p "$deb_root/DEBIAN"
cat > "$deb_root/DEBIAN/control" <<EOF
Package: conductor
Version: $version
Section: admin
Priority: optional
Architecture: $deb_arch
Maintainer: Conductor Project Maintainers
Homepage: https://github.com/conductor-project/conductor
Depends: adduser, systemd
Description: daemon for submitting one-shot jobs and managing service deployments
 Conductor runs jobs and manages services on hosts operated by the administrator.
EOF
for maintainer_script in postinst prerm postrm; do
  install -m 0755 "packaging/debian/$maintainer_script" "$deb_root/DEBIAN/$maintainer_script"
done
cp -R "$root"/. "$deb_root"/
dpkg-deb --root-owner-group --build --uniform-compression "$deb_root" "$deb"

deb_extract="$work/deb-extract"
mkdir -p "$deb_extract"
dpkg-deb --extract "$deb" "$deb_extract"
cmp "$binary" "$deb_extract/usr/bin/conductor"

rpm_top="$work/rpmbuild"
mkdir -p "$rpm_top"/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
tar -C "$work" -czf "$rpm_top/SOURCES/conductor-package.tar.gz" root
cp packaging/rpm/conductor.spec "$rpm_top/SPECS/conductor.spec"
rpmbuild --define "_topdir $rpm_top" --define "package_version $version" --define "package_arch $rpm_arch" -bb "$rpm_top/SPECS/conductor.spec"
rpm_source=$(find "$rpm_top/RPMS" -type f -name '*.rpm' -print -quit)
[[ -n "$rpm_source" ]] || { echo "RPM package was not produced" >&2; exit 1; }
rpm_output="$output_dir/conductor-${version}-1.${rpm_arch}.rpm"
cp "$rpm_source" "$rpm_output"

rpm_extract="$work/rpm-extract"
mkdir -p "$rpm_extract"
rpm2cpio "$rpm_output" | (cd "$rpm_extract" && cpio -idm --quiet)
cmp "$binary" "$rpm_extract/usr/bin/conductor"

binary_sha256=$(sha256sum "$binary" | cut -d ' ' -f 1)
cat > "$output_dir/BUILD-INFO.json" <<EOF
{
  "sourceRevision": "$source_revision",
  "version": "$version",
  "targetArchitecture": "$rpm_arch",
  "buildProfile": "$profile",
  "binarySha256": "$binary_sha256"
}
EOF
(cd "$output_dir" && sha256sum "$(basename "$deb")" "$(basename "$rpm_output")" BUILD-INFO.json > SHA256SUMS)
[[ "$(dpkg-deb --field "$deb" Package)" == conductor ]]
[[ "$(dpkg-deb --field "$deb" Version)" == "$version" ]]
[[ "$(dpkg-deb --field "$deb" Architecture)" == "$deb_arch" ]]
[[ "$(rpm -qp --queryformat '%{NAME}' "$rpm_output")" == conductor ]]
[[ "$(rpm -qp --queryformat '%{VERSION}' "$rpm_output")" == "$version" ]]
[[ "$(rpm -qp --queryformat '%{ARCH}' "$rpm_output")" == "$rpm_arch" ]]
