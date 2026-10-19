#!/usr/bin/env python3
"""Check package artifact metadata before a release promotion."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path


def run(*args: str) -> str:
    return subprocess.run(args, check=True, text=True, capture_output=True).stdout.strip()


def main() -> int:
    if len(sys.argv) != 5:
        raise SystemExit("usage: verify-release-candidate.py <artifact-dir> <version> <source-sha> <architecture>")
    directory = Path(sys.argv[1])
    version, source_sha, architecture = sys.argv[2:]
    if architecture not in {"x86_64", "aarch64"}:
        raise SystemExit("unsupported package architecture")
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise SystemExit("source revision is not a full Git commit ID")

    info = json.loads((directory / "BUILD-INFO.json").read_text(encoding="utf-8"))
    expected = {
        "sourceRevision": source_sha,
        "version": version,
        "targetArchitecture": architecture,
        "buildProfile": "release",
    }
    for key, value in expected.items():
        if info.get(key) != value:
            raise SystemExit(f"BUILD-INFO.json {key} does not match the release candidate")
    if not re.fullmatch(r"[0-9a-f]{64}", str(info.get("binarySha256", ""))):
        raise SystemExit("BUILD-INFO.json has an invalid binary digest")

    deb_arch = "amd64" if architecture == "x86_64" else "arm64"
    deb = directory / f"conductor_{version}_{deb_arch}.deb"
    rpm = directory / f"conductor-{version}-1.{architecture}.rpm"
    for package in (deb, rpm):
        if not package.is_file():
            raise SystemExit(f"expected package is missing: {package.name}")
        if not Path(f"{package}.sigstore.json").is_file():
            raise SystemExit(f"Cosign bundle is missing for {package.name}")

    if run("dpkg-deb", "--field", str(deb), "Package") != "conductor":
        raise SystemExit("Debian package name does not match")
    if run("dpkg-deb", "--field", str(deb), "Version") != version:
        raise SystemExit("Debian package version does not match")
    if run("dpkg-deb", "--field", str(deb), "Architecture") != deb_arch:
        raise SystemExit("Debian package architecture does not match")
    if run("rpm", "-qp", "--queryformat", "%{NAME}", str(rpm)) != "conductor":
        raise SystemExit("RPM package name does not match")
    if run("rpm", "-qp", "--queryformat", "%{VERSION}", str(rpm)) != version:
        raise SystemExit("RPM package version does not match")
    if run("rpm", "-qp", "--queryformat", "%{ARCH}", str(rpm)) != architecture:
        raise SystemExit("RPM package architecture does not match")

    with tempfile.TemporaryDirectory(prefix="conductor-release-verify-") as temporary:
        root = Path(temporary)
        deb_root = root / "deb"
        deb_root.mkdir()
        run("dpkg-deb", "--extract", str(deb), str(deb_root))
        deb_binary = deb_root / "usr/bin/conductor"
        rpm_root = root / "rpm"
        rpm_root.mkdir()
        rpm_archive = subprocess.run(
            ["rpm2cpio", str(rpm)], check=True, capture_output=True
        ).stdout
        subprocess.run(
            ["cpio", "-idm", "--quiet"],
            cwd=rpm_root,
            input=rpm_archive,
            check=True,
            capture_output=True,
        )
        rpm_binary = rpm_root / "usr/bin/conductor"
        for package_format, embedded_binary in (
            ("Debian", deb_binary),
            ("RPM", rpm_binary),
        ):
            if not embedded_binary.is_file():
                raise SystemExit(f"{package_format} package omits the Conductor binary")
            digest = hashlib.sha256(embedded_binary.read_bytes()).hexdigest()
            if digest != info["binarySha256"]:
                raise SystemExit("package binary does not match the digest in BUILD-INFO.json")

    expected_sum_names = {deb.name, rpm.name, "BUILD-INFO.json"}
    sums = (directory / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    entries: dict[str, str] = {}
    for line in sums:
        if not line.strip():
            continue
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if match is None or match.group(2) in entries:
            raise SystemExit("SHA256SUMS contains an invalid or duplicate entry")
        entries[match.group(2)] = match.group(1)
    if set(entries) != expected_sum_names:
        raise SystemExit("SHA256SUMS does not list exactly the release packages and build metadata")
    for name, expected_digest in entries.items():
        actual_digest = hashlib.sha256((directory / name).read_bytes()).hexdigest()
        if actual_digest != expected_digest:
            raise SystemExit(f"SHA256SUMS digest does not match {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
