#!/usr/bin/env python3
"""Check package artifact metadata before a release promotion."""

from __future__ import annotations

import json
import re
import subprocess
import sys
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

    expected_sum_names = sorted([deb.name, rpm.name, "BUILD-INFO.json"])
    sums = (directory / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
    listed = sorted(line.split(maxsplit=1)[1].lstrip("* ") for line in sums if line.strip())
    if listed != expected_sum_names:
        raise SystemExit("SHA256SUMS does not list exactly the release packages and build metadata")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
