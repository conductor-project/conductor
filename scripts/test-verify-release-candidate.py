#!/usr/bin/env python3
"""Exercise release-candidate rejection paths without privileged fixtures."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("verify-release-candidate.py")
SPEC = importlib.util.spec_from_file_location("verify_release_candidate", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
VERIFY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERIFY)

VERSION = "0.0.1"
SOURCE_SHA = "0123456789abcdef" * 2 + "01234567"
ARCHITECTURE = "x86_64"
BINARY = b"synthetic conductor executable"


class ReleaseCandidateVerifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="conductor-release-candidate-test-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.deb_name = f"conductor_{VERSION}_amd64.deb"
        self.rpm_name = f"conductor-{VERSION}-1.{ARCHITECTURE}.rpm"
        self.info = {
            "sourceRevision": SOURCE_SHA,
            "version": VERSION,
            "targetArchitecture": ARCHITECTURE,
            "buildProfile": "release",
            "binarySha256": hashlib.sha256(BINARY).hexdigest(),
        }
        self.write_candidate()

    def write_candidate(self) -> None:
        (self.directory / "BUILD-INFO.json").write_text(json.dumps(self.info), encoding="utf-8")
        for name in (self.deb_name, self.rpm_name):
            package = self.directory / name
            package.write_bytes(b"synthetic package")
            Path(f"{package}.sigstore.json").write_text("{}", encoding="utf-8")
        names = [self.deb_name, self.rpm_name, "BUILD-INFO.json"]
        (self.directory / "SHA256SUMS").write_text(
            "".join(
                f"{hashlib.sha256((self.directory / name).read_bytes()).hexdigest()}  {name}\n"
                for name in names
            ),
            encoding="utf-8",
        )

    def fake_command(self, args: list[str] | tuple[str, ...], *, cwd: str | None = None, **_: object) -> subprocess.CompletedProcess:
        args = list(args)
        if args[:2] == ["dpkg-deb", "--field"]:
            value = {"Package": "conductor", "Version": VERSION, "Architecture": "amd64"}[args[-1]]
            return subprocess.CompletedProcess(args, 0, stdout=value, stderr="")
        if args[:2] == ["dpkg-deb", "--extract"]:
            destination = Path(args[3]) / "usr/bin"
            destination.mkdir(parents=True)
            (destination / "conductor").write_bytes(BINARY)
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
        if args[0] == "rpm" and args[1:3] == ["-qp", "--queryformat"]:
            value = {"%{NAME}": "conductor", "%{VERSION}": VERSION, "%{ARCH}": ARCHITECTURE}[args[3]]
            return subprocess.CompletedProcess(args, 0, stdout=value, stderr="")
        if args[0] == "rpm2cpio":
            return subprocess.CompletedProcess(args, 0, stdout=b"synthetic archive", stderr=b"")
        if args[:2] == ["cpio", "-idm"]:
            destination = Path(cwd or ".") / "usr/bin"
            destination.mkdir(parents=True)
            (destination / "conductor").write_bytes(BINARY)
            return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
        raise AssertionError(f"unexpected verifier command: {args[0]}")

    def verify(self, *, architecture: str = ARCHITECTURE, source_sha: str = SOURCE_SHA) -> int:
        with patch.object(sys, "argv", [str(SCRIPT), str(self.directory), VERSION, source_sha, architecture]):
            with patch.object(subprocess, "run", side_effect=self.fake_command):
                return VERIFY.main()

    def test_accepts_matching_candidate(self) -> None:
        self.assertEqual(self.verify(), 0)

    def test_rejects_wrong_source_revision(self) -> None:
        self.info["sourceRevision"] = "f" * 40
        self.write_candidate()
        with self.assertRaisesRegex(SystemExit, "sourceRevision does not match"):
            self.verify()

    def test_rejects_wrong_version(self) -> None:
        self.info["version"] = "9.9.9"
        self.write_candidate()
        with self.assertRaisesRegex(SystemExit, "version does not match"):
            self.verify()

    def test_rejects_wrong_architecture(self) -> None:
        self.info["targetArchitecture"] = "aarch64"
        self.write_candidate()
        with self.assertRaisesRegex(SystemExit, "targetArchitecture does not match"):
            self.verify()

    def test_rejects_debug_profile(self) -> None:
        self.info["buildProfile"] = "debug"
        self.write_candidate()
        with self.assertRaisesRegex(SystemExit, "buildProfile does not match"):
            self.verify()

    def test_rejects_malformed_source_revision_argument(self) -> None:
        with self.assertRaisesRegex(SystemExit, "full Git commit ID"):
            self.verify(source_sha="not-a-commit")

    def test_rejects_invalid_binary_digest(self) -> None:
        self.info["binarySha256"] = "not-a-digest"
        self.write_candidate()
        with self.assertRaisesRegex(SystemExit, "invalid binary digest"):
            self.verify()

    def test_rejects_changed_embedded_binary(self) -> None:
        def changed_binary(args: list[str] | tuple[str, ...], *, cwd: str | None = None, **kwargs: object) -> subprocess.CompletedProcess:
            args = list(args)
            if args[:2] == ["dpkg-deb", "--extract"]:
                destination = Path(args[3]) / "usr/bin"
                destination.mkdir(parents=True)
                (destination / "conductor").write_bytes(b"changed executable")
                return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
            if args[:2] == ["cpio", "-idm"]:
                destination = Path(cwd or ".") / "usr/bin"
                destination.mkdir(parents=True)
                (destination / "conductor").write_bytes(b"changed executable")
                return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")
            return self.fake_command(args, cwd=cwd, **kwargs)

        with patch.object(sys, "argv", [str(SCRIPT), str(self.directory), VERSION, SOURCE_SHA, ARCHITECTURE]):
            with patch.object(subprocess, "run", side_effect=changed_binary):
                with self.assertRaisesRegex(SystemExit, "does not match the digest"):
                    VERIFY.main()

    def test_rejects_checksum_manifest_with_extra_entries(self) -> None:
        with (self.directory / "SHA256SUMS").open("a", encoding="utf-8") as sums:
            sums.write(f"{'0' * 64}  unrelated-file\n")
        with self.assertRaisesRegex(SystemExit, "does not list exactly"):
            self.verify()

    def test_rejects_package_checksum_mismatch(self) -> None:
        lines = (self.directory / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
        lines[0] = f"{'0' * 64}  {self.deb_name}"
        (self.directory / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(SystemExit, "SHA256SUMS digest does not match"):
            self.verify()

    def test_rejects_missing_signature_bundle(self) -> None:
        (self.directory / f"{self.deb_name}.sigstore.json").unlink()
        with self.assertRaisesRegex(SystemExit, "Cosign bundle is missing"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
