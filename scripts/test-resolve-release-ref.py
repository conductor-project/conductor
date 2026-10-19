#!/usr/bin/env python3
"""Exercise release-tag authorization with unprivileged temporary Git refs."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "resolve_release_ref", Path(__file__).with_name("resolve-release-ref.py")
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
resolve = _MODULE.resolve


class ReleaseRefTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="conductor-release-ref-")
        self.repository = Path(self.temporary.name)
        self.git("init", "--quiet", "-b", "main")
        self.git("config", "user.name", "Release Ref Test")
        self.git("config", "user.email", "release-ref-test@example.invalid")
        self.git("config", "commit.gpgsign", "false")

        self.main_base = self.commit("base")
        self.git("tag", "v0.0.1", self.main_base)
        self.git("update-ref", "refs/remotes/origin/main", self.main_base)

        self.git("switch", "--quiet", "-c", "release/0.0")
        self.maintenance_commit = self.commit("maintenance")
        self.git("tag", "v0.0.2", self.maintenance_commit)
        self.git("update-ref", "refs/remotes/origin/release/0.0", self.maintenance_commit)

        self.git("switch", "--quiet", "main")
        self.current_commit = self.commit("current")
        self.git("tag", "v0.1.0", self.current_commit)
        self.git("update-ref", "refs/remotes/origin/main", self.current_commit)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def git(self, *arguments: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(self.repository), *arguments],
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        return result.stdout.strip()

    def commit(self, content: str) -> str:
        marker = self.repository / "marker"
        marker.write_text(content, encoding="utf-8")
        self.git("add", "marker")
        self.git("commit", "--quiet", "-m", content)
        return self.git("rev-parse", "HEAD")

    def test_current_line_tag_resolves_to_main(self) -> None:
        result = resolve(self.repository, "v0.1.0", "0.1.0")
        self.assertEqual(result, {
            "version": "0.1.0",
            "sha": self.current_commit,
            "line": "0.1",
            "branch": "main",
        })

    def test_maintenance_tag_resolves_to_its_release_branch(self) -> None:
        result = resolve(self.repository, "v0.0.2", "0.0.2")
        self.assertEqual(result, {
            "version": "0.0.2",
            "sha": self.maintenance_commit,
            "line": "0.0",
            "branch": "release/0.0",
        })

    def test_tag_on_main_uses_main_even_if_shared_with_maintenance_line(self) -> None:
        result = resolve(self.repository, "v0.0.1", "0.0.1")
        self.assertEqual(result["branch"], "main")

    def test_rejects_invalid_tag_syntax_and_version_mismatch(self) -> None:
        for tag, version in (("v00.1.0", "0.1.0"), ("v0.1.0", "0.1.1")):
            with self.subTest(tag=tag):
                with self.assertRaises(ValueError):
                    resolve(self.repository, tag, version)

    def test_rejects_missing_and_unauthorized_tags(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not resolve"):
            resolve(self.repository, "v0.1.2", "0.1.2")
        self.git("tag", "v0.1.1", self.maintenance_commit)
        with self.assertRaisesRegex(ValueError, "authorized release branch"):
            resolve(self.repository, "v0.1.1", "0.1.1")


if __name__ == "__main__":
    unittest.main(verbosity=2)
