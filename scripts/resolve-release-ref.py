#!/usr/bin/env python3
"""Resolve a version tag to its authorized release line."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path


TAG_PATTERN = re.compile(r"^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


def git(repository: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repository), *arguments],
        text=True,
        capture_output=True,
        check=False,
    )


def is_ancestor(repository: Path, commit: str, reference: str) -> bool:
    result = git(repository, "merge-base", "--is-ancestor", commit, reference)
    return result.returncode == 0


def resolve(repository: Path, tag: str, workspace_version: str) -> dict[str, str]:
    match = TAG_PATTERN.fullmatch(tag)
    if match is None:
        raise ValueError("release tag must use vMAJOR.MINOR.PATCH without leading zeroes")
    version = tag[1:]
    if workspace_version != version:
        raise ValueError("release tag version does not match the workspace version")

    tag_result = git(repository, "rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}")
    if tag_result.returncode != 0:
        raise ValueError("release tag does not resolve to a Git commit")
    commit = tag_result.stdout.strip()
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("release tag did not resolve to a full Git commit ID")

    major, minor, _patch = match.groups()
    line = f"{major}.{minor}"
    main_ref = "refs/remotes/origin/main"
    if is_ancestor(repository, commit, main_ref):
        branch = "main"
    else:
        branch = f"release/{line}"
        maintenance_ref = f"refs/remotes/origin/{branch}"
        if not is_ancestor(repository, commit, maintenance_ref):
            raise ValueError("release tag commit is not on its authorized release branch")

    return {"version": version, "sha": commit, "line": line, "branch": branch}


def main() -> int:
    if len(sys.argv) != 4:
        raise SystemExit("usage: resolve-release-ref.py <repository> <tag> <workspace-version>")
    try:
        result = resolve(Path(sys.argv[1]), sys.argv[2], sys.argv[3])
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
