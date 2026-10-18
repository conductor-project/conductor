#!/usr/bin/env python3
"""Validate workspace lint inheritance and centralized dependency decisions."""

from pathlib import Path
import json
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
errors: list[str] = []

root_text = (ROOT / "Cargo.toml").read_text()
lint_section = re.search(r"(?ms)^\[workspace\.lints\.rust\]\s*$(.*?)(?=^\[|\Z)", root_text)
if not lint_section or not re.search(r'^\s*unsafe_code\s*=\s*"forbid"\s*$', lint_section.group(1), re.M):
    errors.append('root [workspace.lints.rust].unsafe_code must be "forbid"')

expected_release = {
    "opt-level": "3",
    "lto": '"fat"',
    "codegen-units": "1",
    "incremental": "false",
    "overflow-checks": "true",
    "debug-assertions": "true",
    "panic": '"unwind"',
    "debug": '"line-tables-only"',
    "strip": '"none"',
}
release_section = re.search(r"(?ms)^\[profile\.release\]\s*$(.*?)(?=^\[|\Z)", root_text)
for key, value in expected_release.items():
    if not release_section or not re.search(rf"^\s*{re.escape(key)}\s*=\s*{re.escape(value)}\s*$", release_section.group(1), re.M):
        errors.append(f"[profile.release].{key} must be {value}")

decisions = (ROOT / "dependency-decisions.md").read_text()
metadata = json.loads(subprocess.check_output(
    ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
    cwd=ROOT,
    text=True,
))
workspace_members = set(metadata["workspace_members"])
external_dependencies: set[str] = set()

for package in metadata["packages"]:
    if package["id"] not in workspace_members:
        continue
    manifest_path = Path(package["manifest_path"])
    manifest_text = manifest_path.read_text()
    if not re.search(r"(?m)^\[lints\]\s*\nworkspace\s*=\s*true\s*$", manifest_text):
        errors.append(f"{manifest_path.relative_to(ROOT)} must inherit workspace lints")
    for dependency in package["dependencies"]:
        name = dependency["name"]
        workspace_reference = re.search(
            rf"(?m)^\s*{re.escape(name)}\.workspace\s*=\s*true\s*$",
            manifest_text,
        ) or re.search(
            rf"(?m)^\s*{re.escape(name)}\s*=\s*\{{[^\n}}]*\bworkspace\s*=\s*true\b[^\n}}]*\}}\s*$",
            manifest_text,
        )
        if not workspace_reference:
            errors.append(f"{manifest_path.relative_to(ROOT)} must inherit centralized dependency {name}")
        if dependency["uses_default_features"]:
            errors.append(f"{manifest_path.relative_to(ROOT)} enables default features for {name}")
        if dependency["source"] is not None:
            external_dependencies.add(name)

for name in sorted(external_dependencies):
    if f"`{name}`" not in decisions:
        errors.append(f"external dependency {name} is missing from dependency-decisions.md")

if errors:
    print("Dependency policy check failed:", file=sys.stderr)
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    sys.exit(1)

print("Dependency policy checks passed.")
