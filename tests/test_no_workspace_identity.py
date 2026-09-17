"""Prevent workspace identities from entering version-controlled text."""

from __future__ import annotations

import subprocess
from pathlib import Path


def test_no_workspace_identity_in_versioned_text():
    root = Path(__file__).resolve().parents[1]
    output = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    excluded_parts = {".git", ".venv", "generated"}
    patterns = (
        "@" + "example.com",
        "adb-" + "8804925006404308",
        "0724-" + "160225-u7kxvijl",
        "/Workspace" + "/Users/",
    )
    findings: list[str] = []

    for relative_bytes in output.split(b"\0"):
        if not relative_bytes:
            continue
        relative = Path(relative_bytes.decode())
        if excluded_parts.intersection(relative.parts):
            continue
        try:
            text = (root / relative).read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for pattern in patterns:
            if pattern in text:
                findings.append(f"{relative}: {pattern}")

    assert not findings, "Workspace identity found:\n" + "\n".join(findings)
