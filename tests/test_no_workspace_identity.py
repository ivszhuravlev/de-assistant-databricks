"""Prevent workspace identities and secrets from entering version-controlled text."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_CYRILLIC = re.compile("[\u0400-\u04ff]")
_AZURE_SAS = re.compile(r"sv=\d{4}-\d{2}-\d{2}.*sig=", re.I | re.S)
_DATABRICKS_PAT = re.compile(r"\bdapi[a-f0-9]{16,}\b", re.I)
_OPENAI_KEY = re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")
_NOT_EMAIL_HOSTS = ("dfs.core.windows.net", "blob.core.windows.net")


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
        "dlsprovis" + "qa001",
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
        if _CYRILLIC.search(text):
            findings.append(f"{relative}: cyrillic")
        if _AZURE_SAS.search(text):
            findings.append(f"{relative}: azure-sas")
        if _DATABRICKS_PAT.search(text):
            findings.append(f"{relative}: databricks-pat")
        if _OPENAI_KEY.search(text):
            findings.append(f"{relative}: openai-key")
        for match in _EMAIL.finditer(text):
            host = match.group(0).rsplit("@", 1)[-1].lower()
            if host.endswith(_NOT_EMAIL_HOSTS):
                continue
            findings.append(f"{relative}: email {match.group(0)}")

    assert not findings, "Workspace identity found:\n" + "\n".join(findings)
