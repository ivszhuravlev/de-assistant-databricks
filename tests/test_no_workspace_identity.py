"""Prevent workspace identities and secrets from entering version-controlled text.

The checks are generic on purpose. Naming a real host, account, or company domain
here would put that identity back into the public tree.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_CHECKS = (
    ("databricks-host", re.compile(r"adb-\d{8,}\.\d+\.azuredatabricks\.net", re.I)),
    ("cluster-id", re.compile(r"\b\d{4}-\d{6}-[a-z0-9]{8}\b")),
    ("workspace-user-path", re.compile("/Workspace" + "/Users/")),
    ("adls-account", re.compile(r"@([A-Za-z0-9][A-Za-z0-9-]{2,})\.dfs\.core\.windows\.net")),
    ("blob-account", re.compile(r"//([A-Za-z0-9][A-Za-z0-9-]{2,})\.blob\.core\.windows\.net")),
    ("azure-sas", re.compile(r"sv=\d{4}-\d{2}-\d{2}[^\s\"']*sig=", re.I)),
    ("databricks-pat", re.compile(r"\bdapi[a-f0-9]{16,}\b", re.I)),
    ("openai-key", re.compile(r"\bsk-[A-Za-z0-9]{20,}\b")),
    ("non-latin-prose", re.compile("[\u0400-\u04ff\u0600-\u06ff\u4e00-\u9fff]")),
)
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_STORAGE_HOSTS = ("dfs.core.windows.net", "blob.core.windows.net")
_PLACEHOLDER_ACCOUNTS = {"exampleaccount", "account", "storageaccount"}
_OPERATOR_ONLY_FILES = {"HANDOFF.md", "AGENTS.md", ".env", "config/cluster-e2e.json"}


def _tracked_paths(root: Path) -> list[Path]:
    output = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout
    excluded_parts = {".git", ".venv", "generated"}
    paths = []
    for relative_bytes in output.split(b"\0"):
        if not relative_bytes:
            continue
        relative = Path(relative_bytes.decode())
        if excluded_parts.intersection(relative.parts):
            continue
        paths.append(relative)
    return paths


def test_no_workspace_identity_in_versioned_text():
    root = Path(__file__).resolve().parents[1]
    findings: list[str] = []

    for relative in _tracked_paths(root):
        if relative.as_posix() in _OPERATOR_ONLY_FILES:
            findings.append(f"{relative}: operator-only file must stay untracked")
        try:
            text = (root / relative).read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for name, pattern in _CHECKS:
            match = pattern.search(text)
            if not match:
                continue
            if name in {"adls-account", "blob-account"}:
                account = match.group(1).lower()
                if account in _PLACEHOLDER_ACCOUNTS:
                    continue
            findings.append(f"{relative}: {name} {match.group(0)!r}")
        for match in _EMAIL.finditer(text):
            host = match.group(0).rsplit("@", 1)[-1].lower()
            if host.endswith(_STORAGE_HOSTS):
                continue
            findings.append(f"{relative}: email {match.group(0)}")

    assert not findings, "Workspace identity found:\n" + "\n".join(findings)
