"""Resolve repository paths without embedding workspace identities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _widget(dbutils: Any, name: str) -> str:
    try:
        return str(dbutils.widgets.get(name)).strip()
    except Exception:
        return ""


def _option_value(option: Any) -> str:
    try:
        return str(option.get()).strip()
    except Exception:
        return ""


def _context_paths(dbutils: Any) -> list[str]:
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        return []

    values: list[str] = []
    try:
        tags = context.tags()
        for key in ("workspace.file_path", "notebook_path", "notebookPath"):
            value = _option_value(tags.get(key))
            if value:
                values.append(value)
    except Exception:
        pass
    try:
        value = _option_value(context.notebookPath())
        if value:
            values.append(value)
    except Exception:
        pass
    return values


def _repo_candidate(value: str) -> Path | None:
    if not value:
        return None
    candidate = Path(value).expanduser()
    if candidate.name.endswith(".py"):
        candidate = candidate.parent
    for path in (candidate, *candidate.parents):
        if (path / "DE-assistant-framework").is_dir():
            return path
    if candidate.name in {"notebooks", "DE-assistant-framework"}:
        return candidate.parent
    return candidate


def default_repo_root(dbutils: Any) -> str:
    """Resolve repo_root from widgets, context, then the current directory."""
    repo_root = _widget(dbutils, "repo_root")
    if repo_root:
        return str(Path(repo_root).expanduser())

    config_json = _widget(dbutils, "config_json")
    if config_json:
        try:
            configured = str(json.loads(config_json).get("repo_root", "")).strip()
        except (AttributeError, TypeError, ValueError):
            configured = ""
        if configured:
            return str(Path(configured).expanduser())

    for value in _context_paths(dbutils):
        candidate = _repo_candidate(value)
        if candidate is not None:
            return str(candidate)

    candidate = _repo_candidate(str(Path.cwd()))
    return str(candidate or Path.cwd())
