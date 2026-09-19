"""Keyword search over previous failed-run errors."""

from __future__ import annotations

import json
import difflib
import re
from datetime import datetime, timezone
from typing import Any, Iterable

TOKEN_RE = re.compile(r"[a-z0-9_]+", re.IGNORECASE)
_MISSING = (
    "path does not exist",
    "table or view not found",
    "cannot be found",
    "filenotfound",
    "is not a delta table",
    "[delta_missing",
    "nodirectory",
)

SEARCH_FIELDS = (
    "error_type",
    "error",
    "diagnostics",
    "pipeline",
    "layer",
    "table",
    "pipeline_run_id",
)


class ErrorStoreError(RuntimeError):
    """The error log exists but cannot be read."""


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text or "") if len(token) > 1]


def haystack(record: dict[str, Any]) -> str:
    return " ".join(str(record.get(field) or "") for field in SEARCH_FIELDS).lower()


def keyword_search(
    records: Iterable[dict[str, Any]],
    query: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    tokens = tokenize(query)
    if not tokens:
        return []
    scored: list[tuple[int, str, dict[str, Any]]] = []
    for record in records:
        if str(record.get("status") or "FAILED").upper() != "FAILED":
            continue
        found = set(tokenize(haystack(record)))
        overlap = sum(1 for token in tokens if token in found)
        if not overlap:
            continue
        scored.append((overlap, str(record.get("created_at") or ""), record))
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [record for _, _, record in scored[: max(limit, 0)]]


def error_record(
    *,
    pipeline: str,
    error: str,
    error_type: str = "Error",
    layer: str | None = None,
    table: str | None = None,
    pipeline_run_id: str | None = None,
    run_id: str | None = None,
    diagnostics: dict[str, Any] | None = None,
    status: str = "FAILED",
) -> dict[str, Any]:
    key = pipeline_run_id or run_id or ""
    return {
        "pipeline_run_id": key,
        "pipeline": pipeline,
        "layer": layer or "",
        "table": table or "",
        "status": status,
        "error_type": error_type,
        "error": error,
        "diagnostics": json.dumps(diagnostics or {}, sort_keys=True),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def persist_error(spark, paths, record: dict[str, Any]) -> str:
    location = paths.error_root
    fqtn = paths.error_table()
    spark.createDataFrame([record]).write.format("delta").mode("append").save(location)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {paths.ops_schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fqtn} USING DELTA LOCATION '{location}'")
    try:
        spark.sql(f"REFRESH TABLE {fqtn}")
    except Exception as exc:
        print(
            json.dumps(
                {
                    "event": "error_table_refresh_failed",
                    "table": fqtn,
                    "delta_location": location,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                sort_keys=True,
            )
        )
    return fqtn


def _diagnostics(record: dict[str, Any]) -> dict[str, Any]:
    value = record.get("diagnostics") or {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def _attempt(record: dict[str, Any]) -> int:
    try:
        return int(_diagnostics(record).get("attempt", 0))
    except (TypeError, ValueError):
        return 0


def _missing(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in _MISSING)


def load_error_records(spark, paths) -> list[dict[str, Any]]:
    fqtn = paths.error_table()
    try:
        rows = spark.table(fqtn).collect()
    except Exception as table_exc:
        if not _missing(table_exc):
            raise ErrorStoreError(f"Cannot read {fqtn}: {table_exc}") from table_exc
        try:
            rows = spark.read.format("delta").load(paths.error_root).collect()
        except Exception as path_exc:
            if _missing(path_exc):
                return []
            raise ErrorStoreError(f"Cannot read {paths.error_root}: {path_exc}") from path_exc
    return [row.asDict(recursive=True) for row in rows]


def search_run_errors(spark, paths, query: str, limit: int = 10) -> list[dict[str, Any]]:
    failed = [row for row in load_error_records(spark, paths) if row.get("status") == "FAILED"]
    return keyword_search(failed, query, limit=limit)


def get_last_error(spark, paths, pipeline: str, layer: str) -> dict[str, Any] | None:
    """Newest failed attempt. Prefer this pipeline+layer, then this layer, then any failure."""
    failed = [
        row for row in load_error_records(spark, paths) if row.get("status") == "FAILED"
    ]
    return _latest_failed(failed, pipeline, layer)


def _latest_failed(
    records: list[dict[str, Any]], pipeline: str, layer: str
) -> dict[str, Any] | None:
    failed = [
        row
        for row in records
        if str(row.get("status") or "FAILED").upper() == "FAILED"
    ]
    if not failed:
        return None
    for matchers in (
        lambda row: str(row.get("pipeline") or "") == pipeline
        and str(row.get("layer") or "") == layer,
        lambda row: str(row.get("layer") or "") == layer,
        lambda _row: True,
    ):
        matches = [row for row in failed if matchers(row)]
        if matches:
            return max(matches, key=lambda row: str(row.get("created_at") or ""))
    return None


def find_past_fix(
    spark,
    paths,
    error_message: str,
    layer: str,
    *,
    limit_lines: int = 120,
) -> str | None:
    """Find a lexical error match whose next attempt passed and return its code diff."""
    records = [row for row in load_error_records(spark, paths) if row.get("layer") == layer]
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in records:
        grouped.setdefault(str(row.get("pipeline_run_id") or ""), []).append(row)

    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for rows in grouped.values():
        by_attempt = {
            _attempt(row): row
            for row in rows
            if _attempt(row) > 0
        }
        for attempt, failed in by_attempt.items():
            passed = by_attempt.get(attempt + 1)
            if failed.get("status") == "FAILED" and passed and passed.get("status") == "PASSED":
                pairs.append((failed, passed))

    query_tokens = set(tokenize(error_message))
    scored = [
        (len(query_tokens & set(tokenize(str(failed.get("error") or "")))), failed, passed)
        for failed, passed in pairs
    ]
    scored = [item for item in scored if item[0] > 0]
    if not scored:
        return None
    _, failed, passed = max(scored, key=lambda item: item[0])
    failed_code = str(_diagnostics(failed).get("generated_code") or "")
    fixed_code = str(_diagnostics(passed).get("generated_code") or "")
    if not failed_code or not fixed_code:
        return None
    diff = list(
        difflib.unified_diff(
            failed_code.splitlines(),
            fixed_code.splitlines(),
            fromfile="failed_attempt",
            tofile="passed_attempt",
            lineterm="",
            n=2,
        )
    )
    if not diff:
        return None
    clipped = diff[:limit_lines]
    suffix = "\n... (diff truncated)" if len(diff) > limit_lines else ""
    return (
        "A similar prior error was followed by a passed retry. Use this proven fixing diff "
        "as guidance and re-check it against current data:\n"
        + "\n".join(clipped)
        + suffix
    )
