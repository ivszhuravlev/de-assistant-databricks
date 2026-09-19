"""Observer tables: pipeline_run, layer_run, data_quality, run_errors."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

PIPELINE_RUN = "pipeline_run"
LAYER_RUN = "layer_run"
DATA_QUALITY = "data_quality"
RUN_ERRORS = "run_errors"


def metadata_table(paths, name: str) -> str:
    return f"{paths.ops_schema}.{name}"


def pipeline_run_row(
    *,
    pipeline_run_id: str,
    pipeline_name: str,
    status: str,
    started_at: str | None = None,
    ended_at: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": pipeline_name,
        "status": status,
        "started_at": started_at or now,
        "ended_at": ended_at or "",
    }


def layer_run_row(
    *,
    pipeline_run_id: str,
    layer: str,
    table_name: str,
    row_count: int,
    status: str,
) -> dict[str, Any]:
    return {
        "pipeline_run_id": pipeline_run_id,
        "layer": layer,
        "table_name": table_name,
        "row_count": int(row_count),
        "status": status,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


def data_quality_row(
    *,
    pipeline_run_id: str,
    layer: str,
    table_name: str,
    check_name: str,
    passed: bool,
    actual: str = "",
    detail: str = "",
) -> dict[str, Any]:
    return {
        "pipeline_run_id": pipeline_run_id,
        "layer": layer,
        "table_name": table_name,
        "check_name": check_name,
        "passed": bool(passed),
        "actual": actual,
        "detail": detail,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }


METADATA_COLUMNS = {
    PIPELINE_RUN: (
        "pipeline_run_id",
        "pipeline_name",
        "status",
        "started_at",
        "ended_at",
    ),
    LAYER_RUN: (
        "pipeline_run_id",
        "layer",
        "table_name",
        "row_count",
        "status",
        "recorded_at",
    ),
    DATA_QUALITY: (
        "pipeline_run_id",
        "layer",
        "table_name",
        "check_name",
        "passed",
        "actual",
        "detail",
        "recorded_at",
    ),
}

_METADATA_HELPERS = {
    PIPELINE_RUN: "pipeline_run_row",
    LAYER_RUN: "layer_run_row",
    DATA_QUALITY: "data_quality_row",
}

_METADATA_MARKERS = {
    PIPELINE_RUN: "pipeline_name",
    LAYER_RUN: "row_count",
    DATA_QUALITY: "check_name",
}


def align_metadata_rows(table_name: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Project rows onto the observer schema. Wrong helper → TypeError."""
    columns = METADATA_COLUMNS.get(table_name)
    if columns is None:
        raise ValueError(f"Unknown observer table: {table_name}")
    helper = _METADATA_HELPERS[table_name]
    marker = _METADATA_MARKERS[table_name]
    aligned: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise TypeError(f"append_metadata_rows({table_name}) requires dict rows from {helper}()")
        if marker not in row:
            raise TypeError(
                f"append_metadata_rows({table_name}) got a row without {marker!r}. "
                f"Build rows with {helper}(); do not pass layer_run rows to data_quality "
                "or data_quality rows to layer_run."
            )
        aligned.append({column: row.get(column) for column in columns})
    return aligned


def append_metadata_rows(spark, paths, table_name: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return metadata_table(paths, table_name)
    aligned = align_metadata_rows(table_name, rows)
    location = paths.ops_delta(table_name)
    fqtn = metadata_table(paths, table_name)
    spark.createDataFrame(aligned).write.format("delta").mode("append").save(location)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {paths.ops_schema}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fqtn} USING DELTA LOCATION '{location}'")
    spark.sql(f"REFRESH TABLE {fqtn}")
    return fqtn
