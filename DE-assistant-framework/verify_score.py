"""Operator-only generated-vs-eval score. Not used by the generator."""

from __future__ import annotations

from typing import Any

EXPECTED_COUNTS = {
    "bronze.yellow_tripdata": 1369765,
    "bronze.green_tripdata": 76518,
    "bronze.taxi_zone_lookup": 265,
    "silver.trips": 1297232,
    "silver.rejected_trips": 134399,
    "gold.dim_zones": 265,
    "gold.fct_trips": 1297232,
    "gold.fct_monthly_zone_revenue": 495,
}

TABLES = tuple(tuple(key.split(".", 1)) for key in EXPECTED_COUNTS)


def expected_match(eval_counts: dict[str, Any], generated_counts: dict[str, Any]) -> dict[str, bool]:
    return {
        key: generated_counts.get(key) == eval_counts.get(key) == expected
        for key, expected in EXPECTED_COUNTS.items()
    }


def eval_untouched(eval_counts: dict[str, Any]) -> bool:
    return all(eval_counts.get(key) == expected for key, expected in EXPECTED_COUNTS.items())


def same_as_eval(eval_counts: dict[str, Any], generated_counts: dict[str, Any]) -> bool:
    return all(
        generated_counts.get(key) is not None and generated_counts.get(key) == eval_counts.get(key)
        for key in EXPECTED_COUNTS
    )


def collect_failures(result: dict[str, Any]) -> list[str]:
    """Return machine-readable fail reasons. Empty means the score passed."""
    failures: list[str] = []
    if not result.get("same_as_eval"):
        failures.append("same_as_eval")
    match = result.get("expected_match") or {}
    if not match or not all(match.values()):
        failures.append("expected_match")
    if not result.get("eval_untouched"):
        failures.append("eval_untouched")
    for key in (
        "silver_duplicate_groups",
        "invalid_dead_letter_rows",
        "missing_reject_reasons",
        "monthly_duplicate_groups",
        "monthly_mismatches",
    ):
        value = result.get(key)
        if value:
            failures.append(key)
    if int(result.get("pipeline_rows") or 0) < 1:
        failures.append("pipeline_rows")
    if int(result.get("layer_rows") or 0) < len(EXPECTED_COUNTS):
        failures.append("layer_rows")
    if int(result.get("quality_rows") or 0) < len(EXPECTED_COUNTS):
        failures.append("quality_rows")
    if result.get("idempotency_passed") is False:
        failures.append("idempotency_passed")
    if result.get("missing_generated_tables"):
        failures.append("missing_generated_tables")
    return failures
