"""Shared generated-vs-eval scoring. Operator-only."""

from __future__ import annotations

from typing import Any, Iterable


def tables_from_counts(expected: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(tuple(key.split(".", 1)) for key in expected)


def expected_match(
    expected: dict[str, Any],
    eval_counts: dict[str, Any],
    generated_counts: dict[str, Any],
) -> dict[str, bool]:
    return {
        key: generated_counts.get(key) == eval_counts.get(key) == value
        for key, value in expected.items()
    }


def eval_untouched(expected: dict[str, Any], eval_counts: dict[str, Any]) -> bool:
    return all(eval_counts.get(key) == value for key, value in expected.items())


def same_as_eval(
    expected: dict[str, Any],
    eval_counts: dict[str, Any],
    generated_counts: dict[str, Any],
) -> bool:
    return all(
        generated_counts.get(key) is not None and generated_counts.get(key) == eval_counts.get(key)
        for key in expected
    )


def collect_failures(
    result: dict[str, Any],
    *,
    expected: dict[str, Any],
    extra_keys: Iterable[str] = (),
) -> list[str]:
    failures: list[str] = []
    if not result.get("same_as_eval"):
        failures.append("same_as_eval")
    match = result.get("expected_match") or {}
    if not match or not all(match.values()):
        failures.append("expected_match")
    if not result.get("eval_untouched"):
        failures.append("eval_untouched")
    for key in extra_keys:
        if result.get(key):
            failures.append(key)
    if int(result.get("pipeline_rows") or 0) < 1:
        failures.append("pipeline_rows")
    if int(result.get("layer_rows") or 0) < len(expected):
        failures.append("layer_rows")
    if int(result.get("quality_rows") or 0) < len(expected):
        failures.append("quality_rows")
    if result.get("idempotency_passed") is False:
        failures.append("idempotency_passed")
    if result.get("missing_generated_tables"):
        failures.append("missing_generated_tables")
    return failures
