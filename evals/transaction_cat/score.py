"""Transaction-cat generated-vs-eval score. Operator-only; the generator must not import this."""

from __future__ import annotations

from typing import Any

from evals.common import (
    collect_failures as _collect_failures,
    eval_untouched as _eval_untouched,
    expected_match as _expected_match,
    same_as_eval as _same_as_eval,
    tables_from_counts,
)

PIPELINE = "transaction_cat"

EXPECTED_COUNTS = {
    "bronze.transactions": 1_000_000,
    "bronze.category_taxonomy": 10,
    "bronze.country_currency": 5,
}

TABLES = tables_from_counts(EXPECTED_COUNTS) + (
    ("silver", "transactions"),
    ("silver", "rejected_transactions"),
    ("gold", "dim_category"),
    ("gold", "dim_geo"),
    ("gold", "fct_transactions"),
    ("gold", "fct_category_country"),
)

_EXTRA = (
    "invalid_dead_letter_rows",
    "missing_reject_reasons",
    "category_country_mismatches",
)


def expected_match(eval_counts: dict[str, Any], generated_counts: dict[str, Any]) -> dict[str, bool]:
    return _expected_match(EXPECTED_COUNTS, eval_counts, generated_counts)


def eval_untouched(eval_counts: dict[str, Any]) -> bool:
    return _eval_untouched(EXPECTED_COUNTS, eval_counts)


def same_as_eval(eval_counts: dict[str, Any], generated_counts: dict[str, Any]) -> bool:
    return _same_as_eval(EXPECTED_COUNTS, eval_counts, generated_counts)


def collect_failures(result: dict[str, Any]) -> list[str]:
    return _collect_failures(result, expected=EXPECTED_COUNTS, extra_keys=_EXTRA)
