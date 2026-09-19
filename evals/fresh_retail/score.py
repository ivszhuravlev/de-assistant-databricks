"""Fresh retail generated-vs-eval score. Operator-only; the generator must not import this."""

from __future__ import annotations

from typing import Any

from evals.common import (
    collect_failures as _collect_failures,
    eval_untouched as _eval_untouched,
    expected_match as _expected_match,
    same_as_eval as _same_as_eval,
    tables_from_counts,
)

PIPELINE = "fresh_retail"

EXPECTED_COUNTS = {
    "bronze.daily_sales_train": 4_500_000,
    "bronze.daily_sales_eval": 350_000,
}

TABLES = tables_from_counts(EXPECTED_COUNTS) + (
    ("silver", "daily_sales"),
    ("silver", "rejected_sales"),
    ("silver", "dim_store"),  # (store_id, _split)
    ("silver", "dim_product"),  # (product_id, _split)
    ("gold", "dim_store"),  # (store_id, _split)
    ("gold", "dim_product"),  # (product_id, _split)
    ("gold", "fct_daily_sales"),
    ("gold", "fct_store_daily"),
    ("gold", "fct_category_daily"),
)

_EXTRA = (
    "invalid_dead_letter_rows",
    "missing_reject_reasons",
    "store_daily_mismatches",
    "category_daily_mismatches",
)


def expected_match(eval_counts: dict[str, Any], generated_counts: dict[str, Any]) -> dict[str, bool]:
    return _expected_match(EXPECTED_COUNTS, eval_counts, generated_counts)


def eval_untouched(eval_counts: dict[str, Any]) -> bool:
    return _eval_untouched(EXPECTED_COUNTS, eval_counts)


def same_as_eval(eval_counts: dict[str, Any], generated_counts: dict[str, Any]) -> bool:
    return _same_as_eval(EXPECTED_COUNTS, eval_counts, generated_counts)


def collect_failures(result: dict[str, Any]) -> list[str]:
    return _collect_failures(result, expected=EXPECTED_COUNTS, extra_keys=_EXTRA)
