from evals.taxi.score import EXPECTED_COUNTS, collect_failures, expected_match


def test_taxi_score_fails_on_unscored_holes():
    eval_counts = dict(EXPECTED_COUNTS)
    generated = dict(eval_counts)
    generated["gold.fct_monthly_zone_revenue"] = 1
    result = {
        "same_as_eval": False,
        "expected_match": expected_match(eval_counts, generated),
        "eval_untouched": True,
        "silver_duplicate_groups": 0,
        "invalid_dead_letter_rows": 0,
        "missing_reject_reasons": 2,
        "monthly_duplicate_groups": 0,
        "monthly_mismatches": 3,
        "pipeline_rows": 0,
        "layer_rows": 2,
        "quality_rows": 0,
        "idempotency_passed": False,
        "missing_generated_tables": [],
    }
    failures = collect_failures(result)
    for key in (
        "same_as_eval",
        "expected_match",
        "missing_reject_reasons",
        "monthly_mismatches",
        "pipeline_rows",
        "layer_rows",
        "quality_rows",
        "idempotency_passed",
    ):
        assert key in failures


def test_taxi_score_passes_complete_first_run():
    result = {
        "same_as_eval": True,
        "expected_match": expected_match(EXPECTED_COUNTS, EXPECTED_COUNTS),
        "eval_untouched": True,
        "silver_duplicate_groups": 0,
        "invalid_dead_letter_rows": 0,
        "missing_reject_reasons": 0,
        "monthly_duplicate_groups": 0,
        "monthly_mismatches": 0,
        "pipeline_rows": 1,
        "layer_rows": 8,
        "quality_rows": 8,
        "idempotency_passed": None,
        "missing_generated_tables": [],
    }
    assert collect_failures(result) == []
