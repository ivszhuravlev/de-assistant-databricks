from pathlib import Path

from evals import EVAL_IDS
from evals.fresh_retail.score import EXPECTED_COUNTS as RETAIL_COUNTS
from evals.fresh_retail.score import collect_failures as retail_failures
from evals.fresh_retail.score import expected_match as retail_match
from evals.taxi.score import EXPECTED_COUNTS as TAXI_COUNTS
from evals.transaction_cat.score import EXPECTED_COUNTS as TX_COUNTS
from evals.transaction_cat.score import collect_failures as tx_failures
from evals.transaction_cat.score import expected_match as tx_match

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = ("README.md", "brief.md", "spec.json", "score.py", "slice.py")


def test_three_eval_packages_are_homogeneous():
    assert EVAL_IDS == ("taxi", "fresh_retail", "transaction_cat")
    for name in EVAL_IDS:
        package = ROOT / "evals" / name
        missing = [item for item in REQUIRED if not (package / item).is_file()]
        assert missing == [], f"{name} missing {missing}"


def test_retail_score_uses_hub_bronze_counts():
    assert RETAIL_COUNTS["bronze.daily_sales_train"] == 4_500_000
    assert RETAIL_COUNTS["bronze.daily_sales_eval"] == 350_000
    result = {
        "same_as_eval": True,
        "expected_match": retail_match(RETAIL_COUNTS, RETAIL_COUNTS),
        "eval_untouched": True,
        "invalid_dead_letter_rows": 0,
        "missing_reject_reasons": 0,
        "store_daily_mismatches": 0,
        "category_daily_mismatches": 0,
        "pipeline_rows": 1,
        "layer_rows": len(RETAIL_COUNTS),
        "quality_rows": len(RETAIL_COUNTS),
        "idempotency_passed": None,
        "missing_generated_tables": [],
    }
    assert retail_failures(result) == []


def test_transaction_score_uses_landed_bronze_counts():
    assert TX_COUNTS == {
        "bronze.transactions": 1_000_000,
        "bronze.category_taxonomy": 10,
        "bronze.country_currency": 5,
    }
    result = {
        "same_as_eval": True,
        "expected_match": tx_match(TX_COUNTS, TX_COUNTS),
        "eval_untouched": True,
        "invalid_dead_letter_rows": 0,
        "missing_reject_reasons": 0,
        "category_country_mismatches": 0,
        "pipeline_rows": 1,
        "layer_rows": len(TX_COUNTS),
        "quality_rows": len(TX_COUNTS),
        "idempotency_passed": None,
        "missing_generated_tables": [],
    }
    assert tx_failures(result) == []


def test_reset_generated_notebook_covers_all_eval_pipelines():
    text = (ROOT / "notebooks" / "reset_generated_eval.py").read_text(encoding="utf-8")
    assert "from evals.taxi.score import TABLES" in text
    assert "from evals.transaction_cat.score import TABLES" in text
    assert "from evals.fresh_retail.score import TABLES" in text
    assert 'sys.path.insert(0, str(repo_root))' in text
    assert 'load_paths("adls", output_space="generated", pipeline=pipeline)' in text
    assert "paths.delta_root" in text


def test_taxi_expected_counts_unchanged():
    assert TAXI_COUNTS["bronze.yellow_tripdata"] == 1_369_765
    assert TAXI_COUNTS["gold.fct_monthly_zone_revenue"] == 495
