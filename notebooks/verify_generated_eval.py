# Databricks notebook source
"""Compare generated gen_* tables to the hand-built eval tables. Do not overwrite eval."""

import json
import sys
from pathlib import Path

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("repo_root", "")
dbutils.widgets.text(
    "count_snapshot_path",
    "dbfs:/de-assist-databricks/metadata/generated_eval_previous_counts.json",
)

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))
from pipeline_helpers import load_paths
from verify_score import (
    EXPECTED_COUNTS,
    TABLES,
    collect_failures,
    eval_untouched as score_eval_untouched,
    expected_match as score_expected_match,
    same_as_eval as score_same_as_eval,
)

pipeline_run_id = dbutils.widgets.get("pipeline_run_id").strip()
count_snapshot_path = dbutils.widgets.get("count_snapshot_path")
eval_paths = load_paths("adls", output_space="eval")
gen_paths = load_paths("adls", output_space="generated")
if not pipeline_run_id:
    try:
        latest = (
            spark.read.format("delta")
            .load("dbfs:/de-assist-databricks/metadata/generator_runs")
            .orderBy("generated_at", ascending=False)
            .limit(1)
            .first()
        )
        if latest:
            pipeline_run_id = str(latest["pipeline_run_id"])
    except Exception:
        pass


def table_count(paths, layer, table):
    try:
        return spark.table(paths.table(layer, table)).count()
    except Exception:
        return None


def counts_for(paths):
    return {f"{layer}.{table}": table_count(paths, layer, table) for layer, table in TABLES}


def safe_sql_count(sql, default=None):
    try:
        return spark.sql(sql).first()[0]
    except Exception:
        return default


eval_counts = counts_for(eval_paths)
generated_counts = counts_for(gen_paths)
missing_generated_tables = [
    name for name, count in generated_counts.items() if count is None
]
expected_match = score_expected_match(eval_counts, generated_counts)
same_as_eval = score_same_as_eval(eval_counts, generated_counts)
eval_untouched = score_eval_untouched(eval_counts)

silver_trips = gen_paths.table("silver", "trips")
rejected = gen_paths.table("silver", "rejected_trips")
facts = gen_paths.table("gold", "fct_trips")
monthly = gen_paths.table("gold", "fct_monthly_zone_revenue")

silver_duplicate_groups = safe_sql_count(
    f"""
    SELECT count(*) AS groups FROM (
      SELECT vendor_id, pickup_datetime, pickup_location_id, service_type
      FROM {silver_trips}
      GROUP BY vendor_id, pickup_datetime, pickup_location_id, service_type
      HAVING count(*) > 1
    )
    """,
    default=-1,
)
invalid_dead_letter_rows = safe_sql_count(
    f"SELECT count(*) AS rows FROM {rejected} WHERE vendor_id IS NOT NULL AND pickup_datetime IS NOT NULL",
    default=-1,
)
missing_reject_reasons = safe_sql_count(
    f"SELECT count(*) AS rows FROM {rejected} WHERE reject_reason IS NULL",
    default=-1,
)
monthly_duplicate_groups = safe_sql_count(
    f"""
    SELECT count(*) AS groups FROM (
      SELECT pickup_zone, revenue_month, service_type
      FROM {monthly}
      GROUP BY pickup_zone, revenue_month, service_type
      HAVING count(*) > 1
    )
    """,
    default=-1,
)
monthly_mismatches = safe_sql_count(
    f"""
    WITH facts AS (
      SELECT
        coalesce(pickup_zone, 'Unknown Zone') AS pickup_zone,
        date_trunc('month', pickup_datetime) AS revenue_month,
        service_type,
        sum(total_amount) AS total_amount,
        count(trip_id) AS trip_count
      FROM {facts}
      GROUP BY 1, 2, 3
    )
    SELECT count(*) AS groups
    FROM facts f
    FULL JOIN {monthly} m
      USING (pickup_zone, revenue_month, service_type)
    WHERE NOT (f.total_amount <=> m.revenue_monthly_total_amount)
       OR NOT (f.trip_count <=> m.total_monthly_trips)
    """,
    default=-1,
)

pipeline_rows = 0
layer_rows = 0
quality_rows = 0
if pipeline_run_id:
    pipeline_rows = spark.sql(
        f"SELECT count(*) AS rows FROM de_assist.pipeline_run WHERE pipeline_run_id = '{pipeline_run_id}'"
    ).first()["rows"]
    layer_rows = spark.sql(
        f"SELECT count(*) AS rows FROM de_assist.layer_run WHERE pipeline_run_id = '{pipeline_run_id}'"
    ).first()["rows"]
    quality_rows = spark.sql(
        f"SELECT count(*) AS rows FROM de_assist.data_quality WHERE pipeline_run_id = '{pipeline_run_id}'"
    ).first()["rows"]

previous_snapshot = None
try:
    previous_snapshot = json.loads(dbutils.fs.head(count_snapshot_path, 100000))
except Exception:
    pass
previous_counts = (previous_snapshot or {}).get("counts")
previous_pipeline_run_id = (previous_snapshot or {}).get("pipeline_run_id")
previous_complete = bool(
    previous_counts
    and all(previous_counts.get(key) == expected for key, expected in EXPECTED_COUNTS.items())
)
count_deltas = (
    {
        table: (generated_counts[table] or 0) - int(previous_counts.get(table, 0) or 0)
        for table in generated_counts
    }
    if previous_complete
    else None
)
if count_deltas is None:
    idempotency_passed = None
elif previous_pipeline_run_id == pipeline_run_id:
    idempotency_passed = None
else:
    idempotency_passed = all(delta == 0 for delta in count_deltas.values())

result = {
    "eval_counts": eval_counts,
    "generated_counts": generated_counts,
    "missing_generated_tables": missing_generated_tables,
    "generated_schemas": {
        "bronze": gen_paths.layer_schema("bronze"),
        "silver": gen_paths.layer_schema("silver"),
        "gold": gen_paths.layer_schema("gold"),
    },
    "eval_delta_root": eval_paths.delta_root,
    "generated_delta_root": gen_paths.delta_root,
    "same_as_eval": same_as_eval,
    "expected_match": expected_match,
    "eval_untouched": eval_untouched,
    "silver_duplicate_groups": silver_duplicate_groups,
    "invalid_dead_letter_rows": invalid_dead_letter_rows,
    "missing_reject_reasons": missing_reject_reasons,
    "monthly_duplicate_groups": monthly_duplicate_groups,
    "monthly_mismatches": monthly_mismatches,
    "pipeline_rows": pipeline_rows,
    "layer_rows": layer_rows,
    "quality_rows": quality_rows,
    "idempotency_passed": idempotency_passed,
    "count_deltas": count_deltas,
    "pipeline_run_id": pipeline_run_id,
}
failures = collect_failures(result)
result["failures"] = failures
print(json.dumps(result, indent=2, sort_keys=True, default=str))
if failures:
    raise RuntimeError(json.dumps(result, sort_keys=True, default=str)[:2000])
dbutils.fs.put(
    count_snapshot_path,
    json.dumps({"pipeline_run_id": pipeline_run_id, "counts": generated_counts}, sort_keys=True),
    overwrite=True,
)
dbutils.notebook.exit(json.dumps(result, sort_keys=True, default=str))
