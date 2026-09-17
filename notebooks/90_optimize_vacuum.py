# Databricks notebook source
"""OPTIMIZE observer and taxi tables. VACUUM uses the default retention."""

# COMMAND ----------

import sys
from pathlib import Path

dbutils.widgets.text("backend", "dbfs")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from metadata import DATA_QUALITY, LAYER_RUN, PIPELINE_RUN, RUN_ERRORS, metadata_table
from pipeline_helpers import load_paths

paths = load_paths(dbutils.widgets.get("backend"))
tables = [
    paths.table("bronze", "yellow_tripdata"),
    paths.table("bronze", "green_tripdata"),
    paths.table("bronze", "taxi_zone_lookup"),
    paths.table("silver", "trips"),
    paths.table("silver", "rejected_trips"),
    paths.table("gold", "dim_zones"),
    paths.table("gold", "fct_trips"),
    paths.table("gold", "fct_monthly_zone_revenue"),
    metadata_table(paths, PIPELINE_RUN),
    metadata_table(paths, LAYER_RUN),
    metadata_table(paths, DATA_QUALITY),
    metadata_table(paths, RUN_ERRORS),
]

for fqtn in tables:
    try:
        spark.sql(f"OPTIMIZE {fqtn}")
        spark.sql(f"VACUUM {fqtn}")
        print({"table": fqtn, "status": "optimized"})
    except Exception as exc:
        print({"table": fqtn, "status": "skipped", "error": str(exc)})
