# Databricks notebook source
"""Remove generated Hive/Delta only. Eval bronze/silver/gold must stay."""

import sys
from pathlib import Path

dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from pipeline_helpers import configure_session_spark, load_paths

configure_session_spark(spark)
paths = load_paths("adls", output_space="generated")
tables = {
    "bronze": ("yellow_tripdata", "green_tripdata", "taxi_zone_lookup"),
    "silver": ("trips", "rejected_trips"),
    "gold": ("dim_zones", "fct_trips", "fct_monthly_zone_revenue"),
}

for layer, names in tables.items():
    for table in names:
        spark.sql(f"DROP TABLE IF EXISTS {paths.table(layer, table)}")

dbutils.fs.rm(paths.delta_root, True)
dbutils.notebook.exit("generated schemas reset; eval tables untouched")
