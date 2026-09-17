# Databricks notebook source
"""Query row counts from eval or generated taxi tables."""

import json
import sys
from pathlib import Path

dbutils.widgets.text("backend", "adls")
dbutils.widgets.text("output_space", "eval")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from pipeline_helpers import load_paths

paths = load_paths(
    dbutils.widgets.get("backend"),
    output_space=dbutils.widgets.get("output_space"),
)
tables = [
    ("bronze", "yellow_tripdata"),
    ("bronze", "green_tripdata"),
    ("bronze", "taxi_zone_lookup"),
    ("silver", "rejected_trips"),
    ("silver", "trips"),
    ("gold", "dim_zones"),
    ("gold", "fct_trips"),
    ("gold", "fct_monthly_zone_revenue"),
]
counts = {
    f"{paths.layer_schema(layer)}.{table}": spark.table(paths.table(layer, table)).count()
    for layer, table in tables
}
result = {
    "output_space": paths.output_space,
    "delta_root": paths.delta_root,
    "counts": counts,
}
print(json.dumps(result, indent=2, sort_keys=True))
dbutils.notebook.exit(json.dumps(result, sort_keys=True))
