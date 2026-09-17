# Databricks notebook source
"""Query row counts from transaction or retail eval tables."""

import json
import sys
from pathlib import Path

dbutils.widgets.text("backend", "adls")
dbutils.widgets.text("output_space", "eval")
dbutils.widgets.text("pipeline", "transaction_cat")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from pipeline_helpers import load_paths

pipeline = dbutils.widgets.get("pipeline").strip()
paths = load_paths(
    dbutils.widgets.get("backend"),
    output_space=dbutils.widgets.get("output_space"),
    pipeline=pipeline,
)
TABLES = {
    "transaction_cat": [
        ("bronze", "transactions"),
        ("bronze", "category_taxonomy"),
        ("bronze", "country_currency"),
        ("silver", "rejected_transactions"),
        ("silver", "transactions"),
        ("gold", "dim_category"),
        ("gold", "dim_geo"),
        ("gold", "fct_transactions"),
        ("gold", "fct_category_country"),
    ],
    "fresh_retail": [
        ("bronze", "daily_sales_train"),
        ("bronze", "daily_sales_eval"),
        ("silver", "rejected_sales"),
        ("silver", "daily_sales"),
        ("silver", "dim_store"),
        ("silver", "dim_product"),
        ("gold", "dim_store"),
        ("gold", "dim_product"),
        ("gold", "fct_daily_sales"),
        ("gold", "fct_store_daily"),
        ("gold", "fct_category_daily"),
    ],
}
if pipeline not in TABLES:
    raise ValueError(f"Unsupported pipeline {pipeline!r}; expected {sorted(TABLES)}")
counts = {}
for layer, table in TABLES[pipeline]:
    fqtn = paths.table(layer, table)
    try:
        counts[fqtn] = spark.table(fqtn).count()
    except Exception as exc:
        counts[fqtn] = f"{type(exc).__name__}: {exc}"
result = {
    "pipeline": pipeline,
    "output_space": paths.output_space,
    "raw_root": paths.raw_root,
    "delta_root": paths.delta_root,
    "counts": counts,
}
print(json.dumps(result, indent=2, sort_keys=True, default=str))
dbutils.notebook.exit(json.dumps(result, sort_keys=True, default=str))
