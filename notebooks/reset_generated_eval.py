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
sys.path.insert(0, str(repo_root))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from evals.fresh_retail.score import TABLES as RETAIL_TABLES
from evals.taxi.score import TABLES as TAXI_TABLES
from evals.transaction_cat.score import TABLES as TX_TABLES
from pipeline_helpers import configure_session_spark, load_paths

configure_session_spark(spark)

for pipeline, tables in (
    ("taxi", TAXI_TABLES),
    ("transaction_cat", TX_TABLES),
    ("fresh_retail", RETAIL_TABLES),
):
    paths = load_paths("adls", output_space="generated", pipeline=pipeline)
    for layer, table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {paths.table(layer, table)}")
    dbutils.fs.rm(paths.delta_root, True)

dbutils.notebook.exit("generated schemas reset; eval tables untouched")
