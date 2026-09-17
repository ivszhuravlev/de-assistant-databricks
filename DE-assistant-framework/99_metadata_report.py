# Databricks notebook source
"""Print observer tables. Not a dashboard."""

# COMMAND ----------

import json
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
report = {}
for name in (PIPELINE_RUN, LAYER_RUN, DATA_QUALITY, RUN_ERRORS):
    fqtn = metadata_table(paths, name)
    try:
        df = spark.table(fqtn)
        report[fqtn] = df.count()
        print(fqtn)
        df.show(20, truncate=False)
    except Exception as exc:
        report[fqtn] = str(exc)
        print({"table": fqtn, "error": str(exc)})

print(json.dumps(report, indent=2))
dbutils.notebook.exit(json.dumps(report, sort_keys=True))
