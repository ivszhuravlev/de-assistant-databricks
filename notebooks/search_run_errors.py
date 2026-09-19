# Databricks notebook source
"""Search previous failed runs by keyword."""

# COMMAND ----------

import json
import sys
from pathlib import Path

dbutils.widgets.text("query", "LocationID")
dbutils.widgets.text("limit", "10")
dbutils.widgets.text("persist_probe", "false")
dbutils.widgets.text("backend", "dbfs")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from pipeline_helpers import ensure_schema, load_paths
from retrieval import error_record, persist_error, search_run_errors

paths = load_paths(dbutils.widgets.get("backend"))
ensure_schema(spark, paths)

query = dbutils.widgets.get("query")
limit = int(dbutils.widgets.get("limit") or "10")

if dbutils.widgets.get("persist_probe").strip().lower() == "true":
    persist_error(
        spark,
        paths,
        error_record(
            pipeline="error_search_probe",
            layer="gold",
            table="bronze_taxi_zone_lookup",
            error_type="AnalysisException",
            error="Column LocationID not found. Available columns: [locationid, borough, zone, service_zone]",
            diagnostics={"source": "search_run_errors_probe"},
        ),
    )
hits = search_run_errors(spark, paths, query, limit=limit)
print(
    json.dumps(
        {
            "backend": paths.backend,
            "catalog": paths.catalog,
            "error_table": paths.error_table(),
            "error_root": paths.error_root,
            "query": query,
            "hit_count": len(hits),
            "hits": hits,
        },
        indent=2,
        default=str,
    )
)
if not hits:
    raise RuntimeError(f"Keyword search returned no hits for {query!r}")
dbutils.notebook.exit(json.dumps({"hit_count": len(hits), "error_table": paths.error_table()}))
