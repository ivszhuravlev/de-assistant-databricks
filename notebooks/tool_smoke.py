# Databricks notebook source
"""Live Spark smoke test for every read-only generator tool."""

# COMMAND ----------

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("sas", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from config import GeneratorConfig
from pipeline_helpers import configure_adls_from_secret, write_run_log
from placeholders import notify
from tools import make_tools

config = GeneratorConfig.load(repo_root / "config" / "example.json")
if (config.raw_backend or "adls") == "adls":
    configure_adls_from_secret(spark, dbutils, sas_token=dbutils.widgets.get("sas").strip() or None)
tools, paths = make_tools(config, repo_root, spark=spark)
results = []


def run_tool(name, arguments, parked_is_ok=False):
    try:
        raw = tools.call(name, arguments)
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = raw
        failed = isinstance(parsed, dict) and bool(parsed.get("error"))
        if parked_is_ok:
            failed = not (
                isinstance(parsed, dict) and parsed.get("status") == "parked"
            )
        snippet = json.dumps(parsed, sort_keys=True, default=str)
        results.append(
            {
                "tool": name,
                "args": arguments,
                "verdict": "fail" if failed else "ok",
                "result": snippet[:500],
            }
        )
        return parsed
    except Exception as exc:
        results.append(
            {
                "tool": name,
                "args": arguments,
                "verdict": "fail",
                "result": f"{type(exc).__name__}: {exc}"[:500],
            }
        )
        return None


run_tool("list_sources", {})
run_tool("read_source", {"source_id": "pipeline_brief"})
run_tool("read_contract", {})
run_tool("list_raw_files", {})

yellow_path = paths.raw("yellow", "yellow_tripdata_2021-01.csv.gz")
run_tool("peek_raw", {"path": yellow_path, "n": 2})
run_tool("profile_column", {"target": yellow_path, "column": "VendorID"})
run_tool(
    "get_distinct_values",
    {"target": yellow_path, "column": "VendorID", "contains": "1", "limit": 20},
)
run_tool(
    "get_distinct_values",
    {
        "target": "gold.fct_monthly_zone_revenue",
        "column": "pickup_zone",
        "contains": "Airport",
        "limit": 20,
    },
)
run_tool("peek_table", {"layer": "gold", "table": "fct_trips", "n": 2})

error_table = run_tool(
    "peek_table",
    {"layer": "de_assist", "table": "run_errors", "n": 10},
)
error_rows = error_table.get("sample", []) if isinstance(error_table, dict) else []
latest_target = next(
    (row for row in error_rows if str(row.get("status")).upper() == "FAILED"),
    error_rows[0] if error_rows else {},
)
error_query = (
    latest_target.get("pipeline")
    or latest_target.get("layer")
    or latest_target.get("error_type")
    or "failed"
)
error_hits = run_tool(
    "search_previous_errors",
    {"query": error_query, "limit": 10},
)
if isinstance(error_hits, list) and error_hits:
    latest_target = error_hits[0]
run_tool(
    "get_last_error",
    {
        "pipeline": latest_target.get("pipeline", "error_search_smoke"),
        "layer": latest_target.get("layer", "gold"),
    },
)

smoke_run_id = "tool-smoke-" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
write_run_log(
    spark,
    paths,
    f"generator/{smoke_run_id}/bronze.json",
    {"event": "tool_smoke", "pipeline_run_id": smoke_run_id, "status": "SUCCESS"},
)
run_tool(
    "read_log",
    {"run_id": smoke_run_id, "layer": "bronze", "tail_lines": 5},
)
run_tool("list_successful_runs", {"limit": 3})
run_tool(
    "read_tested_pipeline",
    {"pipeline_id": "tool_smoke_sample", "start_line": 0, "num_lines": 4},
)
run_tool("semantic_search", {"query": "taxi"}, parked_is_ok=True)
notify(
    {"status": "SUCCESS", "event": "tool_smoke_notify"},
    spark=spark,
    paths=paths,
    log_name=f"notify/{smoke_run_id}.json",
)
results.append({"tool": "notify", "args": {}, "verdict": "ok", "result": "stub"})

report = {
    "summary": {
        "ok": sum(item["verdict"] == "ok" for item in results),
        "fail": sum(item["verdict"] == "fail" for item in results),
    },
    "results": results,
}
result_json = json.dumps(report, sort_keys=True, default=str)
print(result_json)
dbutils.notebook.exit(result_json)
