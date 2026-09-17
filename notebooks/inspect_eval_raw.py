# Databricks notebook source
"""List ADLS eval prefixes and peek schemas. SAS never printed."""

# COMMAND ----------

import json
import sys
from pathlib import Path

dbutils.widgets.text("sas", "")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from pipeline_helpers import configure_adls_from_secret

configure_adls_from_secret(spark, dbutils, sas_token=dbutils.widgets.get("sas").strip() or None)

PREFIXES = {
    "transaction_cat": "abfss://raw@exampleaccount.dfs.core.windows.net/transaction_cat",
    "fresh_reatail_net": "abfss://raw@exampleaccount.dfs.core.windows.net/fresh_reatail_net",
    "fresh_retail_net": "abfss://raw@exampleaccount.dfs.core.windows.net/fresh_retail_net",
    "taxi_data": "abfss://raw@exampleaccount.dfs.core.windows.net/taxi_data",
}


def list_tree(root: str, depth: int = 0, max_depth: int = 3):
    try:
        items = dbutils.fs.ls(root)
    except Exception as exc:
        return {"path": root, "error": f"{type(exc).__name__}: {exc}", "children": []}
    children = []
    for item in items:
        entry = {
            "path": item.path,
            "name": item.name,
            "size": item.size,
            "is_dir": item.isDir(),
        }
        if item.isDir() and depth < max_depth:
            nested = list_tree(item.path, depth + 1, max_depth)
            entry["children"] = nested.get("children", [])
            if nested.get("error"):
                entry["list_error"] = nested["error"]
        children.append(entry)
    return {"path": root, "children": children}


def peek_parquet(path: str) -> dict:
    try:
        df = spark.read.parquet(path)
        return {
            "path": path,
            "columns": df.columns,
            "dtypes": [f"{name}:{dtype}" for name, dtype in df.dtypes],
            "count": df.count(),
            "sample": [row.asDict(recursive=True) for row in df.limit(3).collect()],
        }
    except Exception as exc:
        return {"path": path, "error": f"{type(exc).__name__}: {exc}"}


def peek_json(path: str) -> dict:
    try:
        df = spark.read.option("multiLine", "true").json(path)
        return {
            "path": path,
            "columns": df.columns,
            "dtypes": [f"{name}:{dtype}" for name, dtype in df.dtypes],
            "count": df.count(),
            "sample": [row.asDict(recursive=True) for row in df.limit(3).collect()],
        }
    except Exception as exc:
        return {"path": path, "error": f"{type(exc).__name__}: {exc}"}


def walk_files(node, acc=None):
    acc = acc if acc is not None else []
    for child in node.get("children") or []:
        if child.get("is_dir"):
            walk_files(child, acc)
        else:
            acc.append(child)
    return acc


report = {"listings": {}, "peeks": []}
for name, root in PREFIXES.items():
    listing = list_tree(root)
    report["listings"][name] = listing
    for file_entry in walk_files(listing):
        path = file_entry["path"]
        lower = path.lower()
        if lower.endswith(".parquet") or "/parquet/" in lower:
            report["peeks"].append(peek_parquet(path))
        elif lower.endswith(".json"):
            report["peeks"].append(peek_json(path))

print(json.dumps(report, indent=2, default=str))
dbutils.notebook.exit(json.dumps(report, default=str, sort_keys=True))
