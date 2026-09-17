# Databricks notebook source
"""One-time copy of Zoomcamp Jan 2021 taxi files into ADLS. SAS is never in this file."""

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

from pipeline_helpers import ADLS_RAW_ROOT, configure_adls_from_secret

sas = dbutils.widgets.get("sas").strip()
root = configure_adls_from_secret(spark, dbutils, sas_token=sas or None)

sources = [
    (
        "dbfs:/de-assist-databricks/raw/taxi_data/yellow/yellow_tripdata_2021-01.csv.gz",
        f"{root}/yellow/yellow_tripdata_2021-01.csv.gz",
    ),
    (
        "dbfs:/de-assist-databricks/raw/taxi_data/green/green_tripdata_2021-01.csv.gz",
        f"{root}/green/green_tripdata_2021-01.csv.gz",
    ),
    (
        "dbfs:/de-assist-databricks/raw/taxi_data/taxi_zone_lookup.csv",
        f"{root}/taxi_zone_lookup.csv",
    ),
    (
        "dbfs:/de-assist-databricks/raw/taxi_data/lookup/taxi_zone_lookup.csv",
        f"{root}/lookup/taxi_zone_lookup.csv",
    ),
]

copied = []
for src, dst in sources:
    dbutils.fs.cp(src, dst, True)
    copied.append({"src": src, "dst": dst})

listing = [item.path for item in dbutils.fs.ls(root)]
yellow = [item.path for item in dbutils.fs.ls(f"{root}/yellow")]
green = [item.path for item in dbutils.fs.ls(f"{root}/green")]
report = {
    "raw_root": root,
    "copied": copied,
    "listing": listing,
    "yellow": yellow,
    "green": green,
}
print(json.dumps(report, indent=2))
if not yellow or not green:
    raise RuntimeError(f"ADLS landing incomplete: {report}")
dbutils.notebook.exit(json.dumps(report, sort_keys=True))
