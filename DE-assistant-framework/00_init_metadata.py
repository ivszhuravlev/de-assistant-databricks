# Databricks notebook source
"""Create Hive schemas and register empty observer tables."""

# COMMAND ----------

import sys
from pathlib import Path

from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

dbutils.widgets.text("backend", "adls")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from metadata import DATA_QUALITY, LAYER_RUN, PIPELINE_RUN, RUN_ERRORS, metadata_table
from pipeline_helpers import ensure_schema, load_paths

# Observer table locations stay on the eval delta root. Generated space only adds gen_* DBs.
paths = load_paths(dbutils.widgets.get("backend"), output_space="eval")
ensure_schema(spark, paths)
ensure_schema(spark, load_paths(dbutils.widgets.get("backend"), output_space="generated"))

schemas = {
    PIPELINE_RUN: StructType(
        [
            StructField("pipeline_run_id", StringType(), False),
            StructField("pipeline_name", StringType(), True),
            StructField("status", StringType(), True),
            StructField("started_at", StringType(), True),
            StructField("ended_at", StringType(), True),
        ]
    ),
    LAYER_RUN: StructType(
        [
            StructField("pipeline_run_id", StringType(), False),
            StructField("layer", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("row_count", LongType(), True),
            StructField("status", StringType(), True),
            StructField("recorded_at", StringType(), True),
        ]
    ),
    DATA_QUALITY: StructType(
        [
            StructField("pipeline_run_id", StringType(), False),
            StructField("layer", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("check_name", StringType(), True),
            StructField("passed", BooleanType(), True),
            StructField("actual", StringType(), True),
            StructField("detail", StringType(), True),
            StructField("recorded_at", StringType(), True),
        ]
    ),
    RUN_ERRORS: StructType(
        [
            StructField("pipeline_run_id", StringType(), True),
            StructField("pipeline", StringType(), True),
            StructField("layer", StringType(), True),
            StructField("table", StringType(), True),
            StructField("status", StringType(), True),
            StructField("error_type", StringType(), True),
            StructField("error", StringType(), True),
            StructField("diagnostics", StringType(), True),
            StructField("created_at", StringType(), True),
        ]
    ),
}

for name, schema in schemas.items():
    location = paths.ops_delta(name)
    fqtn = metadata_table(paths, name)
    if spark.catalog.tableExists(fqtn):
        print({"table": fqtn, "status": "exists"})
        continue
    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").save(location)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fqtn} USING DELTA LOCATION '{location}'")
    print({"table": fqtn, "location": location, "status": "created"})
