# Databricks notebook source
"""Hand-built FreshRetailNet-50K eval pipeline."""

# COMMAND ----------

import json
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

dbutils.widgets.text("backend", "adls")
dbutils.widgets.text("sas", "")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from pipeline_helpers import (
    actual_column,
    configure_adls_from_secret,
    ensure_schema,
    load_paths,
    missing_columns,
    validate_counts,
    write_delta,
    write_run_log,
)
from placeholders import notify
from retrieval import error_record, persist_error
from metadata import (
    DATA_QUALITY,
    LAYER_RUN,
    PIPELINE_RUN,
    append_metadata_rows,
    data_quality_row,
    layer_run_row,
    pipeline_run_row,
)

spark.conf.set("spark.sql.shuffle.partitions", "64")
backend = dbutils.widgets.get("backend")
if backend == "adls":
    configure_adls_from_secret(spark, dbutils, sas_token=dbutils.widgets.get("sas").strip() or None)
paths = load_paths(backend, output_space="eval", pipeline="fresh_retail")
PIPELINE_NAME = "fresh_retail_net"
IDENTITY = ["store_id", "product_id", "sale_date"]
REQUIRED_SOURCE = [
    "city_id",
    "store_id",
    "management_group_id",
    "first_category_id",
    "second_category_id",
    "third_category_id",
    "product_id",
    "dt",
    "sale_amount",
    "stock_hour6_22_cnt",
    "discount",
    "holiday_flag",
    "activity_flag",
    "precpt",
    "avg_temperature",
    "avg_humidity",
    "avg_wind_level",
]

pipeline_run_id = str(uuid.uuid4())
started_at = datetime.now(timezone.utc).isoformat()
active_layer = ""
active_table = ""
result = {}


def typed(df: DataFrame, name: str, data_type: str, alias: Optional[str] = None):
    return F.col(actual_column(df.columns, name)).cast(data_type).alias(alias or name.lower())


def write_table(
    df: DataFrame,
    layer: str,
    table_name: str,
    partition_by: Optional[List[str]] = None,
) -> None:
    global active_layer, active_table
    active_layer = layer
    active_table = table_name
    write_delta(
        df,
        spark,
        paths,
        layer,
        table_name,
        mode="overwrite",
        partition_by=partition_by,
    )


def quality_result(
    layer: str,
    table_name: str,
    check_name: str,
    passed: bool,
    actual: object,
    detail: str = "",
) -> dict:
    return data_quality_row(
        pipeline_run_id=pipeline_run_id,
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        passed=passed,
        actual=str(actual),
        detail=detail,
    )


def audit_table(
    layer: str,
    table_name: str,
    required_columns: List[str],
    unique_grain: Optional[List[str]] = None,
    expected_count: Optional[int] = None,
    allow_empty: bool = False,
    extra_results: Optional[List[dict]] = None,
) -> int:
    global active_layer, active_table
    active_layer = layer
    active_table = table_name
    fqtn = paths.table(layer, table_name)
    table = spark.table(fqtn)
    row_count = table.count()
    missing = missing_columns(table.columns, required_columns)
    duplicate_groups = (
        table.groupBy(*unique_grain).count().where(F.col("count") > 1).count()
        if unique_grain
        else None
    )
    checks = [
        quality_result(
            layer,
            table_name,
            "row_count",
            row_count > 0 or allow_empty,
            row_count,
            "Table must be non-empty." if not allow_empty else "Empty table is allowed.",
        ),
        quality_result(layer, table_name, "required_columns", not missing, missing),
    ]
    if unique_grain:
        checks.append(
            quality_result(
                layer,
                table_name,
                "unique_grain",
                duplicate_groups == 0,
                duplicate_groups,
                ", ".join(unique_grain),
            )
        )
    if expected_count is not None:
        checks.append(
            quality_result(
                layer,
                table_name,
                "expected_row_count",
                row_count == expected_count,
                row_count,
                f"Expected {expected_count}.",
            )
        )
    checks.extend(extra_results or [])
    failures = validate_counts(
        fqtn, row_count, missing, duplicate_groups, allow_empty=allow_empty
    )
    failures.extend(
        f"{fqtn}: {item['check_name']} failed ({item['detail']})"
        for item in checks
        if not item["passed"]
    )
    append_metadata_rows(spark, paths, DATA_QUALITY, checks)
    append_metadata_rows(
        spark,
        paths,
        LAYER_RUN,
        [
            layer_run_row(
                pipeline_run_id=pipeline_run_id,
                layer=layer,
                table_name=table_name,
                row_count=row_count,
                status="FAILED" if failures else "SUCCESS",
            )
        ],
    )
    if failures:
        raise RuntimeError("; ".join(dict.fromkeys(failures)))
    print(json.dumps({"table": fqtn, "row_count": row_count, "status": "SUCCESS"}))
    return row_count


def first_existing(candidates: List[str]) -> str:
    for candidate in candidates:
        try:
            dbutils.fs.ls(candidate)
            return candidate
        except Exception:
            continue
    raise FileNotFoundError(f"None of the candidate paths exist: {candidates}")


def bronze_sales(path: str, split_name: str) -> DataFrame:
    df = (
        spark.read.parquet(path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_split", F.lit(split_name))
    )
    return df.withColumn(
        "sale_date",
        F.to_date(F.col(actual_column(df.columns, "dt"))),
    )


def standardize(df: DataFrame) -> DataFrame:
    return df.select(
        typed(df, "city_id", "int"),
        typed(df, "store_id", "int"),
        typed(df, "management_group_id", "int"),
        typed(df, "first_category_id", "int"),
        typed(df, "second_category_id", "int"),
        typed(df, "third_category_id", "int"),
        typed(df, "product_id", "int"),
        F.col("sale_date"),
        typed(df, "sale_amount", "double"),
        typed(df, "stock_hour6_22_cnt", "int"),
        typed(df, "discount", "double"),
        typed(df, "holiday_flag", "int"),
        typed(df, "activity_flag", "int"),
        typed(df, "precpt", "double"),
        typed(df, "avg_temperature", "double"),
        typed(df, "avg_humidity", "double"),
        typed(df, "avg_wind_level", "double"),
        F.col("_split"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    ).withColumn(
        "is_discounted",
        F.col("discount").isNotNull() & (F.col("discount") < F.lit(1.0)),
    ).withColumn(
        "is_stockout_day",
        F.coalesce(F.col("stock_hour6_22_cnt"), F.lit(0)) > 0,
    )


try:
    print(json.dumps({"event": "pipeline_start", "pipeline_run_id": pipeline_run_id}))
    ensure_schema(spark, paths)

    train_path = first_existing(
        [
            paths.raw("train.parquet"),
            paths.raw("data", "train.parquet"),
        ]
    )
    eval_path = first_existing(
        [
            paths.raw("eval.parquet"),
            paths.raw("data", "eval.parquet"),
        ]
    )

    train_raw = bronze_sales(train_path, "train")
    write_table(train_raw, "bronze", "daily_sales_train", ["sale_date"])
    train_count = audit_table(
        "bronze",
        "daily_sales_train",
        REQUIRED_SOURCE + ["_source_file", "_ingested_at", "sale_date", "_split"],
        expected_count=4_500_000,
    )

    eval_raw = bronze_sales(eval_path, "eval")
    write_table(eval_raw, "bronze", "daily_sales_eval", ["sale_date"])
    eval_count = audit_table(
        "bronze",
        "daily_sales_eval",
        REQUIRED_SOURCE + ["_source_file", "_ingested_at", "sale_date", "_split"],
        expected_count=350_000,
    )

    combined = standardize(
        spark.table(paths.table("bronze", "daily_sales_train"))
    ).unionByName(
        standardize(spark.table(paths.table("bronze", "daily_sales_eval")))
    )
    invalid = (
        F.col("store_id").isNull()
        | F.col("product_id").isNull()
        | F.col("sale_date").isNull()
        | F.col("sale_amount").isNull()
        | (F.col("sale_amount") < 0)
    )
    rejected = combined.where(invalid).withColumn(
        "reject_reason",
        F.concat_ws(
            "|",
            F.when(F.col("store_id").isNull(), F.lit("null_store_id")),
            F.when(F.col("product_id").isNull(), F.lit("null_product_id")),
            F.when(F.col("sale_date").isNull(), F.lit("null_sale_date")),
            F.when(
                F.col("sale_amount").isNull() | (F.col("sale_amount") < 0),
                F.lit("null_or_negative_sale_amount"),
            ),
        ),
    )
    write_table(rejected, "silver", "rejected_sales", ["sale_date"])
    rejected_count = spark.table(paths.table("silver", "rejected_sales")).count()
    audit_table(
        "silver",
        "rejected_sales",
        ["store_id", "product_id", "sale_date", "reject_reason", "_source_file"],
        allow_empty=True,
        extra_results=[
            quality_result(
                "silver",
                "rejected_sales",
                "dead_letter_reason",
                spark.table(paths.table("silver", "rejected_sales"))
                .where(F.col("reject_reason") == "")
                .count()
                == 0,
                rejected_count,
                "Every rejected row must have a reject_reason.",
            )
        ],
    )

    valid = combined.where(~invalid).withColumn(
        "_dedupe_rank",
        F.row_number().over(
            Window.partitionBy(*IDENTITY, "_split").orderBy(
                F.col("_source_file").asc_nulls_last()
            )
        ),
    ).where(F.col("_dedupe_rank") == 1).drop("_dedupe_rank")
    valid = valid.withColumn(
        "sales_id",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(column).cast("string"), F.lit("__NULL__"))
                    for column in IDENTITY + ["_split"]
                ],
            ),
            256,
        ),
    )
    write_table(valid, "silver", "daily_sales", ["sale_date"])
    silver_count = audit_table(
        "silver",
        "daily_sales",
        IDENTITY + ["sales_id", "sale_amount", "city_id", "first_category_id", "_split"],
        IDENTITY + ["_split"],
    )

    dim_store = (
        spark.table(paths.table("silver", "daily_sales"))
        .groupBy("store_id")
        .agg(
            F.max("city_id").alias("city_id"),
            F.max("management_group_id").alias("management_group_id"),
        )
    )
    write_table(dim_store, "silver", "dim_store")
    store_count = audit_table(
        "silver",
        "dim_store",
        ["store_id", "city_id", "management_group_id"],
        ["store_id"],
    )

    dim_product = (
        spark.table(paths.table("silver", "daily_sales"))
        .groupBy("product_id")
        .agg(
            F.max("first_category_id").alias("first_category_id"),
            F.max("second_category_id").alias("second_category_id"),
            F.max("third_category_id").alias("third_category_id"),
            F.max("management_group_id").alias("management_group_id"),
        )
    )
    write_table(dim_product, "silver", "dim_product")
    product_count = audit_table(
        "silver",
        "dim_product",
        ["product_id", "first_category_id", "second_category_id", "third_category_id"],
        ["product_id"],
    )

    write_table(spark.table(paths.table("silver", "dim_store")), "gold", "dim_store")
    audit_table(
        "gold",
        "dim_store",
        ["store_id", "city_id", "management_group_id"],
        ["store_id"],
        store_count,
    )
    write_table(spark.table(paths.table("silver", "dim_product")), "gold", "dim_product")
    audit_table(
        "gold",
        "dim_product",
        ["product_id", "first_category_id"],
        ["product_id"],
        product_count,
    )

    facts = (
        spark.table(paths.table("silver", "daily_sales"))
        .join(
            spark.table(paths.table("gold", "dim_store")).select(
                F.col("store_id").alias("dim_store_id"),
                F.col("city_id").alias("store_city_id"),
                F.col("management_group_id").alias("store_management_group_id"),
            ),
            F.col("store_id") == F.col("dim_store_id"),
            "left",
        )
        .drop("dim_store_id")
        .join(
            spark.table(paths.table("gold", "dim_product")).select(
                F.col("product_id").alias("dim_product_id"),
                F.col("first_category_id").alias("product_first_category_id"),
                F.col("second_category_id").alias("product_second_category_id"),
                F.col("third_category_id").alias("product_third_category_id"),
            ),
            F.col("product_id") == F.col("dim_product_id"),
            "left",
        )
        .drop("dim_product_id")
    )
    write_table(facts, "gold", "fct_daily_sales", ["sale_date"])
    fact_count = spark.table(paths.table("gold", "fct_daily_sales")).count()
    audit_table(
        "gold",
        "fct_daily_sales",
        ["sales_id", "store_id", "product_id", "sale_date", "sale_amount", "store_city_id"],
        ["sales_id"],
        extra_results=[
            quality_result(
                "gold",
                "fct_daily_sales",
                "silver_fact_count_match",
                fact_count == silver_count,
                fact_count,
                f"Expected silver count {silver_count}; store/product joins must not drop rows.",
            )
        ],
    )

    store_daily = (
        spark.table(paths.table("gold", "fct_daily_sales"))
        .groupBy("store_id", "store_city_id", "sale_date", "_split")
        .agg(
            F.sum("sale_amount").alias("store_sale_amount"),
            F.count("sales_id").alias("sku_day_count"),
            F.sum(F.col("is_stockout_day").cast("int")).alias("stockout_sku_days"),
            F.sum(F.col("is_discounted").cast("int")).alias("discounted_sku_days"),
            F.sum(F.col("holiday_flag")).alias("holiday_sku_days"),
            F.avg("avg_temperature").alias("avg_temperature"),
            F.avg("precpt").alias("avg_precipitation"),
        )
    )
    write_table(store_daily, "gold", "fct_store_daily", ["sale_date"])
    store_daily_total = (
        spark.table(paths.table("gold", "fct_store_daily"))
        .agg(F.sum("store_sale_amount"))
        .collect()[0][0]
    )
    fact_sales_total = (
        spark.table(paths.table("gold", "fct_daily_sales"))
        .agg(F.sum("sale_amount"))
        .collect()[0][0]
    )
    store_daily_count = audit_table(
        "gold",
        "fct_store_daily",
        ["store_id", "sale_date", "_split", "store_sale_amount", "sku_day_count"],
        ["store_id", "sale_date", "_split"],
        extra_results=[
            quality_result(
                "gold",
                "fct_store_daily",
                "sales_reconcile_to_facts",
                abs(float(store_daily_total or 0) - float(fact_sales_total or 0)) < 1e-4,
                store_daily_total,
                f"Store-daily sale_amount must reconcile to fct_daily_sales ({fact_sales_total}).",
            )
        ],
    )

    category_daily = (
        spark.table(paths.table("gold", "fct_daily_sales"))
        .withColumn(
            "first_category_id",
            F.coalesce(F.col("product_first_category_id"), F.col("first_category_id")),
        )
        .groupBy("first_category_id", "sale_date", "_split")
        .agg(
            F.sum("sale_amount").alias("category_sale_amount"),
            F.count("sales_id").alias("sku_day_count"),
            F.countDistinct("store_id").alias("active_store_count"),
            F.countDistinct("product_id").alias("active_product_count"),
            F.sum(F.col("is_stockout_day").cast("int")).alias("stockout_sku_days"),
        )
    )
    write_table(category_daily, "gold", "fct_category_daily", ["sale_date"])
    category_total = (
        spark.table(paths.table("gold", "fct_category_daily"))
        .agg(F.sum("category_sale_amount"))
        .collect()[0][0]
    )
    category_daily_count = audit_table(
        "gold",
        "fct_category_daily",
        ["first_category_id", "sale_date", "_split", "category_sale_amount"],
        ["first_category_id", "sale_date", "_split"],
        extra_results=[
            quality_result(
                "gold",
                "fct_category_daily",
                "sales_reconcile_to_facts",
                abs(float(category_total or 0) - float(fact_sales_total or 0)) < 1e-4,
                category_total,
                f"Category-daily sale_amount must reconcile to fct_daily_sales ({fact_sales_total}).",
            )
        ],
    )

    counts = {
        "retail_bronze.daily_sales_train": train_count,
        "retail_bronze.daily_sales_eval": eval_count,
        "retail_silver.daily_sales": silver_count,
        "retail_silver.rejected_sales": rejected_count,
        "retail_silver.dim_store": store_count,
        "retail_silver.dim_product": product_count,
        "retail_gold.fct_daily_sales": fact_count,
        "retail_gold.fct_store_daily": store_daily_count,
        "retail_gold.fct_category_daily": category_daily_count,
    }
    append_metadata_rows(
        spark,
        paths,
        PIPELINE_RUN,
        [
            pipeline_run_row(
                pipeline_run_id=pipeline_run_id,
                pipeline_name=PIPELINE_NAME,
                status="SUCCESS",
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
            )
        ],
    )
    result = {
        "pipeline_run_id": pipeline_run_id,
        "status": "SUCCESS",
        "row_counts": counts,
    }
    print(json.dumps({"event": "pipeline_complete", **result}, indent=2))
    write_run_log(spark, paths, f"eval/{pipeline_run_id}.json", result)
    notify(
        {"status": "SUCCESS", **result},
        spark=spark,
        paths=paths,
        log_name=f"notify/{pipeline_run_id}.json",
    )
except Exception as exc:
    diagnostics = {
        "event": "pipeline_failed",
        "pipeline_run_id": pipeline_run_id,
        "error_type": type(exc).__name__,
        "error": str(exc),
        "backend": paths.backend,
        "raw_root": paths.raw_root,
        "layer": active_layer,
        "table": active_table,
    }
    print(json.dumps(diagnostics, indent=2))
    try:
        append_metadata_rows(
            spark,
            paths,
            PIPELINE_RUN,
            [
                pipeline_run_row(
                    pipeline_run_id=pipeline_run_id,
                    pipeline_name=PIPELINE_NAME,
                    status="FAILED",
                    started_at=started_at,
                    ended_at=datetime.now(timezone.utc).isoformat(),
                )
            ],
        )
        persist_error(
            spark,
            paths,
            error_record(
                pipeline=PIPELINE_NAME,
                layer=active_layer,
                table=active_table,
                pipeline_run_id=pipeline_run_id,
                error_type=type(exc).__name__,
                error=str(exc),
                diagnostics=diagnostics,
            ),
        )
        write_run_log(spark, paths, f"eval/{pipeline_run_id}.json", diagnostics)
        notify(
            {"status": "FAILED", **diagnostics},
            spark=spark,
            paths=paths,
            log_name=f"notify/{pipeline_run_id}.json",
        )
    except Exception as persist_exc:
        print(
            json.dumps(
                {
                    "event": "failure_metadata_persist_failed",
                    "error_type": type(persist_exc).__name__,
                    "error": str(persist_exc),
                }
            )
        )
    traceback.print_exc()
    raise

dbutils.notebook.exit(json.dumps(result, sort_keys=True))
