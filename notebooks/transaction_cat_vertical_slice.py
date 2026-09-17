# Databricks notebook source
"""Hand-built transaction categorization eval pipeline."""

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
paths = load_paths(backend, output_space="eval", pipeline="transaction_cat")
PIPELINE_NAME = "transaction_categorization"
IDENTITY = ["transaction_description", "category", "country", "currency"]

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


try:
    print(json.dumps({"event": "pipeline_start", "pipeline_run_id": pipeline_run_id}))
    ensure_schema(spark, paths)

    tx_path = first_existing(
        [
            paths.raw("transaction_cat.parquet"),
            paths.raw("_generated"),
        ]
    )
    taxonomy_path = paths.raw("category_taxonomy.jsonl")
    geo_path = paths.raw("country_currency.jsonl")

    bronze_tx = (
        spark.read.parquet(tx_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    bronze_tx = bronze_tx.withColumn(
        "_source_row_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(
                        F.col(actual_column(bronze_tx.columns, name)).cast("string"),
                        F.lit("__NULL__"),
                    )
                    for name in IDENTITY
                ],
            ),
            256,
        ),
    )
    write_table(bronze_tx, "bronze", "transactions")
    bronze_tx_count = audit_table(
        "bronze",
        "transactions",
        IDENTITY + ["_source_file", "_ingested_at", "_source_row_hash"],
        expected_count=1_000_000,
    )

    taxonomy = (
        spark.read.json(taxonomy_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    write_table(taxonomy, "bronze", "category_taxonomy")
    taxonomy_count = audit_table(
        "bronze",
        "category_taxonomy",
        ["category", "category_code", "aliases", "_source_file", "_ingested_at"],
        ["category_code"],
        expected_count=10,
    )

    geo = (
        spark.read.json(geo_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    write_table(geo, "bronze", "country_currency")
    geo_count = audit_table(
        "bronze",
        "country_currency",
        ["country", "country_norm", "currency", "_source_file", "_ingested_at"],
        ["country_norm"],
        expected_count=5,
    )

    alias_lookup = (
        spark.table(paths.table("bronze", "category_taxonomy"))
        .select(
            F.col("category").alias("taxonomy_category"),
            "category_code",
            F.explode("aliases").alias("alias"),
            "keywords",
        )
        .withColumn("alias_norm", F.lower(F.trim("alias")))
        .dropDuplicates(["alias_norm"])
    )
    geo_lookup = spark.table(paths.table("bronze", "country_currency")).select(
        F.col("country").alias("geo_country"),
        "country_norm",
        F.col("currency").alias("geo_currency"),
    )

    bronze_df = spark.table(paths.table("bronze", "transactions"))
    staged = (
        bronze_df.select(
            typed(bronze_df, "transaction_description", "string"),
            typed(bronze_df, "category", "string"),
            typed(bronze_df, "country", "string"),
            typed(bronze_df, "currency", "string"),
            F.col("_source_file"),
            F.col("_ingested_at"),
            F.col("_source_row_hash"),
        )
        .withColumn("description_norm", F.lower(F.trim("transaction_description")))
        .withColumn("category_norm", F.lower(F.trim("category")))
        .withColumn("country_norm", F.lower(F.trim("country")))
        .withColumn("currency_norm", F.upper(F.trim("currency")))
        .join(alias_lookup, F.col("category_norm") == F.col("alias_norm"), "left")
        .join(geo_lookup, "country_norm", "left")
        .withColumn(
            "reject_reason",
            F.concat_ws(
                "|",
                F.when(
                    F.col("transaction_description").isNull()
                    | (F.trim(F.col("transaction_description")) == ""),
                    F.lit("null_or_blank_description"),
                ),
                F.when(F.col("category_code").isNull(), F.lit("unknown_category")),
                F.when(
                    F.col("geo_currency").isNull()
                    | (F.col("currency_norm") != F.col("geo_currency")),
                    F.lit("invalid_country_currency"),
                ),
            ),
        )
    )
    rejected = staged.where(F.col("reject_reason") != "")
    valid = staged.where(F.col("reject_reason") == "").drop("reject_reason")
    write_table(rejected, "silver", "rejected_transactions")
    rejected_count = spark.table(paths.table("silver", "rejected_transactions")).count()
    audit_table(
        "silver",
        "rejected_transactions",
        ["transaction_description", "reject_reason", "_source_file"],
        allow_empty=True,
        extra_results=[
            quality_result(
                "silver",
                "rejected_transactions",
                "dead_letter_reason",
                spark.table(paths.table("silver", "rejected_transactions"))
                .where(F.col("reject_reason") == "")
                .count()
                == 0,
                rejected_count,
                "Every rejected row must have a reject_reason.",
            )
        ],
    )

    silver = (
        valid.withColumn(
            "_dedupe_rank",
            F.row_number().over(
                Window.partitionBy("_source_row_hash").orderBy(
                    F.col("_source_file").asc_nulls_last()
                )
            ),
        )
        .where(F.col("_dedupe_rank") == 1)
        .drop("_dedupe_rank")
        .withColumn("transaction_id", F.col("_source_row_hash"))
        .withColumn(
            "merchant_token",
            F.regexp_extract(F.col("transaction_description"), r"^([^#0-9]+)", 1),
        )
        .withColumn("merchant_token", F.trim("merchant_token"))
        .select(
            "transaction_id",
            "transaction_description",
            "category",
            "category_code",
            "country",
            "country_norm",
            "currency_norm",
            "merchant_token",
            "_source_file",
            "_ingested_at",
            "_source_row_hash",
        )
    )
    write_table(silver, "silver", "transactions", ["country_norm"])
    silver_count = audit_table(
        "silver",
        "transactions",
        [
            "transaction_id",
            "transaction_description",
            "category_code",
            "country_norm",
            "currency_norm",
            "_source_file",
        ],
        ["transaction_id"],
    )

    dim_category = (
        spark.table(paths.table("bronze", "category_taxonomy"))
        .select("category_code", "category", "keywords")
        .dropDuplicates(["category_code"])
    )
    write_table(dim_category, "gold", "dim_category")
    audit_table(
        "gold",
        "dim_category",
        ["category_code", "category"],
        ["category_code"],
        taxonomy_count,
    )

    dim_geo = (
        spark.table(paths.table("bronze", "country_currency"))
        .select("country_norm", F.col("country").alias("country_name"), "currency")
        .dropDuplicates(["country_norm"])
    )
    write_table(dim_geo, "gold", "dim_geo")
    audit_table(
        "gold",
        "dim_geo",
        ["country_norm", "country_name", "currency"],
        ["country_norm"],
        geo_count,
    )

    facts = (
        spark.table(paths.table("silver", "transactions"))
        .drop("keywords", "geo_currency", "category", "country")
        .join(
            spark.table(paths.table("gold", "dim_category")).select(
                "category_code",
                F.col("category").alias("category_name"),
                "keywords",
            ),
            "category_code",
            "left",
        )
        .join(
            spark.table(paths.table("gold", "dim_geo")).select(
                "country_norm",
                "country_name",
                F.col("currency").alias("geo_currency"),
            ),
            "country_norm",
            "left",
        )
    )
    write_table(facts, "gold", "fct_transactions", ["country_norm"])
    fact_count = spark.table(paths.table("gold", "fct_transactions")).count()
    audit_table(
        "gold",
        "fct_transactions",
        ["transaction_id", "category_name", "country_name", "currency_norm"],
        ["transaction_id"],
        extra_results=[
            quality_result(
                "gold",
                "fct_transactions",
                "silver_fact_count_match",
                fact_count == silver_count,
                fact_count,
                f"Expected silver count {silver_count}; category/geo joins must not drop rows.",
            )
        ],
    )

    monthly = (
        spark.table(paths.table("gold", "fct_transactions"))
        .groupBy("category_code", "category_name", "country_norm", "country_name", "currency_norm")
        .agg(
            F.count("transaction_id").alias("txn_count"),
            F.countDistinct("transaction_description").alias("distinct_descriptions"),
            F.countDistinct("merchant_token").alias("distinct_merchant_tokens"),
        )
    )
    write_table(monthly, "gold", "fct_category_country")
    agg_total = spark.table(paths.table("gold", "fct_category_country")).agg(F.sum("txn_count")).collect()[0][0]
    monthly_count = audit_table(
        "gold",
        "fct_category_country",
        ["category_code", "country_norm", "currency_norm", "txn_count"],
        ["category_code", "country_norm", "currency_norm"],
        extra_results=[
            quality_result(
                "gold",
                "fct_category_country",
                "agg_matches_facts",
                int(agg_total or 0) == fact_count,
                agg_total,
                f"Sum of txn_count must equal fct_transactions ({fact_count}).",
            )
        ],
    )

    counts = {
        "tx_bronze.transactions": bronze_tx_count,
        "tx_bronze.category_taxonomy": taxonomy_count,
        "tx_bronze.country_currency": geo_count,
        "tx_silver.transactions": silver_count,
        "tx_silver.rejected_transactions": rejected_count,
        "tx_gold.dim_category": taxonomy_count,
        "tx_gold.fct_transactions": fact_count,
        "tx_gold.fct_category_country": monthly_count,
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
