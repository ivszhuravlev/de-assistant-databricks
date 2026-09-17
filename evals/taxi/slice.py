# Databricks notebook source
"""Hand-built January 2021 NYC taxi benchmark pipeline."""

# COMMAND ----------

import json
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

dbutils.widgets.text("backend", "adls")
dbutils.widgets.text("sas", "")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework", "evals", "taxi", "fresh_retail", "transaction_cat"}:
    repo_root_hint = repo_root_hint.parent
if repo_root_hint.name == "evals":
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

spark.conf.set("spark.sql.shuffle.partitions", "32")
backend = dbutils.widgets.get("backend")
if backend == "adls":
    configure_adls_from_secret(spark, dbutils, sas_token=dbutils.widgets.get("sas").strip() or None)
paths = load_paths(backend, output_space="eval")
MONEY = "decimal(18,2)"
PIPELINE_NAME = "nyc_taxi_january_2021"
EXPECTED_COUNTS = {
    ("bronze", "yellow_tripdata"): 1_369_765,
    ("bronze", "green_tripdata"): 76_518,
    ("bronze", "taxi_zone_lookup"): 265,
}
IDENTITY = [
    "vendor_id",
    "pickup_datetime",
    "pickup_location_id",
    "service_type",
]

pipeline_run_id = str(uuid.uuid4())
started_at = datetime.now(timezone.utc).isoformat()
active_layer = ""
active_table = ""


def typed(df: DataFrame, name: str, data_type: str, alias: Optional[str] = None):
    return F.col(actual_column(df.columns, name)).cast(data_type).alias(alias or name.lower())


def optional_typed(
    df: DataFrame,
    name: str,
    data_type: str,
    alias: Optional[str] = None,
):
    try:
        return typed(df, name, data_type, alias)
    except ValueError:
        return F.lit(None).cast(data_type).alias(alias or name.lower())


def lookup_path() -> str:
    candidates = [
        paths.raw("taxi_zone_lookup.csv"),
        paths.raw("lookup", "taxi_zone_lookup.csv"),
    ]
    for candidate in candidates:
        try:
            dbutils.fs.ls(candidate)
            return candidate
        except Exception:
            pass
    raise FileNotFoundError(f"Taxi zone lookup not found at any of: {candidates}")


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
        quality_result(
            layer,
            table_name,
            "required_columns",
            not missing,
            missing,
        ),
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
        fqtn,
        row_count,
        missing,
        duplicate_groups,
        allow_empty=allow_empty,
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


def standardize(
    df: DataFrame,
    pickup_column: str,
    dropoff_column: str,
    service_type: str,
) -> Tuple[DataFrame, DataFrame]:
    shared = [
        typed(df, "VendorID", "int", "vendor_id"),
        typed(df, pickup_column, "timestamp", "pickup_datetime"),
        typed(df, dropoff_column, "timestamp", "dropoff_datetime"),
        typed(df, "RatecodeID", "int", "rate_code_id"),
        typed(df, "PULocationID", "int", "pickup_location_id"),
        typed(df, "DOLocationID", "int", "dropoff_location_id"),
        optional_typed(df, "store_and_fwd_flag", "string"),
        typed(df, "passenger_count", "int"),
        typed(df, "trip_distance", "double"),
        typed(df, "payment_type", "int"),
        typed(df, "fare_amount", MONEY),
        typed(df, "extra", MONEY),
        typed(df, "mta_tax", MONEY),
        typed(df, "tip_amount", MONEY),
        typed(df, "tolls_amount", MONEY),
        optional_typed(df, "congestion_surcharge", MONEY),
        typed(df, "improvement_surcharge", MONEY),
        typed(df, "total_amount", MONEY),
        F.lit(service_type).alias("service_type"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.col("_source_month"),
    ]
    extras = (
        [
            optional_typed(df, "trip_type", "int"),
            optional_typed(df, "ehail_fee", MONEY),
        ]
        if service_type == "Green"
        else [
            F.lit(1).cast("int").alias("trip_type"),
            F.lit(0).cast(MONEY).alias("ehail_fee"),
        ]
    )
    full = (
        df.select(*(shared + extras))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
    )
    invalid = F.col("vendor_id").isNull() | F.col("pickup_datetime").isNull()
    rejected = full.where(invalid).withColumn(
        "reject_reason",
        F.concat_ws(
            "|",
            F.when(F.col("vendor_id").isNull(), F.lit("null_vendor_id")),
            F.when(F.col("pickup_datetime").isNull(), F.lit("null_pickup_datetime")),
        ),
    )
    return full.where(~invalid), rejected


def monthly_matches_facts() -> Tuple[bool, int]:
    facts = (
        spark.table(paths.table("gold", "fct_trips"))
        .withColumn("revenue_month", F.date_trunc("month", "pickup_datetime"))
        .groupBy("revenue_month", "service_type")
        .agg(
            F.sum("total_amount").alias("expected_total"),
            F.count("trip_id").alias("expected_trips"),
        )
    )
    monthly = (
        spark.table(paths.table("gold", "fct_monthly_zone_revenue"))
        .groupBy("revenue_month", "service_type")
        .agg(
            F.sum("revenue_monthly_total_amount").alias("actual_total"),
            F.sum("total_monthly_trips").alias("actual_trips"),
        )
    )
    mismatches = (
        facts.join(monthly, ["revenue_month", "service_type"], "full")
        .where(
            ~F.col("expected_total").eqNullSafe(F.col("actual_total"))
            | ~F.col("expected_trips").eqNullSafe(F.col("actual_trips"))
        )
        .count()
    )
    return mismatches == 0, mismatches


try:
    print(json.dumps({"event": "pipeline_start", "pipeline_run_id": pipeline_run_id}))
    ensure_schema(spark, paths)
    read_options = {"header": "true", "inferSchema": "true"}

    yellow_raw = (
        spark.read.options(**read_options)
        .csv(paths.raw("yellow", "yellow_tripdata_2021-01.csv.gz"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_month", F.lit("2021-01"))
    )
    yellow_raw = yellow_raw.withColumn(
        "pickup_date",
        F.to_date(F.col(actual_column(yellow_raw.columns, "tpep_pickup_datetime"))),
    )
    write_table(yellow_raw, "bronze", "yellow_tripdata", ["pickup_date"])
    yellow_count = audit_table(
        "bronze",
        "yellow_tripdata",
        ["VendorID", "tpep_pickup_datetime", "_source_file", "_ingested_at", "pickup_date"],
        expected_count=EXPECTED_COUNTS[("bronze", "yellow_tripdata")],
    )

    green_raw = (
        spark.read.options(**read_options)
        .csv(paths.raw("green", "green_tripdata_2021-01.csv.gz"))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_month", F.lit("2021-01"))
    )
    green_raw = green_raw.withColumn(
        "pickup_date",
        F.to_date(F.col(actual_column(green_raw.columns, "lpep_pickup_datetime"))),
    )
    write_table(green_raw, "bronze", "green_tripdata", ["pickup_date"])
    green_count = audit_table(
        "bronze",
        "green_tripdata",
        ["VendorID", "lpep_pickup_datetime", "_source_file", "_ingested_at", "pickup_date"],
        expected_count=EXPECTED_COUNTS[("bronze", "green_tripdata")],
    )

    zones_raw = (
        spark.read.options(**read_options)
        .csv(lookup_path())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )
    write_table(zones_raw, "bronze", "taxi_zone_lookup")
    zone_count = audit_table(
        "bronze",
        "taxi_zone_lookup",
        ["LocationID", "Borough", "Zone", "service_zone", "_source_file", "_ingested_at"],
        ["LocationID"],
        EXPECTED_COUNTS[("bronze", "taxi_zone_lookup")],
    )

    yellow, yellow_rejected = standardize(
        spark.table(paths.table("bronze", "yellow_tripdata")),
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "Yellow",
    )
    green, green_rejected = standardize(
        spark.table(paths.table("bronze", "green_tripdata")),
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "Green",
    )
    rejected = yellow_rejected.unionByName(green_rejected)
    write_table(rejected, "silver", "rejected_trips", ["pickup_date"])
    rejected_count = spark.table(paths.table("silver", "rejected_trips")).count()
    valid_rejected_count = (
        spark.table(paths.table("silver", "rejected_trips"))
        .where(F.col("vendor_id").isNull() | F.col("pickup_datetime").isNull())
        .count()
    )
    audit_table(
        "silver",
        "rejected_trips",
        ["vendor_id", "pickup_datetime", "reject_reason", "_source_file", "pickup_date"],
        allow_empty=False,
        extra_results=[
            quality_result(
                "silver",
                "rejected_trips",
                "dead_letter_reason",
                valid_rejected_count == rejected_count,
                f"{valid_rejected_count}/{rejected_count}",
                "Every rejected row must have a null vendor or pickup timestamp.",
            )
        ],
    )

    silver = (
        yellow.unionByName(green)
        .withColumn(
            "_dedupe_rank",
            F.row_number().over(
                Window.partitionBy(*IDENTITY).orderBy(
                    F.col("dropoff_datetime").asc_nulls_last(),
                    F.col("_source_file").asc_nulls_last(),
                )
            ),
        )
        .where(F.col("_dedupe_rank") == 1)
        .drop("_dedupe_rank")
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(column).cast("string"), F.lit("__NULL__"))
                        for column in IDENTITY
                    ],
                ),
                256,
            ),
        )
    )
    write_table(silver, "silver", "trips", ["pickup_date"])
    silver_count = audit_table(
        "silver",
        "trips",
        IDENTITY + ["dropoff_datetime", "trip_id", "pickup_date", "_source_file"],
        IDENTITY,
    )

    bronze_zones = spark.table(paths.table("bronze", "taxi_zone_lookup"))
    dim_zones = bronze_zones.select(
        typed(bronze_zones, "LocationID", "int", "location_id"),
        typed(bronze_zones, "Borough", "string", "borough"),
        typed(bronze_zones, "Zone", "string", "zone"),
        typed(bronze_zones, "service_zone", "string"),
    )
    write_table(dim_zones, "gold", "dim_zones")
    audit_table(
        "gold",
        "dim_zones",
        ["location_id", "borough", "zone", "service_zone"],
        ["location_id"],
        zone_count,
    )

    zones = spark.table(paths.table("gold", "dim_zones"))
    pickup_zones = zones.select(
        F.col("location_id").alias("pickup_zone_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone"),
    )
    dropoff_zones = zones.select(
        F.col("location_id").alias("dropoff_zone_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone"),
    )
    facts = (
        spark.table(paths.table("silver", "trips"))
        .join(
            pickup_zones,
            F.col("pickup_location_id") == F.col("pickup_zone_location_id"),
            "left",
        )
        .drop("pickup_zone_location_id")
        .join(
            dropoff_zones,
            F.col("dropoff_location_id") == F.col("dropoff_zone_location_id"),
            "left",
        )
        .drop("dropoff_zone_location_id")
        .withColumn(
            "trip_duration_minutes",
            (
                F.col("dropoff_datetime").cast("long")
                - F.col("pickup_datetime").cast("long")
            )
            / F.lit(60.0),
        )
    )
    write_table(facts, "gold", "fct_trips", ["pickup_date"])
    fact_count = spark.table(paths.table("gold", "fct_trips")).count()
    audit_table(
        "gold",
        "fct_trips",
        ["trip_id", "pickup_zone", "dropoff_zone", "pickup_date", "total_amount"],
        ["trip_id"],
        extra_results=[
            quality_result(
                "gold",
                "fct_trips",
                "silver_fact_count_match",
                fact_count == silver_count,
                fact_count,
                f"Expected silver count {silver_count}; zone joins must not drop trips.",
            )
        ],
    )

    monthly = (
        spark.table(paths.table("gold", "fct_trips"))
        .withColumn(
            "pickup_zone",
            F.coalesce(F.col("pickup_zone"), F.lit("Unknown Zone")),
        )
        .withColumn("revenue_month", F.date_trunc("month", "pickup_datetime"))
        .groupBy("pickup_zone", "revenue_month", "service_type")
        .agg(
            F.sum("fare_amount").alias("revenue_monthly_fare"),
            F.sum("extra").alias("revenue_monthly_extra"),
            F.sum("mta_tax").alias("revenue_monthly_mta_tax"),
            F.sum("tip_amount").alias("revenue_monthly_tip_amount"),
            F.sum("tolls_amount").alias("revenue_monthly_tolls_amount"),
            F.sum("ehail_fee").alias("revenue_monthly_ehail_fee"),
            F.sum("improvement_surcharge").alias(
                "revenue_monthly_improvement_surcharge"
            ),
            F.sum("total_amount").alias("revenue_monthly_total_amount"),
            F.count("trip_id").alias("total_monthly_trips"),
            F.avg("passenger_count").alias("avg_monthly_passenger_count"),
            F.avg("trip_distance").alias("avg_monthly_trip_distance"),
        )
    )
    write_table(monthly, "gold", "fct_monthly_zone_revenue")
    totals_match, mismatch_count = monthly_matches_facts()
    monthly_count = audit_table(
        "gold",
        "fct_monthly_zone_revenue",
        [
            "pickup_zone",
            "revenue_month",
            "service_type",
            "revenue_monthly_fare",
            "revenue_monthly_total_amount",
            "total_monthly_trips",
        ],
        ["pickup_zone", "revenue_month", "service_type"],
        extra_results=[
            quality_result(
                "gold",
                "fct_monthly_zone_revenue",
                "monthly_totals_match_facts",
                totals_match,
                mismatch_count,
                "Monthly trip counts and total revenue must reconcile to fct_trips.",
            )
        ],
    )

    counts = {
        "bronze.yellow_tripdata": yellow_count,
        "bronze.green_tripdata": green_count,
        "bronze.taxi_zone_lookup": zone_count,
        "silver.trips": silver_count,
        "silver.rejected_trips": rejected_count,
        "gold.dim_zones": zone_count,
        "gold.fct_trips": fact_count,
        "gold.fct_monthly_zone_revenue": monthly_count,
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
