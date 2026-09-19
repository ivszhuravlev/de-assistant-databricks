"""Storage, catalog, and generic table checks."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from metadata import RUN_ERRORS

LEGAL_MODES = ("append", "overwrite", "merge")
LAYER_SCHEMAS = ("bronze", "silver", "gold")
ADLS_ACCOUNT = "exampleaccount"
ADLS_CONTAINER = "raw"
ADLS_RAW_PREFIX = "taxi_data"
ADLS_RAW_ROOT = (
    f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{ADLS_RAW_PREFIX}"
)
# Owner folder spelling for retail is `fresh_reatail_net` (typo kept on purpose).
EVAL_PIPELINES = {
    "taxi": {
        "raw_prefix": "taxi_data",
        "hive_prefix": "",
        "delta_namespace": "",
    },
    "transaction_cat": {
        "raw_prefix": "transaction_cat",
        "hive_prefix": "tx_",
        "delta_namespace": "tx",
    },
    "fresh_retail": {
        "raw_prefix": "fresh_reatail_net",
        "hive_prefix": "retail_",
        "delta_namespace": "retail",
    },
}
ADLS_SECRET_SCOPE = "de-assist-databricks"
ADLS_SECRET_KEY = "adls_sas"
ADLS_LEGACY_SECRET_KEY = "adls-sas"
DBFS_ROOT = "dbfs:/de-assist-databricks"
GENERATED_SCHEMA_PREFIX = "gen_"
# This classic cluster ships with hostile defaults (shuffle 2000, 32 KiB
# maxPartitionBytes, AQE off, no broadcast). Generated notebooks hang on silver
# joins unless the session is reset. Eval slices already override shuffle.
SESSION_SPARK_CONF = {
    "spark.sql.shuffle.partitions": "64",
    "spark.sql.files.maxPartitionBytes": "134217728",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": "10485760",
}


@dataclass(frozen=True)
class RuntimePaths:
    backend: str
    catalog: str
    schema: str
    ops_schema: str
    raw_root: str
    delta_root: str
    log_root: str
    checkpoint_root: str
    error_root: str
    schema_prefix: str = ""
    hive_prefix: str = ""
    output_space: str = "eval"
    pipeline: str = "taxi"

    def raw(self, *parts: str) -> str:
        return _join(self.raw_root, *parts)

    def layer_schema(self, layer: str) -> str:
        if layer in LAYER_SCHEMAS:
            return f"{self.schema_prefix}{self.hive_prefix}{layer}"
        return layer

    def delta(self, layer: str, table: str) -> str:
        return _join(self.delta_root, layer, table)

    def ops_delta(self, table: str) -> str:
        """Observer tables always live on the shared eval Delta root."""
        return _join(DBFS_ROOT, "delta", self.ops_schema, table)

    def table(self, layer: str, table: str | None = None) -> str:
        schema = self.layer_schema(layer) if table is not None else self.schema
        name = table or layer
        if self.catalog != "hive_metastore":
            return f"{self.catalog}.{schema}.{name}"
        return f"{schema}.{name}"

    def error_table(self) -> str:
        if self.catalog != "hive_metastore":
            return f"{self.catalog}.{self.ops_schema}.{RUN_ERRORS}"
        return f"{self.ops_schema}.{RUN_ERRORS}"

    def log(self, name: str) -> str:
        return _join(self.log_root, name)

    def checkpoint(self, name: str) -> str:
        return _join(self.checkpoint_root, name)


def configure_adls(spark, sas_token: str) -> str:
    token = (sas_token or "").lstrip("?")
    if not token:
        raise ValueError("ADLS SAS token is empty")
    dfs = f"{ADLS_ACCOUNT}.dfs.core.windows.net"
    blob = f"{ADLS_ACCOUNT}.blob.core.windows.net"
    settings = {
        f"fs.azure.account.auth.type.{dfs}": "SAS",
        f"fs.azure.sas.token.provider.type.{dfs}": (
            "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
        ),
        f"fs.azure.sas.fixed.token.{dfs}": token,
        f"fs.azure.sas.{ADLS_CONTAINER}.{blob}": token,
        # Directory-scoped SAS tokens cannot authorize ABFS's container-root
        # getAccessControl probe. The account is known to use HNS, so skip it.
        "fs.azure.account.hns.enabled": "true",
        f"fs.azure.account.hns.enabled.{dfs}": "true",
    }
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        for key, value in settings.items():
            hadoop_conf.set(key, value)
    except (AttributeError, TypeError):
        pass
    return ADLS_RAW_ROOT


def configure_adls_from_secret(spark, dbutils=None, sas_token: str | None = None) -> str:
    token = (sas_token or "").lstrip("?")
    if not token and dbutils is not None:
        for secret_key in (ADLS_SECRET_KEY, ADLS_LEGACY_SECRET_KEY):
            try:
                token = str(dbutils.secrets.get(ADLS_SECRET_SCOPE, secret_key)).lstrip("?")
                break
            except Exception:
                continue
    if not token:
        raise RuntimeError(
            f"ADLS SAS missing. Put it in secret {ADLS_SECRET_SCOPE}/{ADLS_SECRET_KEY} "
            f"(legacy key {ADLS_LEGACY_SECRET_KEY} is also supported)."
        )
    return configure_adls(spark, token)


def pipeline_config(pipeline: str = "taxi") -> dict[str, str]:
    key = (pipeline or "taxi").strip().lower()
    if key not in EVAL_PIPELINES:
        raise ValueError(
            f"Unknown pipeline {pipeline!r}; expected {sorted(EVAL_PIPELINES)}"
        )
    return EVAL_PIPELINES[key]


def adls_raw_root(pipeline: str = "taxi") -> str:
    prefix = pipeline_config(pipeline)["raw_prefix"]
    return f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{prefix}"


def load_paths(
    backend: str = "dbfs",
    *,
    catalog: str = "hive_metastore",
    schema: str = "nyc_taxi",
    storage_root: str | None = None,
    error_root: str | None = None,
    output_space: str = "eval",
    pipeline: str = "taxi",
) -> RuntimePaths:
    backend = (backend or "dbfs").strip().lower()
    space = (output_space or "eval").strip().lower()
    if space not in {"eval", "generated"}:
        raise ValueError(f"Unknown output_space: {output_space}")
    cfg = pipeline_config(pipeline)
    hive_prefix = cfg["hive_prefix"]
    namespace = cfg["delta_namespace"]
    generated = GENERATED_SCHEMA_PREFIX if space == "generated" else ""
    dbfs_root = DBFS_ROOT.rstrip("/")
    if namespace:
        delta_root = (
            f"{dbfs_root}/delta/{namespace}/generated"
            if generated
            else f"{dbfs_root}/delta/{namespace}"
        )
    else:
        delta_root = f"{dbfs_root}/delta/generated" if generated else f"{dbfs_root}/delta"
    if backend == "adls":
        return RuntimePaths(
            backend="adls",
            catalog=catalog or "hive_metastore",
            schema=schema or "nyc_taxi",
            ops_schema="de_assist",
            raw_root=adls_raw_root(pipeline),
            delta_root=delta_root,
            log_root=f"{dbfs_root}/logs",
            checkpoint_root=f"{dbfs_root}/checkpoints",
            error_root=error_root or f"{dbfs_root}/delta/de_assist/run_errors",
            schema_prefix=generated,
            hive_prefix=hive_prefix,
            output_space=space,
            pipeline=pipeline or "taxi",
        )
    if backend != "dbfs":
        raise ValueError(f"Unknown storage backend: {backend}")
    root = (storage_root or dbfs_root).rstrip("/")
    if namespace:
        space_delta = (
            f"{root}/delta/{namespace}/generated" if generated else f"{root}/delta/{namespace}"
        )
    else:
        space_delta = f"{root}/delta/generated" if generated else f"{root}/delta"
    return RuntimePaths(
        backend="dbfs",
        catalog=catalog or "hive_metastore",
        schema=schema or "nyc_taxi",
        ops_schema="de_assist",
        raw_root=f"{root}/raw/{cfg['raw_prefix']}",
        delta_root=space_delta,
        log_root=f"{root}/logs",
        checkpoint_root=f"{root}/checkpoints",
        error_root=error_root or f"{root}/delta/de_assist/run_errors",
        schema_prefix=generated,
        hive_prefix=hive_prefix,
        output_space=space,
        pipeline=pipeline or "taxi",
    )


def configure_session_spark(spark) -> None:
    """Make a shared classic session usable. Call from ensure_schema and entry notebooks."""
    if spark is None:
        return
    for key, value in SESSION_SPARK_CONF.items():
        spark.conf.set(key, value)


def ensure_schema(spark, paths: RuntimePaths) -> None:
    configure_session_spark(spark)
    for layer in LAYER_SCHEMAS:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {paths.layer_schema(layer)}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {paths.ops_schema}")


def write_run_log(
    spark: Any | None,
    paths: RuntimePaths,
    name: str,
    payload: Any,
) -> str | None:
    if spark is None:
        return None
    location = paths.log(name)
    try:
        line = json.dumps(payload, sort_keys=True, default=str)
        try:
            jvm = spark.sparkContext._jvm
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            path = jvm.org.apache.hadoop.fs.Path(location)
            filesystem = path.getFileSystem(hadoop_conf)
            filesystem.mkdirs(path.getParent())
            stream = filesystem.create(path, True)
            try:
                jvm.org.apache.commons.io.IOUtils.write(line + "\n", stream, "UTF-8")
            finally:
                stream.close()
            return location
        except (AttributeError, TypeError):
            # Small Spark test doubles do not expose the Hadoop bridge.
            pass
        (
            spark.createDataFrame([(line,)], ["value"])
            .coalesce(1)
            .write.mode("overwrite")
            .text(location)
        )
        return location
    except Exception as exc:
        print(
            json.dumps(
                {
                    "event": "run_log_write_failed",
                    "path": location,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
        )
        return None


def write_delta(
    df,
    spark,
    paths: RuntimePaths,
    layer: str,
    table: str,
    mode: str = "overwrite",
    merge_keys: list[str] | None = None,
    partition_by: list[str] | None = None,
) -> str:
    mode = (mode or "overwrite").strip().lower()
    if mode not in LEGAL_MODES:
        raise ValueError(f"Unsupported write mode {mode!r}; expected {LEGAL_MODES}")
    location = paths.delta(layer, table)
    if mode == "merge":
        _merge_delta(df, spark, location, merge_keys or [])
    else:
        writer = df.write.format("delta").mode(mode).option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(location)
    fqtn = paths.table(layer, table)
    if mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {fqtn}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {fqtn} USING DELTA LOCATION '{location}'")
    try:
        spark.sql(f"REFRESH TABLE {fqtn}")
    except Exception:
        if mode != "overwrite":
            spark.sql(f"DROP TABLE IF EXISTS {fqtn}")
            spark.sql(f"CREATE TABLE {fqtn} USING DELTA LOCATION '{location}'")
    return location


def stage_delta(
    df,
    spark,
    paths: RuntimePaths,
    layer: str,
    table: str,
    mode: str = "overwrite",
    merge_keys: list[str] | None = None,
    partition_by: list[str] | None = None,
) -> str:
    """Build the proposed table state at ``{table}__staging`` without publishing it."""
    mode = (mode or "overwrite").strip().lower()
    if mode not in LEGAL_MODES:
        raise ValueError(f"Unsupported write mode {mode!r}; expected {LEGAL_MODES}")

    staged_table = f"{table}__staging"
    if mode == "overwrite":
        candidate = df
    else:
        try:
            current = spark.read.format("delta").load(paths.delta(layer, table))
        except Exception:
            current = None
        if mode == "append":
            candidate = (
                current.unionByName(df, allowMissingColumns=True)
                if current is not None
                else df
            )
        else:
            candidate = current if current is not None else df.limit(0)

    location = write_delta(
        candidate,
        spark,
        paths,
        layer,
        staged_table,
        mode="overwrite",
        partition_by=partition_by,
    )
    if mode == "merge":
        _merge_delta(df, spark, location, merge_keys or [])
        spark.sql(f"REFRESH TABLE {paths.table(layer, staged_table)}")
    return location


def publish_staged(
    spark,
    paths: RuntimePaths,
    layer: str,
    table: str,
    partition_by: list[str] | None = None,
) -> str:
    """Overwrite the published table from its already-validated staging table."""
    staged = spark.read.format("delta").load(paths.delta(layer, f"{table}__staging"))
    return write_delta(
        staged,
        spark,
        paths,
        layer,
        table,
        mode="overwrite",
        partition_by=partition_by,
    )


def _column_names(columns: Iterable[str]) -> list[str]:
    if hasattr(columns, "columns"):
        return list(columns.columns)
    return list(columns)


def actual_column(columns: Iterable[str], name: str) -> str:
    columns = _column_names(columns)
    matches = [column for column in columns if column.lower() == name.lower()]
    if not matches:
        raise ValueError(
            f"Required source column {name!r} is missing. Available columns: {list(columns)}"
        )
    return matches[0]


def missing_columns(columns: Iterable[str], required: Iterable[str]) -> list[str]:
    available = {column.lower() for column in _column_names(columns)}
    return sorted(column for column in required if column.lower() not in available)


def validate_counts(
    table_name: str,
    row_count: int,
    missing: list[str],
    duplicate_groups: Optional[int] = None,
    allow_empty: bool = False,
) -> list[str]:
    if isinstance(duplicate_groups, (list, tuple, set)):
        raise TypeError(
            "validate_counts(duplicate_groups=) is an int count of groups with "
            "count(*) > 1, not the grain column list. Measure it after write: "
            "df.groupBy(*grain).count().where(F.col('count') > 1).count()"
        )
    failures: list[str] = []
    if row_count <= 0 and not allow_empty:
        failures.append(f"{table_name}: row_count must be greater than zero")
    if missing:
        failures.append(f"{table_name}: missing required columns: {missing}")
    if duplicate_groups:
        failures.append(f"{table_name}: {duplicate_groups} duplicate groups on grain")
    return failures


def _merge_delta(df, spark, location: str, merge_keys: list[str]) -> None:
    if not merge_keys:
        raise ValueError("merge requires merge_keys")
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise RuntimeError("delta-spark is required for merge writes") from exc
    if not DeltaTable.isDeltaTable(spark, location):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(location)
        return
    updates = {column: f"s.{column}" for column in df.columns}
    condition = " AND ".join(f"t.{key} = s.{key}" for key in merge_keys)
    (
        DeltaTable.forPath(spark, location)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdate(set=updates)
        .whenNotMatchedInsertAll()
        .execute()
    )


def _join(root: str, *parts: str) -> str:
    cleaned = [root.rstrip("/")]
    cleaned.extend(p.strip("/") for p in parts if p)
    return "/".join(cleaned)
