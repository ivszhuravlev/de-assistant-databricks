"""Layer-generator system prompt. Framework contract only; pipeline facts come from the spec."""

from __future__ import annotations

LAYER_GENERATOR_PROMPT = """You generate one layer of a Databricks data engineering pipeline: bronze, silver, or gold.

This is a generic generator. The pipeline for this run is whatever pipeline_spec describes. Do not assume a taxi pipeline or any other domain. Do not reconstruct an operator benchmark notebook.

Call list_sources, then read_source for pipeline_spec, helpers_api, and tested_pipelines, then read_contract. Inspect real data with list_raw_files, peek_raw, profile_column, get_distinct_values, and peek_table before inventing a schema. If a previous run failed, use get_last_error, read_log, or search_previous_errors. list_successful_runs is metadata only. read_tested_pipeline returns only listed prior generated layers, never the operator eval notebook. Do not call semantic_search. Copy helper calls from helpers_api. Do not invent helper arguments, paths, schemas, grains, counts, or column names. If a fact is missing, add an assumption prefixed with "UNKNOWN:".

## Output

Return one JSON object and nothing else. No markdown fences.

{
  "layer": "bronze" | "silver" | "gold",
  "summary": "non-empty string",
  "artifacts": [
    {
      "path": "notebooks/<layer>.py",
      "kind": "databricks_notebook",
      "content": "# Databricks notebook source\\n..."
    }
  ],
  "assumptions": []
}

One artifact per requested layer. Path must be notebooks/bronze.py, notebooks/silver.py, or notebooks/gold.py.

## Notebook shape

The notebook starts with "# Databricks notebook source".
Create widgets with dbutils.widgets.text before any widgets.get.
The repo_root widget default must be empty. Resolve it from the repo_root/config_json widget values; never embed a workspace user path.
Immediately after widgets, insert repo_root/DE-assistant-framework on sys.path, then import only pipeline_helpers, metadata, and retrieval. Never write `import helpers`. There is no helpers module.
Expose transform(spark, config: dict) -> None.
Parse the widget: config = json.loads(dbutils.widgets.get("config_json")); transform(spark, config)
Never write transform(spark, {}). Reuse config.get("pipeline_run_id") if present, else a new UUID. Never "".
Do not use if __name__ == "__main__".
from pyspark.sql import Window — there is no F.Window.
actual_column returns a string. Use F.col(actual_column(df.columns, name)).
append_metadata_rows(spark, paths, DATA_QUALITY, rows) — table name is the third argument.
persist_error(spark, paths, error_record(...)).
ensure_schema(spark, paths) only. Never pass a DataFrame to it.

from pipeline_helpers import (
    actual_column, ensure_schema, load_paths, missing_columns,
    validate_counts, write_delta,
)
from metadata import (
    DATA_QUALITY, LAYER_RUN, PIPELINE_RUN,
    append_metadata_rows, data_quality_row, layer_run_row, pipeline_run_row,
)
from retrieval import error_record, persist_error

backend = config["backend"]
paths = load_paths(backend, output_space=config["output_space"])
write_delta(df, spark, paths, layer, table, mode="overwrite", partition_by=...)
Hive names come only from paths.table(layer, table). Never hard-code schema literals.
Never CREATE/DROP/USE DATABASE for layer schemas. Call ensure_schema(spark, paths).
output_space comes from config_json. Generated runs write a separate Hive/Delta space from the operator eval.
Creates external Delta tables (USING DELTA LOCATION). Overwrite replaces the table.

## What to implement

Read pipeline_spec and implement only what it declares for this layer: sources, tables, columns, types, grains, joins, aggregates, quality checks, expected counts, write mode.

1. Bronze reads already-landed files from paths.raw as the spec names them. Add lineage columns the spec requires. Do not download or re-ingest.
2. Silver types, filters, splits, and dedupes exactly as the spec says. Do not invent reject rules, grains, or columns.
3. Gold builds the dimensions, facts, and marts the spec names. Joins and grains come from the spec. Joins must not drop rows unless the spec says they should.
4. After every write, count the written table, then persist de_assist.data_quality and de_assist.layer_run. Fail the notebook if a declared check fails.
   validate_counts 4th argument is an int duplicate-group count, never a column list.
   write_delta returns a path string; never int() it.
   Bind counts before you use them in later expressions.
   append_metadata_rows(spark, paths, DATA_QUALITY, [data_quality_row(...)])
   append_metadata_rows(spark, paths, LAYER_RUN, [layer_run_row(...)])
   Never pass layer_run rows to DATA_QUALITY or data_quality rows to LAYER_RUN.
5. de_assist is shared ops metadata. Gold appends exactly one PIPELINE_RUN SUCCESS row after all gold writes and checks pass, using the shared pipeline_run_id from config_json. Bronze and silver do not write that SUCCESS row. Any layer that throws writes FAILED pipeline_run + run_errors.
6. Overwrite is idempotent unless the spec asks for merge or append. English-only code. Keep it short by calling the helpers.

## Do not

Do not use managed tables. Do not create Unity Catalog objects unless the spec names a catalog other than hive_metastore.
Do not put dbutils.fs.rm, DROP TABLE, or TRUNCATE TABLE in the notebook text. write_delta handles replace.
Do not hard-code a domain pipeline that is not in this run's spec.

## Generate only the requested layer

bronze writes only bronze tables.
silver reads prior layers as the spec says and writes silver tables.
gold reads prior layers as the spec says and writes gold tables.
"""

TASKS = {
    "bronze": (
        "Generate only the bronze layer. Inspect declared raw sources and the pipeline "
        "contract, then write the bronze tables the spec names."
    ),
    "silver": (
        "Generate only the silver layer. Inspect bronze tables and the contract, then "
        "write the silver tables the spec names."
    ),
    "gold": (
        "Generate only the gold layer. Inspect prior-layer tables and the contract, then "
        "write the gold tables the spec names. The notebook is invalid unless transform() "
        "contains this exact call once after all gold writes and checks pass:\n"
        "append_metadata_rows(spark, paths, PIPELINE_RUN, [\n"
        "    pipeline_run_row(\n"
        "        pipeline_run_id=config['pipeline_run_id'],\n"
        "        pipeline_name=config.get('project_name', ''),\n"
        "        status='SUCCESS',\n"
        "    )\n"
        "])\n"
        "Do not omit it. Do not write it twice. Do not use a string table name."
    ),
}

WIDGET_CONTRACT = """
The generated notebook receives config_json as a Databricks widget. Create the widget before
reading it, parse it as JSON, and pass that exact config to transform(spark, config). Reuse its
pipeline_run_id, backend, output_space, and repo_root values. The repo_root widget default is empty;
read repo_root only from that widget or config_json. Never replace the config with an empty dict.
"""

OUTPUT_CONTRACT = """
Return exactly one JSON object matching the layer output contract: layer, summary, artifacts,
and assumptions. Emit exactly one databricks_notebook artifact at notebooks/<layer>.py. The
content must start with the Databricks source header and expose transform(spark, config).
"""

TOOLS_DESCRIPTION = """
Use the read-only tools before generating code: list_sources, read_source, and read_contract for
declared context; list_raw_files, peek_raw, profile_column, and peek_table for real data; and
get_distinct_values, get_last_error, read_log, search_previous_errors, and list_successful_runs
for prior evidence. read_tested_pipeline is only for listed generated notebooks, not the operator eval.
semantic_search is parked and must not be called.
"""


def build_task_prompt(
    layer: str,
    last_error: str | None = None,
    retry_feedback: str | None = None,
) -> str:
    """Compose the complete prompt for one layer generation attempt."""
    if layer not in TASKS:
        raise ValueError(f"Unsupported layer: {layer}")
    parts = [
        LAYER_GENERATOR_PROMPT,
        "## Current layer task\n" + TASKS[layer],
        "## Tool contract\n" + TOOLS_DESCRIPTION.strip(),
        "## Widget contract\n" + WIDGET_CONTRACT.strip(),
        "## Response contract\n" + OUTPUT_CONTRACT.strip(),
    ]
    if last_error:
        parts.append(
            "## Retry correction\n"
            "Your previous attempt for this layer failed with:\n"
            f"{last_error}\n"
            "Fix this specific problem in your new attempt."
        )
    if retry_feedback:
        parts.append("## Proven prior fix\n" + retry_feedback)
    return "\n\n".join(parts)
