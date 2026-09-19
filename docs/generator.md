# Generator guide

Product landing page: [`../README.md`](../README.md).

This folder is the flat runtime kernel for generating Databricks bronze, silver, and gold
notebooks. The model endpoint is `databricks-claude-haiku-4-5`; workspace access uses the
Databricks CLI profile selected by the operator.

## Configure a run

Start from `config/example.json`. Important fields are:

- `layers`: ordered layers to generate. The retry counter resets for each layer.
- `max_attempts`: generation/validation/execution attempts per layer; default `3`.
- `max_tool_rounds`: maximum model/tool turns inside one attempt.
- `execute_generated`: when `false`, stop after validation and writing local artifacts; when
  `true`, upload each artifact and run it as a temporary job.
- `output_space`: `generated` writes `gen_bronze` / `gen_silver` / `gen_gold` and
  `dbfs:/de-assist-databricks/delta/generated`. Never `eval` for the generator.
- `temp_existing_cluster_id`: cluster used by temporary jobs. Set it with
  `DE_ASSIST_CLUSTER_ID`; do not store the ID in JSON.
- `output_root`, `workspace_root`, and `metadata_path`: local artifact, workspace, and generator
  outcome destinations.
- `source_map`: only these repository context files can be read by source tools.

For local E2E execution, copy `config/cluster-e2e.json.example` to the ignored
`config/cluster-e2e.json`, then set `DE_ASSIST_WORKSPACE_ROOT` and `DE_ASSIST_CLUSTER_ID`.

## Storage identity

Raw ADLS is resolved at runtime from `DE_ASSIST_ADLS_ACCOUNT` and `DE_ASSIST_ADLS_CONTAINER`.
Set both in the process environment (and on the cluster for notebook runs). The kernel
does not embed an account name. Per-pipeline prefixes live in `EVAL_PIPELINES`.

The prompt has one source of truth: `DE-assistant-framework/prompts.py`. It is the generic
harness contract. How this pipeline should work comes from `spec/pipeline-brief.md`.
The operator eval (`evals/*/score.py`, `slice.py`, `spec.json`) is not model input.

## Run the generator

From the repository root:

```bash
python -m pip install -e .
python DE-assistant-framework/orchestrator.py \
  --config config/example.json \
  --repo-root .
```

Generated notebooks are written under `generated/notebooks/`. To execute generated code, set
`execute_generated` to `true` first. The executor uploads the notebook, creates a temporary
Databricks job, waits for it, reads task output on failure, and deletes the temporary job.

Deploy the repository bundle separately:

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
databricks bundle deploy -t dev
```

## Retry loop

Every layer runs this bounded loop independently:

1. Build the layer task from the system prompt, per-layer task, tool/widget/output contracts, and
   any previous error.
2. Run the model/tool loop and retain its complete message ledger.
3. Validate the returned JSON and notebook statically.
4. Write the artifact and, when enabled, execute it as a temporary job.
5. On failure, capture the slim Spark/SQL error code, write the attempt record and JSON log,
   and retry.
6. Append the failure and any matching proven past-fix diff as a new user message to the same
   conversation. The original system message and failed generated answer remain in context.
7. Return the successful layer, or raise only after `max_attempts` is exhausted.

A failure in bronze does not spend silver's retry budget. A successful bronze resets the attempt
number when silver starts.

## Read-only model tools

- `list_sources`, `read_source`, `read_contract`: declared repository context.
- `list_raw_files`, `peek_raw`: raw DBFS discovery, schema, and samples.
- `profile_column`, `get_distinct_values`: data profiling before filters or schemas are invented.
- `peek_table`: inspect layer tables in the configured output_space (`gen_*` when generating) plus `de_assist`.
- `search_previous_errors`, `get_last_error`: keyword error retrieval from
  `de_assist.run_errors`.
- `read_log`: tail a prior generator run log.
- `list_successful_runs`: SUCCESS metadata rows only.
- `read_tested_pipeline`: listed notebooks only. The hand-built eval (`taxi_vertical_slice.py`) is hidden.
- The whitelist is `spec/tested-pipelines.json`; promote with the `promote_to_whitelist` widget, while evals stay hidden.

All tools are read-only. Generated code performs writes through `pipeline_helpers.py`.
Databricks Claude prompt caching uses `cache_control` on the stable system prefix.

## Metadata

The generated pipeline writes operational metadata in Hive schema `de_assist`:

- `pipeline_run`: overall pipeline status and timestamps.
- `layer_run`: one row per layer table, including row count and status.
- `data_quality`: one row per measured quality check.
- `run_errors`: failed generator/pipeline attempts. Generator retry records keep attempt number and
  generated code in the JSON `diagnostics` field. A `PASSED` retry marker with the same run and
  next attempt allows lexical retrieval to prove which code diff fixed a prior error.

`retrieval.get_last_error` selects the newest `FAILED` row for a pipeline and layer.
`retrieval.find_past_fix` performs keyword matching only. It pairs a failed attempt with the next
passed attempt, computes a code diff, and injects that diff into retry feedback. It does not use
embeddings or Vector Search.

Inspect recent errors:

```sql
SELECT created_at, pipeline_run_id, pipeline, layer, status, error_type, error, diagnostics
FROM de_assist.run_errors
ORDER BY created_at DESC
LIMIT 20;
```

## DBFS attempt logs

Each attempt writes one JSON file:

```text
dbfs:/de-assist-databricks/logs/generator/<pipeline_run_id>/<layer>/attempt-<n>.json
```

`write_run_log` writes a real UTF-8 file through the Hadoop filesystem bridge; it does not use
Spark `.text()`, which would create a directory of part files.

List and read logs:

```bash
databricks --profile <profile> fs ls \
  dbfs:/de-assist-databricks/logs/generator/<pipeline_run_id>/<layer>/

databricks --profile <profile> fs cat \
  dbfs:/de-assist-databricks/logs/generator/<pipeline_run_id>/<layer>/attempt-1.json
```

The JSON contains the attempt number and either its successful outcome or the exact error sent to
the next retry.

## Evaluation

Eval scores are not in this kernel. Taxi expected counts live in `evals/taxi/score.py`.
Procedure: [`docs/pipeline-eval.md`](pipeline-eval.md). Generated tables go to `gen_*` schemas.
`reset_generated` drops generated Hive tables and each pipeline's generated Delta root
(taxi, transaction_cat, fresh_retail). Eval schemas stay.
