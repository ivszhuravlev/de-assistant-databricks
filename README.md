# de-assistant-databricks

A Databricks job that asks a chat model to write bronze, silver, and gold notebooks from a short pipeline brief, runs them, and scores the tables against a hand-built eval the model cannot read.

This is a harness, not a Databricks App and not a replacement for a data engineer. Tables are Hive external Delta on DBFS. Unity Catalog is not supported. There is no UI and no judge that rewrites a successful run.

Three evals have passed: NYC taxi (Zoomcamp January 2021), transaction categorization (1M snapshot), and FreshRetailNet daily sales. Generated output lives in `gen_*` (and `gen_tx_*` / `gen_retail_*`). Eval schemas stay untouched.

To clone and reproduce you need your own workspace, a classic cluster, ADLS raw files, and a secret-scoped SAS. Copy `.env.example` for the variable names. The public repo does not contain account names, cluster IDs, or tokens. MIT license.

## Layout

```
DE-assistant-framework/   prompt, tools, validate, execute
evals/                    operator benchmarks the generator must not import
notebooks/                generate, verify, reset, raw land
spec/                     helper API and the brief the live generator reads
config/                   example run configs
docs/                     kernel guide and eval pointers
```

## Configure

```bash
cp .env.example .env
# fill DATABRICKS_CONFIG_PROFILE, cluster id, workspace root, ADLS account/container
cp config/cluster-e2e.json.example config/cluster-e2e.json
```

Set the two `DE_ASSIST_ADLS_*` variables on the cluster as well. Missing either raises `RuntimeError`. Per-pipeline prefixes stay in code (`EVAL_PIPELINES`). Put the ADLS SAS in a Databricks secret scope (`adls_sas` or `adls-sas`).

Serving model used in the closed evals: `databricks-claude-haiku-4-5`.

## Run

```bash
python -m pip install -e ".[dev]"
pytest -q
databricks bundle deploy -t dev
databricks bundle run run_generator -t dev -- --config_path config/generate-taxi.json --cluster_id <cluster-id>
databricks bundle run verify_generated -t dev -- --pipeline taxi --cluster_id <cluster-id>
```

`verify_generated` must print `same_as_eval=true`, extras 0, and `eval_untouched=true`. Job SUCCESS alone is not a pass.

`reset_generated` drops generated tables for taxi, transaction_cat, and fresh_retail. It does not drop eval tables.

Eval packages: [`evals/README.md`](evals/README.md). Kernel: [`docs/generator.md`](docs/generator.md).

## What the model sees

Reads: the brief from the config `source_map`, `spec/helpers-api.txt`, `spec/tested-pipelines.json`.

Inspects: raw files, its own `gen_*` tables, its own run history (successes, last error, attempt log, error search).

Blocked: `evals/*`. Not in the source map, refused by `read_tested_pipeline`, and generated code that imports or reads `evals/` fails validation.

`promote_to_whitelist` adds a passed layer to `spec/tested-pipelines.json`. Per-eval details are in each `evals/*/README.md`.
