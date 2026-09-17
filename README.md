# de-assistant-databricks

Generic Databricks data-engineering generator. A model writes bronze, silver, and gold notebooks from a pipeline spec. A hidden hand-built eval scores the result.

Workspace identity, cluster IDs, and storage SAS tokens are runtime parameters. They are not in this repository.

## Layout

```
DE-assistant-framework/   generator kernel
notebooks/                operator jobs (eval, generate, verify)
spec/                     per-run pipeline contract and helper signatures
config/                   example generator config
docs/pipeline-eval.md     taxi eval benchmark
```

## Configure

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
export DE_ASSIST_CLUSTER_ID=<cluster-id>
export DE_ASSIST_WORKSPACE_ROOT=<workspace-root>
cp config/cluster-e2e.json.example config/cluster-e2e.json
```

Put the ADLS SAS in a Databricks secret scope (`adls_sas` or `adls-sas`), not in git.

## Run

```bash
python -m pip install -e ".[dev]"
pytest -q
databricks bundle deploy -t dev
databricks bundle run taxi_vertical_slice -t dev
databricks bundle run run_generator -t dev
databricks bundle run verify_generated -t dev
```

Eval procedure: [`docs/pipeline-eval.md`](docs/pipeline-eval.md).
