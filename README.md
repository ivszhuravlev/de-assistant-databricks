# de-assistant-databricks

Generic Databricks data-engineering generator. A model writes bronze, silver, and gold notebooks from a pipeline spec. A hidden hand-built eval scores the result.

Workspace identity, cluster IDs, and storage SAS tokens are runtime parameters. They are not in this repository.

## Layout

```
DE-assistant-framework/   generator kernel only
evals/                    three operator evals (taxi, fresh_retail, transaction_cat)
notebooks/                generate / verify / init / raw land
spec/                     generic helper API + default taxi spec for generate
config/                   example generator config
docs/                     generator guide; eval procedures live under evals/
```

## Configure

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
export DE_ASSIST_CLUSTER_ID=<cluster-id>
export DE_ASSIST_WORKSPACE_ROOT=<workspace-root>
export DE_ASSIST_ADLS_ACCOUNT=<storage-account>
export DE_ASSIST_ADLS_CONTAINER=<container>
cp config/cluster-e2e.json.example config/cluster-e2e.json
```

## Storage identity

ADLS account and container are not in source. Set `DE_ASSIST_ADLS_ACCOUNT` and `DE_ASSIST_ADLS_CONTAINER` on the operator machine and on the classic cluster (cluster environment variables). Missing either raises `RuntimeError`. Pipeline prefixes stay in `EVAL_PIPELINES`.

Put the ADLS SAS in a Databricks secret scope (`adls_sas` or `adls-sas`), not in git.

## Run

```bash
python -m pip install -e ".[dev]"
pytest -q
databricks bundle deploy -t dev
databricks bundle run taxi_vertical_slice -t dev
databricks bundle run run_generator -t dev
databricks bundle run verify_generated -t dev
databricks bundle run reset_generated -t dev
```

`reset_generated` clears generated tables for taxi, transaction_cat, and fresh_retail. Eval tables stay.

Eval procedures: [`evals/README.md`](evals/README.md).

For `transaction_cat`, the eval contract is the named 1M
`transaction_cat_snapshot.parquet`. The separately landed Hugging Face
`transaction_cat.parquet` is retained for source attribution and is not the eval input.
