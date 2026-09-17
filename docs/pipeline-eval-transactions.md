# Transaction categorization eval pipeline

Hand-built medallion benchmark from Hugging Face `mitulshah/transaction-categorization` plus two small lookups. The generator must not read this notebook.

## Run

Select a Databricks CLI profile and existing cluster at runtime:

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
databricks bundle deploy -t dev
databricks bundle run load_eval_raw -t dev
databricks bundle run transaction_cat_vertical_slice -t dev
```

Raw (already landed after `load_eval_raw`):

`abfss://raw@exampleaccount.dfs.core.windows.net/transaction_cat/`

Files: `transaction_cat.parquet`, `category_taxonomy.jsonl`, `country_currency.jsonl`.

Delta / Hive stay on DBFS. SAS lives in secret scope `de-assist-databricks`, keys `adls_sas` or `adls-sas`.

Taxi eval tables in `bronze` / `silver` / `gold` stay untouched.

## Hive layout

External Delta tables only.

Eval (`transaction_cat_vertical_slice`):

| Schema | Tables | Delta |
|---|---|---|
| `tx_bronze` | `transactions`, `category_taxonomy`, `country_currency` | `dbfs:/de-assist-databricks/delta/tx/bronze/...` |
| `tx_silver` | `transactions`, `rejected_transactions` | `.../delta/tx/silver/...` |
| `tx_gold` | `dim_category`, `dim_geo`, `fct_transactions`, `fct_category_country` | `.../delta/tx/gold/...` |

Generated (`output_space=generated`, `pipeline=transaction_cat`): `gen_tx_bronze` / `gen_tx_silver` / `gen_tx_gold` under `dbfs:/de-assist-databricks/delta/tx/generated`.

Shared ops: `de_assist.pipeline_run`, `layer_run`, `data_quality`, `run_errors`.

## What happens

1. Bronze keeps the raw transaction parquet plus the category taxonomy and country/currency pair lookups.
2. Silver joins description categories onto exploded taxonomy aliases and validates country/currency pairs. Invalid rows go to `rejected_transactions` with reasons, not silent drops.
3. Gold left-joins category and geo dimensions onto the fact (must not drop rows) and aggregates `fct_category_country` so `sum(txn_count)` equals `fct_transactions`.

## Passing eval

After a successful hand-built run:

1. `tx_bronze.category_taxonomy` = 10
2. `tx_bronze.country_currency` = 5
3. `tx_bronze.transactions` is non-empty (full Hub file is 4,501,043; a generated fallback is 1,000,000)
4. `tx_silver.transactions` is unique on `transaction_id`
5. Rejected rows all have `reject_reason`
6. `tx_gold.fct_transactions` count equals `tx_silver.transactions`
7. `tx_gold.fct_category_country` is unique on `(category_code, country_norm, currency_norm)` and reconciles to the fact count
8. A second overwrite does not grow those counts
9. Taxi `bronze.yellow_tripdata` is still 1,369,765

The hand-built notebook is not in `tested-pipelines.json`.
