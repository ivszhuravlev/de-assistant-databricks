# Fresh retail eval pipeline

Hand-built medallion benchmark from Hugging Face `Dingdong-Inc/FreshRetailNet-50K`. The generator must not read this notebook.

Owner raw folder spelling is kept: `fresh_reatail_net`.

## Run

Select a Databricks CLI profile and existing cluster at runtime:

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
databricks bundle deploy -t dev
databricks bundle run load_eval_raw -t dev
databricks bundle run fresh_retail_vertical_slice -t dev
```

Raw (already landed after `load_eval_raw`):

`abfss://raw@exampleaccount.dfs.core.windows.net/fresh_reatail_net/`

Files: `train.parquet`, `eval.parquet`.

Delta / Hive stay on DBFS. SAS lives in secret scope `de-assist-databricks`, keys `adls_sas` or `adls-sas`.

Taxi eval tables in `bronze` / `silver` / `gold` stay untouched.

## Hive layout

External Delta tables only.

Eval (`fresh_retail_vertical_slice`):

| Schema | Tables | Delta |
|---|---|---|
| `retail_bronze` | `daily_sales_train`, `daily_sales_eval` | `dbfs:/de-assist-databricks/delta/retail/bronze/...` |
| `retail_silver` | `daily_sales`, `rejected_sales`, `dim_store`, `dim_product` | `.../delta/retail/silver/...` |
| `retail_gold` | `dim_store`, `dim_product`, `fct_daily_sales`, `fct_store_daily`, `fct_category_daily` | `.../delta/retail/gold/...` |

Generated (`output_space=generated`, `pipeline=fresh_retail`): `gen_retail_*` under `dbfs:/de-assist-databricks/delta/retail/generated`.

Shared ops: `de_assist.pipeline_run`, `layer_run`, `data_quality`, `run_errors`.

## What happens

1. Bronze lands the Hub train and eval splits as separate daily store-product tables, preserving arrays and weather/promo covariates.
2. Silver unions them with `_split`, dead-letters null keys or negative sales, and builds store and product dimensions from distinct keys in the fact.
3. Gold left-joins those dimensions onto the daily fact (must not drop rows) and publishes store-daily and first-category-daily marts. Both marts reconcile `sum(sale_amount)` back to `fct_daily_sales`.

Hourly arrays stay in bronze; gold stays at day grain so the eval is a DE pipeline, not a 24x explode.

## Passing eval

After a successful hand-built run:

1. `retail_bronze.daily_sales_train` and `retail_bronze.daily_sales_eval` are non-empty (Hub sizes are 4,500,000 and 350,000)
2. `retail_silver.daily_sales` is unique on `(store_id, product_id, sale_date, _split)`
3. Rejected rows all have `reject_reason`
4. `retail_gold.fct_daily_sales` count equals `retail_silver.daily_sales`
5. `retail_gold.fct_store_daily` is unique on `(store_id, sale_date, _split)` and sales reconcile
6. `retail_gold.fct_category_daily` is unique on `(first_category_id, sale_date, _split)` and sales reconcile
7. A second overwrite does not grow those counts
8. Taxi `bronze.yellow_tripdata` is still 1,369,765

The hand-built notebook is not in `tested-pipelines.json`.
