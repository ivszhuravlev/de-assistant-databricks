# Fresh retail eval

Hand-built medallion from Hugging Face `Dingdong-Inc/FreshRetailNet-50K`. The generator must not read this package.

Owner raw folder spelling is kept: `fresh_reatail_net`.

## Run

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
databricks bundle deploy -t dev
databricks bundle run load_eval_raw -t dev
databricks bundle run fresh_retail_vertical_slice -t dev
```

Raw: the configured ADLS container under `fresh_reatail_net/` (`train.parquet`, `eval.parquet`). The runtime's existing storage identity is reused.

Taxi `bronze` / `silver` / `gold` stay untouched.

## Hive

| Schema | Tables | Delta |
|---|---|---|
| `retail_bronze` | `daily_sales_train`, `daily_sales_eval` | `dbfs:/de-assist-databricks/delta/retail/bronze/...` |
| `retail_silver` | `daily_sales`, `rejected_sales`, `dim_store`, `dim_product` | `.../delta/retail/silver/...` |
| `retail_gold` | `dim_store`, `dim_product`, `fct_daily_sales`, `fct_store_daily`, `fct_category_daily` | `.../delta/retail/gold/...` |
| `gen_retail_*` | same names | `.../delta/retail/generated` |

## Pass

Exact bronze counts live in `score.py` (Hub: train 4,500,000, eval 350,000).

1. Both bronze splits land
2. `retail_silver.daily_sales` unique on `(store_id, product_id, sale_date, _split)`
3. Invalid and duplicate-grain rows are dead-lettered with `reject_reason`
4. Store and product dimensions are unique by their key plus `_split`, with attributes from the latest sale day in that split
5. `retail_gold.fct_daily_sales` joins dimensions by key plus `_split` and keeps the silver row count
6. Store-daily and category-daily marts reconcile `sum(sale_amount)` to the fact
7. A second overwrite does not grow those counts
8. Taxi `bronze.yellow_tripdata` is still 1,369,765
