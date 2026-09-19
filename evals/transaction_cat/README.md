# Transaction categorization eval

Hand-built medallion from Hugging Face `mitulshah/transaction-categorization` plus two lookups. The generator must not read this package.

## Run

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
databricks bundle deploy -t dev
databricks bundle run load_eval_raw -t dev
databricks bundle run transaction_cat_vertical_slice -t dev
```

Raw: the configured ADLS container under `transaction_cat/`.
The eval input is the named 1M `transaction_cat_snapshot.parquet`; the optional
`transaction_cat.parquet` is the separately retained Hugging Face source for
attribution. The lookups are `category_taxonomy.jsonl` and
`country_currency.jsonl`.

Taxi `bronze` / `silver` / `gold` stay untouched.

## Hive

| Schema | Tables | Delta |
|---|---|---|
| `tx_bronze` | `transactions`, `category_taxonomy`, `country_currency` | `dbfs:/de-assist-databricks/delta/tx/bronze/...` |
| `tx_silver` | `transactions`, `rejected_transactions` | `.../delta/tx/silver/...` |
| `tx_gold` | `dim_category`, `dim_geo`, `fct_transactions`, `fct_category_country` | `.../delta/tx/gold/...` |
| `gen_tx_*` | same names | `.../delta/tx/generated` |

## Pass

Exact bronze counts live in `score.py` (1,000,000 / 10 / 5 for the eval snapshot and lookups).

1. Taxonomy = 10, country/currency = 5, transactions = 1,000,000
2. `tx_silver.transactions` unique on `transaction_id`
3. Rejected rows all have `reject_reason`
4. `tx_gold.fct_transactions` count equals `tx_silver.transactions`
5. `tx_gold.fct_category_country` unique on `(category_code, country_norm, currency_norm)` and reconciles
6. A second overwrite does not grow those counts
7. Taxi `bronze.yellow_tripdata` is still 1,369,765
