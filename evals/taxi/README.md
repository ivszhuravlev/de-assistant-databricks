# Taxi eval

Hand-built January 2021 Zoomcamp taxi pipeline. This is the benchmark the generator must match.

Siblings: [`../fresh_retail`](../fresh_retail/README.md), [`../transaction_cat`](../transaction_cat/README.md). Separate Hive schemas; taxi tables stay put.

## Run

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
databricks bundle deploy -t dev
databricks bundle run taxi_vertical_slice -t dev
```

Raw data is already landed under `taxi_data/` in the configured ADLS container.

Put the ADLS SAS in a Databricks secret scope (`adls_sas` or `adls-sas`). Delta / Hive on DBFS.

## Hive

| Schema | Tables | Delta |
|---|---|---|
| `bronze` | `yellow_tripdata`, `green_tripdata`, `taxi_zone_lookup` | `dbfs:/de-assist-databricks/delta/bronze/...` |
| `silver` | `trips`, `rejected_trips` | `.../delta/silver/...` |
| `gold` | `dim_zones`, `fct_trips`, `fct_monthly_zone_revenue` | `.../delta/gold/...` |
| `gen_*` | same names | `dbfs:/de-assist-databricks/delta/generated/...` |

## Pass

Exact counts live in `score.py`. After a hand-built run:

1. `bronze.yellow_tripdata` = 1369765
2. `bronze.green_tripdata` = 76518
3. `bronze.taxi_zone_lookup` = 265
4. `silver.trips` unique on `(vendor_id, pickup_datetime, pickup_location_id, service_type)`
5. `silver.rejected_trips` is the dead letter, not a silent drop
6. `gold.fct_trips` count equals `silver.trips`
7. `gold.fct_monthly_zone_revenue` unique on `(pickup_zone, revenue_month, service_type)` and has 495 rows
8. A second overwrite does not grow those counts

Generated pass: `output_space=generated`, write `gen_*` only, `verify_generated` reports `same_as_eval` and `eval_untouched`. The slice is not in `tested-pipelines.json`.
