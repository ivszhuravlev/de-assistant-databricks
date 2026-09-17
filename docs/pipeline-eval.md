# NYC taxi eval pipeline

Hand-built January 2021 Zoomcamp taxi pipeline. This is the benchmark the generator must match.

Related evals (separate Hive schemas, taxi tables stay put): `docs/pipeline-eval-transactions.md`, `docs/pipeline-eval-retail.md`.

## Run

Select a Databricks CLI profile and existing cluster at runtime:

```bash
export DATABRICKS_CONFIG_PROFILE=<profile>
export BUNDLE_VAR_existing_cluster_id=<cluster-id>
databricks bundle deploy -t dev
databricks bundle run taxi_vertical_slice -t dev
```

Raw files (already landed, do not re-ingest):

`abfss://raw@exampleaccount.dfs.core.windows.net/taxi_data/`

Delta / Hive stay on DBFS. SAS lives in secret scope `de-assist-databricks`, keys `adls_sas` or `adls-sas`. Not in the repo.

## Hive layout

External Delta tables only (`USING DELTA LOCATION ...`). Not managed tables.

Eval (hand-built `taxi_vertical_slice`):

| Schema | Tables | Delta |
|---|---|---|
| `bronze` | `yellow_tripdata`, `green_tripdata`, `taxi_zone_lookup` | `dbfs:/de-assist-databricks/delta/bronze/...` |
| `silver` | `trips`, `rejected_trips` | `.../delta/silver/...` |
| `gold` | `dim_zones`, `fct_trips`, `fct_monthly_zone_revenue` | `.../delta/gold/...` |

Generated (`output_space=generated`):

| Schema | Same table names | Delta |
|---|---|---|
| `gen_bronze` | `yellow_tripdata`, `green_tripdata`, `taxi_zone_lookup` | `dbfs:/de-assist-databricks/delta/generated/bronze/...` |
| `gen_silver` | `trips`, `rejected_trips` | `.../delta/generated/silver/...` |
| `gen_gold` | `dim_zones`, `fct_trips`, `fct_monthly_zone_revenue` | `.../delta/generated/gold/...` |

Shared ops: `de_assist.pipeline_run`, `layer_run`, `data_quality`, `run_errors`.

## What a passing eval looks like

After a successful **hand-built** run, all of these are true:

1. `SELECT count(*) FROM bronze.yellow_tripdata` = 1369765
2. `SELECT count(*) FROM bronze.green_tripdata` = 76518
3. `SELECT count(*) FROM bronze.taxi_zone_lookup` = 265
4. `silver.trips` is unique on `(vendor_id, pickup_datetime, pickup_location_id, service_type)`
5. `silver.rejected_trips` holds null-vendor / null-pickup rows (dead letter), not dropped silently
6. `gold.fct_trips` count equals `silver.trips` count (zone left joins drop nothing)
7. `gold.fct_monthly_zone_revenue` is unique on `(pickup_zone, revenue_month, service_type)` and has the Zoomcamp monthly revenue sums
8. A second overwrite run does not increase those counts
9. `de_assist.pipeline_run` has one row for the run; `de_assist.layer_run` has one row per layer table with `row_count`; `de_assist.data_quality` has the checks above

Failed runs append to `de_assist.run_errors`. Search them with the `search_previous_errors` tool or:

```sql
SELECT created_at, pipeline, layer, error_type, error
FROM de_assist.run_errors
ORDER BY created_at DESC
LIMIT 20;
```

## Generated vs hand-built

Point the generator at `spec/pipeline-spec.json` with `output_space: generated`. It must write `gen_*` only. Eval tables stay put. The hand-built notebook is the operator benchmark only; it is not in `tested-pipelines.json` and `read_tested_pipeline` will not return it.

Pass when:

1. `gen_bronze.yellow_tripdata` / `green_tripdata` / `taxi_zone_lookup` match the eval counts above.
2. `gen_silver.trips` count equals `silver.trips` and `gen_gold.fct_trips`.
3. Grains, dead-letter reasons, and monthly reconciliation hold on the **gen_*** tables.
4. `bronze.yellow_tripdata` is still 1369765 after the generated run (eval not clobbered).
5. A second generated overwrite does not grow `gen_*` counts.

Reset generated only (`notebooks/reset_generated_eval.py`) drops `gen_bronze` / `gen_silver` / `gen_gold` and `dbfs:/de-assist-databricks/delta/generated`. It must not touch eval schemas.

Verify: `notebooks/verify_generated_eval.py`.
