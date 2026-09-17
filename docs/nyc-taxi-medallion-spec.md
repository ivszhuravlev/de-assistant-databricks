# NYC Taxi Medallion Pipeline Generation Requirements

## 1. Purpose and scope

This is the Gate 1 business and data contract for the pipeline that the new framework must be able to generate. The generated artifact is a Databricks Workflow composed of Databricks notebooks written in PySpark. It must implement:

`raw (already landed) -> bronze -> silver -> gold`

The business logic is adapted from the DataTalksClub Data Engineering Zoomcamp chapters 2–4: scheduled/backfillable yellow and green taxi ingestion, warehouse-oriented partitioning, a unified trip fact, taxi-zone joins, and a monthly zone revenue mart.

This file is the business and data contract for the generated workflow. It is not the framework implementation.

## 2. Raw source contract and location decision

### 2.1 Location ambiguity

The storage account is `exampleaccount`. Three descriptions currently conflict:

1. Supplied URL: `https://exampleaccount.blob.core.windows.net/raw/taxi_data/`  
   Interpretation: container `raw`, directory `taxi_data/`.
2. Owner wording, "raw + taxi folder":  
   Interpretation: container `raw`, directory `taxi/`.
3. Screenshot wording, `raw_data/taxi`:  
   Interpretation: container `raw_data`, directory `taxi/`.

**Gate 1 working assumption:** use the supplied URL literally: container `raw`, directory `taxi_data/`. This is an assumption for generated configuration, not a claim that the path has been verified. Before any run, the owner must confirm the container/directory combination and provide an OAuth-capable Databricks storage configuration. This task must not test the paths or add credentials.

The generated pipeline must take physical paths from the source map; paths must not be embedded in transformation code. Changing the selected candidate must be a configuration-only change.

### 2.2 Logical sources and grains

| Logical source | Expected content | Raw grain | Discovery requirement |
|---|---|---|---|
| `yellow_tripdata` | NYC TLC yellow taxi monthly files | One source record per reported yellow taxi trip | Find files below the configured yellow prefix using year/month-bearing names; support the landed file format declared or detected by the source map. |
| `green_tripdata` | NYC TLC green taxi monthly files | One source record per reported green taxi trip | Find files below the configured green prefix using year/month-bearing names; support the landed file format declared or detected by the source map. |
| `taxi_zone_lookup` | TLC zone reference data | One row per `LocationID` | Resolve exactly one current lookup file below the configured zones prefix. |

Yellow and green records do not have a reliable source primary key. The pipeline must preserve raw rows in bronze and only apply the Zoomcamp-style deduplication in silver.

Required yellow fields are `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `RatecodeID`, `PULocationID`, `DOLocationID`, `store_and_fwd_flag`, `passenger_count`, `trip_distance`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, and `payment_type`.

Required green fields are the equivalent fields using `lpep_pickup_datetime` and `lpep_dropoff_datetime`, plus `trip_type` and `ehail_fee`.

Required zone fields are `LocationID`, `Borough`, `Zone`, and `service_zone`.

Column matching at the bronze boundary must be case-insensitive because TLC releases have varied column casing. The original source field names and values must remain recoverable in bronze.

## 3. Target data model

All target tables are Delta tables in the catalog and schema from `target`. Names below are logical defaults. Trip tables should be partitioned by a derived `pickup_date` when volume justifies partitioning; optimize data layout for common `pickup_datetime`, `service_type`, and location filters without creating high-cardinality physical partitions.

### 3.1 Bronze

#### `bronze.yellow_tripdata`

- **Grain:** one row per raw yellow source record.
- **Business key:** none guaranteed.
- **Columns:** all source columns without business transformations, plus:
  - `_source_file` (full path)
  - `_source_file_modified_at`
  - `_ingested_at`
  - `_ingestion_run_id`
  - `_source_row_hash` (deterministic hash of normalized raw values, for audit/reconciliation; not asserted unique)
  - `_corrupt_record` when the reader supports rescued/corrupt data
- **Write mode:** `append`.
- **Idempotency:** the normal run must append each immutable source file at most once, using a Delta ingestion ledger or equivalent checkpoint keyed by path plus file identity. A retry must not duplicate a file. An explicit backfill-replace operation may replace only the selected year/month partitions.

#### `bronze.green_tripdata`

Same grain, metadata, append mode, and file-level idempotency requirements as yellow.

#### `bronze.taxi_zone_lookup`

- **Grain:** one row per `LocationID`.
- **Key:** `LocationID`.
- **Columns:** the four required zone fields plus source metadata.
- **Write mode:** `overwrite`, because this is a small reference snapshot. The overwrite must be atomic and must occur only after validation succeeds.

Bronze must not discard null vendors, invalid dates, duplicate-looking trips, refunds, zero values, or unmatched location IDs.

### 3.2 Silver

#### `silver.trips`

- **Grain:** one deduplicated yellow or green taxi trip.
- **Primary key:** `trip_id`, a deterministic hash of `vendor_id`, `pickup_datetime`, `pickup_location_id`, and `service_type`, matching the Zoomcamp surrogate-key definition.
- **Deduplication:** partition by those four key inputs; retain the row with the earliest `dropoff_datetime`, with `_source_file` and `_source_row_hash` as deterministic tie-breakers.
- **Write mode:** Delta `merge` on `trip_id`; insert new trips and update a matched trip only when its deterministic winner or normalized values changed.
- **Columns:**
  - identifiers: `trip_id`, `vendor_id`, `service_type`, `rate_code_id`
  - locations: `pickup_location_id`, `dropoff_location_id`
  - time: `pickup_datetime`, `dropoff_datetime`, `pickup_date`
  - trip attributes: `store_and_fwd_flag`, `passenger_count`, `trip_distance`, `trip_type`
  - amounts: `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `ehail_fee`, `improvement_surcharge`, `total_amount`
  - payment: `payment_type`, `payment_type_description`
  - lineage: `_source_file`, `_source_row_hash`, `_processed_at`

Normalization rules:

1. Cast identifiers and counts to integral types, timestamps to timestamp, and amounts/distances to fixed decimal types with sufficient precision.
2. Rename yellow `tpep_*` and green `lpep_*` timestamps to common pickup/dropoff names.
3. Set `service_type` to exactly `Yellow` or `Green`.
4. Schema-align yellow with green by setting yellow `trip_type = 1` (street hail) and yellow `ehail_fee = 0`.
5. Filter rows with null `vendor_id` or null `pickup_datetime` from the conformed silver output and write them to a rejected-records Delta table with reason and lineage. Do not silently discard them.
6. Map null payment codes to `0` and map missing or unknown descriptions to `Unknown`. The minimum lookup is: `0 Unknown`, `1 Credit card`, `2 Cash`, `3 No charge`, `4 Dispute`, `5 Unknown`, `6 Voided trip`.
7. `unionByName` the standardized green and yellow projections; do not use a positional union.

#### `silver.zones`

- **Grain:** one row per taxi zone.
- **Primary key:** `location_id`.
- **Columns:** `location_id`, `borough`, `zone`, `service_zone`, source lineage.
- **Write mode:** validated atomic `overwrite`.
- **Rule:** preserve zone labels from the lookup; do not infer borough or zone from trip data.

#### `silver.rejected_trips`

- **Grain:** one rejected source record per rejection reason.
- **Key:** no business primary key; retain `_source_file`, `_source_row_hash`, and reason.
- **Write mode:** idempotent `append` tied to the processing run; rerunning the same run/file must not duplicate rejection entries.

### 3.3 Gold

#### `gold.dim_zones`

- **Grain:** one row per zone.
- **Primary key:** `location_id`.
- **Source:** `silver.zones`.
- **Write mode:** atomic `overwrite` after validation.

#### `gold.dim_vendors`

- **Grain:** one row per observed `vendor_id`.
- **Primary key:** `vendor_id`.
- **Attributes:** `vendor_name`; map known TLC codes (`1` Creative Mobile Technologies, `2` VeriFone Inc.) and use `Unknown` for other non-null codes.
- **Write mode:** Delta `merge` on `vendor_id`.

#### `gold.fct_trips`

- **Grain:** one row per `silver.trips.trip_id`.
- **Primary key:** `trip_id`.
- **Write mode:** Delta `merge` on `trip_id`.
- **Join logic:** left join `gold.dim_zones` twice:
  - `pickup_location_id = pickup_zone.location_id`
  - `dropoff_location_id = dropoff_zone.location_id`
- **Preservation rule:** both joins must remain left joins so an unknown zone never removes a trip.
- **Added attributes:** `pickup_borough`, `pickup_zone`, `dropoff_borough`, `dropoff_zone`, and `trip_duration_minutes` calculated as elapsed minutes from pickup to dropoff.
- **Other columns:** retain the conformed trip, payment, service, and monetary columns from silver.

#### `gold.monthly_zone_revenue`

This is the required reporting mart.

- **Grain / composite key:** one row per `pickup_zone`, `revenue_month`, and `service_type`.
- **Unknown handling:** group missing pickup-zone labels as `Unknown Zone`.
- **Month:** calendar month derived from `pickup_datetime`.
- **Aggregations:**
  - sum `fare_amount` as `revenue_monthly_fare`
  - sum `extra` as `revenue_monthly_extra`
  - sum `mta_tax` as `revenue_monthly_mta_tax`
  - sum `tip_amount` as `revenue_monthly_tip_amount`
  - sum `tolls_amount` as `revenue_monthly_tolls_amount`
  - sum `ehail_fee` as `revenue_monthly_ehail_fee`
  - sum `improvement_surcharge` as `revenue_monthly_improvement_surcharge`
  - sum `total_amount` as `revenue_monthly_total_amount`
  - count `trip_id` as `total_monthly_trips`
  - average `passenger_count` as `avg_monthly_passenger_count`
  - average `trip_distance` as `avg_monthly_trip_distance`
- **Write mode:** Delta `merge` on the composite key. Each run must recompute complete affected month/service partitions from `gold.fct_trips` before merging; it must not add new aggregate values to old aggregates.

## 4. Generated Databricks Workflow

The framework output must define a parameterized, rerunnable Workflow with this dependency graph:

1. `discover_and_validate_sources`
2. parallel `bronze_yellow`, `bronze_green`, and `bronze_zones`
3. `dq_bronze`
4. `silver_trips` and `silver_zones`
5. `dq_silver`
6. `gold_dimensions`
7. `gold_fact`
8. `gold_monthly_zone_revenue`
9. `dq_gold_and_reconcile`

All transform tasks must be PySpark notebooks. Workflow parameters must include at least `pipeline_id`, `run_id`, `start_month`, `end_month`, `full_refresh`, target catalog/schemas, and the selected source-map reference. The date range must support historical monthly backfills and bounded reruns. Downstream tasks run only after all required upstream tasks and their DQ gates succeed.

The generated notebooks must be configuration-driven, avoid embedded credentials, emit input/output/rejected row counts, and make retries idempotent. A full refresh may use controlled overwrite for trip targets; routine incremental runs use the modes specified above.

## 5. Mandatory data-quality gates

Failure of a hard gate must fail its notebook task and prevent downstream publication. Rejected-record handling must happen before silver hard gates are evaluated.

### 5.1 Source and bronze gates

1. The selected source root resolves to exactly one configured candidate; no automatic fallback between ambiguous containers is allowed.
2. Every requested month has the expected yellow and green file, unless the source map explicitly marks that service/month optional.
3. Every input contains its required columns after case-insensitive resolution.
4. No unreadable/corrupt record may enter the business columns of bronze; corrupt rows must be retained separately and any corrupt row count is a hard failure by default.
5. For each source file, `source_record_count = bronze_rows_written_or_already_registered`; a retry must write zero duplicate rows.
6. `bronze.taxi_zone_lookup.LocationID` is non-null and unique, and its validated row count is non-zero.

### 5.2 Silver gates

1. `trip_id` is non-null and unique.
2. `vendor_id`, `pickup_datetime`, `service_type`, and `total_amount` are non-null in published silver.
3. `service_type` contains only `Yellow` and `Green`.
4. Yellow rows have `trip_type = 1` and `ehail_fee = 0`.
5. `silver.zones.location_id` is non-null and unique.
6. Every non-null pickup/dropoff location ID is either present in `silver.zones` or is counted and reported as unmatched. Unmatched zones are allowed because gold deliberately uses left joins; the configured unmatched-rate threshold defaults to a warning, not silent loss.
7. Reconciliation holds for the processed scope:  
   `bronze input rows = rejected rows + pre-dedup valid rows`, and  
   `pre-dedup valid rows = silver winner rows + duplicate rows removed`.
8. Rows with dropoff before pickup, negative duration, negative distance, or negative monetary values must be profiled and reported. They must not be silently removed: taxi refunds and source anomalies can produce negative monetary values. Threshold/severity is configurable rather than assumed to be zero.

### 5.3 Gold gates

1. `gold.dim_zones.location_id`, `gold.dim_vendors.vendor_id`, and `gold.fct_trips.trip_id` are non-null and unique in their tables.
2. Gold fact row count equals silver trip row count for the processed scope; zone enrichment must lose zero trips.
3. `gold.fct_trips.service_type` contains only `Yellow` and `Green`.
4. `gold.monthly_zone_revenue` is unique and non-null on `pickup_zone`, `revenue_month`, and `service_type`.
5. For each affected month/service, mart `total_monthly_trips` equals the count of fact trips after applying the same month/service scope.
6. For each affected month/service, every revenue sum equals the corresponding null-safe sum from `gold.fct_trips`, within the declared decimal rounding tolerance.
7. A second run over unchanged inputs produces no additional bronze trip rows and no changed silver/gold business results.

## 6. Framework input contract

The generator must accept one structured object with the three required top-level fields below. `pipeline_id` is a stable machine identifier, `prompt` carries the human intent and locked constraints, and `source_map` resolves logical sources independently of notebook code.

```json
{
  "pipeline_id": "nyc_taxi_medallion_v1",
  "prompt": "Generate a Databricks Workflow of PySpark notebooks for raw-to-bronze-to-silver-to-gold NYC TLC yellow and green taxi data. Raw data is already landed. Normalize and union both services, deduplicate using the Zoomcamp trip surrogate key, enrich pickup and dropoff with TLC zones using left joins, and publish a monthly pickup-zone revenue mart. Support bounded monthly backfills, Delta idempotency, the specified DQ gates, and configuration-only source changes.",
  "source_map": {
    "storage_account": "exampleaccount",
    "selected_root": "https://exampleaccount.blob.core.windows.net/raw/taxi_data/",
    "selected_assumption": "container=raw; directory=taxi_data",
    "unverified_candidates": [
      "https://exampleaccount.blob.core.windows.net/raw/taxi/",
      "https://exampleaccount.blob.core.windows.net/raw_data/taxi/"
    ],
    "authentication": {
      "mode": "oauth",
      "status": "not_configured_in_gate_1",
      "credential_reference": null
    },
    "sources": {
      "yellow_tripdata": {
        "relative_prefix": "yellow/",
        "file_pattern": "*yellow*tripdata*YYYY-MM*",
        "format": "detect_or_configure",
        "required": true
      },
      "green_tripdata": {
        "relative_prefix": "green/",
        "file_pattern": "*green*tripdata*YYYY-MM*",
        "format": "detect_or_configure",
        "required": true
      },
      "taxi_zone_lookup": {
        "relative_prefix": "zones/",
        "file_pattern": "*taxi*zone*lookup*",
        "format": "detect_or_configure",
        "required": true
      }
    }
  }
}
```

The relative prefixes and patterns are placeholders until Gate 1 storage discovery is approved. The generator must preserve them as explicit configuration and must not invent a successful path or authentication state.

## 7. Acceptance criteria for generated output

The framework passes this pipeline use case only if its generated output:

1. Contains runnable PySpark notebooks and one correctly ordered Databricks Workflow definition.
2. Creates every table and applies every grain, key, join, aggregation, and write-mode contract above.
3. Supports incremental monthly operation, retry, and bounded backfill without duplicate publication.
4. Implements hard DQ gates as executable checks, rejected-row capture, and reconciliation metrics.
5. Uses only source-map paths and credential references; no secrets or storage paths are hard-coded in notebook logic.
6. Produces equivalent business semantics to the Zoomcamp chapter 4 trip fact and monthly zone revenue mart without requiring dbt.

## 8. Source references

- [Zoomcamp chapter 2: Workflow Orchestration](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration)
- [Chapter 2 cloud taxi workflow](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/11-load-taxi-data-to-bigquery.md)
- [Chapter 2 scheduling and backfills](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/12-schedule-and-backfill-full-dataset.md)
- [Zoomcamp chapter 3: Data Warehousing](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse)
- [Chapter 3 partitioning and clustering](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/02-partitioning-vs-clustering.md)
- [Zoomcamp chapter 4: Analytics Engineering](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering)
- [Chapter 4 companion taxi project](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/04-analytics-engineering/taxi_rides_ny)
- [Chapter 4 models: star schema and union rules](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/06-dbt-models.md)
- [Chapter 4 tests and contracts](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/09-dbt-tests.md)
