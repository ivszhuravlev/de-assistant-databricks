# NYC taxi pipeline: Zoomcamp-faithful specification

## Purpose and boundary

This is the Zoomcamp-derived pipeline that the Databricks framework must be able to generate. It preserves the source data, model grains, unions, joins, deduplication, and monthly revenue mart demonstrated in Data Engineering Zoomcamp modules 2–4, while translating the course's GCS/BigQuery/dbt implementation into bronze, silver, and gold layers.

The course itself does not call these layers bronze, silver, and gold. That terminology and the Azure/Databricks storage mapping below are project adaptations; the transformation semantics are taken from the cited course files. This document is a specification, not framework implementation.

## Authoritative course sources

Fetched from the `main` branch of [DataTalksClub/data-engineering-zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp):

- [Repository README](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/README.md): modules 2 (orchestration), 3 (warehouse), and 4 (analytics engineering).
- [Module 2 taxi-to-Postgres lesson](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/07-load-taxi-data-to-postgres.md), [taxi-to-BigQuery lesson](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/11-load-taxi-data-to-bigquery.md), [schedule/backfill lesson](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/12-schedule-and-backfill-full-dataset.md), and [GCP taxi flow](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/flows/08_gcp_taxi.yaml).
- [Module 3 warehouse lesson](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/01-data-warehouse-and-bigquery.md), [partitioning/clustering lesson](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/02-partitioning-vs-clustering.md), and [example SQL](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/big_query.sql).
- Module 4 dbt project: [project configuration](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/dbt_project.yml), [sources](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/staging/sources.yml), [green staging](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/staging/stg_green_tripdata.sql), [yellow staging](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/staging/stg_yellow_tripdata.sql), [union](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/intermediate/int_trips_unioned.sql), [enrichment/deduplication](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/intermediate/int_trips.sql), [zone dimension](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/marts/dim_zones.sql), [vendor dimension](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/marts/dim_vendors.sql), [vendor mapping macro](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/macros/get_vendor_data.sql), [trip fact](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/marts/fct_trips.sql), and [monthly zone revenue mart](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/models/marts/reporting/fct_monthly_zone_revenue.sql).
- Course data files: gzipped monthly CSV releases at `https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{yellow|green}/{taxi}_tripdata_YYYY-MM.csv.gz`; the course explicitly uses these CSV releases rather than current TLC Parquet files. The taxi-zone lookup is the dbt seed [`taxi_zone_lookup.csv`](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/seeds/taxi_zone_lookup.csv). Module 4 also uses [`payment_type_lookup.csv`](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/taxi_rides_ny/seeds/payment_type_lookup.csv).

## Source map and owner-storage assumption

Required logical sources:

1. `yellow_tripdata`: monthly yellow taxi CSVs, named `yellow_tripdata_YYYY-MM.csv`.
2. `green_tripdata`: monthly green taxi CSVs, named `green_tripdata_YYYY-MM.csv`.
3. `taxi_zone_lookup`: one row per TLC location ID.
4. `payment_type_lookup`: one row per payment-type code; required to reproduce the current chapter 4 `int_trips` enrichment.

Account and container come from operator environment. Azure interprets the first path segment as the container. A typical layout is:

- container: `raw`
- prefix/folder: `taxi_data/`
- equivalent data-lake URI: `abfss://<container>@<adls_account>.dfs.core.windows.net/taxi_data/`

**Working assumption:** use container `raw` and prefix `taxi_data/`. The phrase “taxi folder” is ambiguous with `taxi_data`; confirm whether the actual prefix is `taxi/` or `taxi_data/`. Authentication and connectivity are outside this specification.

Expected source-map entries should identify concrete paths or glob patterns beneath that prefix. A workable convention, to be confirmed against the actual objects, is:

```text
yellow_tripdata -> .../taxi_data/yellow/yellow_tripdata_YYYY-MM.csv[.gz]
green_tripdata  -> .../taxi_data/green/green_tripdata_YYYY-MM.csv[.gz]
taxi_zone_lookup -> .../taxi_data/taxi_zone_lookup.csv
payment_type_lookup -> .../taxi_data/payment_type_lookup.csv
```

This subfolder convention is an assumption, not a claim about existing owner objects. Discovery must resolve the real object names before generation.

## Pipeline contract

The framework-facing input is:

- `pipeline_id`: stable identifier used to namespace generated resources.
- `prompt`: natural-language request for the Zoomcamp NYC taxi pipeline.
- `source_map`: resolved locations for the four logical sources above, including format/compression and any year/month range.

The prompt and source map must not change the required model semantics below. Taxi data is processed by service type and month so historical months can be backfilled and rerun. The course examples select taxi type, year, and month; the scheduled course flow demonstrates historical backfills.

## Bronze

### `bronze_yellow_tripdata`

- **Input:** yellow monthly CSV files.
- **Grain:** one source row per yellow taxi trip record.
- **Columns:** preserve source fields, including `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, passenger/trip/location/payment/fare fields. Add lineage columns such as source file and ingestion timestamp without replacing source values.
- **Write mode:** idempotent incremental ingestion by source file/month. The course creates a monthly table, derives a key from vendor, pickup, dropoff, pickup location, and dropoff location, then `MERGE`s only unmatched rows into the main partitioned table. The Databricks equivalent must prevent duplicate rows on rerun; append is acceptable only with an enforceable file-level/id-level deduplication contract.
- **Physical intent:** partition or otherwise organize around pickup date/month for date-pruned reads, following module 3's pickup-time partitioning rationale.

### `bronze_green_tripdata`

- **Input:** green monthly CSV files.
- **Grain:** one source row per green taxi trip record.
- **Columns:** preserve source fields, including `VendorID`, `lpep_pickup_datetime`, `lpep_dropoff_datetime`, `trip_type`, `ehail_fee`, and the shared trip/payment fields. Add the same lineage columns.
- **Write mode:** same idempotent monthly incremental/merge behavior as yellow, using the course key fields with the green timestamps.
- **Physical intent:** organize around green pickup date/month.

### `bronze_taxi_zone_lookup` and `bronze_payment_type_lookup`

- **Grain:** one row per `LocationID`, and one row per payment type respectively.
- **Write mode:** replace/overwrite the small reference snapshot when its source changes.

Bronze performs ingestion, schema capture, and lineage only. Yellow and green remain separate because their timestamp names and a few attributes differ.

## Silver

### `silver_green_tripdata` and `silver_yellow_tripdata`

These reproduce the chapter 4 staging models:

- **Grain:** one retained source trip row.
- Rename identifiers to `vendor_id`, `rate_code_id`, `pickup_location_id`, and `dropoff_location_id`.
- Standardize `lpep_*` and `tpep_*` to `pickup_datetime` and `dropoff_datetime`.
- Cast identifiers, timestamps, trip measures, and payment measures to their intended types.
- Filter records with null source `vendorid`.
- Keep green-only `trip_type` and `ehail_fee`; do not invent source values for yellow at this stage.
- **Write mode:** views are the exact dbt project default for staging. A framework may materialize them incrementally only if the result remains equivalent.

### `silver_trips_unioned`

- **Grain:** one standardized input row from either staging relation.
- `UNION ALL` green and yellow.
- Add `service_type` as `Green` or `Yellow`.
- Align schemas exactly as the course does: yellow gets `trip_type = 1` (street-hail) and `ehail_fee = 0`.
- **Write mode:** the course intermediate default is a full-refresh table. The generated pipeline may use an idempotent incremental equivalent.

### `silver_trips`

- **Grain:** one deduplicated trip under the course identity: `(vendor_id, pickup_datetime, pickup_location_id, service_type)`.
- Generate `trip_id` as a surrogate key over those identity columns.
- Left join `payment_type_lookup` on `coalesce(payment_type, 0)`.
- Set missing payment code to `0` and missing description to `Unknown`.
- Deduplicate with `row_number()` over the identity, ordered by `dropoff_datetime`, retaining row 1.
- **Write mode:** the course intermediate default is a full-refresh table. An incremental merge keyed by `trip_id` is acceptable if updates preserve the same earliest-dropoff winner.

No zone join occurs in silver; chapter 4 applies zone enrichment in the trip fact.

## Gold

### `gold_dim_zones`

- **Grain:** one row per `location_id`.
- Fields: `location_id`, `borough`, `zone`, `service_zone`.
- Source: taxi-zone lookup.
- **Write mode:** full replace/overwrite, matching a small dimension built from the seed.

### `gold_dim_vendors`

- **Grain:** one row per distinct vendor ID present in `gold_fct_trips`.
- Derive the course vendor labels from the vendor macro/model: `1 = Creative Mobile Technologies`, `2 = VeriFone Inc.`, and `4 = Unknown/Other`. The course macro returns null for any unmapped ID; do not invent a broader fallback.
- **Write mode:** full replace/overwrite.

### `gold_fct_trips`

- **Grain:** one deduplicated trip (`trip_id`).
- Start from `silver_trips`.
- Left join `gold_dim_zones` twice: pickup on `pickup_location_id`, dropoff on `dropoff_location_id`. Left joins are required so missing lookup rows do not discard trips.
- Carry service, vendor, rate, locations, timestamps, passenger/trip, and complete payment breakdown.
- Add pickup/dropoff borough and zone names.
- Derive `trip_duration_minutes` from dropoff minus pickup.
- **Write mode:** incremental merge keyed by `trip_id`, matching the chapter 4 `materialized='incremental'`, `unique_key='trip_id'`, `incremental_strategy='merge'` model. The course filters incremental input after the current maximum pickup timestamp and appends new columns on schema change.

## Required gold mart: `gold_fct_monthly_zone_revenue`

This is the chapter 4 reporting model and the primary generated gold mart.

- **Grain:** one row per `(pickup_zone, revenue_month, service_type)`.
- **Input:** `gold_fct_trips`.
- Replace null pickup-zone labels with `Unknown Zone`.
- Derive `revenue_month` by truncating `pickup_datetime` to calendar month.
- Aggregate sums:
  - `revenue_monthly_fare`
  - `revenue_monthly_extra`
  - `revenue_monthly_mta_tax`
  - `revenue_monthly_tip_amount`
  - `revenue_monthly_tolls_amount`
  - `revenue_monthly_ehail_fee`
  - `revenue_monthly_improvement_surcharge`
  - `revenue_monthly_total_amount`
- Aggregate operating measures:
  - `total_monthly_trips = count(trip_id)`
  - `avg_monthly_passenger_count`
  - `avg_monthly_trip_distance`
- **Write mode:** full replace/overwrite is faithful to the dbt project, where marts default to tables and this reporting model has no incremental override. A keyed merge on the complete grain is acceptable only if affected months are recomputed, so late-arriving trips cannot leave stale aggregates.

## Acceptance criteria

1. Both yellow and green monthly files can be independently backfilled and safely rerun.
2. Bronze preserves source-specific fields and source-file lineage.
3. Silver standardizes both services, uses `UNION ALL`, enriches payment type, and applies the course deduplication rule.
4. Gold zone joins preserve trips with unknown locations.
5. `gold_fct_trips` is unique by `trip_id`.
6. The monthly mart is unique by pickup zone, calendar month, and service type and exposes every chapter 4 revenue and operating measure listed above.
7. Generated resources are driven by `pipeline_id`, prompt, and a validated source map; unresolved `taxi/` versus `taxi_data/` storage ambiguity fails validation rather than being guessed.
