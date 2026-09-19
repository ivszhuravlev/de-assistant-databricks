# NYC taxi, January 2021

Finance wants monthly revenue by pickup zone, split yellow vs green, for January 2021 TLC data.

Raw is already under the `taxi_data` prefix on the configured ADLS container: yellow trips, green trips, zone lookup. Do not download again. Look at the landing zone for real file names and columns.

Downstream already expects these table names (not a recipe for how to build them):
bronze `yellow_tripdata`, `green_tripdata`, `taxi_zone_lookup`; silver `trips` and `rejected_trips`; gold `dim_zones`, `fct_trips`, `fct_monthly_zone_revenue`.

Published columns the consumers already use (rename from the files if needed; do not invent extra business meaning):
- silver `trips` identity: `vendor_id`, `pickup_datetime`, `pickup_location_id`, `service_type`
- silver `rejected_trips`: those keys plus `reject_reason`
- gold `fct_trips`: `trip_id`, `pickup_zone`, `dropoff_zone`, `pickup_datetime`, `service_type`, `total_amount`; `Unknown Zone` on the zone columns themselves
- gold `fct_monthly_zone_revenue` unique on `pickup_zone`, `revenue_month`, `service_type`, with `revenue_monthly_total_amount` and `total_monthly_trips`

How it should work:

- Land those feeds as bronze, with lineage, no extra business filters.
- Silver is one trip stream: yellow and green together, typed, service type from which file they came from. A trip is vendor + pickup time + pickup location + service type; dedupe on that. Missing vendor or pickup time is invalid — keep it in a dead letter with a reason, do not delete it.
- Gold attaches zone names for pickup and dropoff from the lookup without losing trips; put `Unknown Zone` on the trip fact itself when an id is missing, not only on the monthly rollup, then roll up pickup zone × month of pickup × service type with the usual TLC money fields, trip counts, average passengers and distance.
- Money as decimal. Each trip must carry pickup day so monthly cuts are possible. Overwrite must not grow counts.

Inspect data instead of assuming spellings or row counts. If a business rule is not here and not in the files, do not invent it.
