# NYC taxi, yellow only

Same idea as January 2021 taxi monthly zone revenue, but only yellow trips. Ignore green files. Do not build a green bronze feed.

Downstream names, yellow-only: bronze `yellow_tripdata`, `taxi_zone_lookup`; silver `trips` and `rejected_trips`; gold `dim_zones`, `fct_trips`, `fct_monthly_zone_revenue`. No `green_tripdata`.

Published columns: same as the yellow+green taxi brief (`vendor_id`, `pickup_datetime`, `pickup_location_id`, `service_type`, `reject_reason`, `trip_id`, `pickup_zone`, `dropoff_zone`, `revenue_month`, `revenue_monthly_total_amount`, `total_monthly_trips`).

Identity, dead letter, zone names, `Unknown Zone`, monthly grain, decimal money — same intent as the yellow+green taxi brief. Raw is still `taxi_data`. Look at the files yourself.
