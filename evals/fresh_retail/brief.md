# Fresh retail daily sales

Store and category managers want daily sales from FreshRetailNet. Keep it at day grain — do not explode hourly arrays into the marts.

Raw is already under the configured fresh retail prefix on the ADLS container: train and eval parquet. Do not re-download from Hugging Face. Read the files for columns.

Downstream already expects these table names (not a recipe for how to build them):
bronze `daily_sales_train`, `daily_sales_eval`; silver `daily_sales`, `rejected_sales`, `dim_store`, `dim_product`; gold `dim_store`, `dim_product`, `fct_daily_sales`, `fct_store_daily`, `fct_category_daily`.

Published columns the consumers already use (rename from the files if needed; do not invent extra business meaning):
- silver grain: `store_id`, `product_id`, `sale_date`, `_split`
- silver `rejected_sales`: those keys plus `reject_reason`
- silver/gold dims unique on the natural key plus `_split`
- gold `fct_daily_sales`: `sales_id`, `sale_amount`, `first_category_id`
- gold `fct_store_daily` unique on `store_id`, `sale_date`, `_split`, with `store_sale_amount`
- gold `fct_category_daily` unique on `first_category_id`, `sale_date`, `_split`, with `category_sale_amount`

How it should work:

- Bronze lands the two splits separately.
- Silver brings them together with a split marker. Grain is store, product, sale date, split. Null keys or null/negative sale amount go to a dead letter with a reason, and duplicates at that grain are dead-lettered rather than silently lost.
- Store and product dims come from keys seen in that fact, not a separate master. Each split gets its own dimension member, using attributes from that split's latest sale day; train must not enrich eval.
- Gold is a daily fact that keeps every silver row after joining those split-aware dims, plus a store-daily mart and a first-category-daily mart. Both must reconcile `sale_amount` back to the daily fact.
- Overwrite must not grow counts. Do not touch taxi tables.

Inspect arrays and date columns in the files. Do not assume Hub row counts. If a business rule is not here and not in the files, do not invent it.
