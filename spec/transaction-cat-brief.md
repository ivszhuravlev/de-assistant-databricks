# Transaction categorization

We have a named 1M transaction snapshot plus two small lookups (category taxonomy and country/currency pairs). We want to count valid transactions by category and country. The larger Hugging Face file is source attribution, not the eval input.

Raw is already under `transaction_cat` on the configured ADLS container: `transaction_cat_snapshot.parquet` and two jsonl lookups. We should inspect the landed files before shaping them.

Downstream already expects these table names (not a recipe for how to build them):
bronze `transactions`, `category_taxonomy`, `country_currency`; silver `transactions` and `rejected_transactions`; gold `dim_category`, `dim_geo`, `fct_transactions`, `fct_category_country`.

Published columns the consumers already use (rename from the files if needed; do not invent extra business meaning):
- silver `rejected_transactions`: `reject_reason`
- silver `transactions` and gold `fct_transactions`: stable `transaction_id`
- gold `dim_category`: `category_code`, `category`
- gold `dim_geo`: `country_norm`, `country_name`, `currency`
- gold `fct_transactions`: `category_code`, `category_name`, `country_norm`, `country_name`, `currency_norm`
- gold `fct_category_country` unique on `category_code`, `country_norm`, `currency_norm`, with `txn_count` reconciling to the fact

How it should work:

- Bronze keeps the transactions and both lookups as they landed.
- Silver checks description, category, and country/currency. Blank description, category not in the taxonomy aliases, or a country/currency pair not in the lookup → dead letter with a reason. Each event needs a stable id across reruns, so repeated four-field values remain separate events. Only exact full-payload copies are duplicates: keep one and dead-letter the extras as exact copies.
- Gold: category and geo dims from the lookups, a transaction fact that does not lose silver rows, and a category × country × currency rollup whose counts reconcile to the fact.
- Overwrite must not grow counts. Do not touch taxi tables.

If a business rule is not here and not in the files, do not invent it.
