# Copy for LinkedIn and Medium

Paste from here. Do not treat this file as kernel docs.

## LinkedIn

I closed a first end-to-end on a Databricks pipeline generator.

The job gives a chat model a short brief and a helper API, not the finished notebook. The model writes bronze, silver, and gold, Spark runs them, and a hidden eval checks row counts and a few extras. Eval tables and generated tables are different Hive schemas. If the generator overwrites the benchmark, that is a fail.

Three pipelines matched: Zoomcamp taxi for January 2021, a 1M transaction snapshot, and FreshRetailNet daily sales.

It is Hive Delta on DBFS, classic cluster, one serving model. No Unity Catalog, no App, no "clone and it runs on your lakehouse."

Repo: https://github.com/ivszhuravlev/de-assistant-databricks

## Medium

Title: A Databricks generator that has to match a hidden pipeline

I wanted a model that writes a medallion job the way a colleague would: from a brief, against real files, with helpers that already exist in the workspace. I did not want a chatbot that dumps SQL and calls it a pipeline.

The test is boring on purpose. A person already built the pipeline. The generator writes a second copy into `gen_*` schemas. A notebook compares counts and a short list of extras (duplicates, dead-letter reasons, mart reconciliation). The eval package is not on the model's source map. If `verify_generated` does not print `same_as_eval=true` and `eval_untouched=true`, the run is not done. Job SUCCESS is not a score.

Three briefs went through that loop:

1. NYC TLC yellow and green for January 2021, Zoomcamp grains, monthly zone revenue.
2. Transaction categorization, 1M snapshot, category and country dims.
3. FreshRetailNet daily sales, store and category marts, train and eval splits kept apart.

The kernel is one prompt file plus tools, static checks, and a temporary job that executes each layer. Bronze, silver, and gold retry on their own budgets. Timeouts and cancels are not "try a new notebook." After a failed Spark execute the model can read driver Spark UI JSON (failed jobs, stage hint). That path is debug, not an optimizer. There is a `judge` stub. It is empty.

What the model is allowed to see: the brief, helper signatures, raw files, tables it already published. What it is not allowed to see: `evals/*/score.py`, the hand-built slice, expected counts.

Storage is explicit. ADLS account and container come from environment variables. The SAS sits in a secret scope. Delta and Hive stay on DBFS. The public git tree does not contain a workspace host, a cluster id, or a storage account. Unity Catalog is out of scope. Most new workspaces are UC-first. This harness is not. Adding UC later is a new backend (catalog, external location or volume), not a boolean on `load_paths`.

If you clone the repo, you still need a classic cluster, landed raw data, and those env vars. The README is the landing page. `docs/nyc-taxi-medallion-spec.md` and the Zoomcamp notes are leftover contract drafts. They are not the pitch.

I am not publishing an App and I am not claiming this replaces a staff DE. I am publishing a harness that already had to beat three hidden evals on one workspace.

Repo: https://github.com/ivszhuravlev/de-assistant-databricks
