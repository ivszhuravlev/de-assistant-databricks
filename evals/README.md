# Evals

Three operator benchmarks. They sit outside `DE-assistant-framework/`. The generator must not read these packages.

| Id | Package | Eval Hive | Generated Hive |
|---|---|---|---|
| `taxi` | `evals/taxi` | `bronze` / `silver` / `gold` | `gen_*` |
| `fresh_retail` | `evals/fresh_retail` | `retail_*` | `gen_retail_*` |
| `transaction_cat` | `evals/transaction_cat` | `tx_*` | `gen_tx_*` |

Each package has the same files:

- `README.md` — how to run and what pass means
- `spec.json` — pipeline contract for that eval
- `score.py` — expected counts and fail reasons
- `slice.py` — hand-built notebook

Shared ops stay `de_assist.*`. Taxi tables stay put when the other two run.

`spec/helpers-api.txt` is generic harness API, not an eval. The live taxi generate still reads `spec/pipeline-spec.json` (copy of `evals/taxi/spec.json`).
