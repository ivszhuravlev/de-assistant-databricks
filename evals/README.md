# Evals

Three operator benchmarks. They sit outside `DE-assistant-framework/`. The generator must not read these packages.

| Id | Package | Eval Hive | Generated Hive |
|---|---|---|---|
| `taxi` | `evals/taxi` | `bronze` / `silver` / `gold` | `gen_*` |
| `fresh_retail` | `evals/fresh_retail` | `retail_*` | `gen_retail_*` |
| `transaction_cat` | `evals/transaction_cat` | `tx_*` | `gen_tx_*` |

Each package has the same files:

- `brief.md` — what a DE would write before building (idea, not the finished job)
- `README.md` / `score.py` / `slice.py` / `spec.json` — operator-only. The generator does not read them.

`spec/pipeline-brief.md` is the taxi brief the live generator reads. `spec/helpers-api.txt` is the platform helper contract.
