# DE-assistant-framework

Flat kernel. Same modules as the working framework.

| File | Role |
|---|---|
| `orchestrator.py` | bronze → silver → gold generation loop |
| `llm_agent.py` | tool loop |
| `llm_model.py` | Databricks Model Serving |
| `tools.py` | context: sources, raw/table peek, keyword errors, tested pipelines |
| `validation.py` | static contract checks |
| `executor.py` | temporary job run |
| `job_assembler.py` | sequential temporary-job task payloads |
| `config.py` | run config |
| `pipeline_helpers.py` | DBFS / Hive paths and writes |
| `metadata.py` | `pipeline_run`, `layer_run`, `data_quality` |
| `retrieval.py` | keyword search over `run_errors` |
| `prompts.py` | system prompt and per-layer task builder |
| `placeholders.py` | notify stub; judge unused |
| `00_init_metadata.py` | create observer tables |
| `90_optimize_vacuum.py` | OPTIMIZE / VACUUM |
| `99_metadata_report.py` | print observer tables |

Eval: [`docs/pipeline-eval.md`](../docs/pipeline-eval.md).
