# Generator kernel

Runtime only: prompt, tools, validate, execute. No eval scores and no operator docs live here.

| Module | Role |
|---|---|
| `orchestrator.py` | bronze → silver → gold loop |
| `llm_agent.py` / `llm_model.py` | model + tool loop |
| `tools.py` | read-only context tools |
| `validation.py` | static notebook contract |
| `executor.py` / `job_assembler.py` | temporary job run |
| `pipeline_helpers.py` / `metadata.py` / `retrieval.py` | paths, observer tables, error search |
| `prompts.py` | generic harness contract |
| `config.py` / `workspace_paths.py` | run config, repo root |
| `placeholders.py` | `notify` no-op for eval slices; unused `judge` stub |

Operator docs: [`docs/generator.md`](../docs/generator.md).
Taxi score: [`evals/taxi/score.py`](../evals/taxi/score.py).
Init / optimize / report notebooks: `notebooks/`.
