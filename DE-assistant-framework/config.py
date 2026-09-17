"""Runtime config for one generation run."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

LAYERS = ("bronze", "silver", "gold")


@dataclass
class GeneratorConfig:
    project_name: str
    input_root: str
    contract_file: str
    output_root: str
    workspace_root: str
    metadata_path: str
    source_map: list[dict[str, str]]
    model_endpoint: str = "databricks-claude-haiku-4-5"
    layers: list[str] = field(default_factory=lambda: list(LAYERS))
    execute_generated: bool = False
    temp_existing_cluster_id: str = "UNKNOWN"
    max_tool_rounds: int = 8
    max_attempts: int = 3
    error_log_path: str = "dbfs:/de-assist-databricks/delta/de_assist/run_errors"
    raw_backend: str = "adls"
    output_space: str = "generated"

    @classmethod
    def load(cls, path: str | Path) -> "GeneratorConfig":
        values = json.loads(Path(path).read_text(encoding="utf-8"))
        env_overrides = {
            "workspace_root": os.environ.get("DE_ASSIST_WORKSPACE_ROOT", "").strip(),
            "temp_existing_cluster_id": os.environ.get("DE_ASSIST_CLUSTER_ID", "").strip(),
        }
        values.update({key: value for key, value in env_overrides.items() if value})
        return cls(**values)
