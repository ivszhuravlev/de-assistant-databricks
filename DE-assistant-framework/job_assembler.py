"""Build temporary Databricks Jobs payloads for generated notebooks."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any


def assemble_job_payload(
    name: str,
    workspace_notebooks: list[str],
    *,
    existing_cluster_id: str,
    parameters: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Return sequential notebook-task settings accepted by the Jobs SDK."""
    if not name:
        raise ValueError("Job name must be non-empty")
    if not existing_cluster_id:
        raise ValueError("existing_cluster_id must be non-empty")

    tasks: list[dict[str, Any]] = []
    for index, notebook in enumerate(workspace_notebooks):
        path = PurePosixPath(notebook)
        if not notebook.startswith("/") or path.suffix:
            raise ValueError("Workspace notebook paths must be absolute and extension-free")
        task: dict[str, Any] = {
            "task_key": f"generated_{index:02d}_{path.name}",
            "existing_cluster_id": existing_cluster_id,
            "notebook_task": {
                "notebook_path": notebook,
                "source": "WORKSPACE",
                "base_parameters": parameters or {},
            },
        }
        if tasks:
            task["depends_on"] = [{"task_key": tasks[-1]["task_key"]}]
        tasks.append(task)
    return {"name": name, "tasks": tasks}
