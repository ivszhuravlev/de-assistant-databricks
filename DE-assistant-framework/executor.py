"""Run generated notebooks as a temporary Databricks job."""

from __future__ import annotations

import re
import time
import uuid
from pathlib import Path
from typing import Any

from job_assembler import assemble_job_payload

_ERROR_CODE = re.compile(r"\[([A-Z][A-Z0-9_]+)\]")
_SQLSTATE = re.compile(r"SQLSTATE:\s*[A-Z0-9]+")
_DUMP_MARKERS = (
    "== DataFrame ==",
    "== Physical Plan ==",
    "Traceback (most recent call last)",
    "-----",
)
_NOISE = (
    "task failed",
    "workload failed",
    "see run output for details",
    "temporary job ended",
)

TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def _state_value(value: Any) -> str:
    return getattr(value, "value", str(value)) if value is not None else ""


def _failed_run_detail(workspace: Any, parent_run: Any) -> str:
    """Fetch task errors before the temporary job and its run history are deleted."""
    parts = []
    parent_state = getattr(parent_run, "state", None)
    state_message = getattr(parent_state, "state_message", None)
    if state_message:
        parts.append(str(state_message))
    for task in getattr(parent_run, "tasks", None) or []:
        task_run_id = getattr(task, "run_id", None)
        if task_run_id is None:
            continue
        try:
            output = workspace.jobs.get_run_output(run_id=task_run_id)
        except Exception as exc:
            parts.append(f"task run {task_run_id}: could not fetch output: {exc}")
            continue
        task_key = getattr(task, "task_key", None) or str(task_run_id)
        error = getattr(output, "error", None)
        if error:
            parts.append(f"{task_key}: {slim_error(error)}")
    return slim_error("\n".join(parts))


def slim_error(text: str) -> str:
    """Keep the Spark/SQL error code. Drop DataFrame dumps and traces."""
    raw = str(text or "").strip()
    if not raw:
        return ""
    cut = raw
    for marker in _DUMP_MARKERS:
        idx = cut.find(marker)
        if idx > 0:
            cut = cut[:idx]
    lines = [candidate.strip() for candidate in cut.splitlines() if candidate.strip()]
    useful = [
        line
        for line in lines
        if _ERROR_CODE.search(line) or not any(noise in line.lower() for noise in _NOISE)
    ]
    if not useful:
        useful = lines
    match = _ERROR_CODE.search(cut)
    if match:
        line = next((item for item in useful if f"[{match.group(1)}]" in item), useful[0])
    else:
        line = useful[0]
    sqlstate = _SQLSTATE.search(cut)
    if sqlstate and sqlstate.group(0) not in line:
        line = f"{line} {sqlstate.group(0)}"
    return " ".join(line.split())[:400]


def execute_with_temp_job(
    workspace: Any,
    workspace_root: str,
    local_paths: list[Path],
    config_json: str,
    existing_cluster_id: str,
    timeout_seconds: int = 1800,
    pipeline_run_id: str = "",
    pipeline: str = "",
    layer: str = "",
) -> dict[str, Any]:
    from databricks.sdk.service.jobs import Task
    from databricks.sdk.service.workspace import ImportFormat, Language

    if not existing_cluster_id or existing_cluster_id == "UNKNOWN":
        raise NotImplementedError("temp_existing_cluster_id is not set")
    created = None
    run_root = f"{workspace_root}/generated/{uuid.uuid4().hex}"
    try:
        workspace.workspace.mkdirs(run_root)
        remote_paths = []
        for local_path in local_paths:
            remote_path = f"{run_root}/{local_path.stem}"
            workspace.workspace.upload(
                remote_path,
                local_path.read_bytes(),
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                overwrite=True,
            )
            remote_paths.append(remote_path)
        payload = assemble_job_payload(
            f"de-generator-temp-{uuid.uuid4().hex[:10]}",
            remote_paths,
            existing_cluster_id=existing_cluster_id,
            parameters={"config_json": config_json},
        )
        created = workspace.jobs.create(
            name=payload["name"],
            tasks=[Task.from_dict(task) for task in payload["tasks"]],
        )
        run = workspace.jobs.run_now(job_id=created.job_id)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            parent_run = workspace.jobs.get_run(run_id=run.run_id)
            state = parent_run.state
            life_cycle = _state_value(state.life_cycle_state)
            if life_cycle in TERMINAL_STATES:
                result_state = _state_value(state.result_state)
                if result_state != "SUCCESS":
                    detail = _failed_run_detail(workspace, parent_run)
                    summary = f"Temporary job ended as {life_cycle}/{result_state}"
                    raise RuntimeError(f"{summary}\n{detail}" if detail else summary)
                return {"job_id": created.job_id, "run_id": run.run_id, "result": result_state}
            time.sleep(10)
        workspace.jobs.cancel_run(run_id=run.run_id)
        raise TimeoutError(f"Temporary job exceeded {timeout_seconds} seconds")
    finally:
        if created is not None:
            workspace.jobs.delete(job_id=created.job_id)
        try:
            workspace.workspace.delete(run_root, recursive=True)
        except Exception:
            pass
