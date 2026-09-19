"""Layer loop: prompt → LLM + tools → validate → write → optional execute."""

from __future__ import annotations

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import LAYERS, GeneratorConfig
from executor import execute_with_temp_job, slim_error
from llm_agent import _info, emit_event, run_tool_loop, tool_ledger
from llm_model import DatabricksChatClient
from pipeline_helpers import write_run_log
from prompts import build_task_prompt
from retrieval import error_record, find_past_fix, persist_error
from tools import (
    _HIDDEN_PIPELINE_IDS,
    _TESTED_FILE,
    _hidden_pipeline_entry,
    _tested_catalog,
    _visible_tested_catalog,
    make_tools,
)
from validation import validate_layer, write_artifacts

_PRIOR_WHITELIST_TOOLS = {"read_tested_pipeline"}
_PRIOR_SUCCESS_TOOLS = {"list_successful_runs"}
_PRIOR_HISTORY_TOOLS = {"get_last_error", "read_log", "search_previous_errors"}
_PRIOR_EVIDENCE_TOOLS = _PRIOR_WHITELIST_TOOLS | _PRIOR_SUCCESS_TOOLS | _PRIOR_HISTORY_TOOLS


def run_generator(
    config: GeneratorConfig,
    repo_root: Path,
    spark: Any | None = None,
    workspace: Any | None = None,
) -> list[dict[str, Any]]:
    tools, runtime_paths = make_tools(config, repo_root, spark)
    client = DatabricksChatClient(config.model_endpoint)
    pipeline_run_id = uuid.uuid4().hex
    _info(f"run log folder: {runtime_paths.log_root}/generator/{pipeline_run_id}")
    outcomes = []
    if config.max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    emit_event(
        "generator_start",
        pipeline=config.project_name,
        pipeline_run_id=pipeline_run_id,
        layers=list(config.layers),
    )
    for layer in config.layers:
        if layer not in LAYERS:
            raise ValueError(f"Unsupported layer: {layer}")
        prior_messages: list[dict[str, Any]] | None = None
        last_error: str | None = None
        for attempt in range(1, config.max_attempts + 1):
            emit_event(
                "layer_attempt",
                pipeline=config.project_name,
                pipeline_run_id=pipeline_run_id,
                layer=layer,
                attempt=attempt,
            )
            retry_feedback = None
            if last_error:
                retry_feedback = (
                    "Your previous attempt failed with:\n"
                    f"{last_error}\n"
                    "Fix this specific problem and return the complete layer JSON again."
                )
                if spark is not None:
                    past_fix = find_past_fix(spark, runtime_paths, last_error, layer)
                    if past_fix:
                        retry_feedback += "\n\n" + past_fix
            task_prompt = build_task_prompt(layer, last_error, retry_feedback)
            generated: dict[str, Any] | None = None
            try:
                generated, prior_messages = run_tool_loop(
                    client,
                    tools,
                    task_prompt,
                    layer,
                    config.max_tool_rounds,
                    prior_messages=prior_messages,
                    retry_feedback=retry_feedback,
                    attempt=attempt,
                )
                _require_prior_evidence(
                    prior_messages,
                    _visible_tested_catalog(repo_root / config.input_root / _TESTED_FILE),
                )
                errors = validate_layer(generated, layer)
                if errors:
                    raise ValueError(f"{layer} validation failed: {'; '.join(errors)}")
                paths = write_artifacts(generated, repo_root / config.output_root)
                execution = {"status": "SKIPPED"}
                # The tool loop runs before execute_generated, so this job's Spark UI does not
                # exist yet. Do not add another LLM round after SUCCESS; a failed job's Spark UI
                # would support debugging, not optimization, and is outside this task.
                if config.execute_generated:
                    emit_event(
                        "layer_execute",
                        pipeline=config.project_name,
                        pipeline_run_id=pipeline_run_id,
                        layer=layer,
                        attempt=attempt,
                    )
                    active_workspace = workspace or client.workspace
                    execution = execute_with_temp_job(
                        active_workspace,
                        config.workspace_root,
                        paths,
                        json.dumps(
                            {
                                "project_name": config.project_name,
                                "backend": config.raw_backend or "adls",
                                "output_space": config.output_space or "generated",
                                "pipeline": getattr(config, "pipeline", "taxi") or "taxi",
                                "repo_root": str(repo_root),
                                "pipeline_run_id": pipeline_run_id,
                            }
                        ),
                        config.temp_existing_cluster_id,
                        pipeline_run_id=pipeline_run_id,
                        pipeline=config.project_name,
                        layer=layer,
                    )
                    execution["status"] = execution.get("result", "UNKNOWN")
                if config.promote_to_whitelist:
                    promote_to_whitelist(config, repo_root, layer)
            except Exception as exc:
                failed_messages = getattr(exc, "prior_messages", None)
                if isinstance(failed_messages, list):
                    prior_messages = failed_messages
                last_error = f"{type(exc).__name__}: {slim_error(str(exc))}"
                emit_event(
                    "layer_failed",
                    pipeline=config.project_name,
                    pipeline_run_id=pipeline_run_id,
                    layer=layer,
                    attempt=attempt,
                    error=last_error,
                )
                _record_attempt(
                    spark,
                    runtime_paths,
                    pipeline=config.project_name,
                    layer=layer,
                    pipeline_run_id=pipeline_run_id,
                    attempt=attempt,
                    status="FAILED",
                    error=last_error,
                    generated=generated,
                    messages=prior_messages,
                )
                if _non_retryable(exc) or attempt == config.max_attempts:
                    raise RuntimeError(
                        f"{layer} failed after {config.max_attempts} attempts: {last_error}"
                    ) from exc
                continue

            outcome = {
                "pipeline_run_id": pipeline_run_id,
                "project_name": config.project_name,
                "layer": layer,
                "attempt": attempt,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "model_endpoint": config.model_endpoint,
                "artifact_paths": [str(path.relative_to(repo_root)) for path in paths],
                "execution": execution,
                "assumptions": generated["assumptions"],
            }
            if spark is not None:
                try:
                    row = {
                        key: json.dumps(value, sort_keys=True) if isinstance(value, (dict, list)) else value
                        for key, value in outcome.items()
                    }
                    spark.createDataFrame([row]).write.format("delta").mode("append").save(
                        config.metadata_path
                    )
                except Exception as exc:
                    print(
                        json.dumps(
                            {
                                "event": "metadata_write_failed",
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                                "metadata_path": config.metadata_path,
                            }
                        )
                    )
            _record_attempt(
                spark,
                runtime_paths,
                pipeline=config.project_name,
                layer=layer,
                pipeline_run_id=pipeline_run_id,
                attempt=attempt,
                status="PASSED",
                error="",
                generated=generated,
                outcome=outcome,
                messages=prior_messages,
            )
            emit_event(
                "layer_passed",
                pipeline=config.project_name,
                pipeline_run_id=pipeline_run_id,
                layer=layer,
                attempt=attempt,
                execution=execution.get("status"),
            )
            outcomes.append(outcome)
            break
    return outcomes


def _require_prior_evidence(
    messages: list[dict[str, Any]] | None,
    visible_catalog: list[dict[str, Any]],
) -> None:
    if not visible_catalog:
        return
    used = {
        str(entry.get("name") or "")
        for entry in tool_ledger(messages, limit=10_000)
        if entry.get("name")
    }
    missing = []
    if not used.intersection(_PRIOR_WHITELIST_TOOLS):
        missing.append("read_tested_pipeline")
    if not used.intersection(_PRIOR_SUCCESS_TOOLS):
        missing.append("list_successful_runs")
    if not used.intersection(_PRIOR_HISTORY_TOOLS):
        missing.append("get_last_error|read_log|search_previous_errors")
    if missing:
        raise ValueError(
            "Prior-evidence tools were required because a tested pipeline is listed: "
            + ", ".join(missing)
        )


def promote_to_whitelist(config: GeneratorConfig, repo_root: Path, layer: str) -> None:
    """Publish one successfully generated layer to the tested-pipeline catalog."""
    catalog_path = repo_root / config.input_root / _TESTED_FILE
    pipeline_id = f"{config.project_name}_{layer}"
    kept = [
        item
        for item in _tested_catalog(catalog_path)
        if item.get("id") not in {"tool_smoke_sample", pipeline_id, *_HIDDEN_PIPELINE_IDS}
        and not _hidden_pipeline_entry(item)
    ]
    kept.append(
        {
            "id": pipeline_id,
            "layer": layer,
            "path": f"{config.output_root.rstrip('/')}/notebooks/{layer}.py",
            "status": "tested",
            "project_name": config.project_name,
        }
    )
    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    catalog_path.write_text(
        json.dumps({"pipelines": kept}, indent=2) + "\n",
        encoding="utf-8",
    )


def _non_retryable(exc: BaseException) -> bool:
    """Timeouts and user/job cancels are stuck signals, not code the model should rewrite."""
    if isinstance(exc, TimeoutError):
        return True
    text = f"{type(exc).__name__}: {exc}".lower()
    return "timeout" in text or "canceled" in text or "cancelled" in text


def _generated_code(generated: dict[str, Any] | None) -> str:
    if not generated:
        return ""
    return "\n\n".join(
        str(artifact.get("content") or "") for artifact in generated.get("artifacts") or []
    )


def _record_attempt(
    spark: Any | None,
    runtime_paths: Any,
    pipeline: str,
    layer: str,
    pipeline_run_id: str,
    attempt: int,
    status: str,
    error: str,
    generated: dict[str, Any] | None,
    outcome: dict[str, Any] | None = None,
    messages: list[dict[str, Any]] | None = None,
) -> None:
    ledger = tool_ledger(messages)
    record = error_record(
        pipeline=pipeline,
        layer=layer,
        pipeline_run_id=pipeline_run_id,
        error_type=error.split(":", 1)[0] if error else "",
        error=error,
        status=status,
        diagnostics={
            "source": "generator",
            "attempt": attempt,
            "generated_code": _generated_code(generated),
            "tool_ledger": ledger,
        },
    )
    if spark is not None:
        try:
            persist_error(spark, runtime_paths, record)
        except Exception as persist_exc:
            print(
                json.dumps(
                    {
                        "event": "error_persist_failed",
                        "error_type": type(persist_exc).__name__,
                        "error": str(persist_exc),
                    }
                )
            )
    log_path = write_run_log(
        spark,
        runtime_paths,
        f"generator/{pipeline_run_id}/{layer}/attempt-{attempt}.json",
        outcome
        or {
            **record,
            "error_delta_location": runtime_paths.error_root,
            "error_table": runtime_paths.error_table(),
        },
    )
    if log_path is not None:
        _info(f"run_log_written path={log_path}", blank_after=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config/example.json")
    parser.add_argument("--repo-root", default=".")
    args = parser.parse_args()
    root = Path(args.repo_root).resolve()
    print(json.dumps(run_generator(GeneratorConfig.load(root / args.config), root), indent=2))


if __name__ == "__main__":
    main()
