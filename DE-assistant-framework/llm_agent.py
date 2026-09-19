"""LLM tool loop for one layer."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any

from llm_model import DatabricksChatClient
from tools import ReadOnlyTools


_MILESTONE_EVENTS = frozenset(
    {"generator_start", "layer_failed", "layer_passed", "layer_execute"}
)


def _blank() -> None:
    print("", flush=True)
    sys.stdout.flush()


def _info(message: str, *, blank_after: bool = False) -> None:
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{stamp} INFO {message}", flush=True)
    if blank_after:
        _blank()
    sys.stdout.flush()


def emit_event(event: str, **fields: Any) -> None:
    """Watchable notebook line in the same INFO stream as the agent loop."""
    extras = " ".join(
        f"{key}={value}"
        for key, value in fields.items()
        if value is not None
    )
    _info(f"{event} {extras}".rstrip(), blank_after=event in _MILESTONE_EVENTS)


def run_tool_loop(
    client: DatabricksChatClient,
    tools: ReadOnlyTools,
    system_prompt: str,
    layer: str,
    max_rounds: int,
    prior_messages: list[dict[str, Any]] | None = None,
    retry_feedback: str | None = None,
    attempt: int = 1,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    endpoint = getattr(client, "endpoint", "") or ""
    if prior_messages is None:
        _info(f"layer {layer} generation first")
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": f"Generate the {layer} layer. Inspect all relevant declared sources first.",
            },
        ]
    else:
        _info(f"layer {layer} generation retry")
        messages = list(prior_messages)
        messages.append(
            {
                "role": "user",
                "content": retry_feedback
                or "The previous attempt failed. Fix it and return the complete JSON again.",
            }
        )
    _info(f"layer {layer} attempt {attempt} agent loop started, endpoint={endpoint}")
    for turn in range(1, max_rounds + 1):
        try:
            message = client.complete(messages, tools.definitions())
        except Exception as exc:
            setattr(exc, "prior_messages", messages)
            raise
        usage = getattr(client, "last_usage", None) or {}
        tool_calls = message.get("tool_calls") or []
        tool_names = ",".join(
            str((call.get("function") or {}).get("name") or "")
            for call in tool_calls
        ) or "-"
        _info(
            f"layer {layer} attempt {attempt} turn {turn} endpoint={endpoint} "
            f"prompt={usage.get('prompt', 0)} completion={usage.get('completion', 0)} "
            f"tool_calls={len(tool_calls)} tools={tool_names} "
            f"cache_read={usage.get('cache_read', 0)} "
            f"cache_write={usage.get('cache_write', 0)}"
        )
        messages.append(message)
        if not tool_calls:
            content = message.get("content") or ""
            try:
                return json.loads(_extract_json(content)), messages
            except Exception as exc:
                setattr(exc, "prior_messages", messages)
                raise
        for call in tool_calls:
            function = call["function"]
            name = function["name"]
            arguments = function.get("arguments") or "{}"
            _info(f"layer {layer} attempt {attempt} tool {name}")
            try:
                result = tools.call(name, json.loads(arguments or "{}"))
            except Exception as exc:
                result = json.dumps({"error": str(exc)})
            messages.append(
                {"role": "tool", "tool_call_id": call["id"], "content": result}
            )
    error = RuntimeError(f"Model exceeded {max_rounds} tool rounds")
    error.prior_messages = messages
    raise error


def tool_ledger(
    messages: list[dict[str, Any]] | None,
    limit: int = 40,
    clip: int = 400,
) -> list[dict[str, Any]]:
    """Bounded redacted ledger of tool names, args, and results."""
    if not messages:
        return []
    ledger: list[dict[str, Any]] = []
    for message in messages:
        for call in message.get("tool_calls") or []:
            function = call.get("function") or {}
            ledger.append(
                {
                    "name": function.get("name"),
                    "arguments": _clip(function.get("arguments") or "", clip),
                }
            )
        if message.get("role") == "tool":
            ledger.append(
                {
                    "tool_result": True,
                    "tool_call_id": message.get("tool_call_id"),
                    "content": _clip(str(message.get("content") or ""), clip),
                }
            )
    return ledger[-limit:]


def _clip(value: Any, limit: int) -> str:
    text = value if isinstance(value, str) else json.dumps(value, default=str)
    return text if len(text) <= limit else text[:limit] + "…"


_CONTRACT_KEYS = {"layer", "summary", "artifacts", "assumptions"}


def _extract_json(content: str) -> str:
    """Return one JSON object. Haiku often emits a valid object plus trailing JSON."""
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        text = "\n".join(lines)
    decoder = json.JSONDecoder()
    idx = 0
    fallback: dict[str, Any] | None = None
    while idx < len(text):
        start = text.find("{", idx)
        if start < 0:
            break
        try:
            obj, end = decoder.raw_decode(text, start)
        except json.JSONDecodeError:
            idx = start + 1
            continue
        if isinstance(obj, dict):
            if _CONTRACT_KEYS <= set(obj):
                return json.dumps(obj)
            if fallback is None:
                fallback = obj
        idx = max(end, start + 1)
    if fallback is not None:
        return json.dumps(fallback)
    raise ValueError("Model response did not contain a JSON object")
