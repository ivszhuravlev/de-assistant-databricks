"""LLM tool loop for one layer."""

from __future__ import annotations

import json
from typing import Any

from llm_model import DatabricksChatClient
from tools import ReadOnlyTools


def run_tool_loop(
    client: DatabricksChatClient,
    tools: ReadOnlyTools,
    system_prompt: str,
    layer: str,
    max_rounds: int,
    prior_messages: list[dict[str, Any]] | None = None,
    retry_feedback: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if prior_messages is None:
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": f"Generate the {layer} layer. Inspect all relevant declared sources first.",
            },
        ]
    else:
        messages = list(prior_messages)
        messages.append(
            {
                "role": "user",
                "content": retry_feedback
                or "The previous attempt failed. Fix it and return the complete JSON again.",
            }
        )
    for _ in range(max_rounds):
        try:
            message = client.complete(messages, tools.definitions())
        except Exception as exc:
            setattr(exc, "prior_messages", messages)
            raise
        messages.append(message)
        tool_calls = message.get("tool_calls") or []
        if not tool_calls:
            content = message.get("content") or ""
            try:
                return json.loads(_extract_json(content)), messages
            except Exception as exc:
                setattr(exc, "prior_messages", messages)
                raise
        for call in tool_calls:
            function = call["function"]
            try:
                result = tools.call(function["name"], json.loads(function.get("arguments") or "{}"))
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


def _extract_json(content: str) -> str:
    text = content.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(lines[1:-1])
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < start:
        raise ValueError("Model response did not contain a JSON object")
    return text[start : end + 1]
