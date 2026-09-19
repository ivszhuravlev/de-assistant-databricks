"""Databricks Model Serving chat client."""

from __future__ import annotations

from typing import Any


class DatabricksChatClient:
    def __init__(self, endpoint: str):
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError as exc:
            raise RuntimeError("Install project dependencies before calling the model") from exc
        self.endpoint = endpoint
        self.workspace = WorkspaceClient()
        self.last_usage: dict[str, int] = {}

    def complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        response = self.workspace.api_client.do(
            "POST",
            f"/serving-endpoints/{self.endpoint}/invocations",
            body=build_request_body(messages, tools),
        )
        self.last_usage = usage_from_response(response)
        return response["choices"][0]["message"]


def build_request_body(
    messages: list[dict[str, Any]], tools: list[dict[str, Any]]
) -> dict[str, Any]:
    """Build an invocation body with cache markers on stable prompt inputs."""
    request_messages = []
    for message in messages:
        request_message = dict(message)
        if request_message.get("role") == "system" and isinstance(
            request_message.get("content"), str
        ):
            request_message["content"] = [
                {
                    "type": "text",
                    "text": request_message["content"],
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        request_messages.append(request_message)

    request_tools = [dict(tool) for tool in tools]
    if request_tools:
        request_tools[-1]["cache_control"] = {"type": "ephemeral"}

    return {
        "messages": request_messages,
        "tools": request_tools,
        "tool_choice": "auto",
        "temperature": 0,
        "max_tokens": 16384,
    }


def usage_from_response(response: dict[str, Any] | None) -> dict[str, int]:
    """Normalize serving usage. Missing fields become 0."""
    payload = response or {}
    usage = payload.get("usage") or {}
    if not isinstance(usage, dict):
        usage = {}
    details = usage.get("prompt_tokens_details") or usage.get("input_tokens_details") or {}
    if not isinstance(details, dict):
        details = {}
    return {
        "prompt": _as_int(usage.get("prompt_tokens") or usage.get("input_tokens")),
        "completion": _as_int(usage.get("completion_tokens") or usage.get("output_tokens")),
        "cache_read": _as_int(
            usage.get("cache_read_input_tokens")
            or usage.get("cached_tokens")
            or details.get("cached_tokens")
            or details.get("cache_read_input_tokens")
            or payload.get("cache_read_input_tokens")
        ),
        "cache_write": _as_int(
            usage.get("cache_creation_input_tokens")
            or usage.get("cache_write_tokens")
            or details.get("cache_creation_input_tokens")
            or payload.get("cache_creation_input_tokens")
        ),
    }


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
