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

    def complete(self, messages: list[dict[str, Any]], tools: list[dict[str, Any]]) -> dict[str, Any]:
        response = self.workspace.api_client.do(
            "POST",
            f"/serving-endpoints/{self.endpoint}/invocations",
            body={
                "messages": messages,
                "tools": tools,
                "tool_choice": "auto",
                "temperature": 0,
                "max_tokens": 16384,
            },
        )
        return response["choices"][0]["message"]
