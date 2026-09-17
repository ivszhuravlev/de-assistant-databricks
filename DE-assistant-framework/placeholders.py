"""Optional hooks. Unused."""

from __future__ import annotations


def judge(*_args, **_kwargs):
    raise NotImplementedError("judge is not configured")


def notify(*_args, **_kwargs) -> None:
    return None
