"""Optional hooks. Unused."""

from __future__ import annotations


def judge(result, spark_ui_snapshot=None):
    """Future scoring hook; spark_ui_snapshot is optional post-run optimization evidence."""
    raise NotImplementedError("judge is not configured")


def notify(*_args, **_kwargs) -> None:
    return None
