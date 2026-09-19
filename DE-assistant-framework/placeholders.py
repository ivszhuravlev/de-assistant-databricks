"""No-op hook used by the operator eval slices."""

from __future__ import annotations


def notify(*_args, **_kwargs) -> None:
    return None
