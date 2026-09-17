"""Parked semantic retrieval interface; keyword search lives in retrieval.py."""


def semantic_search(*_args, **_kwargs) -> dict[str, str]:
    """Return status only; embeddings and Vector Search are intentionally absent."""
    return {"status": "parked"}
