# Databricks notebook source
"""Shim. Canonical notebook: evals/transaction_cat/slice.py"""

from pathlib import Path
import runpy

dbutils.widgets.text("backend", "adls")
dbutils.widgets.text("sas", "")
dbutils.widgets.text("repo_root", "")

root = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if root.name in {"notebooks", "DE-assistant-framework"}:
    root = root.parent
runpy.run_path(str(root / "evals" / "transaction_cat" / "slice.py"), run_name="__main__")
