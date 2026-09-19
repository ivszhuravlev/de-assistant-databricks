# Databricks notebook source
"""Workspace entry point for the generic generator."""

# COMMAND ----------

import sys
from pathlib import Path

dbutils.widgets.text("config_path", "config/cluster-e2e.json.example")
dbutils.widgets.text("config_json", "{}")
dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("workspace_root", "")
dbutils.widgets.text("cluster_id", "")
dbutils.widgets.text("sas", "")
dbutils.widgets.text("promote_to_whitelist", "false")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from config import GeneratorConfig
from llm_agent import _info
from orchestrator import run_generator
from pipeline_helpers import configure_adls_from_secret, configure_session_spark
from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
config = GeneratorConfig.load(repo_root / dbutils.widgets.get("config_path"))
workspace_root = dbutils.widgets.get("workspace_root").strip()
if workspace_root:
    config.workspace_root = workspace_root
cluster_id = dbutils.widgets.get("cluster_id").strip()
if cluster_id:
    config.temp_existing_cluster_id = cluster_id
if dbutils.widgets.get("promote_to_whitelist").strip().lower() in {"true", "1", "yes"}:
    config.promote_to_whitelist = True
configure_session_spark(spark)
if (config.raw_backend or "adls") == "adls":
    configure_adls_from_secret(spark, dbutils, sas_token=dbutils.widgets.get("sas").strip() or None)
outcomes = run_generator(config, repo_root, spark=spark)
for outcome in outcomes:
    execution = outcome.get("execution") or {}
    _info(
        "done "
        f"layer={outcome.get('layer')} attempt={outcome.get('attempt')} "
        f"execution={execution.get('status') or execution.get('result')}"
    )
