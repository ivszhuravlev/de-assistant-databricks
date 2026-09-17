# Databricks notebook source
"""Workspace entry point for the generic generator."""

# COMMAND ----------

import json
import sys
from pathlib import Path

dbutils.widgets.text("config_path", "config/cluster-e2e.json.example")
dbutils.widgets.text("config_json", "{}")
dbutils.widgets.text("repo_root", "")
dbutils.widgets.text("workspace_root", "")
dbutils.widgets.text("cluster_id", "")
dbutils.widgets.text("sas", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from config import GeneratorConfig
from orchestrator import run_generator
from pipeline_helpers import configure_adls_from_secret
from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
config = GeneratorConfig.load(repo_root / dbutils.widgets.get("config_path"))
workspace_root = dbutils.widgets.get("workspace_root").strip()
if workspace_root:
    config.workspace_root = workspace_root
cluster_id = dbutils.widgets.get("cluster_id").strip()
if cluster_id:
    config.temp_existing_cluster_id = cluster_id
if (config.raw_backend or "adls") == "adls":
    configure_adls_from_secret(spark, dbutils, sas_token=dbutils.widgets.get("sas").strip() or None)
outcomes = run_generator(config, repo_root, spark=spark)
print(json.dumps(outcomes, indent=2))
