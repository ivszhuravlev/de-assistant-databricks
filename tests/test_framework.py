import json
from pathlib import Path

import pytest

from config import GeneratorConfig
from job_assembler import assemble_job_payload
from pipeline_helpers import load_paths
from prompts import build_task_prompt
from tools import ReadOnlyTools
from validation import validate_layer
from workspace_paths import default_repo_root
import orchestrator


def config() -> GeneratorConfig:
    return GeneratorConfig(
        project_name="test",
        input_root="spec",
        contract_file="contracts/layer_output.schema.json",
        output_root="generated",
        workspace_root="/Users/test/project",
        metadata_path="dbfs:/test/metadata",
        source_map=[{"id": "spec", "path": "pipeline-spec.json", "description": "test"}],
    )


def valid_result() -> dict:
    return {
        "layer": "bronze",
        "summary": "Generated bronze.",
        "artifacts": [
            {
                "path": "notebooks/bronze.py",
                "kind": "databricks_notebook",
                "content": (
                    "# Databricks notebook source\n"
                    "def transform(spark, config: dict) -> None:\n"
                    "    return None\n"
                ),
            }
        ],
        "assumptions": ["UNKNOWN: source path"],
    }


def test_static_validation_accepts_valid_notebook():
    assert validate_layer(valid_result(), "bronze") == []


def test_static_validation_rejects_self_referential_assign():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    fact_count = audit_table('gold', 'fct_trips', extra_results=[\n"
        "        quality_result('gold', 'fct_trips', 'match', fact_count == 1, fact_count)\n"
        "    ])\n"
    )
    assert any("fact_count" in error for error in validate_layer(result, "bronze"))


def test_static_validation_rejects_empty_config_call():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    return None\n"
        "transform(spark, {})\n"
    )
    assert any("config_json" in error for error in validate_layer(result, "bronze"))


def test_static_validation_rejects_helpers_import():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "import helpers\n"
        "def transform(spark, config: dict) -> None:\n"
        "    return None\n"
    )
    assert any("helpers module" in error for error in validate_layer(result, "bronze"))


def test_static_validation_rejects_path_escape():
    result = valid_result()
    result["artifacts"][0]["path"] = "../bronze.py"
    assert any("Unsafe artifact path" in error for error in validate_layer(result, "bronze"))


def test_gold_missing_pipeline_run_write_is_rejected():
    result = valid_result()
    result["layer"] = "gold"
    result["artifacts"][0]["path"] = "notebooks/gold.py"
    errors = validate_layer(result, "gold")
    assert any("PIPELINE_RUN" in error for error in errors)


def test_pipeline_run_write_requirement_is_gold_only():
    for layer in ("bronze", "silver"):
        result = valid_result()
        result["layer"] = layer
        result["artifacts"][0]["path"] = f"notebooks/{layer}.py"
        assert not any("PIPELINE_RUN" in error for error in validate_layer(result, layer))


def test_gold_pipeline_run_write_is_accepted():
    result = valid_result()
    result["layer"] = "gold"
    result["artifacts"][0]["path"] = "notebooks/gold.py"
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    append_metadata_rows(spark, paths, PIPELINE_RUN, [\n"
        "        pipeline_run_row(pipeline_run_id=config['pipeline_run_id'], "
        "pipeline_name=config['project_name'], status='SUCCESS')\n"
        "    ])\n"
    )
    assert validate_layer(result, "gold") == []


def test_gold_pipeline_run_string_table_is_accepted():
    result = valid_result()
    result["layer"] = "gold"
    result["artifacts"][0]["path"] = "notebooks/gold.py"
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    append_metadata_rows(spark, paths, 'pipeline_run', [\n"
        "        pipeline_run_row(pipeline_run_id=config['pipeline_run_id'], "
        "pipeline_name=config['project_name'], status='SUCCESS')\n"
        "    ])\n"
    )
    assert validate_layer(result, "gold") == []


def test_read_only_tools_only_read_declared_source(tmp_path: Path):
    (tmp_path / "spec").mkdir()
    (tmp_path / "contracts").mkdir()
    (tmp_path / "spec" / "pipeline-spec.json").write_text('{"ok": true}')
    (tmp_path / "contracts" / "layer_output.schema.json").write_text("{}")
    tools = ReadOnlyTools(config(), tmp_path)
    assert json.loads(tools.call("list_sources", {})) == config().source_map
    assert json.loads(tools.call("read_source", {"source_id": "spec"})) == {"ok": True}
    with pytest.raises(ValueError):
        tools.call("write_source", {})


def test_retry_prompt_contains_previous_error():
    prompt = build_task_prompt("bronze", "AnalysisException: missing LocationID")
    assert "previous attempt for this layer failed" in prompt
    assert "AnalysisException: missing LocationID" in prompt


def test_system_prompt_is_not_the_taxi_eval():
    prompt = build_task_prompt("silver")
    leaked = (
        "working eval pipeline",
        "trip_type=1",
        "Unknown Zone",
        "taxi_zone_lookup.csv",
        "fct_monthly_zone_revenue",
        "copy the eval",
        "Yellow: trip_type",
    )
    assert all(item not in prompt for item in leaked)
    assert "pipeline_brief" in prompt


def test_generator_retries_with_same_conversation(tmp_path, monkeypatch):
    cfg = config()
    cfg.layers = ["bronze"]
    cfg.max_attempts = 2
    calls = []

    class Tools:
        @staticmethod
        def definitions():
            return []

    class Client:
        workspace = object()

        def complete(self, messages, _tools):
            calls.append(list(messages))
            result = valid_result()
            if len(calls) == 1:
                result["layer"] = "silver"
            return {"role": "assistant", "content": json.dumps(result)}

    monkeypatch.setattr(orchestrator, "DatabricksChatClient", lambda _endpoint: Client())
    monkeypatch.setattr(
        orchestrator, "make_tools", lambda *_args: (Tools(), load_paths(storage_root="dbfs:/test"))
    )

    outcomes = orchestrator.run_generator(cfg, tmp_path)

    assert outcomes[0]["attempt"] == 2
    assert len(calls) == 2
    assert sum(message["role"] == "system" for message in calls[1]) == 1
    assert any(
        message["role"] == "user" and "Expected layer bronze" in message["content"]
        for message in calls[1]
    )


def test_job_assembler_builds_executor_payload():
    payload = assemble_job_payload(
        "test",
        ["/Workspace/a", "/Workspace/b"],
        existing_cluster_id="cluster-1",
        parameters={"config_json": "{}"},
    )
    assert payload["tasks"][1]["depends_on"][0]["task_key"] == payload["tasks"][0]["task_key"]
    assert payload["tasks"][0]["existing_cluster_id"] == "cluster-1"
    assert payload["tasks"][0]["notebook_task"]["base_parameters"]["config_json"] == "{}"


def test_context_tools_are_registered():
    names = {item["function"]["name"] for item in ReadOnlyTools(config(), Path(".")).definitions()}
    assert {
        "list_raw_files",
        "peek_raw",
        "profile_column",
        "get_distinct_values",
        "peek_table",
        "list_tables",
        "ask_clarification",
        "get_last_error",
        "read_log",
        "list_successful_runs",
        "read_tested_pipeline",
        "spark_ui_applications",
        "spark_ui_failed_jobs",
        "semantic_search",
        "search_previous_errors",
    } <= names
    assert "ask_clarification" in names
    tools = ReadOnlyTools(config(), Path("."))
    asked = json.loads(tools.call("ask_clarification", {"question": "What is the grain?"}))
    assert asked["status"] == "unanswered"


def test_data_tools_require_spark():
    tools = ReadOnlyTools(config(), Path("."))
    assert json.loads(tools.call("peek_raw", {"path": "dbfs:/de-assist-databricks/raw/taxi_data/x.csv"}))[
        "error"
    ]
    assert json.loads(tools.call("spark_ui_applications", {}))["error"]
    assert json.loads(tools.call("spark_ui_failed_jobs", {}))["error"]
    assert json.loads(tools.call("semantic_search", {"query": "zones"}))["status"] == "parked"


def test_peek_raw_out_of_root_returns_json():
    tools = ReadOnlyTools(config(), Path("."), spark=object())
    payload = json.loads(tools.call("peek_raw", {"path": "dbfs:/somewhere/else.csv"}))
    assert payload["error"]


def test_tool_schemas_declare_required_args():
    defs = {item["function"]["name"]: item["function"]["parameters"] for item in ReadOnlyTools(config(), Path(".")).definitions()}
    assert defs["read_source"]["required"] == ["source_id"]
    assert defs["peek_raw"]["required"] == ["path"]
    assert defs["profile_column"]["required"] == ["target", "column"]
    assert defs["get_distinct_values"]["required"] == ["target", "column"]
    assert defs["peek_table"]["required"] == ["layer", "table"]
    assert defs["search_previous_errors"]["required"] == ["query"]
    assert defs["get_last_error"]["required"] == ["layer"]
    assert defs["read_log"]["required"] == ["run_id", "layer"]


def test_eval_pipeline_is_hidden_from_generator_tools():
    root = Path(__file__).resolve().parents[1]
    tools = ReadOnlyTools(config(), root)
    missing = json.loads(tools.call("read_tested_pipeline", {"pipeline_id": "eval_taxi"}))
    assert missing.get("error")
    ids = {item.get("id") for item in missing.get("available") or []}
    assert "eval_taxi" not in ids
    sample = json.loads(tools.call("read_tested_pipeline", {"pipeline_id": "tool_smoke_sample"}))
    assert sample["id"] == "tool_smoke_sample"
    assert "taxi_vertical_slice" not in sample.get("content", "")


def test_get_last_error_uses_pipeline_and_layer():
    records = [
        {
            "pipeline": "test",
            "layer": "silver",
            "status": "FAILED",
            "error": "older",
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "pipeline": "test",
            "layer": "silver",
            "status": "FAILED",
            "error": "newer",
            "created_at": "2026-01-02T00:00:00Z",
        },
    ]
    tools = ReadOnlyTools(config(), Path("."), error_records=records)
    payload = json.loads(tools.call("get_last_error", {"layer": "silver"}))
    assert payload["last_error"]["error"] == "newer"


def test_read_tested_pipeline_returns_requested_slice(tmp_path: Path):
    (tmp_path / "spec").mkdir()
    (tmp_path / "contracts").mkdir()
    (tmp_path / "notebooks").mkdir()
    (tmp_path / "contracts" / "layer_output.schema.json").write_text("{}")
    (tmp_path / "notebooks" / "example.py").write_text("zero\none\ntwo\nthree\n")
    (tmp_path / "notebooks" / "taxi_vertical_slice.py").write_text("SECRET_EVAL\n")
    (tmp_path / "spec" / "tested-pipelines.json").write_text(
        json.dumps(
            {
                "pipelines": [
                    {
                        "id": "example",
                        "layer": "bronze",
                        "path": "notebooks/example.py",
                        "status": "tested",
                    },
                    {
                        "id": "eval_taxi",
                        "layer": "all",
                        "path": "notebooks/taxi_vertical_slice.py",
                        "status": "tested",
                    },
                ]
            }
        )
    )
    tools = ReadOnlyTools(config(), tmp_path)
    payload = json.loads(
        tools.call(
            "read_tested_pipeline",
            {"pipeline_id": "example", "start_line": 1, "num_lines": 2},
        )
    )
    assert payload["content"] == "one\ntwo\n"
    assert payload["total_lines"] == 4
    hidden = json.loads(tools.call("read_tested_pipeline", {"pipeline_id": "eval_taxi"}))
    assert hidden.get("error")
    assert "SECRET_EVAL" not in json.dumps(hidden)


def test_adls_account_env_is_required(monkeypatch):
    monkeypatch.delenv("DE_ASSIST_ADLS_ACCOUNT", raising=False)
    try:
        load_paths("adls")
    except RuntimeError as exc:
        assert "DE_ASSIST_ADLS_ACCOUNT is required" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_adls_raw_stays_on_account_and_delta_stays_on_dbfs():
    paths = load_paths("adls")
    assert paths.backend == "adls"
    assert paths.raw_root == "abfss://raw@exampleaccount.dfs.core.windows.net/taxi_data"
    assert paths.raw("yellow", "yellow_tripdata_2021-01.csv.gz").startswith(paths.raw_root)
    assert paths.delta_root.startswith("dbfs:/")
    assert paths.log_root.startswith("dbfs:/")


def test_generated_hive_and_delta_do_not_collide_with_eval():
    eval_paths = load_paths("adls", output_space="eval")
    gen_paths = load_paths("adls", output_space="generated")
    assert eval_paths.table("bronze", "yellow_tripdata") == "bronze.yellow_tripdata"
    assert gen_paths.table("bronze", "yellow_tripdata") == "gen_bronze.yellow_tripdata"
    assert eval_paths.delta_root == "dbfs:/de-assist-databricks/delta"
    assert gen_paths.delta_root == "dbfs:/de-assist-databricks/delta/generated"
    assert eval_paths.delta("bronze", "yellow_tripdata") != gen_paths.delta(
        "bronze", "yellow_tripdata"
    )
    assert eval_paths.raw_root == gen_paths.raw_root
    assert eval_paths.ops_delta("data_quality") == gen_paths.ops_delta("data_quality")
    assert eval_paths.ops_delta("data_quality") == (
        "dbfs:/de-assist-databricks/delta/de_assist/data_quality"
    )


def test_transaction_and_retail_eval_paths_do_not_collide_with_taxi():
    taxi = load_paths("adls")
    tx = load_paths("adls", pipeline="transaction_cat")
    retail = load_paths("adls", pipeline="fresh_retail")
    gen_tx = load_paths("adls", output_space="generated", pipeline="transaction_cat")
    gen_retail = load_paths("adls", output_space="generated", pipeline="fresh_retail")
    assert taxi.table("bronze", "yellow_tripdata") == "bronze.yellow_tripdata"
    assert tx.table("bronze", "transactions") == "tx_bronze.transactions"
    assert retail.table("bronze", "daily_sales_train") == "retail_bronze.daily_sales_train"
    assert gen_tx.table("bronze", "transactions") == "gen_tx_bronze.transactions"
    assert gen_retail.table("gold", "fct_store_daily") == "gen_retail_gold.fct_store_daily"
    assert tx.raw_root.endswith("/transaction_cat")
    assert retail.raw_root.startswith("abfss://raw@exampleaccount.dfs.core.windows.net/")
    assert retail.raw_root != taxi.raw_root
    assert retail.raw_root != tx.raw_root
    assert tx.delta_root == "dbfs:/de-assist-databricks/delta/tx"
    assert gen_tx.delta_root == "dbfs:/de-assist-databricks/delta/tx/generated"
    assert retail.delta_root == "dbfs:/de-assist-databricks/delta/retail"
    assert taxi.delta_root == "dbfs:/de-assist-databricks/delta"
    assert taxi.raw_root != tx.raw_root
    assert tx.delta("bronze", "transactions") != taxi.delta("bronze", "transactions")
    assert tx.ops_delta("data_quality") == taxi.ops_delta("data_quality")


def test_align_metadata_rows_rejects_wrong_helper():
    from metadata import DATA_QUALITY, LAYER_RUN, align_metadata_rows, layer_run_row

    row = layer_run_row(
        pipeline_run_id="p",
        layer="bronze",
        table_name="yellow_tripdata",
        row_count=1,
        status="SUCCESS",
    )
    try:
        align_metadata_rows(DATA_QUALITY, [row])
    except TypeError as exc:
        assert "data_quality_row" in str(exc)
    else:
        raise AssertionError("expected TypeError")
    aligned = align_metadata_rows(LAYER_RUN, [row])
    assert aligned[0]["row_count"] == 1
    assert "check_name" not in aligned[0]


def test_validate_rejects_layer_run_rows_on_data_quality():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    append_metadata_rows(spark, paths, DATA_QUALITY, [layer_run_row(\n"
        "        pipeline_run_id='p', layer='bronze', table_name='t', row_count=1, status='SUCCESS'\n"
        "    )])\n"
    )
    assert any("data_quality_row" in error for error in validate_layer(result, "bronze"))


def test_validate_rejects_name_guard():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    return None\n"
        "if __name__ != '__main__':\n"
        "    transform(spark, config)\n"
    )
    assert any("__name__" in error for error in validate_layer(result, "bronze"))


def test_validate_rejects_write_delta_for_published_table():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    write_delta(df, spark, paths, 'bronze', 'yellow_tripdata')\n"
        "    data_quality_row(pipeline_run_id='p', layer='bronze', "
        "table_name='yellow_tripdata', check_name='ok', passed=True)\n"
        "    layer_run_row(pipeline_run_id='p', layer='bronze', "
        "table_name='yellow_tripdata', row_count=1, status='SUCCESS')\n"
    )
    errors = validate_layer(result, "bronze")
    assert any("write_delta" in error and "publish_staged" in error for error in errors)


def test_prompt_forbids_name_guard_and_direct_write_delta():
    prompt = build_task_prompt("silver")
    assert "Never mention __name__" in prompt
    assert "Do not call write_delta for a published table" in prompt
    assert "unconditionally" in prompt


def test_validate_rejects_published_table_without_data_quality_row():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    publish_staged(spark, paths, 'bronze', 'yellow_tripdata')\n"
        "    layer_run_row('p', 'bronze', 'yellow_tripdata', 1, 'SUCCESS')\n"
    )
    errors = validate_layer(result, "bronze")
    assert any("data_quality_row for the same table" in error for error in errors)


def test_validate_accepts_metadata_rows_for_published_table():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    publish_staged(spark, paths, 'bronze', table='yellow_tripdata')\n"
        "    data_quality_row('p', 'bronze', table_name='yellow_tripdata', "
        "check_name='row_count_positive', passed=True)\n"
        "    layer_run_row('p', 'bronze', table_name='yellow_tripdata', "
        "row_count=1, status='SUCCESS')\n"
    )
    assert validate_layer(result, "bronze") == []


def test_validate_rejects_hardcoded_hive_names():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    spark.table('bronze.yellow_tripdata')\n"
    )
    assert any("hard-code" in error for error in validate_layer(result, "bronze"))


def test_validate_rejects_hardcoded_sql_schema_and_prefixed_hive():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    spark.sql('SELECT * FROM gold.fct_daily_sales')\n"
    )
    errors = validate_layer(result, "bronze")
    assert any("Hive schema in SQL" in error for error in errors)
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    spark.table('gen_retail_gold.fct_daily_sales')\n"
    )
    assert any("hard-code" in error for error in validate_layer(result, "bronze"))
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    spark.sql(f'SELECT * FROM {paths.table(\"gold\", \"fct_daily_sales\")}')\n"
    )
    assert not any("Hive schema" in error or "hard-code" in error for error in validate_layer(result, "bronze"))


def test_validate_rejects_spark_sql_on_gold():
    result = valid_result()
    result["layer"] = "gold"
    result["artifacts"][0]["path"] = "notebooks/gold.py"
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    spark.sql(f\"SELECT * FROM {paths.table('gold', 'fct_daily_sales')}\")\n"
        "    append_metadata_rows(spark, paths, PIPELINE_RUN, [pipeline_run_row("
        "pipeline_run_id=config['pipeline_run_id'], pipeline_name='', status='SUCCESS')])\n"
    )
    errors = validate_layer(result, "gold")
    assert any("must not call spark.sql" in error for error in errors)


def test_validate_rejects_hardcoded_output_space_eval():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        '    paths = load_paths(backend, output_space="eval")\n'
    )
    errors = validate_layer(result, "bronze")
    assert any("output_space=eval" in error for error in errors)


def test_validate_accepts_config_output_space():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        '    paths = load_paths(backend, output_space=config["output_space"])\n'
    )
    assert not any("output_space" in error for error in validate_layer(result, "bronze"))


def test_validate_rejects_load_paths_without_output_space():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    paths = load_paths(backend)\n"
    )
    assert any("output_space" in error for error in validate_layer(result, "bronze"))


def test_validate_rejects_grain_list_as_duplicate_groups():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    validate_counts(fqtn, row_count, missing, unique_grain)\n"
    )
    assert any("duplicate_groups" in error for error in validate_layer(result, "bronze"))


def test_validate_counts_rejects_grain_list_at_runtime():
    from pipeline_helpers import validate_counts

    try:
        validate_counts("silver.trips", 1, [], ["vendor_id"])
    except TypeError as exc:
        assert "int count" in str(exc)
    else:
        raise AssertionError("expected TypeError")


def test_validate_rejects_hardcoded_create_database():
    result = valid_result()
    result["artifacts"][0]["content"] = (
        "# Databricks notebook source\n"
        "def transform(spark, config: dict) -> None:\n"
        "    spark.sql('CREATE DATABASE IF NOT EXISTS bronze')\n"
    )
    assert any("hard-codes a Hive database" in error for error in validate_layer(result, "bronze"))


def test_config_json_includes_output_space():
    root = Path(__file__).resolve().parents[1]
    cfg = GeneratorConfig.load(root / "config" / "cluster-e2e.json.example")
    assert cfg.output_space == "generated"
    assert cfg.raw_backend == "adls"


def test_config_environment_overrides_local_identity(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "config.json"
    values = config().__dict__
    values["workspace_root"] = ""
    values["temp_existing_cluster_id"] = ""
    config_path.write_text(json.dumps(values), encoding="utf-8")
    monkeypatch.setenv("DE_ASSIST_WORKSPACE_ROOT", "/workspace/operator/repo")
    monkeypatch.setenv("DE_ASSIST_CLUSTER_ID", "cluster-from-environment")

    cfg = GeneratorConfig.load(config_path)

    assert cfg.workspace_root == "/workspace/operator/repo"
    assert cfg.temp_existing_cluster_id == "cluster-from-environment"


def test_default_repo_root_prefers_widget_then_config_json(tmp_path: Path):
    class Widgets:
        values = {"repo_root": str(tmp_path), "config_json": json.dumps({"repo_root": "other"})}

        def get(self, name):
            return self.values[name]

    class Dbutils:
        widgets = Widgets()

    assert default_repo_root(Dbutils()) == str(tmp_path)
    Dbutils.widgets.values["repo_root"] = ""
    assert default_repo_root(Dbutils()) == "other"


def test_notify_is_a_stub():
    from placeholders import notify

    notify({"status": "SUCCESS", "pipeline_run_id": "x"})


def test_extract_json_keeps_first_complete_object_when_model_appends_more():
    from llm_agent import _extract_json

    payload = {
        "layer": "silver",
        "summary": "ok",
        "artifacts": [{"path": "notebooks/silver.py", "kind": "databricks_notebook", "content": "# Databricks notebook source\n"}],
        "assumptions": [],
    }
    blob = json.dumps(payload) + "\n" + json.dumps({"note": "trailing"})
    assert json.loads(_extract_json(blob))["layer"] == "silver"


def test_extract_json_prefers_contract_object_after_preamble():
    from llm_agent import _extract_json

    payload = {
        "layer": "gold",
        "summary": "ok",
        "artifacts": [{"path": "notebooks/gold.py", "kind": "databricks_notebook", "content": "# Databricks notebook source\n"}],
        "assumptions": ["x"],
    }
    blob = json.dumps({"status": "thinking"}) + "\n" + json.dumps(payload)
    parsed = json.loads(_extract_json(blob))
    assert parsed["layer"] == "gold"
    assert parsed["assumptions"] == ["x"]


def test_tool_ledger_is_bounded_and_redacts_nothing_but_length():
    from llm_agent import tool_ledger

    long_args = json.dumps({"path": "x" * 500})
    messages = [
        {
            "role": "assistant",
            "tool_calls": [{"function": {"name": "peek_raw", "arguments": long_args}}],
        },
        {"role": "tool", "tool_call_id": "1", "content": "y" * 500},
        {"role": "assistant", "content": "{}"},
    ]
    ledger = tool_ledger(messages, limit=10, clip=40)
    assert ledger[0]["name"] == "peek_raw"
    assert ledger[0]["arguments"].endswith("…")
    assert ledger[1]["tool_result"] is True
    assert ledger[1]["content"].endswith("…")


def test_read_log_uses_attempt_dir_and_drops_generated_code():
    from tools import _slim_log_line

    slim = json.loads(
        _slim_log_line(
            json.dumps(
                {
                    "status": "FAILED",
                    "error": "boom",
                    "diagnostics": {"attempt": 1, "generated_code": "SECRET_NOTEBOOK"},
                }
            )
        )
    )
    assert slim["error"] == "boom"
    assert "generated_code" not in slim["diagnostics"]

    class Spark:
        def __init__(self, files):
            self.files = files
            self.read = self

        def text(self, path):
            if path not in self.files:
                raise FileNotFoundError(path)
            self._rows = self.files[path]
            return self

        def collect(self):
            return [{"value": row} for row in self._rows]

    from pipeline_helpers import load_paths

    attempt = "dbfs:/de-assist-databricks/logs/generator/run-9/bronze"
    tools = ReadOnlyTools(
        config(),
        Path("."),
        spark=Spark(
            {
                attempt: [
                    json.dumps(
                        {
                            "status": "FAILED",
                            "diagnostics": {"generated_code": "NOTEBOOK"},
                        }
                    )
                ]
            }
        ),
        runtime_paths=load_paths("dbfs"),
    )
    payload = json.loads(tools.call("read_log", {"run_id": "run-9", "layer": "bronze"}))
    assert payload["path"] == attempt
    assert "NOTEBOOK" not in payload["lines"][0]


def test_ensure_schema_resets_hostile_session_spark():
    from pipeline_helpers import SESSION_SPARK_CONF, ensure_schema

    seen = {}

    class Spark:
        class conf:
            @staticmethod
            def set(key, value):
                seen[key] = value

        @staticmethod
        def sql(_statement):
            return None

    ensure_schema(Spark(), load_paths(storage_root="dbfs:/test"))
    assert seen == SESSION_SPARK_CONF


def test_timeout_is_not_retried(tmp_path, monkeypatch):
    cfg = config()
    cfg.layers = ["bronze"]
    cfg.max_attempts = 3
    cfg.execute_generated = True
    cfg.temp_existing_cluster_id = "cluster-1"
    calls = []

    class Tools:
        @staticmethod
        def definitions():
            return []

    class Client:
        workspace = object()

        def complete(self, messages, _tools):
            calls.append(list(messages))
            return {"role": "assistant", "content": json.dumps(valid_result())}

    monkeypatch.setattr(orchestrator, "DatabricksChatClient", lambda _endpoint: Client())
    monkeypatch.setattr(
        orchestrator, "make_tools", lambda *_args: (Tools(), load_paths(storage_root="dbfs:/test"))
    )
    monkeypatch.setattr(
        orchestrator,
        "execute_with_temp_job",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            TimeoutError("Temporary job exceeded 1800 seconds")
        ),
    )

    with pytest.raises(RuntimeError, match="TimeoutError"):
        orchestrator.run_generator(cfg, tmp_path)
    assert len(calls) == 1


def test_canceled_execute_is_not_retried(tmp_path, monkeypatch):
    cfg = config()
    cfg.layers = ["bronze"]
    cfg.max_attempts = 3
    cfg.execute_generated = True
    cfg.temp_existing_cluster_id = "cluster-1"
    calls = []

    class Tools:
        @staticmethod
        def definitions():
            return []

    class Client:
        workspace = object()

        def complete(self, messages, _tools):
            calls.append(1)
            return {"role": "assistant", "content": json.dumps(valid_result())}

    monkeypatch.setattr(orchestrator, "DatabricksChatClient", lambda _endpoint: Client())
    monkeypatch.setattr(
        orchestrator, "make_tools", lambda *_args: (Tools(), load_paths(storage_root="dbfs:/test"))
    )

    def canceled(*_args, **_kwargs):
        raise RuntimeError("Temporary job ended as TERMINATED/CANCELED")

    monkeypatch.setattr(orchestrator, "execute_with_temp_job", canceled)

    with pytest.raises(RuntimeError, match="CANCELED"):
        orchestrator.run_generator(cfg, tmp_path)
    assert len(calls) == 1
