import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from config import GeneratorConfig
from executor import _failed_run_detail
from pipeline_helpers import (
    actual_column,
    load_paths,
    missing_columns,
    validate_counts,
    write_delta,
    write_run_log,
)
from retrieval import (
    error_record,
    find_past_fix,
    get_last_error,
    keyword_search,
    persist_error,
    tokenize,
)
from tools import ReadOnlyTools, make_tools


def test_keyword_search_finds_previous_failed_run():
    older = error_record(
        pipeline="nyc_taxi_january_2021",
        layer="gold",
        table="bronze_taxi_zone_lookup",
        error_type="RuntimeError",
        error="DQ failed for bronze.taxi_zone_lookup: missing required columns: ['LocationID', 'Zone']",
        diagnostics={"backend": "dbfs"},
    )
    older["created_at"] = "2026-09-16T19:00:00+00:00"
    newer = error_record(
        pipeline="nyc_taxi_january_2021",
        layer="gold",
        table="bronze_taxi_zone_lookup",
        error_type="RuntimeError",
        error="DQ failed for bronze.taxi_zone_lookup: missing required columns: ['LocationID', 'Zone']",
        diagnostics={"backend": "dbfs"},
    )
    newer["created_at"] = "2026-09-16T20:00:00+00:00"
    other = error_record(
        pipeline="other",
        error="Out of memory on shuffle",
        error_type="Py4JJavaError",
    )
    hits = keyword_search([older, newer, other], "LocationID zone lookup")
    assert hits[0]["created_at"] == newer["created_at"]
    assert keyword_search([older, newer, other], "shuffle memory")[0]["pipeline"] == "other"
    mixed = keyword_search([older, newer, other], "AnalysisException LocationID missing")
    assert mixed[0]["table"] == "bronze_taxi_zone_lookup"


def test_tokenize_ignores_punctuation_and_tiny_tokens():
    assert tokenize("DQ failed: LocationID, Zone") == [
        "dq",
        "failed",
        "locationid",
        "zone",
    ]


def test_generator_can_search_previous_errors(tmp_path):
    (tmp_path / "spec").mkdir()
    (tmp_path / "contracts").mkdir()
    (tmp_path / "spec" / "pipeline-spec.json").write_text("{}", encoding="utf-8")
    (tmp_path / "contracts" / "layer_output.schema.json").write_text("{}", encoding="utf-8")
    config = GeneratorConfig(
        project_name="test",
        input_root="spec",
        contract_file="contracts/layer_output.schema.json",
        output_root="generated",
        workspace_root="/Users/test/project",
        metadata_path="dbfs:/test/metadata",
        source_map=[{"id": "spec", "path": "pipeline-spec.json", "description": "test"}],
    )
    tools = ReadOnlyTools(
        config,
        tmp_path,
        error_records=[
            error_record(
                pipeline="nyc_taxi_january_2021",
                error="AnalysisException: Column LocationID not found",
                error_type="AnalysisException",
            )
        ],
    )
    names = [item["function"]["name"] for item in tools.definitions()]
    assert "search_previous_errors" in names
    hits = json.loads(
        tools.call("search_previous_errors", {"query": "LocationID AnalysisException", "limit": 5})
    )
    assert len(hits) == 1
    assert hits[0]["error_type"] == "AnalysisException"


def test_dq_helpers_are_case_insensitive():
    assert actual_column(["locationid", "zone"], "LocationID") == "locationid"
    assert missing_columns(["locationid", "zone"], ["LocationID", "Borough"]) == ["Borough"]
    assert validate_counts("silver.trips", 0, [], None)
    assert not validate_counts("silver.trips", 10, [], 0)


def test_error_paths_stay_on_dbfs_hive():
    paths = load_paths("dbfs", schema="other_schema", error_root="dbfs:/custom/errors")
    assert paths.schema == "other_schema"
    assert paths.error_root == "dbfs:/custom/errors"
    assert paths.error_table() == "de_assist.run_errors"
    assert paths.catalog == "hive_metastore"


def test_canonical_error_path_is_pinned():
    canonical = "dbfs:/de-assist-databricks/delta/de_assist/run_errors"
    assert load_paths().error_root == canonical
    assert GeneratorConfig.__dataclass_fields__["error_log_path"].default == canonical
    example = GeneratorConfig.load(Path(__file__).parents[1] / "config" / "example.json")
    assert example.error_log_path == canonical


def test_run_log_paths_and_writer_are_safe():
    paths = load_paths()
    assert paths.log_root == "dbfs:/de-assist-databricks/logs"
    expected = "dbfs:/de-assist-databricks/logs/generator/run-1/gold.json"
    assert paths.log("generator/run-1/gold.json") == expected
    assert write_run_log(None, paths, "generator/run-1/gold.json", {"error": "boom"}) is None

    writes = []

    class Stream:
        def close(self):
            writes.append("closed")

    class FileSystem:
        def mkdirs(self, parent):
            writes.append(("mkdirs", parent))

        def create(self, path, overwrite):
            writes.append(("create", path.location, overwrite))
            return Stream()

    class Spark:
        pass

    class HadoopPath:
        def __init__(self, location):
            self.location = location

        def getFileSystem(self, _conf):
            return FileSystem()

        def getParent(self):
            return "parent"

    jvm = SimpleNamespace(
        org=SimpleNamespace(
            apache=SimpleNamespace(
                hadoop=SimpleNamespace(fs=SimpleNamespace(Path=HadoopPath)),
                commons=SimpleNamespace(
                    io=SimpleNamespace(
                        IOUtils=SimpleNamespace(
                            write=lambda value, _stream, encoding: writes.append(
                                ("write", value, encoding)
                            )
                        )
                    )
                ),
            )
        )
    )
    Spark.sparkContext = SimpleNamespace(
        _jvm=jvm,
        _jsc=SimpleNamespace(hadoopConfiguration=lambda: object()),
    )

    assert write_run_log(
        Spark(), paths, "generator/run-1/gold.json", {"error": "boom"}
    ) == expected
    assert ("create", expected, True) in writes
    assert ("write", '{"error": "boom"}\n', "UTF-8") in writes
    assert "closed" in writes

    class ExplodingSpark:
        def createDataFrame(self, *_args, **_kwargs):
            raise RuntimeError("log storage unavailable")

    assert (
        write_run_log(
            ExplodingSpark(), paths, "generator/run-1/gold.json", {"error": "boom"}
        )
        is None
    )


def test_persist_error_does_not_drop_table_on_happy_path():
    sql_calls = []

    class Writer:
        def format(self, file_format):
            assert file_format == "delta"
            return self

        def mode(self, mode):
            assert mode == "append"
            return self

        def save(self, location):
            assert location == "dbfs:/de-assist-databricks/delta/de_assist/run_errors"

    class Frame:
        write = Writer()

    class Spark:
        def createDataFrame(self, rows):
            assert rows[0]["status"] == "FAILED"
            return Frame()

        def sql(self, statement):
            sql_calls.append(statement)

    paths = load_paths()
    fqtn = persist_error(Spark(), paths, error_record(pipeline="test", error="boom"))
    assert fqtn == "de_assist.run_errors"
    assert not any(statement.startswith("DROP TABLE") for statement in sql_calls)
    assert (
        "CREATE TABLE IF NOT EXISTS de_assist.run_errors USING DELTA LOCATION "
        "'dbfs:/de-assist-databricks/delta/de_assist/run_errors'"
    ) in sql_calls
    assert "REFRESH TABLE de_assist.run_errors" in sql_calls


def test_persist_error_keeps_table_when_refresh_fails():
    sql_calls = []

    class Writer:
        def format(self, _file_format):
            return self

        def mode(self, _mode):
            return self

        def save(self, _location):
            return None

    class Frame:
        write = Writer()

    class Spark:
        def createDataFrame(self, _rows):
            return Frame()

        def sql(self, statement):
            sql_calls.append(statement)
            if statement.startswith("REFRESH TABLE"):
                raise RuntimeError("refresh unavailable")

    paths = load_paths()
    assert persist_error(Spark(), paths, error_record(pipeline="test", error="boom")) == (
        "de_assist.run_errors"
    )
    assert not any(statement.startswith("DROP TABLE") for statement in sql_calls)


def test_write_delta_rejects_unknown_mode():
    with pytest.raises(ValueError, match="Unsupported write mode"):
        write_delta(None, None, load_paths("dbfs"), "bronze", "t", mode="upsert")


def test_make_tools_wires_error_loader(tmp_path):
    (tmp_path / "spec").mkdir()
    (tmp_path / "contracts").mkdir()
    (tmp_path / "spec" / "pipeline-spec.json").write_text("{}", encoding="utf-8")
    (tmp_path / "contracts" / "layer_output.schema.json").write_text("{}", encoding="utf-8")
    config = GeneratorConfig(
        project_name="test",
        input_root="spec",
        contract_file="contracts/layer_output.schema.json",
        output_root="generated",
        workspace_root="/Users/test/project",
        metadata_path="dbfs:/test/metadata",
        source_map=[{"id": "spec", "path": "pipeline-spec.json", "description": "test"}],
    )
    unwired, _ = make_tools(config, tmp_path, spark=None)
    assert unwired.error_loader is None
    wired, paths = make_tools(config, tmp_path, spark=object())
    assert wired.error_loader is not None
    assert paths.error_root == "dbfs:/de-assist-databricks/delta/de_assist/run_errors"
    assert config.error_log_path == "dbfs:/de-assist-databricks/delta/de_assist/run_errors"


def test_last_error_and_past_fix_use_attempt_ledger(monkeypatch):
    failed = error_record(
        pipeline="taxi",
        layer="silver",
        pipeline_run_id="run-1",
        error="AnalysisException missing vendor_id",
        diagnostics={"attempt": 1, "generated_code": "bad = vendor"},
    )
    passed = error_record(
        pipeline="taxi",
        layer="silver",
        pipeline_run_id="run-1",
        error="",
        status="PASSED",
        diagnostics={"attempt": 2, "generated_code": "good = vendor_id"},
    )
    monkeypatch.setattr("retrieval.load_error_records", lambda *_args: [failed, passed])
    assert get_last_error(None, None, "taxi", "silver") == failed
    fix = find_past_fix(None, None, "missing vendor_id AnalysisException", "silver")
    assert fix is not None
    assert "-bad = vendor" in fix
    assert "+good = vendor_id" in fix


def test_executor_extracts_task_error_and_trace():
    output = SimpleNamespace(error="NameError: missing_name", error_trace="stack trace detail")
    workspace = SimpleNamespace(
        jobs=SimpleNamespace(get_run_output=lambda run_id: output if run_id == 456 else None)
    )
    parent = SimpleNamespace(
        state=SimpleNamespace(state_message="task failed"),
        tasks=[SimpleNamespace(run_id=456, task_key="generated_00_bronze")],
    )
    detail = _failed_run_detail(workspace, parent)
    assert "NameError: missing_name" in detail
    assert "stack trace detail" in detail
