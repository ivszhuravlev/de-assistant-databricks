"""Read-only tools that gather context. No embeddings."""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from typing import Any, Callable
from urllib import request

from config import GeneratorConfig
from pipeline_helpers import load_paths
from retrieval import get_last_error as load_last_error
from retrieval import keyword_search, load_error_records

_TESTED_FILE = "tested-pipelines.json"
_HIDDEN_PIPELINE_IDS = {"eval_taxi"}
_HIDDEN_PIPELINE_FILES = {
    "taxi_vertical_slice.py",
    "fresh_retail_vertical_slice.py",
    "transaction_cat_vertical_slice.py",
    "slice.py",
    "evals/taxi/score.py",
    "evals/taxi/slice.py",
    "evals/fresh_retail/score.py",
    "evals/fresh_retail/slice.py",
    "evals/transaction_cat/score.py",
    "evals/transaction_cat/slice.py",
}
_LAYERS = ("bronze", "silver", "gold")
_TABLE_LAYERS = (*_LAYERS, "de_assist")


class ReadOnlyTools:
    def __init__(
        self,
        config: GeneratorConfig,
        repo_root: Path,
        error_records: list[dict[str, Any]] | None = None,
        error_loader: Callable[[], list[dict[str, Any]]] | None = None,
        spark: Any | None = None,
        runtime_paths: Any | None = None,
        run_loader: Callable[[int], dict[str, list[dict[str, Any]]]] | None = None,
    ):
        self.config = config
        self.repo_root = repo_root
        self.input_root = (repo_root / config.input_root).resolve()
        self.contract_path = (repo_root / config.contract_file).resolve()
        self.by_id = {item["id"]: item for item in config.source_map}
        self.error_records = error_records or []
        self.error_loader = error_loader
        self.spark = spark
        self.paths = runtime_paths or load_paths("dbfs", error_root=config.error_log_path)
        self.run_loader = run_loader

    def definitions(self) -> list[dict[str, Any]]:
        def tool(
            name: str,
            description: str,
            properties: dict[str, Any],
            required: list[str] | None = None,
        ) -> dict[str, Any]:
            parameters: dict[str, Any] = {
                "type": "object",
                "properties": properties,
                "additionalProperties": False,
            }
            if required:
                parameters["required"] = required
            return {
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": parameters,
                },
            }

        return [
            tool("list_sources", "List declared source-map entries.", {}),
            tool(
                "read_source",
                "Read one declared source-map entry by id.",
                {"source_id": {"type": "string"}},
                ["source_id"],
            ),
            tool("read_contract", "Read the static output contract.", {}),
            tool(
                "list_raw_files",
                "List files under this run's raw landing path.",
                {},
            ),
            tool(
                "peek_raw",
                "Schema and sample rows of a raw file. Looks inside the data.",
                {"path": {"type": "string"}, "n": {"type": "integer"}},
                ["path"],
            ),
            tool(
                "profile_column",
                "Nulls, min, max, approx distinct for one column on a raw path or Hive table.",
                {
                    "target": {"type": "string"},
                    "column": {"type": "string"},
                },
                ["target", "column"],
            ),
            tool(
                "get_distinct_values",
                "List distinct values for one column on a raw path or Hive table. Use contains "
                "to verify sparse string patterns before generating filters.",
                {
                    "target": {"type": "string"},
                    "column": {"type": "string"},
                    "contains": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                ["target", "column"],
            ),
            tool(
                "list_tables",
                "List Hive tables already written in one logical layer for this run.",
                {"layer": {"type": "string"}},
                ["layer"],
            ),
            tool(
                "peek_table",
                "Schema and sample rows of a Hive bronze/silver/gold/de_assist table.",
                {
                    "layer": {"type": "string"},
                    "table": {"type": "string"},
                    "n": {"type": "integer"},
                },
                ["layer", "table"],
            ),
            tool(
                "search_previous_errors",
                "Keyword search over previous FAILED runs.",
                {"query": {"type": "string"}, "limit": {"type": "integer"}},
                ["query"],
            ),
            tool(
                "get_last_error",
                "Most recent failure for this pipeline and layer, if any.",
                {
                    "pipeline": {"type": "string"},
                    "layer": {"type": "string"},
                },
                ["layer"],
            ),
            tool(
                "read_log",
                "Tail of the generator log file for a prior framework run and layer.",
                {
                    "run_id": {"type": "string"},
                    "layer": {"type": "string"},
                    "tail_lines": {"type": "integer"},
                },
                ["run_id", "layer"],
            ),
            tool(
                "list_successful_runs",
                "Previous SUCCESS pipeline_run / layer_run rows from this framework.",
                {"limit": {"type": "integer"}},
            ),
            tool(
                "read_tested_pipeline",
                "Read a line-range slice of a tested or previously generated pipeline notebook.",
                {
                    "pipeline_id": {"type": "string"},
                    "layer": {"type": "string"},
                    "start_line": {"type": "integer"},
                    "num_lines": {"type": "integer"},
                },
            ),
            tool(
                "ask_clarification",
                "Record a business question that is not in the brief and not observable in data. "
                "The unattended harness cannot answer. Do not guess; add UNKNOWN: in assumptions.",
                {"question": {"type": "string"}},
                ["question"],
            ),
            tool(
                "spark_ui_applications",
                "Return the live Spark application id on this classic cluster for debugging only; "
                "do not use it for optimization.",
                {},
            ),
            tool(
                "spark_ui_failed_jobs",
                "Return failed or killed Spark jobs with the first error/stage hint for debugging "
                "only; do not use it for optimization.",
                {},
            ),
        ]

    def call(self, name: str, arguments: dict[str, Any]) -> str:
        handlers = {
            "list_sources": lambda: json.dumps(self.config.source_map, sort_keys=True),
            "read_source": lambda: self._read_source(str(arguments.get("source_id") or "")),
            "read_contract": lambda: self.contract_path.read_text(encoding="utf-8"),
            "list_raw_files": self._list_raw_files,
            "peek_raw": lambda: self._peek_raw(
                str(arguments.get("path") or ""), int(arguments.get("n") or 5)
            ),
            "profile_column": lambda: self._profile_column(
                str(arguments.get("target") or ""),
                str(arguments.get("column") or ""),
            ),
            "get_distinct_values": lambda: self._get_distinct_values(
                str(arguments.get("target") or ""),
                str(arguments.get("column") or ""),
                str(arguments.get("contains") or ""),
                int(arguments.get("limit") or 20),
            ),
            "list_tables": lambda: self._list_tables(str(arguments.get("layer") or "")),
            "peek_table": lambda: self._peek_table(
                str(arguments.get("layer") or ""),
                str(arguments.get("table") or ""),
                int(arguments.get("n") or 5),
            ),
            "search_previous_errors": lambda: self._search_previous_errors(
                str(arguments.get("query") or ""),
                int(arguments.get("limit") or 10),
            ),
            "get_last_error": lambda: self._get_last_error(
                str(arguments.get("pipeline") or self.config.project_name),
                str(arguments.get("layer") or ""),
            ),
            "read_log": lambda: self._read_log(
                str(arguments.get("run_id") or ""),
                str(arguments.get("layer") or ""),
                int(arguments.get("tail_lines") or 100),
            ),
            "list_successful_runs": lambda: self._list_successful_runs(
                int(arguments.get("limit") or 10)
            ),
            "read_tested_pipeline": lambda: self._read_tested_pipeline(
                str(arguments.get("pipeline_id") or ""),
                str(arguments.get("layer") or ""),
                int(arguments.get("start_line") or 0),
                int(arguments.get("num_lines") or 60),
            ),
            "ask_clarification": lambda: self._ask_clarification(
                str(arguments.get("question") or "")
            ),
            "spark_ui_applications": self._spark_ui_applications,
            "spark_ui_failed_jobs": self._spark_ui_failed_jobs,
        }
        if name not in handlers:
            raise ValueError(f"Unknown or non-read-only tool: {name}")
        return handlers[name]()

    def _need_spark(self) -> str | None:
        if self.spark is None:
            return json.dumps({"error": "spark is required for this tool"})
        return None

    def _spark_ui_applications(self) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        try:
            applications = self._live_spark_applications()
            rows = [
                {
                    "application_id": str(application.get("id") or ""),
                    "name": str(application.get("name") or ""),
                    "status": "RUNNING",
                }
                for application in applications
            ]
            return json.dumps({"applications": rows}, sort_keys=True)
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    def _spark_ui_failed_jobs(self) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        try:
            applications = self._live_spark_applications()
            jobs = []
            for application in applications:
                application_id = str(application.get("id") or "")
                payload = self._spark_ui_get(f"/applications/{application_id}/jobs")
                for job in payload if isinstance(payload, list) else []:
                    status = str(job.get("status") or "").upper()
                    if status not in {"FAILED", "KILLED"}:
                        continue
                    stage_ids = list(job.get("stageIds") or [])
                    stage_hint = ""
                    error_snippet = ""
                    if stage_ids:
                        stage_hint = f"stage {stage_ids[0]}"
                        try:
                            stage = self._spark_ui_get(
                                f"/applications/{application_id}/stages/{stage_ids[0]}"
                            )
                            attempts = stage if isinstance(stage, list) else [stage]
                            attempt = next(
                                (
                                    item
                                    for item in attempts
                                    if isinstance(item, dict)
                                    and str(item.get("status") or "").upper()
                                    in {"FAILED", "KILLED"}
                                ),
                                attempts[0] if attempts else {},
                            )
                            if isinstance(attempt, dict):
                                error_snippet = str(attempt.get("failureReason") or "")
                                stage_name = str(attempt.get("name") or "")
                                if stage_name:
                                    stage_hint = f"{stage_hint}: {stage_name}"
                        except Exception as exc:
                            error_snippet = str(exc)
                    if not error_snippet:
                        error_snippet = str(
                            job.get("failureReason")
                            or job.get("description")
                            or job.get("name")
                            or ""
                        )
                    jobs.append(
                        {
                            "application_id": application_id,
                            "job_id": job.get("jobId"),
                            "status": status,
                            "error_snippet": error_snippet[:500],
                            "stage_hint": stage_hint[:500],
                        }
                    )
            return json.dumps({"jobs": jobs}, sort_keys=True)
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    def _live_spark_applications(self) -> list[dict[str, Any]]:
        payload = self._spark_ui_get("/applications")
        if not isinstance(payload, list):
            raise RuntimeError("Spark UI applications response was not a list")
        live = []
        for application in payload:
            attempts = application.get("attempts") or []
            if not attempts or any(not attempt.get("completed", False) for attempt in attempts):
                live.append(application)
        return live

    def _spark_ui_get(self, path: str) -> Any:
        relative = "/api/v1" + (path if path.startswith("/") else "/" + path)
        failures = []
        for port in (4040, 40001):
            try:
                return _read_json_url(f"http://localhost:{port}{relative}")
            except Exception as exc:
                failures.append(f"localhost:{port}: {exc}")

        host = _spark_conf(self.spark, "spark.databricks.workspaceUrl")
        cluster_id = _spark_conf(
            self.spark, "spark.databricks.clusterUsageTags.clusterId"
        )
        org_id = _spark_conf(self.spark, "spark.databricks.clusterUsageTags.orgId")
        token = _databricks_context_value(self.spark, "apiToken")
        if host and cluster_id and org_id and token:
            for port in (4040, 40001):
                try:
                    url = (
                        f"https://{host.removeprefix('https://').rstrip('/')}/driver-proxy-api/o/"
                        f"{org_id}/{cluster_id}/{port}{relative}"
                    )
                    return _read_json_url(url, {"Authorization": f"Bearer {token}"})
                except Exception as exc:
                    failures.append(f"driver-proxy:{port}: {exc}")
        else:
            failures.append("driver-proxy connection details are unavailable")
        raise RuntimeError("Spark UI unreachable (" + "; ".join(failures) + ")")

    def _read_source(self, source_id: str) -> str:
        if source_id not in self.by_id:
            raise ValueError(f"Source id is not declared: {source_id}")
        relative = PurePosixPath(self.by_id[source_id]["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"Unsafe source path: {relative}")
        path = (self.input_root / relative).resolve()
        if self.input_root not in path.parents:
            raise ValueError(f"Source escapes input root: {relative}")
        return path.read_text(encoding="utf-8")

    def _safe_raw_path(self, path: str) -> str:
        text = (path or "").strip()
        root = self.paths.raw_root.rstrip("/")
        if not text.startswith(("dbfs:/", "abfss://", "wasbs://")):
            text = self.paths.raw(*[p for p in text.split("/") if p])
        if text != root and not text.startswith(root + "/"):
            raise ValueError(f"Path is outside raw root: {path}")
        return text

    def _list_raw_files(self) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        root = self.paths.raw_root
        rows = []
        try:
            for item in self.spark.read.format("binaryFile").option(
                "recursiveFileLookup", "true"
            ).load(root).select("path", "length").limit(200).collect():
                rows.append({"path": item["path"], "length": int(item["length"])})
        except Exception as exc:
            return json.dumps({"error": str(exc), "root": root})
        return json.dumps({"root": root, "files": rows}, sort_keys=True)

    def _peek_raw(self, path: str, n: int) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        try:
            location = self._safe_raw_path(path)
            limit = max(1, min(n, 20))
            df = self._read_raw_dataframe(location)
            schema = [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]
            sample = [row.asDict(recursive=True) for row in df.limit(limit).collect()]
        except Exception as exc:
            return json.dumps({"error": str(exc), "path": path})
        return json.dumps(
            {"path": location, "schema": schema, "sample": _jsonable(sample)},
            sort_keys=True,
            default=str,
        )

    def _profile_column(self, target: str, column: str) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        if not column:
            return json.dumps({"error": "column is required"})
        try:
            if "/" in target or target.startswith("dbfs:"):
                df = self._read_raw_dataframe(self._safe_raw_path(target))
            else:
                layer, _, table = target.partition(".")
                if layer not in _LAYERS or not table:
                    return json.dumps(
                        {"error": "Hive target must be bronze/silver/gold layer.table"}
                    )
                df = self.spark.table(self.paths.table(layer, table))
            from pipeline_helpers import actual_column

            name = actual_column(df.columns, column)
            quoted = f"`{name.replace('`', '``')}`"
            stats = df.selectExpr(
                f"count(*) - count({quoted}) AS nulls",
                f"min({quoted}) AS min",
                f"max({quoted}) AS max",
                f"approx_count_distinct({quoted}) AS approx_distinct",
            ).collect()[0].asDict(recursive=True)
        except Exception as exc:
            return json.dumps({"error": str(exc), "target": target, "column": column})
        return json.dumps(
            {
                "target": target,
                "column": name,
                **stats,
            },
            sort_keys=True,
            default=str,
        )

    def _get_distinct_values(
        self, target: str, column: str, contains: str, limit: int
    ) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        if not column:
            return json.dumps({"error": "column is required"})
        cap = max(1, min(limit, 50))
        try:
            if "/" in target or target.startswith("dbfs:"):
                location = self._safe_raw_path(target)
                df = self._read_raw_dataframe(location)
            else:
                layer, _, table = target.partition(".")
                if layer not in _LAYERS or not table:
                    return json.dumps(
                        {"error": "Hive target must be bronze/silver/gold/de_assist layer.table"}
                    )
                location = self.paths.table(layer, table)
                df = self.spark.table(location)
            from pipeline_helpers import actual_column
            from pyspark.sql import functions as F

            name = actual_column(df.columns, column)
            value = F.col(name)
            if contains:
                df = df.where(F.lower(value.cast("string")).contains(contains.lower()))
            rows = (
                df.select(value.alias("value"))
                .distinct()
                .orderBy(F.col("value").asc_nulls_last())
                .limit(cap)
                .collect()
            )
            values = [row["value"] for row in rows]
        except Exception as exc:
            return json.dumps({"error": str(exc), "target": target, "column": column})
        return json.dumps(
            {
                "target": location,
                "column": name,
                "contains": contains or None,
                "limit": cap,
                "values": values,
            },
            sort_keys=True,
            default=str,
        )

    def _list_tables(self, layer: str) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        if layer not in _TABLE_LAYERS:
            return json.dumps({"error": f"Unsupported layer: {layer}"})
        schema = self.paths.layer_schema(layer)
        try:
            rows = self.spark.sql(f"SHOW TABLES IN `{schema}`").collect()
            tables = [str(row["tableName"]) for row in rows]
        except Exception as exc:
            return json.dumps({"error": str(exc), "layer": layer, "schema": schema})
        return json.dumps({"layer": layer, "schema": schema, "tables": tables}, sort_keys=True)

    def _ask_clarification(self, question: str) -> str:
        text = (question or "").strip()
        if not text:
            return json.dumps({"error": "question is required"})
        return json.dumps(
            {
                "status": "unanswered",
                "question": text,
                "instruction": (
                    "Do not invent this business rule. Add an assumption prefixed "
                    "with UNKNOWN: and skip implementing it."
                ),
            },
            sort_keys=True,
        )

    def _peek_table(self, layer: str, table: str, n: int) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        if layer not in _TABLE_LAYERS:
            return json.dumps({"error": f"Unsupported layer: {layer}"})
        fqtn = self.paths.table(layer, table)
        limit = max(1, min(n, 20))
        try:
            df = self.spark.table(fqtn)
            schema = [{"name": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]
            sample = [row.asDict(recursive=True) for row in df.limit(limit).collect()]
        except Exception as exc:
            return json.dumps({"error": str(exc), "table": fqtn})
        return json.dumps(
            {"table": fqtn, "schema": schema, "sample": _jsonable(sample)},
            sort_keys=True,
            default=str,
        )

    def _get_last_error(self, pipeline: str, layer: str) -> str:
        if not layer:
            return json.dumps({"error": "layer is required"})
        try:
            if self.spark is not None:
                latest = load_last_error(self.spark, self.paths, pipeline, layer)
            else:
                from retrieval import _latest_failed

                records = self.error_loader() if self.error_loader else self.error_records
                latest = _latest_failed(list(records or []), pipeline, layer)
        except Exception as exc:
            return json.dumps({"error": str(exc), "pipeline": pipeline, "layer": layer})
        return json.dumps(
            {"pipeline": pipeline, "layer": layer, "last_error": latest},
            sort_keys=True,
            default=str,
        )

    def _search_previous_errors(self, query: str, limit: int) -> str:
        try:
            records = self.error_loader() if self.error_loader else self.error_records
            failed = [
                record
                for record in records
                if str(record.get("status") or "FAILED").upper() == "FAILED"
            ]
            if (query or "").strip():
                matches = keyword_search(failed, query, limit)
            else:
                matches = sorted(
                    failed,
                    key=lambda record: str(record.get("created_at") or ""),
                    reverse=True,
                )[: max(1, min(limit, 10))]
        except Exception as exc:
            return json.dumps({"error": str(exc), "query": query})
        return json.dumps(matches, sort_keys=True, default=str)

    def _read_log(self, run_id: str, layer: str, tail_lines: int) -> str:
        missing = self._need_spark()
        if missing:
            return missing
        if not _safe_segment(run_id) or not _safe_segment(layer):
            return json.dumps({"error": "run_id and layer must be safe non-empty path segments"})
        cap = max(1, min(tail_lines, 20))
        candidates = [self.paths.log(f"generator/{run_id}/{layer}.json")]
        attempt_pattern = self.paths.log(f"generator/{run_id}/{layer}/attempt-*.json")
        candidates.extend(_latest_attempt_logs(self.spark, attempt_pattern))
        candidates.append(self.paths.log(f"generator/{run_id}/{layer}"))
        if attempt_pattern not in candidates:
            candidates.append(attempt_pattern)
        last_error = None
        for path in candidates:
            try:
                rows = self.spark.read.text(path).collect()
                lines = [_slim_log_line(str(row["value"])) for row in rows[-cap:]]
                return json.dumps(
                    {"path": path, "tail_lines": cap, "lines": lines},
                    sort_keys=True,
                    default=str,
                )
            except Exception as exc:
                last_error = exc
        return json.dumps(
            {
                "error": str(last_error) if last_error else "log not found",
                "path": candidates[0],
            }
        )

    def _list_successful_runs(self, limit: int) -> str:
        cap = max(1, min(limit, 20))
        if self.run_loader:
            try:
                rows = self.run_loader(cap)
            except Exception as exc:
                return json.dumps({"error": str(exc)})
            return json.dumps(rows, sort_keys=True, default=str)
        if self.spark is None:
            return json.dumps({"error": "spark is required for this tool"})
        try:
            rows = _load_successful_runs(self.spark, self.paths, cap)
        except Exception as exc:
            return json.dumps({"error": str(exc)})
        return json.dumps(rows, sort_keys=True, default=str)

    def _read_tested_pipeline(
        self, pipeline_id: str, layer: str, start_line: int, num_lines: int
    ) -> str:
        catalog = _visible_tested_catalog(self.input_root / _TESTED_FILE)
        match = None
        if pipeline_id:
            match = next((item for item in catalog if item.get("id") == pipeline_id), None)
        elif layer:
            match = next(
                (item for item in catalog if item.get("layer") == layer and item.get("status") == "tested"),
                None,
            )
            if match is None:
                match = next((item for item in catalog if item.get("layer") == layer), None)
        if match is None:
            return json.dumps({"error": "No tested pipeline matched", "available": catalog})
        relative = PurePosixPath(str(match["path"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"Unsafe pipeline path: {relative}")
        path = (self.repo_root / relative).resolve()
        if self.repo_root not in path.parents and path.parent != self.repo_root:
            raise ValueError(f"Pipeline escapes repo: {relative}")
        if not path.is_file():
            return json.dumps({"error": f"File not found: {relative}", "id": match.get("id")})
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        start = max(0, start_line)
        count = max(1, min(num_lines, 200))
        text = "".join(lines[start : start + count])
        return json.dumps(
            {
                "id": match.get("id"),
                "path": str(relative),
                "start_line": start,
                "num_lines": len(lines[start : start + count]),
                "total_lines": len(lines),
                "content": text,
            }
        )

    def _read_raw_dataframe(self, location: str):
        lower = location.lower()
        if lower.endswith(".csv.gz") or lower.endswith(".csv"):
            return (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                .csv(location)
            )
        if lower.endswith(".json"):
            return self.spark.read.option("inferSchema", "true").json(location)
        if lower.endswith(".parquet") or lower.endswith(".pq"):
            return self.spark.read.parquet(location)
        suffix = PurePosixPath(location).suffix.lower()
        raise ValueError(f"Unsupported raw file type: {suffix or '<none>'}")


def _tested_catalog(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return list(payload.get("pipelines") or [])


def _visible_tested_catalog(path: Path) -> list[dict[str, Any]]:
    visible = []
    for item in _tested_catalog(path):
        if _hidden_pipeline_entry(item):
            continue
        visible.append(item)
    return visible


def _hidden_pipeline_entry(item: dict[str, Any]) -> bool:
    if item.get("id") in _HIDDEN_PIPELINE_IDS or item.get("hidden_from_generator"):
        return True
    path = PurePosixPath(str(item.get("path") or ""))
    hidden_names = {PurePosixPath(value).name for value in _HIDDEN_PIPELINE_FILES}
    return str(path) in _HIDDEN_PIPELINE_FILES or path.name in hidden_names


def _jsonable(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        out.append({key: str(value) if value is not None else None for key, value in row.items()})
    return out


def _safe_segment(value: str) -> bool:
    return bool(value) and value not in {".", ".."} and "/" not in value and "\\" not in value


def _slim_log_line(line: str, limit: int = 2000) -> str:
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return line[:limit]
    if not isinstance(payload, dict):
        return line[:limit]
    payload.pop("generated_code", None)
    diagnostics = payload.get("diagnostics")
    if isinstance(diagnostics, dict):
        diagnostics.pop("generated_code", None)
    elif isinstance(diagnostics, str):
        try:
            parsed = json.loads(diagnostics)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            parsed.pop("generated_code", None)
            payload["diagnostics"] = parsed
    return json.dumps(payload, sort_keys=True, default=str)[:limit]


def _load_successful_runs(spark, paths, limit: int = 10) -> dict[str, list[dict[str, Any]]]:
    result = {"pipeline_run": [], "layer_run": []}
    for name in result:
        fqtn = f"{paths.ops_schema}.{name}"
        frame = spark.table(fqtn).where("status = 'SUCCESS'")
        if "created_at" in frame.columns:
            frame = frame.orderBy("created_at", ascending=False)
        result[name] = [
            row.asDict(recursive=True)
            for row in frame.limit(limit).collect()
        ]
    return result


def _latest_attempt_logs(spark: Any, pattern: str) -> list[str]:
    """Return matching attempt logs newest-attempt first when Hadoop listing is available."""
    try:
        jvm = spark._jvm
        path = jvm.org.apache.hadoop.fs.Path(pattern)
        filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
        statuses = filesystem.globStatus(path) or []
        paths = [str(status.getPath().toString()) for status in statuses]
        return sorted(paths, key=_attempt_number, reverse=True)
    except Exception:
        return []


def _attempt_number(path: str) -> int:
    name = PurePosixPath(path).name
    if name.startswith("attempt-") and name.endswith(".json"):
        try:
            return int(name[len("attempt-") : -len(".json")])
        except ValueError:
            pass
    return -1


def _read_json_url(url: str, headers: dict[str, str] | None = None) -> Any:
    http_request = request.Request(url, headers=headers or {})
    with request.urlopen(http_request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _spark_conf(spark, key: str) -> str:
    try:
        return str(spark.conf.get(key) or "")
    except Exception:
        return ""


def _databricks_context_value(spark, name: str) -> str:
    try:
        context = (
            spark.sparkContext._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()
            .notebook()
            .getContext()
        )
        value = getattr(context, name)()
        return str(value.get()) if value.isDefined() else ""
    except Exception:
        return ""


def make_tools(
    config: GeneratorConfig,
    repo_root: Path,
    spark: Any | None = None,
):
    runtime_paths = load_paths(
        getattr(config, "raw_backend", "adls") or "adls",
        error_root=config.error_log_path,
        output_space=getattr(config, "output_space", "generated") or "generated",
        pipeline=getattr(config, "pipeline", "taxi") or "taxi",
    )
    loader = None
    run_loader = None
    if spark is not None:
        loader = lambda: load_error_records(spark, runtime_paths)
        run_loader = lambda limit: _load_successful_runs(spark, runtime_paths, limit)
    return (
        ReadOnlyTools(
            config,
            repo_root,
            error_loader=loader,
            spark=spark,
            runtime_paths=runtime_paths,
            run_loader=run_loader,
        ),
        runtime_paths,
    )
