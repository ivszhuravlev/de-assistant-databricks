"""Static checks on a generated layer before anything is written or run."""

from __future__ import annotations

import ast
import re
from pathlib import Path, PurePosixPath
from typing import Any

from config import LAYERS


def _self_referential_assigns(tree: ast.AST) -> list[str]:
    """Names read in their assignment before any binding in the lexical scope."""
    parents: dict[ast.AST, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[child] = parent

    scope_types = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)

    def enclosing_scope(node: ast.AST) -> ast.AST:
        current = parents.get(node)
        while current is not None and not isinstance(current, scope_types):
            current = parents.get(current)
        return current or tree

    def scoped_nodes(scope: ast.AST):
        pending = list(ast.iter_child_nodes(scope))
        while pending:
            child = pending.pop()
            if isinstance(child, scope_types[1:]):
                continue
            yield child
            pending.extend(ast.iter_child_nodes(child))

    found: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        targets = {target.id for target in node.targets if isinstance(target, ast.Name)}
        used = {name.id for name in ast.walk(node.value) if isinstance(name, ast.Name)}
        candidates = targets & used
        if not candidates:
            continue
        scope = enclosing_scope(node)
        prior_bindings = {
            name.id
            for name in scoped_nodes(scope)
            if isinstance(name, ast.Name)
            and isinstance(name.ctx, ast.Store)
            and getattr(name, "lineno", node.lineno) < node.lineno
        }
        if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            prior_bindings.update(arg.arg for arg in scope.args.args)
            prior_bindings.update(arg.arg for arg in scope.args.kwonlyargs)
            if scope.args.vararg:
                prior_bindings.add(scope.args.vararg.arg)
            if scope.args.kwarg:
                prior_bindings.add(scope.args.kwarg.arg)
        found.extend(sorted(candidates - prior_bindings))
    return found


_GRAIN_LIST_NAMES = {"IDENTITY", "unique_grain", "required_cols", "required_columns"}


def _validate_counts_grain_list_misuse(tree: ast.AST) -> bool:
    """True when validate_counts is given a grain list instead of a duplicate count."""
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function_name = (
            node.func.id
            if isinstance(node.func, ast.Name)
            else node.func.attr
            if isinstance(node.func, ast.Attribute)
            else ""
        )
        if function_name != "validate_counts":
            continue
        fourth = node.args[3] if len(node.args) >= 4 else None
        for keyword in node.keywords:
            if keyword.arg == "duplicate_groups":
                fourth = keyword.value
        if fourth is None:
            continue
        if isinstance(fourth, ast.List):
            return True
        if isinstance(fourth, ast.Name) and fourth.id in _GRAIN_LIST_NAMES:
            return True
    return False


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _metadata_row_helper_mismatch(tree: ast.AST) -> list[str]:
    """Reject append_metadata_rows(table, rows) when the row helper does not match."""
    expected = {
        "DATA_QUALITY": "data_quality_row",
        "LAYER_RUN": "layer_run_row",
        "PIPELINE_RUN": "pipeline_run_row",
    }
    wrong = {
        "DATA_QUALITY": {"layer_run_row", "pipeline_run_row"},
        "LAYER_RUN": {"data_quality_row", "pipeline_run_row"},
        "PIPELINE_RUN": {"data_quality_row", "layer_run_row"},
    }
    errors: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or _call_name(node.func) != "append_metadata_rows":
            continue
        table = node.args[2] if len(node.args) >= 3 else None
        rows = node.args[3] if len(node.args) >= 4 else None
        for keyword in node.keywords:
            if keyword.arg == "table_name":
                table = keyword.value
            if keyword.arg == "rows":
                rows = keyword.value
        if not isinstance(table, ast.Name) or table.id not in expected or rows is None:
            continue
        for child in ast.walk(rows):
            if isinstance(child, ast.Call) and _call_name(child.func) in wrong[table.id]:
                errors.append(
                    f"append_metadata_rows({table.id}) must use {expected[table.id]}() rows"
                )
                break
    return errors


def _pipeline_run_append_count(tree: ast.AST) -> int:
    """Count calls passing PIPELINE_RUN as append_metadata_rows' table argument."""
    count = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function_name = (
            node.func.id
            if isinstance(node.func, ast.Name)
            else node.func.attr
            if isinstance(node.func, ast.Attribute)
            else ""
        )
        if function_name != "append_metadata_rows":
            continue
        table_args = list(node.args[2:3])
        table_args.extend(
            keyword.value for keyword in node.keywords if keyword.arg == "table_name"
        )
        if any(_is_pipeline_run_table(value) for value in table_args):
            count += 1
    return count


def _is_pipeline_run_table(value: ast.AST) -> bool:
    if isinstance(value, ast.Name):
        return value.id == "PIPELINE_RUN"
    if isinstance(value, ast.Attribute):
        return value.attr == "PIPELINE_RUN"
    if isinstance(value, ast.Constant) and value.value == "pipeline_run":
        return True
    return False


def validate_layer(result: dict[str, Any], expected_layer: str) -> list[str]:
    errors: list[str] = []
    if set(result) != {"layer", "summary", "artifacts", "assumptions"}:
        errors.append("Output keys do not match the static contract")
    if result.get("layer") != expected_layer or expected_layer not in LAYERS:
        errors.append(f"Expected layer {expected_layer}")
    if not isinstance(result.get("summary"), str) or not result.get("summary"):
        errors.append("Summary must be a non-empty string")
    if not isinstance(result.get("assumptions"), list):
        errors.append("Assumptions must be a list")
    artifacts = result.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        errors.append("At least one artifact is required")
        return errors
    seen: set[str] = set()
    for artifact in artifacts:
        if set(artifact) != {"path", "kind", "content"}:
            errors.append("Artifact keys do not match the static contract")
            continue
        path = PurePosixPath(str(artifact["path"]))
        if (
            path.is_absolute()
            or ".." in path.parts
            or not str(path).startswith("notebooks/")
            or path.suffix != ".py"
        ):
            errors.append(f"Unsafe artifact path: {path}")
        if str(path) != f"notebooks/{expected_layer}.py":
            errors.append(f"Expected artifact path notebooks/{expected_layer}.py")
        if str(path) in seen:
            errors.append(f"Duplicate artifact path: {path}")
        seen.add(str(path))
        if artifact["kind"] != "databricks_notebook":
            errors.append(f"Unsupported artifact kind: {artifact['kind']}")
        content = artifact["content"]
        if not isinstance(content, str) or not content.startswith("# Databricks notebook source"):
            errors.append(f"{path} is not a Databricks source notebook")
            continue
        if "def transform(" not in content:
            errors.append(f"{path} does not expose transform")
        if re.search(r"transform\(\s*spark\s*,\s*\{\s*\}\s*\)", content):
            errors.append(f"{path} calls transform(spark, {{}}) and drops config_json")
        if re.search(r"(?m)^\s*(import helpers|from helpers import)\b", content):
            errors.append(f"{path} imports a nonexistent helpers module")
        if "load_paths(" in content and "output_space" not in content:
            errors.append(f"{path} must pass output_space into load_paths")
        if re.search(r"""output_space\s*=\s*['"]eval['"]""", content):
            errors.append(
                f"{path} hard-codes output_space=eval; use config['output_space']"
            )
        if "load_paths(" in content and not re.search(
            r"""output_space\s*=\s*config(?:\[['\"]output_space['\"]\]|\.get\(\s*['\"]output_space['\"])""",
            content,
        ):
            errors.append(
                f"{path} must pass output_space=config['output_space'] into load_paths"
            )
        if re.search(
            r"""spark\.table\(\s*['"](?:gen_)?(?:bronze|silver|gold)\.""",
            content,
        ):
            errors.append(f"{path} hard-codes a Hive table name; use paths.table")
        if re.search(
            r"(?i)\b(?:from|join|into)\s+(?:gen_)?(?:bronze|silver|gold)\.",
            content,
        ):
            errors.append(f"{path} hard-codes a Hive schema in SQL; use paths.table")
        if re.search(
            r"(?i)\b(?:create|drop|use)\s+(?:database|schema)\s+(?:if\s+(?:not\s+)?exists\s+)?(?:gen_)?(?:bronze|silver|gold)\b",
            content,
        ):
            errors.append(f"{path} hard-codes a Hive database; use ensure_schema(spark, paths)")
        try:
            tree = ast.parse(content)
        except SyntaxError as exc:
            errors.append(f"{path} has invalid Python: {exc}")
        else:
            for name in _self_referential_assigns(tree):
                errors.append(
                    f"{path} assigns {name} while using {name} in the same statement"
                )
            if _validate_counts_grain_list_misuse(tree):
                errors.append(
                    f"{path} passes a grain list to validate_counts; "
                    "duplicate_groups must be the int count of duplicate groups"
                )
            for mismatch in _metadata_row_helper_mismatch(tree):
                errors.append(f"{path} {mismatch}")
            if expected_layer == "gold" and _pipeline_run_append_count(tree) != 1:
                errors.append(
                    f"{path} must append exactly one SUCCESS row with "
                    "append_metadata_rows(spark, paths, PIPELINE_RUN, [...])"
                )
        lowered = content.lower()
        for destructive in ("dbutils.fs.rm(", "drop table", "truncate table"):
            if destructive in lowered:
                errors.append(f"{path} contains destructive operation: {destructive}")
    return errors


def write_artifacts(result: dict[str, Any], output_root: Path) -> list[Path]:
    written: list[Path] = []
    for artifact in result["artifacts"]:
        path = output_root / artifact["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(artifact["content"].rstrip() + "\n", encoding="utf-8")
        written.append(path)
    return written
