"""Bind StreamSQLAgent candidates to the exact Flink job executed by the benchmark.

The trusted StreamSQLBench runtime owns Kafka topics, offsets, source/sink DDL,
and ``INSERT INTO``.  StreamSQLAgent owns the logical ``SELECT``/``WITH`` query.
This module deliberately does not render a second copy of that infrastructure.
It validates logical candidates and, after a successful formal execution,
freezes the runtime's byte-exact ``execute.sql`` as the method's ``final.sql``.
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from .paths import vldb_root
from types import ModuleType
from typing import Any, Mapping


BACKEND_RESULT_CONTRACT = "streamsqlbench_rq2_docker_backend_result_v1"
ORDERED_RESULT_CONTRACT = "streamsqlbench_ordered_streaming_v1"


class JobAssemblyError(RuntimeError):
    """The trusted runtime did not produce a complete, freezable Flink job."""


@dataclass(frozen=True)
class FrozenJob:
    final_sql: Path
    executed_sql: Path
    final_csv: Path
    changelog: Path
    runtime_result: Path
    task_id: str
    job_id: str
    byte_size: int


def _vldb_root() -> Path:
    # Shared runtime root is independent of the version checkout location.
    return vldb_root()


def _load_rq2_runtime_adapter() -> ModuleType:
    """Load the frozen candidate parser from the existing RQ2 implementation."""

    source_root = _vldb_root()
    package_file = source_root / "streamsqlbench_rq2" / "runtime_adapter.py"
    if not package_file.is_file():
        raise JobAssemblyError(f"RQ2 runtime adapter is missing: {package_file}")
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))
    # Import the package name so its relative imports keep using the tested code.
    from streamsqlbench_rq2 import runtime_adapter  # type: ignore

    return runtime_adapter


def validate_logical_query(sql: str) -> str:
    """Apply the same single SELECT/WITH boundary as the formal runtime."""

    try:
        return str(_load_rq2_runtime_adapter().extract_logical_query(sql))
    except (OSError, ValueError) as exc:
        raise JobAssemblyError(str(exc)) from exc


def write_logical_candidate(sql: str, path: Path) -> Path:
    """Write one validated query for the existing method-feedback/formal backend."""

    query = validate_logical_query(sql)
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(query.rstrip(";") + "\n", encoding="utf-8")
    os.replace(temporary, path)
    return path


def _read_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise JobAssemblyError(f"cannot read {label}: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise JobAssemblyError(f"{label} must contain a JSON object: {path}")
    return value


def _runtime_result_from_backend(backend_result: Path) -> tuple[dict[str, Any], Path]:
    backend = _read_object(backend_result, "backend result")
    if (
        backend.get("contract") != BACKEND_RESULT_CONTRACT
        or backend.get("status") != "success"
        or backend.get("backend_kind") != "ordered_final"
    ):
        raise JobAssemblyError("final.sql requires one successful ordered-final backend result")
    raw = backend.get("raw_artifact")
    if not isinstance(raw, str) or not raw:
        raise JobAssemblyError("backend result does not identify its ordered runtime artifact")
    runtime_result = Path(raw).resolve()
    return backend, runtime_result


def _require_successful_runtime(runtime_result: Path) -> dict[str, Any]:
    result = _read_object(runtime_result, "ordered runtime result")
    if (
        result.get("contract") != ORDERED_RESULT_CONTRACT
        or result.get("ok") is not True
        or result.get("execution_mode") != "STREAMING"
        or result.get("stage") != "complete"
        or result.get("substage") != "complete"
        or result.get("candidate_query_unchanged") is not True
    ):
        raise JobAssemblyError("ordered runtime did not complete the unchanged candidate in STREAMING mode")
    if result.get("evaluation_scope") != "final_csv_only":
        raise JobAssemblyError("ordered runtime used an unexpected evaluation scope")
    completion = str(result.get("completion") or "")
    if "FINISHED" not in completion or "end offsets" not in completion:
        raise JobAssemblyError("ordered runtime lacks its FINISHED/end-offset completion attestation")
    collection = result.get("collection")
    if not isinstance(collection, Mapping) or collection.get("collection_complete") is not True:
        raise JobAssemblyError("output changelog collection is incomplete")
    if collection.get("isolation") != "read_committed":
        raise JobAssemblyError("output changelog was not collected with read_committed isolation")
    end_offset = collection.get("end_offset_exclusive")
    consumer_position = collection.get("consumer_position")
    if (
        not isinstance(end_offset, int)
        or not isinstance(consumer_position, int)
        or end_offset != consumer_position
    ):
        raise JobAssemblyError("output consumer did not reach the frozen end offset")
    materialization = result.get("materialization")
    if not isinstance(materialization, Mapping) or "final_row_count" not in materialization:
        raise JobAssemblyError("ordered runtime lacks changelog materialization evidence")
    if result.get("row_count") != materialization.get("final_row_count"):
        raise JobAssemblyError("runtime row count and changelog materialization disagree")

    runtime_dir = runtime_result.parent
    terminal = _read_object(runtime_dir / "job-terminal.json", "Flink terminal state")
    job_id = result.get("job_id")
    if (
        not isinstance(job_id, str)
        or re.fullmatch(r"[a-f0-9]{32}", job_id) is None
        or terminal.get("state") != "FINISHED"
        or terminal.get("jid") != job_id
    ):
        raise JobAssemblyError("Flink terminal state is not FINISHED or is bound to another job")

    ordering = _read_object(runtime_dir / "input-order.json", "input order")
    input_counts = ordering.get("complete_counts")
    transports = ordering.get("source_transports")
    if (
        ordering.get("input_multiset_preserved") is not True
        or ordering.get("per_topic_publication_plan_nondecreasing_verified") is not True
        or not isinstance(transports, Mapping)
        or not transports
        or not isinstance(input_counts, Mapping)
        or set(transports) != set(input_counts)
        or any(str(value).casefold() != "kafka" for value in transports.values())
    ):
        raise JobAssemblyError("input rows, event order, or all-Kafka transport was not verified")
    publication = _read_object(runtime_dir / "publication.json", "Kafka publication receipt")
    acknowledged = publication.get("acknowledged_counts")
    if (
        publication.get("input_topic_partition_counts_verified") is not True
        or publication.get("all_published_messages_partition_zero") is not True
        or publication.get("per_topic_publication_timestamps_nondecreasing_verified") is not True
        or not isinstance(acknowledged, Mapping)
    ):
        raise JobAssemblyError("Kafka input partitioning or timestamp order was not verified")
    for name, count in input_counts.items():
        matching = [
            value for topic, value in acknowledged.items()
            if str(topic).endswith("." + str(name))
        ]
        if len(matching) != 1 or int(matching[0]) != int(count):
            raise JobAssemblyError(f"Kafka end offset does not cover all input rows for {name}")
    if (
        collection.get("output_topic_partition_count_verified") is not True
        or collection.get("all_collected_messages_partition_zero") is not True
    ):
        raise JobAssemblyError("Kafka output partition contract was not verified")
    return result


def _validate_runtime_files(runtime_dir: Path) -> tuple[Path, Path, Path]:
    executed_sql = runtime_dir / "execute.sql"
    final_csv = runtime_dir / "final.csv"
    changelog = runtime_dir / "output-changelog.jsonl"
    if not changelog.is_file():
        changelog = runtime_dir / "output-changelog.jsonl.gz"
    for label, path in (
        ("executed Flink SQL", executed_sql),
        ("materialized final CSV", final_csv),
        ("output changelog", changelog),
    ):
        if not path.is_file():
            raise JobAssemblyError(f"{label} is missing: {path}")
    sql = executed_sql.read_text(encoding="utf-8")
    upper = sql.upper()
    if "CREATE TABLE" not in upper or "INSERT INTO __ORDERED_SINK" not in upper:
        raise JobAssemblyError("runtime execute.sql is not a complete source-to-sink Flink job")
    return executed_sql, final_csv, changelog


def freeze_executed_job(
    backend_result: Path,
    destination: Path,
    *,
    expected_logical_query: str | None = None,
) -> FrozenJob:
    """Freeze the exact successfully executed job as ``final.sql``.

    No SQL is regenerated here.  This prevents a post-run assembler from
    silently producing a script different from the one that generated the
    scored output relation.
    """

    backend, runtime_result = _runtime_result_from_backend(backend_result.resolve())
    result = _require_successful_runtime(runtime_result)
    runtime_dir = runtime_result.parent
    executed_sql, final_csv, changelog = _validate_runtime_files(runtime_dir)

    backend_csv = backend.get("final_csv")
    if not isinstance(backend_csv, str) or Path(backend_csv).resolve() != final_csv.resolve():
        raise JobAssemblyError("backend final_csv does not bind the ordered runtime final.csv")
    job_id = result.get("job_id")
    if not isinstance(job_id, str) or re.fullmatch(r"[a-f0-9]{32}", job_id) is None:
        raise JobAssemblyError("ordered runtime result has no valid Flink job id")

    backend_task_id = backend.get("task_id")
    if backend_task_id != result.get("task_id"):
        raise JobAssemblyError("backend and ordered runtime task ids disagree")
    if backend.get("execution_profile") != "ordered_final_csv_v1":
        raise JobAssemblyError("backend did not use the frozen ordered final-CSV profile")

    if expected_logical_query is not None:
        query = validate_logical_query(expected_logical_query).strip().rstrip(";")
        executed = executed_sql.read_text(encoding="utf-8")
        marker = "INSERT INTO __ordered_sink " + query + ";"
        if marker not in executed:
            raise JobAssemblyError("execute.sql does not contain the selected logical candidate verbatim")

    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.name != "final.sql":
        raise JobAssemblyError("the formal StreamSQLAgent SQL artifact must be named final.sql")
    data = executed_sql.read_bytes()
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_bytes(data)
    os.replace(temporary, destination)
    if destination.read_bytes() != data:
        raise JobAssemblyError("final.sql differs from the runtime's executed SQL")

    task_id = result.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise JobAssemblyError("ordered runtime result has no task id")
    return FrozenJob(
        final_sql=destination,
        executed_sql=executed_sql.resolve(),
        final_csv=final_csv.resolve(),
        changelog=changelog.resolve(),
        runtime_result=runtime_result,
        task_id=task_id,
        job_id=job_id,
        byte_size=len(data),
    )
