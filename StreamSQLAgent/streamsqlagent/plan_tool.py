"""Gold-free, real-Flink EXPLAIN tool for StreamSQLAgent.

This is a planning-only bridge.  It registers the same ordered-streaming
source/sink DDL used by the formal runtime and asks the configured Flink SQL
Gateway for ``EXPLAIN CHANGELOG_MODE INSERT INTO``.  It never reads fixtures, Gold,
Oracle output, or evaluator artifacts and never substitutes a SQL parser or a
mock planner for Flink.
"""

from __future__ import annotations

import fcntl
import json
import re
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from .paths import vldb_root
from typing import Any, Mapping

from .job_assembler import JobAssemblyError, validate_logical_query
from .diagnostics import extract_failure_diagnostic


DESCRIPTOR_CONTRACT = "streamsqlbench_rq2_task_package_descriptor_v1"
ORDERED_CONTRACT = "streamsqlbench_ordered_streaming_v1"


class _PlanGatewayTransport:
    """Same Gateway protocol, retaining this request's error before truncation.

    The shared transport consumes HTTPError's body before raising its bounded
    exception. Keep this Agent-only transport local; do not patch the shared
    implementation or fetch another operation's diagnostics.
    """

    def __init__(self, base_url: str, artifact_dir: Path, timeout_seconds: float = 30.0) -> None:
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("SQL Gateway base URL must be an absolute http(s) URL")
        self.base_url = base_url.rstrip("/")
        self.artifact_dir = artifact_dir
        self.timeout_seconds = timeout_seconds
        self.error_count = 0

    def request(self, method: str, path: str, payload: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        from streamsqlbench_runtime.sql_gateway import (
            SqlGatewayProtocolError, SqlGatewayStatementError, SqlGatewayTransportError,
        )

        if not path.startswith("/"):
            raise SqlGatewayProtocolError(f"gateway request path is not absolute: {path!r}")
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.base_url + path, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            full_detail = exc.read().decode("utf-8", errors="replace")
            try:
                decoded = json.loads(full_detail)
            except json.JSONDecodeError:
                decoded = None
            errors = decoded.get("errors") if isinstance(decoded, Mapping) else None
            if errors:
                failure = SqlGatewayStatementError(f"SQL Gateway operation HTTP {exc.code}: {str(errors)[:8000]}")
                # Only the exception envelope is retained: never the request's
                # SQL, result rows, or other fields in a response object.
                self.error_count += 1
                evidence = {"method": method, "path": path, "http_status": exc.code, "errors": errors}
                artifact = self.artifact_dir / f"gateway-error-{self.error_count:03d}.json"
                artifact.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
                failure.agent_gateway_error = evidence
                raise failure from exc
            raise SqlGatewayTransportError(
                f"SQL Gateway HTTP {exc.code} for {path}: {full_detail[:8000]}"
            ) from exc
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as exc:
            raise SqlGatewayTransportError(f"cannot reach SQL Gateway at {self.base_url}: {exc}") from exc
        try:
            decoded = json.loads(raw.decode("utf-8"), parse_float=Decimal)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SqlGatewayProtocolError(f"SQL Gateway returned non-JSON data for {path}") from exc
        if not isinstance(decoded, Mapping):
            raise SqlGatewayProtocolError(f"SQL Gateway response root is not an object for {path}")
        return decoded


@dataclass(frozen=True)
class PlanSummary:
    operators: tuple[str, ...]
    source_tables: tuple[str, ...]
    changelog_modes: tuple[str, ...]
    has_window: bool
    has_rank: bool
    has_join: bool
    has_deduplication: bool
    has_local_global_aggregate: bool
    has_exchange: bool
    declared_source_tables: tuple[str, ...] = ()
    changelog_observed: bool = False


@dataclass(frozen=True)
class PlanObservation:
    status: str
    observation: str
    columns: tuple[dict[str, str], ...]
    plan_text: str
    summary: PlanSummary | None
    artifact_dir: Path
    stage: str
    failure_origin: str | None = None
    reason_code: str | None = None
    # Internal provenance only; the Agent localizes it against public schema.
    # A missing field in older cached observations remains compatible.
    parser_focus: Mapping[str, Any] | None = None


def _schema_query_parser_origin(exc: Exception, call_dir: Path, source: Path, query: str) -> dict[str, str] | None:
    """Confirm this invocation failed at sqlQuery, not executeSql(source DDL).

    The planner companion handles both in one process. Its function name alone
    is not provenance. A missing/stale/ambiguous source or call line is unknown.
    """
    if not str(exc).startswith("Flink planner schema error:"):
        return None
    try:
        source_lines = source.read_text(encoding="utf-8").splitlines()
        query_lines = [i for i, line in enumerate(source_lines, 1)
                       if 'getMethod("sqlQuery", String.class)' in line
                       and '.invoke(environment, query)' in line]
        log = (call_dir / "output-schema.log").read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None
    stack_lines = {int(n) for n in re.findall(r"\bat OrderedSchema\.main\(OrderedSchema\.java:(\d+)\)", log)}
    if len(query_lines) != 1 or stack_lines != set(query_lines):
        return None
    return {"statement_kind": "schema_query", "submitted_statement": query}


def _vldb_root() -> Path:
    return vldb_root()


def _read_descriptor(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read task-package descriptor: {exc}") from exc
    if not isinstance(value, dict) or value.get("contract") != DESCRIPTOR_CONTRACT:
        raise ValueError("unsupported task-package descriptor")
    if value.get("backend_kind") != "ordered_final":
        raise ValueError("StreamSQLAgent plan tool requires the ordered-final backend")
    spec = value.get("ordered_spec")
    if not isinstance(spec, dict) or spec.get("contract") != ORDERED_CONTRACT:
        raise ValueError("descriptor lacks an ordered-streaming specification")
    # The descriptor is a public runtime binding.  Evaluation assets must never
    # become visible merely because a caller supplied a different JSON file.
    forbidden = {"gold", "gold_sql", "gold_csv", "gold_relation", "oracle", "expected_rows", "verdict"}

    def keys(node: Any) -> set[str]:
        if isinstance(node, Mapping):
            return {str(key).casefold() for key in node} | {
                child for item in node.values() for child in keys(item)
            }
        if isinstance(node, list):
            return {child for item in node for child in keys(item)}
        return set()

    leaked = forbidden & keys(value)
    if leaked:
        raise ValueError(f"task-package descriptor exposes evaluator-only fields: {sorted(leaked)}")
    return value


def summarize_plan_text(plan_text: str, declared_sources: tuple[str, ...] = ()) -> PlanSummary:
    """Create a bounded structural summary while retaining the full plan artifact."""

    # Prefer the physical plan: unused logical sources can disappear in optimization.
    physical = re.search(r"== Optimized Physical Plan[^\n]*==\s*(.*?)(?=\n==|\Z)", plan_text, re.S | re.I)
    structural_text = physical.group(1) if physical else plan_text
    canonical = structural_text.casefold()
    operator_patterns = {
        "source": r"\b(?:tablesourcescan|tablesource|source|tablescan)\b",
        "calc": r"\bcalc\b",
        "filter": r"\bfilter\b",
        "join": r"\b(?:join|intervaljoin|lookupjoin|temporaljoin)\b",
        "window_aggregate": r"\b(?:windowaggregate|window aggregate|groupwindowaggregate)\b",
        "group_aggregate": r"\b(?:groupaggregate|group aggregate)\b",
        "local_aggregate": r"\blocal(?:window)?aggregate\b",
        "global_aggregate": r"\bglobal(?:window)?aggregate\b",
        "rank": r"\b(?:windowrank|rank|topn|top-n)\b",
        "deduplicate": r"\b(?:windowdeduplicate|deduplicate)\b",
        "union": r"\bunion\b",
        "exchange": r"\bexchange\b",
        "sink": r"\b(?:tablesink|sink)\b",
    }
    operators = tuple(
        name for name, pattern in operator_patterns.items() if re.search(pattern, canonical)
    )
    parsed_sources: set[str] = set()
    for match in re.finditer(
        r"\b(?:TableSourceScan|TableSource|Source|TableScan)\s*\([^()\n]*?table\s*=\s*\[\[([^\]]+)\]\]",
        structural_text, flags=re.I,
    ):
        parts = [part.strip().strip("`") for part in match.group(1).split(",")]
        if parts:
            parsed_sources.add(parts[-1])
    changelog_modes = tuple(sorted({
        ",".join(part.strip().upper() for part in value.split(","))
        for value in re.findall(r"changelog(?:mode)?\s*=\s*\[([^\]]+)\]", structural_text, flags=re.I)
    }))
    local_global = (
        "local_aggregate" in operators and "global_aggregate" in operators
    )
    return PlanSummary(
        operators=operators,
        source_tables=tuple(sorted(parsed_sources)),
        changelog_modes=changelog_modes,
        has_window="window_aggregate" in operators or "windowrank" in canonical,
        has_rank="rank" in operators,
        has_join="join" in operators,
        has_deduplication="deduplicate" in operators,
        has_local_global_aggregate=local_global,
        has_exchange="exchange" in operators,
        declared_source_tables=tuple(sorted(set(declared_sources))),
        changelog_observed=bool(changelog_modes),
    )


class FlinkPlanTool:
    """Run real Flink planning against one public ordered task package."""

    def __init__(self, task_package: Path, artifact_root: Path) -> None:
        self.task_package = task_package.resolve()
        self.descriptor = _read_descriptor(self.task_package)
        self.artifact_root = artifact_root.resolve()
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self.calls = 0

    def explain(self, sql: str) -> PlanObservation:
        self.calls += 1
        call_dir = self.artifact_root / f"call-{self.calls:03d}"
        call_dir.mkdir(parents=True, exist_ok=False)
        try:
            query = validate_logical_query(sql)
            return self._explain(query, call_dir)
        except Exception as exc:
            observation = self._classify_failure(exc, call_dir)
            self._write_result(observation)
            return observation

    def _explain(self, query: str, call_dir: Path) -> PlanObservation:
        descriptor = self.descriptor
        spec = descriptor["ordered_spec"]
        runtime_source = Path(str(descriptor["runtime_source_root"])).resolve()
        # Use the same complete curation runtime as the frozen RQ2 adapter.
        # The publication checkout intentionally contains a smaller source
        # subset and does not own runtime-only Java planner companions.
        curation_source = _vldb_root()
        for source in (runtime_source, curation_source):
            if not source.is_dir():
                raise RuntimeError(f"required runtime source is missing: {source}")
            if str(source) not in sys.path:
                sys.path.insert(0, str(source))

        from streamsqlbench_curation import ordered_streaming  # type: ignore
        from streamsqlbench_runtime.sql_gateway import SqlGatewayExecutor, split_sql_statements  # type: ignore

        sources = spec.get("sources")
        if not isinstance(sources, list) or not sources:
            raise ValueError("ordered task has no public sources")
        names = tuple(str(source["name"]) for source in sources)
        if len(names) != len(set(names)):
            raise ValueError("ordered task has duplicated source names")
        token = "streamsqlagent-plan-" + uuid.uuid4().hex[:16]
        topics = {name: f"ssb.plan.{token}.{name}" for name in names}
        # EXPLAIN never consumes Kafka. Zero is a valid bounded offset used only
        # to make the exact production DDL plannable without publishing data.
        counts = {name: 0 for name in names}
        bootstrap = str(spec.get("kafka_bootstrap_servers", "kafka:29092"))
        ddl = ordered_streaming.render_source_ddl(
            str(spec["source_ddl"]), topics, counts, token, bootstrap
        )
        project = Path(str(descriptor["runtime_project_root"])).resolve()
        gateway_url = str(spec.get("gateway_url", descriptor.get("gateway_url", "http://127.0.0.1:8083")))
        lock_path = ordered_streaming.runtime_lock_path(project)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        session = None
        plan_text = ""
        columns: list[dict[str, str]] = []
        with lock_path.open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            executor = SqlGatewayExecutor(
                gateway_url, transport=_PlanGatewayTransport(gateway_url, call_dir),
                operation_timeout_seconds=90,
            )
            transport = executor.transport
            try:
                try:
                    columns = ordered_streaming.planner_schema(
                        ddl, query, project, token, call_dir / "output-schema.json"
                    )
                except Exception as exc:
                    origin = _schema_query_parser_origin(
                        exc, call_dir, Path(ordered_streaming.__file__).with_name("OrderedSchema.java"), query)
                    if origin:
                        exc.agent_parser_origin = origin
                    raise
                sink = ordered_streaming.sink_ddl(
                    columns, f"ssb.plan.{token}.output", token, bootstrap
                )
                properties = {
                    "execution.runtime-mode": "STREAMING",
                    "table.local-time-zone": "UTC",
                    "parallelism.default": "1",
                    "rest.address": "jobmanager",
                    "rest.port": "8081",
                }
                opened = transport.request("POST", "/v1/sessions", {
                    "sessionName": token,
                    "properties": properties,
                })
                session = opened["sessionHandle"]
                for statement in split_sql_statements(ddl):
                    executor._execute_statement(session, statement)
                executor._execute_statement(session, sink)
                planned = self._execute_explain_statement(executor, session, query)
                plan_text = "\n".join(str(row[0]) for row in planned[1])
                (call_dir / "plan.txt").write_text(plan_text + "\n", encoding="utf-8")
                (call_dir / "explain.sql").write_text(
                    ddl + sink + ";\nEXPLAIN CHANGELOG_MODE INSERT INTO __ordered_sink " + query + ";\n",
                    encoding="utf-8",
                )
            finally:
                if session is not None:
                    transport.request("DELETE", f"/v1/sessions/{session}")

        summary = summarize_plan_text(plan_text, names)
        observation = PlanObservation(
            status="success",
            observation=(
                f"Real Flink EXPLAIN succeeded; output has {len(columns)} columns and "
                f"the plan exposes {len(summary.operators)} operator classes."
            ),
            columns=tuple(dict(column) for column in columns),
            plan_text=plan_text,
            summary=summary,
            artifact_dir=call_dir,
            stage="compile",
        )
        self._write_result(observation)
        return observation

    @staticmethod
    def _execute_explain_statement(executor, session: str, query: str):
        statement = "EXPLAIN CHANGELOG_MODE INSERT INTO __ordered_sink " + query
        try:
            return executor._execute_statement(session, statement)
        except Exception as exc:
            # Attach only to this statement's exception; an unrelated cleanup
            # exception must not inherit the prior statement via __context__.
            exc.agent_parser_origin = {"statement_kind": "explain_insert", "submitted_statement": statement}
            raise

    @staticmethod
    def _classify_failure(exc: Exception, call_dir: Path) -> PlanObservation:
        message = f"{type(exc).__name__}: {exc}"
        parser_origin = getattr(exc, "agent_parser_origin", None)
        # Follow only explicit causes. An unrelated cleanup failure can have a
        # preceding statement in __context__; it must not inherit that SQL's
        # attribution. Never reload old gateway-error files from disk here.
        current: BaseException | None = exc
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            if current is not exc:
                message += f"\nCaused by: {type(current).__name__}: {current}"
            evidence = getattr(current, "agent_gateway_error", None)
            if isinstance(evidence, Mapping) and evidence.get("errors"):
                message = json.dumps({"errors": evidence["errors"]}, ensure_ascii=False)
                break
            current = current.__cause__
        if str(exc).startswith("Flink planner schema error:"):
            # The shared planner raises only the final 9000 stderr characters;
            # its complete log belongs to this call and retains the cause head.
            try:
                schema_log = (call_dir / "output-schema.log").read_text(encoding="utf-8")
            except (OSError, UnicodeError):
                schema_log = ""
            if schema_log.strip():
                message = f"{type(exc).__name__}: Flink planner schema error: {schema_log}"
        detail = extract_failure_diagnostic(message, stage="compile")
        # Calcite/Flink can surface planner exceptions for candidate-local SQL
        # features that this streaming planner does not support.  The SQL
        # Gateway and Flink cluster are still healthy in these cases: a
        # different candidate can be planned immediately.  Treat only the
        # precise, observed planner markers below as candidate-local so the
        # Agent repairs the SQL instead of pausing the entire campaign.  Other
        # NullPointerExceptions and TableExceptions remain infrastructure
        # failures.
        correlation_planner_failure = "cm.mapCorToCorRel.get($cor" in message
        multi_field_count_planner_failure = (
            "We now only support the count of one field." in message
        )
        unsupported_calendar_window_planner_failure = (
            "Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit. "
            "MONTH and YEAR time unit are not supported yet." in message
        )
        candidate_failure = (
            detail["category"] not in {"infrastructure", "memory_limit", "checkpoint_timeout", "state_runtime"}
            and (isinstance(exc, (JobAssemblyError, ValueError))
                 or re.search(r"\b(?:ValidationException|SqlValidatorException|SqlParserException|SqlParseException)\b", str(detail["message"]))
                 or detail["category"] not in {"runtime_failure", "unknown"}
                 or ("Error while applying rule " in message
                     and "org.apache.calcite.plan.volcano.VolcanoRuleCall" in message
                     and "java.lang.IndexOutOfBoundsException" in message))
            or correlation_planner_failure
            or multi_field_count_planner_failure
            or unsupported_calendar_window_planner_failure
        )
        return PlanObservation(
            status="compile_error" if candidate_failure else "infrastructure_error",
            observation=str(detail["message"]),
            columns=(),
            plan_text="",
            summary=None,
            artifact_dir=call_dir,
            stage="compile" if candidate_failure else "infrastructure",
            failure_origin="candidate_sql" if candidate_failure else "infrastructure",
            reason_code="flink_explain" if candidate_failure else "planner_infrastructure",
            parser_focus=(dict(parser_origin) if candidate_failure and detail["category"] == "planner_syntax"
                          and isinstance(parser_origin, Mapping) else None),
        )

    @staticmethod
    def _write_result(observation: PlanObservation) -> None:
        payload = asdict(observation)
        payload["artifact_dir"] = str(observation.artifact_dir)
        path = observation.artifact_dir / "plan-result.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
