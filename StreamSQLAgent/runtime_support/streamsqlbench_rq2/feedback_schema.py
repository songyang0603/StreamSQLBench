"""Planner-owned schema for read-only method queries, independent of final Gold.

The query is not rewritten. Flink describes it and a Debezium sink retains
INSERT/DELETE changes and duplicate rows without inventing a primary key.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def source_ddl(wrapper: str, split_statements) -> str:
    statements = []
    for statement in split_statements(wrapper):
        visible = re.sub(r"(?m)^\s*--[^\n]*", "", statement).strip()
        if re.match(r"(?is)^INSERT\s+INTO\s+canary_sink\b", visible):
            continue
        if re.match(r"(?is)^CREATE\s+(?:TEMPORARY\s+)?TABLE\s+`?canary_sink`?\s*\(", visible):
            continue
        statements.append(statement)
    return ";\n".join(statements) + ";\n"


def sink_ddl(columns: list[dict[str, str]]) -> str:
    if not columns or len({c["name"] for c in columns}) != len(columns):
        raise ValueError("query result must have a nonempty unambiguous planner schema")
    fields = []
    for column in columns:
        name = column["name"].replace("`", "``")
        # Rowtime indicators describe planner metadata, not a sink field type.
        kind = re.sub(r"\s*\*(?:\(ROWTIME\)|ROWTIME)\*", "", column["type"])
        fields.append(f"`{name}` {kind}")
    return (
        "CREATE TABLE canary_sink (" + ", ".join(fields) + ") WITH (\n"
        "'connector'='kafka', 'topic'='__OUTPUT_TOPIC__',\n"
        "'properties.bootstrap.servers'='kafka:29092', 'format'='debezium-json',\n"
        "'sink.delivery-guarantee'='exactly-once',\n"
        "'sink.transactional-id-prefix'='__RUN_ID__-',\n"
        "'properties.transaction.timeout.ms'='300000');\n"
    )


def describe_query(ddl: str, query: str, gateway_url: str, artifact: Path) -> dict[str, Any]:
    """Only planner/DDL operations; never executes candidate rows or reads Gold."""
    from streamsqlbench_runtime.sql_gateway import SqlGatewayExecutor, split_sql_statements
    executor = SqlGatewayExecutor(gateway_url, operation_timeout_seconds=90)
    transport = executor.transport
    opened = transport.request("POST", "/v1/sessions", {
        "sessionName": "rq2-readonly-query-schema",
        "properties": {"execution.runtime-mode": "STREAMING", "table.local-time-zone": "UTC",
                       "parallelism.default": "1", "rest.address": "jobmanager", "rest.port": "8081"},
    })
    session = opened["sessionHandle"]
    try:
        for statement in split_sql_statements(ddl):
            executor._execute_statement(session, statement)
        executor._execute_statement(session, "CREATE TEMPORARY VIEW __rq2_query AS " + query)
        schema = executor._execute_statement(session, "DESCRIBE __rq2_query")
        plan = executor._execute_statement(session, "EXPLAIN PLAN FOR " + query)
        columns = [{"name": str(row[0]), "type": str(row[1])} for row in schema[1]]
        plan_text = "\n".join(str(row[0]) for row in plan[1])
        result = {"columns": columns, "plan": plan_text}
        import json
        artifact.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        return result
    finally:
        transport.request("DELETE", f"/v1/sessions/{session}")


def plan_source_names(plan: str) -> set[str]:
    """Read physical table names from Flink EXPLAIN, not SQL string heuristics."""
    names = set()
    for match in re.finditer(r"\btable=\[\[([^\]]+)\]\]", plan):
        parts = [p.strip().strip("`") for p in match.group(1).split(",")]
        if parts:
            names.add(parts[-1])
    return names


def execute_static_query(ddl, query, gateway_url):
    """Run a planner-confirmed source-free/static query with its own schema."""
    from streamsqlbench_runtime.sql_gateway import SqlGatewayExecutor, split_sql_statements
    executor = SqlGatewayExecutor(gateway_url)
    transport = executor.transport
    session = transport.request("POST", "/v1/sessions", {
        "properties": {"execution.runtime-mode": "STREAMING", "table.local-time-zone": "UTC",
                       "parallelism.default": "1", "rest.address": "jobmanager", "rest.port": "8081"},
    })["sessionHandle"]
    try:
        for statement in split_sql_statements(ddl):
            executor._execute_statement(session, statement)
        columns, rows, kinds, _ = executor._execute_statement(session, query)
        if any(kind != "INSERT" for kind in kinds):
            raise ValueError("static query unexpectedly returned a non-insert changelog")
        return columns, rows
    finally:
        transport.request("DELETE", f"/v1/sessions/{session}")
