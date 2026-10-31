"""Strict Flink SQL Gateway REST collector for bounded historical queries.

The collector uses SQL Gateway's versioned JSON protocol instead of scraping
SQL-client TABLEAU output.  It executes one renderer-owned SQL file in one
session, drains every operation to an explicit ``EOS``, and exposes the final
statement's planner schema plus typed rows.  Transport, statement, and protocol
failures are distinct so the assembled runner can preserve layer attribution.
"""

from __future__ import annotations

import json
import math
import re
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


class SqlGatewayError(RuntimeError):
    """Base class for SQL Gateway collector failures."""


class SqlGatewayTransportError(SqlGatewayError):
    """The configured gateway cannot be reached or returned an HTTP failure."""


class SqlGatewayStatementError(SqlGatewayError):
    """Flink rejected or failed a submitted SQL statement."""


class SqlGatewayProtocolError(SqlGatewayError):
    """The gateway response is incomplete, ambiguous, or structurally invalid."""


class SqlGatewayOperationTimeout(SqlGatewayStatementError):
    """A submitted operation did not terminate before the explicit deadline."""


@dataclass(frozen=True)
class GatewayColumn:
    name: str
    logical_type: Mapping[str, Any]

    @property
    def type_name(self) -> str:
        value = self.logical_type.get("type")
        return str(value).upper() if isinstance(value, str) else ""

    @property
    def nullable(self) -> bool:
        return self.logical_type.get("nullable") is not False

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "logicalType": dict(self.logical_type)}


def _typed_scalar(value: Any) -> list[Any]:
    if value is None:
        return ["null", None]
    if isinstance(value, bool):
        return ["boolean", value]
    if isinstance(value, int):
        return ["integer", str(value)]
    if isinstance(value, Decimal):
        return ["decimal", format(value, "f")]
    if isinstance(value, float):
        return ["float", repr(value)]
    if isinstance(value, str):
        return ["string", value]
    return [type(value).__name__, value]


@dataclass(frozen=True)
class GatewayResult:
    columns: tuple[GatewayColumn, ...]
    rows: tuple[tuple[Any, ...], ...]
    row_kinds: tuple[str, ...]
    page_count: int
    eos_observed: bool
    operation_count: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "collector_contract": "flink_sql_gateway_json_v1",
            "columns": [column.as_dict() for column in self.columns],
            "data": [
                {
                    "kind": kind,
                    "fields": [_typed_scalar(value) for value in row],
                }
                for kind, row in zip(self.row_kinds, self.rows)
            ],
            "page_count": self.page_count,
            "eos_observed": self.eos_observed,
            "operation_count": self.operation_count,
        }


class JsonTransport(Protocol):
    def request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]: ...


class UrllibJsonTransport:
    """Small dependency-free JSON transport with no automatic request retry."""

    def __init__(self, base_url: str, timeout_seconds: float = 30.0) -> None:
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("SQL Gateway base URL must be an absolute http(s) URL")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        if not path.startswith("/"):
            raise SqlGatewayProtocolError(f"gateway request path is not absolute: {path!r}")
        body = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base_url + path,
            data=body,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            # Decode and parse the complete response before bounding the text
            # copied into an exception.  Flink's ValidationException payloads
            # can exceed the diagnostic limit; truncating first turns valid
            # ``{"errors": ...}`` JSON into a transport failure and loses the
            # statement/compile attribution.
            full_detail = raw.decode("utf-8", errors="replace")
            try:
                decoded = json.loads(full_detail)
            except json.JSONDecodeError:
                decoded = None
            errors = decoded.get("errors") if isinstance(decoded, Mapping) else None
            if errors:
                bounded_errors = str(errors)[:8000]
                raise SqlGatewayStatementError(
                    f"SQL Gateway operation HTTP {exc.code}: {bounded_errors}"
                ) from exc
            detail = full_detail[:8000]
            raise SqlGatewayTransportError(
                f"SQL Gateway HTTP {exc.code} for {path}: {detail}"
            ) from exc
        except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as exc:
            raise SqlGatewayTransportError(
                f"cannot reach SQL Gateway at {self.base_url}: {exc}"
            ) from exc
        try:
            decoded = json.loads(raw.decode("utf-8"), parse_float=Decimal)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SqlGatewayProtocolError(
                f"SQL Gateway returned non-JSON data for {path}"
            ) from exc
        if not isinstance(decoded, Mapping):
            raise SqlGatewayProtocolError(
                f"SQL Gateway response root is not an object for {path}"
            )
        return decoded


def _sql_without_comments(statement: str) -> str:
    statement = re.sub(r"/\*.*?\*/", " ", statement, flags=re.S)
    statement = re.sub(r"--[^\n]*(?:\n|$)", " ", statement)
    return statement.strip()


def split_sql_statements(sql: str) -> tuple[str, ...]:
    """Split renderer SQL at top-level semicolons, preserving quoted content."""

    statements: list[str] = []
    current: list[str] = []
    quote: str | None = None
    line_comment = False
    block_comment = False
    index = 0
    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if line_comment:
            current.append(char)
            if char == "\n":
                line_comment = False
            index += 1
            continue
        if block_comment:
            current.append(char)
            if char == "*" and following == "/":
                current.append(following)
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if quote is not None:
            current.append(char)
            if char == quote:
                if following == quote and quote in {"'", '"', "`"}:
                    current.append(following)
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char == "-" and following == "-":
            current.extend((char, following))
            line_comment = True
            index += 2
            continue
        if char == "/" and following == "*":
            current.extend((char, following))
            block_comment = True
            index += 2
            continue
        if char in {"'", '"', "`"}:
            quote = char
            current.append(char)
            index += 1
            continue
        if char == ";":
            candidate = "".join(current).strip()
            if _sql_without_comments(candidate):
                statements.append(candidate)
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    if quote is not None or block_comment:
        raise SqlGatewayProtocolError("SQL file contains an unterminated quote or comment")
    trailing = "".join(current).strip()
    if _sql_without_comments(trailing):
        raise SqlGatewayProtocolError("every SQL Gateway statement must end with a semicolon")
    if not statements:
        raise SqlGatewayProtocolError("SQL file contains no executable statements")
    return tuple(statements)


_PRESENTATION_SET = re.compile(
    r"^SET\s+'sql-client\.execution\.result-mode'\s*=", re.I | re.S
)
_ALLOWED_SETUP = re.compile(r"^(?:SET\b|CREATE\s+TEMPORARY\s+TABLE\b)", re.I | re.S)
_ALLOWED_TERMINAL = re.compile(r"^(?:SELECT\b|WITH\b|EXPLAIN\b)", re.I | re.S)


def _validate_statement_boundary(statements: Sequence[str]) -> tuple[str, ...]:
    cleaned = tuple(_sql_without_comments(statement) for statement in statements)
    if not _ALLOWED_TERMINAL.match(cleaned[-1]):
        raise SqlGatewayProtocolError(
            "terminal renderer statement must be SELECT, WITH, or EXPLAIN"
        )
    for statement in cleaned[:-1]:
        if not _ALLOWED_SETUP.match(statement):
            raise SqlGatewayProtocolError(
                f"unsupported renderer setup statement: {statement[:80]!r}"
            )
    return tuple(
        original
        for original, plain in zip(statements, cleaned)
        if not _PRESENTATION_SET.match(plain)
    )


_INTEGER_TYPES = {"TINYINT", "SMALLINT", "INTEGER", "INT", "BIGINT"}
_FLOAT_TYPES = {"FLOAT", "REAL", "DOUBLE"}
_STRING_TYPES = {
    "CHAR",
    "VARCHAR",
    "STRING",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "TIME_WITHOUT_TIME_ZONE",
    "TIMESTAMP_WITHOUT_TIME_ZONE",
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
}
_BINARY_TYPES = {"BINARY", "VARBINARY"}


def coerce_gateway_scalar(value: Any, column: GatewayColumn) -> Any:
    """Validate one JSON field against the planner-owned logical type."""

    if value is None:
        if not column.nullable:
            raise SqlGatewayProtocolError(
                f"non-nullable gateway column {column.name!r} returned NULL"
            )
        return None
    type_name = column.type_name
    if type_name in _INTEGER_TYPES:
        if isinstance(value, bool) or not isinstance(value, int):
            raise SqlGatewayProtocolError(
                f"gateway column {column.name!r} expected {type_name}, got {value!r}"
            )
        return value
    if type_name == "DECIMAL":
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise SqlGatewayProtocolError(
                f"gateway column {column.name!r} expected DECIMAL, got {value!r}"
            )
        decimal = value if isinstance(value, Decimal) else Decimal(str(value))
        if not decimal.is_finite():
            raise SqlGatewayProtocolError("gateway DECIMAL must be finite")
        return decimal
    if type_name in _FLOAT_TYPES:
        if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
            raise SqlGatewayProtocolError(
                f"gateway column {column.name!r} expected {type_name}, got {value!r}"
            )
        floating = float(value)
        if not math.isfinite(floating):
            raise SqlGatewayProtocolError("gateway floating value must be finite")
        return floating
    if type_name in _STRING_TYPES or type_name in _BINARY_TYPES:
        if not isinstance(value, str):
            raise SqlGatewayProtocolError(
                f"gateway column {column.name!r} expected string JSON, got {value!r}"
            )
        return value
    if type_name == "BOOLEAN":
        if not isinstance(value, bool):
            raise SqlGatewayProtocolError(
                f"gateway column {column.name!r} expected BOOLEAN, got {value!r}"
            )
        return value
    if type_name == "NULL":
        raise SqlGatewayProtocolError(
            f"gateway NULL column {column.name!r} returned a non-NULL value"
        )
    raise SqlGatewayProtocolError(
        f"unsupported SQL Gateway logical type for strict scalar evaluation: {type_name!r}"
    )


def coerce_gold_scalar(raw: str, column: GatewayColumn) -> Any:
    """Interpret historical Gold text using the planner-owned output schema."""

    if raw == "":
        if column.type_name in _STRING_TYPES or column.type_name in _BINARY_TYPES:
            raise SqlGatewayProtocolError(
                "Gold CSV empty field is ambiguous between SQL NULL and an empty "
                f"string for planner column {column.name!r}"
            )
        return None
    type_name = column.type_name
    try:
        if type_name in _INTEGER_TYPES:
            if not re.fullmatch(r"[+-]?\d+", raw.strip()):
                raise ValueError("not an integer")
            return int(raw)
        if type_name == "DECIMAL":
            value = Decimal(raw)
            if not value.is_finite():
                raise ValueError("not finite")
            return value
        if type_name in _FLOAT_TYPES:
            value = float(raw)
            if not math.isfinite(value):
                raise ValueError("not finite")
            return value
        if type_name == "BOOLEAN":
            normalized = raw.strip().casefold()
            if normalized in {"true", "1"}:
                return True
            if normalized in {"false", "0"}:
                return False
            raise ValueError("not a boolean")
        if type_name in _STRING_TYPES or type_name in _BINARY_TYPES:
            return raw
        if type_name == "NULL":
            raise ValueError("Gold NULL must use an empty CSV field")
    except (ValueError, InvalidOperation) as exc:
        raise SqlGatewayProtocolError(
            f"Gold value {raw!r} cannot be decoded as {type_name} for {column.name!r}"
        ) from exc
    raise SqlGatewayProtocolError(
        f"unsupported SQL Gateway logical type for Gold: {type_name!r}"
    )


def _columns_from_payload(raw: Any) -> tuple[GatewayColumn, ...]:
    if not isinstance(raw, list) or not raw:
        raise SqlGatewayProtocolError("SQL Gateway payload has no result columns")
    columns: list[GatewayColumn] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise SqlGatewayProtocolError("SQL Gateway column is not an object")
        name = item.get("name")
        logical_type = item.get("logicalType")
        if not isinstance(name, str) or not isinstance(logical_type, Mapping):
            raise SqlGatewayProtocolError("SQL Gateway column lacks name/logicalType")
        column = GatewayColumn(name, dict(logical_type))
        if not column.type_name:
            raise SqlGatewayProtocolError("SQL Gateway logicalType lacks type")
        columns.append(column)
    return tuple(columns)


class SqlGatewayExecutor:
    """Execute one bounded SQL file and collect its final structured result."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8083",
        *,
        transport: JsonTransport | None = None,
        operation_timeout_seconds: float = 600.0,
        poll_interval_seconds: float = 0.2,
        max_pages: int = 100_000,
    ) -> None:
        if operation_timeout_seconds <= 0 or poll_interval_seconds < 0 or max_pages <= 0:
            raise ValueError("invalid SQL Gateway timeout/poll/page configuration")
        self.base_url = base_url.rstrip("/")
        self.transport = transport or UrllibJsonTransport(self.base_url)
        self.operation_timeout_seconds = operation_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.max_pages = max_pages

    @staticmethod
    def _result_path(session: str, operation: str, uri: str | None = None) -> str:
        prefix = f"/v1/sessions/{session}/operations/{operation}/result/"
        if uri is None:
            return prefix + "0?rowFormat=JSON"
        parsed = urllib.parse.urlsplit(uri)
        if not parsed.path.startswith(prefix):
            raise SqlGatewayProtocolError(
                f"nextResultUri escaped the active operation: {uri!r}"
            )
        token = parsed.path[len(prefix) :]
        if not token.isdigit():
            raise SqlGatewayProtocolError(
                f"nextResultUri has a non-numeric result token: {uri!r}"
            )
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        query["rowFormat"] = ["JSON"]
        return parsed.path + "?" + urllib.parse.urlencode(query, doseq=True)

    def _fetch_operation(
        self, session: str, operation: str
    ) -> tuple[tuple[GatewayColumn, ...], tuple[tuple[Any, ...], ...], tuple[str, ...], int]:
        deadline = time.monotonic() + self.operation_timeout_seconds
        path = self._result_path(session, operation)
        columns: tuple[GatewayColumn, ...] | None = None
        rows: list[tuple[Any, ...]] = []
        kinds: list[str] = []
        page_count = 0
        while True:
            if time.monotonic() >= deadline:
                try:
                    self.transport.request(
                        "POST", f"/v1/sessions/{session}/operations/{operation}/cancel", {}
                    )
                except SqlGatewayError:
                    pass
                raise SqlGatewayOperationTimeout(
                    f"SQL Gateway operation exceeded {self.operation_timeout_seconds:g}s"
                )
            response = self.transport.request("GET", path)
            result_type = response.get("resultType")
            if result_type == "NOT_READY":
                if self.poll_interval_seconds:
                    time.sleep(self.poll_interval_seconds)
                continue
            if result_type not in {"PAYLOAD", "EOS"}:
                raise SqlGatewayProtocolError(
                    f"unknown SQL Gateway resultType: {result_type!r}"
                )
            results = response.get("results")
            if results is not None:
                if not isinstance(results, Mapping):
                    raise SqlGatewayProtocolError("SQL Gateway results is not an object")
                raw_columns = results.get("columns")
                raw_data = results.get("data")
                # Flink may encode the terminal EOS page as an empty ResultSet.
                # It contributes no schema or rows; at least one earlier page
                # (or a schema-bearing EOS page for a zero-row query) is still
                # required before the result can be evaluated.
                if result_type == "EOS" and raw_columns in (None, []) and raw_data in (
                    None,
                    [],
                ):
                    page_columns = None
                else:
                    page_columns = _columns_from_payload(raw_columns)
                if page_columns is None:
                    data = []
                else:
                    data = raw_data
                    if not isinstance(data, list):
                        raise SqlGatewayProtocolError("SQL Gateway data is not an array")
                if page_columns is None:
                    pass
                elif columns is None:
                    columns = page_columns
                elif columns != page_columns:
                    raise SqlGatewayProtocolError("SQL Gateway schema changed between pages")
                for item in data:
                    if not isinstance(item, Mapping):
                        raise SqlGatewayProtocolError("SQL Gateway row is not an object")
                    kind = item.get("kind")
                    fields = item.get("fields")
                    if kind != "INSERT":
                        raise SqlGatewayProtocolError(
                            f"bounded snapshot returned unsupported row kind {kind!r}"
                        )
                    assert page_columns is not None
                    if not isinstance(fields, list) or len(fields) != len(page_columns):
                        raise SqlGatewayProtocolError("SQL Gateway row arity disagrees with schema")
                    rows.append(
                        tuple(
                            coerce_gateway_scalar(value, column)
                            for value, column in zip(fields, page_columns)
                        )
                    )
                    kinds.append(kind)
                if page_columns is not None:
                    page_count += 1
                    if page_count > self.max_pages:
                        raise SqlGatewayProtocolError("SQL Gateway result exceeded max_pages")
            next_uri = response.get("nextResultUri")
            if result_type == "EOS":
                if next_uri is not None:
                    raise SqlGatewayProtocolError("EOS response has a non-null nextResultUri")
                if columns is None:
                    raise SqlGatewayProtocolError("SQL Gateway reached EOS without a schema")
                return columns, tuple(rows), tuple(kinds), page_count
            if not isinstance(next_uri, str) or not next_uri:
                raise SqlGatewayProtocolError("PAYLOAD response has no nextResultUri")
            path = self._result_path(session, operation, next_uri)

    def _execute_statement(
        self, session: str, statement: str
    ) -> tuple[tuple[GatewayColumn, ...], tuple[tuple[Any, ...], ...], tuple[str, ...], int]:
        response = self.transport.request(
            "POST",
            f"/v1/sessions/{session}/statements",
            {
                "statement": statement,
                "executionConfig": {"rest.address": "jobmanager", "rest.port": "8081"},
            },
        )
        operation = response.get("operationHandle")
        if not isinstance(operation, str) or not operation:
            raise SqlGatewayProtocolError("statement response has no operationHandle")
        try:
            return self._fetch_operation(session, operation)
        finally:
            try:
                self.transport.request(
                    "DELETE", f"/v1/sessions/{session}/operations/{operation}/close"
                )
            except SqlGatewayError:
                pass

    def run(self, sql_file: Path) -> GatewayResult:
        statements = _validate_statement_boundary(
            split_sql_statements(sql_file.read_text(encoding="utf-8"))
        )
        response = self.transport.request(
            "POST",
            "/v1/sessions",
            {
                "sessionName": f"streamsqlbench-{sql_file.parent.name}",
                "properties": {
                    "execution.runtime-mode": "BATCH",
                    "parallelism.default": "1",
                    "table.local-time-zone": "UTC",
                    "rest.address": "jobmanager",
                    "rest.port": "8081",
                },
            },
        )
        session = response.get("sessionHandle")
        if not isinstance(session, str) or not session:
            raise SqlGatewayProtocolError("open-session response has no sessionHandle")
        final: tuple[
            tuple[GatewayColumn, ...], tuple[tuple[Any, ...], ...], tuple[str, ...], int
        ] | None = None
        try:
            for statement in statements:
                final = self._execute_statement(session, statement)
        finally:
            try:
                self.transport.request("DELETE", f"/v1/sessions/{session}")
            except SqlGatewayError:
                pass
        if final is None:
            raise SqlGatewayProtocolError("no SQL Gateway operation was executed")
        columns, rows, kinds, pages = final
        return GatewayResult(
            columns=columns,
            rows=rows,
            row_kinds=kinds,
            page_count=pages,
            eos_observed=True,
            operation_count=len(statements),
        )
