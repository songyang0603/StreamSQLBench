"""Feedback Observer: turn raw tool results into compact, factual feedback.

The observer is deterministic.  It never judges semantic correctness; it only
states what the engine reported and what the produced result looks like, so
that the Author and Verifier reason from the same observations:

* engine failures -> the actual message, its SQL location and the matching
  verified Flink rewrite recipe (``engine_knowledge``);
* successful executions on the complete input -> row count, column types, a
  bounded preview and result-shape signals (empty result, duplicate rows,
  all-NULL columns, integer values where a fractional quantity is requested,
  percentage/ratio scale).
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal, InvalidOperation
import re
from typing import Any, Mapping, Sequence

from .engine_knowledge import recipe_packet, rule_ids

_FRACTIONAL_WORDS = re.compile(r"\b(?:avg|average|mean|ratio|rate|percent(?:age)?|pct|share|proportion|per)\b|%", re.I)
_PERCENT_WORDS = re.compile(r"\b(?:percent(?:age)?|pct)\b|%", re.I)
_INTEGER_TYPES = ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT")
_PADDED_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}\.000$")


def _number(value: Any) -> Decimal | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return number if number.is_finite() else None


def _cell(value: Any, limit: int = 80) -> Any:
    if isinstance(value, str) and len(value) > limit:
        return value[: limit - 3] + "..."
    if isinstance(value, (bool, int, float, str)) or value is None:
        return value
    return str(value)


def result_observation(columns: Sequence[str], column_types: Sequence[str], rows: Sequence[Sequence[Any]],
                       *, question: str, preview_rows: int = 15) -> dict[str, Any]:
    columns = [str(c) for c in columns]
    types = [str(t) for t in column_types] if len(column_types) == len(columns) else [""] * len(columns)
    materialized = [list(row) if isinstance(row, (list, tuple)) else [row] for row in rows]
    total = len(materialized)
    signals: list[str] = []
    if total == 0:
        signals.append(
            "EMPTY_RESULT: the query produced no rows on the complete input. For a question that asks for a value "
            "or a list this usually means a literal/code, case, NULL-vs-empty condition, date format or join key "
            "does not match the stored data; check the data facts before accepting.")
    keyed = Counter(tuple(repr(v) for v in row) for row in materialized)
    duplicates = sum(count - 1 for count in keyed.values() if count > 1)
    if duplicates:
        signals.append(
            f"DUPLICATE_ROWS: {duplicates} of {total} rows repeat another row exactly. This is right only if the "
            "question asks for one row per record/event; if it asks for distinct entities or values, a DISTINCT, "
            "a join fan-out or an extra grouping key is the likely cause.")
    column_stats = []
    for index, name in enumerate(columns):
        values = [row[index] if index < len(row) else None for row in materialized]
        non_null = [v for v in values if v is not None]
        stat: dict[str, Any] = {"name": name, "type": types[index], "nulls": total - len(non_null),
                                "distinct": len({repr(v) for v in non_null})}
        numbers = [n for n in (_number(v) for v in non_null) if n is not None]
        if numbers and len(numbers) == len(non_null):
            stat["min"], stat["max"] = _cell(float(min(numbers))), _cell(float(max(numbers)))
        column_stats.append(stat)
        if total and not non_null:
            signals.append(f"ALL_NULL_COLUMN: {name} is NULL in every row.")
        texts = [v for v in non_null if isinstance(v, str)]
        if texts and len(texts) == len(non_null) and all(_PADDED_TIMESTAMP.match(v) for v in texts):
            signals.append(
                f"TIMESTAMP_TEXT_PADDING: every {name} value ends in '.000' (Flink renders CAST(TIMESTAMP(3) AS STRING) "
                "with milliseconds). If the stored source timestamps have no fractional seconds, format the value like "
                "the source, e.g. DATE_FORMAT(t, 'yyyy-MM-dd HH:mm:ss').")
        integer_typed = types[index].upper().split("(")[0].replace(" NOT NULL", "") in _INTEGER_TYPES
        if numbers and _FRACTIONAL_WORDS.search(name.replace('_', ' ')) and integer_typed:
            signals.append(
                f"INTEGER_RESULT: {name} is {types[index]} although its name suggests an average/ratio/rate; "
                "AVG over integers and integer division truncate in Flink.")
        if numbers and _PERCENT_WORDS.search(name.replace('_', ' ')) and max(numbers) <= 1 and min(numbers) >= 0 and len(numbers) >= 1:
            signals.append(
                f"PERCENT_SCALE: {name} is named as a percentage but every value lies in [0, 1]; check whether "
                "the question wants a 0-100 percentage.")
    preview = [[_cell(v) for v in row] for row in materialized[:preview_rows]]
    return {
        "row_count": total,
        "columns": [f"{c} {t}".strip() for c, t in zip(columns, types)],
        "preview_rows": preview,
        "preview_note": (f"first {len(preview)} of {total} rows" if total > len(preview) else "all rows"),
        "column_stats": column_stats,
        "signals": signals,
    }


def compile_feedback(sql: str, message: str) -> dict[str, Any]:
    packet = recipe_packet(message, sql)
    packet["stage"] = "flink_planning"
    return packet


def runtime_feedback(sql: str, message: str, *, resource_signal: str | None = None) -> dict[str, Any]:
    packet = recipe_packet(message, sql)
    packet["stage"] = "flink_execution"
    if resource_signal and resource_signal not in {"within_budget", "not_observed"}:
        packet["resource_signal"] = resource_signal
    return packet


def contract_feedback(expected: Sequence[str], actual: Sequence[str]) -> dict[str, Any]:
    return {
        "stage": "output_contract",
        "engine_message": "The query's output columns differ from the required output columns.",
        "required_columns_in_order": list(expected),
        "produced_columns_in_order": list(actual),
        "fix": "Return exactly the required columns in this order, aliasing each expression to its required name.",
    }


def failure_rule_ids(message: str) -> tuple[str, ...]:
    return rule_ids(message)


__all__ = ["compile_feedback", "contract_feedback", "failure_rule_ids", "result_observation", "runtime_feedback"]
