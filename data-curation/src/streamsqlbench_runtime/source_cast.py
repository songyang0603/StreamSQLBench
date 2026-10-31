"""Lossless source-value casting used by the ordered streaming curator."""

from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping


class SourceCastError(ValueError):
    """A source value cannot be represented by its declared contract."""


_INTEGER = re.compile(r"^[+-]?\d+$")
_DECIMAL_TYPE = re.compile(r"^DECIMAL\((\d+),(\d+)\)$", re.I)


def _validate_decimal(value: Decimal, flink_type: str) -> Decimal:
    match = _DECIMAL_TYPE.fullmatch(flink_type)
    if match is None:
        raise SourceCastError(f"decimal cast has invalid Flink type: {flink_type!r}")
    if not value.is_finite():
        raise SourceCastError("decimal value must be finite")
    precision, scale = map(int, match.groups())
    if precision <= 0 or scale < 0 or scale > precision:
        raise SourceCastError(f"invalid DECIMAL precision/scale: {flink_type}")
    quantum = Decimal(1).scaleb(-scale)
    try:
        if value.quantize(quantum) != value:
            raise SourceCastError(f"decimal {value} exceeds declared scale {scale}")
    except InvalidOperation as exc:
        raise SourceCastError(f"decimal {value} cannot fit {flink_type}") from exc
    if abs(value) >= Decimal(10) ** (precision - scale):
        raise SourceCastError(f"decimal {value} exceeds declared precision {precision}")
    return value


def cast_source_value(raw: str, contract: Mapping[str, Any]) -> Any:
    """Cast one CSV field without lossy Decimal or NULL coercion."""
    if raw == "":
        empty_policy = contract.get("empty_string_policy", "null")
        if empty_policy == "preserve":
            if contract.get("producer_cast") != "string":
                raise SourceCastError(
                    "empty_string_policy=preserve is valid only for string columns"
                )
            return ""
        if empty_policy != "null":
            raise SourceCastError(f"unsupported empty_string_policy: {empty_policy!r}")
        return None
    cast = contract.get("producer_cast")
    try:
        if cast == "integer":
            if not _INTEGER.fullmatch(raw.strip()):
                raise SourceCastError(f"invalid integer literal: {raw!r}")
            return int(raw)
        if cast == "float":
            value = float(raw)
            if not math.isfinite(value):
                raise SourceCastError(f"float must be finite: {raw!r}")
            return value
        if cast == "decimal":
            return _validate_decimal(Decimal(raw), str(contract.get("flink_type", "")))
        if cast == "boolean":
            normalized = raw.strip().casefold()
            if normalized in {"true", "1"}:
                return True
            if normalized in {"false", "0"}:
                return False
            raise SourceCastError(f"invalid boolean literal: {raw!r}")
        if cast == "string":
            return raw
    except (ValueError, InvalidOperation) as exc:
        raise SourceCastError(f"invalid {cast} literal: {raw!r}") from exc
    raise SourceCastError(f"unsupported producer_cast: {cast!r}")

