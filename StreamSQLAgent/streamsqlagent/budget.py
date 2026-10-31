"""Thread-safe multidimensional budget accounting for StreamSQLAgent."""

from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import threading
import uuid
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from .contracts import (
    BUDGET_SCHEMA_VERSION,
    JSONValue,
    ContractError,
    require_schema_version,
    utc_now_iso,
)


class BudgetExceeded(RuntimeError):
    def __init__(self, dimensions: list[str]) -> None:
        self.dimensions = tuple(dimensions)
        super().__init__("budget exceeded: " + ", ".join(dimensions))


@dataclass(frozen=True)
class BudgetLimits:
    max_model_calls: int
    max_total_tokens: int
    max_explain_calls: int
    max_runtime_probes: int
    max_wall_clock_seconds: float
    max_active_candidates: int

    def __post_init__(self) -> None:
        for name, value in self.to_dict().items():
            if value <= 0:
                raise ContractError(f"budget limit {name} must be positive")

    def to_dict(self) -> dict[str, int | float]:
        return {
            "max_model_calls": self.max_model_calls,
            "max_total_tokens": self.max_total_tokens,
            "max_explain_calls": self.max_explain_calls,
            "max_runtime_probes": self.max_runtime_probes,
            "max_wall_clock_seconds": self.max_wall_clock_seconds,
            "max_active_candidates": self.max_active_candidates,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BudgetLimits":
        return cls(
            max_model_calls=int(value["max_model_calls"]),
            max_total_tokens=int(value["max_total_tokens"]),
            max_explain_calls=int(value["max_explain_calls"]),
            max_runtime_probes=int(value["max_runtime_probes"]),
            max_wall_clock_seconds=float(value["max_wall_clock_seconds"]),
            max_active_candidates=int(value["max_active_candidates"]),
        )


@dataclass
class BudgetUsage:
    model_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    explain_calls: int = 0
    runtime_probes: int = 0
    active_candidates: int = 0

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def to_dict(self) -> dict[str, int]:
        return {
            "model_calls": self.model_calls,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "explain_calls": self.explain_calls,
            "runtime_probes": self.runtime_probes,
            "active_candidates": self.active_candidates,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "BudgetUsage":
        usage = cls(
            model_calls=int(value.get("model_calls", 0)),
            input_tokens=int(value.get("input_tokens", 0)),
            output_tokens=int(value.get("output_tokens", 0)),
            explain_calls=int(value.get("explain_calls", 0)),
            runtime_probes=int(value.get("runtime_probes", 0)),
            active_candidates=int(value.get("active_candidates", 0)),
        )
        if any(item < 0 for item in usage.to_dict().values()):
            raise ContractError("budget usage cannot be negative")
        declared_total = value.get("total_tokens")
        if declared_total is not None and int(declared_total) != usage.total_tokens:
            raise ContractError("serialized total_tokens does not match input + output tokens")
        return usage


class Budget:
    """Account for inference, tools, elapsed active time, and candidate width.

    Wall-clock time excludes explicit infrastructure pauses.  All mutation
    methods preflight the projected aggregate before committing, so a failed
    charge leaves every dimension unchanged.
    """

    def __init__(
        self,
        limits: BudgetLimits,
        *,
        usage: BudgetUsage | None = None,
        elapsed_active_seconds: float = 0.0,
        paused: bool = False,
        created_at: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        _restore_completed_usage: bool = False,
    ) -> None:
        if elapsed_active_seconds < 0:
            raise ContractError("elapsed_active_seconds cannot be negative")
        self.limits = limits
        self.usage = usage or BudgetUsage()
        self._elapsed_active_seconds = float(elapsed_active_seconds)
        self._paused = bool(paused)
        self.created_at = created_at or utc_now_iso()
        self._clock = clock
        self._segment_started = None if self._paused else self._clock()
        self._lock = threading.RLock()
        if not _restore_completed_usage:
            self._assert_usage_within_non_time_limits()

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def elapsed_active_seconds(self) -> float:
        with self._lock:
            elapsed = self._elapsed_active_seconds
            if not self._paused and self._segment_started is not None:
                elapsed += max(0.0, self._clock() - self._segment_started)
            return elapsed

    def _assert_usage_within_non_time_limits(self) -> None:
        exceeded = self._projected_exceeded(self.usage)
        exceeded = [item for item in exceeded if item != "wall_clock_seconds"]
        if exceeded:
            raise BudgetExceeded(exceeded)

    def _projected_exceeded(
        self, usage: BudgetUsage, *, elapsed: float | None = None
    ) -> list[str]:
        exceeded: list[str] = []
        if usage.model_calls > self.limits.max_model_calls:
            exceeded.append("model_calls")
        if usage.total_tokens > self.limits.max_total_tokens:
            exceeded.append("total_tokens")
        if usage.explain_calls > self.limits.max_explain_calls:
            exceeded.append("explain_calls")
        if usage.runtime_probes > self.limits.max_runtime_probes:
            exceeded.append("runtime_probes")
        if usage.active_candidates > self.limits.max_active_candidates:
            exceeded.append("active_candidates")
        if (self.elapsed_active_seconds if elapsed is None else elapsed) > self.limits.max_wall_clock_seconds:
            exceeded.append("wall_clock_seconds")
        return exceeded

    def exhausted_dimensions(self) -> tuple[str, ...]:
        """Return dimensions that cannot accept any further unit of work."""

        with self._lock:
            result: list[str] = []
            if self.usage.model_calls >= self.limits.max_model_calls:
                result.append("model_calls")
            if self.usage.total_tokens >= self.limits.max_total_tokens:
                result.append("total_tokens")
            if self.usage.explain_calls >= self.limits.max_explain_calls:
                result.append("explain_calls")
            if self.usage.runtime_probes >= self.limits.max_runtime_probes:
                result.append("runtime_probes")
            if self.usage.active_candidates >= self.limits.max_active_candidates:
                result.append("active_candidates")
            if self.elapsed_active_seconds >= self.limits.max_wall_clock_seconds:
                result.append("wall_clock_seconds")
            return tuple(result)

    def check_wall_clock(self) -> None:
        exceeded = self._projected_exceeded(self.usage)
        if exceeded:
            raise BudgetExceeded(exceeded)

    def consume_model(self, *, input_tokens: int, output_tokens: int) -> None:
        if input_tokens < 0 or output_tokens < 0:
            raise ContractError("model token counts cannot be negative")
        with self._lock:
            projected = BudgetUsage(**{
                "model_calls": self.usage.model_calls + 1,
                "input_tokens": self.usage.input_tokens + input_tokens,
                "output_tokens": self.usage.output_tokens + output_tokens,
                "explain_calls": self.usage.explain_calls,
                "runtime_probes": self.usage.runtime_probes,
                "active_candidates": self.usage.active_candidates,
            })
            self._commit_projected(projected)

    def record_completed_model_call(self, *, input_tokens: int, output_tokens: int) -> None:
        """Record an already billed call even if its atomic response crossed a limit.

        ``consume_model`` remains the atomic preflight-style operation used by
        ordinary callers.  A remote model response cannot be unbilled after it
        arrives, so the Agent uses this method to keep observed resource usage
        exact and then terminates if the response crossed a stop threshold.
        """

        if input_tokens < 0 or output_tokens < 0:
            raise ContractError("model token counts cannot be negative")
        with self._lock:
            projected = BudgetUsage(**{
                "model_calls": self.usage.model_calls + 1,
                "input_tokens": self.usage.input_tokens + input_tokens,
                "output_tokens": self.usage.output_tokens + output_tokens,
                "explain_calls": self.usage.explain_calls,
                "runtime_probes": self.usage.runtime_probes,
                "active_candidates": self.usage.active_candidates,
            })
            exceeded = self._projected_exceeded(projected)
            self.usage = projected
            if exceeded:
                raise BudgetExceeded(exceeded)

    def consume_explain(self) -> None:
        with self._lock:
            projected = BudgetUsage.from_dict(self.usage.to_dict())
            projected.explain_calls += 1
            self._commit_projected(projected)

    def consume_runtime_probe(self) -> None:
        with self._lock:
            projected = BudgetUsage.from_dict(self.usage.to_dict())
            projected.runtime_probes += 1
            self._commit_projected(projected)

    def acquire_candidate(self) -> None:
        with self._lock:
            projected = BudgetUsage.from_dict(self.usage.to_dict())
            projected.active_candidates += 1
            self._commit_projected(projected)

    def release_candidate(self) -> None:
        with self._lock:
            if self.usage.active_candidates <= 0:
                raise ContractError("cannot release an active candidate when none is acquired")
            self.usage.active_candidates -= 1

    def _commit_projected(self, projected: BudgetUsage) -> None:
        exceeded = self._projected_exceeded(projected)
        if exceeded:
            raise BudgetExceeded(exceeded)
        self.usage = projected

    def pause(self) -> None:
        """Stop charging wall-clock time during an infrastructure pause."""

        with self._lock:
            if self._paused:
                return
            assert self._segment_started is not None
            self._elapsed_active_seconds += max(0.0, self._clock() - self._segment_started)
            self._segment_started = None
            self._paused = True

    def resume(self) -> None:
        with self._lock:
            if not self._paused:
                return
            if self._elapsed_active_seconds > self.limits.max_wall_clock_seconds:
                raise BudgetExceeded(["wall_clock_seconds"])
            self._segment_started = self._clock()
            self._paused = False

    def remaining(self) -> dict[str, int | float]:
        with self._lock:
            return {
                "model_calls": max(0, self.limits.max_model_calls - self.usage.model_calls),
                "total_tokens": max(0, self.limits.max_total_tokens - self.usage.total_tokens),
                "explain_calls": max(0, self.limits.max_explain_calls - self.usage.explain_calls),
                "runtime_probes": max(0, self.limits.max_runtime_probes - self.usage.runtime_probes),
                "wall_clock_seconds": max(
                    0.0, self.limits.max_wall_clock_seconds - self.elapsed_active_seconds
                ),
                "active_candidates": max(
                    0, self.limits.max_active_candidates - self.usage.active_candidates
                ),
            }

    def to_dict(self) -> dict[str, JSONValue]:
        with self._lock:
            return {
                "schema_version": BUDGET_SCHEMA_VERSION,
                "created_at": self.created_at,
                "paused": self._paused,
                "elapsed_active_seconds": self.elapsed_active_seconds,
                "limits": self.limits.to_dict(),
                "usage": self.usage.to_dict(),
                "remaining": self.remaining(),
                "overrun_dimensions": self._projected_exceeded(self.usage),
            }

    @classmethod
    def from_dict(
        cls, value: Mapping[str, Any], *, clock: Callable[[], float] = time.monotonic
    ) -> "Budget":
        require_schema_version(value.get("schema_version"), BUDGET_SCHEMA_VERSION)
        return cls(
            BudgetLimits.from_dict(value["limits"]),
            usage=BudgetUsage.from_dict(value.get("usage", {})),
            elapsed_active_seconds=float(value.get("elapsed_active_seconds", 0.0)),
            paused=bool(value.get("paused", False)),
            created_at=str(value.get("created_at", utc_now_iso())),
            clock=clock,
            _restore_completed_usage=True,
        )


class CampaignBudgetPaused(RuntimeError):
    """Pause the serial campaign without reducing later task capabilities."""

    def __init__(self, reason: str, state: Mapping[str, Any]) -> None:
        self.reason = reason
        self.state = dict(state)
        super().__init__("campaign token budget paused: " + reason)


class CampaignTokenBudget:
    """Durable request reservations for a serial model-call campaign.

    Reserve the estimated input plus the configured full completion allowance
    before sending a request. A pending reservation survives a crash and blocks
    further calls until its actual usage is recovered, the caller confirms
    that it was never sent, or explicitly settles unknown usage with a hold.
    A timeout alone never authorizes cancellation or another request.
    This ledger is for the existing serial campaign, not concurrent workers.
    """

    VERSION = "streamsqlagent_campaign_token_budget_v1"

    def __init__(self, path: Path, token_limit: int) -> None:
        self.path = Path(path)
        self.token_limit = self._tokens(token_limit, "token_limit")
        if self.token_limit == 0:
            raise ContractError("campaign token_limit must be positive")
        self._lock = threading.RLock()
        if self.path.exists():
            self._read()
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._write({"schema_version": self.VERSION, "token_limit": self.token_limit,
                         "created_at": utc_now_iso(), "reservations": {}})

    @staticmethod
    def _tokens(value: int, name: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ContractError(f"{name} must be a nonnegative integer")
        return value

    def _read(self) -> dict[str, Any]:
        try:
            state = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ContractError("cannot read campaign token budget") from exc
        if not isinstance(state, dict) or state.get("schema_version") != self.VERSION:
            raise ContractError("unsupported campaign token budget schema")
        if state.get("token_limit") != self.token_limit:
            raise ContractError("campaign token limit differs from the persisted limit")
        reservations = state.get("reservations")
        if not isinstance(reservations, dict):
            raise ContractError("invalid campaign reservations")
        for item in reservations.values():
            if not isinstance(item, dict) or item.get("status") not in {"pending", "completed", "cancelled", "unknown"}:
                raise ContractError("invalid campaign reservation state")
            self._tokens(item.get("input_tokens"), "input_tokens")
            self._tokens(item.get("max_output_tokens"), "max_output_tokens")
            if item["status"] == "completed":
                self._tokens(item.get("actual_tokens"), "actual_tokens")
            if item["status"] == "unknown":
                self._tokens(item.get("input_hold"), "input_hold")
                self._tokens(item.get("output_hold"), "output_hold")
                if ("actual_tokens" in item or not isinstance(item.get("evidence"), str)
                        or not item["evidence"].strip()
                        or item["input_hold"] < item["input_tokens"]
                        or item["output_hold"] < item["max_output_tokens"]):
                    raise ContractError("invalid unknown campaign usage hold")
        return state

    def _write(self, state: Mapping[str, Any]) -> None:
        # Same-directory replace preserves the prior ledger if writing fails.
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self.path.parent,
                                             prefix="." + self.path.name + ".", delete=False) as stream:
                temporary = stream.name
                json.dump(state, stream, ensure_ascii=False, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if temporary is not None and os.path.exists(temporary):
                os.unlink(temporary)

    def _snapshot(self, state: Mapping[str, Any]) -> dict[str, Any]:
        reservations = state["reservations"]
        committed = sum(item["actual_tokens"] for item in reservations.values()
                        if item["status"] == "completed")
        reserved = sum(item["input_tokens"] + item["max_output_tokens"]
                       for item in reservations.values() if item["status"] == "pending")
        unknown_input = sum(item["input_hold"] for item in reservations.values() if item["status"] == "unknown")
        unknown_output = sum(item["output_hold"] for item in reservations.values() if item["status"] == "unknown")
        unknown_hold = unknown_input + unknown_output
        return {**state, "committed_tokens": committed, "reserved_tokens": reserved,
                "unknown_input_hold": unknown_input, "unknown_output_hold": unknown_output,
                "unknown_hold_tokens": unknown_hold,
                "remaining_tokens": max(0, self.token_limit - committed - reserved - unknown_hold),
                "overrun_tokens": max(0, committed + unknown_hold - self.token_limit)}

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return self._snapshot(self._read())

    def reserve(self, input_tokens: int, max_output_tokens: int) -> str:
        input_tokens = self._tokens(input_tokens, "input_tokens")
        max_output_tokens = self._tokens(max_output_tokens, "max_output_tokens")
        if max_output_tokens == 0:
            raise ContractError("max_output_tokens must preserve a positive completion allowance")
        with self._lock:
            state = self._read()
            snapshot = self._snapshot(state)
            if any(item["status"] == "pending" for item in state["reservations"].values()):
                raise CampaignBudgetPaused("unresolved_request", snapshot)
            if input_tokens + max_output_tokens > snapshot["remaining_tokens"]:
                raise CampaignBudgetPaused("token_limit", snapshot)
            reservation = uuid.uuid4().hex
            state["reservations"][reservation] = {
                "status": "pending", "input_tokens": input_tokens,
                "max_output_tokens": max_output_tokens, "reserved_at": utc_now_iso(),
            }
            self._write(state)
            return reservation

    def complete(self, reservation: str, actual_tokens: int) -> None:
        actual_tokens = self._tokens(actual_tokens, "actual_tokens")
        with self._lock:
            state = self._read()
            item = state["reservations"].get(reservation)
            if item is None:
                raise ContractError("unknown campaign reservation")
            if item["status"] == "completed":
                if item["actual_tokens"] != actual_tokens:
                    raise ContractError("completed campaign reservation has different actual usage")
                return
            if item["status"] != "pending":
                raise ContractError("cannot complete a " + item["status"] + " campaign reservation")
            item.update(status="completed", actual_tokens=actual_tokens, completed_at=utc_now_iso())
            # Billed usage is recorded even when the input estimate was too low;
            # the next request pauses instead of hiding the observed overrun.
            self._write(state)

    def settle_unknown(self, reservation: str, input_hold: int, output_hold: int, evidence: str) -> None:
        """Explicitly retain unknown billed usage while permitting a later request.

        This is an operator-authorized recovery action, never called by the
        transport on failure. Holds consume budget but are not actual usage.
        """
        input_hold = self._tokens(input_hold, "input_hold")
        output_hold = self._tokens(output_hold, "output_hold")
        if not isinstance(evidence, str) or not evidence.strip():
            raise ContractError("unknown usage settlement requires evidence")
        settlement = {"input_hold": input_hold, "output_hold": output_hold, "evidence": evidence}
        with self._lock:
            state = self._read()
            item = state["reservations"].get(reservation)
            if item is None:
                raise ContractError("unknown campaign reservation")
            if item["status"] == "unknown":
                if any(item[key] != value for key, value in settlement.items()):
                    raise ContractError("unknown campaign reservation has different settlement")
                return
            if item["status"] != "pending":
                raise ContractError("only a pending campaign reservation can settle unknown usage")
            if input_hold < item["input_tokens"] or output_hold < item["max_output_tokens"]:
                raise ContractError("unknown usage hold cannot reduce the original reservation")
            item.update(status="unknown", **settlement, settled_at=utc_now_iso())
            self._write(state)

    def cancel(self, reservation: str, *, request_not_sent: bool = False) -> None:
        if request_not_sent is not True:
            raise ContractError("cancel requires explicit confirmation that the request was not sent")
        with self._lock:
            state = self._read()
            item = state["reservations"].get(reservation)
            if item is None:
                raise ContractError("unknown campaign reservation")
            if item["status"] in {"completed", "unknown"}:
                raise ContractError("cannot cancel a " + item["status"] + " campaign reservation")
            if item["status"] == "cancelled":
                return
            item.update(status="cancelled", request_not_sent=True, cancelled_at=utc_now_iso())
            self._write(state)
