"""Matched GLM-5.1 transport for StreamSQLAgent.

The transport deliberately reuses the exact OpenAI-compatible client used by
the frozen RQ2 baselines.  This keeps the model identifier, disabled-thinking
setting and trace format comparable while exposing usage to the agent's
multidimensional budget. This adapter makes one transport attempt per journal
request; uncertain failures stay pending instead of silently reposting.
"""

from __future__ import annotations

from dataclasses import dataclass
import importlib.util
import json
import os
from pathlib import Path
from .paths import vldb_root
import sys
from types import ModuleType
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class ModelResponse:
    text: str
    input_tokens: int
    output_tokens: int
    finish_reason: str | None
    usage_reported: bool


def load_environment(path: Path) -> None:
    """Load KEY=VALUE lines without printing or serializing credentials."""

    if not path.is_file():
        raise FileNotFoundError(f"model environment file does not exist: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _load_common() -> ModuleType:
    vldb = vldb_root()
    rq2 = vldb
    source_root = rq2
    common_path = rq2 / "adapters" / "common.py"
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))
    name = "_streamsqlagent_matched_rq2_common"
    cached = sys.modules.get(name)
    if cached is not None:
        return cached
    spec = importlib.util.spec_from_file_location(name, common_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import matched GLM client: {common_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def _last_jsonl(path: Path) -> Mapping[str, Any]:
    if not path.is_file():
        return {}
    with path.open("rb") as handle:
        handle.seek(0, 2)
        end = handle.tell()
        if end == 0:
            return {}
        position = end - 1
        while position > 0:
            handle.seek(position)
            if handle.read(1) == b"\n" and position < end - 1:
                break
            position -= 1
        handle.seek(position + (1 if position > 0 else 0))
        raw = handle.read().decode("utf-8").strip()
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return {}
    return value if isinstance(value, Mapping) else {}


def _response_from_trace(trace: Mapping[str, Any], messages: Sequence[Mapping[str, Any]]) -> ModelResponse:
    text = str(trace["response"])
    usage = trace.get("usage") if isinstance(trace.get("usage"), Mapping) else {}
    incoming = usage.get("prompt_tokens", usage.get("input_tokens"))
    outgoing = usage.get("completion_tokens", usage.get("output_tokens"))
    reported = type(incoming) is int and incoming >= 0 and type(outgoing) is int and outgoing >= 0
    return ModelResponse(
        text=text,
        input_tokens=incoming if reported else max(1, len(json.dumps(list(messages), ensure_ascii=False)) // 4),
        output_tokens=outgoing if reported else max(1, len(text) // 4),
        finish_reason=str(trace["finish_reason"]) if trace.get("finish_reason") is not None else None,
        usage_reported=reported,
    )


def recover_response(
    trace_path: Path, messages: Sequence[Mapping[str, Any]], *, temperature: float,
    max_tokens: int, top_p: float = 1.0, model: str = "glm-5.1",
    trace_start_offset: int = 0,
) -> ModelResponse | None:
    """Recover one durable reply to the actual sent parameters, without I/O to the provider.

    The trace omits transport-only stream=False but records every generation
    parameter used by this adapter. A partial final line is ignored. Multiple
    matching attempts or missing request identity stay unresolved: their costs
    cannot be reconstructed by selecting the last successful reply alone.
    New journal entries bind a byte offset recorded before dispatch, so an
    explicitly authorized retry cannot recover an earlier attempt's reply.
    """
    path = Path(trace_path)
    if not path.is_file():
        return None
    identity = {"model": model, "messages": [dict(message) for message in messages],
                "temperature": temperature, "top_p": top_p,
                "max_completion_tokens": max_tokens, "thinking": {"type": "disabled"}}
    matches = []
    with path.open("rb") as handle:
        if type(trace_start_offset) is not int or trace_start_offset < 0:
            return None
        handle.seek(0, 2)
        if trace_start_offset > handle.tell():
            return None
        if trace_start_offset:
            handle.seek(trace_start_offset - 1)
            if handle.read(1) != b"\n":
                return None
        handle.seek(trace_start_offset)
        for line in handle:
            try:
                row = json.loads(line)
            except (ValueError, UnicodeDecodeError):
                continue
            if isinstance(row, Mapping) and all(row.get(key) == value for key, value in identity.items()):
                matches.append(row)
    if len(matches) != 1:
        return None
    row = matches[0]
    if row.get("attempt") != 1 or row.get("error") or not isinstance(row.get("response"), str) or not row["response"].strip():
        return None
    return _response_from_trace(row, messages)


class GLM51Client:
    """One-task GLM-5.1 client with the same transport as all RQ2 methods."""

    def __init__(self, artifact_dir: Path) -> None:
        self._common = _load_common()
        self._client = self._common.GLMClient(artifact_dir)
        self.trace_path = artifact_dir / "llm-trace.jsonl"

    def chat(
        self,
        messages: Sequence[Mapping[str, Any]],
        *,
        temperature: float,
        max_tokens: int,
        top_p: float = 1.0,
    ) -> ModelResponse:
        text = self._client.chat(
            [dict(message) for message in messages],
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            retries=1,
        )
        trace = _last_jsonl(self.trace_path)
        return _response_from_trace({**trace, "response": text}, messages)

    def recover_response(
        self, messages: Sequence[Mapping[str, Any]], *, temperature: float,
        max_tokens: int, top_p: float = 1.0, trace_start_offset: int = 0,
    ) -> ModelResponse | None:
        return recover_response(self.trace_path, messages, temperature=temperature,
                                max_tokens=max_tokens, top_p=top_p, model=self._client.model,
                                trace_start_offset=trace_start_offset)



__all__ = ["GLM51Client", "ModelResponse", "load_environment", "recover_response"]
