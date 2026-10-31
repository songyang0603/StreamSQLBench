from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

from .contracts import FailureClass
from .runtime_lifecycle import run_supervised


@dataclass(frozen=True)
class ProcessResult:
    returncode: int | None
    stdout_path: Path
    stderr_path: Path
    timed_out: bool


def render_command(command: Sequence[str], bindings: Mapping[str, Path | str]) -> list[str]:
    rendered: list[str] = []
    string_bindings = {key: str(value) for key, value in bindings.items()}
    for part in command:
        try:
            rendered.append(part.format_map(string_bindings))
        except KeyError as exc:
            raise ValueError(f"unknown command placeholder {exc.args[0]!r}") from exc
    return rendered


def run_process(
    command: Sequence[str], *, cwd: Path, timeout_seconds: int,
    artifact_dir: Path, environment: Mapping[str, str] | None = None,
) -> ProcessResult:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = artifact_dir / "stdout.log"
    stderr_path = artifact_dir / "stderr.log"
    try:
        completed = run_supervised(
            list(command),
            cwd=cwd,
            env=dict(environment or {}),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            cleanup_artifact=artifact_dir / "process-timeout-cleanup.json",
        )
        stdout_path.write_text(_redact(completed.stdout, environment), encoding="utf-8")
        stderr_path.write_text(_redact(completed.stderr, environment), encoding="utf-8")
        return ProcessResult(completed.returncode, stdout_path, stderr_path, False)
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = exc.stderr.decode() if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        stdout_path.write_text(_redact(stdout, environment), encoding="utf-8")
        stderr_path.write_text(_redact(stderr, environment), encoding="utf-8")
        return ProcessResult(None, stdout_path, stderr_path, True)


def sanitized_environment(allowlist: Sequence[str]) -> dict[str, str]:
    """Pass only explicitly allowed variables and never serialize their values."""

    base = {"PATH": os.environ.get("PATH", ""), "PYTHONUNBUFFERED": "1"}
    for name in allowlist:
        if name in os.environ:
            base[name] = os.environ[name]
    return base


def _redact(text: str, environment: Mapping[str, str] | None) -> str:
    redacted = text
    for name, value in (environment or {}).items():
        if value and any(token in name.upper() for token in ("KEY", "TOKEN", "SECRET", "PASSWORD")):
            redacted = redacted.replace(value, "[REDACTED]")
    return redacted


def load_result(path: Path, expected_contract: str) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"missing or invalid structured result {path}: {exc}") from exc
    if not isinstance(value, dict) or value.get("contract") != expected_contract:
        raise ValueError(f"unsupported structured result contract: {path}")
    return value


def parse_failure_class(value: object, default: FailureClass) -> FailureClass:
    try:
        return FailureClass(str(value))
    except ValueError:
        return default
