"""Classify explicit infrastructure failures without relabeling bad SQL."""

from __future__ import annotations

import re
from typing import Any, Mapping


def explicit_infrastructure_message(message: str) -> bool:
    text = message.casefold()
    # The shared transport also raises ModelTransportError for empty content.
    # Empty/truncated/model-invalid responses remain genuine model failures.
    if "glm transport failed after" in text:
        if "response content is empty" in text:
            return False
        return bool(re.search(
            r"http error (?:401|403|408|429|5\d\d)|timed? out|"
            r"connection (?:reset|refused|aborted)|remote end closed|"
            r"urlopen error|name or service not known|"
            r"nodename nor servname|temporary failure in name resolution|"
            r"certificate_verify_failed|network is unreachable", text,
        ))
    return any(marker in text for marker in (
        "runtimeinfrastructureerror:", "no space left on device",
        "cannot connect to the docker daemon", "is the docker daemon running",
        "error during connect:", "docker daemon is not running",
        "docker desktop is not running", "disk quota exceeded",
        "brokenpipeerror:", "socket broken pipe", "error while fetching server api version",
        "runtime tool emitted no feedback result", "runtime feedback contract mismatch",
        "missing or invalid structured result", "unsupported structured result contract",
        "modulenotfounderror: no module named", "filenotfounderror:",
    ))


def is_infrastructure_failure(value: Mapping[str, Any]) -> bool:
    return (
        value.get("failure_class") in {"infra", "evaluator"}
        or value.get("reason_code") in {"infrastructure", "model_transport"}
        or explicit_infrastructure_message(str(value.get("message") or ""))
    )
