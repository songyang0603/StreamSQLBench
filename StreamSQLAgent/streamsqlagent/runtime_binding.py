"""Restore this frozen campaign's explicit runtime binding in child adapters."""
from __future__ import annotations

import json
import os
from pathlib import Path

from .runtime_cleanup import CleanupPending, EXCLUSIVE_ENV


def install_binding(path: Path) -> Path:
    """Read deployment authorization, never infer exclusivity from a descriptor."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or value.get("contract") != "streamsqlagent_new4_runtime_binding_v1":
            raise ValueError("unsupported runtime binding contract")
        raw = value.get("exclusive_runtime_project_root")
        if not isinstance(raw, str) or not raw:
            raise ValueError("missing exclusive runtime project root")
        project = Path(raw)
        if not project.is_absolute() or str(project.resolve(strict=True)) != raw or not project.is_dir():
            raise ValueError("runtime binding must name an existing canonical absolute directory")
        inherited = os.environ.get(EXCLUSIVE_ENV)
        if inherited is not None and inherited != raw:
            raise ValueError("inherited runtime binding disagrees with frozen deployment")
    except (OSError, ValueError) as exc:
        raise CleanupPending("invalid frozen runtime binding: " + str(exc)) from exc
    os.environ[EXCLUSIVE_ENV] = raw
    return project
