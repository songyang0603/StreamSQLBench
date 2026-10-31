#!/usr/bin/env python3
"""Regenerate the deterministic StreamSQLAgent source manifest."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from verify_release import ROOT, source_files


def build(root: Path = ROOT) -> dict:
    files = []
    for name, path in sorted(source_files(root).items()):
        data = path.read_bytes()
        files.append({
            "path": name,
            "size_bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        })
    return {
        "contract": "streamsqlagent_source_release_v1",
        "method_name": "StreamSQLAgent",
        "version": "3.0.0",
        "frozen_at_utc": datetime.now(timezone.utc).isoformat(),
        "method_components": [
            "three LLM roles: Data Explorer, SQL Author, Streaming Semantic Verifier",
            "three program roles: Coordinator, Execution Worker, Feedback Observer",
            "read-only complete-input data mirror reproducing the publisher's value conversion",
            "real Flink planning and complete-input streaming runs",
            "engine-limitation diagnoses with rewrite recipes verified on the benchmark runtime",
            "evidence-based semantic review with bounded data probes",
            "persistent candidates, evidence-based selection and resumable model journal",
        ],
        "local_state_excluded": [
            ".DS_Store", ".env", ".git", ".pytest_cache", ".runtime-data",
            ".venv", "FROZEN-MANIFEST.json", "__pycache__", "artifacts",
            "build", "config/runtime-binding.json", "data", "dist",
            "migration", "operations", "outputs", "runtime/.env", "vendor",
            "*.class", "*.pyc", "*.pyo",
        ],
        "files": files,
    }


if __name__ == "__main__":
    destination = ROOT / "FROZEN-MANIFEST.json"
    destination.write_text(
        json.dumps(build(), indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(destination)
