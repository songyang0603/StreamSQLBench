"""Resolve bundled runtime support without searching the author's workspace."""
from pathlib import Path


def vldb_root() -> Path:
    """Compatibility name for the self-contained runtime support directory."""
    root = Path(__file__).resolve().parents[1] / 'runtime_support'
    if not (root / 'adapters/common.py').is_file():
        raise FileNotFoundError('runtime_support is missing; use the complete repository checkout')
    return root
