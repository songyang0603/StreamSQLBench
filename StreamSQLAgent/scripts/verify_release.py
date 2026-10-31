#!/usr/bin/env python3
"""Verify the published StreamSQLAgent source inventory without models or SQL."""
from pathlib import Path
import argparse
import hashlib
import json

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_PARTS = {'__pycache__', '.pytest_cache', '.venv', 'build', 'dist',
                  '.git', '.runtime-data', 'artifacts', 'migration', 'vendor',
                  'data', 'outputs', 'operations'}
EXCLUDED_FILES = {'FROZEN-MANIFEST.json', '.env', 'runtime/.env',
                  'config/runtime-binding.json', '.DS_Store'}


def source_files(root: Path) -> dict[str, Path]:
    return {path.relative_to(root).as_posix(): path for path in root.rglob('*')
            if path.is_file()
            and not EXCLUDED_PARTS.intersection(path.relative_to(root).parts)
            and not any(part.endswith('.egg-info') for part in path.relative_to(root).parts)
            and path.relative_to(root).as_posix() not in EXCLUDED_FILES
            and path.name != '.DS_Store' and path.suffix not in {'.pyc', '.pyo', '.class'}}


def verify(root: Path = ROOT) -> dict:
    manifest = json.loads((root / 'FROZEN-MANIFEST.json').read_text(encoding='utf-8'))
    if manifest.get('contract') != 'streamsqlagent_source_release_v1':
        raise ValueError('unsupported release manifest')
    expected = {row['path']: row for row in manifest['files']}
    if len(expected) != len(manifest['files']):
        raise ValueError('duplicate release paths')
    actual = source_files(root)
    problems = []
    for name in sorted(expected.keys() - actual.keys()):
        problems.append({'path': name, 'reason': 'missing'})
    for name in sorted(actual.keys() - expected.keys()):
        problems.append({'path': name, 'reason': 'unexpected'})
    for name in sorted(actual.keys() & expected.keys()):
        data = actual[name].read_bytes()
        if len(data) != expected[name]['size_bytes'] or hashlib.sha256(data).hexdigest() != expected[name]['sha256']:
            problems.append({'path': name, 'reason': 'changed'})
    return {'passed': not problems, 'version': manifest['version'],
            'source_files': len(expected), 'problems': problems,
            'models_called': 0, 'sql_executed': 0}


if __name__ == '__main__':
    argparse.ArgumentParser(description=__doc__).parse_args()
    try:
        result = verify()
    except (OSError, ValueError, KeyError) as exc:
        result = {'passed': False, 'error': str(exc)}
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result['passed'] else 1)
