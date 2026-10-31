#!/usr/bin/env python3
"""Read-only check of dependencies and an optional running reference cluster."""
from pathlib import Path
import argparse
import importlib.metadata
import json
import shutil
import subprocess
import sys
import urllib.request
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'runtime_support'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-project', type=Path, default=ROOT / 'runtime')
    parser.add_argument('--live', action='store_true', help='Also query local Flink health; never starts containers')
    parser.add_argument('--jobmanager-url', default='http://127.0.0.1:8081')
    parser.add_argument('--gateway-url', default='http://127.0.0.1:8083')
    args = parser.parse_args()
    report = {'python': sys.version.split()[0], 'dependencies': {}, 'problems': [], 'models_called': 0}
    for name in ('sqlglot', 'pandas'):
        try:
            report['dependencies'][name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            report['problems'].append('missing Python dependency: ' + name)
    try:
        from streamsqlagent.agent import StreamSQLAgent
        from streamsqlagent.job_assembler import _load_rq2_runtime_adapter
        from streamsqlagent.model_client import _load_common
        from streamsqlagent.public_contract import _ddl_parser
        _load_rq2_runtime_adapter(); _load_common(); _ddl_parser()
    except Exception as exc:
        report['problems'].append('source dependency check: ' + str(exc))
    if not shutil.which('javac'):
        report['problems'].append('Java compiler not found; install JDK 17')
    if not shutil.which('docker'):
        report['problems'].append('Docker CLI not found')
    else:
        checked = subprocess.run(['docker', 'compose', 'config', '--quiet'], cwd=args.runtime_project,
                                 capture_output=True, text=True, timeout=30)
        if checked.returncode:
            report['problems'].append('Compose configuration: ' + checked.stderr[-2000:])
    if args.live:
        for key, url in [('flink', args.jobmanager_url.rstrip('/') + '/overview'),
                         ('gateway', args.gateway_url.rstrip('/') + '/v1/info')]:
            try:
                with urllib.request.urlopen(url, timeout=5) as response:
                    report[key] = json.load(response)
                if key == 'flink' and (report[key].get('jobs-running') != 0 or
                        report[key].get('taskmanagers', 0) < 1 or report[key].get('slots-total', 0) < 1):
                    report['problems'].append('Flink must have registered workers, slots, and no active jobs')
            except Exception as exc:
                report['problems'].append(key + ': ' + str(exc))
    report['passed'] = not report['problems']
    print(json.dumps(report, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
