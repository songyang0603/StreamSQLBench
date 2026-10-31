#!/usr/bin/env python3
"""Finish a recorded, owned cleanup obligation without replaying SQL or LLM calls."""
from pathlib import Path
import argparse
import json
import re
import sys
from urllib.parse import urlparse
from datetime import datetime, timezone
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'runtime_support'))
from streamsqlagent.release_io import read, write
from streamsqlagent.runtime_binding import install_binding
from streamsqlagent import runtime_cleanup as cleanup
from streamsqlbench_curation import ordered_streaming as runtime


def restore_transport_receipts(invocation: Path, result: dict) -> list[str]:
    """Rebuild only paused transport receipts from the saved original run."""
    from streamsqlbench_rq2 import ordered_adapter
    from streamsqlbench_rq2.runtime_adapter import _formal_payload, _feedback_payload

    namespace = dict(ordered_adapter.__dict__)
    source = ROOT / 'streamsqlagent/v13_failure_classification.py'
    exec(compile(source.read_text(), str(source), 'exec'), namespace)
    ordered_adapter.infrastructure_failure = namespace['infrastructure_failure']
    restored = []
    for path, contract, builder in (
        (invocation.parent / 'backend-result.json', 'streamsqlbench_rq2_docker_backend_result_v1', _formal_payload),
        (invocation.parent.parent / 'feedback-result.json', 'streamsqlbench_rq2_method_feedback_result_v1', _feedback_payload),
    ):
        if not path.is_file():
            continue
        prior = read(path)
        if prior.get('contract') != contract or prior.get('reason_code') != 'runtime_cleanup_pending':
            continue
        if Path(prior.get('raw_artifact', '/')).resolve() != (invocation / 'result.json').resolve():
            raise ValueError('paused transport receipt points to a different invocation')
        execution = ordered_adapter.execution_from_result(result, invocation)
        descriptor = {'task_id': result['task_id'], 'backend_kind': 'ordered_final'}
        payload = builder(descriptor, execution)
        backup = path.with_name(path.name + '.before-release-recovery')
        if not backup.exists():
            backup.write_bytes(path.read_bytes())
        write(path, payload)
        restored.append(str(path))
    return restored


def recover(output: Path) -> dict:
    output = output.resolve(strict=True)
    project = install_binding(ROOT / 'config/runtime-binding.json')
    marker_path = project / 'artifacts/new4-cleanup-pending.json'
    with cleanup._lock(project / 'artifacts/.new4-execution.lock'):
        if not marker_path.exists():
            return {'status': 'no_pending_marker', 'sql_replayed': False, 'models_called': 0}
        marker = read(marker_path)
        invocation = Path(marker['output_dir']).resolve(strict=True)
        if not invocation.is_relative_to(output):
            raise ValueError('pending invocation is outside the specified output; refusing recovery')
        result_path = invocation / 'result.json'
        result = read(result_path)
        binding = result.get('runtime_resource_binding') or {}
        if Path(binding.get('runtime_project_root', '/')).resolve() != project:
            raise ValueError('runtime resource ownership differs from deployment binding')
        job = result.get('job_id')
        if not isinstance(job, str) or not re.fullmatch(r'[0-9a-f]{32}', job):
            raise ValueError('no confirmed job identity; inspect the submission before cleanup')
        topics = result.get('topics')
        if (not isinstance(topics, list) or not topics or
            any(not isinstance(t, str) or not re.fullmatch(r'ssb\.ordered\.[0-9a-f]{24}\.[A-Za-z0-9_]+', t) for t in topics) or
            len({t.split('.')[2] for t in topics}) != 1):
            raise ValueError('invalid recorded invocation topic namespace')
        if marker.get('job_id') not in (None, job) or marker.get('topics') not in (None, topics):
            raise ValueError('pending marker and original result disagree')
        compose = binding.get('compose_project_name')
        url = str(binding.get('jobmanager_url', '')).rstrip('/')
        if binding.get('kafka_bootstrap_servers', 'kafka:29092') != 'kafka:29092':
            raise ValueError('recovery supports only the bound kafka:29092 service')
        if result.get('runtime_lock_name', runtime.DEFAULT_LOCK_NAME) != runtime.DEFAULT_LOCK_NAME:
            raise ValueError('unexpected runtime lock name; refusing recovery')
        with cleanup._lock(runtime.runtime_lock_path(project, result.get('runtime_lock_name', runtime.DEFAULT_LOCK_NAME))):
            container = cleanup._container(runtime, project, compose)
            managers = cleanup._run(runtime._compose(compose, 'ps', '-q', 'jobmanager'), project).split()
            if len(managers) != 1:
                raise ValueError('expected one bound JobManager')
            manager = json.loads(cleanup._run(['docker', 'inspect', managers[0]], project))[0]
            labels = manager['Config'].get('Labels') or {}
            if (labels.get('com.docker.compose.project') != container['Config']['Labels']['com.docker.compose.project']
                    or labels.get('com.docker.compose.service') != 'jobmanager'):
                raise ValueError('JobManager ownership differs from the bound Compose deployment')
            address = urlparse(url)
            ports = manager.get('NetworkSettings', {}).get('Ports', {}).get('8081/tcp') or []
            if address.hostname not in {'127.0.0.1', 'localhost'} or not any(
                    str(port.get('HostPort')) == str(address.port or 80) for port in ports):
                raise ValueError('JobManager endpoint does not belong to the bound Compose deployment')
            brokers = cleanup._run(runtime._compose(compose, 'ps', '-q', 'kafka'), project).split()
            if len(brokers) != 1:
                raise ValueError('expected one bound Kafka service')
            broker = json.loads(cleanup._run(['docker', 'inspect', brokers[0]], project))[0]
            broker_labels = broker['Config'].get('Labels') or {}
            if (broker_labels.get('com.docker.compose.project') != labels.get('com.docker.compose.project')
                    or broker_labels.get('com.docker.compose.service') != 'kafka'
                    or Path(broker_labels.get('com.docker.compose.project.working_dir', '/')).resolve() != project):
                raise ValueError('Kafka ownership differs from the bound Compose deployment')
            cleanup._check_jobs(url, job)
            report = {'original_ok': result.get('ok'), 'job_id': job, 'topics': topics,
                      'cleanup_pending': True, 'candidate_retries': 0,
                      'recovered_at': datetime.now(timezone.utc).isoformat()}
            if not result.get('ok'):
                report['recovery'] = cleanup._recover(runtime, project, compose, url, job)
            deletion = runtime._worker(project, {'mode': 'delete', 'topics': topics,
                                      'job_terminal': True, 'bootstrap_servers': binding.get('kafka_bootstrap_servers', 'kafka:29092')},
                                      compose_project_name=compose, timeout=30)
            # Preserve the original records before changing only cleanup fields.
            for source in (result_path, invocation / 'new4-runtime-cleanup.json'):
                backup = source.with_name(source.name + '.before-release-recovery')
                if source.is_file() and not backup.exists():
                    backup.write_bytes(source.read_bytes())
            report['cleanup_pending'] = False
            result['cleanup_pending'] = False
            result['topic_cleanup'] = deletion
            result.pop('topic_cleanup_error', None)
            write(invocation / 'new4-runtime-cleanup.json', report)
            write(result_path, result)
            report['restored_transport_receipts'] = restore_transport_receipts(invocation, result)
            write(invocation / 'release-cleanup-recovery.json', report)
            marker_path.unlink()
            return {'status': 'cleanup_completed', 'original_sql_ok': result.get('ok'),
                    'job_id': job, 'sql_replayed': False, 'models_called': 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(recover(args.output), indent=2))


if __name__ == '__main__':
    main()
