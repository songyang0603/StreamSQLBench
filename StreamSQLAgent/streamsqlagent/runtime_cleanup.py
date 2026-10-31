"""New2-only teardown: preserve failed outcomes while retiring a dirty TM JVM."""
from __future__ import annotations

import contextlib
import fcntl
import functools
import importlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.request

TERMINAL = {"FINISHED", "FAILED", "CANCELED", "SUSPENDED"}
EXCLUSIVE_ENV = "STREAMSQLAGENT_NEW4_EXCLUSIVE_RUNTIME"


class CleanupPending(RuntimeError):
    pass


def _write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n')
    temp.replace(path)


@contextlib.contextmanager
def _lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CleanupPending('runtime lock occupied: ' + str(path)) from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _run(command, project):
    result = subprocess.run(command, cwd=project, text=True, capture_output=True, timeout=30)
    if result.returncode:
        raise CleanupPending('runtime cleanup command failed: ' + result.stderr[-1500:])
    return result.stdout


def _get(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        return json.load(response)


def _container(runtime, project, compose):
    ids = _run(runtime._compose(compose, 'ps', '-q', 'taskmanager'), project).split()
    if len(ids) != 1:
        raise CleanupPending('expected exactly one bound taskmanager container')
    data = json.loads(_run(['docker', 'inspect', ids[0]], project))[0]
    labels = data['Config'].get('Labels') or {}
    if (labels.get('com.docker.compose.service') != 'taskmanager'
            or Path(labels.get('com.docker.compose.project.working_dir', '/')).resolve() != project
            or not labels.get('com.docker.compose.project')
            or (compose and labels['com.docker.compose.project'] != compose)):
        raise CleanupPending('taskmanager Docker ownership labels mismatch')
    return data


def _check_jobs(url, job_id):
    jobs = _get(url + '/jobs/overview').get('jobs')
    if not isinstance(jobs, list):
        raise CleanupPending('cannot establish active job ownership')
    active = [job for job in jobs if job.get('state') not in TERMINAL]
    if active:
        # Original ordered cleanup already cancels this job. An unresolved own
        # job also blocks JVM replacement; never restart through active work.
        raise CleanupPending('active jobs remain; refusing taskmanager restart')


def _recover(runtime, project, compose, url, job_id):
    if os.environ.get(EXCLUSIVE_ENV) != str(project):
        raise CleanupPending('exclusive runtime campaign binding is missing')
    if not re.fullmatch('[0-9a-f]{32}', str(job_id)):
        raise CleanupPending('invalid owned job ID')
    before = _container(runtime, project, compose)
    # Bind the REST endpoint to this Compose project via the JobManager port.
    jm_ids = _run(runtime._compose(compose, 'ps', '-q', 'jobmanager'), project).split()
    if len(jm_ids) != 1:
        raise CleanupPending('expected one bound jobmanager')
    jm = json.loads(_run(['docker', 'inspect', jm_ids[0]], project))[0]
    labels = jm['Config'].get('Labels') or {}
    if (labels.get('com.docker.compose.project') != before['Config']['Labels']['com.docker.compose.project']
            or labels.get('com.docker.compose.service') != 'jobmanager'):
        raise CleanupPending('jobmanager Docker ownership labels mismatch')
    from urllib.parse import urlparse
    parsed = urlparse(url)
    ports = jm.get('NetworkSettings', {}).get('Ports', {}).get('8081/tcp') or []
    if parsed.hostname not in {'127.0.0.1', 'localhost'} or not any(str(p.get('HostPort')) == str(parsed.port or 80) for p in ports):
        raise CleanupPending('JobManager endpoint is not bound to owned Compose runtime')
    _check_jobs(url, job_id)
    command = runtime._compose(compose, 'restart', 'taskmanager')
    _run(command, project)
    deadline = time.monotonic() + 45
    last = 'taskmanager recovery deadline'
    while time.monotonic() < deadline:
        try:
            after = _container(runtime, project, compose)
            if (after['Id'] != before['Id'] or after['Config'] != before['Config']
                    or after['HostConfig'] != before['HostConfig']):
                raise CleanupPending('taskmanager configuration/container changed during restart')
            state = after['State']
            overview = _get(url + '/overview')
            _check_jobs(url, job_id)
            if (state.get('Running') and state.get('StartedAt') != before['State'].get('StartedAt')
                    and state.get('Health', {}).get('Status', 'healthy') == 'healthy'
                    and overview.get('taskmanagers') == 1
                    and overview.get('slots-total', 0) > 0
                    and overview.get('slots-available') == overview.get('slots-total')):
                return {'command': command, 'container_id': after['Id'],
                        'before_started_at': before['State']['StartedAt'],
                        'after_started_at': state['StartedAt'], 'health': overview}
        except Exception as exc:
            last = str(exc)
        time.sleep(.5)
    raise CleanupPending(last)


def execute_with_cleanup(original_execute, spec, sql, output_dir, *, exclusive_project=None, **kwargs):
    """One real execution; a failed candidate is never replayed by teardown."""
    if getattr(original_execute, '_new4_cleanup_wrapped', False):
        return original_execute(spec, sql, output_dir, **kwargs)
    runtime = importlib.import_module('streamsqlbench_curation.ordered_streaming')
    project = Path(kwargs.get('runtime_project_root') or spec['runtime_project_root']).resolve()
    if Path(spec['runtime_project_root']).resolve() != project:
        raise CleanupPending('descriptor/runtime project binding mismatch')
    if exclusive_project is not None and project != exclusive_project:
        raise CleanupPending('descriptor disagrees with frozen exclusive runtime binding')
    compose = kwargs.get('compose_project_name', spec.get('compose_project_name'))
    url = str(kwargs.get('jobmanager_url') or spec.get('jobmanager_url', 'http://127.0.0.1:8081')).rstrip('/')
    pending = project / 'artifacts' / 'new4-cleanup-pending.json'
    output_dir = Path(output_dir)
    with _lock(project / 'artifacts' / '.new4-execution.lock'):
        if pending.exists():
            raise CleanupPending('prior runtime cleanup pending: ' + str(pending))
        kwargs['cleanup_topics'] = False
        _write(pending, {'cleanup_pending': True, 'output_dir': str(output_dir.resolve()),
                         'reason': 'execution in progress; unexpected exit requires recovery'})
        result = original_execute(spec, sql, output_dir, **kwargs)
        report = {'original_ok': result.get('ok'), 'job_id': result.get('job_id'),
                  'topics': result.get('topics', []), 'cleanup_pending': True,
                  'candidate_retries': 0}
        # Persist the recovery obligation before any fallible teardown action.
        _write(pending, dict(report, output_dir=str(output_dir.resolve())))
        try:
            with _lock(runtime.runtime_lock_path(project, kwargs.get('lock_name', runtime.DEFAULT_LOCK_NAME))):
                if not result.get('job_id') and result.get('stage') == 'submission':
                    raise CleanupPending('submission outcome unknown; retain owned topics')
                if result.get('job_id') and not result.get('ok'):
                    report['recovery'] = _recover(runtime, project, compose, url, result['job_id'])
                topics = result.get('topics', [])
                if topics:
                    if (not isinstance(topics, list) or not all(isinstance(t, str) and re.fullmatch(r'ssb\.ordered\.[0-9a-f]{24}\.[A-Za-z0-9_]+', t) for t in topics)
                            or len({t.split('.')[2] for t in topics}) != 1):
                        raise CleanupPending('invalid invocation topic namespace')
                    result['topic_cleanup'] = runtime._worker(project, {
                        'mode': 'delete', 'topics': topics, 'job_terminal': True,
                        'bootstrap_servers': kwargs.get('kafka_bootstrap_servers', spec.get('kafka_bootstrap_servers', 'kafka:29092'))},
                        compose_project_name=compose, timeout=30)
                report['cleanup_pending'] = False
                pending.unlink()
        except Exception as exc:
            report['error'] = str(exc)
            result['topic_cleanup_error'] = str(exc)
            _write(pending, dict(report, output_dir=str(output_dir.resolve())))
        result['cleanup_pending'] = report['cleanup_pending']
        _write(output_dir / 'new4-runtime-cleanup.json', report)
        _write(output_dir / 'result.json', result)
        return result


def install(runtime, *, exclusive_project=None):
    if getattr(runtime.execute, '_new4_cleanup_wrapped', False):
        return
    original = runtime.execute
    @functools.wraps(original)
    def wrapped(spec, sql, output_dir, **kwargs):
        return execute_with_cleanup(original, spec, sql, output_dir,
                                    exclusive_project=exclusive_project, **kwargs)
    wrapped._new4_cleanup_wrapped = True
    runtime.execute = wrapped
