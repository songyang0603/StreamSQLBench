"""Offline tests of the Coordinator loop with scripted roles and tools."""
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any

import pytest

from streamsqlagent.agent import AgentConfig, StreamSQLAgent
from streamsqlagent.budget import CampaignBudgetPaused, CampaignTokenBudget
from streamsqlagent.data_explorer import QueryResult
from streamsqlagent.model_client import ModelResponse


@dataclass
class Plan:
    status: str
    observation: str
    columns: tuple = ()


@dataclass
class Run:
    status: str
    observation: str
    columns: tuple = ()
    column_types: tuple = ()
    rows: tuple = ()

    @property
    def completed_streaming_run(self):
        return self.status == 'success'

    def to_agent_feedback(self):
        return {'resource_signal': 'within_budget' if self.status == 'success' else 'not_observed'}


class FakePlanner:
    def __init__(self, outcomes):
        self.outcomes, self.calls = outcomes, []

    def explain(self, sql):
        self.calls.append(sql)
        for marker, outcome in self.outcomes.items():
            if marker in sql:
                return outcome
        return Plan('success', 'ok', ({'name': 'shipped_orders', 'type': 'BIGINT'},))


class FakeRuntime:
    physical_executions = 0

    def __init__(self, outcomes=None, crash_on=None):
        self.outcomes, self.calls, self.crash_on = outcomes or {}, [], crash_on

    def execute(self, sql, timeout_seconds=None):
        self.timeouts = getattr(self, 'timeouts', []) + [timeout_seconds]
        if self.crash_on and self.crash_on in sql:
            raise KeyboardInterrupt('simulated worker stop')
        self.calls.append(sql)
        self.physical_executions += 1
        for marker, outcome in self.outcomes.items():
            if marker in sql:
                return outcome
        return Run('success', 'ok', ('shipped_orders',), ('BIGINT',), ((2,),))


class FakeMirror:
    def __init__(self):
        self.queries = []

    def profile(self):
        return {'orders': {'rows': 3, 'columns': [
            {'name': 'status', 'type': 'VARCHAR', 'nulls': 0, 'distinct': 2, 'distinct_exact': True,
             'values': [['SHIPPED', 2], ['Pending', 1]]}]}}

    def query(self, sql):
        self.queries.append(sql)
        return QueryResult('ok', ('status', 'n'), (('SHIPPED', 2),), 1)

    def close(self, remove=True):
        pass


class ScriptedModel:
    """Reply by role/phase; record every prompt."""

    def __init__(self, script, fail_first=0):
        self.script, self.prompts, self.fail_first = script, [], fail_first

    def chat(self, messages, *, temperature, max_tokens):
        if self.fail_first:
            self.fail_first -= 1
            raise OSError('connection reset')
        system, user = messages[0]['content'], messages[1]['content']
        role = ('explorer' if 'Data Explorer' in system else 'verifier' if 'Verifier' in system else 'author')
        self.prompts.append((role, user, temperature))
        reply = self.script(role, user, len([p for p in self.prompts if p[0] == role]))
        return ModelResponse(reply, input_tokens=len(user) // 4, output_tokens=len(reply) // 4,
                             finish_reason='stop', usage_reported=True)


ACCEPT = json.dumps({'verdict': 'accept', 'issues': [], 'probes': []})
EXPLORE = json.dumps({'queries': [{'purpose': 'status codes', 'sql': 'SELECT status, COUNT(*) AS n FROM orders GROUP BY 1'}]})


def make_agent(task_dir, model, planner=None, runtime=None, mirror=None, config=None, **kwargs):
    public = json.loads((task_dir / 'public-input.json').read_text())
    return StreamSQLAgent(public_input=public, task_package=task_dir / 'task-package.json',
                          runtime_tool=task_dir / 'unused.json', artifact_dir=task_dir / 'method',
                          config=config or AgentConfig(), model=model, plan_tool=planner or FakePlanner({}),
                          runtime_bridge=runtime or FakeRuntime(), data_mirror=mirror or FakeMirror(),
                          sleep=lambda s: None, **kwargs)


def test_simple_task_generate_execute_accept(task_dir):
    model = ScriptedModel(lambda role, prompt, n: {
        'explorer': EXPLORE, 'author': "SELECT COUNT(*) AS shipped_orders FROM orders WHERE status = 'SHIPPED'",
        'verifier': ACCEPT}[role])
    result = make_agent(task_dir, model).run()
    assert result.status == 'success' and result.publicly_verified
    assert result.termination_reason == 'verifier_accepted' and result.model_calls == 3
    assert (task_dir / 'method/candidate.sql').read_text().strip().endswith("'SHIPPED'")
    author_prompt = next(p for r, p, _ in model.prompts if r == 'author')
    # data facts, profile, engine facts and guide reach the Author; labels never do
    assert "SHIPPED" in author_prompt and 'NO column is a Flink time attribute' not in author_prompt
    assert 'events.event_time' in author_prompt and 'Flink 1.20 streaming SQL facts' in author_prompt
    for _, prompt, _ in model.prompts:
        assert 'D1' not in prompt and 'difficulty' not in prompt.lower() and 'Commerce' not in prompt


def test_engine_error_repair_receives_verified_recipe(task_dir):
    planner = FakePlanner({'DENSE_RANK': Plan('compile_error', 'org.apache.flink.table.api.TableException: '
                                                                'DENSE_RANK() on streaming table is not supported currently')})

    def script(role, prompt, n):
        if role == 'explorer':
            return EXPLORE
        if role == 'verifier':
            return ACCEPT
        return ('SELECT DENSE_RANK() OVER (ORDER BY 1) AS shipped_orders FROM orders' if n == 1
                else 'SELECT COUNT(*) AS shipped_orders FROM orders')
    model = ScriptedModel(script)
    result = make_agent(task_dir, model, planner=planner).run()
    assert result.candidate_id == 'c2' and result.termination_reason == 'verifier_accepted'
    repair_prompt = [p for r, p, _ in model.prompts if r == 'author'][1]
    assert 'ranking_function' in repair_prompt and 'rn <= K' in repair_prompt


def test_repeated_engine_rule_triggers_fresh_rewrite(task_dir):
    planner = FakePlanner({'LAG(': Plan('compile_error', "OVER windows' ordering in stream mode must be defined on a time attribute.")})

    def script(role, prompt, n):
        if role == 'explorer':
            return EXPLORE
        if role == 'verifier':
            return ACCEPT
        return (f'SELECT LAG(order_id) OVER (ORDER BY created) AS shipped_orders, {n} AS v FROM orders' if n <= 2
                else 'SELECT COUNT(*) AS shipped_orders FROM orders')
    model = ScriptedModel(script)
    result = make_agent(task_dir, model, planner=planner).run()
    assert result.candidate_id == 'c3'
    third = [p for p in model.prompts if p[0] == 'author'][2]
    assert 'fresh formulation' in third[1] and third[2] == AgentConfig().rewrite_temperature


def test_verifier_findings_drive_semantic_repair_and_rereview(task_dir):
    revise = json.dumps({'verdict': 'revise', 'issues': [{'severity': 'high', 'check': 'A', 'problem': 'wrong code',
                                                          'evidence': 'profile shows SHIPPED', 'fix': "use 'SHIPPED'"}]})

    def script(role, prompt, n):
        if role == 'explorer':
            return EXPLORE
        if role == 'verifier':
            return revise if n == 1 else ACCEPT
        return ("SELECT COUNT(*) AS shipped_orders FROM orders WHERE status = 'shipped'" if n == 1
                else "SELECT COUNT(*) AS shipped_orders FROM orders WHERE status = 'SHIPPED'")
    model = ScriptedModel(script)
    result = make_agent(task_dir, model).run()
    assert result.candidate_id == 'c2' and result.publicly_verified
    second_review = [p for r, p, _ in model.prompts if r == 'verifier'][1]
    assert 'EARLIER FINDINGS THAT THIS REVISION SHOULD FIX' in second_review and 'wrong code' in second_review
    assert "PREVIOUS CANDIDATE'S RESULT" in second_review


def test_low_severity_or_unparsable_review_does_not_rewrite(task_dir):
    low = json.dumps({'verdict': 'revise', 'issues': [{'severity': 'low', 'problem': 'maybe style'}]})
    for reply in (low, 'I think it is fine.'):
        model = ScriptedModel(lambda role, prompt, n, reply=reply: {
            'explorer': EXPLORE, 'author': 'SELECT COUNT(*) AS shipped_orders FROM orders', 'verifier': reply}[role])
        agent_dir = task_dir / 'method'
        if agent_dir.exists():
            import shutil
            shutil.rmtree(agent_dir)
        result = make_agent(task_dir, model).run()
        assert result.status == 'success' and result.candidate_id == 'c1'
        assert len([p for p in model.prompts if p[0] == 'author']) == 1


def test_verifier_probe_round_runs_data_queries(task_dir):
    probe = json.dumps({'verdict': 'revise', 'issues': [], 'probes': [
        {'purpose': 'key unique?', 'sql': 'SELECT order_id, COUNT(*) FROM events GROUP BY 1 HAVING COUNT(*) > 1'}]})
    mirror = FakeMirror()
    model = ScriptedModel(lambda role, prompt, n: {
        'explorer': EXPLORE, 'author': 'SELECT COUNT(*) AS shipped_orders FROM orders',
        'verifier': probe if n == 1 else ACCEPT}[role])
    result = make_agent(task_dir, model, mirror=mirror).run()
    assert result.termination_reason == 'verifier_accepted'
    assert any('HAVING COUNT(*) > 1' in q for q in mirror.queries)
    assert 'key unique?' in [p for r, p, _ in model.prompts if r == 'verifier'][1]


def test_budget_exhaustion_selects_latest_executed_revision(task_dir):
    revise = json.dumps({'verdict': 'revise', 'issues': [{'severity': 'high', 'problem': 'p', 'evidence': 'e'}]})
    model = ScriptedModel(lambda role, prompt, n: {
        'explorer': EXPLORE, 'author': f'SELECT {n} AS shipped_orders FROM orders', 'verifier': revise}[role])
    result = make_agent(task_dir, model, config=AgentConfig(max_model_calls=5)).run()
    # explore, gen, review(c1), repair -> c2 executed, review(c2), then budget stops before another repair
    assert result.termination_reason.startswith('budget_exhausted') or result.termination_reason == 'review_limit'
    assert result.status == 'success'
    selection = json.loads((task_dir / 'method/selection.json').read_text())
    assert selection['candidate_id'] in {'c2', 'c3'}


def test_cycle_is_detected_and_stops(task_dir):
    planner = FakePlanner({'BAD': Plan('compile_error', 'Sort on a non-time-attribute field is not supported.')})
    model = ScriptedModel(lambda role, prompt, n: {
        'explorer': EXPLORE, 'author': 'SELECT order_id AS shipped_orders FROM orders ORDER BY BAD',
        'verifier': ACCEPT}[role])
    result = make_agent(task_dir, model, planner=planner).run()
    assert result.termination_reason == 'repeated_candidate'
    assert result.status == 'failure' and result.candidate_id is None


def test_contract_and_empty_result_feedback(task_dir):
    planner = FakePlanner({'AS wrong_name': Plan('success', 'ok', ({'name': 'wrong_name', 'type': 'BIGINT'},))})
    runtime = FakeRuntime({"'shipped'": Run('success', 'ok', ('shipped_orders',), ('BIGINT',), ())})
    revise_empty = json.dumps({'verdict': 'revise', 'issues': [{'severity': 'high', 'problem': 'empty', 'evidence': 'EMPTY_RESULT'}]})

    def script(role, prompt, n):
        if role == 'explorer':
            return EXPLORE
        if role == 'verifier':
            current = prompt.split('EXECUTION ON THE COMPLETE INPUT:')[1].split('EARLIER FINDINGS')[0]
            return revise_empty if '"row_count": 0' in current else ACCEPT
        return {1: 'SELECT COUNT(*) AS wrong_name FROM orders',
                2: "SELECT COUNT(*) AS shipped_orders FROM orders WHERE status = 'shipped'",
                3: "SELECT COUNT(*) AS shipped_orders FROM orders WHERE status = 'SHIPPED'"}[n]
    model = ScriptedModel(script)
    result = make_agent(task_dir, model, planner=planner, runtime=runtime).run()
    assert result.candidate_id == 'c3' and result.publicly_verified
    prompts = [p for r, p, _ in model.prompts if r == 'author']
    assert 'required_columns_in_order' in prompts[1]


def test_transport_failure_is_retried_with_conservative_hold(task_dir):
    model = ScriptedModel(lambda role, prompt, n: {
        'explorer': EXPLORE, 'author': 'SELECT COUNT(*) AS shipped_orders FROM orders', 'verifier': ACCEPT}[role],
        fail_first=1)
    ledger = task_dir / 'campaign-budget.json'
    result = make_agent(task_dir, model, campaign_budget_path=ledger, campaign_token_limit=1_000_000).run()
    assert result.status == 'success'
    state = CampaignTokenBudget(ledger, 1_000_000).to_dict()
    statuses = sorted(r['status'] for r in state['reservations'].values())
    assert statuses.count('unknown') == 1 and statuses.count('completed') == 3


def test_resume_replays_journal_without_new_model_calls(task_dir):
    revise = json.dumps({'verdict': 'revise', 'issues': [{'severity': 'high', 'problem': 'p', 'evidence': 'e'}]})

    def script(role, prompt, n):
        if role == 'explorer':
            return EXPLORE
        if role == 'verifier':
            return revise if 'SELECT 1 AS' in prompt else ACCEPT
        return 'SELECT 1 AS shipped_orders FROM orders' if 'CURRENT SQL' not in prompt else 'SELECT 2 AS shipped_orders FROM orders'
    first = ScriptedModel(script)
    with pytest.raises(KeyboardInterrupt):
        make_agent(task_dir, first, runtime=FakeRuntime(crash_on='SELECT 2')).run()
    paid = len(first.prompts)
    second = ScriptedModel(script)
    result = make_agent(task_dir, second).run()
    assert result.candidate_id == 'c2' and result.publicly_verified
    # explore, generate, review(c1), repair were replayed; only review(c2) is new
    assert paid == 4 and len(second.prompts) == 1 and second.prompts[0][0] == 'verifier'
    assert result.model_calls == 5


def test_mirror_unavailable_degrades_gracefully(task_dir):
    class Broken(FakeMirror):
        def profile(self):
            raise MemoryError('no memory')
    model = ScriptedModel(lambda role, prompt, n: {
        'explorer': EXPLORE, 'author': 'SELECT COUNT(*) AS shipped_orders FROM orders', 'verifier': ACCEPT}[role])
    agent = make_agent(task_dir, model, mirror=Broken())
    result = agent.run()
    assert result.status == 'success'
    assert '(data profile unavailable)' in [p for r, p, _ in model.prompts if r == 'author'][0]


def test_complete_input_run_uses_the_runtime_tool_call_budget(task_dir):
    """The task's 900 s job limit belongs to the runtime adapter, not to the whole call."""
    runtime = FakeRuntime()
    model = ScriptedModel(lambda role, prompt, n: {
        'explorer': EXPLORE, 'author': 'SELECT COUNT(*) AS shipped_orders FROM orders', 'verifier': ACCEPT}[role])
    make_agent(task_dir, model, runtime=runtime).run()
    assert runtime.timeouts == [None]
