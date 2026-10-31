"""Offline tests of the deterministic tool roles (no model, Flink or Docker)."""
import json
from pathlib import Path

import pytest

from streamsqlagent.data_explorer import DataMirror, render_profile, validate_exploration_query
from streamsqlagent.engine_knowledge import diagnose, locate_error, rule_ids
from streamsqlagent.engine_profile import engine_facts, parse_source_ddl, registered_tables
from streamsqlagent.observer import result_observation

duckdb = pytest.importorskip("duckdb")


def test_ddl_parser_reads_types_computed_columns_and_time_attribute(task_dir):
    package = json.loads((task_dir / 'task-package.json').read_text())
    tables = registered_tables(package)
    assert [c.name for c in tables['orders'].columns] == ['order_id', 'status', 'amount', 'note', 'created']
    assert tables['orders'].rowtime is None
    events = tables['events']
    assert events.rowtime == 'event_time'
    assert [c.name for c in events.physical_columns] == ['event_id', 'order_id', 'kind', 'event_ts']
    assert events.columns[-1].computed.startswith('TO_TIMESTAMP')
    facts = engine_facts(tables)
    assert facts['time_attributes'] == {'events': 'event_time'}
    assert 'events.event_time' in facts['summary']
    none = engine_facts(parse_source_ddl("CREATE TABLE `t` (`a` BIGINT, `b` DECIMAL(10, 2)) WITH ('x'='y')"))
    assert none['time_attributes'] == {} and 'NO column is a Flink time attribute' in none['summary']


@pytest.fixture
def mirror(task_dir, tmp_path):
    package = json.loads((task_dir / 'task-package.json').read_text())
    m = DataMirror(package, registered_tables(package), tmp_path / 'mirror', base_dir=task_dir)
    m.build()
    yield m
    m.close()


def test_mirror_reproduces_publisher_conversion(mirror):
    rows = mirror.query("SELECT order_id, status, amount, note FROM orders ORDER BY order_id").rows
    # empty numeric cell -> NULL; preserve policy keeps the empty string; quoted comma kept.
    assert rows == ((1, 'SHIPPED', 10.5, ''), (2, 'Pending', None, 'a, quoted note'), (3, 'SHIPPED', 7.25, 'x'))
    typed = mirror.query("SELECT typeof(order_id), typeof(amount) FROM orders LIMIT 1").rows[0]
    assert typed == ('BIGINT', 'DOUBLE')
    assert mirror.receipt['row_counts'] == {'orders': 3, 'events': 3}


def test_mirror_is_read_only_and_cannot_reach_other_files(mirror, tmp_path):
    secret = tmp_path / 'gold.csv'
    secret.write_text('answer\n42\n')
    for sql in [f"SELECT * FROM read_csv('{secret}')", f"SELECT * FROM '{secret}'",
                "CREATE TABLE x AS SELECT 1", f"COPY orders TO '{tmp_path / 'x.csv'}'",
                "SELECT 1; SELECT 2", "SELECT * FROM glob('/')"]:
        result = mirror.query(sql)
        assert result.status in {'rejected', 'error'}, sql
        assert '42' not in json.dumps(result.to_dict())


def test_exploration_query_validation():
    assert validate_exploration_query('  with a as (select 1) select * from a; ') .lower().startswith('with')
    for bad in ['DROP TABLE t', 'SELECT 1; SELECT 2', "SELECT * FROM read_json_auto('x')", '']:
        with pytest.raises(ValueError):
            validate_exploration_query(bad)
    # identifiers that merely contain a keyword are fine
    assert validate_exploration_query('SELECT last_update, "update" FROM t')


def test_profile_reports_nulls_value_sets_and_formats(mirror, task_dir):
    package = json.loads((task_dir / 'task-package.json').read_text())
    profile = mirror.profile()
    status = next(c for c in profile['orders']['columns'] if c['name'] == 'status')
    assert status['values'] == [['SHIPPED', 2], ['Pending', 1]]
    amount = next(c for c in profile['orders']['columns'] if c['name'] == 'amount')
    assert amount['nulls'] == 1 and amount['distinct'] == 2
    created = next(c for c in profile['orders']['columns'] if c['name'] == 'created')
    assert created['text_formats'][0]['shape'] == '9999-99-99 99:99:99.9'
    text = render_profile(profile, registered_tables(package), {'orders': 'created'})
    assert '[stream order field]' in text and 'time attribute: event_time' in text
    assert 'NULL=1' in text


REAL_MESSAGES = {
    'ranking_function': ['org.apache.flink.table.api.TableException: DENSE_RANK() on streaming table is not supported currently',
                         'The window can only be ordered in ASCENDING mode.'],
    'over_needs_time_attribute': ["OVER windows' ordering in stream mode must be defined on a time attribute.",
                                  'The window can only be ordered by a single time column.'],
    'window_needs_time_attribute': ['The window function requires the timecol is a time attribute type, but is TIMESTAMP(3).',
                                    'Window aggregate can only be defined over a time attribute column, but TIMESTAMP(3) encountered.'],
    'over_on_updating_input': ["StreamPhysicalOverAggregate doesn't support consuming update and delete changes which is produced by node GroupAggregate"],
    'timestamp_arithmetic': ["Cannot apply '-' to arguments of type '<TIMESTAMP(3)> - <TIMESTAMP(3)>'."],
    'interval_precision': ['Interval field value 120 exceeds precision of MINUTE(2) field'],
    'multi_column_count_distinct': ['We now only support the count of one field.'],
    'sort_without_limit': ['Sort on a non-time-attribute field is not supported.'],
    'correlated_subquery': ['unexpected correlate variable $cor0 in the plan'],
    'timestamp_parse': ['java.time.format.DateTimeParseException: Text 2013-06-14T00:00:00.000Z could not be parsed'],
}


@pytest.mark.parametrize('rule,messages', REAL_MESSAGES.items())
def test_engine_rules_match_observed_messages(rule, messages):
    for message in messages:
        assert rule_ids(message)[0] == rule
        assert diagnose(message)[0].recipe


def test_unknown_message_has_no_rule_and_location_is_rendered():
    assert rule_ids('some unrelated failure') == ()
    assert locate_error('SELECT a\nFROM t\nWHERE x', 'Encountered "x" at line 3, column 7').splitlines()[0] == 'line 3: WHERE x'


def test_result_observation_signals():
    empty = result_observation(['n'], ['BIGINT'], [], question='How many?')
    assert empty['row_count'] == 0 and empty['signals'][0].startswith('EMPTY_RESULT')
    dup = result_observation(['name'], ['STRING'], [['a'], ['a'], ['b']], question='List names')
    assert any(s.startswith('DUPLICATE_ROWS: 1 of 3') for s in dup['signals'])
    avg = result_observation(['avg_quantity'], ['BIGINT'], [[3]], question='average')
    assert any(s.startswith('INTEGER_RESULT') for s in avg['signals'])
    pct = result_observation(['inactive_percentage'], ['DOUBLE'], [[0.0245]], question='percentage')
    assert any(s.startswith('PERCENT_SCALE') for s in pct['signals'])
    fine = result_observation(['inactive_percentage'], ['DOUBLE'], [[2.45]], question='percentage')
    assert fine['signals'] == [] and fine['preview_rows'] == [[2.45]]


def test_sql_lints_explain_parser_failures_the_message_does_not_name():
    from streamsqlagent.engine_knowledge import recipe_packet, sql_lints
    sql = "SELECT FLOOR(t TO INTERVAL '30' MINUTE) AS b, x::INT FROM s WHERE t > t2 + INTERVAL 5 MINUTE"
    hints = sql_lints(sql)
    assert len(hints) == 3 and 'unit keyword' in hints[0]
    packet = recipe_packet("SqlParseException: Incorrect syntax near the keyword 'INTERVAL' at line 1, column 23", sql)
    assert packet['recognized_constructs'] == hints and packet['diagnosis'][0]['rule'] == 'parse_error'
    assert sql_lints('SELECT FLOOR(t TO HOUR) FROM s') == []


def test_timestamp_padding_signal_and_bang_equal_lint():
    from streamsqlagent.engine_knowledge import rule_ids, sql_lints
    padded = result_observation(['bucket_start'], ['STRING'], [['2016-05-01 00:10:00.000'], ['2016-05-01 00:20:00.000']], question='')
    assert any(s.startswith('TIMESTAMP_TEXT_PADDING') for s in padded['signals'])
    plain = result_observation(['bucket_start'], ['STRING'], [['2016-05-01 00:10:00']], question='')
    assert not any(s.startswith('TIMESTAMP_TEXT_PADDING') for s in plain['signals'])
    assert rule_ids("SqlParseException: Bang equal '!=' is not allowed under the current SQL conformance level") == ('parse_error',)
    assert any("'<>'" in h for h in sql_lints('SELECT 1 FROM t WHERE a != b'))


def test_over_rule_explains_row_number_sequencing():
    rule = diagnose('The window can only be ordered by a single time column.')[0]
    assert rule.rule_id == 'over_needs_time_attribute' and 'rn + 1' in rule.diagnosis
    assert 'exactly one declared time attribute' in rule.diagnosis
