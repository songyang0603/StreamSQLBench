"""No clinical rows: real no-row controls plus synthetic conversion fixtures."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

REPOSITORY = Path(__file__).resolve().parents[2]
REPRODUCTION = REPOSITORY / "reproduction"
sys.path.insert(0, str(REPRODUCTION))
sys.path.insert(0, str(REPOSITORY / "data-curation/provenance"))
import prepare_evaluation
import derive_inputs as prepare_sources

METADATA = REPRODUCTION / "fixed402/manifest.json"
DATA = REPOSITORY / "benchmark/data"


@pytest.fixture
def data_root(tmp_path):
    """A data root with the real no-row metadata and no source files."""
    (tmp_path / "data").mkdir()
    for name in ("source_storage.jsonl", "source_storage_files.jsonl", "upstream_sources.json"):
        (tmp_path / "data" / name).write_bytes((DATA / name).read_bytes())
    return tmp_path


def test_plan_covers_402_547_with_an_exact_route_for_every_binding(data_root):
    _, rows, report = prepare_sources.build_plan(REPOSITORY, METADATA, data_root)
    assert report["registered_task_count"] == report["selected_task_count"] == 402
    assert report["registered_source_binding_count"] == len(rows) == 547
    assert report["expected_prepared_bytes"] == 19_857_269_848
    assert report["unsupported_bindings"] == []
    routes = {route: sum(row["route"] == route for row in rows) for route in {row["route"] for row in rows}}
    assert routes == {"exact_copy": 288, "native_projection": 219, "native_projection_from_original_sqlite": 40}
    assert sum(bool(row["key_order"]) for row in rows) == 2
    assert len(report["missing_source_files"]) == 142
    assert report["source_content_verified"] is False


def test_missing_sources_do_not_create_partial_output(data_root):
    target = data_root / "output"
    with pytest.raises(ValueError, match="fetch_sources.py|download all selected source files"):
        prepare_sources.prepare(REPOSITORY, METADATA, data_root, target)
    assert not target.exists()


def test_wrong_source_payload_cannot_leave_prepared_output(data_root):
    _, rows, _ = prepare_sources.build_plan(REPOSITORY, METADATA, data_root)
    row = min((r for r in rows if r["route"] == "exact_copy"), key=lambda r: r["source_size_bytes"])
    tid = row["requirement"]["task_id"]
    _, rows, _ = prepare_sources.build_plan(REPOSITORY, METADATA, data_root, task_ids=[tid])
    for r in rows:
        r["input_path"].parent.mkdir(parents=True, exist_ok=True)
        # Synthetic bytes of the right size that disagree with the frozen file.
        r["input_path"].write_bytes(b"x" * r["source_size_bytes"])
    target = data_root / "prepared"
    with pytest.raises(ValueError):
        prepare_sources.prepare(REPOSITORY, METADATA, data_root, target, task_ids=[tid])
    assert not target.exists()
    assert not list(data_root.glob(".prepared-*"))


def test_registered_key_order_is_used_for_projection(tmp_path):
    database = tmp_path / "db.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE t (a INTEGER, b TEXT, c TEXT)")
        connection.execute("INSERT INTO t VALUES (1, 'x', NULL)")
    payload = (json.dumps({"a": 1, "c": None, "b": "x"}) + "\n").encode()
    row = {"binding_id": "task/t", "representation": "sqlite", "sqlite_table": "t", "row_count": 1,
           "source": {"columns": [{"name": "a"}, {"name": "b"}, {"name": "c"}]}, "key_order": ["a", "c", "b"],
           "requirement": {"size_bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}}
    target = tmp_path / "out.jsonl"
    prepare_sources.write_projection(database, target, row)
    assert target.read_bytes() == payload
    row["key_order"] = ["a", "b"]
    with pytest.raises(ValueError, match="key order"):
        prepare_sources.write_projection(database, tmp_path / "bad.jsonl", row)


@pytest.mark.parametrize("value", ["", ".", "../source", "/source", "nested/../../source", "C:/file", "a\\b", "a//b"])
def test_release_relative_paths_fail_closed(value):
    with pytest.raises(ValueError):
        prepare_sources.relative_path(value)


def test_existing_symlink_cannot_escape_source_root(tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    root.mkdir(); outside.mkdir()
    (root / "link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="escapes"):
        prepare_sources.under(root, "link/payload.jsonl")


def test_native_sqlite_projection_keeps_null_empty_types_order_and_time(tmp_path):
    source = tmp_path / "original.sqlite"
    connection = sqlite3.connect(source)
    connection.execute('CREATE TABLE "original table" (i INTEGER, text TEXT, empty TEXT, ts TEXT, real REAL, unused TEXT)')
    connection.executemany('INSERT INTO "original table" VALUES (?, ?, ?, ?, ?, ?)', [
        (8, None, "", "2030-01-01 01:02:03", 1.25, "omit"),
        (2, "汉字", "", "2020-01-01 00:00:00", 0.0, "omit"),
    ])
    connection.commit(); connection.close()
    before = prepare_sources.digest(source)
    names = ["ts", "i", "empty", "text", "real"]
    rows = list(prepare_sources.projected_rows(source, "sqlite", names, "original table"))
    assert rows == [dict(zip(names, ["2030-01-01 01:02:03", 8, "", None, 1.25])),
                    dict(zip(names, ["2020-01-01 00:00:00", 2, "", "汉字", 0.0]))]
    assert list(rows[0]) == names
    assert prepare_sources.digest(source) == before


def test_jsonl_projection_preserves_values_and_removes_only_unregistered_columns(tmp_path):
    source = tmp_path / "original.jsonl"
    source.write_text('{"ignored": true, "a": 4, "b": null, "c": ""}\n')
    assert list(prepare_sources.projected_rows(source, "jsonl", ["c", "b", "a"])) == [{"c": "", "b": None, "a": 4}]


def test_complete_schema_json_uses_registered_full_relation_fields(tmp_path):
    source = tmp_path / "source.json"
    source.write_text(json.dumps({"column_names": ["i", "text"], "sample_rows": [{"i": 3, "text": ""}, {"i": 4, "text": None}]}))
    assert list(prepare_sources.projected_rows(source, "official_schema_json_complete_rows", ["text", "i"])) == [
        {"text": "", "i": 3}, {"text": None, "i": 4}]


def test_csv_casts_are_never_inferred(tmp_path):
    source = tmp_path / "original.csv"
    source.write_text("i,text\n1,\n")
    with pytest.raises(ValueError, match="lossy CSV"):
        list(prepare_sources.projected_rows(source, "csv", ["i", "text"]))


def test_source_identity_checks_complete_file_and_native_sample(tmp_path):
    path = tmp_path / "prepared.jsonl"
    path.write_text('{"i": 1, "text": null}\n')
    row = [{"i": 1, "text": None}]
    requirement = {"binding_id": "example/source", "format": "jsonl", "size_bytes": path.stat().st_size,
                   "sha256": prepare_sources.digest(path), "sample_count": 1,
                   "sample_sha256": hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()}
    prepare_sources.verify(path, requirement)
    changed = dict(requirement, sample_sha256="0" * 64)
    with pytest.raises(ValueError, match="native sample differs"):
        prepare_sources.verify(path, changed)
    path.write_text('{"i": 1, "text": ""}\n')
    with pytest.raises(ValueError, match="prepared bytes differ"):
        prepare_sources.verify(path, requirement)


@pytest.mark.parametrize("separators,encoding", [(None, "spaced"), ((",", ":"), "compact")])
def test_projection_encoding_selected_only_by_frozen_sha(tmp_path, separators, encoding):
    incoming, outgoing = tmp_path / "input.jsonl", tmp_path / "output.jsonl"
    native = [{"text": "", "i": 3, "null": None}, {"text": "汉字", "i": 0, "null": None}]
    incoming.write_text("".join(json.dumps(dict(item, ignored=True), ensure_ascii=False) + "\n" for item in native))
    expected = "".join(json.dumps(item, ensure_ascii=False, allow_nan=False, separators=separators) + "\n" for item in native).encode()
    row = {"binding_id": "synthetic/source", "row_count": len(native), "representation": "jsonl", "sqlite_table": None,
           "source": {"columns": [{"name": name} for name in native[0]]},
           "requirement": {"size_bytes": len(expected), "sha256": hashlib.sha256(expected).hexdigest()}}
    profile = prepare_sources.write_projection(incoming, outgoing, row)
    assert outgoing.read_bytes() == expected
    assert profile["json_encoding"] == encoding
    assert not list(tmp_path.glob("output.jsonl.*"))


def test_registered_rowid_order_can_recover_original_builder_bytes(tmp_path):
    incoming, outgoing = tmp_path / "input.sqlite", tmp_path / "output.jsonl"
    connection = sqlite3.connect(incoming)
    connection.execute("CREATE TABLE original (k TEXT PRIMARY KEY, unused TEXT)")
    connection.executemany("INSERT INTO original VALUES (?, ?)", [("b", "other"), ("a", "other")])
    connection.commit(); connection.close()
    expected = b'{"k":"b"}\n{"k":"a"}\n'
    row = {"binding_id": "synthetic/source", "row_count": 2, "representation": "sqlite", "sqlite_table": "original",
           "source": {"columns": [{"name": "k"}]},
           "requirement": {"size_bytes": len(expected), "sha256": hashlib.sha256(expected).hexdigest()}}
    profile = prepare_sources.write_projection(incoming, outgoing, row)
    assert outgoing.read_bytes() == expected
    assert profile == {"json_encoding": "compact", "sqlite_order": "rowid"}


def test_frozen_gold_parser_preserves_null_empty_and_literal_backslash(tmp_path):
    # Synthetic relation only, no task Gold is read for this check.
    evaluator = prepare_evaluation.frozen_evaluator(REPOSITORY)
    csv = tmp_path / "gold.csv"
    rows = [[None, "", r"\N", r"\\N", 1]]
    evaluator.old.write_final_csv(csv, ["null", "empty", "literal", "slashes", "integer"], rows)
    relation = evaluator.read_final_csv(csv)
    assert relation["rows"] == [[None, "", r"\N", r"\\N", "1"]]
    assert relation["null_encoding"] == evaluator.old.CSV_NULL_ENCODING


@pytest.mark.parametrize("task_ids", [["unknown"], ["bird_00008", "bird_00008"]])
def test_invalid_subset_rejected_before_any_source_read(data_root, task_ids):
    with pytest.raises(ValueError, match="unknown or duplicate"):
        prepare_sources.build_plan(REPOSITORY, METADATA, data_root, task_ids=task_ids)


def test_prepared_override_is_explicit_and_no_unknown_binding_accepted(data_root):
    mapping = data_root / "override.json"
    mapping.write_text(json.dumps({"unknown/binding": "payload.jsonl"}))
    with pytest.raises(ValueError, match="unknown binding"):
        prepare_sources.build_plan(REPOSITORY, METADATA, data_root, prepared_map=mapping)
