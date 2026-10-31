"""Offline tests of fetch_sources.py with small synthetic packages (no network, no real data)."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

PROVENANCE = Path(__file__).resolve().parents[1] / "provenance"
sys.path.insert(0, str(PROVENANCE))
import fetch_sources


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_database(path: Path) -> bytes:
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE events (id INTEGER, name TEXT, at TEXT, score REAL)")
        connection.executemany("INSERT INTO events VALUES (?, ?, ?, ?)",
                               [(1, "a b", "2020-01-01 00:00:00.0", 1.5), (2, None, "2020-01-02", 2.0),
                                (3, 'q"x', None, None)])
    return path.read_bytes()


def python_csv(rows, columns):
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\r\n")
    writer.writerow(columns)
    writer.writerows(["" if v is None else v for v in r] for r in rows)
    return buffer.getvalue().encode()


@pytest.fixture
def release(tmp_path):
    database = make_database(tmp_path / "shop.sqlite")
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as archive:
        archive.writestr("train_databases/shop/shop.sqlite", database)
    packages = tmp_path / "packages"
    packages.mkdir()
    with zipfile.ZipFile(packages / "train.zip", "w") as archive:
        archive.writestr("train/train_databases.zip", inner.getvalue())
    rows = [(1, "a b", "2020-01-01 00:00:00.0", 1.5), (2, None, "2020-01-02", 2.0), (3, 'q"x', None, None)]
    columns = ["id", "name", "at", "score"]
    py = python_csv(rows, columns)
    shell = ('id,name,at,score\r\n1,"a b","2020-01-01 00:00:00.0",1.5\r\n2,,2020-01-02,2.0\r\n'
             '3,"q""x",,\r\n').encode()
    root = tmp_path / "root"
    (root / "data").mkdir(parents=True)
    catalog = {"contract": "streamsqlbench_upstream_sources_v1",
               "packages": [{"id": "bird_train", "benchmark": "BIRD", "file": "train.zip", "url": "https://invalid.example/train.zip",
                             "page": "https://invalid.example", "license": "test",
                             "bytes": (packages / "train.zip").stat().st_size,
                             "sha256": sha((packages / "train.zip").read_bytes())}],
               "databases": [{"path": "sources/bird/databases/shop/shop.sqlite", "package": "bird_train",
                              "member": "shop.sqlite", "bytes": len(database), "sha256": sha(database), "keep": True}],
               "csv_exports": [{"path": "sources/bird/tables/shop/events/data.csv", "bytes": len(py), "sha256": sha(py),
                                "database": "sources/bird/databases/shop/shop.sqlite", "sqlite_table": "events",
                                "dialect": "python_csv"},
                               {"path": "sources/bird/tables/shop/events_shell/data.csv", "bytes": len(shell),
                                "sha256": sha(shell), "database": "sources/bird/databases/shop/shop.sqlite",
                                "sqlite_table": "events", "dialect": "sqlite_shell_csv"}]}
    (root / "data/upstream_sources.json").write_text(json.dumps(catalog))
    return root, packages, catalog


def run(root, packages, *extra):
    return subprocess.run([sys.executable, str(PROVENANCE / "fetch_sources.py"), "--data-root", str(root),
                           "--package-dir", str(packages), *extra], capture_output=True, text=True)


def test_rebuilds_nested_database_and_both_csv_dialects(release):
    root, packages, catalog = release
    process = run(root, packages)
    assert process.returncode == 0, process.stderr
    assert json.loads(process.stdout.splitlines()[-1])["status"] == "passed"
    for entry in catalog["databases"] + catalog["csv_exports"]:
        assert sha((root / entry["path"]).read_bytes()) == entry["sha256"]
    # Idempotent: a second run rebuilds nothing.
    again = json.loads(run(root, packages).stdout.splitlines()[-1])
    assert again["databases"] == again["csv_exports"] == 0


def test_wrong_package_is_rejected(release):
    root, packages, catalog = release
    with zipfile.ZipFile(packages / "train.zip", "a") as archive:
        archive.writestr("extra.txt", "changed")
    process = run(root, packages)
    assert process.returncode != 0
    assert "not the expected release" in process.stderr + process.stdout
    assert not (root / "sources").exists()


def test_changed_export_is_rejected(release):
    root, packages, catalog = release
    catalog["csv_exports"][0]["sha256"] = "0" * 64
    (root / "data/upstream_sources.json").write_text(json.dumps(catalog))
    process = run(root, packages)
    assert process.returncode != 0
    assert "differs from the frozen benchmark source" in process.stderr + process.stdout
    assert not (root / catalog["csv_exports"][0]["path"]).exists()


def test_plan_downloads_nothing(release):
    root, packages, _ = release
    process = run(root, packages, "--plan")
    assert process.returncode == 0
    plan = json.loads(process.stdout)
    assert plan["databases"] == 1 and plan["csv_exports"] == 2
    assert not (root / "sources").exists()


@pytest.mark.parametrize("value", ["../x", "/abs", "data/x", "sources/../../x"])
def test_unsafe_paths_rejected(tmp_path, value):
    with pytest.raises(ValueError):
        fetch_sources.under(tmp_path, value)
