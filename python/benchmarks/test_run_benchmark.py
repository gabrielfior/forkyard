import csv
import io

import pytest

import run_benchmark
from run_benchmark import FIELDS, _check_binaries_on_path, parse_int_list, write_records
from agent import ActionRecord


def test_parse_int_list_splits_and_converts():
    assert parse_int_list("1,2,5,10") == [1, 2, 5, 10]
    assert parse_int_list("7") == [7]


def test_write_records_produces_one_csv_row_per_record():
    records = [
        ActionRecord("forkyard", 20_000_000, 2, 0, "transfer", 12.5, True),
        ActionRecord("forkyard", 20_000_000, 2, 0, "discard", 3.1, True),
    ]
    buf = io.StringIO()
    write_records(buf, records)
    rows = list(csv.DictReader(io.StringIO(buf.getvalue())))
    assert len(rows) == 2
    assert rows[0]["action"] == "transfer"
    assert rows[0]["ok"] == "True"
    assert rows[1]["backend"] == "forkyard"


def test_check_binaries_on_path_names_every_missing_binary(monkeypatch):
    monkeypatch.setattr(run_benchmark.shutil, "which", lambda name: None)
    with pytest.raises(RuntimeError) as excinfo:
        _check_binaries_on_path()
    message = str(excinfo.value)
    assert "forkyard" in message and "anvil" in message
    assert "cargo build -p forkyard --release" in message
    assert "Foundry" in message


def test_check_binaries_on_path_reports_only_the_one_that_is_missing(monkeypatch):
    monkeypatch.setattr(
        run_benchmark.shutil, "which", lambda name: None if name == "anvil" else "/usr/bin/forkyard"
    )
    with pytest.raises(RuntimeError, match="anvil"):
        _check_binaries_on_path()


def test_check_binaries_on_path_passes_when_both_are_present(monkeypatch):
    monkeypatch.setattr(run_benchmark.shutil, "which", lambda name: f"/usr/bin/{name}")
    _check_binaries_on_path()  # must not raise


def test_fields_and_row_stay_in_lockstep():
    """The incremental writer in main() drives a DictWriter with FIELDS
    directly, so a field added to one and not the other would raise only
    mid-sweep, after real work had already been done."""
    record = ActionRecord("forkyard", 20_000_000, 1, 0, "transfer", 1.0, True)
    assert list(run_benchmark._row(record).keys()) == FIELDS
