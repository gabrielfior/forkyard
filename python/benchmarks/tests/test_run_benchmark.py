import csv
import io
import json

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


def test_write_records_carries_the_error_column_for_failed_actions():
    records = [
        ActionRecord("anvil", 20_000_000, 1, 0, "transfer", 4.0, False, "RuntimeError('tx reverted')"),
        ActionRecord("anvil", 20_000_000, 1, 0, "get_balance", 1.0, True, ""),
    ]
    buf = io.StringIO()
    write_records(buf, records)
    reader = csv.DictReader(io.StringIO(buf.getvalue()))
    rows = list(reader)
    assert reader.fieldnames == FIELDS
    assert FIELDS[-1] == "error", "error stays last so consumers of the first 7 columns still work"
    assert rows[0]["error"] == "RuntimeError('tx reverted')"
    assert rows[1]["error"] == ""


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
    """main()'s incremental writer drives a DictWriter with FIELDS directly,
    so a mismatch would only raise mid-sweep."""
    record = ActionRecord("forkyard", 20_000_000, 1, 0, "transfer", 1.0, True)
    assert list(run_benchmark._row(record).keys()) == FIELDS


def test_upstream_row_reports_calls_per_agent_and_the_busiest_methods():
    """`calls_per_agent` stays flat for a shared cache and climbs for a
    per-agent one."""
    from rpc_proxy import ProxyStats
    from run_benchmark import UPSTREAM_FIELDS, upstream_row

    stats = ProxyStats(
        http_requests=40,
        jsonrpc_calls=100,
        by_method={"eth_getStorageAt": 60, "eth_getCode": 30, "eth_getBalance": 10},
        upstream_errors=2,
    )
    row = upstream_row("anvil", 20_000_000, 10, 3, stats)

    assert list(row.keys()) == UPSTREAM_FIELDS, "row and header must stay in lockstep"
    assert row["calls_per_agent"] == 10.0
    assert row["episodes"] == 3
    assert row["upstream_errors"] == 2
    assert json.loads(row["top_methods"])["eth_getStorageAt"] == 60


def test_upstream_row_keeps_only_the_five_busiest_methods():
    from rpc_proxy import ProxyStats
    from run_benchmark import upstream_row

    stats = ProxyStats(jsonrpc_calls=28, by_method={f"m{i}": i for i in range(1, 8)})
    top = json.loads(upstream_row("forkyard", 1, 1, 1, stats)["top_methods"])

    assert list(top) == ["m7", "m6", "m5", "m4", "m3"]


def test_backend_factories_survive_pickling_for_client_processes():
    import pickle
    from run_benchmark import AnvilFactory, ForkyardFactory

    forkyard = pickle.loads(pickle.dumps(ForkyardFactory("http://127.0.0.1:1")))
    assert forkyard(3).keywords == {"base_url": "http://127.0.0.1:1"}

    anvil = pickle.loads(pickle.dumps(AnvilFactory(19000, 2, "http://up", 5, True)))
    assert anvil == AnvilFactory(19000, 2, "http://up", 5, True)


def test_one_client_process_runs_every_agent_in_threads(monkeypatch):
    seen: list[int] = []

    def fake_run_agent(make_backend, backend_name, rng, agent_id, *args, **kwargs):
        seen.append(agent_id)
        return [ActionRecord(backend_name, 1, 3, agent_id, "acquire", 1.0, True, "")]

    monkeypatch.setattr(run_benchmark, "run_agent", fake_run_agent)
    records, total_ms = run_benchmark._run_agents(lambda i: None, "forkyard", 3, 1, 5, 1)
    assert sorted(seen) == [0, 1, 2]
    assert [r.agent_id for r in records] == [0, 1, 2]
    assert total_ms >= 0
