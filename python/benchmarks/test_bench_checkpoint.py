import csv
import io
import sys

import pytest

import bench_checkpoint
from bench_checkpoint import (
    FIELDS,
    Sample,
    _measure,
    _row,
    blob_size_bytes,
    measure_anvil,
    measure_forkyard,
    slot_hex,
    touch_slots,
    value_hex,
    write_samples,
)


class FakeManager:
    """Records every JSON-RPC method the measurement drives, and answers
    the two that return something the script depends on."""

    def __init__(self, dump_blob="0x" + "ab" * 64, fail_on=()):
        self.calls: list[tuple[str, list]] = []
        self._dump_blob = dump_blob
        self._fail_on = set(fail_on)

    def request_blocking(self, method, params):
        self.calls.append((method, params))
        if method in self._fail_on:
            raise RuntimeError(f"{method} exploded")
        if method == "evm_snapshot":
            return "0x1"
        if method == "anvil_dumpState":
            return self._dump_blob
        return True


class FakeWeb3:
    def __init__(self, manager):
        self.manager = manager


class FakeAnvil:
    def __init__(self, manager):
        self._manager = manager
        self.stored: list[tuple[str, str, str]] = []
        self.discarded = False

    def web3(self):
        return FakeWeb3(self._manager)

    def set_storage(self, address, slot, value):
        self.stored.append((address, slot, value))

    def discard(self):
        self.discarded = True


def test_fields_and_row_stay_in_lockstep():
    """main() drives a DictWriter with FIELDS directly, so a column added
    to one and not the other only raises mid-run, after real forks."""
    sample = Sample("anvil", "dump", 100, 1.0, 4096, True, "")
    assert list(_row(sample).keys()) == FIELDS


def test_blob_size_bytes_measures_the_encoded_dump():
    assert blob_size_bytes("0x" + "ff" * 10) == 10
    assert blob_size_bytes("ff" * 10) == 10  # a build that omits the prefix
    assert blob_size_bytes(b"\x00" * 7) == 7
    assert blob_size_bytes(None) == 0


def test_blob_size_bytes_handles_a_json_object_dump():
    """Older Foundry builds returned a state object rather than a hex
    string; the column is about magnitude, so measure it either way."""
    assert blob_size_bytes({"accounts": {}}) == len(str({"accounts": {}}).encode())


def test_slot_and_value_words_are_full_32_byte_words():
    assert len(slot_hex(0)) == 66 and slot_hex(0).startswith("0x")
    assert len(value_hex(0)) == 66
    # A zero word is a storage delete on some backends, which would shrink
    # the state this script is trying to grow.
    assert int(value_hex(0), 16) != 0


def test_touch_slots_writes_one_distinct_slot_per_unit_of_state_size():
    written: list[tuple[str, str, str]] = []
    touch_slots(lambda a, s, v: written.append((a, s, v)), 5)
    assert len(written) == 5
    assert len({s for _, s, _ in written}) == 5, "duplicate slots would not grow the state"
    assert {a for a, _, _ in written} == {bench_checkpoint.DIRTY_CONTRACT}


def test_measure_records_the_blob_size_the_operation_moved():
    sample = _measure("anvil", "dump", 1000, lambda: 4096)
    assert (sample.backend, sample.operation, sample.state_size) == ("anvil", "dump", 1000)
    assert (sample.blob_bytes, sample.ok, sample.error) == (4096, True, "")
    assert sample.elapsed_ms >= 0


def test_measure_captures_the_failure_reason_and_reports_no_blob():
    def boom():
        raise RuntimeError("x" * 5_000)

    sample = _measure("anvil", "load", 100, boom)
    assert sample.ok is False
    assert sample.blob_bytes == 0
    assert len(sample.error) == bench_checkpoint._MAX_ERROR_CHARS


def test_measure_anvil_times_both_checkpoint_mechanisms_per_repeat(monkeypatch):
    manager = FakeManager(dump_blob="0x" + "cd" * 500)
    backend = FakeAnvil(manager)
    monkeypatch.setattr(bench_checkpoint, "AnvilBackend", lambda *a, **k: backend)

    samples = measure_anvil("http://rpc.example", 25_795_072, 3, 19200, repeats=2)

    assert [s.operation for s in samples] == [
        "snapshot", "revert", "dump", "load", "snapshot", "revert", "dump", "load",
    ]
    assert all(s.ok for s in samples), [s.error for s in samples]
    assert all(s.state_size == 3 for s in samples)
    assert len(backend.stored) == 3, "the state must be dirtied before any checkpoint is timed"
    assert backend.discarded, "the instance must not outlive the measurement"


def test_measure_anvil_reports_blob_bytes_only_where_a_blob_exists(monkeypatch):
    """evm_snapshot/evm_revert keep the state in memory; only dumpState
    materialises bytes, and that is the number the claim rests on."""
    manager = FakeManager(dump_blob="0x" + "cd" * 500)
    backend = FakeAnvil(manager)
    monkeypatch.setattr(bench_checkpoint, "AnvilBackend", lambda *a, **k: backend)

    samples = measure_anvil("http://rpc.example", 1, 2, 19200, repeats=1)

    by_op = {s.operation: s for s in samples}
    assert by_op["snapshot"].blob_bytes == 0
    assert by_op["revert"].blob_bytes == 0
    assert by_op["dump"].blob_bytes == 500
    assert by_op["load"].blob_bytes == 500, "load moves the same blob dump produced"


def test_measure_anvil_records_a_failed_snapshot_without_reverting_a_stale_id(monkeypatch):
    """Reverting to whatever id happened to be lying around would time a
    revert of some earlier snapshot and record it as a success."""
    manager = FakeManager(fail_on={"evm_snapshot"})
    backend = FakeAnvil(manager)
    monkeypatch.setattr(bench_checkpoint, "AnvilBackend", lambda *a, **k: backend)

    samples = measure_anvil("http://rpc.example", 1, 1, 19200, repeats=1)

    by_op = {s.operation: s for s in samples}
    assert by_op["snapshot"].ok is False
    assert by_op["revert"].ok is False
    assert "evm_snapshot" in by_op["revert"].error
    assert ("evm_revert", ["0x1"]) not in manager.calls


def test_measure_forkyard_times_a_branch_off_the_base_and_its_discard(monkeypatch):
    opened: list[str] = []
    discarded: list[str] = []

    class FakeForkyard:
        def __init__(self, session_url=None, *, base_url=None):
            self.session_url = session_url or f"{base_url}/session/base"
            self.stored: list[tuple[str, str, str]] = []

        def set_storage(self, address, slot, value):
            self.stored.append((address, slot, value))
            dirty_writes.append(slot)

        def discard(self):
            discarded.append(self.session_url)

    dirty_writes: list[str] = []

    def fake_open(base_url, timeout_s=30.0):
        opened.append(base_url)
        return f"{base_url}/session/{len(opened)}"

    monkeypatch.setattr(bench_checkpoint, "ForkyardBackend", FakeForkyard)
    monkeypatch.setattr(bench_checkpoint, "open_forkyard_session", fake_open)

    samples = measure_forkyard("http://127.0.0.1:18600", state_size=4, repeats=3)

    assert [s.operation for s in samples] == ["fork", "discard"] * 3
    assert all(s.ok for s in samples), [s.error for s in samples]
    # Every forkyard row is blob-free by construction: branching off the
    # shared base serializes nothing, which is the whole claim.
    assert all(s.blob_bytes == 0 for s in samples)
    assert len(dirty_writes) == 4, "the dirty session must be written before forking"
    assert len(opened) == 3, "one fresh session per repeat"
    assert len(discarded) == 4, "three branched sessions plus the dirty base"


def test_measure_forkyard_does_not_discard_a_session_it_failed_to_open(monkeypatch):
    class FakeForkyard:
        def __init__(self, session_url=None, *, base_url=None):
            self.session_url = session_url
            self.discards = discards

        def set_storage(self, address, slot, value):
            pass

        def discard(self):
            discards.append(self.session_url)

    discards: list[str] = []

    def fake_open(base_url, timeout_s=30.0):
        raise RuntimeError("session pool exhausted")

    monkeypatch.setattr(bench_checkpoint, "ForkyardBackend", FakeForkyard)
    monkeypatch.setattr(bench_checkpoint, "open_forkyard_session", fake_open)

    samples = measure_forkyard("http://127.0.0.1:18600", state_size=1, repeats=1)

    by_op = {s.operation: s for s in samples}
    assert by_op["fork"].ok is False
    assert by_op["discard"].ok is False
    assert discards == [None], "only the dirty base session was ever opened"


def test_write_samples_round_trips_through_csv():
    buf = io.StringIO()
    write_samples(buf, [
        Sample("forkyard", "fork", 10000, 0.9, 0, True, ""),
        Sample("anvil", "load", 10000, 812.0, 5_000_000, False, "RuntimeError('nope')"),
    ])
    reader = csv.DictReader(io.StringIO(buf.getvalue()))
    rows = list(reader)
    assert reader.fieldnames == FIELDS
    assert rows[0]["operation"] == "fork" and rows[0]["blob_bytes"] == "0"
    assert rows[1]["blob_bytes"] == "5000000"
    assert rows[1]["error"] == "RuntimeError('nope')"


def test_default_ports_do_not_collide_with_the_main_sweep():
    """run_benchmark.py owns 18555/18556 and 19000+; a collision would make
    one script silently measure the other's processes."""
    assert bench_checkpoint.FORKYARD_PORT == 18600
    assert bench_checkpoint.FORKYARD_MCP_PORT == 18601
    assert bench_checkpoint.ANVIL_BASE_PORT == 19200


def test_cli_help_says_what_is_being_compared(monkeypatch, capsys):
    """--help is where a reader learns this is not a like-named API
    comparison; without that sentence the two elapsed_ms columns look
    like the same operation measured twice."""
    monkeypatch.setattr(sys, "argv", ["bench_checkpoint.py", "--help"])
    with pytest.raises(SystemExit):
        bench_checkpoint.main()
    # argparse rewraps the description, so compare on collapsed whitespace
    # rather than on wherever the terminal width happened to break a line.
    help_text = " ".join(capsys.readouterr().out.split())
    assert "anvil_dumpState" in help_text
    assert "forkyard has no snapshot RPC" in help_text


def test_cli_refuses_to_run_without_an_endpoint(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_checkpoint.py"])
    monkeypatch.delenv("RPC_URL", raising=False)
    with pytest.raises(SystemExit):
        bench_checkpoint.main()
    assert "--rpc-url" in capsys.readouterr().err
