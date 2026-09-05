import csv
import io
import sys
import types

import pytest
from web3 import Web3

import bench_writers
from bench_writers import (
    FIELDS,
    RssSampler,
    SweepResult,
    WriterOutcome,
    _row,
    process_pids,
    run_writer,
    run_writers,
    summarize,
    total_rss_mb,
    write_results,
    writer_value,
)


class FakeWeb3Eth:
    def __init__(self, balances, address_key):
        self._balances = balances
        self._key = address_key

    def get_balance(self, address):
        return self._balances.get(address, 0)


class IsolatedBackend:
    """Its own private state, like a real session or a real Anvil."""

    def __init__(self, shared_state=None):
        # Passing a dict in makes every "environment" the *same* dict — the
        # leak this benchmark exists to rule out.
        self._state = {} if shared_state is None else shared_state
        self.storage_writes: list[tuple[str, str, str]] = []
        self.discarded = False

    def web3(self):
        return types.SimpleNamespace(eth=FakeWeb3Eth(self._state, None))

    def set_native_balance(self, address, wei):
        self._state[address] = wei

    def set_storage(self, address, slot, value):
        self.storage_writes.append((address, slot, value))

    def discard(self):
        self.discarded = True


def test_fields_and_row_stay_in_lockstep():
    result = SweepResult("forkyard", 10, 90.0, 1200.0, 160.0, 113.7, 0, True)
    assert list(_row(result).keys()) == FIELDS


def test_writer_value_is_unique_per_writer_and_per_round():
    """A value shared between two writers would make a leak look like a
    correct read; a value reused across rounds would hide a stale read."""
    values = [writer_value(w, r) for w in range(50) for r in range(20)]
    assert len(set(values)) == len(values)


def test_run_writer_writes_twice_per_round_and_verifies_its_own_value():
    backend = IsolatedBackend()
    outcome = run_writer(lambda: backend, writer_index=3, rounds=4)

    assert outcome.ok and outcome.error == ""
    # Two writes per round: one balance, one shared storage slot.
    assert outcome.writes == 8
    assert len(backend.storage_writes) == 4
    assert outcome.violations == 0
    assert backend.discarded, "an undiscarded environment would inflate the next sweep's RSS"


def test_run_writer_targets_the_same_account_and_slot_for_every_writer():
    """Writers aiming at different keys could never collide, so the
    isolation check would pass even on a completely shared environment."""
    first, second = IsolatedBackend(), IsolatedBackend()
    run_writer(lambda: first, 0, 1)
    run_writer(lambda: second, 1, 1)
    assert [w[:2] for w in first.storage_writes] == [w[:2] for w in second.storage_writes]
    assert first.storage_writes[0][0] == bench_writers.SHARED_CONTRACT


def test_run_writer_counts_a_violation_when_it_reads_someone_elses_value():
    class LeakyBackend(IsolatedBackend):
        def web3(self):
            # Whatever this writer wrote, it reads back a value that is not
            # its own — exactly what a shared environment would produce.
            return types.SimpleNamespace(
                eth=FakeWeb3Eth({bench_writers.SHARED_ACCOUNT: 1}, None)
            )

    outcome = run_writer(lambda: LeakyBackend(), writer_index=0, rounds=3)
    assert outcome.violations == 3
    assert outcome.ok is True, "the writes themselves succeeded; the isolation did not"


def test_two_writers_sharing_one_state_are_detected_as_a_leak():
    """The real failure mode, made deterministic: two environments over one
    state, and the other writer lands its write between this writer's write
    and its read-back. Concurrency is what makes this happen in the sweep;
    here it is scripted so the test cannot be flaky."""
    shared: dict[str, int] = {}
    other = IsolatedBackend(shared)

    class InterleavedBackend(IsolatedBackend):
        def web3(self):
            other.set_native_balance(bench_writers.SHARED_ACCOUNT, writer_value(1, 0))
            return super().web3()

    outcome = run_writer(lambda: InterleavedBackend(shared), writer_index=0, rounds=2)

    assert outcome.violations == 2, "every read-back saw writer 1's value, not its own"
    assert summarize("forkyard", 1, [outcome], 50.0, 1000.0).ok is False


def test_run_writer_records_an_acquisition_failure_as_zero_writes():
    def boom():
        raise RuntimeError("anvil did not become ready")

    outcome = run_writer(boom, writer_index=7, rounds=5)
    assert (outcome.ok, outcome.writes, outcome.writer_index) == (False, 0, 7)
    assert "did not become ready" in outcome.error


def test_run_writer_keeps_the_writes_it_managed_before_failing():
    class FlakyBackend(IsolatedBackend):
        def set_storage(self, address, slot, value):
            raise RuntimeError("connection reset")

    outcome = run_writer(lambda: FlakyBackend(), writer_index=0, rounds=3)
    assert outcome.ok is False
    assert outcome.writes == 1, "the balance write landed before the storage write failed"


def test_run_writer_survives_a_failing_discard():
    """Anvil's discard kills a process that may already be gone; that is a
    teardown problem, not a measurement failure."""
    class UndiscardableBackend(IsolatedBackend):
        def discard(self):
            raise RuntimeError("no such process")

    outcome = run_writer(lambda: UndiscardableBackend(), writer_index=0, rounds=1)
    assert outcome.ok is True


def test_run_writers_gives_every_writer_its_own_index():
    seen: list[int] = []
    backends = [IsolatedBackend() for _ in range(6)]

    def factory(i):
        seen.append(i)
        return lambda: backends[i]

    outcomes, wall_ms = run_writers(factory, writers=6, rounds=1)
    assert sorted(o.writer_index for o in outcomes) == list(range(6))
    assert sorted(seen) == list(range(6))
    assert wall_ms > 0


def test_summarize_computes_throughput_and_the_headline_density():
    outcomes = [WriterOutcome(i, writes=20, violations=0, ok=True) for i in range(10)]
    result = summarize("forkyard", 10, outcomes, peak_rss_mb=100.0, wall_clock_ms=2000.0)

    assert result.writes_per_sec == 100.0  # 200 writes in 2 s
    assert result.writers_per_gb == 102.4  # 10 writers per 100 MB
    assert result.ok is True


def test_summarize_fails_the_row_on_any_isolation_violation():
    """A leak makes the memory number meaningless — K environments that
    share state are not K isolated writers, however cheap they were."""
    outcomes = [
        WriterOutcome(0, writes=20, violations=0, ok=True),
        WriterOutcome(1, writes=20, violations=1, ok=True),
    ]
    result = summarize("forkyard", 2, outcomes, peak_rss_mb=50.0, wall_clock_ms=1000.0)
    assert result.isolation_violations == 1
    assert result.ok is False


def test_summarize_fails_the_row_when_a_writer_never_got_an_environment():
    outcomes = [
        WriterOutcome(0, writes=20, violations=0, ok=True),
        WriterOutcome(1, writes=0, violations=0, ok=False, error="RuntimeError('timeout')"),
    ]
    assert summarize("anvil", 2, outcomes, 60.0, 1000.0).ok is False


def test_summarize_reports_zero_density_rather_than_inventing_one():
    """Dividing by an unsampled RSS would print a spectacular number that
    means nothing."""
    outcomes = [WriterOutcome(0, writes=2, violations=0, ok=True)]
    result = summarize("forkyard", 1, outcomes, peak_rss_mb=0.0, wall_clock_ms=100.0)
    assert result.writers_per_gb == 0.0


def test_total_rss_mb_sums_ps_output_in_megabytes(monkeypatch):
    monkeypatch.setattr(
        bench_writers.subprocess, "run",
        lambda *a, **k: types.SimpleNamespace(stdout=" 30720\n 61440\n", returncode=0),
    )
    assert total_rss_mb({1, 2}) == pytest.approx(90.0)  # ps reports KiB


def test_total_rss_mb_is_zero_without_pids(monkeypatch):
    def fail(*a, **k):
        raise AssertionError("ps must not be spawned for an empty pid set")

    monkeypatch.setattr(bench_writers.subprocess, "run", fail)
    assert total_rss_mb(set()) == 0.0


def test_process_pids_parses_pgrep_and_ignores_noise(monkeypatch):
    monkeypatch.setattr(
        bench_writers.subprocess, "run",
        lambda *a, **k: types.SimpleNamespace(stdout="101\n102\nnot-a-pid\n", returncode=0),
    )
    assert process_pids("anvil") == {101, 102}


def test_process_pids_survives_a_machine_without_pgrep(monkeypatch):
    def missing(*a, **k):
        raise FileNotFoundError("pgrep")

    monkeypatch.setattr(bench_writers.subprocess, "run", missing)
    assert process_pids("anvil") == set()


def test_rss_sampler_excludes_processes_that_were_already_running(monkeypatch):
    """A forkyard the developer left running on another port would
    otherwise be charged to this sweep's memory."""
    sampled: list[set[int]] = []
    monkeypatch.setattr(bench_writers, "process_pids", lambda name: {1, 2, 3})
    monkeypatch.setattr(
        bench_writers, "total_rss_mb",
        lambda pids: (sampled.append(set(pids)), 10.0 * len(pids))[1],
    )

    sampler = RssSampler("forkyard", exclude_pids={1, 2})
    assert sampler.sample_once() == 10.0
    assert sampled == [{3}]


def test_rss_sampler_keeps_the_peak_not_the_last_reading(monkeypatch):
    """Anvil's processes are killed by their own writers as they finish, so
    the final reading is near zero; the peak is when they all coexisted."""
    readings = iter([30.0, 1500.0, 20.0])
    monkeypatch.setattr(bench_writers, "process_pids", lambda name: {9})
    monkeypatch.setattr(bench_writers, "total_rss_mb", lambda pids: next(readings))

    sampler = RssSampler("anvil", exclude_pids=set())
    for _ in range(3):
        sampler.sample_once()
    assert sampler.peak_mb == 1500.0


def test_rss_sampler_records_a_sample_before_the_first_tick(monkeypatch):
    """A one-writer sweep can finish inside a single sampling interval, and
    a zero there would silently become an infinite writers-per-GB."""
    monkeypatch.setattr(bench_writers, "process_pids", lambda name: {9})
    monkeypatch.setattr(bench_writers, "total_rss_mb", lambda pids: 42.0)

    sampler = RssSampler("forkyard", exclude_pids=set(), interval_s=60.0).start()
    assert sampler.stop() == 42.0


def test_write_results_round_trips_through_csv():
    buf = io.StringIO()
    write_results(buf, [
        SweepResult("forkyard", 50, 180.0, 4000.0, 250.0, 284.4, 0, True),
        SweepResult("anvil", 50, 1600.0, 30000.0, 33.3, 32.0, 0, True),
    ])
    reader = csv.DictReader(io.StringIO(buf.getvalue()))
    rows = list(reader)
    assert reader.fieldnames == FIELDS
    assert rows[0]["writers_per_gb"] == "284.4"
    assert rows[1]["backend"] == "anvil"


def test_shared_targets_are_checksummed():
    """web3.py rejects lowercase hex outright, which would turn every
    writer into an instant client-side failure."""
    assert Web3.to_checksum_address(bench_writers.SHARED_ACCOUNT) == bench_writers.SHARED_ACCOUNT
    assert Web3.to_checksum_address(bench_writers.SHARED_CONTRACT) == bench_writers.SHARED_CONTRACT


def test_default_ports_do_not_collide_with_the_other_benchmarks():
    import bench_checkpoint

    assert bench_writers.FORKYARD_PORT == 18610
    assert bench_writers.ANVIL_BASE_PORT == 19300
    assert bench_writers.FORKYARD_PORT != bench_checkpoint.FORKYARD_PORT
    assert bench_writers.ANVIL_BASE_PORT != bench_checkpoint.ANVIL_BASE_PORT


def test_cli_help_states_the_isolation_requirement(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_writers.py", "--help"])
    with pytest.raises(SystemExit):
        bench_writers.main()
    help_text = " ".join(capsys.readouterr().out.split())
    assert "isolation" in help_text
    assert "must be 0 for a row to mean anything" in help_text


def test_cli_refuses_to_run_without_an_endpoint(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_writers.py"])
    monkeypatch.delenv("RPC_URL", raising=False)
    with pytest.raises(SystemExit):
        bench_writers.main()
    assert "--rpc-url" in capsys.readouterr().err
