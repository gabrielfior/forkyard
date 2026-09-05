import csv
import io
import random
import threading
import time

import pytest

import bench_arrivals
from bench_arrivals import (
    FIELDS,
    SUMMARY_FIELDS,
    ArrivalRecord,
    ConcurrencyGauge,
    PortAllocator,
    parse_float_list,
    percentile,
    poisson_arrivals,
    run_arrival,
    summarize,
)


class FakeBackend:
    """Stands in for a forkyard session or an Anvil instance: enough surface
    for `run_arrival`, none of the process/network machinery."""

    name = "fake"

    def __init__(self):
        self.funded: list[tuple[str, int]] = []
        self.discarded = False

    def web3(self):
        raise AssertionError("run_arrival must not talk web3 outside actions.transfer")

    def set_native_balance(self, address, wei):
        self.funded.append((address, wei))

    def set_storage(self, address, slot_hex, value_hex):
        raise AssertionError("unused")

    def discard(self):
        self.discarded = True


def _ok_transfer(*args, **kwargs):
    return ("transfer", 1.0, True, "")


def _failed_transfer(*args, **kwargs):
    return ("transfer", 1.0, False, "RuntimeError('reverted')")


def test_poisson_arrivals_is_reproducible_for_a_seed():
    """Both backends replay the identical schedule; if the generator were
    not seed-reproducible they would be answering different arrival
    processes and the comparison would be meaningless."""
    a = poisson_arrivals(5.0, 10.0, random.Random("seed"))
    b = poisson_arrivals(5.0, 10.0, random.Random("seed"))
    assert a == b


def test_poisson_arrivals_are_increasing_and_inside_the_window():
    arrivals = poisson_arrivals(20.0, 3.0, random.Random(1))
    assert arrivals == sorted(arrivals)
    assert len(set(arrivals)) == len(arrivals)
    assert all(0 <= t < 3.0 for t in arrivals)


def test_poisson_arrivals_average_out_to_lambda_times_duration():
    """The mean count is the property the whole experiment leans on: a
    generator that quietly produced half the arrivals would understate every
    backlog."""
    counts = [len(poisson_arrivals(10.0, 10.0, random.Random(seed))) for seed in range(200)]
    mean = sum(counts) / len(counts)
    assert 90 < mean < 110  # expected 100, ~7% s.e. of the mean over 200 trials


def test_poisson_arrivals_rejects_a_non_positive_rate():
    with pytest.raises(ValueError, match="positive"):
        poisson_arrivals(0.0, 10.0, random.Random(0))


def test_percentile_interpolates_between_neighbours():
    values = [0.0, 10.0]
    assert percentile(values, 0) == 0.0
    assert percentile(values, 50) == 5.0
    assert percentile(values, 100) == 10.0
    assert percentile(values, 95) == 9.5


def test_percentile_matches_a_hand_computed_vector():
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert percentile(values, 50) == 5.5
    assert percentile(values, 95) == pytest.approx(9.55)
    assert percentile(values, 99) == pytest.approx(9.91)


def test_percentile_does_not_depend_on_input_order():
    assert percentile([9, 1, 5], 50) == 5


def test_percentile_of_a_single_value_is_that_value():
    assert percentile([42.0], 99) == 42.0


def test_percentile_rejects_empty_input_and_an_out_of_range_q():
    with pytest.raises(ValueError):
        percentile([], 50)
    with pytest.raises(ValueError, match=r"\[0, 100\]"):
        percentile([1.0], 101)


def _record(agent_id, ms, ok=True):
    return ArrivalRecord("anvil", 5.0, agent_id, float(agent_id), ms, ok, "" if ok else "boom")


def test_summarize_reports_percentiles_over_successes_and_counts_failures():
    """Folding a 20s Anvil spawn timeout into the tail would blame the
    architecture for this machine's resource limit; the failure count is how
    such a run announces itself instead."""
    records = [_record(i, float(i)) for i in range(1, 11)] + [_record(11, 20_000.0, ok=False)]
    row = summarize(records, "anvil", 5.0, peak_concurrent_envs=7)

    assert list(row.keys()) == SUMMARY_FIELDS, "row and header must stay in lockstep"
    assert row["arrivals"] == 11
    assert row["completed"] == 10
    assert row["failures"] == 1
    assert row["p50_ms"] == 5.5
    assert row["max_ms"] == 10.0, "the failed 20s arrival must not become the max"
    assert row["peak_concurrent_envs"] == 7


def test_summarize_leaves_percentiles_blank_when_nothing_succeeded():
    row = summarize([_record(1, 5.0, ok=False)], "anvil", 20.0, peak_concurrent_envs=1)
    assert row["completed"] == 0
    assert row["p50_ms"] == "" and row["max_ms"] == ""


def test_fields_and_row_stay_in_lockstep():
    """`main` drives a DictWriter with FIELDS directly, so a mismatch would
    surface only mid-run, after real measurements had been taken."""
    row = bench_arrivals._row(_record(3, 1.5))
    assert list(row.keys()) == FIELDS
    assert FIELDS[-1] == "error", "error stays last so positional consumers keep working"


def test_row_round_trips_through_a_csv_writer():
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerow(bench_arrivals._row(_record(0, 12.3456789, ok=False)))
    parsed = list(csv.DictReader(io.StringIO(buf.getvalue())))[0]
    assert parsed["time_to_first_success_ms"] == "12.346"
    assert parsed["ok"] == "False"
    assert parsed["error"] == "boom"


def test_concurrency_gauge_tracks_the_peak_not_the_total():
    gauge = ConcurrencyGauge()
    gauge.enter()
    gauge.enter()
    gauge.leave()
    gauge.enter()
    gauge.leave()
    gauge.leave()
    assert gauge.peak == 2
    assert gauge.alive == 0


def test_concurrency_gauge_blocks_past_its_limit():
    """The cap is what keeps a λ=20 Anvil run from asking for hundreds of
    processes at once; the wait it imposes is deliberately inside the
    arrival's latency."""
    gauge = ConcurrencyGauge(limit=1)
    gauge.enter()
    entered = threading.Event()

    def second():
        gauge.enter()
        entered.set()

    thread = threading.Thread(target=second, daemon=True)
    thread.start()
    assert not entered.wait(0.2), "the second environment got a slot while the first held it"
    gauge.leave()
    assert entered.wait(2), "releasing a slot did not unblock the waiter"
    thread.join(timeout=2)
    assert gauge.peak == 1


def test_run_arrival_measures_from_the_scheduled_instant_not_from_thread_start(monkeypatch):
    """The queueing an overloaded backend causes is the measurement. A clock
    started when the worker thread finally got going would delete it."""
    monkeypatch.setattr(bench_arrivals, "transfer", _ok_transfer)
    backend = FakeBackend()
    gauge = ConcurrencyGauge()

    scheduled_at = time.monotonic() - 0.25  # the agent "arrived" 250ms ago
    record = run_arrival(lambda: backend, "fake", 3, 5.0, 1.5, scheduled_at, gauge)

    assert record.ok and record.error == ""
    assert record.time_to_first_success_ms >= 250
    assert record.agent_id == 3 and record.arrival_s == 1.5 and record.arrival_rate == 5.0
    assert backend.funded, "the agent must fund itself before transacting"
    assert backend.discarded, "the environment must be handed back"
    assert gauge.alive == 0


def test_run_arrival_records_a_failed_transfer_without_raising(monkeypatch):
    monkeypatch.setattr(bench_arrivals, "transfer", _failed_transfer)
    backend = FakeBackend()
    gauge = ConcurrencyGauge()

    record = run_arrival(lambda: backend, "fake", 0, 1.0, 0.0, time.monotonic(), gauge)

    assert record.ok is False
    assert "reverted" in record.error
    assert backend.discarded


def test_run_arrival_records_a_failed_acquisition_and_frees_its_slot():
    """A never-ready Anvil must appear as one failed arrival, not as a run
    that dies — and must not permanently consume a concurrency slot."""
    gauge = ConcurrencyGauge(limit=1)

    def explode():
        raise RuntimeError("anvil on http://127.0.0.1:19400 did not become ready in 20.0s")

    record = run_arrival(explode, "anvil", 1, 20.0, 0.5, time.monotonic(), gauge)

    assert record.ok is False
    assert "did not become ready" in record.error
    assert gauge.alive == 0
    gauge.enter()  # the semaphore was released, so this must not hang


def test_run_arrival_survives_a_failing_discard(monkeypatch):
    """The discard happens after the stopwatch stopped, so failing it must
    not turn a served arrival into a failed one."""
    monkeypatch.setattr(bench_arrivals, "transfer", _ok_transfer)

    class UndiscardableBackend(FakeBackend):
        def discard(self):
            raise RuntimeError("session already gone")

    record = run_arrival(UndiscardableBackend, "fake", 0, 1.0, 0.0, time.monotonic(), ConcurrencyGauge())
    assert record.ok is True


def test_port_allocator_never_hands_out_the_same_port_twice():
    """A killed Anvil's port lingers in TIME_WAIT, so reuse inside a run
    would show up as spurious startup failures at the higher rates."""
    ports = PortAllocator(19400)
    handed = [ports.next() for _ in range(500)]
    assert handed[0] == 19400
    assert len(set(handed)) == 500


def test_parse_float_list_accepts_fractional_rates():
    assert parse_float_list("0.5,1,20") == [0.5, 1.0, 20.0]
