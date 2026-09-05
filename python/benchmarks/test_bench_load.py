import bench_load
import csv
import io
import json
import pytest
import random
import threading
import time

from agent import ActionRecord
from bench_load import (
    ARRIVALS_FIELDS,
    ARRIVALS_SUMMARY_FIELDS,
    ArrivalRecord,
    ConcurrencyGauge,
    FRESHNESS_FIELDS,
    FRESHNESS_SUMMARY_FIELDS,
    ForkyardRefresher,
    QUOTA_FIELDS,
    RefreshRecord,
    TipPoller,
    action_success_rate,
    anvil_reset_to_latest,
    arrivals_summarize,
    fetch_tip,
    freshness_summarize,
    max_sustainable_agents,
    poisson_arrivals,
    quota_row,
    run_arrival,
    run_refresh_loop,
    schedule_refreshes,
)
from bench_common import PortAllocator, parse_float_list, percentile
from rpc_proxy import ProxyStats


# --- from test_bench_arrivals

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


def _arrivals_record(agent_id, ms, ok=True):
    return ArrivalRecord("anvil", 5.0, agent_id, float(agent_id), ms, ok, "" if ok else "boom")


def test_summarize_reports_percentiles_over_successes_and_counts_failures():
    """Folding a 20s Anvil spawn timeout into the tail would blame the
    architecture for this machine's resource limit; the failure count is how
    such a run announces itself instead."""
    records = [_arrivals_record(i, float(i)) for i in range(1, 11)] + [_arrivals_record(11, 20_000.0, ok=False)]
    row = arrivals_summarize(records, "anvil", 5.0, peak_concurrent_envs=7)

    assert list(row.keys()) == ARRIVALS_SUMMARY_FIELDS, "row and header must stay in lockstep"
    assert row["arrivals"] == 11
    assert row["completed"] == 10
    assert row["failures"] == 1
    assert row["p50_ms"] == 5.5
    assert row["max_ms"] == 10.0, "the failed 20s arrival must not become the max"
    assert row["peak_concurrent_envs"] == 7


def test_summarize_leaves_percentiles_blank_when_nothing_succeeded():
    row = arrivals_summarize([_arrivals_record(1, 5.0, ok=False)], "anvil", 20.0, peak_concurrent_envs=1)
    assert row["completed"] == 0
    assert row["p50_ms"] == "" and row["max_ms"] == ""


def test_arrivals_fields_and_row_stay_in_lockstep():
    """`arrivals_main` drives a DictWriter with ARRIVALS_FIELDS directly, so a mismatch would
    surface only mid-run, after real measurements had been taken."""
    row = bench_load._arrivals_row(_arrivals_record(3, 1.5))
    assert list(row.keys()) == ARRIVALS_FIELDS
    assert ARRIVALS_FIELDS[-1] == "error", "error stays last so positional consumers keep working"


def test_arrivals_row_round_trips_through_a_csv_writer():
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=ARRIVALS_FIELDS)
    writer.writeheader()
    writer.writerow(bench_load._arrivals_row(_arrivals_record(0, 12.3456789, ok=False)))
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
    monkeypatch.setattr(bench_load, "transfer", _ok_transfer)
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
    monkeypatch.setattr(bench_load, "transfer", _failed_transfer)
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
    monkeypatch.setattr(bench_load, "transfer", _ok_transfer)

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


# --- from test_bench_freshness

class FakeManager:
    def __init__(self):
        self.calls: list[tuple[str, list]] = []

    def request_blocking(self, method, params):
        self.calls.append((method, params))
        return None


class FakeEth:
    def __init__(self, block_number=0):
        self.block_number = block_number


class FakeWeb3:
    def __init__(self, block_number=0):
        self.manager = FakeManager()
        self.eth = FakeEth(block_number)


class FakeSession:
    """Stands in for a forkyard session: a block number and a discard."""

    def __init__(self, block_number):
        self._w3 = FakeWeb3(block_number)
        self.discarded = False

    def web3(self):
        return self._w3

    def discard(self):
        self.discarded = True


def test_schedule_refreshes_starts_at_zero_and_covers_the_duration():
    assert schedule_refreshes(120, 30) == [0.0, 30.0, 60.0, 90.0]
    assert schedule_refreshes(60, 30) == [0.0, 30.0]


def test_schedule_refreshes_rounds_a_partial_interval_up():
    """A 100s window at 30s spacing still gets its fourth refresh: dropping
    it would leave the last third of the run unmeasured."""
    assert schedule_refreshes(100, 30) == [0.0, 30.0, 60.0, 90.0]


def test_schedule_refreshes_always_yields_at_least_one():
    assert schedule_refreshes(0, 30) == [0.0]


def test_schedule_refreshes_rejects_a_non_positive_interval():
    with pytest.raises(ValueError, match="positive"):
        schedule_refreshes(120, 0)


def test_anvil_reset_asks_for_latest_by_omitting_the_block_number():
    """Naming a block would pin the instance, and a pinned Anvil has no
    freshness to measure — this is the line that keeps the Anvil side
    honestly chasing the tip."""
    w3 = FakeWeb3()
    anvil_reset_to_latest(w3, "http://127.0.0.1:1234")

    assert w3.manager.calls == [
        ("anvil_reset", [{"forking": {"jsonRpcUrl": "http://127.0.0.1:1234"}}])
    ]
    assert "blockNumber" not in w3.manager.calls[0][1][0]["forking"]


def test_fetch_tip_parses_the_hex_block_number(monkeypatch):
    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {"jsonrpc": "2.0", "id": 1, "result": "0x1899e00"}

    posted = {}

    def fake_post(url, json=None, timeout=None):
        posted["url"], posted["json"] = url, json
        return Response()

    monkeypatch.setattr(bench_load.requests, "post", fake_post)
    assert fetch_tip("http://endpoint") == 0x1899E00
    assert posted["url"] == "http://endpoint"
    assert posted["json"]["method"] == "eth_blockNumber"


def test_tip_poller_has_an_answer_before_start_returns(monkeypatch):
    """The first refresh happens at t=0; without a synchronous first poll it
    would have no yardstick and its lag column would be blank."""
    monkeypatch.setattr(bench_load, "fetch_tip", lambda url, **kw: 100)
    # A long interval keeps the background thread from polling at all: this
    # test is about what `start()` itself guarantees.
    poller = TipPoller("http://endpoint", interval_s=3600).start()
    try:
        assert poller.tip() == 100
    finally:
        poller.stop()


def test_tip_poller_keeps_the_last_good_tip_when_the_endpoint_blips(monkeypatch):
    monkeypatch.setattr(bench_load, "fetch_tip", lambda url, **kw: 100)
    poller = TipPoller("http://endpoint", interval_s=3600)
    poller._poll_once()

    def boom(url, **kw):
        raise RuntimeError("502")

    monkeypatch.setattr(bench_load, "fetch_tip", boom)
    poller._poll_once()

    assert poller.tip() == 100, "a blip must not erase the yardstick"
    assert poller.errors == 1


def test_forkyard_refresher_opens_a_new_session_each_time(monkeypatch):
    """Opening a session *is* forkyard's refresh: the follower has already
    re-forked the shared base, and a session inherits whichever base existed
    when it was opened."""
    blocks = iter([1000, 1001, 1002])
    opened: list[FakeSession] = []

    def fake_backend(base_url=None, session_url=None):
        session = FakeSession(next(blocks))
        opened.append(session)
        return session

    monkeypatch.setattr(bench_load, "ForkyardBackend", fake_backend)
    refresher = ForkyardRefresher("http://127.0.0.1:18630")

    assert refresher.refresh() == 1000
    assert refresher.refresh() == 1001
    assert len(opened) == 2


def test_forkyard_refresher_returns_the_old_session_outside_the_timed_call(monkeypatch):
    """`refresh()` is the only timed call, so the discard of the session it
    replaced belongs in `settle()` — charging it to the refresh would
    overstate what an agent waits for."""
    opened: list[FakeSession] = []

    def fake_backend(base_url=None, session_url=None):
        session = FakeSession(1)
        opened.append(session)
        return session

    monkeypatch.setattr(bench_load, "ForkyardBackend", fake_backend)
    refresher = ForkyardRefresher("http://127.0.0.1:18630")

    refresher.refresh()
    refresher.refresh()
    assert opened[0].discarded is False, "the old session was discarded inside the stopwatch"

    refresher.settle()
    assert opened[0].discarded is True
    assert opened[1].discarded is False, "the session in use must survive settle()"

    refresher.close()
    assert opened[1].discarded is True


class FakeRefresher:
    def __init__(self, blocks):
        self._blocks = iter(blocks)
        self.settled = 0

    def refresh(self):
        value = next(self._blocks)
        if isinstance(value, Exception):
            raise value
        return value

    def settle(self):
        self.settled += 1

    def close(self):
        pass


def test_run_refresh_loop_scores_lag_against_the_tip_read_after_the_refresh():
    refresher = FakeRefresher([1000, 1001])
    records = run_refresh_loop(
        refresher, "forkyard", 5, 2, [0.0, 0.0], time.monotonic(), lambda: 1003
    )

    assert [r.refresh_index for r in records] == [0, 1]
    assert [r.observed_block for r in records] == [1000, 1001]
    assert [r.block_lag for r in records] == [3, 2]
    assert all(r.ok and r.agents == 5 and r.agent_id == 2 for r in records)
    assert refresher.settled == 2, "cleanup must run after every refresh, not only the last"


def test_run_refresh_loop_records_a_failed_refresh_and_keeps_going():
    refresher = FakeRefresher([RuntimeError("anvil_reset failed"), 1001])
    records = run_refresh_loop(
        refresher, "anvil", 1, 0, [0.0, 0.0], time.monotonic(), lambda: 1001
    )

    assert records[0].ok is False
    assert records[0].observed_block == -1
    assert records[0].block_lag == -1, "a refresh that produced no block has no lag to report"
    assert "anvil_reset failed" in records[0].error
    assert records[1].ok is True and records[1].block_lag == 0


def test_run_refresh_loop_tolerates_a_yardstick_that_never_answered():
    records = run_refresh_loop(
        FakeRefresher([1000]), "forkyard", 1, 0, [0.0], time.monotonic(), lambda: None
    )
    assert records[0].ok is True
    assert records[0].true_tip == -1 and records[0].block_lag == -1


def _freshness_record(agent_id, index, lag, ms, ok=True):
    return RefreshRecord(
        "anvil", 5, agent_id, index, 1000, 1000 + lag, lag if ok else -1, ms, ok,
        "" if ok else "boom",
    )


def test_summarize_reports_lag_latency_and_the_upstream_bill():
    records = [_freshness_record(i, 0, lag=i, ms=float(i * 10)) for i in range(1, 11)]
    stats = ProxyStats(
        http_requests=50, jsonrpc_calls=200,
        by_method={"eth_getStorageAt": 120, "eth_getCode": 60, "eth_blockNumber": 20},
        upstream_errors=1,
    )
    row = freshness_summarize(records, "anvil", 5, stats)

    assert list(row.keys()) == FRESHNESS_SUMMARY_FIELDS, "row and header must stay in lockstep"
    assert row["refreshes"] == 10 and row["ok_refreshes"] == 10
    assert row["lag_p50"] == 5.5
    assert row["refresh_ms_p50"] == 55.0
    assert row["calls_per_agent_refresh"] == 20.0, "200 calls over 10 agent-refreshes"
    assert row["upstream_errors"] == 1
    assert list(json.loads(row["top_methods"])) == ["eth_getStorageAt", "eth_getCode", "eth_blockNumber"]


def test_summarize_excludes_failed_refreshes_from_the_lag_statistics():
    """A failed refresh has no observed block; scoring it as lag 0 would
    flatter the backend that failed, and as lag ∞ would invent a number."""
    records = [_freshness_record(0, 0, lag=1, ms=10.0), _freshness_record(1, 0, lag=99, ms=99.0, ok=False)]
    row = freshness_summarize(records, "forkyard", 2, ProxyStats(jsonrpc_calls=4))

    assert row["ok_refreshes"] == 1
    assert row["lag_p50"] == 1 and row["lag_p95"] == 1
    assert row["calls_per_agent_refresh"] == 2.0, "the denominator is every attempted refresh"


def test_summarize_leaves_statistics_blank_when_every_refresh_failed():
    row = freshness_summarize([_freshness_record(0, 0, lag=0, ms=1.0, ok=False)], "anvil", 1, ProxyStats())
    assert row["lag_p50"] == "" and row["refresh_ms_p95"] == ""


def test_freshness_fields_and_row_stay_in_lockstep():
    row = bench_load._freshness_row(_freshness_record(1, 2, lag=3, ms=4.5))
    assert list(row.keys()) == FRESHNESS_FIELDS
    assert FRESHNESS_FIELDS[-1] == "error"


def test_freshness_row_round_trips_through_a_csv_writer():
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FRESHNESS_FIELDS)
    writer.writeheader()
    writer.writerow(bench_load._freshness_row(_freshness_record(0, 1, lag=2, ms=3.14159)))
    parsed = list(csv.DictReader(io.StringIO(buf.getvalue())))[0]
    assert parsed["block_lag"] == "2"
    assert parsed["refresh_ms"] == "3.142"


def test_latest_anvil_is_spawned_unpinned_and_without_the_foundry_cache(monkeypatch):
    """Two flags carry the whole Anvil side: no `--fork-block-number` (a
    pinned instance cannot go stale, so it would measure nothing), and
    `--no-storage-caching` (otherwise a "refresh" is served out of
    ~/.foundry/cache written by an earlier run)."""
    spawned: dict[str, list[str]] = {}

    class FakeProcess:
        pid = 4321

        def terminate(self):
            pass

        def wait(self, timeout=None):
            return 0

        def kill(self):
            pass

    monkeypatch.setattr(
        bench_load.subprocess, "Popen",
        lambda argv, *a, **k: (spawned.__setitem__("argv", argv), FakeProcess())[1],
    )
    monkeypatch.setattr(
        bench_load.LatestAnvilBackend, "_wait_until_ready", lambda self, timeout: None
    )

    backend = bench_load.LatestAnvilBackend(19500, "http://rpc.example")

    assert "--fork-block-number" not in spawned["argv"]
    assert "--no-storage-caching" in spawned["argv"]
    assert spawned["argv"][:3] == ["anvil", "--fork-url", "http://rpc.example"]
    assert backend.name == "anvil"


def test_a_failed_spawn_still_discards_the_anvils_that_did_start(monkeypatch):
    """`list(pool.map(...))` re-raised the first failure and dropped the
    successful backends on the floor, leaving them running. One timed-out
    spawn out of 25 left 24 live Anvils resident under every later
    benchmark on the machine, quietly taxing their measurements."""
    import bench_load

    discarded: list[int] = []

    class FakeAnvil:
        def __init__(self, port, rpc_url):
            if port == 21005:
                raise RuntimeError(f"anvil on {port} did not become ready in 30.0s")
            self.port = port

        def discard(self):
            discarded.append(self.port)

    monkeypatch.setattr(bench_load, "LatestAnvilBackend", FakeAnvil)

    class Ports:
        def __init__(self):
            self.n = 21000

        def next(self):
            self.n += 1
            return self.n

    with pytest.raises(RuntimeError, match="did not become ready"):
        bench_load.run_anvil_freshness(
            "http://rpc", 8, [0.0], lambda: 1, Ports(), object()
        )

    assert 21005 not in discarded, "the one that never started has nothing to discard"
    assert sorted(discarded) == [21001, 21002, 21003, 21004, 21006, 21007, 21008], (
        "every Anvil that did start must be torn down before the failure propagates"
    )


# --- from test_bench_quota

def _records(oks: list[bool], action: str = "transfer") -> list[ActionRecord]:
    return [ActionRecord("anvil", 1, 1, 0, action, 1.0, ok, "" if ok else "boom") for ok in oks]


def test_action_success_rate_is_the_fraction_that_worked():
    assert action_success_rate(_records([True] * 4)) == 1.0
    assert action_success_rate(_records([True, True, True, False])) == 0.75


def test_a_failed_acquire_counts_against_the_rate():
    """Under a tight quota the first thing to fail is Anvil forking at all.
    An agent that never got an environment has failed, and must not be
    excluded just because it produced fewer rows than a healthy one."""
    records = _records([False], action="acquire")
    assert action_success_rate(records) == 0.0


def test_an_empty_run_is_zero_not_one():
    """Vacuous success is the failure mode that would quietly declare any
    crashed backend infinitely scalable."""
    assert action_success_rate([]) == 0.0


def test_max_sustainable_is_the_largest_count_that_passed():
    points = [(5, 1.0), (10, 1.0), (25, 0.264), (50, 0.1)]
    assert max_sustainable_agents(points, threshold=0.99) == 10


def test_max_sustainable_stops_at_the_first_failure_even_if_a_later_point_passes():
    """A pass above a failure is noise or luck, not capacity. The raw
    points stay in the CSV so the anomaly is still visible."""
    points = [(5, 1.0), (10, 0.5), (25, 1.0)]
    assert max_sustainable_agents(points, threshold=0.99) == 5


def test_max_sustainable_is_zero_when_even_the_smallest_point_fails():
    assert max_sustainable_agents([(5, 0.9), (10, 0.2)], threshold=0.99) == 0


def test_max_sustainable_sorts_its_input():
    assert max_sustainable_agents([(50, 0.1), (5, 1.0), (10, 1.0)], threshold=0.99) == 10


def test_the_threshold_is_what_decides():
    points = [(5, 1.0), (10, 0.95)]
    assert max_sustainable_agents(points, threshold=0.99) == 5
    assert max_sustainable_agents(points, threshold=0.90) == 10


def test_quota_row_stays_in_lockstep_with_the_header():
    """quota_main() drives a DictWriter with QUOTA_FIELDS, so a mismatch would raise
    only after the first sweep had already been run and thrown away."""
    stats = ProxyStats(jsonrpc_calls=400, throttled_calls=310, total_delay_ms=1234.56)
    row = quota_row("anvil", 25, 50, 0.264, 9876.5, stats, 10)

    assert list(row.keys()) == QUOTA_FIELDS
    assert row["action_success_rate"] == 0.264
    assert row["throttled_calls"] == 310
    assert row["total_delay_ms"] == 1234.6
    assert row["max_sustainable_agents"] == 10


class _FakeProxy:
    """Stands in for CountingProxy: records how it was configured, so the
    test can assert the quota really reached the limiter."""

    built: list[dict[str, object]] = []

    def __init__(self, upstream, port=0, host="127.0.0.1", rate_limit_rps=None, burst=None, limit_mode="delay"):
        self.url = f"http://127.0.0.1:{port}"
        self.rate_limit_rps = rate_limit_rps
        self.stopped = False
        _FakeProxy.built.append(
            {"upstream": upstream, "port": port, "rate_limit_rps": rate_limit_rps, "limit_mode": limit_mode}
        )

    def start(self):
        return self

    def reset(self):
        pass

    def snapshot(self):
        return ProxyStats(jsonrpc_calls=100, throttled_calls=7, total_delay_ms=42.0)

    def stop(self):
        self.stopped = True


@pytest.fixture
def fake_sweeps(monkeypatch):
    """No subprocesses and no network: the sweep functions are replaced by
    a model in which forkyard always copes and Anvil falls over past one
    agent — the shape the real thing produces, run in milliseconds."""
    _FakeProxy.built = []
    calls: list[dict[str, object]] = []

    def fake_forkyard(rpc_url, block_height, num_agents, actions_per_agent, port, mcp_port, episodes=1):
        calls.append({"backend": "forkyard", "rpc_url": rpc_url, "num_agents": num_agents, "port": port})
        return _records([True] * (num_agents * actions_per_agent)), 100.0 * num_agents

    def fake_anvil(rpc_url, block_height, num_agents, actions_per_agent, base_port, episodes=1):
        calls.append({"backend": "anvil", "rpc_url": rpc_url, "num_agents": num_agents, "port": base_port})
        total = num_agents * actions_per_agent
        failures = 0 if num_agents <= 1 else total // 2
        return _records([True] * (total - failures) + [False] * failures), 500.0 * num_agents

    monkeypatch.setattr(bench_load, "CountingProxy", _FakeProxy)
    monkeypatch.setattr(bench_load, "run_forkyard_sweep", fake_forkyard)
    monkeypatch.setattr(bench_load, "run_anvil_sweep", fake_anvil)
    monkeypatch.setattr(bench_load, "_check_binaries_on_path", lambda: None)
    return calls


def _run_main(monkeypatch, out_path, extra: list[str] | None = None) -> list[dict[str, str]]:
    argv = [
        "bench_load.py", "--quotas", "10", "--agents", "1,2",
        "--actions-per-agent", "2", "--settle-s", "0",
        "--rpc-url", "http://upstream.example", "--out", str(out_path),
        *(extra or []),
    ]
    monkeypatch.setattr("sys.argv", argv)
    bench_load.quota_main()
    with open(out_path, newline="") as f:
        return list(csv.DictReader(f))


def test_main_writes_a_row_per_point_and_the_curve_verdict(tmp_path, monkeypatch, fake_sweeps):
    rows = _run_main(monkeypatch, tmp_path / "quota.csv")

    assert [r for r in rows if r["backend"] == "forkyard"], rows
    forkyard = {int(r["num_agents"]): r for r in rows if r["backend"] == "forkyard"}
    anvil = {int(r["num_agents"]): r for r in rows if r["backend"] == "anvil"}

    assert set(forkyard) == {1, 2}, "forkyard passes everywhere, so the whole curve is run"
    assert forkyard[2]["max_sustainable_agents"] == "2"
    assert anvil[2]["action_success_rate"] == "0.5"
    assert anvil[2]["max_sustainable_agents"] == "1", "Anvil's curve breaks at 2 agents"
    assert forkyard[1]["throttled_calls"] == "7"
    assert forkyard[1]["quota_rps"] == "10"


def test_main_stops_a_curve_after_the_first_failing_point(tmp_path, monkeypatch, fake_sweeps):
    """Points above a failure cannot raise max_sustainable_agents, and each
    one costs a full sweep of real subprocesses."""
    monkeypatch.setattr("sys.argv", [
        "bench_load.py", "--quotas", "10", "--agents", "1,2,4",
        "--actions-per-agent", "2", "--settle-s", "0",
        "--rpc-url", "http://upstream.example", "--out", str(tmp_path / "q.csv"),
    ])
    bench_load.quota_main()
    with open(tmp_path / "q.csv", newline="") as f:
        rows = list(csv.DictReader(f))

    anvil = sorted(int(r["num_agents"]) for r in rows if r["backend"] == "anvil")
    assert anvil == [1, 2], "the 4-agent point must not be run once 2 already failed"


def test_full_curve_keeps_going_past_the_first_failure(tmp_path, monkeypatch, fake_sweeps):
    rows = _run_main(monkeypatch, tmp_path / "full.csv", ["--full-curve", "--agents", "1,2,4"])

    anvil = sorted(int(r["num_agents"]) for r in rows if r["backend"] == "anvil")
    assert anvil == [1, 2, 4]
    assert all(r["max_sustainable_agents"] == "1" for r in rows if r["backend"] == "anvil")


def test_both_backends_are_pointed_at_the_throttled_proxy(tmp_path, monkeypatch, fake_sweeps):
    """The whole measurement is void if a backend talks to the endpoint
    directly: it would then be the only one not paying the quota."""
    _run_main(monkeypatch, tmp_path / "quota.csv")

    assert fake_sweeps, "no sweep ran"
    assert {c["rpc_url"] for c in fake_sweeps} == {f"http://127.0.0.1:{bench_load.PROXY_PORT}"}
    assert {c["backend"] for c in fake_sweeps} == {"forkyard", "anvil"}


def test_the_quota_reaches_the_limiter(tmp_path, monkeypatch, fake_sweeps):
    _run_main(monkeypatch, tmp_path / "quota.csv", ["--quotas", "10,25", "--limit-mode", "reject"])

    built = _FakeProxy.built
    assert [b["rate_limit_rps"] for b in built] == [10, 25], "one proxy per quota, carrying that quota"
    assert all(b["limit_mode"] == "reject" for b in built)
    assert all(b["port"] == bench_load.PROXY_PORT for b in built)


def test_forkyard_and_anvil_use_the_documented_ports(tmp_path, monkeypatch, fake_sweeps):
    """Fixed and distinct from run_benchmark.py's, so a long quota sweep
    and an ordinary sweep can coexist."""
    _run_main(monkeypatch, tmp_path / "quota.csv")

    ports = {c["backend"]: c["port"] for c in fake_sweeps}
    assert ports["forkyard"] == bench_load.FORKYARD_PORT
    assert ports["anvil"] == bench_load.ANVIL_BASE_PORT
