import csv
import io
import json
import time

import pytest

import bench_freshness
from bench_freshness import (
    FIELDS,
    SUMMARY_FIELDS,
    ForkyardRefresher,
    RefreshRecord,
    TipPoller,
    anvil_reset_to_latest,
    fetch_tip,
    run_refresh_loop,
    schedule_refreshes,
    summarize,
)
from rpc_proxy import ProxyStats


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

    monkeypatch.setattr(bench_freshness.requests, "post", fake_post)
    assert fetch_tip("http://endpoint") == 0x1899E00
    assert posted["url"] == "http://endpoint"
    assert posted["json"]["method"] == "eth_blockNumber"


def test_tip_poller_has_an_answer_before_start_returns(monkeypatch):
    """The first refresh happens at t=0; without a synchronous first poll it
    would have no yardstick and its lag column would be blank."""
    monkeypatch.setattr(bench_freshness, "fetch_tip", lambda url, **kw: 100)
    # A long interval keeps the background thread from polling at all: this
    # test is about what `start()` itself guarantees.
    poller = TipPoller("http://endpoint", interval_s=3600).start()
    try:
        assert poller.tip() == 100
    finally:
        poller.stop()


def test_tip_poller_keeps_the_last_good_tip_when_the_endpoint_blips(monkeypatch):
    monkeypatch.setattr(bench_freshness, "fetch_tip", lambda url, **kw: 100)
    poller = TipPoller("http://endpoint", interval_s=3600)
    poller._poll_once()

    def boom(url, **kw):
        raise RuntimeError("502")

    monkeypatch.setattr(bench_freshness, "fetch_tip", boom)
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

    monkeypatch.setattr(bench_freshness, "ForkyardBackend", fake_backend)
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

    monkeypatch.setattr(bench_freshness, "ForkyardBackend", fake_backend)
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


def _record(agent_id, index, lag, ms, ok=True):
    return RefreshRecord(
        "anvil", 5, agent_id, index, 1000, 1000 + lag, lag if ok else -1, ms, ok,
        "" if ok else "boom",
    )


def test_summarize_reports_lag_latency_and_the_upstream_bill():
    records = [_record(i, 0, lag=i, ms=float(i * 10)) for i in range(1, 11)]
    stats = ProxyStats(
        http_requests=50, jsonrpc_calls=200,
        by_method={"eth_getStorageAt": 120, "eth_getCode": 60, "eth_blockNumber": 20},
        upstream_errors=1,
    )
    row = summarize(records, "anvil", 5, stats)

    assert list(row.keys()) == SUMMARY_FIELDS, "row and header must stay in lockstep"
    assert row["refreshes"] == 10 and row["ok_refreshes"] == 10
    assert row["lag_p50"] == 5.5
    assert row["refresh_ms_p50"] == 55.0
    assert row["calls_per_agent_refresh"] == 20.0, "200 calls over 10 agent-refreshes"
    assert row["upstream_errors"] == 1
    assert list(json.loads(row["top_methods"])) == ["eth_getStorageAt", "eth_getCode", "eth_blockNumber"]


def test_summarize_excludes_failed_refreshes_from_the_lag_statistics():
    """A failed refresh has no observed block; scoring it as lag 0 would
    flatter the backend that failed, and as lag ∞ would invent a number."""
    records = [_record(0, 0, lag=1, ms=10.0), _record(1, 0, lag=99, ms=99.0, ok=False)]
    row = summarize(records, "forkyard", 2, ProxyStats(jsonrpc_calls=4))

    assert row["ok_refreshes"] == 1
    assert row["lag_p50"] == 1 and row["lag_p95"] == 1
    assert row["calls_per_agent_refresh"] == 2.0, "the denominator is every attempted refresh"


def test_summarize_leaves_statistics_blank_when_every_refresh_failed():
    row = summarize([_record(0, 0, lag=0, ms=1.0, ok=False)], "anvil", 1, ProxyStats())
    assert row["lag_p50"] == "" and row["refresh_ms_p95"] == ""


def test_fields_and_row_stay_in_lockstep():
    row = bench_freshness._row(_record(1, 2, lag=3, ms=4.5))
    assert list(row.keys()) == FIELDS
    assert FIELDS[-1] == "error"


def test_row_round_trips_through_a_csv_writer():
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerow(bench_freshness._row(_record(0, 1, lag=2, ms=3.14159)))
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
        bench_freshness.subprocess, "Popen",
        lambda argv, *a, **k: (spawned.__setitem__("argv", argv), FakeProcess())[1],
    )
    monkeypatch.setattr(
        bench_freshness.LatestAnvilBackend, "_wait_until_ready", lambda self, timeout: None
    )

    backend = bench_freshness.LatestAnvilBackend(19500, "http://rpc.example")

    assert "--fork-block-number" not in spawned["argv"]
    assert "--no-storage-caching" in spawned["argv"]
    assert spawned["argv"][:3] == ["anvil", "--fork-url", "http://rpc.example"]
    assert backend.name == "anvil"


def test_a_failed_spawn_still_discards_the_anvils_that_did_start(monkeypatch):
    """`list(pool.map(...))` re-raised the first failure and dropped the
    successful backends on the floor, leaving them running. One timed-out
    spawn out of 25 left 24 live Anvils resident under every later
    benchmark on the machine, quietly taxing their measurements."""
    import bench_freshness

    discarded: list[int] = []

    class FakeAnvil:
        def __init__(self, port, rpc_url):
            if port == 21005:
                raise RuntimeError(f"anvil on {port} did not become ready in 30.0s")
            self.port = port

        def discard(self):
            discarded.append(self.port)

    monkeypatch.setattr(bench_freshness, "LatestAnvilBackend", FakeAnvil)

    class Ports:
        def __init__(self):
            self.n = 21000

        def next(self):
            self.n += 1
            return self.n

    with pytest.raises(RuntimeError, match="did not become ready"):
        bench_freshness.run_anvil_freshness(
            "http://rpc", 8, [0.0], lambda: 1, Ports(), object()
        )

    assert 21005 not in discarded, "the one that never started has nothing to discard"
    assert sorted(discarded) == [21001, 21002, 21003, 21004, 21006, 21007, 21008], (
        "every Anvil that did start must be torn down before the failure propagates"
    )
