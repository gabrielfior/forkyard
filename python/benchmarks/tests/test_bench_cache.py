import bench_cache
import csv
import io
import pytest

from bench_cache import (
    AgentOutcome,
    BLOCKS_FIELDS,
    BlockRecord,
    SUMMARY_FIELDS,
    SummaryRow,
    WARMSTART_FIELDS,
    _row,
    _summary_row,
    assign_agent_blocks,
    block_heights,
    clear_dir,
    distinct_state_verified,
    foundry_cache_dir,
    group_agents_by_block,
    open_pinned_session,
    parse_args,
    probe_environment,
    run_block_agent,
    summarize_round,
    write_records,
    write_summaries,
)
from pathlib import Path


# --- from test_bench_warmstart

class _FakeBackend:
    def web3(self):
        class Eth:
            def estimate_gas(self, tx):
                return 21_000

            def get_balance(self, address):
                return 0

        class W3:
            eth = Eth()

        return W3()

    def discard(self):
        pass


class _FakeProcess:
    pid = 1

    def terminate(self):
        pass

    def wait(self, timeout=None):
        return 0

    def kill(self):
        pass


def test_foundry_cache_is_cleared_per_block_not_wholesale():
    """Clearing all of ~/.foundry/cache would throw away the user's own
    unrelated work; the benchmark only owns the block it pinned."""
    path = foundry_cache_dir(25_795_072)

    assert path.name == "25795072"
    assert path.parent.name == "mainnet"
    assert "cache" in path.parts and ".foundry" in path.parts


def test_clear_dir_is_a_noop_on_a_missing_directory(tmp_path):
    clear_dir(tmp_path / "never-existed")  # the cold run must not need one to exist


def test_clear_dir_removes_a_populated_cache(tmp_path):
    block_dir = tmp_path / "1"
    block_dir.mkdir()
    (block_dir / "25795072.json").write_text("{}")

    clear_dir(tmp_path)

    assert not tmp_path.exists()


def test_forkyard_run_never_inherits_the_cache_kill_switch(monkeypatch):
    """The measurement pass exports FORKYARD_CACHE_DISABLED=1 so every other
    benchmark stays cold-vs-cold. Leaking it in here would make the warm row
    a second cold row — the finding would read as "persistence does nothing"."""
    captured: dict[str, dict[str, str]] = {}

    monkeypatch.setenv("FORKYARD_CACHE_DISABLED", "1")
    monkeypatch.setattr(
        bench_cache.subprocess, "Popen",
        lambda argv, env=None, **kw: (captured.__setitem__("env", env), _FakeProcess())[1],
    )
    monkeypatch.setattr(bench_cache, "_wait_for_forkyard", lambda url, **kw: None)
    monkeypatch.setattr(bench_cache, "_terminate", lambda process: None)
    monkeypatch.setattr(bench_cache, "ForkyardBackend", lambda **kw: _FakeBackend())

    bench_cache.run_forkyard("http://rpc", 1, 1, ["0xabc"], Path("/tmp/x"))

    assert "FORKYARD_CACHE_DISABLED" not in captured["env"]
    assert captured["env"]["FORKYARD_CACHE_DIR"] == "/tmp/x"


def test_forkyard_is_stopped_politely_so_its_cache_actually_lands(monkeypatch):
    """SIGKILL would skip the save path, leaving the warm run cold."""
    terminated: list[object] = []

    monkeypatch.setattr(
        bench_cache.subprocess, "Popen", lambda argv, env=None, **kw: _FakeProcess()
    )
    monkeypatch.setattr(bench_cache, "_wait_for_forkyard", lambda url, **kw: None)
    monkeypatch.setattr(bench_cache, "_terminate", lambda process: terminated.append(process))
    monkeypatch.setattr(bench_cache, "ForkyardBackend", lambda **kw: _FakeBackend())

    bench_cache.run_forkyard("http://rpc", 1, 2, ["0xabc"], Path("/tmp/x"))

    assert len(terminated) == 1, "the process must go through _terminate, not be leaked or killed"


def test_anvil_runs_with_foundrys_cache_enabled(monkeypatch):
    """The cold row clears the cache; the arm itself must never disable it,
    or the warm row measures nothing."""
    seen: list[bool] = []

    def fake_anvil(port, fork_url, block, rpc_cache=False):
        seen.append(rpc_cache)
        return _FakeBackend()

    monkeypatch.setattr(bench_cache, "AnvilBackend", fake_anvil)

    bench_cache.run_anvil("http://rpc", 1, 3, ["0xabc"])

    assert seen == [True, True, True]


def test_fields_cover_both_the_cost_and_the_outcome_columns():
    assert WARMSTART_FIELDS[:4] == ["backend", "condition", "agents", "contracts"]
    assert "jsonrpc_calls" in WARMSTART_FIELDS
    assert WARMSTART_FIELDS[-2:] == ["ok", "error"]


# --- from test_bench_blocks

class FakeEth:
    def __init__(self, block_number: int, balance: int, gas_price: int):
        self.block_number = block_number
        self._balance = balance
        self.gas_price = gas_price
        self.estimate_gas_calls: list[dict] = []

    def get_balance(self, address):
        return self._balance

    def estimate_gas(self, tx):
        self.estimate_gas_calls.append(tx)
        return 25_000


class FakeWeb3:
    def __init__(self, eth):
        self.eth = eth


class FakeBackend:
    """Stands in for a session or an Anvil: answers the probe with whatever
    block/state it was told to have, and remembers being discarded."""

    def __init__(self, block_number: int = 100, balance: int = 7, gas_price: int = 3,
                 fail_reads: bool = False):
        self._eth = FakeEth(block_number, balance, gas_price)
        self.discarded = False
        self.fail_reads = fail_reads

    def web3(self):
        if self.fail_reads:
            raise RuntimeError("backend is down")
        return FakeWeb3(self._eth)

    def set_native_balance(self, address, wei):  # pragma: no cover - unused here
        raise AssertionError("the read workload must not write")

    def set_storage(self, address, slot, value):  # pragma: no cover - unused here
        raise AssertionError("the read workload must not write")

    def discard(self):
        self.discarded = True


def _outcome(agent_id: int, block: int, reported: int | None, fingerprint: str | None):
    return AgentOutcome(agent_id, block, reported, fingerprint, [], True)


def test_block_heights_steps_backwards_and_stays_distinct():
    assert block_heights(25_795_072, 1_000, 4) == [25_795_072, 25_794_072, 25_793_072, 25_792_072]


def test_block_heights_of_one_is_just_the_base_block():
    """B=1 has to reduce to exactly what every other benchmark forks at, or
    the B=1 row is not a baseline for the others."""
    assert block_heights(25_795_072, 1_000, 1) == [25_795_072]


@pytest.mark.parametrize("base,stride,count", [(100, 1, 0), (100, 0, 4), (100, 1_000, 4)])
def test_block_heights_rejects_nonsense(base, stride, count):
    """Walking past genesis would surface as an opaque upstream error deep
    inside an arm, long after the sweep committed to it."""
    with pytest.raises(ValueError):
        block_heights(base, stride, count)


def test_assign_agent_blocks_is_round_robin_and_even_when_divisible():
    assignment = assign_agent_blocks(24, [10, 20, 30, 40])
    assert assignment[:5] == [10, 20, 30, 40, 10]
    assert all(assignment.count(b) == 6 for b in (10, 20, 30, 40))


def test_assign_agent_blocks_covers_every_block():
    """A block nobody is pinned to would silently shrink B, which is the
    sweep's independent variable."""
    for num_blocks in (1, 2, 4, 8):
        blocks = block_heights(1_000_000, 1_000, num_blocks)
        assert set(assign_agent_blocks(24, blocks)) == set(blocks)


def test_assign_agent_blocks_refuses_fewer_agents_than_blocks():
    with pytest.raises(ValueError):
        assign_agent_blocks(3, [1, 2, 3, 4])


def test_group_agents_by_block_inverts_the_assignment_in_block_order():
    groups = group_agents_by_block(assign_agent_blocks(6, [10, 20, 30]))
    assert list(groups) == [10, 20, 30]
    assert groups == {10: [0, 3], 20: [1, 4], 30: [2, 5]}


def test_distinct_state_verified_when_each_block_has_its_own_fingerprint():
    outcomes = [_outcome(0, 10, 10, "a"), _outcome(1, 20, 20, "b"), _outcome(2, 10, 10, "a")]
    assert distinct_state_verified(outcomes) == "yes"


def test_distinct_state_not_verified_when_two_blocks_agree():
    """Identical state at two different blocks means the pinning did nothing
    — the exact false positive this benchmark would otherwise publish."""
    assert distinct_state_verified([_outcome(0, 10, 10, "a"), _outcome(1, 20, 20, "a")]) == "no"


def test_distinct_state_not_verified_when_agents_at_one_block_disagree():
    """Two sessions at the same block must see the same base; if they don't,
    the 'one cache per block' claim is false however cheap the run was."""
    outcomes = [_outcome(0, 10, 10, "a"), _outcome(1, 10, 10, "z"), _outcome(2, 20, 20, "b")]
    assert distinct_state_verified(outcomes) == "no"


def test_distinct_state_is_na_with_a_single_block():
    """B=1 has nothing to distinguish; 'no' there would read as a failure."""
    assert distinct_state_verified([_outcome(0, 10, 10, "a"), _outcome(1, 10, 10, "a")]) == "n/a"


def test_distinct_state_is_na_when_every_probe_failed():
    assert distinct_state_verified([_outcome(0, 10, None, None), _outcome(1, 20, None, None)]) == "n/a"


def test_summarize_round_counts_wrong_and_unreadable_blocks_as_mismatches():
    outcomes = [
        _outcome(0, 10, 10, "a"),     # correct
        _outcome(1, 20, 19, "b"),     # wrong block
        _outcome(2, 30, None, None),  # unverifiable, which is not "verified"
    ]
    summary = summarize_round("forkyard", 3, 3, 1, outcomes, 42, 91.5, 1234.0)
    assert summary.block_mismatches == 2
    assert summary.jsonrpc_calls == 42
    assert summary.peak_rss_mb == 91.5
    assert summary.wall_clock_ms == 1234.0
    assert summary.distinct_state_verified == "yes"


def test_summarize_round_is_clean_when_every_session_reports_its_own_block():
    outcomes = [_outcome(i, 10 * (i % 2), 10 * (i % 2), f"fp{i % 2}") for i in range(8)]
    assert summarize_round("forkyard", 2, 8, 2, outcomes, 3, 80.0, 500.0).block_mismatches == 0


def test_run_block_agent_acquires_reads_every_contract_and_discards():
    backend = FakeBackend(block_number=777, balance=5, gas_price=2)
    outcome = run_block_agent(
        lambda: backend, arm="forkyard", blocks=2, agents=4, round_index=1,
        agent_id=3, block_number=777, contracts=["0xA", "0xB", "0xC"],
    )

    assert outcome.ok and outcome.reported_block == 777
    assert outcome.fingerprint == "5:2"
    phases = [r.phase for r in outcome.records]
    assert phases == ["acquire", "read", "read", "read", "discard"]
    assert all(r.ok for r in outcome.records)
    # Every row carries the sweep coordinates, or the CSV cannot be sliced.
    assert {(r.arm, r.blocks, r.agents, r.round, r.agent_id, r.block_number)
            for r in outcome.records} == {("forkyard", 2, 4, 1, 3, 777)}
    assert backend.discarded, "an undiscarded environment would inflate the next round"


def test_run_block_agent_reads_get_reserves_on_each_contract():
    backend = FakeBackend()
    run_block_agent(
        lambda: backend, arm="forkyard", blocks=1, agents=1, round_index=1,
        agent_id=0, block_number=100, contracts=["0xA", "0xB"],
    )
    calls = backend._eth.estimate_gas_calls
    assert [c["to"] for c in calls] == ["0xA", "0xB"]
    assert all(c["data"] == bench_cache.GET_RESERVES_SELECTOR for c in calls)


def test_run_block_agent_records_a_failed_acquire_and_stops_there():
    """A session forkyard refused must produce one honest failed row, not an
    exception that takes the other 23 agents' data with it."""
    def boom():
        raise RuntimeError("cannot open a session at block 1: no such block")

    outcome = run_block_agent(
        boom, arm="forkyard", blocks=2, agents=2, round_index=1,
        agent_id=1, block_number=1, contracts=["0xA"],
    )
    assert not outcome.ok
    assert [r.phase for r in outcome.records] == ["acquire"]
    assert "cannot open a session at block 1" in outcome.records[0].error
    assert outcome.reported_block is None


def test_run_block_agent_survives_an_unprobeable_backend():
    """A backend that cannot answer the probe still runs its reads; the
    unreadable block just becomes a mismatch in the summary."""
    backend = FakeBackend(fail_reads=True)
    outcome = run_block_agent(
        lambda: backend, arm="anvil", blocks=1, agents=1, round_index=1,
        agent_id=0, block_number=100, contracts=["0xA"],
    )
    assert outcome.reported_block is None and outcome.fingerprint is None
    assert not outcome.ok
    assert [r.phase for r in outcome.records] == ["acquire", "read", "discard"]


def test_run_block_agent_with_an_override_charges_the_shared_spawn_and_skips_the_noop_discard():
    """anvil-shared-unsafe: the group spawned the process, so its cost is
    charged to each waiter and the no-op per-agent discard is not recorded."""
    backend = FakeBackend()
    outcome = run_block_agent(
        lambda: backend, arm="anvil-shared-unsafe", blocks=2, agents=4, round_index=1,
        agent_id=2, block_number=50, contracts=["0xA"],
        acquire_override=("acquire", 4321.0, True, ""), emit_discard=False,
    )
    assert [r.phase for r in outcome.records] == ["acquire", "read"]
    assert outcome.records[0].elapsed_ms == 4321.0


def test_run_block_agent_with_a_failed_override_never_touches_the_backend():
    called = []
    outcome = run_block_agent(
        lambda: called.append(1), arm="anvil-shared-unsafe", blocks=1, agents=1,
        round_index=1, agent_id=0, block_number=50, contracts=["0xA"],
        acquire_override=("acquire", 12.0, False, "anvil did not start"), emit_discard=False,
    )
    assert not outcome.ok and called == []
    assert [r.phase for r in outcome.records] == ["acquire"]


def test_probe_environment_reads_block_balance_and_gas_price():
    reported, fingerprint = probe_environment(FakeBackend(block_number=9, balance=11, gas_price=13))
    assert (reported, fingerprint) == (9, "11:13")


def test_probe_environment_swallows_failures_rather_than_aborting_a_round():
    assert probe_environment(FakeBackend(fail_reads=True)) == (None, None)


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_open_pinned_session_posts_the_block_number_body(monkeypatch):
    seen = {}

    def fake_post(url, json=None, timeout=None):
        seen.update(url=url, json=json, timeout=timeout)
        return FakeResponse({"session_id": 7})

    monkeypatch.setattr(bench_cache.requests, "post", fake_post)
    url = open_pinned_session("http://127.0.0.1:18660", 25_706_811)

    assert seen["url"] == "http://127.0.0.1:18660/session"
    assert seen["json"] == {"block_number": 25_706_811}
    assert url == "http://127.0.0.1:18660/session/7"


def test_open_pinned_session_raises_on_the_error_payload(monkeypatch):
    """forkyard answers a refused block with HTTP 200 and {"error": ...}, so
    raise_for_status alone would sail straight past it."""
    monkeypatch.setattr(
        bench_cache.requests, "post",
        lambda *a, **k: FakeResponse({"error": "cannot open a session at block 1: too old"}),
    )
    with pytest.raises(RuntimeError, match="cannot open a session at block 1"):
        open_pinned_session("http://127.0.0.1:18660", 1)


def test_row_and_fields_stay_in_lockstep():
    record = BlockRecord("forkyard", 4, 24, 2, 3, 25_795_072, "read", 1.25, True, "")
    assert list(_row(record).keys()) == BLOCKS_FIELDS


def test_summary_row_and_fields_stay_in_lockstep():
    summary = SummaryRow("anvil", 4, 24, 1, 900, 1500.0, 42_000.0, 0, "yes")
    assert list(_summary_row(summary).keys()) == SUMMARY_FIELDS


def test_written_csvs_round_trip():
    buf = io.StringIO()
    write_records(buf, [BlockRecord("anvil", 2, 2, 1, 0, 100, "acquire", 3.5, False, "boom")])
    row = next(iter(csv.DictReader(io.StringIO(buf.getvalue()))))
    assert row["arm"] == "anvil" and row["phase"] == "acquire" and row["error"] == "boom"

    sbuf = io.StringIO()
    write_summaries(sbuf, [SummaryRow("forkyard", 2, 24, 2, 11, 90.0, 1000.0, 0, "yes")])
    srow = next(iter(csv.DictReader(io.StringIO(sbuf.getvalue()))))
    assert srow["jsonrpc_calls"] == "11" and srow["distinct_state_verified"] == "yes"


def test_defaults_put_max_pinned_blocks_above_the_largest_b():
    """At or below B the LRU evicts mid-round and the arm measures refetching
    rather than the sharing it claims to measure."""
    args = parse_args(["--rpc-url", "http://x", "--blocks", "1,2,4,8"])
    assert args.max_pinned_blocks is None
    assert (args.max_pinned_blocks or max(args.blocks) + 2) > max(args.blocks)


def test_blocks_for_prefers_an_explicit_block_list():
    args = parse_args(["--rpc-url", "http://x", "--block-list", "500,400,300"])
    assert bench_cache.blocks_for(args, 2) == [500, 400]
    with pytest.raises(ValueError):
        bench_cache.blocks_for(args, 4)


def test_anvil_shared_arm_is_off_by_default():
    """It gives up isolation between agents at a block; opting in has to be
    deliberate."""
    args = parse_args(["--rpc-url", "http://x"])
    assert "anvil-shared-unsafe" not in args.arms
