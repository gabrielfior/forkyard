import csv

import pytest

import bench_quota
from agent import ActionRecord
from bench_quota import (
    FIELDS,
    action_success_rate,
    max_sustainable_agents,
    quota_row,
)
from rpc_proxy import ProxyStats


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
    """main() drives a DictWriter with FIELDS, so a mismatch would raise
    only after the first sweep had already been run and thrown away."""
    stats = ProxyStats(jsonrpc_calls=400, throttled_calls=310, total_delay_ms=1234.56)
    row = quota_row("anvil", 25, 50, 0.264, 9876.5, stats, 10)

    assert list(row.keys()) == FIELDS
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

    monkeypatch.setattr(bench_quota, "CountingProxy", _FakeProxy)
    monkeypatch.setattr(bench_quota, "run_forkyard_sweep", fake_forkyard)
    monkeypatch.setattr(bench_quota, "run_anvil_sweep", fake_anvil)
    monkeypatch.setattr(bench_quota, "_check_binaries_on_path", lambda: None)
    return calls


def _run_main(monkeypatch, out_path, extra: list[str] | None = None) -> list[dict[str, str]]:
    argv = [
        "bench_quota.py", "--quotas", "10", "--agents", "1,2",
        "--actions-per-agent", "2", "--settle-s", "0",
        "--rpc-url", "http://upstream.example", "--out", str(out_path),
        *(extra or []),
    ]
    monkeypatch.setattr("sys.argv", argv)
    bench_quota.main()
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
        "bench_quota.py", "--quotas", "10", "--agents", "1,2,4",
        "--actions-per-agent", "2", "--settle-s", "0",
        "--rpc-url", "http://upstream.example", "--out", str(tmp_path / "q.csv"),
    ])
    bench_quota.main()
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
    assert {c["rpc_url"] for c in fake_sweeps} == {f"http://127.0.0.1:{bench_quota.PROXY_PORT}"}
    assert {c["backend"] for c in fake_sweeps} == {"forkyard", "anvil"}


def test_the_quota_reaches_the_limiter(tmp_path, monkeypatch, fake_sweeps):
    _run_main(monkeypatch, tmp_path / "quota.csv", ["--quotas", "10,25", "--limit-mode", "reject"])

    built = _FakeProxy.built
    assert [b["rate_limit_rps"] for b in built] == [10, 25], "one proxy per quota, carrying that quota"
    assert all(b["limit_mode"] == "reject" for b in built)
    assert all(b["port"] == bench_quota.PROXY_PORT for b in built)


def test_forkyard_and_anvil_use_the_documented_ports(tmp_path, monkeypatch, fake_sweeps):
    """Fixed and distinct from run_benchmark.py's, so a long quota sweep
    and an ordinary sweep can coexist."""
    _run_main(monkeypatch, tmp_path / "quota.csv")

    ports = {c["backend"]: c["port"] for c in fake_sweeps}
    assert ports["forkyard"] == bench_quota.FORKYARD_PORT
    assert ports["anvil"] == bench_quota.ANVIL_BASE_PORT
