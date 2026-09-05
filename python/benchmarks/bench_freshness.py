"""Chain-tip freshness at fleet scale: what does it cost N agents to keep
simulating against the *current* block?

Every other benchmark here pins a historical block, which makes the fork a
one-time cost. Real agents work against the tip, and the tip moves every
~12s. That turns "fork the chain" from a setup step into a recurring bill,
and the two architectures pay it very differently:

  * forkyard re-forks ONE shared base per new block, in the background
    (`ChainTipFollower`), for every session at once. An agent gets fresh
    state by opening a new session — it inherits whatever base exists then.
    The cost of a new block does not depend on how many agents there are.
  * each Anvil is its own process with its own fetch cache. Refreshing it
    means `anvil_reset` with forking params, and every instance refetches
    for itself. N agents means N re-forks.

So this script runs both backends against the LIVE tip (forkyard without
`FORKYARD_FORK_BLOCK_NUMBER`, so the follower actually runs; Anvil without
`--fork-block-number`), has N agents demand fresh state every
`--refresh-secs`, and reports two things per (backend, N): how stale the
state they got was (block lag against an independently polled tip), and how
many upstream JSON-RPC calls the fleet spent to get it.

Both backends are routed through `rpc_proxy.CountingProxy` for that second
number. The tip poller deliberately is NOT — see `TipPoller`. Counting
starts only once every environment is up (see `main`): the initial fork is
setup, and the question here is the cost of *staying* fresh.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import math
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, Protocol, Sequence

import requests
from web3 import Web3

from backend import AnvilBackend, ForkyardBackend
# One implementation of each, shared with the arrivals benchmark: a second
# percentile or a second port counter would be a second thing to trust.
from bench_arrivals import PortAllocator, percentile
from rpc_proxy import CountingProxy, ProxyStats
from run_benchmark import _check_binaries_on_path, _terminate, _wait_for_forkyard

FIELDS = [
    "backend", "agents", "agent_id", "refresh_index",
    "observed_block", "true_tip", "block_lag", "refresh_ms", "ok", "error",
]

# Written to a sibling `<out>.summary.csv`: one row per (backend, N),
# carrying both halves of the claim — how fresh, and at what upstream cost.
SUMMARY_FIELDS = [
    "backend", "agents", "refreshes", "ok_refreshes",
    "lag_p50", "lag_p95", "refresh_ms_p50", "refresh_ms_p95",
    "http_requests", "jsonrpc_calls", "calls_per_agent_refresh", "upstream_errors", "top_methods",
]

_MAX_ERROR_CHARS = 200


@dataclass
class RefreshRecord:
    backend: str
    agents: int
    agent_id: int
    refresh_index: int
    # -1 when the refresh failed and there is no block to report; `true_tip`
    # is likewise -1 only if the poller never got an answer at all.
    observed_block: int
    true_tip: int
    block_lag: int
    refresh_ms: float
    ok: bool
    error: str = ""


def schedule_refreshes(duration_s: float, refresh_secs: float) -> list[float]:
    """Refresh instants (seconds after the phase starts), the first at t=0.

    Starting at 0 rather than at `refresh_secs` makes the first refresh a
    measurement too: for forkyard it is the first session opened after the
    fleet came up, for Anvil the first reset, and both are exactly what an
    agent does when it wants current state."""
    if refresh_secs <= 0:
        raise ValueError(f"refresh interval must be positive, got {refresh_secs}")
    count = max(1, math.ceil(duration_s / refresh_secs))
    return [i * refresh_secs for i in range(count)]


def fetch_tip(rpc_url: str, timeout_s: float = 5.0) -> int:
    resp = requests.post(
        rpc_url,
        json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
        timeout=timeout_s,
    )
    resp.raise_for_status()
    return int(resp.json()["result"], 16)


class TipPoller:
    """The independent yardstick: what block the real chain is on, right now.

    It talks to the endpoint DIRECTLY and never through the CountingProxy.
    Half of what this benchmark reports is "upstream calls spent staying
    fresh", and a yardstick that adds its own steady stream of
    `eth_blockNumber` to that number would inflate both backends' bills by
    the same amount — flattening exactly the ratio under test."""

    def __init__(self, rpc_url: str, interval_s: float = 2.0):
        self._rpc_url = rpc_url
        self._interval_s = interval_s
        self._lock = threading.Lock()
        self._tip: int | None = None
        self.errors = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _poll_once(self) -> None:
        try:
            tip = fetch_tip(self._rpc_url)
        except Exception:
            with self._lock:
                self.errors += 1
            return
        with self._lock:
            self._tip = tip

    def _run(self) -> None:
        while not self._stop.wait(self._interval_s):
            self._poll_once()

    def start(self) -> "TipPoller":
        # One synchronous poll first, so the very first refresh already has
        # a tip to be scored against instead of an empty lag column.
        self._poll_once()
        self._thread.start()
        return self

    def tip(self) -> int | None:
        with self._lock:
            return self._tip

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval_s + 5)


def anvil_reset_to_latest(w3: Web3, fork_url: str) -> None:
    """Re-fork this Anvil at the chain tip.

    Omitting `blockNumber` is what makes it "latest" — naming one would pin
    the instance, which is the opposite of what is being measured. Each of
    the N instances pays for this independently; that is the claim."""
    w3.manager.request_blocking("anvil_reset", [{"forking": {"jsonRpcUrl": fork_url}}])


class Refresher(Protocol):
    """One agent's way of demanding current state.

    `refresh()` is the only timed call. `settle()` is whatever cleanup the
    refresh implies but the agent does not wait on — for forkyard, handing
    back the session it just replaced."""

    def refresh(self) -> int: ...
    def settle(self) -> None: ...
    def close(self) -> None: ...


class ForkyardRefresher:
    """A forkyard agent refreshes by opening a NEW session.

    There is no reset call and nothing per-agent to refetch: the follower
    has already re-forked the shared base, and a session inherits whichever
    base existed when it was opened. Discarding the session it replaces
    happens in `settle()`, outside the stopwatch, because the agent is
    already working in the new one by then."""

    def __init__(self, base_url: str):
        self._base_url = base_url
        self._current: ForkyardBackend | None = None
        self._stale: list[ForkyardBackend] = []

    def refresh(self) -> int:
        session = ForkyardBackend(base_url=self._base_url)
        block = session.web3().eth.block_number
        if self._current is not None:
            self._stale.append(self._current)
        self._current = session
        return block

    def settle(self) -> None:
        while self._stale:
            try:
                self._stale.pop().discard()
            except Exception:
                print("warning: forkyard session failed to discard", file=sys.stderr)

    def close(self) -> None:
        self.settle()
        if self._current is not None:
            try:
                self._current.discard()
            finally:
                self._current = None


class LatestAnvilBackend(AnvilBackend):
    """`AnvilBackend` forked at the tip instead of at a pinned block.

    `__init__` is overridden rather than parameterised because the base
    class always passes `--fork-block-number`, and this file cannot pin
    anything — a pinned instance has no freshness to measure. Everything
    else is inherited: the readiness probe, the terminate → kill teardown,
    the web3 handle, and `--no-storage-caching` (without which an Anvil
    would serve a "refresh" out of `~/.foundry/cache` written by an earlier
    run, i.e. report freshness it never fetched)."""

    def __init__(self, port: int, fork_url: str, startup_timeout_s: float = 30.0):
        try:
            self._process = subprocess.Popen(
                ["anvil", "--fork-url", fork_url, "--port", str(port), "--silent",
                 "--no-storage-caching"],
            )
        except FileNotFoundError as e:
            raise RuntimeError(
                "the `anvil` binary was not found on PATH — install Foundry "
                "(https://book.getfoundry.sh/getting-started/installation) before running the Anvil backend"
            ) from e
        self._url = f"http://127.0.0.1:{port}"
        self._fork_url = fork_url
        try:
            self._wait_until_ready(startup_timeout_s)
        except BaseException:
            self._terminate_process()
            raise
        self._w3 = Web3(Web3.HTTPProvider(self._url))


class AnvilRefresher:
    """An Anvil agent refreshes by re-forking its own instance. The instance
    is spawned once, before the measured phase, so what is timed here is the
    refetch — not the process start."""

    def __init__(self, backend: LatestAnvilBackend, fork_url: str):
        self._backend = backend
        self._fork_url = fork_url

    def refresh(self) -> int:
        anvil_reset_to_latest(self._backend.web3(), self._fork_url)
        return self._backend.web3().eth.block_number

    def settle(self) -> None:
        pass

    def close(self) -> None:
        self._backend.discard()


def run_refresh_loop(
    refresher: Refresher,
    backend_name: str,
    num_agents: int,
    agent_id: int,
    schedule: Sequence[float],
    origin: float,
    tip: Callable[[], int | None],
) -> list[RefreshRecord]:
    """One agent's whole refresh phase: wake at each scheduled instant,
    demand fresh state, score what it got."""
    records: list[RefreshRecord] = []
    for index, at in enumerate(schedule):
        remaining = origin + at - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        start = time.monotonic()
        try:
            observed = refresher.refresh()
            ok, error = True, ""
        except Exception as e:
            observed, ok, error = -1, False, repr(e)[:_MAX_ERROR_CHARS]
        refresh_ms = (time.monotonic() - start) * 1000

        # The tip is read AFTER the refresh, not before: a refresh that
        # takes ten seconds must be scored against where the chain is when
        # the agent finally gets its answer, otherwise a slow backend is
        # credited with freshness it never delivered.
        true_tip = tip()
        lag = true_tip - observed if (ok and true_tip is not None) else -1
        records.append(
            RefreshRecord(
                backend_name, num_agents, agent_id, index, observed,
                true_tip if true_tip is not None else -1, lag, refresh_ms, ok, error,
            )
        )
        refresher.settle()
    return records


def run_refresh_phase(
    refreshers: Sequence[Refresher],
    backend_name: str,
    schedule: Sequence[float],
    tip: Callable[[], int | None],
) -> list[RefreshRecord]:
    """All N agents demand fresh state at the same instants — a fleet
    reacting to the same new block, which is the situation the shared base
    is supposed to absorb."""
    origin = time.monotonic()
    num_agents = len(refreshers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_agents) as pool:
        futures = [
            pool.submit(run_refresh_loop, r, backend_name, num_agents, i, schedule, origin, tip)
            for i, r in enumerate(refreshers)
        ]
        return [record for f in futures for record in f.result()]


def _row(r: RefreshRecord) -> dict[str, object]:
    return {
        "backend": r.backend,
        "agents": r.agents,
        "agent_id": r.agent_id,
        "refresh_index": r.refresh_index,
        "observed_block": r.observed_block,
        "true_tip": r.true_tip,
        "block_lag": r.block_lag,
        "refresh_ms": round(r.refresh_ms, 3),
        "ok": r.ok,
        "error": r.error,
    }


def summarize(
    records: Sequence[RefreshRecord], backend: str, agents: int, stats: ProxyStats
) -> dict[str, object]:
    """Lag statistics come from successful refreshes only — a failed one has
    no observed block, and scoring it as lag 0 or as lag ∞ would both be
    inventions. `calls_per_agent_refresh` is the number the architectures
    differ on: constant-ish for a shared base that is re-forked once, roughly
    a full fork's worth for a fleet that each refetches."""
    ok_records = [r for r in records if r.ok]
    lags = [r.block_lag for r in ok_records if r.block_lag >= 0]
    latencies = [r.refresh_ms for r in ok_records]
    top = dict(sorted(stats.by_method.items(), key=lambda kv: -kv[1])[:5])
    return {
        "backend": backend,
        "agents": agents,
        "refreshes": len(records),
        "ok_refreshes": len(ok_records),
        "lag_p50": round(percentile(lags, 50), 2) if lags else "",
        "lag_p95": round(percentile(lags, 95), 2) if lags else "",
        "refresh_ms_p50": round(percentile(latencies, 50), 3) if latencies else "",
        "refresh_ms_p95": round(percentile(latencies, 95), 3) if latencies else "",
        "http_requests": stats.http_requests,
        "jsonrpc_calls": stats.jsonrpc_calls,
        "calls_per_agent_refresh": round(stats.jsonrpc_calls / len(records), 1) if records else "",
        "upstream_errors": stats.upstream_errors,
        "top_methods": json.dumps(top),
    }


def run_forkyard_freshness(
    rpc_url: str, num_agents: int, schedule: Sequence[float], tip: Callable[[], int | None],
    port: int, mcp_port: int, poll_secs: float, proxy: CountingProxy,
) -> list[RefreshRecord]:
    """Start a forkyard that FOLLOWS THE TIP (no pinned block) and let N
    agents refresh against it."""
    env = {**os.environ, "RPC_URL": rpc_url, "FORKYARD_PORT": str(port),
           "FORKYARD_MCP_HTTP_PORT": str(mcp_port), "FORKYARD_INGEST_POLL_SECS": str(int(poll_secs))}
    # An inherited pinned block would disable the ChainTipFollower outright
    # and silently turn this into a benchmark of a frozen fork.
    env.pop("FORKYARD_FORK_BLOCK_NUMBER", None)
    process = subprocess.Popen(["forkyard"], env=env)
    base_url = f"http://127.0.0.1:{port}"
    refreshers: list[Refresher] = []
    try:
        _wait_for_forkyard(base_url)
        refreshers = [ForkyardRefresher(base_url) for _ in range(num_agents)]
        # Counting starts once the fleet exists: the initial fork is setup,
        # and this benchmark is about what staying fresh costs afterwards.
        proxy.reset()
        return run_refresh_phase(refreshers, "forkyard", schedule, tip)
    finally:
        for refresher in refreshers:
            try:
                refresher.close()
            except Exception:
                pass
        _terminate(process)


def run_anvil_freshness(
    rpc_url: str, num_agents: int, schedule: Sequence[float], tip: Callable[[], int | None],
    ports: PortAllocator, proxy: CountingProxy,
) -> list[RefreshRecord]:
    """Spawn N tip-forked Anvils, then let each refresh itself."""
    assigned = [ports.next() for _ in range(num_agents)]
    backends: list[LatestAnvilBackend] = []
    try:
        # Spawned concurrently: N sequential cold forks would take longer
        # than the measured phase itself at N=25.
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_agents) as pool:
            backends = list(pool.map(lambda p: LatestAnvilBackend(p, rpc_url), assigned))
        proxy.reset()
        refreshers: list[Refresher] = [AnvilRefresher(b, rpc_url) for b in backends]
        return run_refresh_phase(refreshers, "anvil", schedule, tip)
    finally:
        for backend in backends:
            try:
                backend.discard()
            except Exception:
                pass


def parse_int_list(s: str) -> list[int]:
    return [int(x) for x in s.split(",")]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Measure what it costs a fleet of N agents to keep simulating against the "
            "CURRENT block. Both backends run unpinned (forkyard follows the tip and "
            "re-forks one shared base per new block; each Anvil must anvil_reset and "
            "refetch for itself), N agents demand fresh state every --refresh-secs, and "
            "the run reports block lag against an independently polled tip plus the "
            "upstream JSON-RPC calls spent staying fresh."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--agents", type=parse_int_list, default=[5, 25],
                        help="fleet sizes to run, comma-separated")
    parser.add_argument("--duration", type=float, default=120.0, help="seconds per (backend, fleet size)")
    parser.add_argument("--refresh-secs", type=float, default=30.0,
                        help="how often each agent demands fresh state")
    parser.add_argument("--poll-secs", type=float, default=4.0,
                        help="FORKYARD_INGEST_POLL_SECS for the chain-tip follower; below "
                             "the ~12s block time so a new block is picked up within the run")
    parser.add_argument("--tip-poll-secs", type=float, default=2.0,
                        help="how often the independent yardstick polls the real endpoint "
                             "(direct, never through the counting proxy)")
    parser.add_argument("--backends", default="forkyard,anvil", help="comma-separated subset to run")
    parser.add_argument("--forkyard-port", type=int, default=18630)
    parser.add_argument("--forkyard-mcp-port", type=int, default=18631)
    parser.add_argument("--anvil-base-port", type=int, default=19500)
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"),
                        help="upstream endpoint (defaults to $RPC_URL)")
    parser.add_argument("--out", default="freshness.csv")
    args = parser.parse_args()

    if not args.rpc_url:
        parser.error("--rpc-url is required (or set RPC_URL)")
    backends = [b.strip() for b in args.backends.split(",") if b.strip()]
    unknown = set(backends) - {"forkyard", "anvil"}
    if unknown:
        parser.error(f"unknown backend(s): {', '.join(sorted(unknown))}")

    _check_binaries_on_path()
    schedule = schedule_refreshes(args.duration, args.refresh_secs)
    ports = PortAllocator(args.anvil_base_port)
    summary_path = args.out.rsplit(".", 1)[0] + ".summary.csv"

    proxy = CountingProxy(args.rpc_url).start()
    poller = TipPoller(args.rpc_url, args.tip_poll_secs).start()
    try:
        with open(args.out, "w", newline="") as f, open(summary_path, "w", newline="") as sf:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            summary_writer = csv.DictWriter(sf, fieldnames=SUMMARY_FIELDS)
            summary_writer.writeheader()
            f.flush()
            sf.flush()

            for num_agents in args.agents:
                for backend in backends:
                    print(
                        f"running {backend}: agents={num_agents} refreshes={len(schedule)} "
                        f"over {args.duration}s",
                        file=sys.stderr,
                    )
                    if backend == "forkyard":
                        records = run_forkyard_freshness(
                            proxy.url, num_agents, schedule, poller.tip,
                            args.forkyard_port, args.forkyard_mcp_port, args.poll_secs, proxy,
                        )
                    else:
                        records = run_anvil_freshness(
                            proxy.url, num_agents, schedule, poller.tip, ports, proxy
                        )
                    stats = proxy.snapshot()
                    writer.writerows(_row(r) for r in records)
                    f.flush()
                    row = summarize(records, backend, num_agents, stats)
                    summary_writer.writerow(row)
                    sf.flush()
                    print(
                        f"  lag p50={row['lag_p50']} p95={row['lag_p95']} blocks; "
                        f"refresh p50={row['refresh_ms_p50']}ms p95={row['refresh_ms_p95']}ms; "
                        f"upstream {row['jsonrpc_calls']} calls "
                        f"({row['calls_per_agent_refresh']}/agent-refresh)",
                        file=sys.stderr,
                    )
            if poller.errors:
                print(f"warning: the tip poller failed {poller.errors} times", file=sys.stderr)
    finally:
        poller.stop()
        proxy.stop()


if __name__ == "__main__":
    main()
