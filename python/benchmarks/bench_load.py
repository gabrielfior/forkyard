"""Benchmarks under load: arrival latency, chain-tip freshness and quota ceilings."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import math
import os
import random
import requests
import subprocess
import sys
import threading
import time

from actions import transfer
from agent import ActionRecord
from backend import AnvilBackend, Backend, ForkyardBackend
from bench_common import (
    DEFAULT_BLOCK_HEIGHT,
    MAX_ERROR_CHARS,
    PortAllocator,
    parse_float_list,
    parse_int_list,
    percentile,
    summary_path,
)
from dataclasses import dataclass
from eth_account import Account
from rpc_proxy import CountingProxy, ProxyStats
from run_benchmark import (
    _check_binaries_on_path,
    _terminate,
    _wait_for_forkyard,
    run_anvil_sweep,
    run_forkyard_sweep,
)
from typing import Callable, Protocol, Sequence
from web3 import Web3


# --- bench_arrivals: Time-to-first-simulation under an arrival process.

ARRIVALS_FIELDS = ["backend", "arrival_rate", "agent_id", "arrival_s", "time_to_first_success_ms", "ok", "error"]


ARRIVALS_SUMMARY_FIELDS = [
    "backend", "arrival_rate", "arrivals", "completed", "failures",
    "p50_ms", "p95_ms", "p99_ms", "max_ms", "peak_concurrent_envs",
]


ONE_ETH = 10**18


@dataclass
class ArrivalRecord:
    backend: str
    arrival_rate: float
    agent_id: int
    arrival_s: float
    # Scheduled arrival → first successful simulation (the transfer's
    # receipt). On failure: scheduled arrival → the moment the failure was
    # known, which is still the useful number — it is how long the agent
    # waited to find out it got nothing.
    time_to_first_success_ms: float
    ok: bool
    error: str = ""


def poisson_arrivals(rate_per_s: float, duration_s: float, rng: random.Random) -> list[float]:
    """Arrival instants (seconds after t=0) of a homogeneous Poisson process
    of intensity `rate_per_s`, over `[0, duration_s)`."""
    if rate_per_s <= 0:
        raise ValueError(f"arrival rate must be positive, got {rate_per_s}")
    arrivals: list[float] = []
    t = rng.expovariate(rate_per_s)
    while t < duration_s:
        arrivals.append(t)
        t += rng.expovariate(rate_per_s)
    return arrivals


def arrivals_summarize(
    records: Sequence[ArrivalRecord], backend: str, arrival_rate: float, peak_concurrent_envs: int
) -> dict[str, object]:
    """Percentiles are taken over *successful* arrivals only, with failures
    reported alongside as a count."""
    ok_ms = [r.time_to_first_success_ms for r in records if r.ok]
    return {
        "backend": backend,
        "arrival_rate": arrival_rate,
        "arrivals": len(records),
        "completed": len(ok_ms),
        "failures": len(records) - len(ok_ms),
        "p50_ms": round(percentile(ok_ms, 50), 3) if ok_ms else "",
        "p95_ms": round(percentile(ok_ms, 95), 3) if ok_ms else "",
        "p99_ms": round(percentile(ok_ms, 99), 3) if ok_ms else "",
        "max_ms": round(max(ok_ms), 3) if ok_ms else "",
        "peak_concurrent_envs": peak_concurrent_envs,
    }


def _arrivals_row(r: ArrivalRecord) -> dict[str, object]:
    return {
        "backend": r.backend,
        "arrival_rate": r.arrival_rate,
        "agent_id": r.agent_id,
        "arrival_s": round(r.arrival_s, 6),
        "time_to_first_success_ms": round(r.time_to_first_success_ms, 3),
        "ok": r.ok,
        "error": r.error,
    }


class ConcurrencyGauge:
    """Counts environments alive at this instant and remembers the peak —
    the number that says how much of the arrival process a backend was
    actually holding open at once."""

    def __init__(self, limit: int = 0):
        self._lock = threading.Lock()
        self._slots = threading.Semaphore(limit) if limit > 0 else None
        self.alive = 0
        self.peak = 0

    def enter(self) -> None:
        if self._slots is not None:
            self._slots.acquire()
        with self._lock:
            self.alive += 1
            self.peak = max(self.peak, self.alive)

    def leave(self) -> None:
        with self._lock:
            self.alive -= 1
        if self._slots is not None:
            self._slots.release()


def run_arrival(
    make_backend: Callable[[], Backend],
    backend_name: str,
    agent_id: int,
    arrival_rate: float,
    arrival_s: float,
    scheduled_at: float,
    gauge: ConcurrencyGauge,
    funding_wei: int = ONE_ETH,
) -> ArrivalRecord:
    """One arriving agent, end to end."""
    def since_arrival_ms() -> float:
        return (time.monotonic() - scheduled_at) * 1000

    gauge.enter()
    backend: Backend | None = None
    try:
        backend = make_backend()
        signer = Account.create()
        # Funding is a cheat-code write, not a simulation: the agent is only
        # "served" once the EVM has actually executed something for it, so
        # the stopwatch runs until the transfer's receipt.
        backend.set_native_balance(signer.address, funding_wei)
        _, _, ok, error = transfer(
            backend, signer.key.hex(), Account.create().address, funding_wei // 100, nonce=0
        )
        elapsed_ms = since_arrival_ms()
    except Exception as e:
        elapsed_ms, ok, error = since_arrival_ms(), False, repr(e)[:MAX_ERROR_CHARS]
    finally:
        if backend is not None:
            try:
                backend.discard()
            except Exception:
                # The discard is teardown, after the stopwatch stopped. A
                # failure here must not turn a served arrival into a failed
                # one — but it must also not vanish silently.
                print(f"warning: {backend_name} agent {agent_id} failed to discard", file=sys.stderr)
        gauge.leave()

    return ArrivalRecord(backend_name, arrival_rate, agent_id, arrival_s, elapsed_ms, ok, error)


def run_arrival_process(
    make_backend_for_agent: Callable[[int], Callable[[], Backend]],
    backend_name: str,
    arrival_rate: float,
    arrivals: Sequence[float],
    gauge: ConcurrencyGauge,
) -> list[ArrivalRecord]:
    """Dispatch `arrivals` on their own schedule and collect every record."""
    records: list[ArrivalRecord] = []
    lock = threading.Lock()
    threads: list[threading.Thread] = []
    origin = time.monotonic()

    def work(agent_id: int, arrival_s: float, scheduled_at: float, factory: Callable[[], Backend]) -> None:
        record = run_arrival(
            factory, backend_name, agent_id, arrival_rate, arrival_s, scheduled_at, gauge
        )
        with lock:
            records.append(record)

    for agent_id, arrival_s in enumerate(arrivals):
        scheduled_at = origin + arrival_s
        # Ports are allocated here, on the single dispatch thread, so the
        # per-agent port assignment can't race.
        factory = make_backend_for_agent(agent_id)
        remaining = scheduled_at - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        thread = threading.Thread(
            target=work, args=(agent_id, arrival_s, scheduled_at, factory), daemon=True
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    records.sort(key=lambda r: r.agent_id)
    return records


def run_forkyard_arrivals(
    rpc_url: str, block_height: int, arrival_rate: float, arrivals: Sequence[float],
    port: int, mcp_port: int, max_concurrent_envs: int,
) -> tuple[list[ArrivalRecord], int]:
    """Every arrival opens its own session on one already-running forkyard.
    The process start is excluded for the same reason `run_benchmark.py`
    excludes it: it is a one-time shared cost with no Anvil counterpart."""
    env = {
        **os.environ,
        "RPC_URL": rpc_url,
        "FORKYARD_PORT": str(port),
        "FORKYARD_MCP_HTTP_PORT": str(mcp_port),
        "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
    }
    process = subprocess.Popen(["forkyard"], env=env)
    base_url = f"http://127.0.0.1:{port}"
    gauge = ConcurrencyGauge(max_concurrent_envs)
    try:
        _wait_for_forkyard(base_url)
        records = run_arrival_process(
            lambda _i: (lambda: ForkyardBackend(base_url=base_url)),
            "forkyard", arrival_rate, arrivals, gauge,
        )
    finally:
        _terminate(process)
    return records, gauge.peak


def run_anvil_arrivals(
    rpc_url: str, block_height: int, arrival_rate: float, arrivals: Sequence[float],
    ports: PortAllocator, max_concurrent_envs: int,
) -> tuple[list[ArrivalRecord], int]:
    gauge = ConcurrencyGauge(max_concurrent_envs)
    records = run_arrival_process(
        lambda _i: (lambda p=ports.next(): AnvilBackend(p, rpc_url, block_height)),
        "anvil", arrival_rate, arrivals, gauge,
    )
    return records, gauge.peak


def arrivals_main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Measure time-to-first-simulation when agents arrive one at a time as a "
            "Poisson process of rate lambda, rather than all at once. Each arrival "
            "acquires an environment (forkyard session vs. a freshly spawned Anvil), "
            "funds a signer, sends one transfer and waits for the receipt; the reported "
            "latency runs from the scheduled arrival instant, so a backlog the backend "
            "cannot absorb appears as latency instead of disappearing into a queue."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--arrival-rates", type=parse_float_list, default=[1.0, 5.0, 20.0],
                        help="agents per second, comma-separated; one run per rate per backend")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds of arrivals per rate")
    parser.add_argument("--seed", type=int, default=0,
                        help="seeds the arrival schedule; both backends get the identical schedule")
    parser.add_argument("--block-height", type=int, default=DEFAULT_BLOCK_HEIGHT,
                        help="fork block both backends are pinned to")
    parser.add_argument("--backends", default="forkyard,anvil",
                        help="comma-separated subset to run")
    parser.add_argument("--max-concurrent-envs", type=int, default=64,
                        help="cap on environments alive at once (0 = uncapped). A machine "
                             "guard, not part of the experiment: waiting for a slot is counted "
                             "inside the arrival's latency, so past this cap a row measures the cap")
    parser.add_argument("--forkyard-port", type=int, default=18620)
    parser.add_argument("--forkyard-mcp-port", type=int, default=18621)
    parser.add_argument("--anvil-base-port", type=int, default=19400)
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"),
                        help="upstream endpoint (defaults to $RPC_URL)")
    parser.add_argument("--out", default="arrivals.csv")
    args = parser.parse_args()

    if not args.rpc_url:
        parser.error("--rpc-url is required (or set RPC_URL)")
    backends = [b.strip() for b in args.backends.split(",") if b.strip()]
    unknown = set(backends) - {"forkyard", "anvil"}
    if unknown:
        parser.error(f"unknown backend(s): {', '.join(sorted(unknown))}")

    _check_binaries_on_path()
    ports = PortAllocator(args.anvil_base_port)
    summary_csv = args.out.rsplit(".", 1)[0] + ".summary.csv"

    with open(args.out, "w", newline="") as f, open(summary_csv, "w", newline="") as sf:
        writer = csv.DictWriter(f, fieldnames=ARRIVALS_FIELDS)
        writer.writeheader()
        summary_writer = csv.DictWriter(sf, fieldnames=ARRIVALS_SUMMARY_FIELDS)
        summary_writer.writeheader()
        f.flush()
        sf.flush()

        for rate in args.arrival_rates:
            # One schedule per rate, replayed for every backend: the two are
            # then answering the same arrival process, not two samples of it.
            arrivals = poisson_arrivals(rate, args.duration, random.Random(f"{args.seed}-{rate}"))
            print(f"lambda={rate}/s over {args.duration}s -> {len(arrivals)} arrivals", file=sys.stderr)
            for backend in backends:
                if backend == "forkyard":
                    records, peak = run_forkyard_arrivals(
                        args.rpc_url, args.block_height, rate, arrivals,
                        args.forkyard_port, args.forkyard_mcp_port, args.max_concurrent_envs,
                    )
                else:
                    records, peak = run_anvil_arrivals(
                        args.rpc_url, args.block_height, rate, arrivals,
                        ports, args.max_concurrent_envs,
                    )
                # Flushed per (backend, rate) so an interrupted sweep keeps
                # everything already measured.
                writer.writerows(_arrivals_row(r) for r in records)
                f.flush()
                row = arrivals_summarize(records, backend, rate, peak)
                summary_writer.writerow(row)
                sf.flush()
                print(
                    f"  {backend}: p50={row['p50_ms']}ms p95={row['p95_ms']}ms "
                    f"p99={row['p99_ms']}ms max={row['max_ms']}ms "
                    f"completed={row['completed']} failures={row['failures']} peak_envs={peak}",
                    file=sys.stderr,
                )


# --- bench_freshness: Chain-tip freshness at fleet scale: what does it cost N agents to keep

FRESHNESS_FIELDS = [
    "backend", "agents", "agent_id", "refresh_index",
    "observed_block", "true_tip", "block_lag", "refresh_ms", "ok", "error",
]


FRESHNESS_SUMMARY_FIELDS = [
    "backend", "agents", "refreshes", "ok_refreshes",
    "lag_p50", "lag_p95", "refresh_ms_p50", "refresh_ms_p95",
    "http_requests", "jsonrpc_calls", "calls_per_agent_refresh", "upstream_errors", "top_methods",
]


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
    """Refresh instants (seconds after the phase starts), the first at t=0."""
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
    """The independent yardstick: what block the real chain is on, right now."""

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
    """Re-fork this Anvil at the chain tip."""
    w3.manager.request_blocking("anvil_reset", [{"forking": {"jsonRpcUrl": fork_url}}])


class Refresher(Protocol):
    """One agent's way of demanding current state."""

    def refresh(self) -> int: ...
    def settle(self) -> None: ...
    def close(self) -> None: ...


class ForkyardRefresher:
    """A forkyard agent refreshes by opening a NEW session."""

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
    """`AnvilBackend` forked at the tip instead of at a pinned block."""

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
            observed, ok, error = -1, False, repr(e)[:MAX_ERROR_CHARS]
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


def _freshness_row(r: RefreshRecord) -> dict[str, object]:
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


def freshness_summarize(
    records: Sequence[RefreshRecord], backend: str, agents: int, stats: ProxyStats
) -> dict[str, object]:
    """Lag statistics come from successful refreshes only — a failed one has
    no observed block, and scoring it as lag 0 or as lag ∞ would both be
    inventions. `calls_per_agent_refresh` is the number the architectures."""
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
        # than the measured phase itself at N=25. Futures are collected one
        # at a time rather than through `list(pool.map(...))`, which
        # re-raises the first failure and throws every already-built backend
        # away with it — the `finally` below then has an empty list to clean
        # up. That is not hypothetical: when one of 25 spawns timed out, the
        # other 24 Anvils stayed alive for hours and sat resident underneath
        # every benchmark that ran afterwards on the same machine.
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_agents) as pool:
            futures = [pool.submit(LatestAnvilBackend, port, rpc_url) for port in assigned]
            failure: BaseException | None = None
            for future in futures:
                try:
                    backends.append(future.result())
                except BaseException as e:  # noqa: BLE001 — re-raised once every peer is collected
                    failure = failure or e
            if failure is not None:
                raise failure
        proxy.reset()
        refreshers: list[Refresher] = [AnvilRefresher(b, rpc_url) for b in backends]
        return run_refresh_phase(refreshers, "anvil", schedule, tip)
    finally:
        for backend in backends:
            try:
                backend.discard()
            except Exception:
                pass


def freshness_main() -> None:
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
    summary_csv = args.out.rsplit(".", 1)[0] + ".summary.csv"

    proxy = CountingProxy(args.rpc_url).start()
    poller = TipPoller(args.rpc_url, args.tip_poll_secs).start()
    try:
        with open(args.out, "w", newline="") as f, open(summary_csv, "w", newline="") as sf:
            writer = csv.DictWriter(f, fieldnames=FRESHNESS_FIELDS)
            writer.writeheader()
            summary_writer = csv.DictWriter(sf, fieldnames=FRESHNESS_SUMMARY_FIELDS)
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
                    writer.writerows(_freshness_row(r) for r in records)
                    f.flush()
                    row = freshness_summarize(records, backend, num_agents, stats)
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


# --- bench_quota: How many agents each backend can keep alive under a fixed upstream quota.

FORKYARD_PORT = 18640


FORKYARD_MCP_PORT = 18641


ANVIL_BASE_PORT = 19600


PROXY_PORT = 18700


DEFAULT_THRESHOLD = 0.99


QUOTA_FIELDS = [
    "backend", "quota_rps", "num_agents", "action_success_rate", "wall_clock_ms",
    "jsonrpc_calls", "throttled_calls", "total_delay_ms", "max_sustainable_agents",
]


def action_success_rate(records: list[ActionRecord]) -> float:
    """Fraction of an agent's recorded actions that succeeded."""
    if not records:
        return 0.0
    return sum(1 for r in records if r.ok) / len(records)


def max_sustainable_agents(
    points: list[tuple[int, float]], threshold: float = DEFAULT_THRESHOLD
) -> int:
    """Largest tested agent count that met `threshold` **with every smaller
    tested count also meeting it**."""
    best = 0
    for num_agents, rate in sorted(points):
        if rate < threshold:
            break
        best = num_agents
    return best


def quota_row(
    backend: str,
    quota_rps: float,
    num_agents: int,
    success_rate: float,
    wall_clock_ms: float,
    stats,
    max_sustainable: int,
) -> dict[str, object]:
    return {
        "backend": backend,
        "quota_rps": quota_rps,
        "num_agents": num_agents,
        "action_success_rate": round(success_rate, 4),
        "wall_clock_ms": round(wall_clock_ms, 1),
        "jsonrpc_calls": stats.jsonrpc_calls,
        "throttled_calls": stats.throttled_calls,
        "total_delay_ms": round(stats.total_delay_ms, 1),
        "max_sustainable_agents": max_sustainable,
    }


def _run_point(
    backend: str, rpc_url: str, block_height: int, num_agents: int,
    actions_per_agent: int, episodes: int,
) -> tuple[list[ActionRecord], float]:
    """One (backend, agent count) point, through the already-throttled
    proxy URL. The workload itself is `run_benchmark`'s — reimplementing it
    here would let the two benchmarks drift apart and make their numbers
    incomparable, which is the only reason either is interesting."""
    if backend == "forkyard":
        return run_forkyard_sweep(
            rpc_url, block_height, num_agents, actions_per_agent,
            FORKYARD_PORT, FORKYARD_MCP_PORT, episodes,
        )
    return run_anvil_sweep(
        rpc_url, block_height, num_agents, actions_per_agent, ANVIL_BASE_PORT, episodes,
    )


def quota_main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Measure how many concurrent agents each backend sustains under a "
            "capped upstream RPC rate. Both backends run the same workload "
            "through one rate-limited counting proxy; for every (backend, "
            "quota) the sweep reports the largest agent count still completing "
            f"at least {DEFAULT_THRESHOLD:.0%} of its actions, plus the success "
            "rate, wall-clock and throttling at each point."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "The quota is in JSON-RPC calls per second and a batch of M calls "
            "costs M — that is how providers meter. --limit-mode delay models a "
            "provider that queues you (excess volume becomes latency); "
            "--limit-mode reject models one that answers -32005 (excess volume "
            "becomes failed actions). Anvil runs with its cross-process fork "
            "cache disabled, as everywhere else in this harness, so a run does "
            "not measure the machine's benchmark history."
        ),
    )
    parser.add_argument(
        "--quotas", type=parse_int_list, default=[10, 25, 100],
        help="upstream budgets to test, in JSON-RPC calls/sec (default 10,25,100)",
    )
    parser.add_argument(
        "--agents", type=parse_int_list, default=[5, 10, 25, 50],
        help="concurrent agent counts to test at each quota (default 5,10,25,50)",
    )
    parser.add_argument("--block-height", type=int, default=25795072)
    parser.add_argument("--actions-per-agent", type=int, default=5)
    parser.add_argument("--episodes", type=int, default=1)
    parser.add_argument(
        "--threshold", type=float, default=DEFAULT_THRESHOLD,
        help=f"action success rate a point must reach to count as sustained "
             f"(default {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--limit-mode", choices=["delay", "reject"], default="delay",
        help="how the proxy enforces the quota (default delay). 'delay' shows up "
             "as wall-clock and total_delay_ms; 'reject' shows up as a collapsing "
             "action_success_rate.",
    )
    parser.add_argument(
        "--burst", type=float, default=None,
        help="token bucket capacity in calls (default: one second of the quota)",
    )
    parser.add_argument(
        "--full-curve", action="store_true",
        help="keep testing larger agent counts after one has already failed the "
             "threshold. Off by default because every extra point costs a full "
             "sweep and cannot raise max_sustainable_agents — turn it on to show "
             "the whole collapse, not just where it starts.",
    )
    parser.add_argument(
        "--settle-s", type=float, default=1.0,
        help="pause between points so the token bucket refills (default 1.0). "
             "Without it a backend would inherit the queue the previous one left "
             "behind, and whichever ran second would look worse than it is.",
    )
    parser.add_argument("--rpc-url", required=True)
    parser.add_argument("--out", default="quota.csv")
    args = parser.parse_args()

    _check_binaries_on_path()

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=QUOTA_FIELDS)
        writer.writeheader()
        f.flush()

        for quota in args.quotas:
            # One proxy per quota, on a fixed port, torn down before the
            # next: the bucket is the provider, and a provider does not
            # change its plan halfway through.
            proxy = CountingProxy(
                args.rpc_url, PROXY_PORT,
                rate_limit_rps=quota, burst=args.burst, limit_mode=args.limit_mode,
            ).start()
            try:
                for backend in ("forkyard", "anvil"):
                    points: list[tuple[int, float]] = []
                    pending: list[dict[str, object]] = []
                    for num_agents in sorted(args.agents):
                        if args.settle_s:
                            time.sleep(args.settle_s)
                        proxy.reset()
                        print(
                            f"quota={quota}/s {backend}: {num_agents} agents "
                            f"({args.actions_per_agent} actions, {args.episodes} episodes)",
                            file=sys.stderr,
                        )
                        records, total_ms = _run_point(
                            backend, proxy.url, args.block_height, num_agents,
                            args.actions_per_agent, args.episodes,
                        )
                        stats = proxy.snapshot()
                        rate = action_success_rate(records)
                        points.append((num_agents, rate))
                        pending.append(
                            quota_row(backend, quota, num_agents, rate, total_ms, stats, -1)
                        )
                        print(
                            f"  success={rate:.1%} wall={total_ms:.0f}ms "
                            f"calls={stats.jsonrpc_calls} throttled={stats.throttled_calls} "
                            f"delay={stats.total_delay_ms:.0f}ms",
                            file=sys.stderr,
                        )
                        if rate < args.threshold and not args.full_curve:
                            print(
                                f"  below {args.threshold:.0%}: stopping this "
                                f"({backend}, {quota}/s) curve here",
                                file=sys.stderr,
                            )
                            break

                    # max_sustainable_agents is a property of the whole
                    # curve, so the rows can only be written once the curve
                    # is done. Per (backend, quota) is still fine-grained
                    # enough that an interrupted sweep keeps what it has.
                    sustained = max_sustainable_agents(points, args.threshold)
                    for row in pending:
                        row["max_sustainable_agents"] = sustained
                        writer.writerow(row)
                    f.flush()
                    print(
                        f"  -> {backend} sustains {sustained} agents at {quota} calls/s",
                        file=sys.stderr,
                    )
            finally:
                proxy.stop()
