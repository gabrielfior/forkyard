"""Shared plumbing for the bench_* benchmarks: process lifecycle, port
allocation, RSS sampling, percentiles and CSV output."""

from __future__ import annotations

import concurrent.futures
import contextlib
import csv
import math
import os
import subprocess
import threading
from collections.abc import Iterator, Sequence
from typing import Callable, TypeVar

from run_benchmark import _terminate, _wait_for_forkyard

DEFAULT_BLOCK_HEIGHT = 25_795_072
MAX_ERROR_CHARS = 200

T = TypeVar("T")


def truncate_error(exc: BaseException) -> str:
    return repr(exc)[:MAX_ERROR_CHARS]


def parse_int_list(s: str) -> list[int]:
    return [int(x) for x in s.split(",")]


def parse_float_list(s: str) -> list[float]:
    return [float(x) for x in s.split(",")]


class PortAllocator:
    """Hands out ports no run has used before.

    Never reuse one inside a run: a killed Anvil leaves its socket in
    TIME_WAIT for minutes, and the next Anvil either fails to bind or — worse
    — a client reaches the corpse and the measurement is silently empty.
    """

    def __init__(self, base_port: int = 19000):
        self._next = base_port
        self._lock = threading.Lock()

    def take(self, count: int = 1) -> list[int]:
        with self._lock:
            ports = list(range(self._next, self._next + count))
            self._next += count
        return ports

    def next(self) -> int:
        return self.take(1)[0]


@contextlib.contextmanager
def forkyard_process(
    rpc_url: str,
    port: int,
    mcp_port: int,
    block_height: int | None = None,
    extra_env: dict[str, str] | None = None,
    ready_timeout_s: float = 20.0,
) -> Iterator[str]:
    """Run one forkyard for the duration of the block, yielding its base URL.

    Always SIGTERMs rather than kills: forkyard writes its persistent cache
    on that path.
    """
    env = {**os.environ, "RPC_URL": rpc_url, "FORKYARD_PORT": str(port),
           "FORKYARD_MCP_HTTP_PORT": str(mcp_port)}
    if block_height is not None:
        env["FORKYARD_FORK_BLOCK_NUMBER"] = str(block_height)
    env.update(extra_env or {})
    try:
        process = subprocess.Popen(["forkyard"], env=env)
    except FileNotFoundError as e:
        raise RuntimeError(
            "the `forkyard` binary was not found on PATH — build it with "
            "`cargo build -p forkyard --release` and add target/release to PATH"
        ) from e
    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_forkyard(base_url, ready_timeout_s)
        yield base_url
    finally:
        _terminate(process)


def process_pids(process_name: str) -> set[int]:
    try:
        out = subprocess.run(["pgrep", "-x", process_name], capture_output=True, text=True)
    except FileNotFoundError:
        return set()
    return {int(line) for line in out.stdout.split() if line.strip().isdigit()}


def total_rss_mb(pids: set[int]) -> float:
    """`ps -o rss=` reports KiB on macOS and Linux alike; a pid that died
    between the scan and here prints nothing, which is what we want."""
    if not pids:
        return 0.0
    out = subprocess.run(
        ["ps", "-o", "rss=", "-p", ",".join(str(p) for p in sorted(pids))],
        capture_output=True, text=True,
    )
    return sum(int(line) for line in out.stdout.split() if line.strip().isdigit()) / 1024


class RssSampler:
    """Peak RSS of every process named `process_name` that this run started.

    Pids are re-scanned each tick because Anvils appear as the sweep goes,
    and `exclude_pids` is captured beforehand so a forkyard the developer
    already had running is not charged to the measurement.
    """

    def __init__(self, process_name: str, exclude_pids: set[int], interval_s: float = 0.1):
        self._process_name = process_name
        self._exclude = set(exclude_pids)
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.peak_mb = 0.0

    def sample_once(self) -> float:
        mb = total_rss_mb(process_pids(self._process_name) - self._exclude)
        self.peak_mb = max(self.peak_mb, mb)
        return mb

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.sample_once()
            self._stop.wait(self._interval_s)

    def start(self) -> "RssSampler":
        self.sample_once()  # never report 0 for a sweep shorter than one tick
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> float:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        return self.peak_mb


def percentile(values: Sequence[float], q: float) -> float:
    """Linear-interpolated percentile, `q` in [0, 100]. Hand-rolled because
    it is the number the load benchmarks rest on, and a tested pure function
    is cheaper to trust than an argument about interpolation modes."""
    if not values:
        raise ValueError("percentile of an empty sequence")
    if not 0 <= q <= 100:
        raise ValueError(f"percentile must be in [0, 100], got {q}")
    ordered = sorted(values)
    pos = (len(ordered) - 1) * (q / 100)
    lo, hi = math.floor(pos), math.ceil(pos)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (pos - lo)


def run_concurrently(tasks: Sequence[Callable[[], T]]) -> list[T]:
    """Run every task, collecting results one future at a time.

    Not `pool.map`: it re-raises the first failure and discards the results
    that succeeded alongside it, which once left 24 live Anvils behind.
    """
    if not tasks:
        return []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = [pool.submit(task) for task in tasks]
        results: list[T] = []
        failure: BaseException | None = None
        for future in futures:
            try:
                results.append(future.result())
            except BaseException as e:  # noqa: BLE001 — re-raised once peers are collected
                failure = failure or e
        if failure is not None:
            raise failure
        return results


def write_csv(path: str, fields: Sequence[str], rows: Sequence[dict[str, object]]) -> None:
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fields))
        writer.writeheader()
        writer.writerows(rows)


def summary_path(out_path: str, suffix: str = "summary") -> str:
    base, _, ext = out_path.rpartition(".")
    return f"{base}.{suffix}.{ext}" if base else f"{out_path}.{suffix}"
