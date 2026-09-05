"""How many *isolated concurrent writers* fit in a gigabyte.

The claim: Anvil's unit of isolation is an OS process. Every writer that
needs its own view of state costs a whole EVM process (tens of MB of RSS
before it has done anything), so the ceiling on concurrent writers is set
by memory. forkyard's unit of isolation is a session inside one process
sharing one base cache, so the same ceiling should sit orders of
magnitude higher.

"Isolated" is not taken on trust here. Every writer writes a value only it
uses — to the *same* account and the *same* storage slot as every other
writer — and then reads it back. If any writer ever reads a value that is
not its own, the environments are leaking into each other and the memory
number is meaningless; that count is `isolation_violations` and it must be
0 for a row to mean anything.

Two things to know before reading the numbers:

  * The read-back assertion is on `eth_getBalance`. forkyard's per-session
    RPC has no `eth_getStorageAt`, so the shared-slot write is part of the
    write load but cannot itself be read back — the balance is the channel
    the isolation check actually travels over.
  * `wall_clock_ms` (and so `writes_per_sec`) includes each writer
    *acquiring* its environment — a forkyard session open, or an Anvil
    spawn plus wait-until-ready. That is the same timed region
    run_benchmark.py uses, and it is most of Anvil's number at low round
    counts. Raise `--rounds` to push the mix toward steady-state writes.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Callable, IO

from web3 import Web3

from backend import AnvilBackend, Backend, ForkyardBackend
from run_benchmark import _terminate, _wait_for_forkyard, parse_int_list

FIELDS = [
    "backend", "writers", "peak_rss_mb", "wall_clock_ms",
    "writes_per_sec", "writers_per_gb", "isolation_violations", "ok",
]

DEFAULT_BLOCK_HEIGHT = 25_795_072
DEFAULT_WRITERS = [1, 5, 10, 25, 50]

FORKYARD_PORT = 18610
FORKYARD_MCP_PORT = 18611
ANVIL_BASE_PORT = 19300

# Every writer aims at these two, so a leak between environments shows up
# as one writer reading another's value rather than being hidden by each
# writer owning its own key.
SHARED_ACCOUNT = Web3.to_checksum_address("0x00000000000000000000000000000000deadbeef")
SHARED_CONTRACT = Web3.to_checksum_address("0x6B175474E89094C44Da98b954EedeAC495271d0F")  # DAI
SHARED_SLOT = "0x" + (7).to_bytes(32, "big").hex()

_MAX_ERROR_CHARS = 200


@dataclass
class WriterOutcome:
    writer_index: int
    writes: int
    violations: int
    ok: bool
    error: str = ""


@dataclass
class SweepResult:
    backend: str
    writers: int
    peak_rss_mb: float
    wall_clock_ms: float
    writes_per_sec: float
    writers_per_gb: float
    isolation_violations: int
    ok: bool


def _row(r: SweepResult) -> dict[str, object]:
    return {
        "backend": r.backend,
        "writers": r.writers,
        "peak_rss_mb": r.peak_rss_mb,
        "wall_clock_ms": r.wall_clock_ms,
        "writes_per_sec": r.writes_per_sec,
        "writers_per_gb": r.writers_per_gb,
        "isolation_violations": r.isolation_violations,
        "ok": r.ok,
    }


def writer_value(writer_index: int, round_index: int) -> int:
    """Unique per (writer, round), so the read-back catches both kinds of
    leak: seeing a *different writer's* value, and seeing a stale value of
    one's own from an earlier round."""
    return 10**18 + writer_index * 10**12 + round_index


def _value_word(value: int) -> str:
    return "0x" + value.to_bytes(32, "big").hex()


def run_writer(make_backend: Callable[[], Backend], writer_index: int, rounds: int) -> WriterOutcome:
    """One writer's whole life: acquire an environment, write-and-verify
    `rounds` times, discard. Exceptions are captured rather than raised so
    one writer failing at K=50 still leaves the other 49 measurable."""
    writes = 0
    violations = 0
    try:
        backend = make_backend()
    except Exception as e:
        return WriterOutcome(writer_index, 0, 0, False, repr(e)[:_MAX_ERROR_CHARS])
    try:
        for round_index in range(rounds):
            value = writer_value(writer_index, round_index)
            backend.set_native_balance(SHARED_ACCOUNT, value)
            writes += 1
            backend.set_storage(SHARED_CONTRACT, SHARED_SLOT, _value_word(value))
            writes += 1
            if backend.web3().eth.get_balance(SHARED_ACCOUNT) != value:
                violations += 1
        return WriterOutcome(writer_index, writes, violations, True, "")
    except Exception as e:
        return WriterOutcome(writer_index, writes, violations, False, repr(e)[:_MAX_ERROR_CHARS])
    finally:
        try:
            backend.discard()
        except Exception:
            # A failed teardown is not a failed measurement; the writes and
            # the isolation check already happened. Anvil's discard kills a
            # process that may already be gone.
            pass


def process_pids(process_name: str) -> set[int]:
    try:
        out = subprocess.run(["pgrep", "-x", process_name], capture_output=True, text=True)
    except FileNotFoundError:
        return set()
    return {int(line) for line in out.stdout.split() if line.strip().isdigit()}


def total_rss_mb(pids: set[int]) -> float:
    """Summed resident set size of `pids`, in MB. `ps -o rss=` reports KiB
    on both macOS and Linux; a pid that died between the scan and here just
    prints nothing, which is what we want mid-sweep."""
    if not pids:
        return 0.0
    out = subprocess.run(
        ["ps", "-o", "rss=", "-p", ",".join(str(p) for p in sorted(pids))],
        capture_output=True, text=True,
    )
    return sum(int(line) for line in out.stdout.split() if line.strip().isdigit()) / 1024


class RssSampler:
    """Polls the RSS of every process named `process_name` that we started,
    and keeps the peak.

    Pids are re-scanned every tick because Anvil's processes appear over the
    course of the sweep, and `exclude_pids` is captured *before* the sweep so
    an unrelated forkyard or anvil the developer already had running is not
    charged to the measurement."""

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

    def start(self) -> RssSampler:
        self.sample_once()  # never return 0 for a sweep shorter than one tick
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> float:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        return self.peak_mb


def summarize(
    backend: str, writers: int, outcomes: list[WriterOutcome],
    peak_rss_mb: float, wall_clock_ms: float,
) -> SweepResult:
    """Pure reduction of one sweep's writers into the CSV row.

    `writers_per_gb` extrapolates the measured cost per writer to a
    gigabyte — the headline number, and the one that should differ by
    orders of magnitude. It is 0.0 when RSS could not be sampled, since
    dividing by an unknown would fabricate an answer."""
    writes = sum(o.writes for o in outcomes)
    violations = sum(o.violations for o in outcomes)
    writes_per_sec = writes / (wall_clock_ms / 1000) if wall_clock_ms > 0 else 0.0
    writers_per_gb = writers * 1024 / peak_rss_mb if peak_rss_mb > 0 else 0.0
    return SweepResult(
        backend=backend,
        writers=writers,
        peak_rss_mb=round(peak_rss_mb, 1),
        wall_clock_ms=round(wall_clock_ms, 1),
        writes_per_sec=round(writes_per_sec, 1),
        writers_per_gb=round(writers_per_gb, 1),
        isolation_violations=violations,
        # A sweep with a leak is not a passing sweep, however fast it was.
        ok=violations == 0 and all(o.ok for o in outcomes) and len(outcomes) == writers,
    )


def run_writers(
    make_backend_for_writer: Callable[[int], Callable[[], Backend]],
    writers: int, rounds: int,
) -> tuple[list[WriterOutcome], float]:
    """Run `writers` writers concurrently, returning their outcomes and the
    wall clock they took together — acquisition included, matching
    run_benchmark.py's timed region."""
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=writers) as pool:
        futures = [
            pool.submit(lambda i=i: run_writer(make_backend_for_writer(i), i, rounds))
            for i in range(writers)
        ]
        outcomes = [f.result() for f in futures]
    return outcomes, (time.monotonic() - start) * 1000


def sweep_forkyard(
    rpc_url: str, block_height: int, writers: int, rounds: int, sample_interval_s: float,
) -> SweepResult:
    """K sessions in one process. The forkyard process is started fresh per
    K so its RSS reflects only this K's sessions, and the process start
    itself sits outside the timed region (it is a single shared cost with
    no Anvil counterpart — the same exclusion run_benchmark.py makes)."""
    pre_existing = process_pids("forkyard")
    process = subprocess.Popen(
        ["forkyard"],
        env={
            **os.environ,
            "RPC_URL": rpc_url,
            "FORKYARD_PORT": str(FORKYARD_PORT),
            "FORKYARD_MCP_HTTP_PORT": str(FORKYARD_MCP_PORT),
            "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
        },
    )
    base_url = f"http://127.0.0.1:{FORKYARD_PORT}"
    try:
        _wait_for_forkyard(base_url)
        sampler = RssSampler("forkyard", pre_existing, sample_interval_s).start()
        try:
            outcomes, wall_ms = run_writers(
                lambda i: (lambda: ForkyardBackend(base_url=base_url)), writers, rounds
            )
        finally:
            peak = sampler.stop()
        return summarize("forkyard", writers, outcomes, peak, wall_ms)
    finally:
        _terminate(process)


def sweep_anvil(
    rpc_url: str, block_height: int, writers: int, rounds: int, base_port: int,
    sample_interval_s: float, startup_timeout_s: float,
) -> SweepResult:
    """K processes. Every writer's `discard()` kills its own Anvil, so the
    sampler has to be running for the whole concurrent region to catch the
    moment they all coexist."""
    pre_existing = process_pids("anvil")
    sampler = RssSampler("anvil", pre_existing, sample_interval_s).start()
    try:
        outcomes, wall_ms = run_writers(
            lambda i: (lambda: AnvilBackend(
                base_port + i, rpc_url, block_height, startup_timeout_s=startup_timeout_s
            )),
            writers, rounds,
        )
    finally:
        peak = sampler.stop()
    return summarize("anvil", writers, outcomes, peak, wall_ms)


def write_results(out: IO[str], results: list[SweepResult]) -> None:
    writer = csv.DictWriter(out, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(_row(r) for r in results)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Measure how many isolated concurrent writers each architecture "
            "fits in a gigabyte. K writers each set a value only they use on "
            "one shared account and one shared storage slot, then read it "
            "back and assert they see their own — forkyard as K sessions in "
            "one process, Anvil as K processes. Reports peak RSS, wall clock, "
            "writes/sec, extrapolated writers per GB, and the isolation "
            "violation count, which must be 0 for a row to mean anything."
        ),
    )
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"),
                        help="archive endpoint to fork from (default: $RPC_URL)")
    parser.add_argument("--block-height", type=int, default=DEFAULT_BLOCK_HEIGHT,
                        help=f"fork block for both backends (default: {DEFAULT_BLOCK_HEIGHT})")
    parser.add_argument("--writers", type=parse_int_list, default=DEFAULT_WRITERS,
                        help="comma-separated concurrent-writer counts to sweep "
                             f"(default: {','.join(str(x) for x in DEFAULT_WRITERS)})")
    parser.add_argument("--rounds", type=int, default=10,
                        help="write-and-verify cycles per writer; each cycle is two writes "
                             "(balance + storage) and one read-back (default: 10)")
    parser.add_argument("--sample-interval", type=float, default=0.1,
                        help="RSS sampling period in seconds (default: 0.1)")
    parser.add_argument("--anvil-startup-timeout", type=float, default=60.0,
                        help="seconds to wait for each Anvil to answer. Higher than the harness "
                             "default because 50 forking Anvils starting at once are slower than "
                             "one (default: 60)")
    parser.add_argument("--backends", default="forkyard,anvil",
                        help="comma-separated subset to measure (default: both)")
    parser.add_argument("--out", default="writers.csv")
    args = parser.parse_args()

    if not args.rpc_url:
        parser.error("--rpc-url is required (or set RPC_URL)")
    backends = [b.strip() for b in args.backends.split(",") if b.strip()]

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        f.flush()

        next_anvil_port = ANVIL_BASE_PORT
        for writers in args.writers:
            if "forkyard" in backends:
                print(f"forkyard: {writers} sessions x {args.rounds} rounds", file=sys.stderr)
                result = sweep_forkyard(
                    args.rpc_url, args.block_height, writers, args.rounds, args.sample_interval
                )
                writer.writerow(_row(result))
                f.flush()
                print(f"  peak {result.peak_rss_mb} MB, {result.writers_per_gb} writers/GB, "
                      f"{result.isolation_violations} violations", file=sys.stderr)
                # The next sweep rebinds the same fixed port; give the closed
                # listener a moment rather than racing TIME_WAIT.
                time.sleep(1.0)
            if "anvil" in backends:
                print(f"anvil: {writers} processes x {args.rounds} rounds", file=sys.stderr)
                result = sweep_anvil(
                    args.rpc_url, args.block_height, writers, args.rounds, next_anvil_port,
                    args.sample_interval, args.anvil_startup_timeout,
                )
                # Ports are never reused across sweeps: a killed Anvil's port
                # lingers in TIME_WAIT and would fail the next bind.
                next_anvil_port += writers
                writer.writerow(_row(result))
                f.flush()
                print(f"  peak {result.peak_rss_mb} MB, {result.writers_per_gb} writers/GB, "
                      f"{result.isolation_violations} violations", file=sys.stderr)


if __name__ == "__main__":
    main()
