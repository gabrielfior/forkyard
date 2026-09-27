"""Latency benchmarks: how fast a forkyard process is ready to serve, and
what reopening a saved session costs next to rebuilding it."""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import requests

from actions import read_contract
from backend import ForkyardBackend
from bench_architecture import MARKER_ACCOUNT, read_marker, run_prefix
from bench_common import DEFAULT_BLOCK_HEIGHT, MAX_ERROR_CHARS, forkyard_process, parse_int_list
from contracts import GET_RESERVES_SELECTOR, fetch_pair_addresses
from rpc_proxy import CountingProxy
from run_benchmark import _terminate


# --- bench_startup: process spawn -> first session -> first warm read.

STARTUP_FIELDS = ["label", "run", "condition", "ready_ms", "first_read_ms", "upstream_calls", "ok", "error"]


STARTUP_FORKYARD_PORT = 18690


STARTUP_FORKYARD_MCP_PORT = 18691


# Tighter than `_wait_for_forkyard`'s 200ms, which would round every warm
# start up to the poll interval and hide the thing being measured.
READY_POLL_S = 0.002


@dataclass
class StartupRow:
    label: str
    run: int
    condition: str
    ready_ms: float
    first_read_ms: float
    upstream_calls: int
    ok: bool
    error: str = ""


def wait_ready(base_url: str, process: subprocess.Popen, timeout_s: float = 60.0) -> str:
    """Poll `POST /session` until it opens one; returns that session's URL."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"forkyard exited with {process.returncode} before it was ready")
        try:
            resp = requests.post(f"{base_url}/session", timeout=1)
            if resp.ok and "session_id" in resp.json():
                return f"{base_url}/session/{resp.json()['session_id']}"
        except requests.RequestException:
            pass
        time.sleep(READY_POLL_S)
    raise RuntimeError(f"forkyard on {base_url} was not ready within {timeout_s}s")


def run_startup(
    binary: str, rpc_url: str, block_height: int, cache_dir: Path, contracts: list[str],
    proxy: CountingProxy, label: str, run: int, condition: str,
) -> StartupRow:
    """One process: time to a usable session, then to one warm contract
    read, counting every upstream call the whole start made."""
    env = {
        **os.environ,
        "RPC_URL": proxy.url,
        "FORKYARD_PORT": str(STARTUP_FORKYARD_PORT),
        "FORKYARD_MCP_HTTP_PORT": str(STARTUP_FORKYARD_MCP_PORT),
        "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
        "FORKYARD_CACHE_DIR": str(cache_dir),
        "RUST_LOG": "warn",
    }
    env.pop("FORKYARD_CACHE_DISABLED", None)
    base_url = f"http://127.0.0.1:{STARTUP_FORKYARD_PORT}"
    proxy.reset()
    start = time.monotonic()
    process = subprocess.Popen([binary], env=env, stdin=subprocess.DEVNULL)
    try:
        session_url = wait_ready(base_url, process)
        ready_ms = (time.monotonic() - start) * 1000
        backend = ForkyardBackend(session_url=session_url)
        _, _, ok, error = read_contract(backend, contracts[0], GET_RESERVES_SELECTOR)
        first_read_ms = (time.monotonic() - start) * 1000
        # The rest of the contracts, untimed: the warm-up run is what fills
        # the cache every later run starts from.
        for address in contracts[1:]:
            read_contract(backend, address, GET_RESERVES_SELECTOR)
        calls = proxy.snapshot().jsonrpc_calls
        return StartupRow(label, run, condition, round(ready_ms, 1), round(first_read_ms, 1), calls, ok, error[:MAX_ERROR_CHARS])
    finally:
        # SIGTERM: forkyard writes its cache on this path, which is what
        # makes the next run warm.
        _terminate(process)


def startup_main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Time a forkyard process from spawn to its first usable session and "
            "its first contract read, at a pinned block, cold once and then warm, "
            "counting upstream calls through a proxy. Pass --binary to compare two builds."
        ),
    )
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"))
    parser.add_argument("--block-height", type=int, default=DEFAULT_BLOCK_HEIGHT)
    parser.add_argument("--binary", default="forkyard", help="forkyard executable to launch (default: on PATH)")
    parser.add_argument("--label", default=None, help="name for this build in the CSV (default: --binary)")
    parser.add_argument("--runs", type=int, default=5, help="warm starts after the one cold start")
    parser.add_argument("--contracts", type=int, default=4)
    parser.add_argument("--out", default="startup.csv")
    args = parser.parse_args()
    if not args.rpc_url:
        parser.error("--rpc-url is required (or set RPC_URL)")
    label = args.label or args.binary

    contracts = fetch_pair_addresses(args.rpc_url, args.block_height, args.contracts)
    cache_dir = Path(tempfile.mkdtemp(prefix="forkyard-startup-"))
    rows: list[StartupRow] = []
    proxy = CountingProxy(args.rpc_url).start()
    try:
        for run in range(args.runs + 1):
            condition = "cold" if run == 0 else "warm"
            row = run_startup(args.binary, args.rpc_url, args.block_height, cache_dir, contracts,
                              proxy, label, run, condition)
            rows.append(row)
            print(f"{label} {condition} #{run}: ready {row.ready_ms:.1f} ms, first read "
                  f"{row.first_read_ms:.1f} ms, {row.upstream_calls} upstream calls", file=sys.stderr)
    finally:
        proxy.stop()
        shutil.rmtree(cache_dir, ignore_errors=True)

    warm = [r for r in rows if r.condition == "warm"]
    if warm:
        print(f"{label} warm median: ready {statistics.median(r.ready_ms for r in warm):.1f} ms, "
              f"first read {statistics.median(r.first_read_ms for r in warm):.1f} ms, "
              f"upstream calls {statistics.median(r.upstream_calls for r in warm):.0f}", file=sys.stderr)
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=STARTUP_FIELDS)
        writer.writeheader()
        writer.writerows(asdict(r) for r in rows)


# --- bench_resume: reopen a saved session vs rebuild it by replaying.

RESUME_FIELDS = ["prefix_actions", "repeat", "operation", "elapsed_ms", "snapshot_bytes", "ok", "error"]


RESUME_FORKYARD_PORT = 18692


RESUME_FORKYARD_MCP_PORT = 18693


DEFAULT_PREFIX_ACTIONS = [5, 20, 50]


@dataclass
class ResumeRow:
    prefix_actions: int
    repeat: int
    operation: str
    elapsed_ms: float
    snapshot_bytes: int
    ok: bool
    error: str = ""


def marker_for(repeat: int) -> int:
    """A distinct marker per repeat, so each snapshot is new state — the
    store is content-addressed, and an identical snapshot skips its write."""
    return 7 * 10**18 + repeat + 1


def build_session(base_url: str, prefix_actions: int, repeat: int) -> tuple[ForkyardBackend, float, bool]:
    backend = ForkyardBackend(base_url=base_url)
    start = time.monotonic()
    results, _ = run_prefix(backend, prefix_actions)
    backend.set_native_balance(MARKER_ACCOUNT, marker_for(repeat))
    elapsed_ms = (time.monotonic() - start) * 1000
    return backend, elapsed_ms, all(ok for _, _, ok, _ in results)


def resume_session(base_url: str, snapshot_id: str) -> str:
    resp = requests.post(f"{base_url}/session", json={"snapshot_id": snapshot_id}, timeout=30)
    resp.raise_for_status()
    body = resp.json()
    if "session_id" not in body:
        raise RuntimeError(f"resume failed: {body}")
    return f"{base_url}/session/{body['session_id']}"


def timed(fn):
    start = time.monotonic()
    value = fn()
    return value, (time.monotonic() - start) * 1000


def resume_main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Build a session with N real actions (funding, approvals, Uniswap swaps, "
            "transfers), snapshot it, then time three ways back to that state: "
            "replaying the N actions on a fresh session, resuming the snapshot in "
            "the same process, and resuming it after a restart. Every resumed "
            "session's marker balance is checked, so a fast wrong answer fails."
        ),
    )
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"))
    parser.add_argument("--block-height", type=int, default=DEFAULT_BLOCK_HEIGHT)
    parser.add_argument("--prefix-actions", type=parse_int_list, default=DEFAULT_PREFIX_ACTIONS)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--out", default="resume.csv")
    args = parser.parse_args()
    if not args.rpc_url:
        parser.error("--rpc-url is required (or set RPC_URL)")

    snapshot_dir = Path(tempfile.mkdtemp(prefix="forkyard-resume-snapshots-"))
    cache_dir = Path(tempfile.mkdtemp(prefix="forkyard-resume-cache-"))
    extra_env = {"FORKYARD_SNAPSHOT_DIR": str(snapshot_dir), "FORKYARD_CACHE_DIR": str(cache_dir)}
    rows: list[ResumeRow] = []

    def record(size: int, repeat: int, operation: str, elapsed_ms: float, nbytes: int = 0,
               ok: bool = True, error: str = "") -> None:
        rows.append(ResumeRow(size, repeat, operation, round(elapsed_ms, 2), nbytes, ok, error[:MAX_ERROR_CHARS]))
        print(f"N={size} #{repeat} {operation}: {elapsed_ms:.2f} ms{' FAILED ' + error if not ok else ''}",
              file=sys.stderr)

    try:
        pending: list[tuple[int, int, str]] = []
        with forkyard_process(args.rpc_url, RESUME_FORKYARD_PORT, RESUME_FORKYARD_MCP_PORT,
                              args.block_height, extra_env) as base_url:
            # One discarded build warms the shared cache, so replay is timed
            # warm — the fairest case for it.
            build_session(base_url, max(args.prefix_actions), -1)[0].discard()
            for size in args.prefix_actions:
                for repeat in range(args.repeats):
                    original, build_ms, build_ok = build_session(base_url, size, repeat)
                    record(size, repeat, "replay", build_ms, ok=build_ok)

                    info, snap_ms = timed(lambda: original.web3().manager.request_blocking("forkyard_snapshot", []))
                    record(size, repeat, "snapshot", snap_ms, info["bytes"])

                    session_url, resume_ms = timed(lambda: resume_session(base_url, info["snapshot_id"]))
                    marker = read_marker(ForkyardBackend(session_url=session_url))
                    record(size, repeat, "resume", resume_ms, info["bytes"], marker == marker_for(repeat),
                           "" if marker == marker_for(repeat) else f"marker {marker}")
                    original.discard()
                    pending.append((size, repeat, info["snapshot_id"]))

        # A new process: nothing of the old one survives but the files.
        with forkyard_process(args.rpc_url, RESUME_FORKYARD_PORT, RESUME_FORKYARD_MCP_PORT,
                              args.block_height, extra_env) as base_url:
            for size, repeat, snapshot_id in pending:
                session_url, resume_ms = timed(lambda: resume_session(base_url, snapshot_id))
                marker = read_marker(ForkyardBackend(session_url=session_url))
                record(size, repeat, "resume_after_restart", resume_ms, 0, marker == marker_for(repeat),
                       "" if marker == marker_for(repeat) else f"marker {marker}")
    finally:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
        shutil.rmtree(cache_dir, ignore_errors=True)

    for size in args.prefix_actions:
        summary = []
        for operation in ("replay", "snapshot", "resume", "resume_after_restart"):
            times = [r.elapsed_ms for r in rows if r.prefix_actions == size and r.operation == operation]
            if times:
                summary.append(f"{operation} {statistics.median(times):.1f} ms")
        print(f"N={size} median: " + ", ".join(summary), file=sys.stderr)
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESUME_FIELDS)
        writer.writeheader()
        writer.writerows(asdict(r) for r in rows)
