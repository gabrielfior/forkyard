"""What a *restart* costs, now that both backends can persist a fetch cache.

This is the one axis where Anvil was measurably ahead. Foundry writes
fetched fork state to `~/.foundry/cache/rpc/<chain>/<block>/storage.json`,
and every later anvil/forge/cast process at that block reads it — measured
here at 778 upstream calls cold against 30 warm. forkyard's cache used to
die with the process. It no longer does (`FORKYARD_CACHE_DIR`, written on
the SIGTERM path), so this script measures the same thing on both sides
instead of disabling Anvil's.

Four conditions, one workload:

    forkyard cold -> forkyard warm      (its cache dir cleared, then reused)
    anvil    cold -> anvil    warm      (Foundry's cache cleared, then reused)

Two things that would quietly invalidate the result, both handled here:

  * forkyard only writes its cache on the SIGTERM path, so the cold run has
    to be stopped politely. `_terminate` does that (SIGTERM first, SIGKILL
    only if it hangs) — a straight kill would leave the warm run cold and
    the finding backwards.
  * Both caches persist *between invocations of this script*, keyed by
    block. Every cold run therefore clears its own cache first; without
    that, the second time anyone runs this file every row is warm.
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from actions import read_contract
from backend import AnvilBackend, ForkyardBackend
from contracts import GET_RESERVES_SELECTOR, fetch_pair_addresses
from rpc_proxy import CountingProxy
from run_benchmark import _terminate, _wait_for_forkyard

FIELDS = [
    "backend", "condition", "agents", "contracts",
    "jsonrpc_calls", "http_requests", "wall_clock_ms", "ok", "error",
]

DEFAULT_BLOCK_HEIGHT = 25_795_072
FORKYARD_PORT = 18670
FORKYARD_MCP_PORT = 18671
ANVIL_BASE_PORT = 23000


def foundry_cache_dir(block_height: int, chain: str = "mainnet") -> Path:
    """Where Foundry keeps the cache this benchmark clears. It is written
    per resolved block, so clearing one block leaves the rest of the
    machine's Foundry cache — possibly the user's real work — alone."""
    return Path.home() / ".foundry" / "cache" / "rpc" / chain / str(block_height)


def clear_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def run_forkyard(
    rpc_url: str, block_height: int, agents: int, contracts: list[str], cache_dir: Path
) -> tuple[float, bool, str]:
    """One forkyard process, `agents` sessions, each reading every contract."""
    env = {
        **os.environ,
        "RPC_URL": rpc_url,
        "FORKYARD_PORT": str(FORKYARD_PORT),
        "FORKYARD_MCP_HTTP_PORT": str(FORKYARD_MCP_PORT),
        "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
        "FORKYARD_CACHE_DIR": str(cache_dir),
    }
    # The rest of the measurement pass exports this to keep every other
    # benchmark cold-vs-cold. Inheriting it here would make the warm row a
    # second cold row.
    env.pop("FORKYARD_CACHE_DISABLED", None)
    process = subprocess.Popen(["forkyard"], env=env)
    base_url = f"http://127.0.0.1:{FORKYARD_PORT}"
    try:
        _wait_for_forkyard(base_url)
        start = time.monotonic()
        ok, error = True, ""
        for _ in range(agents):
            backend = ForkyardBackend(base_url=base_url)
            for address in contracts:
                _, _, action_ok, action_error = read_contract(backend, address, GET_RESERVES_SELECTOR)
                if not action_ok:
                    ok, error = False, action_error
            backend.discard()
        return (time.monotonic() - start) * 1000, ok, error
    finally:
        _terminate(process)


def run_anvil(
    rpc_url: str, block_height: int, agents: int, contracts: list[str]
) -> tuple[float, bool, str]:
    """`agents` Anvil processes — its only unit of isolation — each reading
    every contract. `rpc_cache=True` because Foundry's on-disk cache is
    exactly what is under test; every other benchmark in this directory
    disables it."""
    start = time.monotonic()
    ok, error = True, ""
    for i in range(agents):
        backend = AnvilBackend(ANVIL_BASE_PORT + i, rpc_url, block_height, rpc_cache=True)
        try:
            for address in contracts:
                _, _, action_ok, action_error = read_contract(backend, address, GET_RESERVES_SELECTOR)
                if not action_ok:
                    ok, error = False, action_error
        finally:
            backend.discard()
    return (time.monotonic() - start) * 1000, ok, error


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"))
    parser.add_argument("--block-height", type=int, default=DEFAULT_BLOCK_HEIGHT)
    parser.add_argument("--agents", type=int, default=5)
    parser.add_argument("--contracts", type=int, default=8)
    parser.add_argument("--cache-dir", default="/tmp/forkyard-warmstart-cache")
    parser.add_argument("--out", default="warmstart.csv")
    args = parser.parse_args()
    if not args.rpc_url:
        parser.error("--rpc-url is required (or set RPC_URL)")

    cache_dir = Path(args.cache_dir)
    # Fetched straight from the endpoint rather than through the proxy:
    # benchmark setup must not land in the counts it is about to take.
    contracts = fetch_pair_addresses(args.rpc_url, args.block_height, args.contracts)

    rows: list[dict[str, object]] = []
    proxy = CountingProxy(args.rpc_url).start()
    try:
        for backend, condition in [
            ("forkyard", "cold"), ("forkyard", "warm"),
            ("anvil", "cold"), ("anvil", "warm"),
        ]:
            if condition == "cold":
                clear_dir(cache_dir if backend == "forkyard" else foundry_cache_dir(args.block_height))
            proxy.reset()
            if backend == "forkyard":
                elapsed_ms, ok, error = run_forkyard(
                    proxy.url, args.block_height, args.agents, contracts, cache_dir
                )
            else:
                elapsed_ms, ok, error = run_anvil(proxy.url, args.block_height, args.agents, contracts)
            stats = proxy.snapshot()
            rows.append({
                "backend": backend, "condition": condition, "agents": args.agents,
                "contracts": len(contracts), "jsonrpc_calls": stats.jsonrpc_calls,
                "http_requests": stats.http_requests, "wall_clock_ms": round(elapsed_ms, 1),
                "ok": ok, "error": error,
            })
            print(
                f"{backend:9s} {condition:5s}: {stats.jsonrpc_calls:5d} upstream calls, "
                f"{elapsed_ms / 1000:.2f}s, ok={ok}",
                file=sys.stderr,
            )
    finally:
        proxy.stop()

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
