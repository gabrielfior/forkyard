"""CLI entrypoint: sweeps (backend, block_height, num_agents) and records
per-action + per-run timings to a CSV. See
docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import os
import random
import shutil
import subprocess
import sys
import time
from typing import IO

import requests

from agent import ActionRecord, run_agent
from backend import AnvilBackend, ForkyardBackend

FIELDS = ["backend", "block_height", "num_agents", "agent_id", "action", "elapsed_ms", "ok"]


def parse_int_list(s: str) -> list[int]:
    return [int(x) for x in s.split(",")]


def _row(r: ActionRecord) -> dict[str, object]:
    return {
        "backend": r.backend,
        "block_height": r.block_height,
        "num_agents": r.num_agents,
        "agent_id": r.agent_id,
        "action": r.action,
        "elapsed_ms": r.elapsed_ms,
        "ok": r.ok,
    }


def write_records(out: IO[str], records: list[ActionRecord]) -> None:
    writer = csv.DictWriter(out, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(_row(r) for r in records)


def _check_binaries_on_path() -> None:
    """Fail before any sweep runs rather than deep inside a worker thread
    on the first Anvil sweep — by which point the whole forkyard half has
    already been run and would be thrown away."""
    missing = []
    if shutil.which("forkyard") is None:
        missing.append(
            "the `forkyard` binary was not found on PATH — build it with "
            "`cargo build -p forkyard --release` and add target/release to PATH"
        )
    if shutil.which("anvil") is None:
        missing.append(
            "the `anvil` binary was not found on PATH — install Foundry "
            "(https://book.getfoundry.sh/getting-started/installation) before running the Anvil backend"
        )
    if missing:
        raise RuntimeError("; ".join(missing))


def _terminate(process: subprocess.Popen) -> None:
    """terminate → wait → kill → wait. The kill fallback matters in a
    `finally`: a bare `wait(timeout=...)` that expires would raise out of
    the `finally`, masking the in-flight exception AND leaving an orphaned
    forkyard holding the sweep's fixed port."""
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"warning: forkyard pid {process.pid} survived SIGKILL", file=sys.stderr)


def _wait_for_forkyard(base_url: str, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            resp = requests.post(f"{base_url}/session", timeout=1)
            if resp.ok:
                return
        except requests.RequestException as e:
            last_error = e
        time.sleep(0.2)
    raise RuntimeError(f"forkyard on {base_url} did not become ready in {timeout_s}s: {last_error}")


def run_forkyard_sweep(
    rpc_url: str, block_height: int, num_agents: int, actions_per_agent: int, port: int, mcp_port: int
) -> tuple[list[ActionRecord], float]:
    env = {
        **os.environ,
        "RPC_URL": rpc_url,
        "FORKYARD_PORT": str(port),
        "FORKYARD_MCP_HTTP_PORT": str(mcp_port),
        "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
    }
    try:
        process = subprocess.Popen(["forkyard"], env=env)
    except FileNotFoundError as e:
        raise RuntimeError(
            "the `forkyard` binary was not found on PATH — build it with "
            "`cargo build -p forkyard --release` and add target/release to PATH"
        ) from e
    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_forkyard(base_url)

        # The timed region INCLUDES, per agent: opening its own forkyard
        # session, its whole action sequence, and its discard. It EXCLUDES
        # the one-time `forkyard` process startup above, which is a shared
        # cost with no Anvil analogue. This mirrors `run_anvil_sweep`
        # exactly — there, each worker's `AnvilBackend(...)` construction
        # (spawn + wait-until-ready) is likewise inside the timer. Keep
        # both sides symmetric: never hoist session-open back out of the
        # pool, or forkyard stops paying its per-agent setup cost while
        # Anvil still pays its own.
        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_agents) as pool:
            futures = [
                pool.submit(
                    lambda i=i: run_agent(
                        ForkyardBackend(base_url=base_url),
                        random.Random(i),
                        i,
                        block_height,
                        num_agents,
                        actions_per_agent,
                    )
                )
                for i in range(num_agents)
            ]
            all_records = [r for f in futures for r in f.result()]
        total_ms = (time.monotonic() - start) * 1000
        return all_records, total_ms
    finally:
        _terminate(process)


def run_anvil_sweep(
    rpc_url: str, block_height: int, num_agents: int, actions_per_agent: int, base_port: int
) -> tuple[list[ActionRecord], float]:
    # The timed region INCLUDES, per agent: spawning its own Anvil and
    # waiting until it is ready (`AnvilBackend.__init__`, inline in the
    # closure below), its whole action sequence, and its discard (which
    # kills that Anvil). Anvil has no shared one-time startup to exclude,
    # which is exactly why `run_forkyard_sweep` excludes only forkyard's
    # single shared process start and times everything per-agent.
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_agents) as pool:
        futures = [
            pool.submit(
                lambda i=i: run_agent(
                    AnvilBackend(base_port + i, rpc_url, block_height),
                    random.Random(i),
                    i,
                    block_height,
                    num_agents,
                    actions_per_agent,
                )
            )
            for i in range(num_agents)
        ]
        all_records = [r for f in futures for r in f.result()]
    total_ms = (time.monotonic() - start) * 1000
    return all_records, total_ms


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agents", type=parse_int_list, required=True)
    parser.add_argument("--block-heights", type=parse_int_list, required=True)
    parser.add_argument("--actions-per-agent", type=int, default=5)
    parser.add_argument("--rpc-url", required=True)
    parser.add_argument("--out", default="results.csv")
    args = parser.parse_args()

    _check_binaries_on_path()

    # Flush after every (block_height, num_agents, backend) combination so
    # a failure part-way through a long sweep keeps everything collected up
    # to that point instead of discarding the whole run.
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        f.flush()

        for block_height in args.block_heights:
            for num_agents in args.agents:
                for sweep_fn, label in [
                    (lambda bh=block_height, na=num_agents: run_forkyard_sweep(args.rpc_url, bh, na, args.actions_per_agent, 18555, 18556), "forkyard"),
                    (lambda bh=block_height, na=num_agents: run_anvil_sweep(args.rpc_url, bh, na, args.actions_per_agent, 19000), "anvil"),
                ]:
                    print(f"running {label}: block={block_height} agents={num_agents}", file=sys.stderr)
                    records, total_ms = sweep_fn()
                    combination = [
                        *records,
                        ActionRecord(
                            label, block_height, num_agents, -1, "__total__", total_ms,
                            all(r.ok for r in records),
                        ),
                    ]
                    writer.writerows(_row(r) for r in combination)
                    f.flush()


if __name__ == "__main__":
    main()
