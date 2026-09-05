"""CLI entrypoint: sweeps (backend, block_height, num_agents) and records
per-action + per-run timings to a CSV. See
docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import os
import random
import shutil
import subprocess
import sys
import time
from typing import Callable, IO

import requests
from pydantic import BaseModel, Field, NonNegativeFloat, NonNegativeInt

from agent import ActionRecord, run_agent
from backend import AnvilBackend, Backend, ForkyardBackend
from contracts import assign_contracts, fetch_pair_addresses
from rpc_proxy import CountingProxy, ProxyStats


class UpstreamRow(BaseModel):
    """One combination's cost to the upstream provider, written to a sibling
    `<out>.upstream.csv` under --count-upstream."""

    backend: str = Field(min_length=1)
    block_height: int = Field(ge=0)
    num_agents: int = Field(ge=1)
    episodes: int = Field(ge=1)
    http_requests: NonNegativeInt
    jsonrpc_calls: NonNegativeInt
    # The number the architectures differ on: Anvil's cache is per process
    # so this tracks the agent count, forkyard's shared one lets it fall away.
    calls_per_agent: NonNegativeFloat
    upstream_errors: NonNegativeInt
    top_methods: str


# CSV column order is the models' field order. benchmark.md and the other
# scripts read these columns by position, so neither may be reordered.
FIELDS = list(ActionRecord.model_fields)
UPSTREAM_FIELDS = list(UpstreamRow.model_fields)


def parse_int_list(s: str) -> list[int]:
    return [int(x) for x in s.split(",")]


def _row(r: ActionRecord) -> dict[str, object]:
    return r.model_dump()


def write_records(out: IO[str], records: list[ActionRecord]) -> None:
    writer = csv.DictWriter(out, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(_row(r) for r in records)


def upstream_row(
    backend: str, block_height: int, num_agents: int, episodes: int, stats: ProxyStats
) -> dict[str, object]:
    top = dict(sorted(stats.by_method.items(), key=lambda kv: -kv[1])[:5])
    return UpstreamRow(
        backend=backend,
        block_height=block_height,
        num_agents=num_agents,
        episodes=episodes,
        http_requests=stats.http_requests,
        jsonrpc_calls=stats.jsonrpc_calls,
        calls_per_agent=round(stats.jsonrpc_calls / num_agents, 1),
        upstream_errors=stats.upstream_errors,
        top_methods=json.dumps(top),
    ).model_dump()


def _check_binaries_on_path() -> None:
    """Fail before any sweep runs: a missing binary discovered on the first
    Anvil sweep would throw away the whole forkyard half."""
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
    """terminate → wait → kill → wait. Called from a `finally`, where a
    bare expiring `wait()` would mask the in-flight exception and leave an
    orphan holding the sweep's fixed port."""
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


def _run_agents(
    make_backend_for_agent: Callable[[int], Callable[[], Backend]],
    backend_name: str,
    num_agents: int,
    block_height: int,
    actions_per_agent: int,
    episodes: int,
    contracts_per_agent: list[list[str]] | None = None,
) -> tuple[list[ActionRecord], float]:
    """Run `num_agents` agents concurrently, returning their records and the
    wall-clock they took together. Shared by both backends so their timed
    regions cannot drift apart."""
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_agents) as pool:
        futures = [
            pool.submit(
                lambda i=i: run_agent(
                    make_backend_for_agent(i),
                    backend_name,
                    random.Random(i),
                    i,
                    block_height,
                    num_agents,
                    actions_per_agent,
                    episodes=episodes,
                    contracts=contracts_per_agent[i] if contracts_per_agent else None,
                )
            )
            for i in range(num_agents)
        ]
        all_records = [r for f in futures for r in f.result()]
    total_ms = (time.monotonic() - start) * 1000
    return all_records, total_ms


def run_forkyard_sweep(
    rpc_url: str, block_height: int, num_agents: int, actions_per_agent: int,
    port: int, mcp_port: int, episodes: int = 1,
    contracts_per_agent: list[list[str]] | None = None,
    cold_cache: bool = False,
) -> tuple[list[ActionRecord], float]:
    env = {
        **os.environ,
        "RPC_URL": rpc_url,
        "FORKYARD_PORT": str(port),
        "FORKYARD_MCP_HTTP_PORT": str(mcp_port),
        "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
    }
    # Set explicitly either way: inheriting a stale FORKYARD_CACHE_DISABLED
    # from the shell would silently make a warm run cold, which reads as
    # "persistence does nothing" rather than as a misconfiguration.
    if cold_cache:
        env["FORKYARD_CACHE_DISABLED"] = "1"
    else:
        env.pop("FORKYARD_CACHE_DISABLED", None)
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

        # Timed per agent and episode: session open, actions, discard —
        # excluding only the one-time process start above, which has no Anvil
        # analogue. Hoisting the session open out of the agent would break
        # that symmetry with run_anvil_sweep.
        return _run_agents(
            lambda i: (lambda: ForkyardBackend(base_url=base_url)),
            "forkyard", num_agents, block_height, actions_per_agent, episodes,
            contracts_per_agent,
        )
    finally:
        _terminate(process)


def run_anvil_sweep(
    rpc_url: str, block_height: int, num_agents: int, actions_per_agent: int,
    base_port: int, episodes: int = 1,
    contracts_per_agent: list[list[str]] | None = None,
    anvil_rpc_cache: bool = True,
) -> tuple[list[ActionRecord], float]:
    # Timed per agent and episode: spawn + wait-until-ready, actions, and
    # the discard that kills the process. Anvil has no shared startup to
    # exclude, which is why run_forkyard_sweep excludes only forkyard's.
    def factory_for(agent_index: int) -> Callable[[], Backend]:
        # A port window per agent: a discarded process leaves its port in
        # TIME_WAIT, so the next episode must not reuse it.
        ports = iter(range(base_port + agent_index * episodes, base_port + (agent_index + 1) * episodes))
        return lambda: AnvilBackend(next(ports), rpc_url, block_height, rpc_cache=anvil_rpc_cache)

    return _run_agents(
        factory_for, "anvil", num_agents, block_height, actions_per_agent, episodes,
        contracts_per_agent,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agents", type=parse_int_list, required=True)
    parser.add_argument("--block-heights", type=parse_int_list, required=True)
    parser.add_argument("--actions-per-agent", type=int, default=5)
    parser.add_argument(
        "--episodes", type=int, default=1,
        help="acquire→act→discard cycles per agent. >1 measures session churn: "
             "the cost an agent pays for a *disposable* fork, which a single "
             "long-lived episode amortises away.",
    )
    parser.add_argument(
        "--count-upstream", action="store_true",
        help="route both backends through a counting proxy and write upstream "
             "RPC totals to <out>.upstream.csv. Adds a local hop, so timings "
             "from such a run are not comparable to a direct one.",
    )
    parser.add_argument(
        "--cold-caches", action="store_true",
        help="run both backends without their persistent caches: Anvil with "
             "--no-storage-caching, forkyard with FORKYARD_CACHE_DISABLED. Both "
             "keep a per-(chain, block) cache across runs, so the default here "
             "is warm against warm; use this to measure a first-ever run.",
    )
    parser.add_argument(
        "--state-overlap", choices=["shared", "disjoint"], default=None,
        help="switch to the read-only state-overlap workload: every agent reads "
             "the same contracts (shared) or its own (disjoint). Isolates how "
             "much of the difference between the two architectures is one cache "
             "being shared rather than N caches being separate.",
    )
    parser.add_argument("--rpc-url", required=True)
    parser.add_argument("--out", default="results.csv")
    args = parser.parse_args()

    _check_binaries_on_path()

    contract_pool: list[str] = []
    if args.state_overlap:
        # Once, from the real endpoint, before counting starts: setup, not
        # agent work.
        needed = max(args.agents) * args.actions_per_agent if args.state_overlap == "disjoint" else args.actions_per_agent
        print(f"fetching {needed} Uniswap V2 pair addresses for the {args.state_overlap} workload", file=sys.stderr)
        contract_pool = fetch_pair_addresses(args.rpc_url, max(args.block_heights), needed)

    proxy = CountingProxy(args.rpc_url).start() if args.count_upstream else None
    rpc_url = proxy.url if proxy else args.rpc_url
    upstream_path = args.out.rsplit(".", 1)[0] + ".upstream.csv"

    # Flushed after every combination, so a failure part-way through a long
    # sweep keeps what it collected.
    try:
        with open(args.out, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            f.flush()

            upstream_file = open(upstream_path, "w", newline="") if proxy else None
            upstream_writer = None
            if upstream_file is not None:
                upstream_writer = csv.DictWriter(upstream_file, fieldnames=UPSTREAM_FIELDS)
                upstream_writer.writeheader()
                upstream_file.flush()

            try:
                for block_height in args.block_heights:
                    for num_agents in args.agents:
                        contracts_per_agent = (
                            assign_contracts(contract_pool, num_agents, args.actions_per_agent, args.state_overlap)
                            if args.state_overlap else None
                        )
                        for sweep_fn, label in [
                            (lambda bh=block_height, na=num_agents, cpa=contracts_per_agent: run_forkyard_sweep(
                                rpc_url, bh, na, args.actions_per_agent, 18555, 18556, args.episodes, cpa,
                                args.cold_caches), "forkyard"),
                            (lambda bh=block_height, na=num_agents, cpa=contracts_per_agent: run_anvil_sweep(
                                rpc_url, bh, na, args.actions_per_agent, 19000, args.episodes, cpa,
                                not args.cold_caches), "anvil"),
                        ]:
                            print(
                                f"running {label}: block={block_height} agents={num_agents} "
                                f"episodes={args.episodes}",
                                file=sys.stderr,
                            )
                            if proxy:
                                proxy.reset()
                            records, total_ms = sweep_fn()
                            combination = [
                                *records,
                                ActionRecord(
                                    label, block_height, num_agents, -1, "__total__", total_ms,
                                    all(r.ok for r in records), "",
                                ),
                            ]
                            writer.writerows(_row(r) for r in combination)
                            f.flush()
                            if proxy and upstream_writer is not None and upstream_file is not None:
                                stats = proxy.snapshot()
                                upstream_writer.writerow(
                                    upstream_row(label, block_height, num_agents, args.episodes, stats)
                                )
                                upstream_file.flush()
                                print(
                                    f"  upstream: {stats.jsonrpc_calls} JSON-RPC calls "
                                    f"({stats.jsonrpc_calls / num_agents:.1f}/agent)",
                                    file=sys.stderr,
                                )
            finally:
                if upstream_file is not None:
                    upstream_file.close()
    finally:
        if proxy:
            proxy.stop()


if __name__ == "__main__":
    main()
