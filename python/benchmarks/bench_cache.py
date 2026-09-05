"""Benchmarks of cached state: restart cost and many blocks in one process."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import itertools
import os
import requests
import shutil
import subprocess
import sys
import time

from actions import ActionResult, WETH, read_contract
from backend import AnvilBackend, Backend, ForkyardBackend
from bench_common import (
    DEFAULT_BLOCK_HEIGHT,
    MAX_ERROR_CHARS,
    RssSampler,
    parse_int_list,
    process_pids,
    summary_path,
)
from contracts import GET_RESERVES_SELECTOR, fetch_pair_addresses
from dataclasses import dataclass
from pathlib import Path
from rpc_proxy import CountingProxy
from run_benchmark import _terminate, _wait_for_forkyard
from typing import Callable, IO, Iterator
from web3 import Web3


# --- bench_warmstart: What a *restart* costs, now that both backends can persist a fetch cache.

WARMSTART_FIELDS = [
    "backend", "condition", "agents", "contracts",
    "jsonrpc_calls", "http_requests", "wall_clock_ms", "ok", "error",
]


WARMSTART_FORKYARD_PORT = 18670


WARMSTART_FORKYARD_MCP_PORT = 18671


WARMSTART_ANVIL_BASE_PORT = 23000


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
        "FORKYARD_PORT": str(WARMSTART_FORKYARD_PORT),
        "FORKYARD_MCP_HTTP_PORT": str(WARMSTART_FORKYARD_MCP_PORT),
        "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
        "FORKYARD_CACHE_DIR": str(cache_dir),
    }
    # The rest of the measurement pass exports this to keep every other
    # benchmark cold-vs-cold. Inheriting it here would make the warm row a
    # second cold row.
    env.pop("FORKYARD_CACHE_DISABLED", None)
    process = subprocess.Popen(["forkyard"], env=env)
    base_url = f"http://127.0.0.1:{WARMSTART_FORKYARD_PORT}"
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
        backend = AnvilBackend(WARMSTART_ANVIL_BASE_PORT + i, rpc_url, block_height, rpc_cache=True)
        try:
            for address in contracts:
                _, _, action_ok, action_error = read_contract(backend, address, GET_RESERVES_SELECTOR)
                if not action_ok:
                    ok, error = False, action_error
        finally:
            backend.discard()
    return (time.monotonic() - start) * 1000, ok, error


def warmstart_main() -> None:
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
        writer = csv.DictWriter(f, fieldnames=WARMSTART_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


# --- bench_blocks: N agents spread over B *different* fork blocks, in one process.

BLOCKS_FIELDS = [
    "arm", "blocks", "agents", "round", "agent_id", "block_number",
    "phase", "elapsed_ms", "ok", "error",
]


SUMMARY_FIELDS = [
    "arm", "blocks", "agents", "round", "jsonrpc_calls", "peak_rss_mb",
    "wall_clock_ms", "block_mismatches", "distinct_state_verified",
]


ARMS = ("forkyard", "anvil", "anvil-shared-unsafe")


DEFAULT_ARMS = ["forkyard", "anvil"]


DEFAULT_BASE_BLOCK = 25_795_072


DEFAULT_BLOCK_STRIDE = 1_000


DEFAULT_BLOCKS = [1, 2, 4, 8]


DEFAULT_AGENTS = 24


DEFAULT_PAIRS = 3


BLOCKS_FORKYARD_PORT = 18660


BLOCKS_FORKYARD_MCP_PORT = 18661


BLOCKS_ANVIL_BASE_PORT = 22000


SESSION_OPEN_TIMEOUT_S = 120.0


@dataclass
class BlockRecord:
    arm: str
    blocks: int
    agents: int
    round: int
    agent_id: int
    block_number: int
    phase: str
    elapsed_ms: float
    ok: bool
    error: str = ""


@dataclass
class AgentOutcome:
    agent_id: int
    block_number: int
    # None when the probe itself failed. Deliberately not conflated with "a
    # wrong block": an unverifiable session is counted as a mismatch below,
    # but the two are different failures and the records show which.
    reported_block: int | None
    fingerprint: str | None
    records: list[BlockRecord]
    ok: bool


@dataclass
class SummaryRow:
    arm: str
    blocks: int
    agents: int
    round: int
    jsonrpc_calls: int
    peak_rss_mb: float
    wall_clock_ms: float
    block_mismatches: int
    distinct_state_verified: str


def _row(r: BlockRecord) -> dict[str, object]:
    return {
        "arm": r.arm,
        "blocks": r.blocks,
        "agents": r.agents,
        "round": r.round,
        "agent_id": r.agent_id,
        "block_number": r.block_number,
        "phase": r.phase,
        "elapsed_ms": round(r.elapsed_ms, 3),
        "ok": r.ok,
        "error": r.error,
    }


def _summary_row(s: SummaryRow) -> dict[str, object]:
    return {
        "arm": s.arm,
        "blocks": s.blocks,
        "agents": s.agents,
        "round": s.round,
        "jsonrpc_calls": s.jsonrpc_calls,
        "peak_rss_mb": round(s.peak_rss_mb, 1),
        "wall_clock_ms": round(s.wall_clock_ms, 1),
        "block_mismatches": s.block_mismatches,
        "distinct_state_verified": s.distinct_state_verified,
    }


def block_heights(base_block: int, stride: int, count: int) -> list[int]:
    """`count` distinct blocks, stepping *backwards* from `base_block`."""
    if count < 1:
        raise ValueError(f"need at least one block, got {count}")
    if stride < 1:
        raise ValueError(f"block stride must be positive, got {stride}")
    oldest = base_block - stride * (count - 1)
    if oldest < 1:
        raise ValueError(
            f"{count} blocks at stride {stride} steps back past genesis from {base_block}"
        )
    return [base_block - stride * i for i in range(count)]


def assign_agent_blocks(num_agents: int, blocks: list[int]) -> list[int]:
    """Round-robin, so agent i gets `blocks[i % B]`."""
    if num_agents < 1:
        raise ValueError(f"need at least one agent, got {num_agents}")
    if not blocks:
        raise ValueError("need at least one block")
    if num_agents < len(blocks):
        raise ValueError(f"{num_agents} agents cannot cover {len(blocks)} distinct blocks")
    return [blocks[i % len(blocks)] for i in range(num_agents)]


def group_agents_by_block(assignment: list[int]) -> dict[int, list[int]]:
    """Invert `assign_agent_blocks`: block -> the agent ids pinned to it."""
    groups: dict[int, list[int]] = {}
    for agent_id, block in enumerate(assignment):
        groups.setdefault(block, []).append(agent_id)
    return groups


def distinct_state_verified(outcomes: list[AgentOutcome]) -> str:
    """"yes" / "no" / "n/a" — did agents at different blocks see different state?"""
    per_block: dict[int, set[str]] = {}
    for o in outcomes:
        if o.fingerprint is not None:
            per_block.setdefault(o.block_number, set()).add(o.fingerprint)
    if len(per_block) < 2:
        return "n/a"
    if any(len(fps) != 1 for fps in per_block.values()):
        return "no"
    fingerprints = [next(iter(fps)) for fps in per_block.values()]
    return "yes" if len(set(fingerprints)) == len(fingerprints) else "no"


def summarize_round(
    arm: str, blocks: int, agents: int, round_index: int,
    outcomes: list[AgentOutcome], jsonrpc_calls: int,
    peak_rss_mb: float, wall_clock_ms: float,
) -> SummaryRow:
    """Pure reduction of one (arm, B, round) into its summary row."""
    mismatches = sum(1 for o in outcomes if o.reported_block != o.block_number)
    return SummaryRow(
        arm=arm,
        blocks=blocks,
        agents=agents,
        round=round_index,
        jsonrpc_calls=jsonrpc_calls,
        peak_rss_mb=peak_rss_mb,
        wall_clock_ms=wall_clock_ms,
        block_mismatches=mismatches,
        distinct_state_verified=distinct_state_verified(outcomes),
    )


def open_pinned_session(base_url: str, block_number: int, timeout_s: float = SESSION_OPEN_TIMEOUT_S) -> str:
    """`POST /session {"block_number": N}` — the pinned-session variant of
    `backend.open_forkyard_session`."""
    resp = requests.post(f"{base_url}/session", json={"block_number": block_number}, timeout=timeout_s)
    resp.raise_for_status()
    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(f"forkyard refused a session at block {block_number}: {payload['error']}")
    return f"{base_url}/session/{payload['session_id']}"


def probe_environment(backend: Backend) -> tuple[int | None, str | None]:
    """The correctness half: which block does this environment think it is
    at, and what does its state look like?"""
    try:
        w3 = backend.web3()
        reported = int(w3.eth.block_number)
        # Two independent block-varying signals so a coincidence in one
        # cannot fake a distinct fingerprint: an account's balance (real
        # state) and the block's gas price (real header).
        fingerprint = f"{w3.eth.get_balance(WETH)}:{w3.eth.gas_price}"
        return reported, fingerprint
    except Exception:
        return None, None


class SharedAnvilBackend:
    """A `Backend` view onto an Anvil somebody else owns."""

    name = "anvil-shared"

    def __init__(self, url: str):
        self._w3 = Web3(Web3.HTTPProvider(url))

    def web3(self) -> Web3:
        return self._w3

    def set_native_balance(self, address: str, wei: int) -> None:
        self._w3.manager.request_blocking("anvil_setBalance", [address, hex(wei)])

    def set_storage(self, address: str, slot_hex: str, value_hex: str) -> None:
        self._w3.manager.request_blocking("anvil_setStorageAt", [address, slot_hex, value_hex])

    def discard(self) -> None:
        return None


def run_block_agent(
    make_backend: Callable[[], Backend],
    *,
    arm: str, blocks: int, agents: int, round_index: int, agent_id: int,
    block_number: int, contracts: list[str],
    acquire_override: ActionResult | None = None, emit_discard: bool = True,
) -> AgentOutcome:
    """acquire -> probe -> read every contract -> discard."""
    records: list[BlockRecord] = []

    def record(result: ActionResult, phase: str) -> bool:
        _, elapsed_ms, ok, error = result
        records.append(BlockRecord(
            arm, blocks, agents, round_index, agent_id, block_number,
            phase, elapsed_ms, ok, error,
        ))
        return ok

    backend: Backend | None = None
    if acquire_override is not None:
        record(acquire_override, "acquire")
        if not acquire_override[2]:
            return AgentOutcome(agent_id, block_number, None, None, records, False)
        backend = make_backend()
    else:
        start = time.monotonic()
        try:
            backend = make_backend()
            acquire: ActionResult = ("acquire", (time.monotonic() - start) * 1000, True, "")
        except Exception as e:
            acquire = ("acquire", (time.monotonic() - start) * 1000, False, repr(e)[:MAX_ERROR_CHARS])
        if not record(acquire, "acquire"):
            return AgentOutcome(agent_id, block_number, None, None, records, False)

    assert backend is not None
    reported, fingerprint = probe_environment(backend)
    ok = True
    try:
        for address in contracts:
            ok &= record(read_contract(backend, address, GET_RESERVES_SELECTOR), "read")
    finally:
        # A failed teardown is not a failed measurement — the reads already
        # happened — but it is recorded, because an Anvil that would not die
        # is exactly what poisons the next round's ports.
        start = time.monotonic()
        try:
            backend.discard()
            discard: ActionResult = ("discard", (time.monotonic() - start) * 1000, True, "")
        except Exception as e:
            discard = ("discard", (time.monotonic() - start) * 1000, False, repr(e)[:MAX_ERROR_CHARS])
        if emit_discard:
            record(discard, "discard")

    return AgentOutcome(agent_id, block_number, reported, fingerprint, records, ok)


def _run_concurrently(
    tasks: list[Callable[[], AgentOutcome]]
) -> tuple[list[AgentOutcome], float]:
    """Run every agent at once and return the wall clock they took together."""
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(tasks))) as pool:
        futures = [pool.submit(t) for t in tasks]
        outcomes = [f.result() for f in futures]
    outcomes.sort(key=lambda o: o.agent_id)
    return outcomes, (time.monotonic() - start) * 1000


def run_forkyard_arm(
    rpc_url: str, assignment: list[int], num_blocks: int, rounds: int,
    contracts: list[str], max_pinned_blocks: int, proxy: CountingProxy | None,
    sample_interval_s: float = 0.1,
) -> tuple[list[BlockRecord], list[SummaryRow]]:
    """ONE process for every round and every block."""
    pre_existing = process_pids("forkyard")
    env = {
        **os.environ,
        "RPC_URL": rpc_url,
        "FORKYARD_PORT": str(BLOCKS_FORKYARD_PORT),
        "FORKYARD_MCP_HTTP_PORT": str(BLOCKS_FORKYARD_MCP_PORT),
        # Above B, always: at or below it the LRU starts evicting bases
        # mid-round and the arm would measure eviction-and-refetch rather
        # than the sharing it is supposed to measure. Eviction behaviour is
        # worth benchmarking; it is not what this file claims.
        "FORKYARD_MAX_PINNED_BLOCKS": str(max_pinned_blocks),
    }
    try:
        process = subprocess.Popen(["forkyard"], env=env)
    except FileNotFoundError as e:
        raise RuntimeError(
            "the `forkyard` binary was not found on PATH — build it with "
            "`cargo build -p forkyard --release` and add target/release to PATH"
        ) from e
    base_url = f"http://127.0.0.1:{BLOCKS_FORKYARD_PORT}"
    records: list[BlockRecord] = []
    summaries: list[SummaryRow] = []
    try:
        # Readiness is probed before the counters are reset: `_wait_for_forkyard`
        # opens a bodyless session at the tip, and those calls belong to
        # neither round.
        _wait_for_forkyard(base_url)
        for round_index in range(1, rounds + 1):
            if proxy:
                proxy.reset()
            sampler = RssSampler("forkyard", pre_existing, sample_interval_s).start()
            try:
                outcomes, wall_ms = _run_concurrently([
                    (lambda i=i, b=b: run_block_agent(
                        lambda: ForkyardBackend(session_url=open_pinned_session(base_url, b)),
                        arm="forkyard", blocks=num_blocks, agents=len(assignment),
                        round_index=round_index, agent_id=i, block_number=b, contracts=contracts,
                    ))
                    for i, b in enumerate(assignment)
                ])
            finally:
                peak = sampler.stop()
            calls = proxy.snapshot().jsonrpc_calls if proxy else 0
            records.extend(r for o in outcomes for r in o.records)
            summaries.append(summarize_round(
                "forkyard", num_blocks, len(assignment), round_index,
                outcomes, calls, peak, wall_ms,
            ))
    finally:
        _terminate(process)
    return records, summaries


def run_anvil_arm(
    rpc_url: str, assignment: list[int], num_blocks: int, rounds: int,
    contracts: list[str], ports: Iterator[int], proxy: CountingProxy | None,
    startup_timeout_s: float = 120.0, sample_interval_s: float = 0.1,
) -> tuple[list[BlockRecord], list[SummaryRow]]:
    """N processes, grouped B ways by block."""
    pre_existing = process_pids("anvil")
    records: list[BlockRecord] = []
    summaries: list[SummaryRow] = []
    for round_index in range(1, rounds + 1):
        if proxy:
            proxy.reset()
        sampler = RssSampler("anvil", pre_existing, sample_interval_s).start()
        try:
            outcomes, wall_ms = _run_concurrently([
                (lambda i=i, b=b, p=next(ports): run_block_agent(
                    lambda: AnvilBackend(p, rpc_url, b, startup_timeout_s=startup_timeout_s),
                    arm="anvil", blocks=num_blocks, agents=len(assignment),
                    round_index=round_index, agent_id=i, block_number=b, contracts=contracts,
                ))
                for i, b in enumerate(assignment)
            ])
        finally:
            peak = sampler.stop()
        calls = proxy.snapshot().jsonrpc_calls if proxy else 0
        records.extend(r for o in outcomes for r in o.records)
        summaries.append(summarize_round(
            "anvil", num_blocks, len(assignment), round_index, outcomes, calls, peak, wall_ms,
        ))
    return records, summaries


def _run_shared_group(
    rpc_url: str, block: int, agent_ids: list[int], port: int, num_blocks: int,
    num_agents: int, round_index: int, contracts: list[str], startup_timeout_s: float,
) -> list[AgentOutcome]:
    """One block's group: spawn a single Anvil, run its agents against it
    concurrently, kill it."""
    start = time.monotonic()
    try:
        owner = AnvilBackend(port, rpc_url, block, startup_timeout_s=startup_timeout_s)
        spawn: ActionResult = ("acquire", (time.monotonic() - start) * 1000, True, "")
    except Exception as e:
        spawn = ("acquire", (time.monotonic() - start) * 1000, False, repr(e)[:MAX_ERROR_CHARS])
        return [
            AgentOutcome(i, block, None, None, [BlockRecord(
                "anvil-shared-unsafe", num_blocks, num_agents, round_index, i, block,
                "acquire", spawn[1], False, spawn[3],
            )], False)
            for i in agent_ids
        ]
    url = f"http://127.0.0.1:{port}"
    try:
        outcomes, _ = _run_concurrently([
            (lambda i=i: run_block_agent(
                lambda: SharedAnvilBackend(url),
                arm="anvil-shared-unsafe", blocks=num_blocks, agents=num_agents,
                round_index=round_index, agent_id=i, block_number=block, contracts=contracts,
                acquire_override=spawn, emit_discard=False,
            ))
            for i in agent_ids
        ])
    finally:
        teardown_start = time.monotonic()
        try:
            owner.discard()
            teardown: ActionResult = ("discard", (time.monotonic() - teardown_start) * 1000, True, "")
        except Exception as e:
            teardown = ("discard", (time.monotonic() - teardown_start) * 1000, False,
                        repr(e)[:MAX_ERROR_CHARS])
    for o in outcomes:
        if o.agent_id == min(agent_ids):
            o.records.append(BlockRecord(
                "anvil-shared-unsafe", num_blocks, num_agents, round_index, o.agent_id, block,
                "discard", teardown[1], teardown[2], teardown[3],
            ))
    return outcomes


def run_anvil_shared_arm(
    rpc_url: str, assignment: list[int], num_blocks: int, rounds: int,
    contracts: list[str], ports: Iterator[int], proxy: CountingProxy | None,
    startup_timeout_s: float = 120.0, sample_interval_s: float = 0.1,
) -> tuple[list[BlockRecord], list[SummaryRow]]:
    """B processes, N/B agents each — and NO isolation within a group."""
    pre_existing = process_pids("anvil")
    groups = group_agents_by_block(assignment)
    records: list[BlockRecord] = []
    summaries: list[SummaryRow] = []
    for round_index in range(1, rounds + 1):
        if proxy:
            proxy.reset()
        sampler = RssSampler("anvil", pre_existing, sample_interval_s).start()
        start = time.monotonic()
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(groups))) as pool:
                futures = [
                    pool.submit(
                        _run_shared_group, rpc_url, block, ids, next(ports), num_blocks,
                        len(assignment), round_index, contracts, startup_timeout_s,
                    )
                    for block, ids in groups.items()
                ]
                outcomes = sorted(
                    (o for f in futures for o in f.result()), key=lambda o: o.agent_id
                )
            wall_ms = (time.monotonic() - start) * 1000
        finally:
            peak = sampler.stop()
        calls = proxy.snapshot().jsonrpc_calls if proxy else 0
        records.extend(r for o in outcomes for r in o.records)
        summaries.append(summarize_round(
            "anvil-shared-unsafe", num_blocks, len(assignment), round_index,
            outcomes, calls, peak, wall_ms,
        ))
    return records, summaries


def write_records(out: IO[str], records: list[BlockRecord]) -> None:
    writer = csv.DictWriter(out, fieldnames=BLOCKS_FIELDS)
    writer.writeheader()
    writer.writerows(_row(r) for r in records)


def write_summaries(out: IO[str], summaries: list[SummaryRow]) -> None:
    writer = csv.DictWriter(out, fieldnames=SUMMARY_FIELDS)
    writer.writeheader()
    writer.writerows(_summary_row(s) for s in summaries)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Spread N agents over B distinct fork blocks. forkyard serves all "
            "of them from one process sharing one cache per block; Anvil needs "
            "one process per isolated agent because --fork-block-number is a "
            "process-level flag. Runs two rounds so forkyard's warm per-block "
            "cache can be compared against Anvil's per-process refetch."
        ),
    )
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"),
                        help="archive endpoint to fork from (default: $RPC_URL). Must be an "
                             "archive node: every block but the newest is historical.")
    parser.add_argument("--agents", type=int, default=DEFAULT_AGENTS,
                        help=f"N, total agents, spread round-robin over the blocks "
                             f"(default: {DEFAULT_AGENTS})")
    parser.add_argument("--blocks", type=parse_int_list, default=DEFAULT_BLOCKS,
                        help="comma-separated B values to sweep, i.e. how many distinct blocks "
                             f"the fleet spans (default: {','.join(str(b) for b in DEFAULT_BLOCKS)})")
    parser.add_argument("--base-block", type=int, default=DEFAULT_BASE_BLOCK,
                        help=f"newest block of the set (default: {DEFAULT_BASE_BLOCK})")
    parser.add_argument("--block-stride", type=int, default=DEFAULT_BLOCK_STRIDE,
                        help="blocks between neighbours, stepping back from --base-block. Wide "
                             "enough that state really moved between them "
                             f"(default: {DEFAULT_BLOCK_STRIDE})")
    parser.add_argument("--block-list", type=parse_int_list, default=None,
                        help="explicit block heights instead of --base-block/--block-stride. Each "
                             "swept B takes the first B of them.")
    parser.add_argument("--rounds", type=int, default=2,
                        help="1 = cold only; 2 = cold then warm, the comparison this file is for "
                             "(default: 2)")
    parser.add_argument("--pairs", type=int, default=DEFAULT_PAIRS,
                        help="Uniswap V2 pairs each agent reads. The SAME pairs for every agent, "
                             "so per-block sharing is the only thing that varies "
                             f"(default: {DEFAULT_PAIRS})")
    parser.add_argument("--arms", default=",".join(DEFAULT_ARMS),
                        help=f"comma-separated subset of {','.join(ARMS)}. anvil-shared-unsafe is "
                             "off by default: its agents are not isolated from each other "
                             f"(default: {','.join(DEFAULT_ARMS)})")
    parser.add_argument("--max-pinned-blocks", type=int, default=None,
                        help="FORKYARD_MAX_PINNED_BLOCKS for the forkyard arm. Defaults to "
                             "max(B) + 2, i.e. comfortably above the largest sweep, because at or "
                             "below B the LRU evicts a base mid-round and the arm would measure "
                             "refetching instead of sharing.")
    parser.add_argument("--no-proxy", action="store_true",
                        help="skip the counting proxy. The proxy adds a local hop to every "
                             "upstream call, so quote wall clocks from a --no-proxy run and "
                             "jsonrpc_calls from a proxied one.")
    parser.add_argument("--anvil-startup-timeout", type=float, default=120.0,
                        help="seconds to wait for each Anvil. High because N=24 forking Anvils "
                             "starting at once are far slower than one (default: 120)")
    parser.add_argument("--sample-interval", type=float, default=0.1,
                        help="RSS sampling period in seconds (default: 0.1)")
    parser.add_argument("--out", default="blocks.csv")
    return parser.parse_args(argv)


def blocks_for(args: argparse.Namespace, count: int) -> list[int]:
    """The `count` block heights this B uses — the first `count` of an
    explicit --block-list, or a stride walk back from --base-block."""
    if args.block_list:
        if len(args.block_list) < count:
            raise ValueError(f"--block-list has {len(args.block_list)} blocks, need {count}")
        return list(args.block_list[:count])
    return block_heights(args.base_block, args.block_stride, count)


def blocks_main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if not args.rpc_url:
        raise SystemExit("--rpc-url is required (or set RPC_URL)")
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    unknown = [a for a in arms if a not in ARMS]
    if unknown:
        raise SystemExit(f"unknown arm(s) {unknown}; choose from {list(ARMS)}")
    max_pinned = args.max_pinned_blocks or max(args.blocks) + 2

    # Pairs are resolved at the OLDEST block any sweep will use. `allPairs(i)`
    # is append-only, so an index that exists at the oldest block exists at
    # every newer one — the other way round, a pair created after the oldest
    # block would have no code there and every read at that block would fail
    # for a reason that has nothing to do with the backends. Fetched straight
    # from the upstream, before the proxy exists: this is setup, not agent work.
    oldest = min(blocks_for(args, max(args.blocks)))
    print(f"fetching {args.pairs} Uniswap V2 pair addresses at block {oldest}", file=sys.stderr)
    contracts = fetch_pair_addresses(args.rpc_url, oldest, args.pairs)

    proxy = None if args.no_proxy else CountingProxy(args.rpc_url).start()
    rpc_url = proxy.url if proxy else args.rpc_url
    summary_csv = args.out.rsplit(".", 1)[0] + ".summary.csv"
    ports = itertools.count(BLOCKS_ANVIL_BASE_PORT)

    try:
        with open(args.out, "w", newline="") as f, open(summary_csv, "w", newline="") as sf:
            writer = csv.DictWriter(f, fieldnames=BLOCKS_FIELDS)
            writer.writeheader()
            summary_writer = csv.DictWriter(sf, fieldnames=SUMMARY_FIELDS)
            summary_writer.writeheader()
            f.flush()
            sf.flush()

            for num_blocks in args.blocks:
                blocks = blocks_for(args, num_blocks)
                assignment = assign_agent_blocks(args.agents, blocks)
                for arm in arms:
                    print(f"{arm}: N={args.agents} over B={num_blocks} blocks {blocks}", file=sys.stderr)
                    if arm == "forkyard":
                        records, summaries = run_forkyard_arm(
                            rpc_url, assignment, num_blocks, args.rounds, contracts,
                            max_pinned, proxy, args.sample_interval,
                        )
                    elif arm == "anvil":
                        records, summaries = run_anvil_arm(
                            rpc_url, assignment, num_blocks, args.rounds, contracts,
                            ports, proxy, args.anvil_startup_timeout, args.sample_interval,
                        )
                    else:
                        records, summaries = run_anvil_shared_arm(
                            rpc_url, assignment, num_blocks, args.rounds, contracts,
                            ports, proxy, args.anvil_startup_timeout, args.sample_interval,
                        )
                    # Flushed per arm so a failure at B=8 keeps everything
                    # the smaller Bs already cost.
                    writer.writerows(_row(r) for r in records)
                    summary_writer.writerows(_summary_row(s) for s in summaries)
                    f.flush()
                    sf.flush()
                    for s in summaries:
                        print(f"  round {s.round}: {s.jsonrpc_calls} upstream calls, "
                              f"{round(s.peak_rss_mb, 1)} MB peak, {round(s.wall_clock_ms)} ms, "
                              f"{s.block_mismatches} block mismatches, "
                              f"distinct state {s.distinct_state_verified}", file=sys.stderr)
                    # The forkyard arm rebinds a fixed port on the next B;
                    # give the closed listener a moment rather than racing
                    # TIME_WAIT.
                    time.sleep(1.0)
    finally:
        if proxy:
            proxy.stop()
