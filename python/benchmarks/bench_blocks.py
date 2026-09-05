"""N agents spread over B *different* fork blocks, in one process.

Every other benchmark here pins the whole sweep to one block. This one asks
the question that only became answerable with per-session block pinning
(`POST /session {"block_number": N}`): what does it cost to serve a fleet
whose agents each need a *different* historical block?

The architectures answer it differently by construction:

  * forkyard: one process. A session names its block in the open request;
    sessions naming the same block share one fetched base and one fallback,
    so the fleet's upstream cost scales with the number of distinct *blocks*,
    not the number of agents. `FORKYARD_MAX_PINNED_BLOCKS` caps how many
    bases stay warm (LRU past that), and this harness sets it above B on
    purpose — see `--max-pinned-blocks`.
  * Anvil: `--fork-block-number` is a process-level flag. B blocks therefore
    means at least B processes, and because an Anvil instance *is* the unit
    of isolation, N isolated agents means **N processes** — one per agent,
    grouped B ways by block. That is the `anvil` arm, and it is the only
    apples-to-apples one.
  * `anvil-shared-unsafe` is the cheaper thing you could do instead: B
    processes total, with the N/B agents at a block all pointing at the same
    Anvil. It is recorded because it is what a cost-conscious operator would
    try, and labelled `unsafe` because those agents are **not isolated from
    each other** — one agent's `anvil_setBalance` or landed transaction is
    visible to every other agent in its group. Read its numbers as "what you
    would pay if you gave up isolation", never as a peer of the other two.

Two rounds are run against the same blocks. Round 1 is cold. Round 2 opens
fresh sessions (forkyard) or spawns fresh processes (Anvil) at the *same*
blocks: forkyard's per-block bases are still resident, so round 2 should
cost it almost nothing upstream, while every new Anvil refetches from
scratch. That gap is the point of the whole file.

Correctness is checked, not assumed. Two facts must hold or the cost
numbers mean nothing:

  * `eth_blockNumber` on each agent's environment equals the block it asked
    for (`block_mismatches`, which must be 0);
  * environments at different blocks really see different state
    (`distinct_state_verified`).

The state fingerprint is WETH's own ETH balance (plus the block's gas
price), *not* a pair's reserves, for a boring reason: forkyard's per-session
RPC has no `eth_call`, `eth_getCode` or `eth_getStorageAt`, so the only
state a session can hand back is an account's balance and nonce. A Uniswap
V2 pair holds its reserves in storage and holds no ETH, so its account state
is byte-identical at every block. WETH's balance is the ETH backing every
one of those pairs' WETH side and moves essentially every block, so it is
the strongest cross-block state signal this RPC surface can actually return.
The pairs are still what the timed read workload touches.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import itertools
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Callable, IO, Iterator

import requests
from web3 import Web3

from actions import ActionResult, WETH, read_contract
from backend import AnvilBackend, Backend, ForkyardBackend
from bench_writers import RssSampler, process_pids
from contracts import GET_RESERVES_SELECTOR, fetch_pair_addresses
from rpc_proxy import CountingProxy
from run_benchmark import _terminate, _wait_for_forkyard, parse_int_list

FIELDS = [
    "arm", "blocks", "agents", "round", "agent_id", "block_number",
    "phase", "elapsed_ms", "ok", "error",
]

# Written to a sibling `<out>.summary.csv`: one row per (arm, B, round),
# carrying everything that is a property of the *fleet* rather than of one
# agent. Upstream calls and RSS cannot be attributed to a single agent in a
# concurrent arm, so they live only here.
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

# Fixed and distinct from every other bench_*.py, so a multi-block sweep can
# be left running without colliding with an ordinary one in another shell.
FORKYARD_PORT = 18660
FORKYARD_MCP_PORT = 18661
ANVIL_BASE_PORT = 22000

_MAX_ERROR_CHARS = 200

# Generous: the first session at a cold block does the base fetch inline, and
# at B=8 several of those are in flight at once against one upstream.
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


# --------------------------------------------------------------------------
# Block selection and agent assignment — pure, so the sweep's shape can be
# tested without a network or a subprocess anywhere near it.
# --------------------------------------------------------------------------


def block_heights(base_block: int, stride: int, count: int) -> list[int]:
    """`count` distinct blocks, stepping *backwards* from `base_block`.

    Backwards because forward would run past the chain tip. The stride has
    to be wide enough that state actually moved between neighbours — 1000
    blocks is ~3.3 hours of mainnet, which is plenty for WETH's balance and
    for the pairs' reserves."""
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
    """Round-robin, so agent i gets `blocks[i % B]`.

    Round-robin rather than contiguous chunks because the arms interleave
    differently: forkyard opens all N sessions at once and contiguous chunks
    would have every agent at a given block starting simultaneously, hiding
    whether the *second* session at a block really reuses the first's base.
    A non-divisible N/B is allowed — the leading blocks just carry one extra
    agent — but it makes the anvil groups uneven, so prefer N % B == 0."""
    if num_agents < 1:
        raise ValueError(f"need at least one agent, got {num_agents}")
    if not blocks:
        raise ValueError("need at least one block")
    if num_agents < len(blocks):
        raise ValueError(f"{num_agents} agents cannot cover {len(blocks)} distinct blocks")
    return [blocks[i % len(blocks)] for i in range(num_agents)]


def group_agents_by_block(assignment: list[int]) -> dict[int, list[int]]:
    """Invert `assign_agent_blocks`: block -> the agent ids pinned to it.

    This is the anvil arms' unit of work — one process group per block —
    and insertion order follows first appearance, so the groups come out in
    the same order as `blocks`."""
    groups: dict[int, list[int]] = {}
    for agent_id, block in enumerate(assignment):
        groups.setdefault(block, []).append(agent_id)
    return groups


def distinct_state_verified(outcomes: list[AgentOutcome]) -> str:
    """"yes" / "no" / "n/a" — did agents at different blocks see different state?

    "n/a" when fewer than two blocks produced a fingerprint at all: B=1 has
    nothing to distinguish, and saying "no" there would read as a failure
    rather than as the absence of a question. "no" means either two distinct
    blocks agreed on their state (so the pinning did nothing) or two agents
    at the *same* block disagreed (so the sharing is not sharing)."""
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
    """Pure reduction of one (arm, B, round) into its summary row.

    A `reported_block` of None counts as a mismatch: a session whose block
    could not be read is not a session known to be at the right block, and
    the cost numbers on this row are only worth quoting if that column is 0."""
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


# --------------------------------------------------------------------------
# One agent's life
# --------------------------------------------------------------------------


def open_pinned_session(base_url: str, block_number: int, timeout_s: float = SESSION_OPEN_TIMEOUT_S) -> str:
    """`POST /session {"block_number": N}` — the pinned-session variant of
    `backend.open_forkyard_session`.

    Kept here rather than in backend.py because it is the one call this
    benchmark exists to exercise. Note the explicit `error` check: forkyard
    answers a refused block with HTTP 200 and `{"error": ...}`, so
    `raise_for_status()` alone would sail past it and the failure would
    surface much later as a KeyError on `session_id`."""
    resp = requests.post(f"{base_url}/session", json={"block_number": block_number}, timeout=timeout_s)
    resp.raise_for_status()
    payload = resp.json()
    if "error" in payload:
        raise RuntimeError(f"forkyard refused a session at block {block_number}: {payload['error']}")
    return f"{base_url}/session/{payload['session_id']}"


def probe_environment(backend: Backend) -> tuple[int | None, str | None]:
    """The correctness half: which block does this environment think it is
    at, and what does its state look like?

    Not recorded as a timed row — it is instrumentation, like the marker
    writes in bench_branching.py — but it does cost a few upstream calls,
    identically in every arm, so it cannot skew the comparison."""
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
    """A `Backend` view onto an Anvil somebody else owns.

    Only used by `anvil-shared-unsafe`, where N/B agents share one process:
    `discard()` must not kill it out from under the others, so it is a no-op
    and the group tears its own Anvil down. That no-op is precisely the lost
    isolation this arm is named for — nothing separates these agents' writes."""

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
    """acquire -> probe -> read every contract -> discard.

    `acquire_override` exists for `anvil-shared-unsafe`, where the process
    the agent attaches to was spawned by its group rather than by itself:
    the group's spawn cost is passed in and charged to every agent that
    waited on it, because each of them really did wait that long before it
    could issue a single call. `emit_discard=False` goes with it: there,
    `discard()` is a no-op by design and a 0 ms row per agent would read as
    a teardown that never happened, so the group records the one real kill
    itself.

    Exceptions are captured into rows rather than raised, so one agent
    failing at B=8 still leaves the other 23 measurable."""
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
            acquire = ("acquire", (time.monotonic() - start) * 1000, False, repr(e)[:_MAX_ERROR_CHARS])
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
            discard = ("discard", (time.monotonic() - start) * 1000, False, repr(e)[:_MAX_ERROR_CHARS])
        if emit_discard:
            record(discard, "discard")

    return AgentOutcome(agent_id, block_number, reported, fingerprint, records, ok)


def _run_concurrently(
    tasks: list[Callable[[], AgentOutcome]]
) -> tuple[list[AgentOutcome], float]:
    """Run every agent at once and return the wall clock they took together.

    Concurrency is the whole point — B blocks served *simultaneously* out of
    one process is the capability — so the pool is always as wide as the
    fleet rather than bounded."""
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(tasks))) as pool:
        futures = [pool.submit(t) for t in tasks]
        outcomes = [f.result() for f in futures]
    outcomes.sort(key=lambda o: o.agent_id)
    return outcomes, (time.monotonic() - start) * 1000


# --------------------------------------------------------------------------
# Arms
# --------------------------------------------------------------------------


def run_forkyard_arm(
    rpc_url: str, assignment: list[int], num_blocks: int, rounds: int,
    contracts: list[str], max_pinned_blocks: int, proxy: CountingProxy | None,
    sample_interval_s: float = 0.1,
) -> tuple[list[BlockRecord], list[SummaryRow]]:
    """ONE process for every round and every block.

    `FORKYARD_FORK_BLOCK_NUMBER` is deliberately NOT set: it pins the whole
    process and disables the tip follower, which is a different feature
    entirely. Every session here names its own block in the open request.

    The process survives round 2 on purpose — that is what makes round 2
    "warm", and the comparison it supports is against Anvil, which has no
    way to keep anything across a process it must respawn."""
    pre_existing = process_pids("forkyard")
    env = {
        **os.environ,
        "RPC_URL": rpc_url,
        "FORKYARD_PORT": str(FORKYARD_PORT),
        "FORKYARD_MCP_HTTP_PORT": str(FORKYARD_MCP_PORT),
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
    base_url = f"http://127.0.0.1:{FORKYARD_PORT}"
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
    """N processes, grouped B ways by block.

    Not N/B and not B: `--fork-block-number` is per process, so B blocks
    already forces B processes, and an Anvil instance is Anvil's only unit
    of isolation, so N isolated agents forces one each. Both rounds spawn a
    fresh set — Anvil has nothing to keep warm between them, which is
    exactly the asymmetry being measured. Ports are drawn from a single
    monotonic counter and never reused: a killed Anvil's port sits in
    TIME_WAIT and would fail the next bind."""
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
    concurrently, kill it.

    The spawn is charged in full to every agent in the group — each of them
    really did wait for it before it could work. The kill is charged to the
    group's lowest agent id only and recorded as 0 ms for the rest, since
    exactly one teardown happened and inflating it N/B-fold would flatter
    the isolated arm."""
    start = time.monotonic()
    try:
        owner = AnvilBackend(port, rpc_url, block, startup_timeout_s=startup_timeout_s)
        spawn: ActionResult = ("acquire", (time.monotonic() - start) * 1000, True, "")
    except Exception as e:
        spawn = ("acquire", (time.monotonic() - start) * 1000, False, repr(e)[:_MAX_ERROR_CHARS])
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
                        repr(e)[:_MAX_ERROR_CHARS])
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
    """B processes, N/B agents each — and NO isolation within a group.

    Recorded because it is the honest cheap alternative an operator would
    reach for, and because its upstream cost is the floor Anvil could reach
    if isolation were free to give up. It is not a peer of the `anvil` arm:
    every agent in a group shares one mutable state, so a single write by
    one of them is visible to all. Never quote it as a comparison unless
    that sentence is quoted with it."""
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


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def write_records(out: IO[str], records: list[BlockRecord]) -> None:
    writer = csv.DictWriter(out, fieldnames=FIELDS)
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


def main(argv: list[str] | None = None) -> None:
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
    summary_path = args.out.rsplit(".", 1)[0] + ".summary.csv"
    ports = itertools.count(ANVIL_BASE_PORT)

    try:
        with open(args.out, "w", newline="") as f, open(summary_path, "w", newline="") as sf:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
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


if __name__ == "__main__":
    main()
