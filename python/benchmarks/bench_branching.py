"""Exploring K what-ifs from one shared starting state.

This is the workload forkyard was built for, so it is the one worth
measuring head-on: an agent gets somewhere interesting (the *prefix* —
fund an account, seed a token balance, approve, swap) and then wants to
try K mutually-exclusive continuations from exactly that point.

  forkyard-branch   run the prefix once, then `forkyard_forkFrom` K
                    times. A session is `Arc<BaseSnapshot>` + overlay, so
                    branching folds the overlay into a new base and hands
                    the child a pointer; the K children are live *at the
                    same time* and cannot see each other's writes. The
                    fold is O(parent overlay), i.e. proportional to what
                    the parent touched, not to mainnet's state, and it is
                    not a serialization.

  anvil-snapshot    one Anvil, `evm_snapshot`, then per branch: run the
                    actions, `evm_revert` back. This arm is **sequential
                    by construction and that is the finding, not a
                    shortcoming of this code**: a revert invalidates every
                    snapshot taken after it, so the branches share one
                    mutable EVM and can only be visited one at a time. No
                    amount of threading makes two of them coexist.

  anvil-processes   K Anvils, each spawning and *replaying the whole
                    prefix* before it can diverge. Branches do coexist
                    here, at the cost of paying the spawn (~0.8s) and
                    every upstream fetch the prefix needs, K times over.

All three arms perform the same total work — one prefix's worth of state
plus K x B branch actions — so the wall-clock columns are comparable. The
phase rows are what explain the difference.

Two honest caveats, both visible in the data rather than hidden by it:

  * `anvil-processes` reports its prefix rows *and* a `branch_create` row
    that spans spawn + that same prefix, so its phases overlap. `total`
    is the only row that can be summed against another arm's `total`.
  * `--count-upstream` (the default) inserts a local proxy hop into every
    upstream call, which inflates every latency a little. Timings and call
    counts should come from two different runs; `--no-proxy` is the one
    to quote wall clocks from.

Isolation is asserted, not assumed. The prefix writes a known marker
balance; every branch first reads it back (proving the branch really did
inherit the parent's overlay — a fork that silently started from the
shared base reads 0 here) and then overwrites it with a value only that
branch uses. For the forkyard arm the markers are re-read *after* every
branch has finished, with all K children still alive, which is the check
Anvil's snapshot stack cannot even be asked to pass. Any disagreement is
counted in `isolation_violations`, and a run with a non-zero count means
nothing however fast it was.
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
from typing import IO, Callable

from eth_account import Account
from eth_utils import keccak
from web3 import Web3

from actions import (
    TOKENS,
    UNISWAP_V2_ROUTER,
    ActionResult,
    _timed,
    approve,
    fund_token,
    set_balance,
    swap_eth_for_token,
    transfer,
)
from backend import AnvilBackend, Backend, ForkyardBackend
from rpc_proxy import CountingProxy
from run_benchmark import _terminate, _wait_for_forkyard, parse_int_list

FIELDS = [
    "arm", "branches", "phase", "branch_id",
    "elapsed_ms", "ok", "error", "isolation_violations", "jsonrpc_calls",
]

ARMS = ("forkyard-branch", "anvil-snapshot", "anvil-processes")

DEFAULT_BLOCK_HEIGHT = 25_795_072
DEFAULT_BRANCHES = [2, 4, 8, 16, 32]

# Distinct from every other script here: run_benchmark.py owns 18555/18556
# and 19000+, bench_checkpoint.py 18600/18601 and 19200+, bench_writers.py
# 18610/18611 and 19300+.
FORKYARD_PORT = 18650
FORKYARD_MCP_PORT = 18651
ANVIL_BASE_PORT = 19700

# K concurrent `anvil --fork-url` spawns contend for CPU and for the
# upstream, and the default 20s is not enough at K=32 on a laptop. A
# branch that times out is recorded as a failed branch_create rather than
# crashing the arm, but the timeout should be generous enough that a
# timeout means something.
ANVIL_STARTUP_TIMEOUT_S = 120.0

# One fixed signer for every arm, so the prefix produces byte-identical
# transactions everywhere and the arms differ only in how they branch. A
# random key per arm would give the swaps different senders and therefore
# different (if similar) gas.
#
# Derived from a project-specific phrase rather than being a round number:
# the obvious constants (0x11..11 and friends) are real, heavily-used
# mainnet addresses. The first draft of this file used 0x11..11 and every
# prefix transaction was rejected `nonce too low` against its live nonce of
# 1926. `run_prefix` reads the fork's nonce anyway, so this is belt and
# braces — but a signer with mainnet history also has mainnet balance and
# approvals, which would quietly change what the prefix is measuring.
SIGNER_KEY = "0x" + keccak(b"forkyard-bench-branching-signer").hex()
SIGNER_ADDRESS = Account.from_key(SIGNER_KEY).address

DAI = TOKENS["DAI"]["address"]

PREFIX_FUNDING_WEI = 100 * 10**18
PREFIX_TOKEN_WEI = 10_000 * 10**18
PREFIX_SWAP_WEI = 10**16
PREFIX_TRANSFER_WEI = 10**15

# The account every arm's isolation check travels over. It is never a
# transaction sender or recipient, so nothing but the marker writes ever
# moves it. `eth_getBalance` is the channel because forkyard's per-session
# RPC has no `eth_getStorageAt` — same constraint bench_writers.py works
# under.
MARKER_ACCOUNT = Web3.to_checksum_address("0x00000000000000000000000000000000c0ffee00")
PARENT_MARKER_WEI = 7 * 10**18

_MAX_ERROR_CHARS = 200

# The action cycle each phase walks. The CSV has no per-action label
# column, so these constants are how a reader maps a row back to its
# action: rows are written in execution order, so the i-th `prefix` row of
# a branch is `PREFIX_STEPS[i % 5]` (with one trailing `parent_marker`
# write), and the i-th `branch_action` row is `BRANCH_STEPS[i % 3]` (after
# one leading `inherit_check`).
#
# The order matters: the balance has to exist before a tx can pay for
# gas, and the token has to exist before it can be approved. The set is
# chosen to give the parent a *real* overlay — a native balance, an ERC-20
# storage slot, an allowance slot, and a swap's worth of pair reserves —
# because branching cost is O(overlay) and a parent that touched nothing
# would make the fold look free for the wrong reason.
PREFIX_STEPS = ("set_balance", "fund_token", "approve", "swap_eth_for_token", "transfer")

# Every branch does the same *shape* of work with branch-specific values,
# so the K continuations genuinely diverge instead of all writing the same
# state and hiding a leak.
BRANCH_STEPS = ("marker", "transfer", "swap_eth_for_token")


@dataclass
class Record:
    arm: str
    branches: int
    # prefix | branch_create | branch_action | total
    phase: str
    # -1 for work that belongs to no single branch (the shared prefix, the
    # arm total).
    branch_id: int
    elapsed_ms: float
    ok: bool
    error: str = ""
    isolation_violations: int = 0
    # Upstream JSON-RPC calls. Only the `total` row can carry this — the
    # counting proxy is per-process, not per-thread, so there is no
    # meaningful way to attribute a call to one branch of a concurrent
    # arm. None renders as an empty cell, which is not the same claim as 0.
    jsonrpc_calls: int | None = None


def _row(r: Record) -> dict[str, object]:
    return {
        "arm": r.arm,
        "branches": r.branches,
        "phase": r.phase,
        "branch_id": r.branch_id,
        "elapsed_ms": r.elapsed_ms,
        "ok": r.ok,
        "error": r.error,
        "isolation_violations": r.isolation_violations,
        "jsonrpc_calls": "" if r.jsonrpc_calls is None else r.jsonrpc_calls,
    }


def write_records(out: IO[str], records: list[Record]) -> None:
    writer = csv.DictWriter(out, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(_row(r) for r in records)


class PortAllocator:
    """Hands out an Anvil port that this run has never used before.

    Never reuse a port inside one run: an Anvil that has been killed leaves
    its listening socket in TIME_WAIT for minutes, and the next Anvil to
    bind it either fails outright or — worse — the *client* connects to the
    corpse's socket and the branch silently measures nothing."""

    def __init__(self, base: int = ANVIL_BASE_PORT):
        self._next = base
        self._lock = threading.Lock()

    def take(self, count: int = 1) -> list[int]:
        with self._lock:
            ports = list(range(self._next, self._next + count))
            self._next += count
        return ports


# ---------------------------------------------------------------------------
# Divergence: every value a branch writes is a function of its id, so a
# leak between branches is a wrong *value*, not a missing one.
# ---------------------------------------------------------------------------


def marker_wei(branch_id: int) -> int:
    return PARENT_MARKER_WEI + (branch_id + 1) * 10**15


def branch_recipient(branch_id: int) -> str:
    return Web3.to_checksum_address(f"0x{0xB0000000 + branch_id:040x}")


def branch_transfer_wei(branch_id: int) -> int:
    return (branch_id + 1) * 10**14


def branch_swap_wei(branch_id: int) -> int:
    return (branch_id + 1) * 10**14


def read_marker(backend: Backend) -> int:
    return backend.web3().eth.get_balance(MARKER_ACCOUNT)


def fork_from(parent: Backend, base_url: str) -> str:
    """`forkyard_forkFrom` on the parent's own session endpoint: the child
    starts from the parent's current state, inherits its block height and
    receipts, and is independent of it from that instant on. Returns the
    child's session URL, ready for `ForkyardBackend(session_url=...)`."""
    result = parent.web3().manager.request_blocking("forkyard_forkFrom", [])
    return f"{base_url}/session/{result['session_id']}"


# ---------------------------------------------------------------------------
# Workload. Both phases take a `Backend`, so the same code drives a
# forkyard session, a reverted Anvil and a freshly spawned Anvil.
# ---------------------------------------------------------------------------


def chain_nonce(backend: Backend) -> int:
    """The signer's nonce on the fork, read rather than assumed to be 0.

    A deterministic key is a *real* mainnet address, and a real mainnet
    address may already have sent transactions — the fork inherits that
    history, so a prefix starting at 0 gets every transaction rejected
    `nonce too low`. One read, identical in every arm, and the failure it
    prevents is silent enough (an ok=False row, not a crash) to be worth
    spending it on."""
    try:
        return backend.web3().eth.get_transaction_count(SIGNER_ADDRESS)
    except Exception:
        return 0


def _advance_nonce(backend: Backend, result: ActionResult, nonce: int) -> int:
    """Same rule agent.py uses: a *reverted* tx still burns its nonce, so
    the increment is unconditional, but a tx rejected before execution
    (nonce/balance validation) burns nothing and would leave the local
    counter permanently ahead, poisoning every later tx. Only failures pay
    the extra round-trip."""
    nonce += 1
    if result[2]:
        return nonce
    try:
        return backend.web3().eth.get_transaction_count(SIGNER_ADDRESS)
    except Exception:
        return nonce


def run_prefix(backend: Backend, count: int) -> tuple[list[ActionResult], int]:
    """The shared starting state, and the nonce a branch continues from.

    Returns one result per action plus a trailing `parent_marker` write —
    the marker is instrumentation rather than workload, so it is recorded
    but deliberately not counted against `--prefix-actions`."""
    results: list[ActionResult] = []
    nonce = chain_nonce(backend)
    for i in range(count):
        step = PREFIX_STEPS[i % len(PREFIX_STEPS)]
        if step == "set_balance":
            results.append(set_balance(backend, SIGNER_ADDRESS, PREFIX_FUNDING_WEI))
        elif step == "fund_token":
            results.append(fund_token(backend, DAI, SIGNER_ADDRESS, PREFIX_TOKEN_WEI))
        elif step == "approve":
            results.append(approve(backend, SIGNER_KEY, DAI, UNISWAP_V2_ROUTER, PREFIX_TOKEN_WEI, nonce))
            nonce = _advance_nonce(backend, results[-1], nonce)
        elif step == "swap_eth_for_token":
            results.append(swap_eth_for_token(backend, SIGNER_KEY, DAI, PREFIX_SWAP_WEI, nonce))
            nonce = _advance_nonce(backend, results[-1], nonce)
        elif step == "transfer":
            results.append(
                transfer(backend, SIGNER_KEY, branch_recipient(0), PREFIX_TRANSFER_WEI, nonce)
            )
            nonce = _advance_nonce(backend, results[-1], nonce)
    results.append(set_balance(backend, MARKER_ACCOUNT, PARENT_MARKER_WEI))
    results[-1] = ("parent_marker", results[-1][1], results[-1][2], results[-1][3])
    return results, nonce


def check_inherited_marker(backend: Backend) -> tuple[ActionResult, int]:
    """A branch's first act: read the marker the prefix wrote.

    This is the proof that the branch started where the parent got to. A
    `forkyard_forkFrom` that had branched off the shared base instead of
    the parent's overlay would read 0 here and every latency below it
    would be measuring the wrong thing."""
    seen: list[int] = []
    result = _timed("inherit_check", lambda: seen.append(read_marker(backend)))
    # A failed read is already an ok=False row; counting it as a violation
    # too would conflate "the branch saw the wrong state" with "the branch
    # could not be reached".
    violations = 1 if result[2] and seen[0] != PARENT_MARKER_WEI else 0
    return result, violations


def branch_marker(backend: Backend, branch_id: int) -> tuple[ActionResult, int]:
    """Write this branch's own marker and read it straight back. Catches a
    leak that happens *during* the branch; the post-hoc sweep in
    `verify_isolation` catches one that happens after it."""
    want = marker_wei(branch_id)
    seen: list[int] = []

    def do() -> None:
        backend.set_native_balance(MARKER_ACCOUNT, want)
        seen.append(read_marker(backend))

    result = _timed("branch_marker", do)
    return result, (1 if result[2] and seen[0] != want else 0)


def run_branch_actions(
    backend: Backend, branch_id: int, count: int, start_nonce: int
) -> tuple[list[ActionResult], int]:
    """One branch's continuation. `start_nonce` comes from the prefix
    rather than from the chain: every arm's branch begins at exactly the
    state the prefix ended in, so they all continue from the same nonce,
    and reading it back per branch would add K round-trips that only one
    of the three arms would be paying for a real reason."""
    results: list[ActionResult] = []
    inherit_result, violations = check_inherited_marker(backend)
    results.append(inherit_result)

    nonce = start_nonce
    for i in range(count):
        step = BRANCH_STEPS[i % len(BRANCH_STEPS)]
        if step == "marker":
            result, v = branch_marker(backend, branch_id)
            results.append(result)
            violations += v
        elif step == "transfer":
            results.append(
                transfer(
                    backend, SIGNER_KEY, branch_recipient(branch_id),
                    branch_transfer_wei(branch_id), nonce,
                )
            )
            nonce = _advance_nonce(backend, results[-1], nonce)
        elif step == "swap_eth_for_token":
            results.append(
                swap_eth_for_token(backend, SIGNER_KEY, DAI, branch_swap_wei(branch_id), nonce)
            )
            nonce = _advance_nonce(backend, results[-1], nonce)
    return results, violations


def verify_isolation(parent: Backend | None, children: dict[int, Backend]) -> int:
    """Re-read every marker with all K children *simultaneously alive*.

    This is the assertion the whole benchmark exists to make, and the one
    `anvil-snapshot` structurally cannot be asked to pass: there is only
    ever one live branch on a snapshot stack, so "do the siblings see each
    other" is not a question that has an answer there. A child must still
    hold its own marker, and the parent must still hold the one it wrote
    before any child existed — neither side sees the other's later writes.
    An unreachable session counts as a violation here, because at this
    point failing to answer is indistinguishable from having lost the
    state."""
    violations = 0
    for branch_id, child in children.items():
        try:
            if read_marker(child) != marker_wei(branch_id):
                violations += 1
        except Exception:
            violations += 1
    if parent is not None:
        try:
            if read_marker(parent) != PARENT_MARKER_WEI:
                violations += 1
        except Exception:
            violations += 1
    return violations


# ---------------------------------------------------------------------------
# Arms
# ---------------------------------------------------------------------------


def _phase_rows(
    arm: str, branches: int, phase: str, branch_id: int, results: list[ActionResult]
) -> list[Record]:
    return [
        Record(arm, branches, phase, branch_id, elapsed_ms, ok, error)
        for _label, elapsed_ms, ok, error in results
    ]


def _total_row(arm: str, branches: int, records: list[Record], wall_ms: float, violations: int) -> Record:
    """A run with a leak is not a passing run, however fast it was — the
    same rule bench_writers.py applies to `writers_per_gb`."""
    return Record(
        arm, branches, "total", -1, round(wall_ms, 1),
        ok=violations == 0 and all(r.ok for r in records),
        error="",
        isolation_violations=violations,
    )


def _discard_quietly(backend: Backend | None) -> None:
    if backend is None:
        return
    try:
        backend.discard()
    except Exception:
        # Teardown failure is not measurement failure: the branch's actions
        # and its isolation check already happened, and Anvil's discard
        # kills a process that may already be gone.
        pass


def run_forkyard_arm(
    base_url: str, branches: int, prefix_actions: int, branch_actions: int
) -> list[Record]:
    """Prefix once, `forkyard_forkFrom` K times, then run all K children
    concurrently — they are live at the same time, which is the whole
    point. The timed region covers opening the parent session, the prefix,
    the K forks and the concurrent branch work; it excludes the discards
    and the post-hoc isolation sweep, which are instrumentation."""
    arm = "forkyard-branch"
    records: list[Record] = []
    children: dict[int, Backend] = {}
    parent: Backend | None = None
    violations = 0

    start = time.monotonic()
    try:
        parent = ForkyardBackend(base_url=base_url)
        prefix_results, nonce = run_prefix(parent, prefix_actions)
        records += _phase_rows(arm, branches, "prefix", -1, prefix_results)

        child_urls: dict[int, str] = {}
        for branch_id in range(branches):
            created: list[str] = []
            assert parent is not None
            result = _timed(
                "fork_from", lambda p=parent: created.append(fork_from(p, base_url))
            )
            records.append(Record(arm, branches, "branch_create", branch_id, result[1], result[2], result[3]))
            if created:
                child_urls[branch_id] = created[0]

        # Constructing the Web3 wrapper is local work, not a round-trip, so
        # it stays out of the branch_create timing above.
        children = {b: ForkyardBackend(session_url=url) for b, url in child_urls.items()}

        if children:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(children)) as pool:
                futures = {
                    branch_id: pool.submit(
                        run_branch_actions, child, branch_id, branch_actions, nonce
                    )
                    for branch_id, child in children.items()
                }
                for branch_id, future in futures.items():
                    results, branch_violations = future.result()
                    records += _phase_rows(arm, branches, "branch_action", branch_id, results)
                    violations += branch_violations

        wall_ms = (time.monotonic() - start) * 1000
        violations += verify_isolation(parent, children)
    finally:
        for child in children.values():
            _discard_quietly(child)
        _discard_quietly(parent)

    records.append(_total_row(arm, branches, records, wall_ms, violations))
    return records


def run_anvil_snapshot_arm(
    rpc_url: str, block_height: int, port: int, branches: int,
    prefix_actions: int, branch_actions: int,
) -> list[Record]:
    """One Anvil, one snapshot stack.

    Sequential is not a limitation of this code, it is the shape of the
    API: `evm_revert` invalidates every snapshot taken after the one it
    restores, so the K branches share a single mutable EVM and have to be
    visited one after another. Running them on a thread pool would not
    make two branches coexist, it would make them corrupt each other. The
    `branch_create` row is `evm_snapshot` + `evm_revert` for that branch,
    which is why it is emitted after the branch's actions rather than
    before them."""
    arm = "anvil-snapshot"
    records: list[Record] = []
    violations = 0
    backend: Backend | None = None

    start = time.monotonic()
    try:
        backend = AnvilBackend(
            port, rpc_url, block_height, startup_timeout_s=ANVIL_STARTUP_TIMEOUT_S
        )
        prefix_results, nonce = run_prefix(backend, prefix_actions)
        records += _phase_rows(arm, branches, "prefix", -1, prefix_results)

        for branch_id in range(branches):
            snapshot_id: list[str] = []
            assert backend is not None
            snapshot_result = _timed(
                "evm_snapshot",
                lambda b=backend: snapshot_id.append(b.web3().manager.request_blocking("evm_snapshot", [])),
            )

            results, branch_violations = run_branch_actions(
                backend, branch_id, branch_actions, nonce
            )
            records += _phase_rows(arm, branches, "branch_action", branch_id, results)
            violations += branch_violations

            def revert(b: Backend = backend) -> None:
                # Reverting to whatever id happens to be lying around would
                # roll back to some *earlier* branch and record it as a
                # success; the next branch would then start from the wrong
                # state and its inherit_check would be the only hint.
                if not snapshot_id:
                    raise RuntimeError("evm_snapshot did not return an id")
                b.web3().manager.request_blocking("evm_revert", [snapshot_id[0]])

            revert_result = _timed("evm_revert", revert)
            records.append(Record(
                arm, branches, "branch_create", branch_id,
                snapshot_result[1] + revert_result[1],
                snapshot_result[2] and revert_result[2],
                snapshot_result[3] or revert_result[3],
            ))

        wall_ms = (time.monotonic() - start) * 1000
    finally:
        _discard_quietly(backend)

    records.append(_total_row(arm, branches, records, wall_ms, violations))
    return records


def run_anvil_processes_arm(
    rpc_url: str, block_height: int, ports: list[int], branches: int,
    prefix_actions: int, branch_actions: int,
) -> list[Record]:
    """K Anvils, concurrently. The branches really do coexist here — the
    price is that each one spawns a process and replays the entire prefix,
    including every upstream fetch the prefix needs, before it can
    diverge by a single action.

    `branch_create` therefore spans spawn + prefix, and *overlaps* the
    per-branch `prefix` rows. Only the `total` row is comparable across
    arms."""
    arm = "anvil-processes"

    def one_branch(branch_id: int, port: int) -> tuple[list[Record], int]:
        records: list[Record] = []
        backend: Backend | None = None
        create_start = time.monotonic()
        try:
            backend = AnvilBackend(
                port, rpc_url, block_height, startup_timeout_s=ANVIL_STARTUP_TIMEOUT_S
            )
        except Exception as e:
            # A branch whose Anvil never came up is a failed branch_create,
            # not a crashed arm — at K=32 that distinction is what keeps a
            # sweep's worth of data.
            return [Record(
                arm, branches, "branch_create", branch_id,
                (time.monotonic() - create_start) * 1000, False, repr(e)[:_MAX_ERROR_CHARS],
            )], 0
        try:
            prefix_results, nonce = run_prefix(backend, prefix_actions)
            records += _phase_rows(arm, branches, "prefix", branch_id, prefix_results)
            records.append(Record(
                arm, branches, "branch_create", branch_id,
                (time.monotonic() - create_start) * 1000,
                all(r.ok for r in records), "",
            ))
            results, violations = run_branch_actions(backend, branch_id, branch_actions, nonce)
            records += _phase_rows(arm, branches, "branch_action", branch_id, results)
            return records, violations
        finally:
            _discard_quietly(backend)

    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=branches) as pool:
        futures = [pool.submit(one_branch, i, ports[i]) for i in range(branches)]
        per_branch = [f.result() for f in futures]
    wall_ms = (time.monotonic() - start) * 1000

    records = [r for branch_records, _ in per_branch for r in branch_records]
    violations = sum(v for _, v in per_branch)
    records.append(_total_row(arm, branches, records, wall_ms, violations))
    return records


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def start_forkyard(rpc_url: str, block_height: int) -> subprocess.Popen:
    try:
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
    except FileNotFoundError as e:
        raise RuntimeError(
            "the `forkyard` binary was not found on PATH — build it with "
            "`cargo build -p forkyard --release` (it must be new enough to "
            "serve forkyard_forkFrom) and add target/release to PATH"
        ) from e
    _wait_for_forkyard(f"http://127.0.0.1:{FORKYARD_PORT}")
    return process


def run_branch_sweep(
    rpc_url: str, block_height: int, branches: int, prefix_actions: int, branch_actions: int,
    ports: PortAllocator, arms: list[str],
    on_arm_start: Callable[[str], None] | None = None,
    on_arm_end: Callable[[str, list[Record]], None] | None = None,
) -> list[Record]:
    """All three arms at one K. The forkyard process is started and torn
    down *per K* rather than once for the sweep: its base cache is shared
    across sessions and would otherwise stay warm from K=2 all the way to
    K=32, so every K after the first would be reading state that an
    earlier K paid for while the Anvil arms (running `--no-storage-caching`)
    refetch from cold every time. Restarting makes each K self-contained
    and its upstream count comparable with the Anvil arms beside it."""
    records: list[Record] = []
    for arm in arms:
        if on_arm_start:
            on_arm_start(arm)
        if arm == "forkyard-branch":
            process = start_forkyard(rpc_url, block_height)
            try:
                arm_records = run_forkyard_arm(
                    f"http://127.0.0.1:{FORKYARD_PORT}", branches, prefix_actions, branch_actions
                )
            finally:
                _terminate(process)
        elif arm == "anvil-snapshot":
            arm_records = run_anvil_snapshot_arm(
                rpc_url, block_height, ports.take()[0], branches, prefix_actions, branch_actions
            )
        elif arm == "anvil-processes":
            arm_records = run_anvil_processes_arm(
                rpc_url, block_height, ports.take(branches), branches, prefix_actions, branch_actions
            )
        else:
            raise ValueError(f"unknown arm {arm!r}, expected one of {ARMS}")
        if on_arm_end:
            on_arm_end(arm, arm_records)
        records += arm_records
    return records


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Measure exploring K what-ifs from one starting state, three "
            "ways: forkyard's forkyard_forkFrom (K children of one parent, "
            "all live at once), Anvil's evm_snapshot/evm_revert stack "
            "(sequential by construction — a revert invalidates later "
            "snapshots, so the branches cannot coexist), and K Anvil "
            "processes each replaying the whole prefix. Every arm does the "
            "same total work; the phase rows say where the time went. "
            "Branch isolation is asserted, not assumed: isolation_violations "
            "must be 0 for a row to mean anything."
        ),
    )
    parser.add_argument("--prefix-actions", type=int, default=5,
                        help="actions establishing the shared starting state, run once per "
                             "branch point (default: 5)")
    parser.add_argument("--branch-actions", type=int, default=3,
                        help="actions each branch runs after diverging (default: 3)")
    parser.add_argument("--branches", type=parse_int_list, default=DEFAULT_BRANCHES,
                        help="comma-separated K values to sweep (default: "
                             f"{','.join(str(k) for k in DEFAULT_BRANCHES)})")
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"),
                        help="archive endpoint to fork from (default: $RPC_URL)")
    parser.add_argument("--block-height", type=int, default=DEFAULT_BLOCK_HEIGHT,
                        help=f"fork block for every arm (default: {DEFAULT_BLOCK_HEIGHT})")
    parser.add_argument("--arms", default=",".join(ARMS),
                        help="comma-separated subset to run (default: all three)")
    parser.add_argument("--no-proxy", action="store_true",
                        help="talk to the upstream directly instead of through the counting "
                             "proxy. The proxy adds a local hop to every upstream call, so "
                             "wall clocks should be quoted from a --no-proxy run and "
                             "jsonrpc_calls from a proxied one — they are two different runs.")
    parser.add_argument("--out", default="branching.csv")
    args = parser.parse_args()

    if not args.rpc_url:
        parser.error("--rpc-url is required (or set RPC_URL)")
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    unknown = [a for a in arms if a not in ARMS]
    if unknown:
        parser.error(f"unknown arm(s) {unknown}, expected a subset of {list(ARMS)}")

    proxy = None if args.no_proxy else CountingProxy(args.rpc_url).start()
    rpc_url = proxy.url if proxy else args.rpc_url
    ports = PortAllocator()

    try:
        with open(args.out, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDS)
            writer.writeheader()
            f.flush()

            for branches in args.branches:
                def on_arm_start(arm: str, k: int = branches) -> None:
                    print(f"running {arm}: branches={k} prefix={args.prefix_actions} "
                          f"branch={args.branch_actions}", file=sys.stderr)
                    if proxy:
                        proxy.reset()

                def on_arm_end(arm: str, records: list[Record], k: int = branches) -> None:
                    if proxy:
                        # Attach the arm's whole upstream cost to its total
                        # row, then flush: a sweep that dies at K=32 keeps
                        # everything K=2..16 measured.
                        calls = proxy.snapshot().jsonrpc_calls
                        for r in records:
                            if r.phase == "total":
                                r.jsonrpc_calls = calls
                        print(f"  upstream: {calls} JSON-RPC calls", file=sys.stderr)
                    total = next((r for r in records if r.phase == "total"), None)
                    if total is not None:
                        print(f"  total: {total.elapsed_ms:.0f} ms ok={total.ok} "
                              f"violations={total.isolation_violations}", file=sys.stderr)
                    writer.writerows(_row(r) for r in records)
                    f.flush()

                run_branch_sweep(
                    rpc_url, args.block_height, branches,
                    args.prefix_actions, args.branch_actions, ports, arms,
                    on_arm_start=on_arm_start, on_arm_end=on_arm_end,
                )
    finally:
        if proxy:
            proxy.stop()


if __name__ == "__main__":
    main()
