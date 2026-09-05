"""Benchmarks of the architectural difference: branching, checkpoint cost and writer density."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import os
import subprocess
import sys
import threading
import time

from actions import (
    ActionResult,
    TOKENS,
    UNISWAP_V2_ROUTER,
    _timed,
    approve,
    fund_token,
    set_balance,
    swap_eth_for_token,
    transfer,
)
from backend import AnvilBackend, Backend, ForkyardBackend, open_forkyard_session
from bench_common import (
    DEFAULT_BLOCK_HEIGHT,
    MAX_ERROR_CHARS,
    PortAllocator,
    RssSampler,
    forkyard_process,
    parse_int_list,
    process_pids,
)
from dataclasses import dataclass
from eth_account import Account
from eth_utils import keccak
from rpc_proxy import CountingProxy
from run_benchmark import _terminate, _wait_for_forkyard
from typing import Callable, IO
from web3 import Web3


# --- bench_branching: Exploring K what-ifs from one shared starting state.

BRANCHING_FIELDS = [
    "arm", "branches", "phase", "branch_id",
    "elapsed_ms", "ok", "error", "isolation_violations", "jsonrpc_calls",
]


ARMS = ("forkyard-branch", "anvil-snapshot", "anvil-processes")


DEFAULT_BRANCHES = [2, 4, 8, 16, 32]


BRANCHING_FORKYARD_PORT = 18650


BRANCHING_FORKYARD_MCP_PORT = 18651


BRANCHING_ANVIL_BASE_PORT = 19700


ANVIL_STARTUP_TIMEOUT_S = 120.0


SIGNER_KEY = "0x" + keccak(b"forkyard-bench-branching-signer").hex()


SIGNER_ADDRESS = Account.from_key(SIGNER_KEY).address


DAI = TOKENS["DAI"]["address"]


PREFIX_FUNDING_WEI = 100 * 10**18


PREFIX_TOKEN_WEI = 10_000 * 10**18


PREFIX_SWAP_WEI = 10**16


PREFIX_TRANSFER_WEI = 10**15


MARKER_ACCOUNT = Web3.to_checksum_address("0x00000000000000000000000000000000c0ffee00")


PARENT_MARKER_WEI = 7 * 10**18


PREFIX_STEPS = ("set_balance", "fund_token", "approve", "swap_eth_for_token", "transfer")


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


def _branching_row(r: Record) -> dict[str, object]:
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
    writer = csv.DictWriter(out, fieldnames=BRANCHING_FIELDS)
    writer.writeheader()
    writer.writerows(_branching_row(r) for r in records)


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


def chain_nonce(backend: Backend) -> int:
    """The signer's nonce on the fork, read rather than assumed to be 0."""
    try:
        return backend.web3().eth.get_transaction_count(SIGNER_ADDRESS)
    except Exception:
        return 0


def _advance_nonce(backend: Backend, result: ActionResult, nonce: int) -> int:
    """Same rule agent.py uses: a *reverted* tx still burns its nonce, so
    the increment is unconditional, but a tx rejected before execution
    (nonce/balance validation) burns nothing and would leave the local."""
    nonce += 1
    if result[2]:
        return nonce
    try:
        return backend.web3().eth.get_transaction_count(SIGNER_ADDRESS)
    except Exception:
        return nonce


def run_prefix(backend: Backend, count: int) -> tuple[list[ActionResult], int]:
    """The shared starting state, and the nonce a branch continues from."""
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
    """A branch's first act: read the marker the prefix wrote."""
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
    state the prefix ended in, so they all continue from the same nonce."""
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
    """Re-read every marker with all K children *simultaneously alive*."""
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
    point. The timed region covers opening the parent session, the prefix."""
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
    """One Anvil, one snapshot stack."""
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
    including every upstream fetch the prefix needs, before it can."""
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
                (time.monotonic() - create_start) * 1000, False, repr(e)[:MAX_ERROR_CHARS],
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


def start_forkyard(rpc_url: str, block_height: int) -> subprocess.Popen:
    try:
        process = subprocess.Popen(
            ["forkyard"],
            env={
                **os.environ,
                "RPC_URL": rpc_url,
                "BRANCHING_FORKYARD_PORT": str(BRANCHING_FORKYARD_PORT),
                "FORKYARD_MCP_HTTP_PORT": str(BRANCHING_FORKYARD_MCP_PORT),
                "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
            },
        )
    except FileNotFoundError as e:
        raise RuntimeError(
            "the `forkyard` binary was not found on PATH — build it with "
            "`cargo build -p forkyard --release` (it must be new enough to "
            "serve forkyard_forkFrom) and add target/release to PATH"
        ) from e
    _wait_for_forkyard(f"http://127.0.0.1:{BRANCHING_FORKYARD_PORT}")
    return process


def run_branch_sweep(
    rpc_url: str, block_height: int, branches: int, prefix_actions: int, branch_actions: int,
    ports: PortAllocator, arms: list[str],
    on_arm_start: Callable[[str], None] | None = None,
    on_arm_end: Callable[[str, list[Record]], None] | None = None,
) -> list[Record]:
    """All three arms at one K. The forkyard process is started and torn
    down *per K* rather than once for the sweep: its base cache is shared
    across sessions and would otherwise stay warm from K=2 all the way to."""
    records: list[Record] = []
    for arm in arms:
        if on_arm_start:
            on_arm_start(arm)
        if arm == "forkyard-branch":
            process = start_forkyard(rpc_url, block_height)
            try:
                arm_records = run_forkyard_arm(
                    f"http://127.0.0.1:{BRANCHING_FORKYARD_PORT}", branches, prefix_actions, branch_actions
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


def branching_main() -> None:
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
            writer = csv.DictWriter(f, fieldnames=BRANCHING_FIELDS)
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
                    writer.writerows(_branching_row(r) for r in records)
                    f.flush()

                run_branch_sweep(
                    rpc_url, args.block_height, branches,
                    args.prefix_actions, args.branch_actions, ports, arms,
                    on_arm_start=on_arm_start, on_arm_end=on_arm_end,
                )
    finally:
        if proxy:
            proxy.stop()


# --- bench_checkpoint: Checkpoint cost against the amount of state that has been touched.

CHECKPOINT_FIELDS = ["backend", "operation", "state_size", "elapsed_ms", "blob_bytes", "ok", "error"]


DEFAULT_STATE_SIZES = [100, 1000, 10000]


CHECKPOINT_FORKYARD_PORT = 18600


CHECKPOINT_FORKYARD_MCP_PORT = 18601


CHECKPOINT_ANVIL_BASE_PORT = 19200


DIRTY_CONTRACT = "0x6B175474E89094C44Da98b954EedeAC495271d0F"  # DAI


@dataclass
class Sample:
    backend: str
    operation: str
    state_size: int
    elapsed_ms: float
    # 0 for every checkpoint that never materialises a blob — evm_snapshot/
    # evm_revert (in-memory) and forkyard's fork/discard (pointer copy).
    # Distinguishing "no blob" from "small blob" is the point of the column.
    blob_bytes: int
    ok: bool
    error: str = ""


def _checkpoint_row(s: Sample) -> dict[str, object]:
    return {
        "backend": s.backend,
        "operation": s.operation,
        "state_size": s.state_size,
        "elapsed_ms": s.elapsed_ms,
        "blob_bytes": s.blob_bytes,
        "ok": s.ok,
        "error": s.error,
    }


def blob_size_bytes(blob: object) -> int:
    """Bytes on the wire for whatever `anvil_dumpState` hands back. Foundry
    has returned both a `0x…` hex string and (older builds) a JSON object,
    and the point of the column is the *magnitude*, so measure the encoded
    form either way rather than trusting one shape."""
    if isinstance(blob, str):
        body = blob[2:] if blob.startswith(("0x", "0X")) else blob
        return len(body) // 2
    if isinstance(blob, (bytes, bytearray)):
        return len(blob)
    if blob is None:
        return 0
    return len(str(blob).encode())


def _measure(
    backend: str, operation: str, state_size: int, fn: Callable[[], int]
) -> Sample:
    """`fn` returns the blob size its operation moved (0 when there is no
    blob), so the size is captured inside the same call that produced it."""
    start = time.monotonic()
    try:
        blob_bytes, ok, error = fn(), True, ""
    except Exception as e:
        blob_bytes, ok, error = 0, False, repr(e)[:MAX_ERROR_CHARS]
    elapsed_ms = (time.monotonic() - start) * 1000
    return Sample(backend, operation, state_size, elapsed_ms, blob_bytes, ok, error)


def slot_hex(index: int) -> str:
    return "0x" + index.to_bytes(32, "big").hex()


def value_hex(index: int) -> str:
    # Non-zero: a zero word is a storage *delete* on some backends and would
    # shrink rather than grow the state we are trying to size.
    return "0x" + (index + 1).to_bytes(32, "big").hex()


def touch_slots(set_storage: Callable[[str, str, str], None], count: int) -> None:
    """Dirty `count` distinct storage slots one RPC call at a time. Both
    backends only accept single-slot writes, so this is the same work on
    both sides; at count=10000 it is the dominant part of the script's
    runtime and is deliberately outside every timed region."""
    for i in range(count):
        set_storage(DIRTY_CONTRACT, slot_hex(i), value_hex(i))


def measure_anvil(
    rpc_url: str, block_height: int, state_size: int, port: int, repeats: int,
    startup_timeout_s: float = 60.0,
) -> list[Sample]:
    """A fresh Anvil per state size: reusing one instance would carry the
    previous size's dirty slots into the next measurement, so the sweep
    would measure a cumulative total rather than the size on the row."""
    backend = AnvilBackend(port, rpc_url, block_height, startup_timeout_s=startup_timeout_s)
    samples: list[Sample] = []
    try:
        touch_slots(backend.set_storage, state_size)
        w3 = backend.web3()
        for _ in range(repeats):
            snapshot_id: list[str] = []

            def take_snapshot() -> int:
                snapshot_id.append(w3.manager.request_blocking("evm_snapshot", []))
                return 0

            samples.append(_measure("anvil", "snapshot", state_size, take_snapshot))

            def revert() -> int:
                # A failed snapshot leaves nothing to revert to; raising here
                # records the revert as failed rather than silently timing a
                # revert of some *earlier* snapshot id.
                if not snapshot_id:
                    raise RuntimeError("evm_snapshot did not return an id")
                w3.manager.request_blocking("evm_revert", [snapshot_id[0]])
                return 0

            samples.append(_measure("anvil", "revert", state_size, revert))

            blob: list[object] = []

            def dump() -> int:
                blob.append(w3.manager.request_blocking("anvil_dumpState", []))
                return blob_size_bytes(blob[0])

            samples.append(_measure("anvil", "dump", state_size, dump))

            def load() -> int:
                if not blob:
                    raise RuntimeError("anvil_dumpState did not return a blob")
                w3.manager.request_blocking("anvil_loadState", [blob[0]])
                return blob_size_bytes(blob[0])

            samples.append(_measure("anvil", "load", state_size, load))
        return samples
    finally:
        backend.discard()


def measure_forkyard(base_url: str, state_size: int, repeats: int) -> list[Sample]:
    """Dirty one session, then time branching another off the shared base.

    The dirty session stays open for the whole measurement so forkyard is
    holding the same X writes Anvil is holding when its checkpoint runs."""
    dirty = ForkyardBackend(base_url=base_url)
    samples: list[Sample] = []
    try:
        touch_slots(dirty.set_storage, state_size)
        for _ in range(repeats):
            session_url: list[str] = []

            def fork() -> int:
                session_url.append(open_forkyard_session(base_url))
                return 0

            samples.append(_measure("forkyard", "fork", state_size, fork))

            def discard() -> int:
                if not session_url:
                    raise RuntimeError("POST /session did not return a session")
                ForkyardBackend(session_url=session_url[0]).discard()
                return 0

            samples.append(_measure("forkyard", "discard", state_size, discard))
        return samples
    finally:
        dirty.discard()


def write_samples(out: IO[str], samples: list[Sample]) -> None:
    writer = csv.DictWriter(out, fieldnames=CHECKPOINT_FIELDS)
    writer.writeheader()
    writer.writerows(_checkpoint_row(s) for s in samples)


def checkpoint_main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Measure what a checkpoint costs as touched state grows. Anvil "
            "serializes: evm_snapshot/evm_revert and anvil_dumpState/"
            "anvil_loadState are timed over X dirtied storage slots, with the "
            "dump blob's size recorded. forkyard has no snapshot RPC, so its "
            "equivalent is timed instead: branching a fresh session off the "
            "shared base (POST /session) and discarding it. Expect Anvil's "
            "cost and blob to grow with X and forkyard's to stay flat."
        ),
    )
    parser.add_argument("--rpc-url", default=os.environ.get("RPC_URL"),
                        help="archive endpoint to fork from (default: $RPC_URL)")
    parser.add_argument("--block-height", type=int, default=DEFAULT_BLOCK_HEIGHT,
                        help=f"fork block for both backends (default: {DEFAULT_BLOCK_HEIGHT})")
    parser.add_argument("--state-sizes", type=parse_int_list, default=DEFAULT_STATE_SIZES,
                        help="comma-separated storage-slot counts to dirty before checkpointing "
                             f"(default: {','.join(str(x) for x in DEFAULT_STATE_SIZES)})")
    parser.add_argument("--repeats", type=int, default=3,
                        help="checkpoint cycles per state size; each emits its own rows, so a "
                             "consumer can take a median instead of trusting one sample (default: 3)")
    parser.add_argument("--backends", default="forkyard,anvil",
                        help="comma-separated subset to measure (default: both)")
    parser.add_argument("--out", default="checkpoint.csv")
    args = parser.parse_args()

    if not args.rpc_url:
        parser.error("--rpc-url is required (or set RPC_URL)")
    backends = [b.strip() for b in args.backends.split(",") if b.strip()]

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CHECKPOINT_FIELDS)
        writer.writeheader()
        f.flush()

        forkyard_proc: subprocess.Popen | None = None
        try:
            if "forkyard" in backends:
                forkyard_proc = subprocess.Popen(
                    ["forkyard"],
                    env={
                        **os.environ,
                        "RPC_URL": args.rpc_url,
                        "CHECKPOINT_FORKYARD_PORT": str(CHECKPOINT_FORKYARD_PORT),
                        "FORKYARD_MCP_HTTP_PORT": str(CHECKPOINT_FORKYARD_MCP_PORT),
                        "FORKYARD_FORK_BLOCK_NUMBER": str(args.block_height),
                    },
                )
                base_url = f"http://127.0.0.1:{CHECKPOINT_FORKYARD_PORT}"
                _wait_for_forkyard(base_url)

            for size_index, state_size in enumerate(args.state_sizes):
                if "forkyard" in backends:
                    print(f"forkyard: dirtying {state_size} slots, then {args.repeats} fork/discard cycles",
                          file=sys.stderr)
                    samples = measure_forkyard(f"http://127.0.0.1:{CHECKPOINT_FORKYARD_PORT}", state_size, args.repeats)
                    writer.writerows(_checkpoint_row(s) for s in samples)
                    f.flush()
                if "anvil" in backends:
                    print(f"anvil: dirtying {state_size} slots, then {args.repeats} snapshot/dump cycles",
                          file=sys.stderr)
                    # A fresh port per state size: the previous instance's
                    # port can still be in TIME_WAIT when the next one binds.
                    samples = measure_anvil(
                        args.rpc_url, args.block_height, state_size,
                        CHECKPOINT_ANVIL_BASE_PORT + size_index, args.repeats,
                    )
                    writer.writerows(_checkpoint_row(s) for s in samples)
                    f.flush()
        finally:
            if forkyard_proc is not None:
                _terminate(forkyard_proc)


# --- bench_writers: How many *isolated concurrent writers* fit in a gigabyte.

WRITERS_FIELDS = [
    "backend", "writers", "peak_rss_mb", "wall_clock_ms",
    "writes_per_sec", "writers_per_gb", "isolation_violations", "ok",
]


DEFAULT_WRITERS = [1, 5, 10, 25, 50]


WRITERS_FORKYARD_PORT = 18610


WRITERS_FORKYARD_MCP_PORT = 18611


WRITERS_ANVIL_BASE_PORT = 19300


SHARED_ACCOUNT = Web3.to_checksum_address("0x00000000000000000000000000000000deadbeef")


SHARED_CONTRACT = Web3.to_checksum_address("0x6B175474E89094C44Da98b954EedeAC495271d0F")  # DAI


SHARED_SLOT = "0x" + (7).to_bytes(32, "big").hex()


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


def _writers_row(r: SweepResult) -> dict[str, object]:
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
        return WriterOutcome(writer_index, 0, 0, False, repr(e)[:MAX_ERROR_CHARS])
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
        return WriterOutcome(writer_index, writes, violations, False, repr(e)[:MAX_ERROR_CHARS])
    finally:
        try:
            backend.discard()
        except Exception:
            # A failed teardown is not a failed measurement; the writes and
            # the isolation check already happened. Anvil's discard kills a
            # process that may already be gone.
            pass


def summarize(
    backend: str, writers: int, outcomes: list[WriterOutcome],
    peak_rss_mb: float, wall_clock_ms: float,
) -> SweepResult:
    """Pure reduction of one sweep's writers into the CSV row."""
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
            "WRITERS_FORKYARD_PORT": str(WRITERS_FORKYARD_PORT),
            "FORKYARD_MCP_HTTP_PORT": str(WRITERS_FORKYARD_MCP_PORT),
            "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
        },
    )
    base_url = f"http://127.0.0.1:{WRITERS_FORKYARD_PORT}"
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
    writer = csv.DictWriter(out, fieldnames=WRITERS_FIELDS)
    writer.writeheader()
    writer.writerows(_writers_row(r) for r in results)


def writers_main() -> None:
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
        writer = csv.DictWriter(f, fieldnames=WRITERS_FIELDS)
        writer.writeheader()
        f.flush()

        next_anvil_port = WRITERS_ANVIL_BASE_PORT
        for writers in args.writers:
            if "forkyard" in backends:
                print(f"forkyard: {writers} sessions x {args.rounds} rounds", file=sys.stderr)
                result = sweep_forkyard(
                    args.rpc_url, args.block_height, writers, args.rounds, args.sample_interval
                )
                writer.writerow(_writers_row(result))
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
                writer.writerow(_writers_row(result))
                f.flush()
                print(f"  peak {result.peak_rss_mb} MB, {result.writers_per_gb} writers/GB, "
                      f"{result.isolation_violations} violations", file=sys.stderr)
