"""Checkpoint cost against the amount of state that has been touched.

The claim: Anvil's checkpoint is a *serialization* — `anvil_dumpState`
walks the touched state into a blob and `anvil_loadState` walks it back
in, so both the time and the blob grow with how much state the
environment has written. forkyard's equivalent is a pointer copy: a new
session branches off the same shared base, so it costs the same whether
the sessions beside it have touched a hundred slots or ten thousand.

This is deliberately **not** a like-named API comparison, and it is not a
comparison of equal work. forkyard exposes no snapshot RPC at all; its
per-session JSON-RPC is chainId/blockNumber/gasPrice/getBalance/
getTransactionCount/estimateGas/sendRawTransaction/getTransactionReceipt
plus forkyard_setBalance/setStorageAt/discard. So the honest framing is
architecture against architecture:

  Anvil     dirty an instance, then serialize-and-restore *those writes*
            (evm_snapshot/evm_revert keep the blob in memory;
             anvil_dumpState/anvil_loadState materialise it).
  forkyard  dirty a session, then branch a *fresh* session off the shared
            base and throw it away (POST /session, forkyard_discard).

The asymmetry that flatters forkyard, stated plainly: the new session
does **not** carry the X writes — it branches from the base, not from the
dirtied session, so it has less to carry by construction. That is the
architectural difference rather than a measurement artifact (forkyard's
unit of work is "another fork of the same base", not "a copy of my
current state"), but a reader comparing the two `elapsed_ms` columns
should know that Anvil's number includes moving X slots and forkyard's
does not. The `state_size` sweep is what makes that legible: Anvil's
curve should bend upward with X and forkyard's should be a flat line.
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Callable, IO

from backend import AnvilBackend, ForkyardBackend, open_forkyard_session
from run_benchmark import _terminate, _wait_for_forkyard, parse_int_list

FIELDS = ["backend", "operation", "state_size", "elapsed_ms", "blob_bytes", "ok", "error"]

DEFAULT_BLOCK_HEIGHT = 25_795_072
DEFAULT_STATE_SIZES = [100, 1000, 10000]

# Fixed ports so a run of this script cannot collide with run_benchmark.py's
# 18555/18556 and 19000+ if the two are ever left running side by side.
FORKYARD_PORT = 18600
FORKYARD_MCP_PORT = 18601
ANVIL_BASE_PORT = 19200

# The address whose slots get dirtied. A real, code-bearing mainnet
# contract rather than an empty account: `anvil_dumpState` serializes an
# account's code alongside its storage, and a contract is what an agent
# would actually be writing to.
DIRTY_CONTRACT = "0x6B175474E89094C44Da98b954EedeAC495271d0F"  # DAI

_MAX_ERROR_CHARS = 200


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


def _row(s: Sample) -> dict[str, object]:
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
        blob_bytes, ok, error = 0, False, repr(e)[:_MAX_ERROR_CHARS]
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
    writer = csv.DictWriter(out, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(_row(s) for s in samples)


def main() -> None:
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
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        f.flush()

        forkyard_process: subprocess.Popen | None = None
        try:
            if "forkyard" in backends:
                forkyard_process = subprocess.Popen(
                    ["forkyard"],
                    env={
                        **os.environ,
                        "RPC_URL": args.rpc_url,
                        "FORKYARD_PORT": str(FORKYARD_PORT),
                        "FORKYARD_MCP_HTTP_PORT": str(FORKYARD_MCP_PORT),
                        "FORKYARD_FORK_BLOCK_NUMBER": str(args.block_height),
                    },
                )
                base_url = f"http://127.0.0.1:{FORKYARD_PORT}"
                _wait_for_forkyard(base_url)

            for size_index, state_size in enumerate(args.state_sizes):
                if "forkyard" in backends:
                    print(f"forkyard: dirtying {state_size} slots, then {args.repeats} fork/discard cycles",
                          file=sys.stderr)
                    samples = measure_forkyard(f"http://127.0.0.1:{FORKYARD_PORT}", state_size, args.repeats)
                    writer.writerows(_row(s) for s in samples)
                    f.flush()
                if "anvil" in backends:
                    print(f"anvil: dirtying {state_size} slots, then {args.repeats} snapshot/dump cycles",
                          file=sys.stderr)
                    # A fresh port per state size: the previous instance's
                    # port can still be in TIME_WAIT when the next one binds.
                    samples = measure_anvil(
                        args.rpc_url, args.block_height, state_size,
                        ANVIL_BASE_PORT + size_index, args.repeats,
                    )
                    writer.writerows(_row(s) for s in samples)
                    f.flush()
        finally:
            if forkyard_process is not None:
                _terminate(forkyard_process)


if __name__ == "__main__":
    main()
