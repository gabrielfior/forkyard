"""Sources the real mainnet contracts the state-overlap workload reads.

The question this workload asks is how much of the win comes from *sharing*:
forkyard's cache is one cache for every session, so agents reading the same
contracts should pay the upstream fetch once between them, while N Anvils
pay it N times. Agents reading disjoint contracts share nothing, and the two
architectures should converge.

Answering it needs a few hundred distinct, real, storage-bearing contracts.
Uniswap V2's factory enumerates exactly that: `allPairs(i)` for i in 0..n,
every one of them a deployed pair with code and reserves.
"""

from __future__ import annotations

import concurrent.futures
import time

import requests
from eth_utils import keccak, to_checksum_address

UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"

# Computed rather than hardcoded so a typo can't silently become "call an
# unknown selector", which returns empty data rather than an error.
ALL_PAIRS_SELECTOR = "0x" + keccak(b"allPairs(uint256)")[:4].hex()
GET_RESERVES_SELECTOR = "0x" + keccak(b"getReserves()")[:4].hex()


def word_to_address(word: str) -> str:
    """Take the low 20 bytes of an ABI-encoded word and return a *checksummed*
    address. web3.py rejects lowercase hex outright, so skipping this turns
    every read into an instant client-side failure — which looks like a very
    fast benchmark rather than a broken one."""
    return to_checksum_address("0x" + word[-40:])


def _eth_call(
    rpc_url: str, to: str, data: str, block_hex: str, timeout: float = 20.0, attempts: int = 4
) -> str:
    """Retried, because this runs a few hundred times back-to-back during
    setup and one throttled or dropped connection would otherwise abort the
    whole sweep before a single agent had run."""
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            resp = requests.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0", "id": 1, "method": "eth_call",
                    "params": [{"to": to, "data": data}, block_hex],
                },
                timeout=timeout,
            )
            resp.raise_for_status()
            payload = resp.json()
            if "error" in payload:
                raise RuntimeError(f"eth_call failed: {payload['error']}")
            return payload["result"]
        except (requests.RequestException, RuntimeError) as e:
            last = e
            if attempt < attempts - 1:
                time.sleep(0.5 * 2**attempt)
    raise RuntimeError(f"eth_call to {to} failed after {attempts} attempts: {last}")


def fetch_pair_addresses(rpc_url: str, block_height: int, count: int, workers: int = 16) -> list[str]:
    """Read `count` pair addresses straight from the upstream endpoint.

    Deliberately talks to `rpc_url` directly rather than through a backend
    or the counting proxy: this is benchmark setup, not agent work, and
    must not land in either the timings or the upstream call counts."""
    block_hex = hex(block_height)

    def one(index: int) -> str:
        data = ALL_PAIRS_SELECTOR + f"{index:064x}"
        word = _eth_call(rpc_url, UNISWAP_V2_FACTORY, data, block_hex)
        return word_to_address(word)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(one, range(count)))


def assign_contracts(
    pairs: list[str], num_agents: int, per_agent: int, overlap: str
) -> list[list[str]]:
    """Hand each agent the contracts it will read.

    `shared`: every agent gets the *same* slice, so the first agent to touch
    a pair warms it for all the others — in forkyard. In Anvil each process
    warms only itself, which is the whole comparison.

    `disjoint`: agent i gets its own slice, so no cache can be shared even
    in principle. This is the control: whatever gap survives here is not
    sharing."""
    if overlap == "shared":
        window = pairs[:per_agent]
        if len(window) < per_agent:
            raise ValueError(f"need {per_agent} pairs, got {len(window)}")
        return [list(window) for _ in range(num_agents)]
    if overlap == "disjoint":
        needed = num_agents * per_agent
        if len(pairs) < needed:
            raise ValueError(f"need {needed} pairs for {num_agents} disjoint agents, got {len(pairs)}")
        return [list(pairs[i * per_agent:(i + 1) * per_agent]) for i in range(num_agents)]
    raise ValueError(f"unknown overlap mode {overlap!r}")
