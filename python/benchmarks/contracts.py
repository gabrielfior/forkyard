"""Sources the real mainnet contracts the state-overlap workload reads, and
assigns them to agents. Uniswap V2's factory enumerates a few hundred
storage-bearing pairs via `allPairs(i)`."""

from __future__ import annotations

import concurrent.futures
import time

import requests
from eth_utils import keccak, to_checksum_address

UNISWAP_V2_FACTORY = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"

# Computed, not hardcoded: an unknown selector returns empty data, not an
# error, so a typo would fail silently.
ALL_PAIRS_SELECTOR = "0x" + keccak(b"allPairs(uint256)")[:4].hex()
GET_RESERVES_SELECTOR = "0x" + keccak(b"getReserves()")[:4].hex()


def word_to_address(word: str) -> str:
    """Low 20 bytes of an ABI-encoded word, checksummed. web3.py rejects
    lowercase hex, so skipping this turns every read into an instant
    client-side failure that reads as a very fast benchmark."""
    return to_checksum_address("0x" + word[-40:])


def _eth_call(
    rpc_url: str, to: str, data: str, block_hex: str, timeout: float = 20.0, attempts: int = 4
) -> str:
    """Retried: a few hundred of these run back-to-back during setup, and
    one throttled connection would abort the sweep before any agent ran."""
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
    """Talks to `rpc_url` directly, never through a backend or the counting
    proxy: this is setup, and must not land in the timings or call counts."""
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
    """`shared`: every agent gets the same slice, so one agent's fetch warms
    the others — in forkyard, but not across N Anvil processes. `disjoint`:
    agent i gets its own slice, the control where no sharing is possible."""
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
