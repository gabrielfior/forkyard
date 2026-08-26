"""Backend abstraction so the same agent/action code drives forkyard's
shared-cache session model and Anvil's one-instance-per-agent model
identically. See docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md.
"""

from __future__ import annotations

import subprocess
import time
from typing import Protocol

import requests
from eth_utils import keccak
from web3 import Web3


def erc20_balance_slot(holder: str, mapping_slot: int) -> bytes:
    """Solidity's mapping storage slot formula: keccak256(bytes32(key) ++
    bytes32(mapping_slot)). Standard for a simple `mapping(address =>
    uint256) balances` declared at `mapping_slot` — not valid for a proxy
    contract with a different layout (that's why USDC is excluded from
    the default TOKENS registry — see actions.py)."""
    key = int(holder, 16).to_bytes(32, "big")
    slot = mapping_slot.to_bytes(32, "big")
    return keccak(key + slot)


class Backend(Protocol):
    name: str

    def web3(self) -> Web3: ...
    def set_native_balance(self, address: str, wei: int) -> None: ...
    def set_storage(self, address: str, slot_hex: str, value_hex: str) -> None: ...
    def discard(self) -> None: ...


class ForkyardBackend:
    """One forkyard session, opened against an already-running forkyard
    process's shared cache. `discard()` calls forkyard_discard rather than
    tearing down any process — the process outlives every session."""

    name = "forkyard"

    def __init__(self, session_url: str):
        self._w3 = Web3(Web3.HTTPProvider(session_url))

    def web3(self) -> Web3:
        return self._w3

    def set_native_balance(self, address: str, wei: int) -> None:
        self._w3.manager.request_blocking("forkyard_setBalance", [address, hex(wei)])

    def set_storage(self, address: str, slot_hex: str, value_hex: str) -> None:
        self._w3.manager.request_blocking("forkyard_setStorageAt", [address, slot_hex, value_hex])

    def discard(self) -> None:
        self._w3.manager.request_blocking("forkyard_discard", [])


class AnvilBackend:
    """One standalone Anvil instance, forked at a specific block, owned
    entirely by this agent. `discard()` kills the process — Anvil has no
    lighter-weight session-close concept than "the instance is the
    session", so tearing it down is the fair equivalent action."""

    name = "anvil"

    def __init__(self, port: int, fork_url: str, fork_block_number: int, startup_timeout_s: float = 20.0):
        try:
            self._process = subprocess.Popen(
                [
                    "anvil",
                    "--fork-url", fork_url,
                    "--fork-block-number", str(fork_block_number),
                    "--port", str(port),
                    "--silent",
                ],
            )
        except FileNotFoundError as e:
            raise RuntimeError(
                "the `anvil` binary was not found on PATH — install Foundry "
                "(https://book.getfoundry.sh/getting-started/installation) before running the Anvil backend"
            ) from e
        self._url = f"http://127.0.0.1:{port}"
        self._wait_until_ready(startup_timeout_s)
        self._w3 = Web3(Web3.HTTPProvider(self._url))

    def _wait_until_ready(self, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                resp = requests.post(
                    self._url,
                    json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
                    timeout=1,
                )
                if resp.ok:
                    return
            except requests.RequestException as e:
                last_error = e
            time.sleep(0.2)
        raise RuntimeError(f"anvil on {self._url} did not become ready in {timeout_s}s: {last_error}")

    def web3(self) -> Web3:
        return self._w3

    def set_native_balance(self, address: str, wei: int) -> None:
        self._w3.manager.request_blocking("anvil_setBalance", [address, hex(wei)])

    def set_storage(self, address: str, slot_hex: str, value_hex: str) -> None:
        self._w3.manager.request_blocking("anvil_setStorageAt", [address, slot_hex, value_hex])

    def discard(self) -> None:
        self._process.terminate()
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)
