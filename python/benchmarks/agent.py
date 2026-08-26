"""Runs one simulated agent's action sequence and returns timed records.
See docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import random
from dataclasses import dataclass

from eth_account import Account

from actions import (
    TOKENS,
    UNISWAP_V2_ROUTER,
    WETH,
    approve,
    discard_session,
    fund_token,
    get_balance,
    set_balance,
    swap_eth_for_token,
    swap_token_for_token,
    transfer,
)
from backend import Backend

ONE_ETH = 10**18


@dataclass
class ActionRecord:
    backend: str
    block_height: int
    num_agents: int
    agent_id: int
    action: str
    elapsed_ms: float
    ok: bool
    # `repr(exception)` (truncated) when ok is False, "" otherwise. Kept
    # last so any positional consumer of the first six fields still works.
    error: str = ""


def run_agent(
    backend: Backend,
    rng: random.Random,
    agent_id: int,
    block_height: int,
    num_agents: int,
    num_actions: int,
    funding_wei: int = ONE_ETH,
) -> list[ActionRecord]:
    signer = Account.create()
    signer_key = signer.key.hex()
    nonce = 0
    funded_tokens: set[str] = set()
    approved_tokens: set[str] = set()

    def record(result) -> ActionRecord:
        label, elapsed_ms, ok, error = result
        return ActionRecord(backend.name, block_height, num_agents, agent_id, label, elapsed_ms, ok, error)

    records: list[ActionRecord] = [record(set_balance(backend, signer.address, funding_wei))]

    def resync_nonce_if_failed(rec: ActionRecord) -> int:
        """A reverted tx still consumes its nonce, so the unconditional
        local increment is right there — resyncing just reads back the
        same value. But a tx *rejected before executing* (nonce/balance
        validation at the RPC layer) consumes nothing, and the local
        counter would then be permanently ahead, poisoning every later tx
        from this agent with nonce-too-high. Only failures pay the extra
        round-trip; the common success path stays purely local."""
        if rec.ok:
            return nonce
        try:
            return backend.web3().eth.get_transaction_count(signer.address)
        except Exception:
            return nonce  # keep the local guess rather than crash the run

    for _ in range(num_actions):
        choices = ["transfer", "get_balance", "swap_eth_for_token", "fund_token"]
        if funded_tokens:
            choices.append("approve")
        if approved_tokens:
            choices.append("swap_token_for_token")
        choice = rng.choice(choices)
        token = rng.choice(list(TOKENS.values()))["address"]

        if choice == "transfer":
            recipient = Account.create().address
            records.append(record(transfer(backend, signer_key, recipient, ONE_ETH // 100, nonce)))
            nonce += 1
            nonce = resync_nonce_if_failed(records[-1])
        elif choice == "get_balance":
            records.append(record(get_balance(backend, signer.address)))
        elif choice == "swap_eth_for_token":
            records.append(record(swap_eth_for_token(backend, signer_key, token, ONE_ETH // 100, nonce)))
            nonce += 1
            nonce = resync_nonce_if_failed(records[-1])
        elif choice == "fund_token":
            records.append(record(fund_token(backend, token, signer.address, ONE_ETH)))
            funded_tokens.add(token)
        elif choice == "approve":
            funded_token = rng.choice(list(funded_tokens))
            records.append(
                record(approve(backend, signer_key, funded_token, UNISWAP_V2_ROUTER, ONE_ETH, nonce))
            )
            nonce += 1
            nonce = resync_nonce_if_failed(records[-1])
            approved_tokens.add(funded_token)
        elif choice == "swap_token_for_token":
            token_in = rng.choice(list(approved_tokens))
            # WETH joins the candidates so a single-entry TOKENS registry
            # still yields a real, liquid pair — falling back to `token_in`
            # would make this a guaranteed-reverting self-swap. `token_in`
            # always comes from TOKENS, which never contains WETH, so this
            # list is never empty.
            candidates = [
                addr for addr in [t["address"] for t in TOKENS.values()] + [WETH] if addr != token_in
            ]
            token_out = rng.choice(candidates)
            records.append(record(swap_token_for_token(backend, signer_key, token_in, token_out, ONE_ETH // 1000, nonce)))
            nonce += 1
            nonce = resync_nonce_if_failed(records[-1])

    records.append(record(discard_session(backend)))
    return records
