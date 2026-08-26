"""Runs one simulated agent's action sequence and returns timed records.
See docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import random
from dataclasses import dataclass

from eth_account import Account

from actions import (
    TOKENS,
    UNISWAP_V2_ROUTER,
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


def run_agent(
    backend: Backend,
    rng: random.Random,
    agent_id: int,
    block_height: int,
    num_agents: int,
    num_actions: int,
    funding_eth: int = ONE_ETH,
) -> list[ActionRecord]:
    signer = Account.create()
    signer_key = signer.key.hex()
    nonce = 0
    funded_tokens: set[str] = set()
    approved_tokens: set[str] = set()

    def record(result) -> ActionRecord:
        label, elapsed_ms, ok = result
        return ActionRecord(backend.name, block_height, num_agents, agent_id, label, elapsed_ms, ok)

    records: list[ActionRecord] = [record(set_balance(backend, signer.address, funding_eth))]

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
        elif choice == "get_balance":
            records.append(record(get_balance(backend, signer.address)))
        elif choice == "swap_eth_for_token":
            records.append(record(swap_eth_for_token(backend, signer_key, token, ONE_ETH // 100, nonce)))
            nonce += 1
        elif choice == "fund_token":
            records.append(record(fund_token(backend, token, signer.address, ONE_ETH)))
            funded_tokens.add(token)
        elif choice == "approve":
            funded_token = rng.choice(list(funded_tokens))
            records.append(
                record(approve(backend, signer_key, funded_token, UNISWAP_V2_ROUTER, ONE_ETH, nonce))
            )
            nonce += 1
            approved_tokens.add(funded_token)
        elif choice == "swap_token_for_token":
            token_in = rng.choice(list(approved_tokens))
            token_out = rng.choice([t["address"] for t in TOKENS.values() if t["address"] != token_in] or [token_in])
            records.append(record(swap_token_for_token(backend, signer_key, token_in, token_out, ONE_ETH // 1000, nonce)))
            nonce += 1

    records.append(record(discard_session(backend)))
    return records
