"""Runs one simulated agent's episodes and returns timed ActionRecords.
See docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import random
import time
from typing import Any, Callable

from eth_account import Account
from pydantic import BaseModel, Field

from actions import (
    TOKENS,
    UNISWAP_V2_ROUTER,
    WETH,
    approve,
    discard_session,
    fund_token,
    get_balance,
    read_contract,
    set_balance,
    swap_eth_for_token,
    swap_token_for_token,
    transfer,
)
from backend import Backend
from contracts import GET_RESERVES_SELECTOR

ONE_ETH = 10**18

_MAX_ERROR_CHARS = 200


class ActionRecord(BaseModel):
    """One row of the benchmark CSV. Field order *is* the column order —
    see run_benchmark.FIELDS, which is derived from it."""

    backend: str = Field(min_length=1)
    block_height: int = Field(ge=0)
    num_agents: int = Field(ge=1)
    agent_id: int = Field(ge=-1)  # -1 marks the per-combination __total__ row
    action: str = Field(min_length=1)
    elapsed_ms: float = Field(ge=0)
    ok: bool
    # `repr(exception)` (truncated) when ok is False, "" otherwise. Last so
    # any positional consumer of the first seven fields still works.
    error: str = ""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Pydantic is keyword-only; callers here and in bench_quota.py build
        # these positionally, in field order.
        super().__init__(**dict(zip(type(self).model_fields, args)), **kwargs)


def _timed_acquire(
    make_backend: Callable[[], Backend],
) -> tuple[Backend | None, tuple[str, float, bool, str]]:
    """Time one agent's environment acquisition — forkyard: opening a
    session; Anvil: spawning a process and waiting until it answers. Its own
    record, because it is the largest asymmetry between the two backends."""
    start = time.monotonic()
    try:
        backend: Backend | None = make_backend()
        ok, error = True, ""
    except Exception as e:
        backend, ok, error = None, False, repr(e)[:_MAX_ERROR_CHARS]
    elapsed_ms = (time.monotonic() - start) * 1000
    return backend, ("acquire", elapsed_ms, ok, error)


def run_agent(
    make_backend: Callable[[], Backend],
    backend_name: str,
    rng: random.Random,
    agent_id: int,
    block_height: int,
    num_agents: int,
    num_actions: int,
    funding_wei: int = ONE_ETH,
    episodes: int = 1,
    contracts: list[str] | None = None,
) -> list[ActionRecord]:
    """Run `episodes` independent episodes for one agent: each acquires its
    own environment, runs `num_actions` random actions in it, and discards it.

    `make_backend` is a factory, not a ready-made backend, because the
    acquire → use → discard cycle *is* the workload: reusing one environment
    would time only the first acquisition. `backend_name` is passed
    separately because a failed acquisition leaves no backend to read it off.
    Passing `contracts` switches to the read-only state-overlap workload
    (see `contracts.assign_contracts`)."""
    episode = _run_read_episode if contracts else _run_episode
    kwargs = {"contracts": contracts} if contracts else {"funding_wei": funding_wei}
    records: list[ActionRecord] = []
    for _ in range(episodes):
        records.extend(
            episode(
                make_backend, backend_name, rng, agent_id, block_height,
                num_agents, num_actions, **kwargs,
            )
        )
    return records


def _run_read_episode(
    make_backend: Callable[[], Backend],
    backend_name: str,
    rng: random.Random,
    agent_id: int,
    block_height: int,
    num_agents: int,
    num_actions: int,
    contracts: list[str],
) -> list[ActionRecord]:
    """No signer and no funding: a read pays no gas, so the only upstream
    traffic is the reads themselves."""
    def record(result) -> ActionRecord:
        label, elapsed_ms, ok, error = result
        return ActionRecord(backend_name, block_height, num_agents, agent_id, label, elapsed_ms, ok, error)

    backend, acquire_result = _timed_acquire(make_backend)
    records: list[ActionRecord] = [record(acquire_result)]
    if backend is None:
        return records

    for i in range(num_actions):
        records.append(record(read_contract(backend, contracts[i % len(contracts)], GET_RESERVES_SELECTOR)))

    records.append(record(discard_session(backend)))
    return records


def _run_episode(
    make_backend: Callable[[], Backend],
    backend_name: str,
    rng: random.Random,
    agent_id: int,
    block_height: int,
    num_agents: int,
    num_actions: int,
    funding_wei: int,
) -> list[ActionRecord]:
    def record(result) -> ActionRecord:
        label, elapsed_ms, ok, error = result
        return ActionRecord(backend_name, block_height, num_agents, agent_id, label, elapsed_ms, ok, error)

    backend, acquire_result = _timed_acquire(make_backend)
    records: list[ActionRecord] = [record(acquire_result)]
    if backend is None:
        # The failed `acquire` row is the whole episode: an Anvil that never
        # came up shows up as data, not as a crashed sweep.
        return records

    signer = Account.create()
    signer_key = signer.key.hex()
    nonce = 0
    funded_tokens: set[str] = set()
    approved_tokens: set[str] = set()

    records.append(record(set_balance(backend, signer.address, funding_wei)))

    def resync_nonce_if_failed(rec: ActionRecord) -> int:
        """A tx rejected before executing (nonce/balance validation) consumes
        no nonce, so the local counter would stay permanently ahead and
        poison every later tx. Reverts do consume one, hence failures only."""
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
            # still yields a liquid pair instead of a reverting self-swap.
            candidates = [
                addr for addr in [t["address"] for t in TOKENS.values()] + [WETH] if addr != token_in
            ]
            token_out = rng.choice(candidates)
            records.append(record(swap_token_for_token(backend, signer_key, token_in, token_out, ONE_ETH // 1000, nonce)))
            nonce += 1
            nonce = resync_nonce_if_failed(records[-1])

    records.append(record(discard_session(backend)))
    return records
