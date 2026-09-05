"""Runs one simulated agent's episodes and returns timed records.
See docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable

from eth_account import Account

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


def _timed_acquire(
    make_backend: Callable[[], Backend],
) -> tuple[Backend | None, tuple[str, float, bool, str]]:
    """Time one agent's environment acquisition — forkyard: opening a
    session; Anvil: spawning a process and waiting until it answers. It was
    always inside the sweep's timed region but never had a record of its
    own, which hid the largest asymmetry between the two backends behind
    the action latencies that followed it."""
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
    own environment, runs `num_actions` random actions in it, and discards
    it again.

    `make_backend` is a factory rather than a ready-made backend because
    that acquire → use → discard cycle *is* the workload being measured:
    an agent that forks, tries something and throws the fork away pays the
    acquisition once per episode. Handing in an already-built backend
    would time only the first acquisition and then silently reuse an
    environment that neither backend still has after a discard.

    `backend_name` is passed separately rather than read off the backend
    because a failed acquisition leaves no backend to read it from, and
    that failure still has to be recorded.

    Passing `contracts` switches the agent to the read-only state-overlap
    workload: instead of the random transaction mix it reads exactly those
    contracts, so whether agents share a cache becomes the only variable
    left. See `contracts.assign_contracts`."""
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
    """Acquire, read `num_actions` contracts, discard. No signer and no
    funding: a read pays no gas, and leaving them out keeps the only
    upstream traffic the reads themselves."""
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
        # No environment, so no actions to run and nothing to discard. The
        # failed `acquire` row is the entire episode — that is how an Anvil
        # which never came up shows up in the data, instead of as a crashed
        # sweep.
        return records

    signer = Account.create()
    signer_key = signer.key.hex()
    nonce = 0
    funded_tokens: set[str] = set()
    approved_tokens: set[str] = set()

    records.append(record(set_balance(backend, signer.address, funding_wei)))

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
