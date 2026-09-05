import random

import agent as agent_module
from actions import TOKENS, WETH
from agent import ActionRecord, run_agent


class FakeBackend:
    """Records every backend call instead of touching a real RPC endpoint,
    so this test proves the ordering/dependency logic in run_agent without
    needing a live forkyard or anvil process."""

    name = "fake"

    def __init__(self):
        self.calls: list[str] = []

    def web3(self):
        class FakeEth:
            chain_id = 1
            gas_price = 1_000_000_000

            def get_balance(self, address):
                return 0

            def get_transaction_count(self, address):
                return 0

        class FakeW3:
            eth = FakeEth()

        return FakeW3()

    def set_native_balance(self, address, wei):
        self.calls.append("set_native_balance")

    def set_storage(self, address, slot_hex, value_hex):
        self.calls.append("set_storage")

    def discard(self):
        self.calls.append("discard")


def test_run_agent_always_ends_with_discard_and_starts_with_acquire_then_funding():
    backend = FakeBackend()
    rng = random.Random(42)

    records = run_agent(
        lambda: backend, "fake", rng,
        agent_id=0, block_height=20_000_000, num_agents=1, num_actions=3,
    )

    assert all(isinstance(r, ActionRecord) for r in records)
    assert records[0].action == "acquire"
    assert records[1].action == "set_balance"
    assert records[-1].action == "discard"
    assert len(records) == 1 + 1 + 3 + 1  # acquire + funding + num_actions + discard
    assert all(r.block_height == 20_000_000 and r.num_agents == 1 and r.agent_id == 0 for r in records)


def test_run_agent_respects_the_fund_approve_swap_dependency_order():
    backend = FakeBackend()
    rng = random.Random(42)

    records = run_agent(
        lambda: backend, "fake", rng,
        agent_id=0, block_height=20_000_000, num_agents=1, num_actions=30,
    )
    actions = [r.action for r in records]

    first_fund_idx = next((i for i, a in enumerate(actions) if a == "fund_token"), None)
    first_approve_idx = next((i for i, a in enumerate(actions) if a == "approve"), None)
    first_swap_idx = next((i for i, a in enumerate(actions) if a == "swap_token_for_token"), None)

    if first_approve_idx is not None:
        assert first_fund_idx is not None and first_fund_idx < first_approve_idx, \
            "approve appeared before any fund_token"
    if first_swap_idx is not None:
        assert first_approve_idx is not None and first_approve_idx < first_swap_idx, \
            "swap_token_for_token appeared before any approve"


def test_swap_token_for_token_never_self_swaps(monkeypatch):
    """A DAI->DAI self-swap reverts every time. With a single-entry TOKENS
    registry the old `or [token_in]` fallback guaranteed exactly that, so
    the action could never once succeed."""
    seen: list[tuple[str, str]] = []
    real_swap = agent_module.swap_token_for_token

    def spy(backend, signer_key, token_in, token_out, amount_in, nonce):
        seen.append((token_in, token_out))
        return real_swap(backend, signer_key, token_in, token_out, amount_in, nonce)

    monkeypatch.setattr(agent_module, "swap_token_for_token", spy)

    for seed in range(20):
        run_agent(
            FakeBackend, "fake", random.Random(seed),
            agent_id=0, block_height=20_000_000, num_agents=1, num_actions=40,
        )

    assert seen, "no swap_token_for_token action was ever chosen across 20 seeds"
    known = {t["address"] for t in TOKENS.values()} | {WETH}
    for token_in, token_out in seen:
        assert token_out != token_in, f"self-swap on {token_in}"
        assert token_out in known, f"unknown token_out {token_out}"


def test_a_failed_send_resyncs_the_nonce_from_the_chain(monkeypatch):
    """Every tx-sending action on FakeBackend fails (no
    send_raw_transaction), so the local counter would run away to 1, 2,
    3... while the chain consumed nothing. The resync must pull it back to
    what the chain reports."""
    chain_nonce = 7

    class ResyncBackend(FakeBackend):
        def web3(self):
            w3 = super().web3()
            # Instance attribute, so it isn't bound — takes just `address`.
            w3.eth.get_transaction_count = lambda address: chain_nonce
            return w3

    nonces: list[int] = []
    real_transfer = agent_module.transfer

    def spy(backend, signer_key, to, value, nonce):
        nonces.append(nonce)
        return real_transfer(backend, signer_key, to, value, nonce)

    monkeypatch.setattr(agent_module, "transfer", spy)
    run_agent(
        ResyncBackend, "fake", random.Random(0),
        agent_id=0, block_height=20_000_000, num_agents=1, num_actions=40,
    )

    assert len(nonces) >= 2, "expected several transfers across 40 actions"
    # Every transfer after the first sees the resynced value, never a
    # locally-incremented one that the chain never accepted.
    assert nonces[1:] == [chain_nonce] * (len(nonces) - 1), nonces


def test_run_agent_records_carry_an_error_string_for_failed_actions():
    # FakeBackend has no send_raw_transaction, so every tx-sending action
    # fails — which is exactly what makes the error field observable here.
    records = run_agent(
        FakeBackend, "fake", random.Random(42),
        agent_id=0, block_height=20_000_000, num_agents=1, num_actions=20,
    )

    failed = [r for r in records if not r.ok]
    assert failed, "expected the fake backend's tx sends to fail"
    assert all(r.error for r in failed), "every failed record must carry a diagnostic"
    assert all(r.error == "" for r in records if r.ok)


def test_each_episode_acquires_its_own_environment_and_discards_it():
    """The churn workload: an agent that forks, does a little, throws the
    fork away, and does it again. Every episode must pay its own
    acquisition — reusing one environment across episodes would measure
    something neither backend supports after a discard."""
    acquired: list[FakeBackend] = []

    def make_backend():
        backend = FakeBackend()
        acquired.append(backend)
        return backend

    records = run_agent(
        make_backend, "fake", random.Random(0),
        agent_id=0, block_height=20_000_000, num_agents=1, num_actions=2, episodes=4,
    )

    assert len(acquired) == 4, "one environment per episode"
    assert [r.action for r in records].count("acquire") == 4
    assert [r.action for r in records].count("discard") == 4
    # Per episode: acquire + set_balance + 2 actions + discard.
    assert len(records) == 4 * (1 + 1 + 2 + 1)
    assert records[0].action == "acquire" and records[-1].action == "discard"
    assert all(b.calls.count("discard") == 1 for b in acquired)


def test_a_failed_acquisition_is_recorded_and_ends_that_episode_only():
    """An Anvil that never comes up must land in the data as a failed
    `acquire` row, not as a crashed sweep — and must not stop the agent's
    remaining episodes."""
    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("anvil did not become ready in 20s")
        return FakeBackend()

    records = run_agent(
        flaky, "anvil", random.Random(0),
        agent_id=3, block_height=20_000_000, num_agents=2, num_actions=2, episodes=2,
    )

    failed = [r for r in records if r.action == "acquire" and not r.ok]
    assert len(failed) == 1
    assert "did not become ready" in failed[0].error
    assert failed[0].backend == "anvil" and failed[0].agent_id == 3
    # The failed episode contributes its acquire row and nothing else; the
    # second episode runs in full.
    assert len(records) == 1 + (1 + 1 + 2 + 1)


def test_acquire_records_carry_the_backend_name_even_though_no_backend_exists():
    records = run_agent(
        FakeBackend, "forkyard", random.Random(0),
        agent_id=0, block_height=20_000_000, num_agents=1, num_actions=1,
    )
    assert records[0].action == "acquire"
    assert records[0].backend == "forkyard"
    assert records[0].elapsed_ms >= 0


def test_contracts_switch_the_agent_to_the_read_only_workload():
    """The state-overlap workload must not sign, fund or transact — any of
    those would put per-agent-unique state back into the traffic the run is
    trying to attribute to sharing."""
    backend = FakeBackend()
    contracts = ["0x" + "11" * 20, "0x" + "22" * 20]

    records = run_agent(
        lambda: backend, "fake", random.Random(0),
        agent_id=0, block_height=20_000_000, num_agents=1, num_actions=4,
        contracts=contracts,
    )

    labels = [r.action for r in records]
    assert labels == ["acquire", "read_contract", "read_contract", "read_contract", "read_contract", "discard"]
    assert "set_native_balance" not in backend.calls, "a read workload must not fund anything"
    assert backend.calls == ["discard"]


def test_read_workload_cycles_through_the_agents_own_contracts(monkeypatch):
    seen: list[str] = []

    def spy(backend, address, data_hex):
        seen.append(address)
        return ("read_contract", 0.0, True, "")

    monkeypatch.setattr(agent_module, "read_contract", spy)
    contracts = ["0xaaa", "0xbbb"]
    run_agent(
        FakeBackend, "fake", random.Random(0),
        agent_id=0, block_height=20_000_000, num_agents=1, num_actions=5,
        contracts=contracts,
    )

    assert seen == ["0xaaa", "0xbbb", "0xaaa", "0xbbb", "0xaaa"]
