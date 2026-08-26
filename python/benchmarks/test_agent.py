import random

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


def test_run_agent_always_ends_with_discard_and_starts_with_funding():
    backend = FakeBackend()
    rng = random.Random(42)

    records = run_agent(backend, rng, agent_id=0, block_height=20_000_000, num_agents=1, num_actions=3)

    assert all(isinstance(r, ActionRecord) for r in records)
    assert records[0].action == "set_balance"
    assert records[-1].action == "discard"
    assert len(records) == 1 + 3 + 1  # funding + num_actions + discard
    assert all(r.block_height == 20_000_000 and r.num_agents == 1 and r.agent_id == 0 for r in records)


def test_run_agent_respects_the_fund_approve_swap_dependency_order():
    backend = FakeBackend()
    rng = random.Random(42)

    records = run_agent(backend, rng, agent_id=0, block_height=20_000_000, num_agents=1, num_actions=30)
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
