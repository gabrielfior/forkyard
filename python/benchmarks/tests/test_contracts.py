import pytest
from eth_utils import keccak

from contracts import ALL_PAIRS_SELECTOR, GET_RESERVES_SELECTOR, assign_contracts, word_to_address

PAIRS = [f"0x{i:040x}" for i in range(20)]


def test_selectors_match_their_signatures():
    """An unknown selector returns empty data, not an error, so a wrong
    hardcoded value would fail silently."""
    assert ALL_PAIRS_SELECTOR == "0x" + keccak(b"allPairs(uint256)")[:4].hex()
    assert GET_RESERVES_SELECTOR == "0x" + keccak(b"getReserves()")[:4].hex()


def test_shared_gives_every_agent_the_same_contracts():
    assignment = assign_contracts(PAIRS, num_agents=4, per_agent=3, overlap="shared")

    assert len(assignment) == 4
    assert all(a == PAIRS[:3] for a in assignment), "shared means literally the same slice"


def test_disjoint_gives_every_agent_its_own_contracts():
    assignment = assign_contracts(PAIRS, num_agents=4, per_agent=3, overlap="disjoint")

    assert len(assignment) == 4
    flat = [c for agent in assignment for c in agent]
    assert len(set(flat)) == len(flat), "no contract may be shared between agents"
    assert all(len(a) == 3 for a in assignment)


def test_disjoint_refuses_rather_than_silently_recycling_addresses():
    """Recycling would make a 'disjoint' run quietly share state."""
    with pytest.raises(ValueError, match="need 30"):
        assign_contracts(PAIRS[:10], num_agents=10, per_agent=3, overlap="disjoint")


def test_shared_refuses_when_the_pool_is_too_small():
    with pytest.raises(ValueError, match="need 5"):
        assign_contracts(PAIRS[:3], num_agents=2, per_agent=5, overlap="shared")


def test_unknown_overlap_mode_is_rejected():
    with pytest.raises(ValueError, match="partial"):
        assign_contracts(PAIRS, num_agents=1, per_agent=1, overlap="partial")


def test_word_to_address_returns_a_checksummed_address():
    """`allPairs` answers in lowercase and web3.py refuses lowercase: the
    raw slice made every read fail client-side in under a millisecond."""
    word = "0x000000000000000000000000b4e16d0168e52d35cacd2c6185b44281ec28c9dc"
    assert word_to_address(word) == "0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc"


def test_eth_call_retries_a_transient_failure_then_succeeds(monkeypatch):
    """One throttled connection must not abort the sweep before any agent
    has run."""
    import contracts

    calls = {"n": 0}

    class Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"result": "0x" + "0" * 64}

    def flaky_post(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] < 3:
            raise contracts.requests.ConnectionError("boom")
        return Resp()

    monkeypatch.setattr(contracts.requests, "post", flaky_post)
    monkeypatch.setattr(contracts.time, "sleep", lambda s: None)

    assert contracts._eth_call("http://x", "0xto", "0xdata", "0x1") == "0x" + "0" * 64
    assert calls["n"] == 3


def test_eth_call_gives_up_with_a_diagnostic_after_exhausting_attempts(monkeypatch):
    import contracts

    def always_fails(*args, **kwargs):
        raise contracts.requests.ConnectionError("boom")

    monkeypatch.setattr(contracts.requests, "post", always_fails)
    monkeypatch.setattr(contracts.time, "sleep", lambda s: None)

    with pytest.raises(RuntimeError, match="failed after 4 attempts"):
        contracts._eth_call("http://x", "0xto", "0xdata", "0x1")
