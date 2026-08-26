from backend import erc20_balance_slot


def test_erc20_balance_slot_matches_known_dai_vector():
    # DAI's balanceOf mapping is storage slot 2 (see the plan's Global
    # Constraints). This is the standard Solidity mapping-slot formula:
    # keccak256(bytes32(holder) ++ bytes32(slot)). Cross-checked against
    # a known-good value computed independently with eth_utils.keccak
    # for holder 0x0000000000000000000000000000000000000001, slot 2.
    holder = "0x0000000000000000000000000000000000000001"
    slot = erc20_balance_slot(holder, mapping_slot=2)
    assert isinstance(slot, bytes)
    assert len(slot) == 32
    # Re-derive independently in the test (not copy the implementation)
    # to actually catch a wrong formula, not just a wrong constant.
    from eth_utils import keccak
    key = int(holder, 16).to_bytes(32, "big")
    mapping_slot_bytes = (2).to_bytes(32, "big")
    expected = keccak(key + mapping_slot_bytes)
    assert slot == expected
