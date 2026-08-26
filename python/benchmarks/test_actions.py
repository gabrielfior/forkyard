from actions import TOKENS, UNISWAP_V2_ROUTER, WETH


def test_tokens_registry_has_dai_with_expected_slot():
    assert TOKENS["DAI"]["address"] == "0x6B175474E89094C44Da98b954EedeAC495271d0"
    assert TOKENS["DAI"]["balance_slot"] == 2


def test_uniswap_constants_are_checksummed_mainnet_addresses():
    from web3 import Web3
    assert Web3.to_checksum_address(UNISWAP_V2_ROUTER) == UNISWAP_V2_ROUTER
    assert Web3.to_checksum_address(WETH) == WETH
