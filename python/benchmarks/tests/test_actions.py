from web3 import Web3

from actions import TOKENS, UNISWAP_V2_ROUTER, WETH, _MAX_ERROR_CHARS, _timed


def test_tokens_registry_has_dai_with_expected_slot():
    assert TOKENS["DAI"]["address"] == "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    assert TOKENS["DAI"]["balance_slot"] == 2


def test_tokens_addresses_are_checksummed():
    for token_name, token_info in TOKENS.items():
        address = token_info["address"]
        checksummed = Web3.to_checksum_address(address)
        assert checksummed == address, f"{token_name} address is not checksummed: {address}"


def test_uniswap_constants_are_checksummed_mainnet_addresses():
    assert Web3.to_checksum_address(UNISWAP_V2_ROUTER) == UNISWAP_V2_ROUTER
    assert Web3.to_checksum_address(WETH) == WETH


def test_timed_reports_an_empty_error_on_success():
    label, elapsed_ms, ok, error = _timed("noop", lambda: None)
    assert (label, ok, error) == ("noop", True, "")
    assert elapsed_ms >= 0


def test_timed_captures_the_exception_repr_on_failure():
    def boom():
        raise RuntimeError("transaction 0xdead reverted")

    label, _elapsed_ms, ok, error = _timed("transfer", boom)
    assert (label, ok) == ("transfer", False)
    assert error == "RuntimeError('transaction 0xdead reverted')"


def test_timed_truncates_a_very_long_error_so_it_cannot_bloat_the_csv():
    def boom():
        raise RuntimeError("x" * 5_000)

    *_, error = _timed("approve", boom)
    assert len(error) == _MAX_ERROR_CHARS


class _SyncSendW3:
    """Counts the reads `_send_signed` makes and answers the one send."""

    def __init__(self, status):
        self.reads = 0
        self.sent: list[tuple[str, list]] = []
        outer = self

        class Eth:
            @property
            def chain_id(self):
                outer.reads += 1
                return 1

            @property
            def gas_price(self):
                outer.reads += 1
                return 7

        class Manager:
            def request_blocking(self, method, params):
                outer.sent.append((method, params))
                return {"status": status, "transactionHash": "0xabc"}

        self.eth = Eth()
        self.manager = Manager()


def test_send_signed_is_one_round_trip_after_the_first():
    import pytest
    from actions import _send_signed

    key = "0x" + "11" * 32
    w3 = _SyncSendW3("0x1")
    for nonce in range(3):
        _send_signed(w3, key, "0x" + "22" * 20, 1, b"", nonce, 21_000)

    assert w3.reads == 2, "chain id and gas price are fetched once, not per transaction"
    assert [m for m, _ in w3.sent] == ["eth_sendRawTransactionSync"] * 3
    assert all(p[0].startswith("0x") for _, p in w3.sent)

    with pytest.raises(RuntimeError, match="reverted"):
        _send_signed(_SyncSendW3(0), key, "0x" + "22" * 20, 1, b"", 0, 21_000)
