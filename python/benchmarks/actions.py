"""One function per simulated-agent action, each timed and returning
(label, elapsed_ms, ok, error). The set is limited to what both backends'
RPC surfaces support; see
docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import time
from typing import Callable

from eth_account import Account
from web3 import Web3

from backend import Backend, erc20_balance_slot

# (label, elapsed_ms, ok, error). `error` is a truncated `repr(exception)`,
# without which an ok=False row cannot be told apart from a revert, a nonce
# rejection, a timeout or a harness bug.
ActionResult = tuple[str, float, bool, str]

_MAX_ERROR_CHARS = 200

UNISWAP_V2_ROUTER = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

TOKENS = {
    "DAI": {"address": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "balance_slot": 2},
}

_ERC20_ABI = [
    {
        "name": "approve", "type": "function", "stateMutability": "nonpayable",
        "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
        "outputs": [{"name": "", "type": "bool"}],
    },
]

_ROUTER_ABI = [
    {
        "name": "swapExactETHForTokens", "type": "function", "stateMutability": "payable",
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
    {
        "name": "swapExactTokensForTokens", "type": "function", "stateMutability": "nonpayable",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
]

_FAR_FUTURE_DEADLINE = 9_999_999_999  # year ~2286


def _timed(label: str, fn: Callable[[], None]) -> ActionResult:
    start = time.monotonic()
    try:
        fn()
        ok, error = True, ""
    except Exception as e:
        ok, error = False, repr(e)[:_MAX_ERROR_CHARS]
    elapsed_ms = (time.monotonic() - start) * 1000
    return (label, elapsed_ms, ok, error)


def _send_signed(w3: Web3, signer_key: str, to: str, value: int, data: bytes, nonce: int, gas: int) -> None:
    gas_price = w3.eth.gas_price
    tx = {
        "chainId": w3.eth.chain_id,
        "nonce": nonce,
        "gas": gas,
        "gasPrice": gas_price,
        "to": to,
        "value": value,
        "data": data,
    }
    signed = Account.sign_transaction(tx, signer_key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=10)
    if receipt.status != 1:
        raise RuntimeError(f"transaction {tx_hash.to_0x_hex()} reverted")


def transfer(backend: Backend, signer_key: str, to: str, value: int, nonce: int) -> ActionResult:
    def do():
        _send_signed(backend.web3(), signer_key, to, value, b"", nonce, gas=21_000)
    return _timed("transfer", do)


def set_balance(backend: Backend, address: str, value: int) -> ActionResult:
    return _timed("set_balance", lambda: backend.set_native_balance(address, value))


def get_balance(backend: Backend, address: str) -> ActionResult:
    def do():
        backend.web3().eth.get_balance(address)
        backend.web3().eth.get_transaction_count(address)
    return _timed("get_balance", do)


def fund_token(backend: Backend, token: str, holder: str, amount: int) -> ActionResult:
    def do():
        info = next(t for t in TOKENS.values() if t["address"] == token)
        slot = erc20_balance_slot(holder, info["balance_slot"])
        value = amount.to_bytes(32, "big")
        backend.set_storage(token, "0x" + slot.hex(), "0x" + value.hex())
    return _timed("fund_token", do)


def approve(backend: Backend, signer_key: str, token: str, spender: str, amount: int, nonce: int) -> ActionResult:
    def do():
        w3 = backend.web3()
        contract = w3.eth.contract(address=token, abi=_ERC20_ABI)
        data = contract.encode_abi("approve", args=[spender, amount])
        _send_signed(w3, signer_key, token, 0, bytes.fromhex(data[2:]), nonce, gas=60_000)
    return _timed("approve", do)


def swap_eth_for_token(backend: Backend, signer_key: str, token: str, amount_in: int, nonce: int) -> ActionResult:
    def do():
        w3 = backend.web3()
        signer = Account.from_key(signer_key)
        router = w3.eth.contract(address=UNISWAP_V2_ROUTER, abi=_ROUTER_ABI)
        data = router.encode_abi(
            "swapExactETHForTokens",
            args=[0, [WETH, token], signer.address, _FAR_FUTURE_DEADLINE],
        )
        _send_signed(w3, signer_key, UNISWAP_V2_ROUTER, amount_in, bytes.fromhex(data[2:]), nonce, gas=250_000)
    return _timed("swap_eth_for_token", do)


def swap_token_for_token(
    backend: Backend, signer_key: str, token_in: str, token_out: str, amount_in: int, nonce: int
) -> ActionResult:
    def do():
        w3 = backend.web3()
        signer = Account.from_key(signer_key)
        router = w3.eth.contract(address=UNISWAP_V2_ROUTER, abi=_ROUTER_ABI)
        data = router.encode_abi(
            "swapExactTokensForTokens",
            args=[amount_in, 0, [token_in, token_out], signer.address, _FAR_FUTURE_DEADLINE],
        )
        _send_signed(w3, signer_key, UNISWAP_V2_ROUTER, 0, bytes.fromhex(data[2:]), nonce, gas=300_000)
    return _timed("swap_token_for_token", do)


def read_contract(backend: Backend, address: str, data_hex: str) -> ActionResult:
    """`eth_estimateGas` rather than `eth_call`, which forkyard's HTTP
    surface does not expose; it still executes in the EVM, so the read pulls
    code and slots through the backend's cache."""
    def do():
        w3 = backend.web3()
        w3.eth.estimate_gas({"to": address, "data": data_hex})
        w3.eth.get_balance(address)
    return _timed("read_contract", do)


def discard_session(backend: Backend) -> ActionResult:
    return _timed("discard", backend.discard)
