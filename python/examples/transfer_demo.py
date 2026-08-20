"""The same fork -> fund -> sign -> send -> assert flow as the Rust
`mainnet_transfer` / `mainnet_transfer_rpc` examples, but from a real
`web3.py` client — proving the RPC surface isn't Rust-specific.

Assumes a forkyard server is already running in another terminal, e.g.:

    cd crates/api-http && RPC_URL=... cargo run --example serve_demo

then, from this directory:

    uv run transfer_demo.py

Reads the server's base URL from FORKYARD_URL (default matches
serve_demo.rs's default port, http://127.0.0.1:8555).

This intentionally stays within the RPC methods the server already
implements today (no eth_gasPrice / eth_estimateGas / eth_getTransactionReceipt
yet) — gas and gas price are hardcoded, and success is confirmed by reading
balances back rather than waiting on a transaction receipt.
"""

import logging
import os
import time

import requests
from eth_account import Account
from web3 import Web3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("transfer_demo")

ONE_ETH = 10**18
TRANSFER_VALUE = ONE_ETH // 10  # 0.1 ETH
GAS_LIMIT = 21_000
GAS_PRICE = 20_000_000_000  # 20 gwei


def timed(label: str, fn):
    start = time.monotonic()
    result = fn()
    elapsed_ms = (time.monotonic() - start) * 1000
    log.info("%s (elapsed_ms=%.1f)", label, elapsed_ms)
    return result


def main() -> None:
    base_url = os.environ.get("FORKYARD_URL", "http://127.0.0.1:8555")
    run_start = time.monotonic()

    # 1. Open a session on the already-running, already-forked server.
    session_id = timed(
        "opened session",
        lambda: requests.post(f"{base_url}/session", timeout=10).json()["session_id"],
    )
    session_url = f"{base_url}/session/{session_id}"
    log.info("session_url=%s", session_url)

    # 2. Connect with a real web3.py HTTPProvider pointed at that session's
    # own endpoint — this is the actual point of this script.
    w3 = Web3(Web3.HTTPProvider(session_url))
    chain_id = w3.eth.chain_id
    log.info("connected, chain_id=%s", chain_id)

    # Fund a freshly generated signer via the test-only cheatcode RPC
    # method, same as the Rust examples — nothing here touches the real
    # chain, it's this session's private overlay only.
    sender = Account.create()
    recipient = Account.create()
    timed(
        "funded sender via forkyard_setBalance",
        lambda: w3.manager.request_blocking(
            "forkyard_setBalance", [sender.address, hex(ONE_ETH)]
        ),
    )
    log.info("sender=%s recipient=%s value=%s", sender.address, recipient.address, TRANSFER_VALUE)

    sender_before, recipient_before = timed(
        "fetched pre-transfer balances",
        lambda: (w3.eth.get_balance(sender.address), w3.eth.get_balance(recipient.address)),
    )
    log.info("sender_before=%s recipient_before=%s", sender_before, recipient_before)

    # 3. Build and sign a real transfer transaction, locally, with
    # eth_account — the same as any real web3.py-based script would.
    nonce = w3.eth.get_transaction_count(sender.address)
    tx = {
        "chainId": chain_id,
        "nonce": nonce,
        "gas": GAS_LIMIT,
        "gasPrice": GAS_PRICE,
        "to": recipient.address,
        "value": TRANSFER_VALUE,
        "data": b"",
    }
    signed = timed("signed transfer transaction", lambda: Account.sign_transaction(tx, sender.key))

    # 4. Send it — eth_sendRawTransaction, executed synchronously against
    # the fork and committed into this session's private overlay.
    tx_hash = timed(
        "sent + executed transfer over RPC",
        lambda: w3.eth.send_raw_transaction(signed.raw_transaction),
    )
    log.info("tx_hash=%s", tx_hash.to_0x_hex())

    # 5. Assert balances actually changed.
    sender_after, recipient_after = timed(
        "fetched post-transfer balances",
        lambda: (w3.eth.get_balance(sender.address), w3.eth.get_balance(recipient.address)),
    )

    assert recipient_after - recipient_before == TRANSFER_VALUE, "recipient did not receive the transfer"
    assert sender_before - sender_after >= TRANSFER_VALUE, "sender did not pay for the transfer"
    log.info(
        "balances confirmed: sender %s -> %s, recipient %s -> %s",
        sender_before, sender_after, recipient_before, recipient_after,
    )

    total_ms = (time.monotonic() - run_start) * 1000
    log.info("done, end to end (total_ms=%.1f)", total_ms)


if __name__ == "__main__":
    main()
