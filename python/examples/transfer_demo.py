"""The same fork -> fund -> sign -> send -> assert flow as the Rust
`mainnet_transfer` / `mainnet_transfer_rpc` examples, but from a real
`web3.py` client — proving the RPC surface isn't Rust-specific.

Assumes a forkyard server is already running in another terminal, e.g.:

    cd crates/api-http && RPC_URL=... cargo run --example serve_demo

then, from this directory:

    uv run transfer_demo.py

Reads the server's base URL from FORKYARD_URL (default matches
serve_demo.rs's default port, http://127.0.0.1:8555).

Uses the full idiomatic web3.py flow now that the server implements it:
gas price via `w3.eth.gas_price`, gas via `w3.eth.estimate_gas` (a real
dry-run through `eth_estimateGas`, not a hardcoded constant), and success
confirmed via `w3.eth.wait_for_transaction_receipt` rather than only
re-reading balances.
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
    log.info("connected, chain_id=%s, block_number=%s", chain_id, w3.eth.block_number)

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

    # 3. Ask the server what this would actually cost — real gas
    # estimation and gas price, not hardcoded constants.
    gas_price = timed("fetched gas price", lambda: w3.eth.gas_price)
    gas = timed(
        "estimated gas",
        lambda: w3.eth.estimate_gas({"from": sender.address, "to": recipient.address, "value": TRANSFER_VALUE}),
    )
    log.info("gas_price=%s gas=%s", gas_price, gas)

    # 4. Build and sign a real transfer transaction, locally, with
    # eth_account — the same as any real web3.py-based script would.
    nonce = w3.eth.get_transaction_count(sender.address)
    tx = {
        "chainId": chain_id,
        "nonce": nonce,
        "gas": gas,
        "gasPrice": gas_price,
        "to": recipient.address,
        "value": TRANSFER_VALUE,
        "data": b"",
    }
    signed = timed("signed transfer transaction", lambda: Account.sign_transaction(tx, sender.key))

    # 5. Send it — eth_sendRawTransaction, executed synchronously against
    # the fork and committed into this session's private overlay.
    tx_hash = timed(
        "sent + executed transfer over RPC",
        lambda: w3.eth.send_raw_transaction(signed.raw_transaction),
    )
    log.info("tx_hash=%s", tx_hash.to_0x_hex())

    # 6. Confirm via a real receipt — the idiomatic web3.py way to know a
    # transaction landed, rather than just re-reading balances. Since
    # execution is synchronous server-side, this resolves immediately;
    # it's still a real eth_getTransactionReceipt round trip, not a
    # local assumption.
    receipt = timed("waited for transaction receipt", lambda: w3.eth.wait_for_transaction_receipt(tx_hash, timeout=10))
    log.info("receipt: status=%s gasUsed=%s blockNumber=%s", receipt.status, receipt.gasUsed, receipt.blockNumber)
    assert receipt.status == 1, "transaction receipt reports failure"

    # 7. Assert balances actually changed too.
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
