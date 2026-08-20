//! End-to-end sanity check: fork real Ethereum mainnet, run a signed ETH
//! transfer against it, and confirm the balance actually moved — all
//! in-process, nothing broadcast, nothing left over once it exits.
//!
//! Requires `RPC_URL` (an Ethereum mainnet endpoint) in the environment or
//! a `.env` file at the workspace root. Run with:
//!   cargo run -p forkyard-fetch --example mainnet_transfer
use std::sync::Arc;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use forkyard_engine::{BaseSnapshot, Session};
use revm::context::TxEnv;
use revm::state::AccountInfo;
use revm::{Database, ExecuteCommitEvm, MainBuilder, MainContext};

fn main() -> eyre::Result<()> {
    dotenvy::dotenv().ok();
    let rpc_url = std::env::var("RPC_URL")
        .expect("set RPC_URL to an Ethereum mainnet RPC endpoint (see .env.example)");

    // 1. Fork Ethereum mainnet.
    let fork = forkyard_fetch::fork(&rpc_url)?;

    // 2. "Connect to the forked RPC" — by design, there is no RPC server
    // to connect to. The fork is used directly in-process (that's the
    // whole cost/latency advantage; see docs/RESEARCH.md, "why this is
    // cheap"). `Session` is that direct handle: real mainnet state via
    // `fork` as its fallback, layered under a private overlay.
    let mut session = Session::fork(Arc::new(BaseSnapshot::default()), fork);

    // Fund a freshly generated signer on top of real forked state — the
    // same role Anvil's `anvil_setBalance` cheatcode plays. Nothing here
    // touches the real chain: this write lands only in `session`'s
    // private overlay.
    let sender = PrivateKeySigner::random();
    let sender_addr = sender.address();
    let one_eth = U256::from(10).pow(U256::from(18));
    session.set_account(
        sender_addr,
        AccountInfo {
            balance: one_eth,
            ..Default::default()
        },
    );

    let recipient_addr = PrivateKeySigner::random().address();
    let transfer_value = one_eth / U256::from(10); // 0.1 ETH

    // 3. Build and sign a real transfer transaction.
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce: 0,
        gas_price: 20_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(recipient_addr),
        value: transfer_value,
        input: Default::default(),
    };
    let signature = sender.sign_transaction_sync(&mut tx)?;
    let signed = tx.into_signed(signature);

    // 4. The transaction hash — computed from the signed envelope, exactly
    // as a real client would, whether or not it's ever broadcast.
    let tx_hash = *signed.hash();
    println!("built + signed transfer: {tx_hash}");

    let sender_before = session.basic(sender_addr)?.unwrap().balance;
    let recipient_before = session
        .basic(recipient_addr)?
        .map(|info| info.balance)
        .unwrap_or(U256::ZERO);

    // "advance": execute against the fork and commit the diff into this
    // session's private overlay only (see docs/RESEARCH.md, "what
    // simulate / advance actually do").
    let tx_env = TxEnv::builder()
        .caller(sender_addr)
        .kind(TxKind::Call(recipient_addr))
        .value(transfer_value)
        .gas_limit(21_000)
        .gas_price(20_000_000_000)
        .nonce(0)
        .chain_id(Some(1))
        .build_fill();

    revm::Context::mainnet()
        .with_db(&mut session)
        .build_mainnet()
        .transact_commit(tx_env)?;

    // 5. Assert balances actually changed.
    let sender_after = session.basic(sender_addr)?.unwrap().balance;
    let recipient_after = session.basic(recipient_addr)?.unwrap().balance;

    assert_eq!(recipient_after - recipient_before, transfer_value);
    assert!(sender_before - sender_after >= transfer_value, "sender must also pay gas");
    println!(
        "sender {sender_before} -> {sender_after}, recipient {recipient_before} -> {recipient_after}"
    );

    // 6. Discard the fork. The overlay drops with `session`; `fork`'s
    // background fetch thread tears down once its last handle drops.
    // Nothing here was ever broadcast, and nothing persists past this line.
    drop(session);

    Ok(())
}
