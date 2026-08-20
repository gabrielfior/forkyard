//! The same fork -> transfer -> assert -> discard flow as
//! `forkyard-fetch`'s `mainnet_transfer` example, but this time the
//! "connect to the forked RPC" step is real: an external `alloy` HTTP
//! provider talks to `forkyard-api-http`'s JSON-RPC server over a real
//! socket, exactly as `cast` or MetaMask would.
//!
//! Requires `RPC_URL` (an Ethereum mainnet endpoint) in the environment or
//! a `.env` file at the workspace root. Run with:
//!   cargo run -p forkyard-api-http --example mainnet_transfer_rpc
use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{TxKind, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenvy::dotenv().ok();
    let rpc_url = std::env::var("RPC_URL")
        .expect("set RPC_URL to an Ethereum mainnet RPC endpoint (see .env.example)");

    // 1. Fork Ethereum mainnet.
    let fork = forkyard_fetch::fork(&rpc_url)?;

    // 2. Start the JSON-RPC server fronting that fork, and connect to it —
    // for real this time, over HTTP, the way an external client would.
    let handle = forkyard_api_http::serve("127.0.0.1:0", fork, 1).await?;
    let local_rpc = format!("http://{}", handle.addr);
    println!("forkyard RPC listening at {local_rpc}");
    let provider = ProviderBuilder::new().connect_http(local_rpc.parse()?);

    // Fund a freshly generated signer via the test-only cheatcode RPC
    // method — the wire equivalent of the in-process `session.set_account`
    // used in the direct example. Nothing here touches the real chain.
    let sender = PrivateKeySigner::random();
    let sender_addr = sender.address();
    let one_eth = U256::from(10).pow(U256::from(18));
    let _: bool = provider
        .client()
        .request(
            "forkyard_setBalance",
            (sender_addr.to_string(), format!("0x{one_eth:x}")),
        )
        .await?;

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
    let raw = alloy_eips::eip2718::Encodable2718::encoded_2718(&signed);

    let sender_before = provider.get_balance(sender_addr).await?;
    let recipient_before = provider.get_balance(recipient_addr).await?;

    // 4. Send it over RPC — `eth_sendRawTransaction`, handled by
    // `forkyard-api-http`, executed against the fork, and committed into
    // that session's private overlay. Returns the same tx hash a real
    // node would compute from the signed envelope.
    let pending = provider.send_raw_transaction(&raw).await?;
    let tx_hash = *pending.tx_hash();
    println!("sent over RPC, tx hash = {tx_hash}");

    // 5. Assert balances actually changed — read back over RPC too.
    let sender_after = provider.get_balance(sender_addr).await?;
    let recipient_after = provider.get_balance(recipient_addr).await?;

    assert_eq!(recipient_after - recipient_before, transfer_value);
    assert!(sender_before - sender_after >= transfer_value, "sender must also pay gas");
    println!(
        "sender {sender_before} -> {sender_after}, recipient {recipient_before} -> {recipient_after}"
    );

    // 6. Discard the fork: tear down the server (and with it, the
    // session's overlay and the fork's background fetch thread). Nothing
    // was ever broadcast, nothing persists past this line.
    handle.shutdown().await;

    Ok(())
}
