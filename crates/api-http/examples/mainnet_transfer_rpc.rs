//! `N` agents, each independently forking Ethereum mainnet, opening their
//! own RPC-backed session, and running a signed transfer — concurrently,
//! not sequentially. Each agent's fork, server, and session are entirely
//! its own; the only thing they share is the same upstream RPC provider.
//!
//! This is the concurrency case the whole design is supposed to make
//! cheap: forking is meant to be fast enough that N of them happening at
//! once isn't N times slower than one. Every log line is tagged with its
//! `agent_id` (via a `tracing` span) so the interleaved concurrent output
//! stays readable, and each agent's total wall-clock time is reported at
//! the end alongside the overall wall-clock time for all of them together.
//!
//! Requires `RPC_URL` (an Ethereum mainnet endpoint) in the environment or
//! a `.env` file at the workspace root. Run with:
//!   cargo run -p forkyard-api-http --example mainnet_transfer_rpc
use std::time::Instant;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{TxKind, B256, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use tracing::info;

const AGENT_COUNT: usize = 3;

/// Logs `label`'s elapsed time, in milliseconds, at `INFO`.
macro_rules! timed {
    ($label:expr, $body:expr) => {{
        let start = Instant::now();
        let result = $body;
        info!(elapsed_ms = start.elapsed().as_millis(), "{}", $label);
        result
    }};
}

struct AgentReport {
    agent_id: usize,
    tx_hash: B256,
    total_ms: u128,
}

/// One agent's full fork -> transfer -> assert -> discard flow, entirely
/// independent of every other agent: its own `forkyard_fetch::fork`, its
/// own `forkyard_api_http` server on its own ephemeral port, its own
/// session. `#[instrument]` tags every log line below with `agent_id`,
/// which is what keeps three interleaved concurrent runs legible.
#[tracing::instrument(skip(rpc_url))]
async fn run_agent(agent_id: usize, rpc_url: String) -> eyre::Result<AgentReport> {
    let agent_start = Instant::now();
    info!("agent starting");

    // 1. Fork Ethereum mainnet.
    let fork = timed!("forked mainnet", forkyard_fetch::fork(&rpc_url)?);

    // 2. Start this agent's own JSON-RPC server, and connect to it.
    let handle = timed!(
        "started RPC server",
        forkyard_api_http::serve("127.0.0.1:0", fork, 1).await?
    );
    let local_rpc = format!("http://{}", handle.addr);
    info!(addr = %local_rpc, "forkyard RPC listening");
    let provider = ProviderBuilder::new().connect_http(local_rpc.parse()?);

    // Fund a freshly generated signer via the test-only cheatcode RPC
    // method. Nothing here touches the real chain.
    let sender = PrivateKeySigner::random();
    let sender_addr = sender.address();
    let one_eth = U256::from(10).pow(U256::from(18));
    let _: bool = timed!(
        "funded sender via forkyard_setBalance",
        provider
            .client()
            .request(
                "forkyard_setBalance",
                (sender_addr.to_string(), format!("0x{one_eth:x}")),
            )
            .await?
    );

    let recipient_addr = PrivateKeySigner::random().address();
    let transfer_value = one_eth / U256::from(10); // 0.1 ETH
    info!(sender = %sender_addr, recipient = %recipient_addr, value = %transfer_value, "signer + recipient ready");

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
    let signed = timed!("signed transfer transaction", {
        let signature = sender.sign_transaction_sync(&mut tx)?;
        tx.into_signed(signature)
    });
    let raw = alloy_eips::eip2718::Encodable2718::encoded_2718(&signed);

    let (sender_before, recipient_before) = timed!(
        "fetched pre-transfer balances",
        (
            provider.get_balance(sender_addr).await?,
            provider.get_balance(recipient_addr).await?,
        )
    );
    info!(%sender_before, %recipient_before);

    // 4. Send it over RPC — executed against this agent's own fork and
    // committed into this agent's own session, isolated from the other
    // two agents entirely.
    let pending = timed!(
        "sent + executed transfer over RPC",
        provider.send_raw_transaction(&raw).await?
    );
    let tx_hash = *pending.tx_hash();
    info!(%tx_hash, "transaction hash");

    // 5. Assert balances actually changed.
    let (sender_after, recipient_after) = timed!(
        "fetched post-transfer balances",
        (
            provider.get_balance(sender_addr).await?,
            provider.get_balance(recipient_addr).await?,
        )
    );

    assert_eq!(recipient_after - recipient_before, transfer_value);
    assert!(sender_before - sender_after >= transfer_value, "sender must also pay gas");
    info!(
        sender = %format!("{sender_before} -> {sender_after}"),
        recipient = %format!("{recipient_before} -> {recipient_after}"),
        "balances confirmed"
    );

    // 6. Discard this agent's fork — its own server, its own session.
    timed!("shut down server + discarded fork", handle.shutdown().await);

    let total_ms = agent_start.elapsed().as_millis();
    info!(total_ms, "agent finished");
    Ok(AgentReport { agent_id, tx_hash, total_ms })
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    dotenvy::dotenv().ok();
    let rpc_url = std::env::var("RPC_URL")
        .expect("set RPC_URL to an Ethereum mainnet RPC endpoint (see .env.example)");

    info!(agents = AGENT_COUNT, "launching agents concurrently");
    let overall_start = Instant::now();

    // Spawned, not just joined on a shared future — each agent genuinely
    // runs on its own tokio task, so this measures real concurrency, not
    // cooperative interleaving on one task.
    let tasks: Vec<_> = (0..AGENT_COUNT)
        .map(|id| tokio::spawn(run_agent(id, rpc_url.clone())))
        .collect();

    let mut reports = Vec::with_capacity(AGENT_COUNT);
    for task in tasks {
        reports.push(task.await??);
    }

    let overall_ms = overall_start.elapsed().as_millis();
    info!(overall_ms, "all agents finished");

    println!();
    println!("{:<9} {:<10} {:<66}", "agent", "total_ms", "tx_hash");
    for r in &reports {
        println!("{:<9} {:<10} {:<66}", r.agent_id, r.total_ms, r.tx_hash);
    }
    println!();
    println!(
        "wall-clock for all {AGENT_COUNT} agents: {overall_ms}ms (sum of their individual totals: {}ms)",
        reports.iter().map(|r| r.total_ms).sum::<u128>()
    );

    Ok(())
}
