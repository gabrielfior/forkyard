//! `N` agents sharing *one* fork of Ethereum mainnet, through *one*
//! `forkyard-session` `SessionManager`, behind *one* `forkyard-api-http`
//! server — each agent only opens its own session on top of that shared
//! base and shared fetch cache, then runs a signed transfer concurrently
//! with the others.
//!
//! This is the thing the previous version of this example didn't
//! exercise: there, each agent got its own independent fork with its own
//! independent cache — three unrelated sandboxes running side by side.
//! Here they share the one thing that actually matters for the "faster
//! than Tenderly" pitch: a warm cache that gets more valuable as more
//! sessions hit it, not three cold ones paying separately for the same
//! network round trips.
//!
//! Every log line is tagged with its `agent_id` (via a `tracing` span);
//! each agent's total wall-clock time is reported at the end alongside
//! the overall wall-clock time for all of them together.
//!
//! Requires `RPC_URL` (an Ethereum mainnet endpoint) in the environment or
//! a `.env` file at the workspace root. Run with:
//!   cargo run -p forkyard-api-http --example mainnet_transfer_rpc
use std::time::{Duration, Instant};

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{TxKind, B256, U256};
use alloy_provider::{Provider, ProviderBuilder};
use alloy_signer_local::PrivateKeySigner;
use forkyard_session::SessionManager;
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
    session_id: u64,
    tx_hash: B256,
    total_ms: u128,
}

/// One agent's full open-session -> transfer -> assert flow, running
/// against a `SessionManager` it shares with every other agent. Its own
/// session is private (its own overlay); the base and the fetch fallback
/// underneath are not.
#[tracing::instrument(skip(base_url, http))]
async fn run_agent(agent_id: usize, base_url: String, http: reqwest::Client) -> eyre::Result<AgentReport> {
    let agent_start = Instant::now();
    info!("agent starting");

    // Open a session on the *shared* manager — not a fork of its own.
    let session_id: u64 = timed!("opened session on shared manager", {
        let resp: serde_json::Value = http
            .post(format!("{base_url}/session"))
            .send()
            .await?
            .json()
            .await?;
        resp["session_id"]
            .as_u64()
            .ok_or_else(|| eyre::eyre!("no session_id in response: {resp}"))?
    });

    let session_url = format!("{base_url}/session/{session_id}");
    info!(%session_url, "session ready");
    let provider = ProviderBuilder::new().connect_http(session_url.parse()?);

    // Fund a freshly generated signer via the test-only cheatcode RPC
    // method — scoped to this agent's own session/overlay only.
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

    let pending = timed!(
        "sent + executed transfer over RPC",
        provider.send_raw_transaction(&raw).await?
    );
    let tx_hash = *pending.tx_hash();
    info!(%tx_hash, "transaction hash");

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

    let total_ms = agent_start.elapsed().as_millis();
    info!(total_ms, "agent finished");
    Ok(AgentReport { agent_id, session_id, tx_hash, total_ms })
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    dotenvy::dotenv().ok();
    let rpc_url = std::env::var("RPC_URL")
        .expect("set RPC_URL to an Ethereum mainnet RPC endpoint (see .env.example)");

    // Fork Ethereum mainnet exactly once...
    let fork = timed!("forked mainnet (once, shared)", forkyard_fetch::fork(&rpc_url)?);

    // ...and build one SessionManager on top of it, sharded across 4
    // worker threads. Every agent below opens a session on *this* manager
    // instead of forking its own — the actual thing this example exists
    // to demonstrate.
    let manager = SessionManager::new(fork, 4, Duration::from_secs(60));
    let handle = timed!(
        "started shared RPC server",
        forkyard_api_http::serve("127.0.0.1:0", manager, 1).await?
    );
    let base_url = format!("http://{}", handle.addr);
    info!(addr = %base_url, "forkyard RPC listening — shared across all agents");

    info!(agents = AGENT_COUNT, "launching agents concurrently");
    let http = reqwest::Client::new();
    let overall_start = Instant::now();

    let tasks: Vec<_> = (0..AGENT_COUNT)
        .map(|id| tokio::spawn(run_agent(id, base_url.clone(), http.clone())))
        .collect();

    let mut reports = Vec::with_capacity(AGENT_COUNT);
    for task in tasks {
        reports.push(task.await??);
    }

    let overall_ms = overall_start.elapsed().as_millis();
    info!(overall_ms, "all agents finished");

    // One shutdown, for the one shared server — not per agent.
    handle.shutdown().await;

    println!();
    println!("{:<9} {:<12} {:<10} {:<66}", "agent", "session_id", "total_ms", "tx_hash");
    for r in &reports {
        println!("{:<9} {:<12} {:<10} {:<66}", r.agent_id, r.session_id, r.total_ms, r.tx_hash);
    }
    println!();
    println!(
        "wall-clock for all {AGENT_COUNT} agents sharing one fork: {overall_ms}ms (sum of their individual totals: {}ms)",
        reports.iter().map(|r| r.total_ms).sum::<u128>()
    );

    Ok(())
}
