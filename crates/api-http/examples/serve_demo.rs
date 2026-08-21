//! Starts one shared fork + SessionManager + RPC server and just keeps it
//! running — for manually poking at with `curl`, `cast`, or a wallet like
//! MetaMask's custom-network UI, instead of running a scripted flow.
//!
//! Requires `RPC_URL` (an Ethereum mainnet endpoint) in the environment or
//! a `.env` file at the workspace root. Optional `FORKYARD_PORT` (default
//! 8555). Run with:
//!   cargo run -p forkyard-api-http --example serve_demo
//!
//! Then, e.g.:
//!   curl -X POST http://127.0.0.1:8555/session
//!   curl -X POST http://127.0.0.1:8555/session/0 -d '{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}'
//!
//! `eth_gasPrice`, `eth_estimateGas`, and `eth_getTransactionReceipt` are
//! all implemented now, backed by the fork's real base fee/block number —
//! a real wallet's send-transaction flow (gas estimate, gas price, send,
//! wait for receipt) should work end to end against a session URL.
use std::sync::Arc;
use std::time::Duration;

use forkyard_session::SessionManager;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenvy::dotenv().ok();
    let rpc_url = std::env::var("RPC_URL")
        .expect("set RPC_URL to an Ethereum mainnet RPC endpoint (see .env.example)");
    let port: u16 = std::env::var("FORKYARD_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8555);

    let (fork, block_env) = forkyard_fetch::fork(&rpc_url).await?;
    let manager = Arc::new(SessionManager::new(fork, block_env, 4, Duration::from_secs(3600)));
    let handle = forkyard_api_http::serve(&format!("127.0.0.1:{port}"), manager, 1).await?;

    println!("forkyard RPC listening at http://{}", handle.addr);
    println!("open a session:  curl -X POST http://{}/session", handle.addr);
    println!("(sessions expire after 1h idle; Ctrl-C to stop the server now)");

    tokio::signal::ctrl_c().await?;
    println!("shutting down...");
    handle.shutdown().await;
    Ok(())
}
