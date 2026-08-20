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
//! Known gap if you point a real wallet at a session URL: balance reads
//! work (`eth_chainId`, `eth_getBalance`, `eth_getTransactionCount`), but
//! the send-transaction confirmation flow calls `eth_estimateGas` and gas
//! fee methods first, which aren't implemented here yet — sending through
//! a wallet's own UI will stall at that step even though `eth_sendRawTransaction`
//! itself works fine if you construct the signed transaction yourself.
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

    let fork = forkyard_fetch::fork(&rpc_url)?;
    let manager = SessionManager::new(fork, 4, Duration::from_secs(3600));
    let handle = forkyard_api_http::serve(&format!("127.0.0.1:{port}"), manager, 1).await?;

    println!("forkyard RPC listening at http://{}", handle.addr);
    println!("open a session:  curl -X POST http://{}/session", handle.addr);
    println!("(sessions expire after 1h idle; Ctrl-C to stop the server now)");

    tokio::signal::ctrl_c().await?;
    println!("shutting down...");
    handle.shutdown().await;
    Ok(())
}
