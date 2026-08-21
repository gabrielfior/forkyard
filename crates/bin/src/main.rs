//! Runs the whole engine as one binary: one shared `SessionManager`, its
//! HTTP JSON-RPC surface (`forkyard-api-http`) and its MCP-over-stdio
//! surface (`forkyard-api-mcp`) served simultaneously, plus a background
//! `forkyard-ingest` task keeping the manager's block context current.
//!
//! Both API surfaces are up at once (not one-or-the-other) so this binary
//! can be launched directly by an agent harness over stdio (the MCP path)
//! while *also* being reachable over the network by anything else that
//! wants to share the same warm cache (the HTTP path) — see
//! docs/RESEARCH.md, "Integration path", for why these are genuinely
//! different sharing models, not two names for the same thing.
//!
//! stdout is reserved for MCP's JSON-RPC framing whenever the stdio
//! transport is live, so every log line here goes to stderr instead —
//! logging to stdout would corrupt the MCP protocol stream for whatever
//! harness launched this process.

use std::sync::Arc;
use std::time::Duration;

use forkyard_api_mcp::ForkyardMcpServer;
use forkyard_ingest::ChainTipFollower;
use forkyard_session::SessionManager;
use tracing_subscriber::EnvFilter;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let rpc_url = std::env::var("RPC_URL")
        .expect("set RPC_URL to an EVM RPC endpoint (see .env.example)");
    let port: u16 = env_or("FORKYARD_PORT", 8555);
    let num_workers: usize = env_or("FORKYARD_NUM_WORKERS", 4);
    let ttl_secs: u64 = env_or("FORKYARD_SESSION_TTL_SECS", 3600);
    let ingest_poll_secs: u64 = env_or("FORKYARD_INGEST_POLL_SECS", 12);
    let chain_id: u64 = env_or("FORKYARD_CHAIN_ID", 1);

    let (fork, block_env) = forkyard_fetch::fork(&rpc_url).await?;
    let manager = Arc::new(SessionManager::new(
        fork,
        block_env,
        num_workers,
        Duration::from_secs(ttl_secs),
    ));
    tracing::info!(num_workers, ttl_secs, "forked upstream chain, session manager ready");

    // Background chain-tip follower — re-forks a fresh fallback whenever
    // the chain actually advances, not just the block number label; see
    // forkyard-ingest's module doc for exactly what stays bounded-stale.
    let (ingest_stop_tx, ingest_stop_rx) = tokio::sync::oneshot::channel();
    let ingest_manager = Arc::clone(&manager);
    let ingest_task = tokio::spawn(async move {
        let follower = ChainTipFollower::new(rpc_url, Duration::from_secs(ingest_poll_secs));
        follower.run(&ingest_manager, ingest_stop_rx).await;
    });

    let http_handle = forkyard_api_http::serve(&format!("127.0.0.1:{port}"), Arc::clone(&manager), chain_id).await?;
    tracing::info!(addr = %http_handle.addr, "HTTP JSON-RPC surface listening — shared cache reachable over the network");

    let mcp_server = ForkyardMcpServer::new(Arc::clone(&manager));
    tracing::info!("MCP stdio surface starting — this is the path an agent harness launching this process actually calls");

    // The MCP transport naturally ends when the harness that launched us
    // closes stdin; Ctrl-C covers running this interactively instead.
    // Either one tears down the HTTP server and ingest loop the same way.
    tokio::select! {
        result = mcp_server.serve_stdio() => {
            if let Err(error) = result {
                tracing::warn!(%error, "MCP stdio surface ended with an error");
            } else {
                tracing::info!("MCP stdio surface ended (stdin closed)");
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Ctrl-C received, shutting down");
        }
    }

    let _ = ingest_stop_tx.send(());
    let _ = ingest_task.await;
    http_handle.shutdown().await;

    Ok(())
}
