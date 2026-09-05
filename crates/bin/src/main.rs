//! Runs the whole engine as one binary: one shared `SessionManager`, its
//! plain HTTP JSON-RPC surface (`forkyard-api-http`), its MCP-over-stdio
//! surface, and its MCP-over-Streamable-HTTP surface (both `forkyard-api-mcp`)
//! served simultaneously, plus a background `forkyard-ingest` task keeping
//! the manager's block context current.
//!
//! All three surfaces are up at once (not one-or-the-other) so this binary
//! can be launched directly by an agent harness over stdio (the MCP-stdio
//! path) while *also* being reachable over the network by anything else
//! that wants to share the same warm cache — either the plain JSON-RPC path
//! (`cast`, `alloy`, `web3.py`, a wallet) or the MCP-over-HTTP path (a
//! browser-based agent, mcp-cli's `"url"` config, a client on another
//! machine) — see docs/RESEARCH.md, "Integration path", for why these are
//! genuinely different sharing models, not three names for the same thing.
//!
//! stdout is reserved for MCP's JSON-RPC framing whenever the stdio
//! transport is live, so every log line here goes to stderr instead —
//! logging to stdout would corrupt the MCP protocol stream for whatever
//! harness launched this process.

use std::sync::Arc;
use std::time::Duration;

use forkyard_api_mcp::ForkyardMcpServer;
use forkyard_fetch::Fork;
use forkyard_ingest::ChainTipFollower;
use forkyard_session::{BlockForkFuture, SessionManager, DEFAULT_MAX_PINNED_BLOCKS};
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
    let mcp_http_port: u16 = env_or("FORKYARD_MCP_HTTP_PORT", 8556);
    let num_workers: usize = env_or("FORKYARD_NUM_WORKERS", 4);
    let ttl_secs: u64 = env_or("FORKYARD_SESSION_TTL_SECS", 3600);
    let ingest_poll_secs: u64 = env_or("FORKYARD_INGEST_POLL_SECS", 12);
    let chain_id: u64 = env_or("FORKYARD_CHAIN_ID", 1);
    let fork_block_number: Option<u64> =
        std::env::var("FORKYARD_FORK_BLOCK_NUMBER").ok().and_then(|v| v.parse().ok());

    // How many *explicitly pinned* blocks (`POST /session {"block_number":
    // N}`) stay warm at once, on top of the default one. Each costs its own
    // fetch backend — cache plus background thread — so this is a real
    // memory bound, not a tidy-up.
    let max_pinned_blocks: usize = env_or("FORKYARD_MAX_PINNED_BLOCKS", DEFAULT_MAX_PINNED_BLOCKS);

    let (fork, block_env) = match fork_block_number {
        Some(n) => forkyard_fetch::fork_at(&rpc_url, n).await?,
        None => forkyard_fetch::fork(&rpc_url).await?,
    };
    // The factory is the only thing that knows the RPC URL — `forkyard-session`
    // deliberately doesn't, which is what keeps it unit-testable with an
    // in-memory fallback and no network.
    let fork_rpc_url = rpc_url.clone();
    let manager = Arc::new(
        SessionManager::new(fork, block_env, num_workers, Duration::from_secs(ttl_secs)).with_block_forks(
            move |number: u64| -> BlockForkFuture<Fork> {
                let rpc_url = fork_rpc_url.clone();
                Box::pin(async move {
                    forkyard_fetch::fork_at(&rpc_url, number).await.map_err(|e| e.to_string())
                })
            },
            max_pinned_blocks,
        ),
    );
    tracing::info!(num_workers, ttl_secs, max_pinned_blocks, "forked upstream chain, session manager ready");

    // Background chain-tip follower — only when the fork isn't pinned to an
    // explicit block. A pinned historical block and "keep following the
    // tip" are contradictory: re-forking to a newer block would silently
    // defeat the whole point of FORKYARD_FORK_BLOCK_NUMBER.
    //
    // It refreshes the *default* base only (`refresh_fallback`), never the
    // per-session pinned blocks: a session opened with `{"block_number":
    // N}` is reproducing something at N and must not be dragged to the tip
    // underneath the agent using it.
    let ingest_handle = match fork_block_number {
        Some(n) => {
            tracing::info!(block = n, "fork pinned to an explicit block; chain-tip following disabled");
            None
        }
        None => {
            let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
            let ingest_manager = Arc::clone(&manager);
            let task = tokio::spawn(async move {
                let follower = ChainTipFollower::new(rpc_url, Duration::from_secs(ingest_poll_secs));
                follower.run(&ingest_manager, stop_rx).await;
            });
            Some((stop_tx, task))
        }
    };

    let http_handle = forkyard_api_http::serve(&format!("127.0.0.1:{port}"), Arc::clone(&manager), chain_id).await?;
    tracing::info!(addr = %http_handle.addr, "HTTP JSON-RPC surface listening — shared cache reachable over the network");

    let mcp_http_handle =
        ForkyardMcpServer::serve_http(Arc::clone(&manager), &format!("127.0.0.1:{mcp_http_port}")).await?;
    tracing::info!(addr = %mcp_http_handle.addr, "MCP-over-HTTP surface listening at /mcp — e.g. for mcp-cli's \"url\" config");

    // Stdio runs as its own background task rather than gating shutdown:
    // it ending (stdin closed, or no stdin at all — e.g. run detached as a
    // persistent HTTP-only service, which is now a legitimate deployment,
    // not just "launched as a subprocess by a stdio harness") shouldn't
    // take the HTTP/MCP-HTTP surfaces down with it. Real shutdown is a
    // signal — Ctrl-C interactively, SIGTERM from whatever supervises this
    // process — the same mechanism every other long-running service here
    // would expect, and the one a harness that spawned this as a
    // subprocess already reaches for to tear it down rather than relying
    // on stdin-EOF detection.
    let mcp_server = ForkyardMcpServer::new(Arc::clone(&manager));
    tracing::info!("MCP stdio surface starting — this is the path an agent harness launching this process actually calls");
    tokio::spawn(async move {
        match mcp_server.serve_stdio().await {
            Ok(()) => tracing::info!("MCP stdio surface ended (stdin closed)"),
            Err(error) => tracing::warn!(%error, "MCP stdio surface ended with an error"),
        }
    });

    wait_for_shutdown_signal().await;

    if let Some((stop_tx, task)) = ingest_handle {
        let _ = stop_tx.send(());
        let _ = task.await;
    }
    http_handle.shutdown().await;
    mcp_http_handle.shutdown().await;

    Ok(())
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("Ctrl-C received, shutting down"),
        _ = sigterm.recv() => tracing::info!("SIGTERM received, shutting down"),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("Ctrl-C received, shutting down");
}
