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

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use forkyard_api_mcp::ForkyardMcpServer;
use forkyard_engine::persist::{default_cache_dir, CacheKey, ForkCache};
use forkyard_engine::BaseSnapshot;
use forkyard_fetch::Fork;
use forkyard_ingest::ChainTipFollower;
use forkyard_session::{BlockForkFuture, SessionManager, DEFAULT_MAX_PINNED_BLOCKS};
use tracing_subscriber::EnvFilter;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Kill switches are typed by hand at a shell prompt or set by a benchmark
/// harness, so accept what people actually write rather than only what
/// `bool::from_str` accepts — `FORKYARD_CACHE_DISABLED=1` silently parsing
/// to "not disabled" would make a cold-vs-warm measurement quietly
/// meaningless.
fn env_flag(key: &str) -> bool {
    match std::env::var(key) {
        Ok(value) => matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => false,
    }
}

/// Fold what this process fetched into what a previous one had already
/// cached, and write the result out atomically.
///
/// The merge is what keeps a warm restart from eroding the cache: reads
/// served out of the seeded base never reach the fetch backend, so
/// `cache_snapshot` alone would return only the *new* misses and each
/// restart would save a smaller file than it loaded, back to empty. It is
/// guarded on the block still matching, because after a chain-tip refresh
/// the seeded snapshot describes a block that is no longer this one, and
/// filing an old block's balances under a new block's number would be
/// wrong rather than merely stale.
///
/// Every failure here is logged and swallowed: this runs on the shutdown
/// path, and a cache that can't be written is a slower next start, not a
/// reason to fail an exit.
fn persist_cache(
    cache: &ForkCache,
    manager: &SessionManager<Fork>,
    chain_id: u64,
    seeded: Option<&(CacheKey, BaseSnapshot)>,
) {
    let Ok(block_number) = u64::try_from(manager.block_env().number) else {
        tracing::warn!("current block number does not fit a u64; not persisting the fork cache");
        return;
    };
    let key = CacheKey::new(chain_id, block_number);

    let fetched = forkyard_fetch::cache_snapshot(&manager.current_fallback());
    let snapshot = match seeded {
        Some((seeded_key, seeded_base)) if *seeded_key == key => seeded_base.merged_with(&fetched),
        _ => fetched,
    };

    match cache.store(key, &snapshot) {
        Ok(()) => tracing::info!(
            path = %cache.path_for(key).display(),
            accounts = snapshot.account_count(),
            storage_slots = snapshot.storage_count(),
            contracts = snapshot.code_count(),
            "persisted the fork cache — a restart at this block starts warm"
        ),
        Err(error) => {
            tracing::warn!(%error, "could not persist the fork cache; the next start at this block will be cold")
        }
    }
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

    // The on-disk fork cache. `FORKYARD_CACHE_DISABLED` exists so a
    // benchmark can measure cold and warm on demand in one command — with
    // no way to turn it off, the second run of anything is warm and the
    // cold number becomes unmeasurable.
    let cache = if env_flag("FORKYARD_CACHE_DISABLED") {
        tracing::info!("FORKYARD_CACHE_DISABLED is set — every start is a cold start");
        None
    } else {
        let dir = std::env::var_os("FORKYARD_CACHE_DIR").map(PathBuf::from).unwrap_or_else(default_cache_dir);
        Some(ForkCache::new(dir))
    };
    // 0 (the default) means "only at shutdown". A non-zero interval buys
    // back the cache a SIGKILL or a power loss would cost, at the price of
    // serializing the whole snapshot that often.
    let cache_flush_secs: u64 = env_or("FORKYARD_CACHE_FLUSH_SECS", 0);

    let (fork, block_env) = match fork_block_number {
        Some(n) => forkyard_fetch::fork_at(&rpc_url, n).await?,
        None => forkyard_fetch::fork(&rpc_url).await?,
    };

    // Seed from disk before any session exists, so even the very first
    // read of a restarted process is warm. A cache file that is missing,
    // truncated, from another chain or block, or written by an older
    // format is reported and ignored — never fatal, the whole point being
    // that the cache is an optimisation and not a dependency.
    let seeded = cache.as_ref().zip(u64::try_from(block_env.number).ok()).and_then(|(cache, block_number)| {
        let key = CacheKey::new(chain_id, block_number);
        match cache.load(key) {
            Ok(base) => {
                tracing::info!(
                    path = %cache.path_for(key).display(),
                    accounts = base.account_count(),
                    storage_slots = base.storage_count(),
                    contracts = base.code_count(),
                    "loaded a persisted fork cache — this start is warm"
                );
                Some((key, base))
            }
            Err(error) if error.is_missing() => {
                tracing::info!(path = %cache.path_for(key).display(), "no persisted fork cache for this block yet — cold start");
                None
            }
            Err(error) => {
                tracing::warn!(%error, "ignoring the persisted fork cache and starting cold");
                None
            }
        }
    });
    // The factory is the only thing that knows the RPC URL — `forkyard-session`
    // deliberately doesn't, which is what keeps it unit-testable with an
    // in-memory fallback and no network.
    let fork_rpc_url = rpc_url.clone();
    let mut manager = SessionManager::new(fork, block_env, num_workers, Duration::from_secs(ttl_secs))
        .with_block_forks(
            move |number: u64| -> BlockForkFuture<Fork> {
                let rpc_url = fork_rpc_url.clone();
                Box::pin(async move {
                    forkyard_fetch::fork_at(&rpc_url, number).await.map_err(|e| e.to_string())
                })
            },
            max_pinned_blocks,
        );
    if let Some((_, base)) = &seeded {
        manager = manager.with_base(base.clone());
    }
    let manager = Arc::new(manager);
    let seeded = seeded.map(Arc::new);
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

    // Optional interval flush, off by default. Runs on a blocking thread
    // because serializing a large snapshot is real CPU and file I/O, and
    // stalling a runtime worker for it would show up as latency on
    // whichever agent's request happened to share that thread.
    let flush_handle = match (&cache, cache_flush_secs) {
        (Some(cache), secs) if secs > 0 => {
            let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
            let cache = cache.clone();
            let flush_manager = Arc::clone(&manager);
            let flush_seeded = seeded.clone();
            let task = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(secs));
                ticker.tick().await; // fires immediately; nothing to save yet
                loop {
                    tokio::select! {
                        _ = &mut stop_rx => break,
                        _ = ticker.tick() => {
                            let (cache, manager, seeded) =
                                (cache.clone(), Arc::clone(&flush_manager), flush_seeded.clone());
                            let _ = tokio::task::spawn_blocking(move || {
                                persist_cache(&cache, &manager, chain_id, seeded.as_deref());
                            })
                            .await;
                        }
                    }
                }
            });
            tracing::info!(every_secs = secs, "periodic fork-cache flush enabled");
            Some((stop_tx, task))
        }
        _ => None,
    };

    wait_for_shutdown_signal().await;

    if let Some((stop_tx, task)) = ingest_handle {
        let _ = stop_tx.send(());
        let _ = task.await;
    }
    if let Some((stop_tx, task)) = flush_handle {
        let _ = stop_tx.send(());
        let _ = task.await;
    }
    http_handle.shutdown().await;
    mcp_http_handle.shutdown().await;

    // After both surfaces are down, so nothing is still fetching into the
    // backend while it's being read. This is on the SIGTERM path
    // deliberately: the benchmark harness terminates the process with
    // SIGTERM, so a cache written only on a clean stdin-EOF exit would
    // never be written at all.
    if let Some(cache) = &cache {
        persist_cache(cache, &manager, chain_id, seeded.as_deref());
    }

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
