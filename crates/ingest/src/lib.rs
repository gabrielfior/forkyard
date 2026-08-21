//! Keeps a `SessionManager`'s block context current to the latest chain
//! head, so new sessions keep forking against a real, moving block instead
//! of whatever was current when the process started. See docs/RESEARCH.md
//! ("System design", layer 1 — chain-tip ingestion).
//!
//! **v1 scope limitation — must be resolved before production.** This
//! only refreshes `SessionManager`'s `BlockEnv` (block number, timestamp,
//! base fee): the thing new sessions get pinned to at fork time. It does
//! **not** invalidate or refresh anything already cached in the shared
//! `forkyard-fetch` fallback (`SharedBackend`'s own cache) — account
//! balances, nonces, storage slots, and code fetched once stay cached for
//! the life of the process, regardless of how far the real chain moves
//! past them. A long-running instance will silently serve increasingly
//! stale account/storage reads even while `eth_blockNumber` correctly
//! advances. This is acceptable for local/dev use (a single agent's
//! session lives seconds to minutes, well inside normal staleness) but is
//! a real correctness gap for any shared, long-lived deployment — closing
//! it needs either a TTL/eviction policy on the fetch cache or rebuilding
//! `BaseSnapshot`/fallback state on some cadence, neither of which this
//! crate does yet.

use std::time::Duration;

use forkyard_fetch::latest_block_env;
use forkyard_session::{Fallback, SessionManager};
use tokio::sync::oneshot;
use tracing::{debug, warn};

/// Polls `rpc_url` for its latest block on a fixed interval and pushes the
/// result into a `SessionManager` via `set_block_env`. Existing sessions
/// are unaffected — each already has its own `BlockEnv` pinned at fork
/// time — only *new* forks pick up the refreshed value.
pub struct ChainTipFollower {
    rpc_url: String,
    poll_interval: Duration,
}

impl ChainTipFollower {
    pub fn new(rpc_url: impl Into<String>, poll_interval: Duration) -> Self {
        Self { rpc_url: rpc_url.into(), poll_interval }
    }

    /// Runs until `stop` fires or receives a sender drop. A failed refresh
    /// (RPC hiccup, timeout) logs a warning and keeps the previous
    /// `BlockEnv` in place rather than tearing the loop down — a transient
    /// upstream failure shouldn't take new session creation down with it.
    pub async fn run<F: Fallback>(&self, manager: &SessionManager<F>, mut stop: oneshot::Receiver<()>)
    where
        F::Error: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
    {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {
                    match latest_block_env(&self.rpc_url).await {
                        Ok(block_env) => {
                            debug!(number = %block_env.number, basefee = block_env.basefee, "refreshed shared block context");
                            manager.set_block_env(block_env);
                        }
                        Err(error) => {
                            warn!(%error, "failed to refresh latest block context this tick, keeping previous value");
                        }
                    }
                }
                _ = &mut stop => {
                    debug!("stop signal received, ending chain-tip follower loop");
                    break;
                }
            }
        }
    }
}
