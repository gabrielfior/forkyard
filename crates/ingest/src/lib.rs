//! Keeps a `SessionManager` current to the latest chain head, so new
//! sessions keep forking against a real, moving block *and* fresh
//! account/storage state, instead of whatever was current when the
//! process started. See docs/RESEARCH.md ("System design", layer 1 —
//! chain-tip ingestion).
//!
//! **What "current" means here.** Every tick does one cheap
//! `eth_getBlockByNumber` lookup (`latest_block_env`) just to check whether
//! the chain has actually moved. Only when the block number has advanced
//! does this do the expensive part: re-fork (`forkyard_fetch::fork`) a
//! brand new fallback backend — its own fresh `SharedBackend`, its own
//! empty cache, pinned to that new block — and swap it into the
//! `SessionManager` via `refresh_fallback`. That's a real full update, not
//! just a relabeled block number: every account balance, nonce, storage
//! slot, and piece of code a *newly forked* session reads afterward comes
//! from this new block, not whatever the previous fallback had cached.
//!
//! **What's still bounded, not eliminated.** Sessions already forked
//! before a refresh keep their own clone of the *old* fallback and old
//! `BlockEnv` — by design, matching every other place this codebase treats
//! an in-flight session as a frozen snapshot (see `SessionManager`'s own
//! docs). So staleness is bounded by `poll_interval` for *new* forks, and
//! by a session's own lifetime for sessions already in flight — it is not
//! "always exactly the tip," but it is no longer "grows unboundedly stale
//! for the life of the process," which was the real gap before this.

use std::time::Duration;

use forkyard_fetch::{latest_block_env, Fork};
use forkyard_session::SessionManager;
use tokio::sync::oneshot;
use tracing::{debug, warn};

/// Polls `rpc_url` on a fixed interval and, whenever the chain has actually
/// produced a new block since the last tick, re-forks a fresh fallback
/// backend and pushes it into a `SessionManager` via `refresh_fallback`.
/// Existing sessions are unaffected — each already has its own fallback
/// and `BlockEnv` pinned at fork time — only *new* forks pick up the
/// refreshed state.
pub struct ChainTipFollower {
    rpc_url: String,
    poll_interval: Duration,
}

impl ChainTipFollower {
    pub fn new(rpc_url: impl Into<String>, poll_interval: Duration) -> Self {
        Self { rpc_url: rpc_url.into(), poll_interval }
    }

    /// Runs until `stop` fires or receives a sender drop. A failed refresh
    /// (RPC hiccup, timeout) logs a warning and keeps the previous fallback
    /// and `BlockEnv` in place rather than tearing the loop down — a
    /// transient upstream failure shouldn't take new session creation down
    /// with it.
    ///
    /// Fixed to `SessionManager<Fork>`, not generic over `Fallback`: this
    /// follower's whole job is re-forking `rpc_url` into a fresh
    /// `forkyard_fetch::Fork`, so it only ever makes sense wired to a
    /// manager whose fallback *is* that type — unlike `SessionManager`
    /// itself, which stays generic so tests can swap in an in-memory
    /// fallback.
    pub async fn run(&self, manager: &SessionManager<Fork>, mut stop: oneshot::Receiver<()>) {
        let mut last_seen_number = manager.block_env().number;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {
                    match latest_block_env(&self.rpc_url).await {
                        Ok(tip) if tip.number == last_seen_number => {
                            debug!(number = %tip.number, "no new block since last tick, nothing to refresh");
                        }
                        Ok(tip) => {
                            match forkyard_fetch::fork(&self.rpc_url).await {
                                Ok((fork, block_env)) => {
                                    debug!(
                                        number = %block_env.number,
                                        basefee = block_env.basefee,
                                        "new block seen, refreshed fallback and block context for new sessions"
                                    );
                                    last_seen_number = block_env.number;
                                    manager.refresh_fallback(fork, block_env);
                                }
                                Err(error) => {
                                    warn!(%error, target_number = %tip.number, "saw a new block but failed to re-fork against it, keeping previous fallback");
                                }
                            }
                        }
                        Err(error) => {
                            warn!(%error, "failed to poll latest block this tick, keeping previous fallback");
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
