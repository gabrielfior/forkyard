//! Keeps one shared, warm `BaseSnapshot` current to the latest block, so
//! every session forked from it starts pre-warmed instead of paying its own
//! fetch latency. See `docs/RESEARCH.md` ("System design", layer 1).
//!
//! Not required for a single local session — `forkyard-fetch`'s
//! `SharedBackend` already caches correctly on its own, as the working
//! `mainnet_transfer` example demonstrates with no ingestion running at
//! all. This crate exists for the case that example doesn't cover: many
//! *concurrent* sessions sharing one base, where `SharedBackend`'s
//! channel-and-thread design adds latency on every read (even cache hits),
//! while a direct, lock-free `imbl` read does not. That's the whole reason
//! `forkyard-engine::BaseSnapshot` exists instead of using `SharedBackend`
//! as the base directly.

use std::sync::Arc;

use forkyard_engine::BaseSnapshot;

/// Subscribes to `newHeads` on one chain and keeps `base` advancing to the
/// latest block. TODO: pick the websocket/pubsub client (`alloy-provider`'s
/// `pubsub` feature is the natural choice, consistent with `forkyard-fetch`)
/// and implement the actual subscription + diff application.
pub struct ChainTipFollower {
    rpc_url: String,
}

impl ChainTipFollower {
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self {
            rpc_url: rpc_url.into(),
        }
    }

    /// Runs until cancelled, publishing each new base version to whatever
    /// holds a clone of `base`. TODO: wire the real subscription.
    pub async fn run(&self, _base: Arc<BaseSnapshot>) -> eyre::Result<()> {
        let _ = &self.rpc_url;
        todo!("subscribe to newHeads on {} and advance base", self.rpc_url)
    }
}
