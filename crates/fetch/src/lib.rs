//! Wraps `foundry-fork-db`'s `SharedBackend` — the same fetch-and-cache
//! primitive Anvil's fork mode runs on — as a revm `Database`, instead of
//! hand-rolling the sync-revm/async-fetch bridge. See `docs/RESEARCH.md`
//! ("System design", layer 4, "Lazy remote fetch — not reinvented").

use alloy_network::Ethereum;
use alloy_provider::ProviderBuilder;
use foundry_fork_db::cache::BlockchainDbMeta;
use foundry_fork_db::{BlockchainDb, SharedBackend};
use revm::context::BlockEnv;
use revm::database_interface::WrapDatabaseRef;

/// A live fork of a real chain, backed by an upstream RPC. `SharedBackend`
/// is internally reference-counted, so cloning a `Fork` is cheap and every
/// clone shares the same background fetch thread and cache.
pub type Fork = WrapDatabaseRef<SharedBackend<Ethereum, BlockEnv>>;

/// Fork `rpc_url` at its current head. Spawns a dedicated background thread
/// that owns the actual network I/O (`foundry-fork-db`'s own pattern,
/// mirrored by our worker-thread design rather than copied wholesale) —
/// reads against the returned `Fork` block until that thread resolves them,
/// then return from cache on every later call, exactly like Anvil's fork
/// mode. Dropping every clone of the returned `Fork` tears the thread down.
pub fn fork(rpc_url: &str) -> eyre::Result<Fork> {
    let url = rpc_url.parse()?;
    let provider = ProviderBuilder::new().connect_http(url);
    let meta = BlockchainDbMeta::new(BlockEnv::default(), rpc_url.to_string());
    let db = BlockchainDb::new(meta, None);
    let backend = SharedBackend::spawn_backend_thread(provider, db, None);
    Ok(WrapDatabaseRef(backend))
}
