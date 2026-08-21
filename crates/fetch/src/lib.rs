//! Wraps `foundry-fork-db`'s `SharedBackend` — the same fetch-and-cache
//! primitive Anvil's fork mode runs on — as a revm `Database`, instead of
//! hand-rolling the sync-revm/async-fetch bridge. See `docs/RESEARCH.md`
//! ("System design", layer 4, "Lazy remote fetch — not reinvented").

use alloy_network::Ethereum;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types::BlockId;
use foundry_fork_db::cache::BlockchainDbMeta;
use foundry_fork_db::{BlockchainDb, SharedBackend};
use revm::context::BlockEnv;
use revm::database_interface::WrapDatabaseRef;
use revm::primitives::U256;

/// A live fork of a real chain, backed by an upstream RPC. `SharedBackend`
/// is internally reference-counted, so cloning a `Fork` is cheap and every
/// clone shares the same background fetch thread and cache.
pub type Fork = WrapDatabaseRef<SharedBackend<Ethereum, BlockEnv>>;

async fn block_env_from_provider<P: Provider<Ethereum>>(provider: &P) -> eyre::Result<BlockEnv> {
    let block = provider
        .get_block(BlockId::latest())
        .await?
        .ok_or_else(|| eyre::eyre!("upstream RPC returned no latest block"))?;
    let header = &block.header;
    Ok(BlockEnv {
        number: U256::from(header.number),
        timestamp: U256::from(header.timestamp),
        basefee: header.base_fee_per_gas.unwrap_or(0),
        gas_limit: header.gas_limit,
        ..Default::default()
    })
}

/// Fetches just the real block context (number, timestamp, base fee) for
/// `rpc_url`'s latest block — the same lookup `fork` does once at startup,
/// exposed standalone so `forkyard-ingest` can call it again periodically
/// to keep a `SessionManager`'s `BlockEnv` from going stale. Opens its own
/// short-lived provider connection each call — negligible overhead against
/// a poll interval measured in seconds, and it keeps this crate decoupled
/// from needing to share `fork`'s own provider instance.
pub async fn latest_block_env(rpc_url: &str) -> eyre::Result<BlockEnv> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    block_env_from_provider(&provider).await
}

/// Fork `rpc_url` at its current head, returning both the fork itself and
/// the real `BlockEnv` (number, timestamp, base fee) of the block it's
/// pinned to. Spawns a dedicated background thread that owns the actual
/// network I/O (`foundry-fork-db`'s own pattern, mirrored by our
/// worker-thread design rather than copied wholesale) — reads against the
/// returned `Fork` block until that thread resolves them, then return from
/// cache on every later call, exactly like Anvil's fork mode. Dropping
/// every clone of the returned `Fork` tears the thread down.
///
/// The returned `BlockEnv` is the caller's responsibility to actually wire
/// into revm's execution context — `foundry-fork-db`'s own `BlockEnv` is
/// only used for its fork-cache bookkeeping, not fed into any `Evm`
/// automatically. Skipping this was a real bug: every transaction run
/// against a `Fork` without it executes with basefee=0, block number=0,
/// regardless of what block was actually forked.
pub async fn fork(rpc_url: &str) -> eyre::Result<(Fork, BlockEnv)> {
    let url = rpc_url.parse()?;
    let provider = ProviderBuilder::new().connect_http(url);
    let block_env = block_env_from_provider(&provider).await?;

    let meta = BlockchainDbMeta::new(block_env.clone(), rpc_url.to_string());
    let db = BlockchainDb::new(meta, None);
    let backend = SharedBackend::spawn_backend_thread(provider, db, None);
    Ok((WrapDatabaseRef(backend), block_env))
}
