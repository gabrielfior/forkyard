//! Wraps `foundry-fork-db`'s `SharedBackend` — the same fetch-and-cache
//! primitive Anvil's fork mode runs on — as a revm `Database`, instead of
//! hand-rolling the sync-revm/async-fetch bridge. See `docs/RESEARCH.md`
//! ("System design", layer 4, "Lazy remote fetch — not reinvented").

use alloy_network::Ethereum;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types::BlockId;
use forkyard_engine::BaseSnapshot;
use foundry_fork_db::cache::BlockchainDbMeta;
use foundry_fork_db::{BlockchainDb, SharedBackend};
use revm::context::BlockEnv;
use revm::database_interface::WrapDatabaseRef;
use revm::primitives::U256;

/// A live fork of a real chain, backed by an upstream RPC. `SharedBackend`
/// is internally reference-counted, so cloning a `Fork` is cheap and every
/// clone shares the same background fetch thread and cache.
pub type Fork = WrapDatabaseRef<SharedBackend<Ethereum, BlockEnv>>;

async fn block_env_from_provider_at<P: Provider<Ethereum>>(
    provider: &P,
    block: BlockId,
) -> eyre::Result<BlockEnv> {
    let block = provider
        .get_block(block)
        .await?
        .ok_or_else(|| eyre::eyre!("upstream RPC returned no block for the requested id"))?;
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
    block_env_from_provider_at(&provider, BlockId::latest()).await
}

async fn fork_impl(rpc_url: &str, block: BlockId) -> eyre::Result<(Fork, BlockEnv)> {
    let url = rpc_url.parse()?;
    let provider = ProviderBuilder::new().connect_http(url);
    let block_env = block_env_from_provider_at(&provider, block).await?;

    let meta = BlockchainDbMeta::new(block_env.clone(), rpc_url.to_string());
    let db = BlockchainDb::new(meta, None);
    // Pin the backend's *state* reads to the same block the `BlockEnv` was
    // read from. `SharedBackend`'s `pin_block: None` means every
    // account/storage/code fetch goes to `latest` no matter which block was
    // forked — so `fork_at(url, N)` used to be a block-number label stuck on
    // live state, not a fork of block N. Invisible when forking the tip;
    // fatal for per-session block pinning, where two sessions at different
    // blocks would otherwise read the exact same (latest) state.
    let pin = BlockId::number(block_env.number.to::<u64>());
    let backend = SharedBackend::spawn_backend_thread(provider, db, Some(pin));
    Ok((WrapDatabaseRef(backend), block_env))
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
    fork_impl(rpc_url, BlockId::latest()).await
}

/// Same as `fork`, but pinned to `block_number` instead of the chain tip —
/// what lets a caller (e.g. `forkyard-bin`, via `FORKYARD_FORK_BLOCK_NUMBER`)
/// run a benchmark or test scenario against a fixed, reproducible block
/// instead of whatever happens to be current.
pub async fn fork_at(rpc_url: &str, block_number: u64) -> eyre::Result<(Fork, BlockEnv)> {
    fork_impl(rpc_url, BlockId::number(block_number)).await
}

/// Everything this fork has actually fetched from upstream so far, as a
/// `BaseSnapshot` — the form `forkyard_engine::persist` writes to disk and
/// `SessionManager::with_base` reads back at startup.
///
/// This has to come from the fetch backend rather than from the manager's
/// own base: a session caches a resolved read in its private overlay and
/// the shared base is only ever seeded, never grown (see `Session`'s
/// `TODO(fetch)`), so the backend's cache is the one place holding
/// everything every session ever paid for. It is also pure chain state —
/// nothing in this workspace ever commits an execution diff into the
/// backend, so a session's `advance` writes cannot leak into what gets
/// persisted.
///
/// Copies the backend's maps under its own locks; paid once, at shutdown.
pub fn cache_snapshot(fork: &Fork) -> BaseSnapshot {
    let backend = &fork.0;
    let accounts = backend.accounts();

    // The backend keeps code inline on each `AccountInfo` and has no
    // hash-keyed map of its own, so build one here: `Session::code_by_hash`
    // looks code up by hash, and code that only ever existed inline would
    // send that lookup upstream for a contract we already hold.
    let code: Vec<_> = accounts
        .values()
        .filter_map(|info| info.code.as_ref())
        .filter(|code| !code.is_empty())
        .map(|code| (code.hash_slow(), code.clone()))
        .collect();

    let storage = backend
        .storage()
        .into_iter()
        .flat_map(|(address, slots)| slots.into_iter().map(move |(key, value)| ((address, key), value)));

    // The engine keys block hashes by `u64` where the backend keys them by
    // `U256`. A height that doesn't fit a u64 can't have been asked for
    // through the engine in the first place, so drop it rather than invent
    // a truncated key that would answer for some other block.
    let block_hashes = backend
        .block_hashes()
        .into_iter()
        .filter_map(|(number, hash)| u64::try_from(number).ok().map(|number| (number, hash)));

    BaseSnapshot::from_parts(accounts.clone(), code, storage, block_hashes)
}
