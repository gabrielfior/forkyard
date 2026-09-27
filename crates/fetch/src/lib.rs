//! Wraps `foundry-fork-db`'s `SharedBackend` — the same fetch-and-cache
//! primitive Anvil's fork mode runs on — as a revm `Database`, instead of
//! hand-rolling the sync-revm/async-fetch bridge. See `docs/RESEARCH.md`
//! ("System design", layer 4, "Lazy remote fetch — not reinvented").

use std::sync::Arc;

use alloy_network::Ethereum;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types::BlockId;
use dashmap::DashMap;
use forkyard_engine::BaseSnapshot;
use foundry_fork_db::cache::BlockchainDbMeta;
use foundry_fork_db::{BlockchainDb, SharedBackend};
use revm::context::BlockEnv;
use revm::database_interface::{DatabaseRef, WrapDatabaseRef};
use revm::primitives::{Address, B256, U256};
use revm::state::{AccountInfo, Bytecode};

/// `foundry-fork-db`'s backend as a revm database — what `Fork` wraps.
pub type Backend = WrapDatabaseRef<SharedBackend<Ethereum, BlockEnv>>;

/// A live fork of a real chain, backed by an upstream RPC. `SharedBackend`
/// is internally reference-counted, so cloning a `Fork` is cheap and every
/// clone shares the same background fetch thread and cache.
pub type Fork = ReadThrough<Backend>;

/// A concurrent read cache in front of a fallback, shared by every clone.
///
/// `SharedBackend` already caches everything it fetches, but only behind
/// its own background thread: every read — a hit included — is a request
/// sent to that one thread and a reply waited for. Every session on every
/// worker funnels through it, so N sessions opening against the same hot
/// contracts queue up on one thread for answers it already has. This
/// answers a repeat read on the calling thread instead. A fork is pinned
/// to one block, so nothing cached here can go stale; errors are never
/// cached, so a transient upstream failure is retried next time.
#[derive(Clone)]
pub struct ReadThrough<D> {
    inner: D,
    cache: Arc<ReadCache>,
}

#[derive(Default)]
struct ReadCache {
    accounts: DashMap<Address, Option<AccountInfo>>,
    storage: DashMap<(Address, U256), U256>,
    block_hashes: DashMap<u64, B256>,
}

impl<D> ReadThrough<D> {
    pub fn new(inner: D) -> Self {
        Self { inner, cache: Arc::default() }
    }

    /// The wrapped fallback — for `cache_snapshot`, which reads the
    /// backend's own cache (a superset of this one) directly.
    pub fn inner(&self) -> &D {
        &self.inner
    }
}

impl<D: DatabaseRef> DatabaseRef for ReadThrough<D> {
    type Error = D::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(hit) = self.cache.accounts.get(&address) {
            return Ok(hit.clone());
        }
        let info = self.inner.basic_ref(address)?;
        self.cache.accounts.insert(address, info.clone());
        Ok(info)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // Code arrives inline on `basic_ref`; the backend has no by-hash
        // lookup to cache.
        self.inner.code_by_hash_ref(code_hash)
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        if let Some(hit) = self.cache.storage.get(&(address, index)) {
            return Ok(*hit);
        }
        let value = self.inner.storage_ref(address, index)?;
        self.cache.storage.insert((address, index), value);
        Ok(value)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        if let Some(hit) = self.cache.block_hashes.get(&number) {
            return Ok(*hit);
        }
        let hash = self.inner.block_hash_ref(number)?;
        self.cache.block_hashes.insert(number, hash);
        Ok(hash)
    }
}

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
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let block_env = block_env_from_provider_at(&provider, block).await?;
    let fork = fork_from_provider(provider, rpc_url, block_env.clone());
    Ok((fork, block_env))
}

fn fork_from_provider<P: Provider<Ethereum> + 'static>(provider: P, rpc_url: &str, block_env: BlockEnv) -> Fork {
    let pin = BlockId::number(block_env.number.to::<u64>());
    let meta = BlockchainDbMeta::new(block_env, rpc_url.to_string());
    let db = BlockchainDb::new(meta, None);
    // `pin_block: None` sends every account/storage/code read to `latest`
    // whatever block was forked, so `fork_at(url, N)` was a label on live
    // state. Two sessions at different blocks read identical state.
    let backend = SharedBackend::spawn_backend_thread(provider, db, Some(pin));
    ReadThrough::new(WrapDatabaseRef(backend))
}

/// A fork at `block_env`'s block without asking upstream for its header —
/// `block_env` must be that block's real one, e.g. what a persisted fork
/// cache recorded (`ForkCache::load_with_block_env`). Makes no network
/// call at all: the backend thread only connects on its first miss. What
/// takes a pinned warm restart from one header round trip to zero.
pub async fn fork_with_block_env(rpc_url: &str, block_env: BlockEnv) -> eyre::Result<Fork> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    Ok(fork_from_provider(provider, rpc_url, block_env))
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

/// Everything this fork has fetched from upstream so far, in the form
/// `forkyard_engine::persist` writes and `SessionManager::with_base` reads.
///
/// Must come from the backend, not the manager's base: the base is only
/// ever seeded, never grown, so the backend's cache is the one place
/// holding what every session paid for. Copies its maps under lock.
pub fn cache_snapshot(fork: &Fork) -> BaseSnapshot {
    let backend = &fork.inner().0;
    let accounts = backend.accounts();

    // The backend only keeps code inline on each `AccountInfo`, so build the
    // hash-keyed map `Session::code_by_hash` needs; otherwise that lookup
    // goes upstream for a contract we already hold.
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

    // The engine keys block hashes by `u64`, the backend by `U256`. Drop
    // what doesn't fit rather than truncate into another block's key.
    let block_hashes = backend
        .block_hashes()
        .into_iter()
        .filter_map(|(number, hash)| u64::try_from(number).ok().map(|number| (number, hash)));

    BaseSnapshot::from_parts(accounts.clone(), code, storage, block_hashes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Counts every read that reaches it, and fails the first `fail_first`
    /// of them — standing in for a transient upstream error.
    #[derive(Clone)]
    struct Counting {
        hits: Arc<AtomicUsize>,
        fail_first: usize,
    }

    #[derive(Debug)]
    struct Upstream;
    impl std::fmt::Display for Upstream {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "upstream failed")
        }
    }
    impl std::error::Error for Upstream {}
    impl revm::database_interface::DBErrorMarker for Upstream {}

    impl Counting {
        fn hit(&self) -> Result<(), Upstream> {
            let n = self.hits.fetch_add(1, Ordering::Relaxed);
            if n < self.fail_first { Err(Upstream) } else { Ok(()) }
        }
    }

    impl DatabaseRef for Counting {
        type Error = Upstream;
        fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            self.hit()?;
            Ok(Some(AccountInfo { balance: U256::from(7u64), ..Default::default() }))
        }
        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            self.hit()?;
            Ok(Bytecode::default())
        }
        fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
            self.hit()?;
            Ok(U256::from(3u64))
        }
        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            self.hit()?;
            Ok(B256::ZERO)
        }
    }

    fn counting(fail_first: usize) -> (ReadThrough<Counting>, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        (ReadThrough::new(Counting { hits: Arc::clone(&hits), fail_first }), hits)
    }

    #[test]
    fn a_repeat_read_from_any_clone_never_reaches_the_backend() {
        let (fork, hits) = counting(0);
        let sibling = fork.clone();
        let address = Address::with_last_byte(1);

        fork.basic_ref(address).unwrap();
        fork.storage_ref(address, U256::ZERO).unwrap();
        fork.block_hash_ref(5).unwrap();
        assert_eq!(hits.load(Ordering::Relaxed), 3);

        assert_eq!(sibling.basic_ref(address).unwrap().unwrap().balance, U256::from(7u64));
        assert_eq!(sibling.storage_ref(address, U256::ZERO).unwrap(), U256::from(3u64));
        sibling.block_hash_ref(5).unwrap();
        assert_eq!(hits.load(Ordering::Relaxed), 3, "a sibling session must be served from the shared layer");
    }

    #[test]
    fn a_failed_read_is_retried_rather_than_cached() {
        let (fork, hits) = counting(1);
        let address = Address::with_last_byte(2);

        assert!(fork.basic_ref(address).is_err());
        assert!(fork.basic_ref(address).is_ok(), "one upstream hiccup must not poison the address for good");
        assert_eq!(hits.load(Ordering::Relaxed), 2);
    }
}
