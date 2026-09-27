//! Wraps `foundry-fork-db`'s `SharedBackend` — the same fetch-and-cache
//! primitive Anvil's fork mode runs on — as a revm `Database`, instead of
//! hand-rolling the sync-revm/async-fetch bridge. See `docs/RESEARCH.md`
//! ("System design", layer 4, "Lazy remote fetch — not reinvented").

use std::cell::RefCell;
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
use alloy_rpc_client::RpcClient;
use revm::primitives::{keccak256, Address, Bytes, B256, KECCAK_EMPTY, U256};
use revm::state::{AccountInfo, Bytecode};

/// `foundry-fork-db`'s backend as a revm database — what `Fork` wraps.
pub type Backend = WrapDatabaseRef<SharedBackend<Ethereum, BlockEnv>>;

/// A live fork of a real chain, backed by an upstream RPC. `SharedBackend`
/// is internally reference-counted, so cloning a `Fork` is cheap and every
/// clone shares the same background fetch thread and cache.
pub type Fork = ReadThrough<Backend>;

/// One piece of upstream state a read needed and didn't have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum StateKey {
    Account(Address),
    Storage(Address, U256),
    BlockHash(u64),
}

thread_local! {
    /// `Some` while `speculate` runs on this thread: the misses so far.
    static MISSES: RefCell<Option<Vec<StateKey>>> = const { RefCell::new(None) };
}

/// Run `f` with every `ReadThrough` miss on this thread *recorded instead of
/// fetched*: the read answers a default (no account, a zero word) at once
/// and execution carries on, so one pass finds as many of the reads it
/// needs as it can instead of stopping at the first. Returns what `f`
/// returned and every key it missed. If that list isn't empty, `f`'s result
/// was computed from made-up values and must be thrown away — along with
/// anything the caller cached from it — then retried once the keys are
/// resolved (`ReadThrough::resolve`). A worker thread never waits on the
/// network this way; it moves on to another session instead.
pub fn speculate<R>(f: impl FnOnce() -> R) -> (R, Vec<StateKey>) {
    let outer = MISSES.with(|m| m.borrow_mut().replace(Vec::new()));
    let result = f();
    let mut misses = MISSES.with(|m| std::mem::replace(&mut *m.borrow_mut(), outer)).unwrap_or_default();
    misses.sort();
    misses.dedup();
    (result, misses)
}

/// `true`, with `key` recorded, when this thread is speculating: the
/// caller answers a default rather than blocking.
fn record_miss(key: StateKey) -> bool {
    MISSES.with(|m| match m.borrow_mut().as_mut() {
        Some(misses) => {
            misses.push(key);
            true
        }
        None => false,
    })
}

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
    /// Resolves many keys in one JSON-RPC batch; `None` resolves them one
    /// inner read each (in parallel), which is what the tests' in-memory
    /// fallbacks get.
    batch: Option<Arc<Batcher>>,
}

/// Straight to upstream, bypassing the backend thread, for `resolve`.
struct Batcher {
    client: RpcClient,
    block: BlockId,
    /// Where the backend keeps what it fetched — written to as well, so
    /// what a batch resolved is persisted like anything else (`cache_snapshot`).
    db: BlockchainDb<BlockEnv>,
    /// A batch is async; `resolve` is called from plain threads.
    runtime: tokio::runtime::Handle,
}

#[derive(Default)]
struct ReadCache {
    accounts: DashMap<Address, Option<AccountInfo>>,
    storage: DashMap<(Address, U256), U256>,
    block_hashes: DashMap<u64, B256>,
}

impl<D> ReadThrough<D> {
    pub fn new(inner: D) -> Self {
        Self { inner, cache: Arc::default(), batch: None }
    }

    /// The wrapped fallback — for `cache_snapshot`, which reads the
    /// backend's own cache (a superset of this one) directly.
    pub fn inner(&self) -> &D {
        &self.inner
    }
}

impl<D: DatabaseRef + Sync> ReadThrough<D>
where
    D::Error: std::fmt::Display,
{
    /// Fetch every key into the shared cache, blocking the calling thread
    /// — one JSON-RPC batch for all of them when this fork has an upstream,
    /// so a speculative pass that missed k keys costs one round trip, not k.
    pub fn resolve(&self, keys: &[StateKey]) -> Result<(), String> {
        let keys: Vec<StateKey> = keys.iter().copied().filter(|k| !self.is_cached(k)).collect();
        if keys.is_empty() {
            return Ok(());
        }
        match &self.batch {
            Some(batch) => self.resolve_batched(batch, &keys),
            None => std::thread::scope(|scope| {
                let reads: Vec<_> = keys.iter().map(|key| scope.spawn(move || self.read_blocking(key))).collect();
                reads.into_iter().try_for_each(|r| r.join().map_err(|_| "resolver panicked".to_string())?)
            }),
        }
    }

    fn is_cached(&self, key: &StateKey) -> bool {
        match key {
            StateKey::Account(a) => self.cache.accounts.contains_key(a),
            StateKey::Storage(a, i) => self.cache.storage.contains_key(&(*a, *i)),
            StateKey::BlockHash(n) => self.cache.block_hashes.contains_key(n),
        }
    }

    fn read_blocking(&self, key: &StateKey) -> Result<(), String> {
        let result = match key {
            StateKey::Account(a) => self.basic_ref(*a).map(drop),
            StateKey::Storage(a, i) => self.storage_ref(*a, *i).map(drop),
            StateKey::BlockHash(n) => self.block_hash_ref(*n).map(drop),
        };
        result.map_err(|e| e.to_string())
    }

    fn resolve_batched(&self, batch: &Batcher, keys: &[StateKey]) -> Result<(), String> {
        let block = batch.block;
        let mut request = batch.client.new_batch();
        let mut accounts = Vec::new();
        let mut slots = Vec::new();
        let mut hashes = Vec::new();
        fn err(e: impl std::fmt::Display) -> String {
            e.to_string()
        }
        for key in keys {
            match *key {
                StateKey::Account(address) => accounts.push((
                    address,
                    request.add_call::<_, U256>("eth_getBalance", &(address, block)).map_err(err)?,
                    request.add_call::<_, U256>("eth_getTransactionCount", &(address, block)).map_err(err)?,
                    request.add_call::<_, Bytes>("eth_getCode", &(address, block)).map_err(err)?,
                )),
                StateKey::Storage(address, index) => slots.push((
                    address,
                    index,
                    request.add_call::<_, U256>("eth_getStorageAt", &(address, index, block)).map_err(err)?,
                )),
                // No lighter call for a block hash than the whole header;
                // rare enough to leave to the backend.
                StateKey::BlockHash(number) => hashes.push(number),
            }
        }
        batch.runtime.block_on(async {
            request.send().await.map_err(err)?;
            for (address, balance, nonce, code) in accounts {
                let (balance, nonce, code) = (balance.await.map_err(err)?, nonce.await.map_err(err)?, code.await.map_err(err)?);
                // The shape `SharedBackend` itself stores: code inline,
                // hashed, `KECCAK_EMPTY` for none.
                let (code_hash, code) = if code.is_empty() {
                    (KECCAK_EMPTY, None)
                } else {
                    (keccak256(&code), Some(Bytecode::new_raw(code)))
                };
                let info = AccountInfo { balance, nonce: nonce.to::<u64>(), code_hash, code, ..Default::default() };
                batch.db.accounts().write().insert(address, info.clone());
                self.cache.accounts.insert(address, Some(info));
            }
            for (address, index, value) in slots {
                let value = value.await.map_err(err)?;
                batch.db.storage().write().entry(address).or_default().insert(index, value);
                self.cache.storage.insert((address, index), value);
            }
            Ok::<_, String>(())
        })?;
        hashes.into_iter().try_for_each(|n| self.read_blocking(&StateKey::BlockHash(n)))
    }
}

impl<D: DatabaseRef> DatabaseRef for ReadThrough<D> {
    type Error = D::Error;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(hit) = self.cache.accounts.get(&address) {
            return Ok(hit.clone());
        }
        if record_miss(StateKey::Account(address)) {
            return Ok(None);
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
        if record_miss(StateKey::Storage(address, index)) {
            return Ok(U256::ZERO);
        }
        let value = self.inner.storage_ref(address, index)?;
        self.cache.storage.insert((address, index), value);
        Ok(value)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        if let Some(hit) = self.cache.block_hashes.get(&number) {
            return Ok(*hit);
        }
        if record_miss(StateKey::BlockHash(number)) {
            return Ok(B256::ZERO);
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
    let fork = fork_from_provider(provider, rpc_url, block_env.clone())?;
    Ok((fork, block_env))
}

/// Must be called from inside a tokio runtime: the batcher keeps its handle.
fn fork_from_provider<P: Provider<Ethereum> + 'static>(
    provider: P,
    rpc_url: &str,
    block_env: BlockEnv,
) -> eyre::Result<Fork> {
    let pin = BlockId::number(block_env.number.to::<u64>());
    let meta = BlockchainDbMeta::new(block_env, rpc_url.to_string());
    let db = BlockchainDb::new(meta, None);
    let batch = Batcher {
        client: RpcClient::new_http(rpc_url.parse()?),
        block: pin,
        db: db.clone(),
        runtime: tokio::runtime::Handle::try_current()?,
    };
    // `pin_block: None` sends every account/storage/code read to `latest`
    // whatever block was forked, so `fork_at(url, N)` was a label on live
    // state. Two sessions at different blocks read identical state.
    let backend = SharedBackend::spawn_backend_thread(provider, db, Some(pin));
    let mut fork = ReadThrough::new(WrapDatabaseRef(backend));
    fork.batch = Some(Arc::new(batch));
    Ok(fork)
}

/// A fork at `block_env`'s block without asking upstream for its header —
/// `block_env` must be that block's real one, e.g. what a persisted fork
/// cache recorded (`ForkCache::load_with_block_env`). Makes no network
/// call at all: the backend thread only connects on its first miss. What
/// takes a pinned warm restart from one header round trip to zero.
pub async fn fork_with_block_env(rpc_url: &str, block_env: BlockEnv) -> eyre::Result<Fork> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    fork_from_provider(provider, rpc_url, block_env)
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

    #[test]
    fn a_speculative_miss_is_recorded_and_answered_without_the_backend() {
        let (fork, hits) = counting(0);
        let address = Address::with_last_byte(3);

        let ((info, slot), misses) =
            speculate(|| (fork.basic_ref(address).unwrap(), fork.storage_ref(address, U256::from(1u64)).unwrap()));

        assert_eq!((info, slot), (None, U256::ZERO), "a miss answers a default at once");
        assert_eq!(misses, vec![StateKey::Account(address), StateKey::Storage(address, U256::from(1u64))]);
        assert_eq!(hits.load(Ordering::Relaxed), 0, "speculating must never reach the backend");

        // Nothing made up was cached: a normal read still goes upstream.
        assert_eq!(fork.basic_ref(address).unwrap().unwrap().balance, U256::from(7u64));
        assert_eq!(hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn resolving_the_misses_makes_the_next_speculative_pass_clean() {
        let (fork, hits) = counting(0);
        let address = Address::with_last_byte(4);
        let (_, misses) = speculate(|| fork.basic_ref(address).unwrap());

        fork.resolve(&misses).unwrap();
        assert_eq!(hits.load(Ordering::Relaxed), 1);

        let (info, misses) = speculate(|| fork.basic_ref(address).unwrap());
        assert!(misses.is_empty());
        assert_eq!(info.unwrap().balance, U256::from(7u64));

        // Already cached: resolving again costs nothing.
        fork.resolve(&[StateKey::Account(address)]).unwrap();
        assert_eq!(hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_failed_resolve_says_so() {
        let (fork, _) = counting(usize::MAX);
        assert!(fork.resolve(&[StateKey::Account(Address::with_last_byte(5))]).is_err());
    }

    #[test]
    fn speculation_is_per_thread() {
        let (fork, hits) = counting(0);
        let address = Address::with_last_byte(6);
        let (_, misses) = speculate(|| {
            // Another thread isn't speculating, so its read really happens.
            std::thread::scope(|s| s.spawn(|| fork.basic_ref(address).unwrap()).join().unwrap())
        });
        assert!(misses.is_empty());
        assert_eq!(hits.load(Ordering::Relaxed), 1);
    }
}
