//! Wraps `foundry-fork-db`'s `SharedBackend` — the same fetch-and-cache
//! primitive Anvil's fork mode runs on — as a revm `Database`, instead of
//! hand-rolling the sync-revm/async-fetch bridge. See `docs/RESEARCH.md`
//! ("System design", layer 4, "Lazy remote fetch — not reinvented").

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

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
    let restore = Restore(Some(MISSES.with(|m| m.borrow_mut().replace(Vec::new()))));
    let result = f();
    let mut misses = restore.finish().unwrap_or_default();
    misses.sort();
    misses.dedup();
    (result, misses)
}

/// Puts back what this thread was recording before `speculate` began —
/// on return *and* on unwind. A worker survives a job's panic, and a
/// thread left speculating would answer every later read, blocking ones
/// included, with a placeholder instead of real state.
struct Restore(Option<Option<Vec<StateKey>>>);

impl Restore {
    /// The misses recorded since `speculate` began.
    fn finish(mut self) -> Option<Vec<StateKey>> {
        let outer = self.0.take().expect("finished once");
        MISSES.with(|m| std::mem::replace(&mut *m.borrow_mut(), outer))
    }
}

impl Drop for Restore {
    fn drop(&mut self) {
        if let Some(outer) = self.0.take() {
            MISSES.with(|m| *m.borrow_mut() = outer);
        }
    }
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

/// How often the shared upstream connection is exercised to keep it open —
/// well inside typical idle timeouts (reqwest's pool: 90 s). Three cheap
/// `eth_chainId` calls a minute, process-wide.
const KEEPALIVE: Duration = Duration::from_secs(20);

/// Default cap on batches in flight to one upstream at once
/// (`set_upstream_max_in_flight` overrides it). A burst beyond it queues here
/// instead of arriving at the provider all at once and being refused: 1,000
/// agents opening together had 427 batches rejected with `-32005: rate
/// limit exceeded`.
pub const DEFAULT_UPSTREAM_MAX_IN_FLIGHT: usize = 256;

static MAX_IN_FLIGHT: AtomicUsize = AtomicUsize::new(DEFAULT_UPSTREAM_MAX_IN_FLIGHT);

/// Set the cap for upstream clients created from now on — call it before
/// the first fork. `0` is taken as `1`.
pub fn set_upstream_max_in_flight(max: usize) {
    MAX_IN_FLIGHT.store(max.max(1), Ordering::Relaxed);
}

/// Retries of a rate-limited batch before its error is returned. With
/// `retry_delay`'s schedule, about 6 s of backing off in all.
const RATE_LIMIT_RETRIES: u32 = 6;

/// The one upstream connection for a URL in this process, and the cap on
/// what's in flight over it — shared, so the cap holds across every fork.
#[derive(Clone)]
struct Upstream {
    client: RpcClient,
    in_flight: Arc<tokio::sync::Semaphore>,
}

/// The one upstream client for `rpc_url` in this process, shared by every
/// fork — the default block, pinned blocks, each re-fork at a new tip — so
/// they all resolve over one warm, HTTP/2-multiplexed connection.
///
/// It's opened in the background the first time it's asked for and kept
/// open from then on. Without that, the first burst of misses paid for the
/// connection: 50 agents opening at once each found no connection yet and
/// dialed their own, and every agent's first miss took ~100 ms longer
/// than any later one. Must be called inside a tokio runtime.
fn shared_upstream(rpc_url: &str) -> eyre::Result<Upstream> {
    static UPSTREAMS: OnceLock<Mutex<HashMap<String, Upstream>>> = OnceLock::new();
    let mut upstreams = UPSTREAMS.get_or_init(Default::default).lock().unwrap();
    if let Some(upstream) = upstreams.get(rpc_url) {
        return Ok(upstream.clone());
    }
    let client = RpcClient::new_http(rpc_url.parse()?);
    let keepalive = client.clone();
    tokio::spawn(async move {
        loop {
            // A failure is harmless: the next real request dials again.
            let _ = keepalive.request_noparams::<U256>("eth_chainId").await;
            tokio::time::sleep(KEEPALIVE).await;
        }
    });
    let upstream = Upstream {
        client,
        in_flight: Arc::new(tokio::sync::Semaphore::new(MAX_IN_FLIGHT.load(Ordering::Relaxed))),
    };
    upstreams.insert(rpc_url.to_string(), upstream.clone());
    Ok(upstream)
}

/// A provider saying "slow down" rather than "no": JSON-RPC's `-32005`
/// (what Infura, Alchemy, QuickNode and Tenderly return) or an HTTP 429.
///
/// Matched on the phrases alloy renders those as — `error code -32005`,
/// `HTTP error 429` — never a bare number: error text carries hashes and
/// addresses, and "429" turns up in about 1.5% of random 32-byte hex,
/// which would turn a permanent error into ~6 s of pointless retries.
fn is_rate_limited(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("error code -32005")
        || error.contains("\"code\":-32005")
        || error.contains("http error 429")
        || error.contains("rate limit exceeded")
        || error.contains("too many requests")
}

/// Exponential from 100 ms, capped at 2 s, with jitter, so a burst refused
/// together doesn't retry together.
fn retry_delay(attempt: u32) -> Duration {
    let base = (100u64 << attempt.min(5)).min(2_000);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
    // ±25% around `base`.
    let jitter = u64::from(nanos) % (base / 2 + 1);
    Duration::from_millis(base * 3 / 4 + jitter)
}

/// Straight to upstream, bypassing the backend thread, for `resolve`.
struct Batcher {
    upstream: Upstream,
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
    /// Keys some `resolve` is fetching right now. A session that misses one
    /// waits for that fetch instead of sending its own: without this, N
    /// sessions opening cold against the same contracts each fetched them —
    /// 10 agents cost 343 upstream calls where one agent costs 37.
    in_flight: Mutex<HashMap<StateKey, Arc<Flight>>>,
}

/// One key's fetch in progress, for anyone else who needs that key to wait on.
#[derive(Default)]
struct Flight {
    landed: Mutex<bool>,
    wake: Condvar,
}

impl Flight {
    fn wait(&self) {
        let mut landed = self.landed.lock().unwrap();
        while !*landed {
            landed = self.wake.wait(landed).unwrap();
        }
    }
}

/// Ends the flights for `keys` when dropped — on success, error or unwind
/// alike, so a failed fetch can never leave its waiters waiting for good.
struct Landing<'a> {
    cache: &'a ReadCache,
    keys: Vec<StateKey>,
}

impl Drop for Landing<'_> {
    fn drop(&mut self) {
        let mut in_flight = self.cache.in_flight.lock().unwrap();
        for key in &self.keys {
            if let Some(flight) = in_flight.remove(key) {
                *flight.landed.lock().unwrap() = true;
                flight.wake.notify_all();
            }
        }
    }
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
    /// A key another session is already fetching isn't asked for again:
    /// this waits for that fetch, and fetches the key itself only if that
    /// fetch failed.
    pub fn resolve(&self, keys: &[StateKey]) -> Result<(), String> {
        loop {
            let (mine, theirs) = self.claim(keys);
            if !mine.is_empty() {
                let landing = Landing { cache: &self.cache, keys: mine };
                match &self.batch {
                    Some(batch) => self.resolve_batched(batch, &landing.keys),
                    None => std::thread::scope(|scope| {
                        let reads: Vec<_> =
                            landing.keys.iter().map(|key| scope.spawn(move || self.read_blocking(key))).collect();
                        reads.into_iter().try_for_each(|r| r.join().map_err(|_| "resolver panicked".to_string())?)
                    }),
                }?;
            }
            if theirs.is_empty() {
                return Ok(());
            }
            theirs.iter().for_each(|flight| flight.wait());
            // Whatever those fetches failed to land is still missing, and no
            // longer in flight: the next round claims it.
        }
    }

    /// Split the uncached `keys` into those this call now owns the fetch
    /// of, and the flights of those someone else is already fetching.
    fn claim(&self, keys: &[StateKey]) -> (Vec<StateKey>, Vec<Arc<Flight>>) {
        let mut in_flight = self.cache.in_flight.lock().unwrap();
        let (mut mine, mut theirs) = (Vec::new(), Vec::new());
        for key in keys {
            // Checked under the lock: a fetch caches what it got before its
            // flight ends, so a key is always either cached or in flight.
            if self.is_cached(key) {
                continue;
            }
            match in_flight.get(key) {
                Some(flight) => theirs.push(Arc::clone(flight)),
                None => {
                    in_flight.insert(*key, Arc::default());
                    mine.push(*key);
                }
            }
        }
        (mine, theirs)
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

    /// `send_batch`, retried with backoff while the provider is rate
    /// limiting. Each retry asks only for what's still missing: a partly
    /// answered batch keeps what it got.
    fn resolve_batched(&self, batch: &Batcher, keys: &[StateKey]) -> Result<(), String> {
        let mut attempt = 0;
        loop {
            let missing: Vec<StateKey> = keys.iter().copied().filter(|k| !self.is_cached(k)).collect();
            match self.send_batch(batch, &missing) {
                Err(error) if is_rate_limited(&error) && attempt < RATE_LIMIT_RETRIES => {
                    std::thread::sleep(retry_delay(attempt));
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    fn send_batch(&self, batch: &Batcher, keys: &[StateKey]) -> Result<(), String> {
        if keys.is_empty() {
            return Ok(());
        }
        let block = batch.block;
        let mut request = batch.upstream.client.new_batch();
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
        // Only block hashes missing: no batch to send. alloy would send `[]`,
        // which some providers answer with an error.
        if accounts.is_empty() && slots.is_empty() {
            return hashes.into_iter().try_for_each(|n| self.read_blocking(&StateKey::BlockHash(n)));
        }
        batch.runtime.block_on(async {
            // Held until the reply is in: the cap is on requests in flight.
            let _permit = batch.upstream.in_flight.acquire().await.map_err(err)?;
            request.send().await.map_err(err)?;
            for (address, balance, nonce, code) in accounts {
                let (balance, nonce, code) = (balance.await.map_err(err)?, nonce.await.map_err(err)?, code.await.map_err(err)?);
                // The shape `SharedBackend` itself stores: code inline,
                // hashed, `KECCAK_EMPTY` for none.
                let (code_hash, code) = if code.is_empty() {
                    (KECCAK_EMPTY, None)
                } else {
                    // Checked: upstream bytes that don't decode are an
                    // error to report, not a panic on a resolver thread.
                    (keccak256(&code), Some(Bytecode::new_raw_checked(code).map_err(|e| format!("code of {address}: {e}"))?))
                };
                let nonce = u64::try_from(nonce).map_err(|_| format!("nonce of {address} does not fit a u64: {nonce}"))?;
                let info = AccountInfo { balance, nonce, code_hash, code, ..Default::default() };
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
        upstream: shared_upstream(rpc_url)?,
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

    /// `Counting`, but every read takes `delay` — long enough that
    /// concurrent resolves of the same key overlap.
    #[derive(Clone)]
    struct Slow {
        inner: Counting,
        delay: Duration,
    }

    impl DatabaseRef for Slow {
        type Error = Upstream;
        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            std::thread::sleep(self.delay);
            self.inner.basic_ref(address)
        }
        fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
            self.inner.code_by_hash_ref(code_hash)
        }
        fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
            std::thread::sleep(self.delay);
            self.inner.storage_ref(address, index)
        }
        fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
            self.inner.block_hash_ref(number)
        }
    }

    fn slow(fail_first: usize) -> (ReadThrough<Slow>, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let inner = Counting { hits: Arc::clone(&hits), fail_first };
        (ReadThrough::new(Slow { inner, delay: Duration::from_millis(100) }), hits)
    }

    /// Resolve `keys` from `n` sessions at once, all released together.
    fn resolve_concurrently(fork: &ReadThrough<Slow>, n: usize, keys: &[StateKey]) -> Vec<Result<(), String>> {
        let start = std::sync::Barrier::new(n);
        std::thread::scope(|s| {
            let resolves: Vec<_> = (0..n)
                .map(|_| {
                    let session = fork.clone();
                    let start = &start;
                    s.spawn(move || {
                        start.wait();
                        session.resolve(keys)
                    })
                })
                .collect();
            resolves.into_iter().map(|r| r.join().unwrap()).collect()
        })
    }

    #[test]
    fn sessions_missing_the_same_state_at_once_fetch_it_once() {
        let (fork, hits) = slow(0);
        let address = Address::with_last_byte(7);
        let keys = [StateKey::Account(address), StateKey::Storage(address, U256::from(1u64))];

        for result in resolve_concurrently(&fork, 10, &keys) {
            result.unwrap();
        }
        assert_eq!(hits.load(Ordering::Relaxed), 2, "ten cold sessions, one fetch per key");
        assert_eq!(fork.basic_ref(address).unwrap().unwrap().balance, U256::from(7u64));
    }

    #[test]
    fn a_session_waiting_on_a_failed_fetch_fetches_the_key_itself() {
        // The first fetch fails; whoever waited on it must not give up or
        // spin, but claim the key and fetch it once more.
        let (fork, hits) = slow(1);
        let keys = [StateKey::Account(Address::with_last_byte(8))];

        let results = resolve_concurrently(&fork, 4, &keys);
        assert_eq!(results.iter().filter(|r| r.is_err()).count(), 1, "only the failed fetch's owner sees its error");
        assert_eq!(hits.load(Ordering::Relaxed), 2, "one failed fetch, one retry, no more");
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

    #[test]
    fn rate_limit_errors_are_told_apart_from_real_failures() {
        assert!(is_rate_limited("server returned an error response: error code -32005: rate limit exceeded"));
        assert!(is_rate_limited("HTTP error 429 with body: Too Many Requests"));
        assert!(!is_rate_limited("error code -32000: header not found"));
        assert!(!is_rate_limited("connection refused"));
    }

    #[test]
    fn backoff_grows_is_capped_and_is_jittered_within_bounds() {
        for attempt in 0..10 {
            let base = (100u64 << attempt.min(5)).min(2_000);
            let delay = retry_delay(attempt).as_millis() as u64;
            assert!(delay >= base * 3 / 4 && delay <= base * 5 / 4, "attempt {attempt}: {delay} ms around {base}");
        }
        assert!(retry_delay(9) <= Duration::from_millis(2_500), "capped");
    }

    /// A one-endpoint JSON-RPC server that refuses the first `refuse`
    /// batches with `-32005`, then answers every account as empty.
    async fn rate_limited_upstream(refuse: usize) -> (String, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let batches = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&batches);
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    loop {
                        let mut chunk = [0u8; 8192];
                        let Ok(n) = socket.read(&mut chunk).await else { return };
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        // One request at a time: headers, then Content-Length bytes.
                        while let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                            let len: usize = head
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()))
                                .unwrap_or(0);
                            if buf.len() < end + 4 + len {
                                break;
                            }
                            let body: serde_json::Value = serde_json::from_slice(&buf[end + 4..end + 4 + len]).unwrap();
                            buf.drain(..end + 4 + len);
                            let reply = match &body {
                                serde_json::Value::Array(calls) => {
                                    let n = seen.fetch_add(1, Ordering::SeqCst);
                                    serde_json::Value::Array(calls.iter().map(|call| {
                                        let id = call["id"].clone();
                                        if n < refuse {
                                            serde_json::json!({"jsonrpc":"2.0","id":id,"error":{"code":-32005,"message":"rate limit exceeded"}})
                                        } else {
                                            let result = if call["method"] == "eth_getCode" { "0x" } else { "0x0" };
                                            serde_json::json!({"jsonrpc":"2.0","id":id,"result":result})
                                        }
                                    }).collect())
                                }
                                single => serde_json::json!({"jsonrpc":"2.0","id":single["id"].clone(),"result":"0x1"}),
                            };
                            let payload = reply.to_string();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{payload}",
                                payload.len()
                            );
                            if socket.write_all(response.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        (url, batches)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_rate_limited_batch_is_retried_until_it_goes_through() {
        let (url, batches) = rate_limited_upstream(2).await;
        let block_env = BlockEnv { number: U256::from(1u64), ..Default::default() };
        let fork = fork_with_block_env(&url, block_env).await.unwrap();
        let address = Address::with_last_byte(9);

        let resolver = fork.clone();
        let result = tokio::task::spawn_blocking(move || resolver.resolve(&[StateKey::Account(address)]))
            .await
            .unwrap();

        assert_eq!(result, Ok(()));
        assert_eq!(batches.load(Ordering::SeqCst), 3, "two refusals, then the one that went through");
        let (info, misses) = speculate(|| fork.basic_ref(address).unwrap());
        assert!(misses.is_empty(), "the account must be cached once the retry succeeds");
        assert_eq!(info.unwrap().balance, U256::ZERO);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_provider_that_never_relents_gets_its_error_back() {
        let (url, batches) = rate_limited_upstream(usize::MAX).await;
        let block_env = BlockEnv { number: U256::from(1u64), ..Default::default() };
        let fork = fork_with_block_env(&url, block_env).await.unwrap();

        let result = tokio::task::spawn_blocking(move || fork.resolve(&[StateKey::Account(Address::with_last_byte(8))]))
            .await
            .unwrap();

        let error = result.unwrap_err();
        assert!(is_rate_limited(&error), "{error}");
        assert_eq!(batches.load(Ordering::SeqCst), 1 + RATE_LIMIT_RETRIES as usize);
    }

    #[test]
    fn a_panic_mid_speculation_leaves_the_thread_reading_for_real() {
        let (fork, hits) = counting(0);
        let address = Address::with_last_byte(10);
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            speculate(|| {
                let _ = fork.basic_ref(address);
                panic!("a job blew up mid-pass");
            })
        }));
        assert!(panicked.is_err());

        // What a worker's next blocking pass does: this must really fetch.
        assert_eq!(fork.basic_ref(address).unwrap().unwrap().balance, U256::from(7u64));
        assert_eq!(hits.load(Ordering::Relaxed), 1, "the thread must not still be recording misses");
    }

    #[test]
    fn only_a_rate_limit_is_retried_not_a_number_that_happens_to_contain_429() {
        let pruned = "server returned an error response: error code -32000: missing trie node \
                      0x3a4429ff00000000000000000000000000000000000000000000000000004290";
        assert!(!is_rate_limited(pruned));
        assert!(!is_rate_limited("nonce of 0x4290000000000000000000000000000000000429 does not fit a u64"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_miss_of_only_block_hashes_sends_no_empty_batch() {
        let (url, batches) = rate_limited_upstream(0).await;
        let fork = fork_with_block_env(&url, BlockEnv { number: U256::from(1u64), ..Default::default() }).await.unwrap();

        // The hash goes to the backend's own lookup, which this fake
        // upstream can't answer; what matters is that no batch went out.
        let _ = tokio::task::spawn_blocking(move || fork.resolve(&[StateKey::BlockHash(1)])).await.unwrap();
        assert_eq!(batches.load(Ordering::SeqCst), 0, "an empty `[]` batch must never be sent");
    }
}
