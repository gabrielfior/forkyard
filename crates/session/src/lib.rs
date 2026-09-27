//! A registry of `forkyard-engine` sessions sharing one base snapshot and
//! one fetch fallback, sharded across a fixed pool of worker threads. This
//! is what the multi-agent RPC example didn't exercise: there, each agent
//! got its own independent fork with its own independent cache. Here, N
//! sessions fork off the *same* base and the *same* fetch fallback — the
//! actual cost advantage the whole design is for (see docs/RESEARCH.md,
//! "System design").
//!
//! Isolation is threads, not processes (see the "isolation boundary"
//! decision): sessions are hashed onto a fixed set of worker threads, each
//! owning its own sessions with no cross-thread mutex on the hot path.
//! `catch_unwind` around every job bounds a panic's blast radius to the
//! sessions on that one worker, not the whole registry. A background sweep
//! on each worker's own recv loop expires sessions past the TTL — "no
//! cleanup job an agent has to remember to call."

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use forkyard_engine::persist::{SnapshotInfo, SnapshotStore};
use forkyard_engine::{BaseSnapshot, Session, SessionState};
pub use forkyard_fetch::StateKey;
use revm::context::result::{EVMError, ExecutionResult, InvalidTransaction};
use revm::context::{BlockEnv, TxEnv};
use revm::database_interface::DatabaseRef;
use revm::primitives::{Address, Bytes, StorageKey, StorageValue, TxKind, KECCAK_EMPTY};
use revm::state::AccountInfo;
use revm::{Database, DatabaseCommit, ExecuteEvm, MainBuilder, MainContext};
use tokio::sync::{oneshot, OnceCell};

pub type SessionId = u64;

/// How many explicitly-pinned blocks stay warm at once
/// (`FORKYARD_MAX_PINNED_BLOCKS` overrides it). Small on purpose: each
/// costs a whole fetch backend — its own cache and fetch thread.
pub const DEFAULT_MAX_PINNED_BLOCKS: usize = 8;

/// What a `BlockForkFactory` hands back. Boxed because the factory is held
/// as a trait object; `String`-errored because the factory's own error type
/// (an `eyre::Report`, a test stub's) has no place in these signatures.
pub type BlockForkFuture<F> = Pin<Box<dyn Future<Output = Result<(F, BlockEnv), String>> + Send>>;

/// Builds the fallback (and its real `BlockEnv`) for one pinned block.
/// Injected because this crate knows nothing about RPC URLs, which is what
/// lets the tests below pin blocks against a stub with no network.
pub trait BlockForkFactory<F>: Send + Sync + 'static {
    fn fork_at(&self, block_number: u64) -> BlockForkFuture<F>;
}

impl<F, T> BlockForkFactory<F> for T
where
    T: Fn(u64) -> BlockForkFuture<F> + Send + Sync + 'static,
{
    fn fork_at(&self, block_number: u64) -> BlockForkFuture<F> {
        (self)(block_number)
    }
}

/// One pinned block's shared state: every session opened at that block
/// forks off this base and clones this fallback, so two agents at block X
/// don't each refetch X (what two `--fork-block-number X` Anvils do).
struct PinnedBlock<F> {
    base: Arc<BaseSnapshot>,
    fallback: F,
    block_env: BlockEnv,
}

/// Bounded block -> shared-state cache, evicting least-recently-*used*
/// first. `OnceCell` so two concurrent `fork_at_block(X)` calls collapse
/// into one factory invocation, and so a failure stores nothing and can be
/// retried rather than poisoning that block.
struct PinnedBlocks<F> {
    cells: HashMap<u64, Arc<OnceCell<PinnedBlock<F>>>>,
    /// Least-recently-used first. A `Vec` scan is enough: `cap` is a
    /// handful of blocks, touched only on session creation.
    recency: Vec<u64>,
    cap: usize,
}

impl<F> PinnedBlocks<F> {
    fn get_or_insert(&mut self, block_number: u64) -> Arc<OnceCell<PinnedBlock<F>>> {
        self.recency.retain(|n| *n != block_number);
        self.recency.push(block_number);
        let cell = Arc::clone(self.cells.entry(block_number).or_default());

        // Eviction drops only this map's handle: live sessions hold their
        // own base and fallback and keep working. What's lost is sharing —
        // the next session at that block pays the factory again.
        while self.recency.len() > self.cap {
            let evicted = self.recency.remove(0);
            self.cells.remove(&evicted);
        }
        cell
    }
}

/// Bound every fetch fallback in this crate needs to satisfy — the same
/// one `forkyard_engine::Session<F>` already requires, spelled out once
/// here since it has to be repeated at every generic site below. `Sync` is
/// needed on top of what `Session<F>` itself requires because `F` lives
/// inside `SessionManager`, which is shared (via `Arc`) across axum
/// handlers running on genuinely concurrent OS threads — not just
/// interleaved on one, the way `#[tokio::test]`'s default single-threaded
/// runtime would have let a missing bound here slip by unnoticed.
pub trait Fallback: DatabaseRef + Clone + Send + Sync + 'static {}
impl<F> Fallback for F
where
    F: DatabaseRef + Clone + Send + Sync + 'static,
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
}

/// Fetches what a speculative pass found missing into `F`'s cache,
/// blocking the calling thread — never a worker's: a worker hands it to a
/// thread of its own and serves other sessions meanwhile. The default reads
/// each key through `F` in parallel; `forkyard-bin` swaps in
/// `forkyard_fetch::Fork::resolve`, which sends them as one batch.
pub type Resolver<F> = Arc<dyn Fn(&F, &[StateKey]) -> Result<(), String> + Send + Sync>;

fn default_resolver<F: Fallback>() -> Resolver<F>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    Arc::new(|fallback: &F, keys: &[StateKey]| {
        std::thread::scope(|scope| {
            let reads: Vec<_> = keys.iter().map(|key| scope.spawn(move || read_key(fallback, key))).collect();
            reads.into_iter().try_for_each(|r| r.join().map_err(|_| "resolver panicked".to_string())?)
        })
    })
}

fn read_key<F: Fallback>(fallback: &F, key: &StateKey) -> Result<(), String>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let result = match *key {
        StateKey::Account(address) => fallback.basic_ref(address).map(drop),
        StateKey::Storage(address, index) => fallback.storage_ref(address, index).map(drop),
        StateKey::BlockHash(number) => fallback.block_hash_ref(number).map(drop),
    };
    result.map_err(|e| e.to_string())
}

/// Speculative passes one job may take before it's run blocking instead —
/// a bound on a pathological chain of dependent reads, not a normal path:
/// a pass that finds nothing new missing is clean by definition.
const MAX_SPECULATIVE_ROUNDS: u32 = 16;

#[derive(Debug)]
pub enum SessionError {
    Unknown(SessionId),
    Execution(String),
    /// revm rejected the transaction during validation, before any of its
    /// code ran — an underpriced `gas_price`, a nonce that doesn't line
    /// up, a sender who can't cover the fee. Kept as revm's own type
    /// rather than flattened into a string so a surface can build real
    /// advice from it: the fee variants carry the numbers involved, and
    /// only the caller knows what to name (`set_balance` on MCP,
    /// `forkyard_setBalance` over JSON-RPC). Boxed to keep `SessionError`
    /// small, since every fallible session call returns one.
    InvalidTransaction(Box<InvalidTransaction>),
    /// The worker thread this session was assigned to is gone — a bug
    /// (a worker's own job loop panicked past `catch_unwind`, or the
    /// manager was dropped), not a normal runtime condition.
    WorkerGone,
    /// No fallback could be built for the requested block — upstream can't
    /// serve it, or no block-fork factory was configured. A caller error to
    /// report, not a panic to take a worker down with.
    BlockUnavailable(u64, String),
    /// A snapshot couldn't be written or read back — no store configured,
    /// an unknown or malformed id, a file from another chain. Carries the
    /// store's own explanation, which already names the file.
    Snapshot(String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown(id) => write!(f, "unknown or expired session {id}"),
            Self::Execution(msg) => write!(f, "execution error: {msg}"),
            // revm's own wording, which reads far better than the `{:?}`
            // form this used to be stringified into.
            Self::InvalidTransaction(reason) => write!(f, "invalid transaction: {reason}"),
            Self::WorkerGone => write!(f, "worker thread is gone"),
            Self::BlockUnavailable(number, reason) => {
                write!(f, "cannot open a session at block {number}: {reason}")
            }
            Self::Snapshot(reason) => write!(f, "snapshot: {reason}"),
        }
    }
}
impl std::error::Error for SessionError {}

// The `DatabaseRef` bound is only here because `Branch`/`Adopt` carry a
// whole `Session<F>`, which declares it. Every `F` used here is a
// `Fallback`, which already implies it.
enum Job<F: DatabaseRef> {
    Fork {
        id: SessionId,
        base: Arc<BaseSnapshot>,
        fallback: F,
        block_env: BlockEnv,
        /// A snapshot's state to lay over the base — `resume`. `None` is
        /// an ordinary fresh fork.
        seed: Option<Box<SessionState>>,
        reply: oneshot::Sender<()>,
    },
    /// Everything `id` holds that its shared base doesn't, and the block
    /// it holds it at — what `snapshot` writes to disk.
    State {
        id: SessionId,
        reply: oneshot::Sender<Result<(Box<SessionState>, u64), SessionError>>,
    },
    /// Branch `parent` on the worker that owns it, handing the child back
    /// for `fork_from` to register. Two hops, because parent and child are
    /// almost never on the same shard.
    Branch {
        parent: SessionId,
        reply: oneshot::Sender<Result<Box<Session<F>>, SessionError>>,
    },
    /// Register an already-built session (a `Branch`'s child) under `id`,
    /// on the same insert-with-a-fresh-TTL-stamp path `Fork` takes.
    Adopt {
        id: SessionId,
        session: Box<Session<F>>,
        reply: oneshot::Sender<()>,
    },
    Simulate {
        id: SessionId,
        tx: Box<TxEnv>,
        disable_checks: bool,
        reply: oneshot::Sender<Result<ExecutionResult, SessionError>>,
    },
    Advance {
        id: SessionId,
        tx: Box<TxEnv>,
        reply: oneshot::Sender<Result<ExecutionResult, SessionError>>,
    },
    Discard {
        id: SessionId,
        reply: oneshot::Sender<()>,
    },
    /// Read one storage slot. The read-only counterpart to `SetStorage`,
    /// resolving overlay, then base, then fallback exactly the way
    /// execution does.
    Storage {
        id: SessionId,
        address: Address,
        key: StorageKey,
        reply: oneshot::Sender<Result<StorageValue, SessionError>>,
    },
    /// Read an account's deployed bytecode.
    Code {
        id: SessionId,
        address: Address,
        reply: oneshot::Sender<Result<Bytes, SessionError>>,
    },
    Basic {
        id: SessionId,
        address: Address,
        reply: oneshot::Sender<Result<Option<AccountInfo>, SessionError>>,
    },
    /// The block *this* session was forked at, which since `fork_at_block`
    /// exists is no longer necessarily the manager's own current block.
    BlockEnvOf {
        id: SessionId,
        reply: oneshot::Sender<Result<Box<BlockEnv>, SessionError>>,
    },
    SetAccount {
        id: SessionId,
        address: Address,
        info: AccountInfo,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    SetStorage {
        id: SessionId,
        address: Address,
        key: StorageKey,
        value: StorageValue,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    /// Posted by a resolver thread back to the worker that parked `id`'s
    /// job: its keys are in, retry. `failed` retries blocking, so an
    /// upstream error surfaces as the job's own error rather than looping.
    Unblock { id: SessionId, failed: bool },
}

impl<F: DatabaseRef> Job<F> {
    /// The session whose order this job must keep: jobs for one session
    /// run in arrival order even while one of them waits on upstream.
    fn session_id(&self) -> Option<SessionId> {
        match self {
            Job::Fork { id, .. }
            | Job::Adopt { id, .. }
            | Job::State { id, .. }
            | Job::Simulate { id, .. }
            | Job::Advance { id, .. }
            | Job::Discard { id, .. }
            | Job::Storage { id, .. }
            | Job::Code { id, .. }
            | Job::Basic { id, .. }
            | Job::BlockEnvOf { id, .. }
            | Job::SetAccount { id, .. }
            | Job::SetStorage { id, .. } => Some(*id),
            Job::Branch { parent, .. } => Some(*parent),
            Job::Unblock { .. } => None,
        }
    }
}

/// A registry of sessions sharing `fallback` and `base`, sharded across
/// `num_workers` OS threads. Cloning `F` per session is cheap by
/// construction — see `forkyard_fetch::Fork`'s own doc comment — so every
/// session gets its own handle to the same underlying fetch cache.
pub struct SessionManager<F: Fallback>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    fallback: Arc<RwLock<F>>,
    /// Swappable because `with_base` seeds it from a cache file and
    /// `refresh_fallback` must drop it when the chain moves. Sessions fork
    /// from the inner `Arc`, so a swap never disturbs one already holding
    /// the old base.
    base: RwLock<Arc<BaseSnapshot>>,
    block_env: Arc<RwLock<BlockEnv>>,
    workers: Vec<std_mpsc::Sender<Job<F>>>,
    counts: Vec<Arc<AtomicUsize>>,
    next_id: AtomicU64,
    /// `None` unless `with_block_forks` was called: a manager with no way
    /// to fetch a block says so via `BlockUnavailable`.
    block_forks: Option<Arc<dyn BlockForkFactory<F>>>,
    pinned: Mutex<PinnedBlocks<F>>,
    /// `None` unless `with_snapshots` was called; `snapshot` and `resume`
    /// then say so rather than guessing a directory.
    snapshots: Option<SnapshotStore>,
    /// Shared with every worker, so `with_resolver` can swap it after the
    /// workers are already running.
    resolver: Arc<RwLock<Resolver<F>>>,
}

impl<F: Fallback> SessionManager<F>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    /// `num_workers` sessions-sharding threads, each reaping its own
    /// sessions idle past `ttl`. `base` starts empty; advancing it as the
    /// chain moves is `forkyard-ingest`'s job, not this crate's — every
    /// session here still reads through to `fallback` on a miss. `block_env`
    /// is the real block (number, timestamp, base fee) every session forked
    /// from this manager is pinned to — e.g. what `forkyard_fetch::fork`
    /// returns alongside the fork itself.
    pub fn new(fallback: F, block_env: BlockEnv, num_workers: usize, ttl: Duration) -> Self {
        let num_workers = num_workers.max(1);
        let resolver = Arc::new(RwLock::new(default_resolver::<F>()));
        let mut workers = Vec::with_capacity(num_workers);
        let mut counts = Vec::with_capacity(num_workers);
        for idx in 0..num_workers {
            let count = Arc::new(AtomicUsize::new(0));
            workers.push(spawn_worker(idx, ttl, Arc::clone(&count), Arc::clone(&resolver)));
            counts.push(count);
        }
        Self {
            fallback: Arc::new(RwLock::new(fallback)),
            base: RwLock::new(Arc::new(BaseSnapshot::default())),
            block_env: Arc::new(RwLock::new(block_env)),
            workers,
            counts,
            next_id: AtomicU64::new(0),
            block_forks: None,
            pinned: Mutex::new(PinnedBlocks {
                cells: HashMap::new(),
                recency: Vec::new(),
                cap: DEFAULT_MAX_PINNED_BLOCKS,
            }),
            snapshots: None,
            resolver,
        }
    }

    /// Resolve speculative misses with `resolver` instead of one read per
    /// key through the fallback — e.g. batched into one upstream request.
    pub fn with_resolver(
        self,
        resolver: impl Fn(&F, &[StateKey]) -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        *self.resolver.write().unwrap() = Arc::new(resolver);
        self
    }

    /// Enable `snapshot` and `resume`, writing to and reading from `store`.
    pub fn with_snapshots(mut self, store: SnapshotStore) -> Self {
        self.snapshots = Some(store);
        self
    }

    /// Enable `fork_at_block`: `factory` builds the fallback for one block,
    /// and at most `max_pinned_blocks` stay warm at once. A builder rather
    /// than a `new` parameter, to leave the four-argument `new` alone.
    pub fn with_block_forks(mut self, factory: impl BlockForkFactory<F>, max_pinned_blocks: usize) -> Self {
        self.block_forks = Some(Arc::new(factory));
        self.pinned.get_mut().unwrap().cap = max_pinned_blocks.max(1);
        self
    }

    /// Start with `base` already warm instead of empty — how `forkyard-bin`
    /// replays a cache file from a previous run (`forkyard_engine::persist`)
    /// so a restart isn't cold.
    ///
    /// The caller must ensure the base describes `block_env`'s block:
    /// another block's accounts are wrong, not merely stale, which is why
    /// `persist` refuses a file whose recorded block doesn't match.
    pub fn with_base(self, base: BaseSnapshot) -> Self {
        *self.base.write().unwrap() = Arc::new(base);
        self
    }

    /// The shared base new sessions currently fork from — an `Arc` clone,
    /// O(1). Exposed so a caller can fold it back into what it writes to
    /// disk at shutdown.
    pub fn base(&self) -> Arc<BaseSnapshot> {
        Arc::clone(&self.base.read().unwrap())
    }

    /// A clone of the fallback new sessions currently read through — the
    /// *current* one, since `refresh_fallback` may have replaced the one
    /// the process started with.
    pub fn current_fallback(&self) -> F {
        self.fallback.read().unwrap().clone()
    }

    /// The real block new sessions from this manager are pinned to right
    /// now — an owned clone, not a reference, since `forkyard-ingest` can
    /// swap this out from another thread between calls (see
    /// `set_block_env`).
    pub fn block_env(&self) -> BlockEnv {
        self.block_env.read().unwrap().clone()
    }

    /// Swap the block context new sessions are forked against, on its own,
    /// with no change to the underlying fallback. Existing sessions are
    /// unaffected — each already has its own `BlockEnv` pinned at fork
    /// time. Prefer `refresh_fallback` when the fallback itself also needs
    /// to move to a new block — this alone leaves cached account/storage
    /// reads on the old fallback in place.
    pub fn set_block_env(&self, block_env: BlockEnv) {
        *self.block_env.write().unwrap() = block_env;
    }

    /// Swap in a completely fresh fallback (e.g. a new `forkyard_fetch::Fork`
    /// re-forked at the latest block) alongside the `BlockEnv` it was forked
    /// at. This is what actually keeps new sessions' account/storage reads
    /// from going stale: `set_block_env` alone only changes the
    /// number/timestamp/base fee new forks see, while old cached
    /// balances/nonces/storage/code on the previous fallback would
    /// otherwise still be served forever. Existing sessions are unaffected
    /// — each already holds its own clone of the *old* fallback from fork
    /// time, and keeps reading through that until it's discarded or its TTL
    /// expires (at which point the old fallback's background thread tears
    /// down once nothing references it anymore). Only sessions forked after
    /// this call see the new one.
    pub fn refresh_fallback(&self, fallback: F, block_env: BlockEnv) {
        *self.fallback.write().unwrap() = fallback;
        *self.block_env.write().unwrap() = block_env;
        // The base goes too: since `with_base`, it can hold real balances
        // read at the *old* block, and it is checked before the fallback —
        // so keeping it would make the refresh a no-op for exactly the
        // accounts anyone cared about.
        *self.base.write().unwrap() = Arc::new(BaseSnapshot::default());
    }

    /// A cloned `Sender`, not a borrowed one — `Sender::clone` is a cheap
    /// refcount bump, and cloning sidesteps ever needing `Sender: Sync`
    /// for concurrent callers (several axum handlers on several real OS
    /// threads, not just interleaved on one).
    fn worker_for(&self, id: SessionId) -> std_mpsc::Sender<Job<F>> {
        let idx = (id as usize) % self.workers.len();
        self.workers[idx].clone()
    }

    /// Fork a new session off the shared base and the shared fallback —
    /// the actual thing this crate exists for. O(1) modulo the channel
    /// hop: no state is copied, only an `Arc` and a cheap `F` clone.
    pub async fn fork(&self) -> Result<SessionId, SessionError> {
        self.fork_seeded(None).await
    }

    async fn fork_seeded(&self, seed: Option<Box<SessionState>>) -> Result<SessionId, SessionError> {
        let base = self.base();
        let fallback = self.fallback.read().unwrap().clone();
        let block_env = self.block_env();
        self.register(base, fallback, block_env, seed).await
    }

    async fn register(
        &self,
        base: Arc<BaseSnapshot>,
        fallback: F,
        block_env: BlockEnv,
        seed: Option<Box<SessionState>>,
    ) -> Result<SessionId, SessionError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::Fork { id, base, fallback, block_env, seed, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?;
        Ok(id)
    }

    /// Fork a session pinned to `block_number`, whatever block this manager
    /// defaults to. Sessions at the same block share one base and one
    /// fallback: the first pays the factory, later ones are as cheap as
    /// `fork`. `BlockUnavailable` if the block can't be forked or
    /// `with_block_forks` was never called.
    ///
    /// Not short-circuited to the default base when the numbers match: a
    /// pinned session must survive `refresh_fallback`, which moves the
    /// default base out from under it.
    pub async fn fork_at_block(&self, block_number: u64) -> Result<SessionId, SessionError> {
        self.fork_at_block_seeded(block_number, None).await
    }

    async fn fork_at_block_seeded(
        &self,
        block_number: u64,
        seed: Option<Box<SessionState>>,
    ) -> Result<SessionId, SessionError> {
        let (base, fallback, block_env) = {
            let cell = self.pinned.lock().unwrap().get_or_insert(block_number);
            let factory = Arc::clone(self.block_forks.as_ref().ok_or_else(|| {
                SessionError::BlockUnavailable(
                    block_number,
                    "this session manager was built without a block-fork factory".to_string(),
                )
            })?);
            let pinned = cell
                .get_or_try_init(|| async move {
                    let (fallback, block_env) = factory.fork_at(block_number).await?;
                    Ok::<_, String>(PinnedBlock { base: Arc::new(BaseSnapshot::default()), fallback, block_env })
                })
                .await
                .map_err(|reason| SessionError::BlockUnavailable(block_number, reason))?;
            (Arc::clone(&pinned.base), pinned.fallback.clone(), pinned.block_env.clone())
        };
        self.register(base, fallback, block_env, seed).await
    }

    /// `id`'s own state — what it wrote and what it read through, branch
    /// ancestry included — and the block it's at. Laid over any base at
    /// that block by `restore`, it reproduces the session.
    pub async fn session_state(&self, id: SessionId) -> Result<(u64, SessionState), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::State { id, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        let (state, block_number) = rx.await.map_err(|_| SessionError::WorkerGone)??;
        Ok((block_number, *state))
    }

    /// A new session at `block_number` holding `state` — the inverse of
    /// `session_state`. On the default base when that's the block this
    /// manager is on, otherwise through `fork_at_block`'s pinned blocks
    /// (so a snapshot outlives the chain tip moving on).
    pub async fn restore(&self, block_number: u64, state: SessionState) -> Result<SessionId, SessionError> {
        let seed = Some(Box::new(state));
        if u64::try_from(self.block_env().number).ok() == Some(block_number) {
            self.fork_seeded(seed).await
        } else {
            self.fork_at_block_seeded(block_number, seed).await
        }
    }

    /// Write `id`'s state to the snapshot store and return its id: a few
    /// KB for a typical session, since the shared base isn't in it. The
    /// session itself is untouched and stays live.
    pub async fn snapshot(&self, id: SessionId) -> Result<SnapshotInfo, SessionError> {
        let store = self.snapshot_store()?;
        let (block_number, state) = self.session_state(id).await?;
        tokio::task::spawn_blocking(move || store.store(block_number, &state))
            .await
            .map_err(|e| SessionError::Snapshot(e.to_string()))?
            .map_err(|e| SessionError::Snapshot(e.to_string()))
    }

    /// Open a new session from a snapshot id `snapshot` handed out — in
    /// this process or any other sharing the store's directory, before or
    /// after a restart. Resuming the same id twice gives two independent
    /// sessions, the way `fork_from` does.
    pub async fn resume(&self, snapshot_id: &str) -> Result<SessionId, SessionError> {
        let store = self.snapshot_store()?;
        let snapshot_id = snapshot_id.to_string();
        let (block_number, state) = tokio::task::spawn_blocking(move || store.load(&snapshot_id))
            .await
            .map_err(|e| SessionError::Snapshot(e.to_string()))?
            .map_err(|e| SessionError::Snapshot(e.to_string()))?;
        self.restore(block_number, state).await
    }

    fn snapshot_store(&self) -> Result<SnapshotStore, SessionError> {
        self.snapshots
            .clone()
            .ok_or_else(|| SessionError::Snapshot("this session manager was built without a snapshot store".to_string()))
    }

    /// The block one session is pinned to, not the manager's default. An
    /// RPC surface answering `eth_blockNumber` must ask this, or a session
    /// pinned at a historical block reports the tip.
    pub async fn session_block_env(&self, id: SessionId) -> Result<BlockEnv, SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::BlockEnvOf { id, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?.map(|env| *env)
    }

    /// How many explicitly-pinned blocks are kept warm. Never counts the
    /// manager's own default block.
    pub fn pinned_block_count(&self) -> usize {
        self.pinned.lock().unwrap().cells.len()
    }

    /// Branch a new session off `parent`'s *current* state — everything it
    /// has written or cached, not just the base `fork` starts from, and
    /// still without copying state (see `Session::branch`).
    ///
    /// The child is an ordinary session: its own id, its own shard (ids are
    /// hashed, so usually not the parent's), its own TTL clock. The parent
    /// stays live, and neither sees the other's later writes.
    /// `SessionError::Unknown` if `parent` is gone or expired.
    pub async fn fork_from(&self, parent: SessionId) -> Result<SessionId, SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(parent)
            .send(Job::Branch { parent, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        let session = rx.await.map_err(|_| SessionError::WorkerGone)??;

        // Allocated only after the branch succeeded, so a `fork_from`
        // against a dead parent doesn't burn an id.
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::Adopt { id, session, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?;
        Ok(id)
    }

    /// Read-only account lookup — overlay, then base, then fallback,
    /// exactly `Session::basic`'s own resolution order. Doesn't touch
    /// revm's execution machinery at all.
    pub async fn basic(&self, id: SessionId, address: Address) -> Result<Option<AccountInfo>, SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::Basic { id, address, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?
    }

    /// Override an account directly in `id`'s private overlay — the
    /// test-only cheatcode role, never touching the shared base or the
    /// real chain. See `Session::set_account`.
    pub async fn set_account(&self, id: SessionId, address: Address, info: AccountInfo) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::SetAccount { id, address, info, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?
    }

    /// Override a single storage slot directly in `id`'s private overlay —
    /// the test-only cheatcode role, never touching the shared base or the
    /// real chain. See `Session::set_storage`.
    pub async fn set_storage(
        &self,
        id: SessionId,
        address: Address,
        key: StorageKey,
        value: StorageValue,
    ) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::SetStorage { id, address, key, value, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?
    }

    /// Read one storage slot out of `id`'s view of the chain — overlay,
    /// then base, then the fetch fallback, the same resolution order
    /// `basic` follows. The read-only counterpart to `set_storage`, and
    /// what lets a caller inspect contract state (an ERC-20 `balanceOf`
    /// entry, say) without executing a transaction.
    pub async fn storage(
        &self,
        id: SessionId,
        address: Address,
        key: StorageKey,
    ) -> Result<StorageValue, SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::Storage { id, address, key, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?
    }

    /// Read an account's deployed bytecode in `id`'s view — empty for an
    /// EOA. Resolved by code hash through the same overlay/base/fallback
    /// chain execution uses, so it never reaches past the session.
    pub async fn code(&self, id: SessionId, address: Address) -> Result<Bytes, SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::Code { id, address, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?
    }

    /// Run `tx` read-only against `id`'s session — no commit, nothing
    /// persists — with balance and base-fee checks enforced, same as
    /// `advance`. Answers "would this really work right now." See
    /// docs/RESEARCH.md, "what simulate / advance actually do".
    pub async fn simulate(&self, id: SessionId, tx: TxEnv) -> Result<ExecutionResult, SessionError> {
        self.dispatch(id, tx, false, false).await
    }

    /// Run `tx` against `id`'s session and commit the diff into that
    /// session's private overlay only.
    pub async fn advance(&self, id: SessionId, tx: TxEnv) -> Result<ExecutionResult, SessionError> {
        self.dispatch(id, tx, true, false).await
    }

    /// Run `tx` read-only for its *return data* — `eth_call` semantics.
    /// Like `simulate` in that nothing is committed, but balance and
    /// base-fee checks are disabled: reading a contract shouldn't require
    /// the caller to hold gas money, and a client asking `balanceOf` has
    /// no signer at all. Use `simulate` instead to ask "would this
    /// transaction really work right now."
    pub async fn call(&self, id: SessionId, tx: TxEnv) -> Result<ExecutionResult, SessionError> {
        self.dispatch(id, tx, false, true).await
    }

    /// Dry-run `tx` for a gas estimate, the same way real Ethereum nodes'
    /// `eth_estimateGas` does: balance-sufficiency and base-fee checks are
    /// disabled, since the point is "how much gas would this need," not
    /// "does the caller currently hold funds for the price they'll
    /// actually send at." Never commits.
    pub async fn estimate_gas(&self, id: SessionId, tx: TxEnv) -> Result<ExecutionResult, SessionError> {
        self.dispatch(id, tx, false, true).await
    }

    async fn dispatch(
        &self,
        id: SessionId,
        tx: TxEnv,
        commit: bool,
        disable_checks: bool,
    ) -> Result<ExecutionResult, SessionError> {
        let (reply, rx) = oneshot::channel();
        let job = if commit {
            Job::Advance { id, tx: Box::new(tx), reply }
        } else {
            Job::Simulate { id, tx: Box::new(tx), disable_checks, reply }
        };
        self.worker_for(id).send(job).map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?
    }

    /// Explicitly discard a session ahead of its TTL. Not required —
    /// letting it expire does the same thing — but available for a caller
    /// that already knows it's done.
    pub async fn discard(&self, id: SessionId) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::Discard { id, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)
    }

    /// Total live sessions across every worker, for observability — not
    /// on any hot path.
    pub fn active_session_count(&self) -> usize {
        self.counts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }
}

fn spawn_worker<F: Fallback>(
    idx: usize,
    ttl: Duration,
    count: Arc<AtomicUsize>,
    resolver: Arc<RwLock<Resolver<F>>>,
) -> std_mpsc::Sender<Job<F>>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let (tx, rx) = std_mpsc::channel::<Job<F>>();
    let requeue = tx.clone();
    std::thread::Builder::new()
        .name(format!("forkyard-worker-{idx}"))
        .spawn(move || {
            let worker =
                Worker { sessions: HashMap::new(), blocked: HashMap::new(), requeue, resolver, count: Arc::clone(&count) };
            worker_loop(worker, rx, ttl, count)
        })
        .expect("failed to spawn forkyard worker thread");
    tx
}

/// One worker thread's state. A worker never waits on the network: a job
/// whose pass missed upstream state is parked, its keys fetched on another
/// thread, and it runs again on `Job::Unblock` — while every other session
/// on this worker carries on. The fixed pool used to stall whole shards
/// behind one ~200 ms upstream read.
struct Worker<F: Fallback>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    sessions: HashMap<SessionId, (Session<F>, Instant)>,
    /// Sessions whose front job is waiting on upstream, with every later
    /// job for that session queued behind it.
    blocked: HashMap<SessionId, Blocked<F>>,
    /// This worker's own inbox, for resolver threads to post `Unblock` to.
    requeue: std_mpsc::Sender<Job<F>>,
    resolver: Arc<RwLock<Resolver<F>>>,
    /// Live sessions on this worker, for `active_session_count` — updated
    /// before a fork or discard is answered, so a caller that just got its
    /// reply never reads the old count.
    count: Arc<AtomicUsize>,
}

struct Blocked<F: DatabaseRef> {
    queue: VecDeque<Job<F>>,
    /// Passes the front job has taken so far.
    rounds: u32,
}

enum Outcome<F: DatabaseRef> {
    Done,
    /// The job back, unanswered, with what its pass found missing.
    Deferred(Box<Job<F>>, Vec<StateKey>),
}

fn worker_loop<F: Fallback>(mut worker: Worker<F>, rx: std_mpsc::Receiver<Job<F>>, ttl: Duration, count: Arc<AtomicUsize>)
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(job) => {
                // A panic here (e.g. a revm bug on malformed input) is
                // caught at this boundary — revm has no unsafe/FFI in its
                // hot path, so this is sound — and only poisons this one
                // worker's sessions, not the other shards.
                if std::panic::catch_unwind(AssertUnwindSafe(|| worker.accept(job))).is_err() {
                    tracing::error!("forkyard worker job panicked; that session's state may be inconsistent");
                }
                count.store(worker.sessions.len(), Ordering::Relaxed);
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                let before = worker.sessions.len();
                let now = Instant::now();
                worker.sessions.retain(|_, (_, touched)| now.duration_since(*touched) < ttl);
                let removed = before - worker.sessions.len();
                if removed > 0 {
                    tracing::info!(removed, "reaped expired sessions");
                    count.store(worker.sessions.len(), Ordering::Relaxed);
                }
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

impl<F: Fallback> Worker<F>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    fn accept(&mut self, job: Job<F>) {
        if let Job::Unblock { id, failed } = job {
            return self.unblock(id, failed);
        }
        if let Some(blocked) = job.session_id().and_then(|id| self.blocked.get_mut(&id)) {
            blocked.queue.push_back(job);
            return;
        }
        if let Outcome::Deferred(job, keys) = self.handle(job, false) {
            self.park(*job, keys, Blocked { queue: VecDeque::new(), rounds: 0 });
        }
    }

    /// Put `job` at the front of its session's queue and resolve `keys` on
    /// a thread of its own.
    fn park(&mut self, job: Job<F>, keys: Vec<StateKey>, mut blocked: Blocked<F>) {
        let id = job.session_id().expect("only a session's job can miss its state");
        // It missed, so it ran, so the session exists.
        let fallback = self.sessions.get(&id).map(|(session, _)| session.fallback().clone());
        blocked.rounds += 1;
        let rounds = blocked.rounds;
        blocked.queue.push_front(job);
        self.blocked.insert(id, blocked);

        let resolver = Arc::clone(&self.resolver.read().unwrap());
        let requeue = self.requeue.clone();
        std::thread::spawn(move || {
            let start = Instant::now();
            let failed = match &fallback {
                Some(fallback) => resolver(fallback, &keys).is_err(),
                None => true,
            };
            tracing::debug!(session = id, keys = keys.len(), round = rounds, failed, elapsed_ms = start.elapsed().as_millis() as u64, "resolved a speculative pass's misses");
            let _ = requeue.send(Job::Unblock { id, failed });
        });
    }

    /// Run `id`'s queue from the front until it's empty or a job misses
    /// again.
    fn unblock(&mut self, id: SessionId, failed: bool) {
        let Some(mut blocked) = self.blocked.remove(&id) else { return };
        let mut blocking = failed || blocked.rounds >= MAX_SPECULATIVE_ROUNDS;
        while let Some(job) = blocked.queue.pop_front() {
            match self.handle(job, blocking) {
                Outcome::Done => {
                    blocking = false;
                    blocked.rounds = 0;
                }
                Outcome::Deferred(job, keys) => return self.park(*job, keys, blocked),
            }
        }
    }

    /// Run one job. With `blocking` false, a job that reads upstream state
    /// comes back `Deferred` instead of waiting for it.
    fn handle(&mut self, job: Job<F>, blocking: bool) -> Outcome<F> {
        let sessions = &mut self.sessions;
        match job {
            Job::Simulate { id, tx, disable_checks, reply } => {
                let Some((session, touched)) = sessions.get_mut(&id) else {
                    let _ = reply.send(Err(SessionError::Unknown(id)));
                    return Outcome::Done;
                };
                *touched = Instant::now();
                match execute(session, &tx, false, disable_checks, blocking) {
                    Ok(result) => {
                        let _ = reply.send(result);
                    }
                    Err(keys) => return Outcome::Deferred(Box::new(Job::Simulate { id, tx, disable_checks, reply }), keys),
                }
            }
            Job::Advance { id, tx, reply } => {
                let Some((session, touched)) = sessions.get_mut(&id) else {
                    let _ = reply.send(Err(SessionError::Unknown(id)));
                    return Outcome::Done;
                };
                *touched = Instant::now();
                match execute(session, &tx, true, false, blocking) {
                    Ok(result) => {
                        let _ = reply.send(result);
                    }
                    Err(keys) => return Outcome::Deferred(Box::new(Job::Advance { id, tx, reply }), keys),
                }
            }
            Job::Storage { id, address, key, reply } => {
                let Some((session, touched)) = sessions.get_mut(&id) else {
                    let _ = reply.send(Err(SessionError::Unknown(id)));
                    return Outcome::Done;
                };
                *touched = Instant::now();
                let read = attempt(session, blocking, |session| {
                    Database::storage(session, address, key).map_err(|e| SessionError::Execution(format!("{e}")))
                });
                match read {
                    Ok(result) => {
                        let _ = reply.send(result);
                    }
                    Err(keys) => return Outcome::Deferred(Box::new(Job::Storage { id, address, key, reply }), keys),
                }
            }
            Job::Code { id, address, reply } => {
                let Some((session, touched)) = sessions.get_mut(&id) else {
                    let _ = reply.send(Err(SessionError::Unknown(id)));
                    return Outcome::Done;
                };
                *touched = Instant::now();
                match attempt(session, blocking, |session| code_of(session, address)) {
                    Ok(result) => {
                        let _ = reply.send(result);
                    }
                    Err(keys) => return Outcome::Deferred(Box::new(Job::Code { id, address, reply }), keys),
                }
            }
            Job::Basic { id, address, reply } => {
                let Some((session, touched)) = sessions.get_mut(&id) else {
                    let _ = reply.send(Err(SessionError::Unknown(id)));
                    return Outcome::Done;
                };
                *touched = Instant::now();
                let read = attempt(session, blocking, |session| {
                    Database::basic(session, address).map_err(|e| SessionError::Execution(format!("{e}")))
                });
                match read {
                    Ok(result) => {
                        let _ = reply.send(result);
                    }
                    Err(keys) => return Outcome::Deferred(Box::new(Job::Basic { id, address, reply }), keys),
                }
            }
            other => handle_job(sessions, &self.count, other),
        }
        Outcome::Done
    }
}

/// Run `f` against `session` speculatively — every upstream miss recorded,
/// none waited for (`forkyard_fetch::speculate`) — and keep what it cached
/// only if nothing was missing. `Err` carries the missing keys; `f`'s
/// result is dropped, since it was computed from placeholder values.
/// `blocking` runs `f` the old way, waiting on each read.
fn attempt<F: Fallback, R>(
    session: &mut Session<F>,
    blocking: bool,
    f: impl FnOnce(&mut Session<F>) -> R,
) -> Result<R, Vec<StateKey>> {
    if blocking {
        return Ok(f(session));
    }
    session.begin_speculation();
    let (result, misses) = forkyard_fetch::speculate(|| f(&mut *session));
    session.end_speculation(misses.is_empty());
    if misses.is_empty() {
        Ok(result)
    } else {
        Err(misses)
    }
}

/// Run `tx`; with `commit`, write its diff into the overlay — but only
/// after a clean pass, never one computed from placeholders.
fn execute<F: Fallback>(
    session: &mut Session<F>,
    tx: &TxEnv,
    commit: bool,
    disable_checks: bool,
    blocking: bool,
) -> Result<Result<ExecutionResult, SessionError>, Vec<StateKey>>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let outcome = attempt(session, blocking, |session| {
        // Sender and recipient are known before anything runs. Touch them
        // first, so a pass that stops early — an unfunded sender fails
        // validation before the recipient is ever read — still reports
        // both, and one round trip fetches the pair.
        let _ = Database::basic(session, tx.caller);
        if let TxKind::Call(to) = tx.kind {
            let _ = Database::basic(session, to);
        }
        transact(session, tx.clone(), disable_checks)
    })?;
    Ok(outcome.map(|(result, state)| {
        if commit {
            DatabaseCommit::commit(session, state);
        }
        result
    }))
}

/// Resolve an account's bytecode the way the EVM does: the account's own
/// inlined code if the fallback supplied it, otherwise a lookup by code
/// hash. `KECCAK_EMPTY` short-circuits, because asking the fallback for
/// the empty-code hash is how an EOA read turns into a spurious
/// `CodeMiss` error.
fn code_of<F: Fallback>(session: &mut Session<F>, address: Address) -> Result<Bytes, SessionError> {
    let info = Database::basic(session, address)
        .map_err(|e| SessionError::Execution(format!("{e}")))?
        .unwrap_or_default();
    if let Some(code) = info.code {
        return Ok(code.original_bytes());
    }
    if info.code_hash == KECCAK_EMPTY {
        return Ok(Bytes::new());
    }
    Database::code_by_hash(session, info.code_hash)
        .map(|code| code.original_bytes())
        .map_err(|e| SessionError::Execution(format!("{e}")))
}

/// Every job that never reads upstream state, so never defers.
fn handle_job<F: Fallback>(sessions: &mut HashMap<SessionId, (Session<F>, Instant)>, count: &AtomicUsize, job: Job<F>)
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    match job {
        Job::Fork { id, base, fallback, block_env, seed, reply } => {
            let session = match seed {
                Some(state) => Session::restore(base, fallback, block_env, *state),
                None => Session::fork(base, fallback, block_env),
            };
            sessions.insert(id, (session, Instant::now()));
            count.store(sessions.len(), Ordering::Relaxed);
            let _ = reply.send(());
        }
        Job::State { id, reply } => {
            let result = match sessions.get_mut(&id) {
                Some((session, touched)) => {
                    // Snapshotting counts as activity, like branching.
                    *touched = Instant::now();
                    u64::try_from(session.block_env().number)
                        .map(|number| (Box::new(session.state()), number))
                        .map_err(|_| SessionError::Execution("block number does not fit a u64".to_string()))
                }
                None => Err(SessionError::Unknown(id)),
            };
            let _ = reply.send(result);
        }
        Job::Branch { parent, reply } => {
            let result = match sessions.get_mut(&parent) {
                Some((session, touched)) => {
                    // Branching counts as activity: a root that is only
                    // ever branched from must not be reaped mid-run.
                    *touched = Instant::now();
                    Ok(Box::new(session.branch()))
                }
                None => Err(SessionError::Unknown(parent)),
            };
            let _ = reply.send(result);
        }
        Job::Adopt { id, session, reply } => {
            sessions.insert(id, (*session, Instant::now()));
            count.store(sessions.len(), Ordering::Relaxed);
            let _ = reply.send(());
        }
        Job::Discard { id, reply } => {
            sessions.remove(&id);
            count.store(sessions.len(), Ordering::Relaxed);
            let _ = reply.send(());
        }
        Job::BlockEnvOf { id, reply } => {
            let result = match sessions.get_mut(&id) {
                // Counts as activity: a client polling `eth_blockNumber` on
                // a session is using it.
                Some((session, touched)) => {
                    *touched = Instant::now();
                    Ok(Box::new(session.block_env().clone()))
                }
                None => Err(SessionError::Unknown(id)),
            };
            let _ = reply.send(result);
        }
        Job::SetAccount { id, address, info, reply } => {
            let result = match sessions.get_mut(&id) {
                Some((session, touched)) => {
                    *touched = Instant::now();
                    session.set_account(address, info);
                    Ok(())
                }
                None => Err(SessionError::Unknown(id)),
            };
            let _ = reply.send(result);
        }
        Job::SetStorage { id, address, key, value, reply } => {
            let result = match sessions.get_mut(&id) {
                Some((session, touched)) => {
                    *touched = Instant::now();
                    session.set_storage(address, key, value);
                    Ok(())
                }
                None => Err(SessionError::Unknown(id)),
            };
            let _ = reply.send(result);
        }
        Job::Simulate { .. }
        | Job::Advance { .. }
        | Job::Storage { .. }
        | Job::Code { .. }
        | Job::Basic { .. }
        | Job::Unblock { .. } => unreachable!("routed through Worker::handle and Worker::accept"),
    }
}

/// One EVM pass over `tx`, uncommitted: the result and the diff it would
/// write.
fn transact<F: Fallback>(
    session: &mut Session<F>,
    tx: TxEnv,
    disable_checks: bool,
) -> Result<(ExecutionResult, revm::state::EvmState), SessionError>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let block_env = session.block_env().clone();
    let ctx = revm::Context::mainnet().with_db(session).with_block(block_env);
    // Real nodes' eth_estimateGas disables balance/base-fee checks too —
    // the point of an estimate is "how much gas," not "does the caller
    // hold funds for the price they'll actually send at." simulate/advance
    // never take this path — they answer "would this really work."
    let ctx = if disable_checks {
        ctx.modify_cfg_chained(|cfg| {
            cfg.disable_balance_check = true;
            cfg.disable_base_fee = true;
        })
    } else {
        ctx
    };
    let mut evm = ctx.build_mainnet();
    let out = evm.transact(tx).map_err(execution_error)?;
    Ok((out.result, out.state))
}

/// Keeps a validation rejection typed and stringifies everything else.
/// Database and header failures have no caller-actionable shape, so their
/// text is all there is to report; `EVMError::Transaction` is the one that
/// does, and it is exactly what the fee messages are built from.
fn execution_error<DB: fmt::Debug>(error: EVMError<DB>) -> SessionError {
    match error {
        EVMError::Transaction(reason) => SessionError::InvalidTransaction(Box::new(reason)),
        other => SessionError::Execution(format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::primitives::{Address, TxKind, B256, U256};
    use revm::state::{AccountInfo, Bytecode};

    /// A fallback with no network at all: one fixed address is "funded"
    /// with exactly enough for one transfer, everything else has zero
    /// balance/nonce and empty code. Enough to unit-test the registry
    /// (sharing, isolation, TTL) without live state.
    const FUNDED: Address = Address::new([9u8; 20]);
    const FUNDED_BALANCE: u64 = 100;

    #[derive(Clone)]
    struct FundedFallback;

    #[derive(Debug)]
    struct FundedFallbackError;
    impl fmt::Display for FundedFallbackError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "funded fallback has no real data")
        }
    }
    impl std::error::Error for FundedFallbackError {}
    impl revm::database_interface::DBErrorMarker for FundedFallbackError {}

    impl DatabaseRef for FundedFallback {
        type Error = FundedFallbackError;
        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            let balance = if address == FUNDED { U256::from(FUNDED_BALANCE) } else { U256::ZERO };
            Ok(Some(AccountInfo { balance, ..Default::default() }))
        }
        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }
        fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
            Ok(U256::ZERO)
        }
        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    fn manager() -> SessionManager<FundedFallback> {
        SessionManager::new(FundedFallback, BlockEnv::default(), 2, Duration::from_millis(200))
    }

    /// A fallback whose reported balance for `WATCHED` is whatever it was
    /// constructed with — stands in for "the chain moved, the same address
    /// now has a different real balance," so `refresh_fallback` has
    /// something observable to swap between.
    #[derive(Clone)]
    struct ValueFallback(u64);

    impl DatabaseRef for ValueFallback {
        type Error = FundedFallbackError;
        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            let balance = if address == WATCHED { U256::from(self.0) } else { U256::ZERO };
            Ok(Some(AccountInfo { balance, ..Default::default() }))
        }
        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }
        fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
            Ok(U256::ZERO)
        }
        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    const WATCHED: Address = Address::new([7u8; 20]);

    /// A block this stub factory refuses to serve — stands in for a block
    /// the upstream RPC can't produce (not yet mined, pruned, unreachable).
    const UNMINED_BLOCK: u64 = 999_999_999;

    /// Hands out `ValueFallback(block_number)` — "at block N, WATCHED's
    /// balance is N" — and counts calls. The count is what makes sharing
    /// provable: two sessions at one block must cost one fetch.
    #[derive(Clone, Default)]
    struct CountingBlockForks {
        calls: Arc<AtomicUsize>,
    }

    impl BlockForkFactory<ValueFallback> for CountingBlockForks {
        fn fork_at(&self, block_number: u64) -> BlockForkFuture<ValueFallback> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async move {
                if block_number == UNMINED_BLOCK {
                    return Err("upstream RPC returned no block for the requested id".to_string());
                }
                Ok((
                    ValueFallback(block_number),
                    BlockEnv { number: U256::from(block_number), ..Default::default() },
                ))
            })
        }
    }

    /// Default block `ValueFallback(1)`, block pinning via the counting
    /// stub, returned with its call counter.
    fn pinning_manager(max_pinned_blocks: usize) -> (SessionManager<ValueFallback>, Arc<AtomicUsize>) {
        let forks = CountingBlockForks::default();
        let calls = Arc::clone(&forks.calls);
        let mgr = SessionManager::new(ValueFallback(1), BlockEnv::default(), 2, Duration::from_secs(60))
            .with_block_forks(forks, max_pinned_blocks);
        (mgr, calls)
    }

    async fn watched_balance(mgr: &SessionManager<ValueFallback>, id: SessionId) -> U256 {
        mgr.basic(id, WATCHED).await.unwrap().unwrap().balance
    }

    #[tokio::test]
    async fn sessions_pinned_to_different_blocks_see_different_state_and_stay_isolated() {
        let (mgr, calls) = pinning_manager(DEFAULT_MAX_PINNED_BLOCKS);
        let at_100 = mgr.fork_at_block(100).await.unwrap();
        let at_200 = mgr.fork_at_block(200).await.unwrap();

        // One process, two blocks — the thing that needed two Anvil
        // processes before.
        assert_eq!(watched_balance(&mgr, at_100).await, U256::from(100));
        assert_eq!(watched_balance(&mgr, at_200).await, U256::from(200));
        assert_eq!(mgr.session_block_env(at_100).await.unwrap().number, U256::from(100));
        assert_eq!(mgr.session_block_env(at_200).await.unwrap().number, U256::from(200));
        assert_eq!(calls.load(Ordering::Relaxed), 2, "two distinct blocks, two fetches");

        // Writes on one block's session are invisible to the other's, the
        // same as any two sessions on one block.
        mgr.set_account(at_100, WATCHED, AccountInfo { balance: U256::from(7), ..Default::default() })
            .await
            .unwrap();
        assert_eq!(watched_balance(&mgr, at_100).await, U256::from(7));
        assert_eq!(watched_balance(&mgr, at_200).await, U256::from(200));

        // And neither disturbs the manager's own default block.
        let at_default = mgr.fork().await.unwrap();
        assert_eq!(watched_balance(&mgr, at_default).await, U256::from(1));
    }

    #[tokio::test]
    async fn sessions_at_the_same_block_share_one_fetched_base_and_fallback() {
        let (mgr, calls) = pinning_manager(DEFAULT_MAX_PINNED_BLOCKS);

        // Eight agents at the same historical block — the reproduce-an-
        // incident case. Eight Anvil processes would fetch that block's
        // state eight times.
        let mut ids = Vec::new();
        for _ in 0..8 {
            ids.push(mgr.fork_at_block(4_242).await.unwrap());
        }
        assert_eq!(
            calls.load(Ordering::Relaxed),
            1,
            "every session at one block must come off the same fetched base and fallback"
        );
        assert_eq!(mgr.pinned_block_count(), 1);

        for id in &ids {
            assert_eq!(watched_balance(&mgr, *id).await, U256::from(4_242));
        }

        // Sharing the fetch is not sharing the state: one agent's write
        // stays in its own overlay.
        mgr.set_account(ids[0], WATCHED, AccountInfo { balance: U256::ZERO, ..Default::default() })
            .await
            .unwrap();
        assert_eq!(watched_balance(&mgr, ids[1]).await, U256::from(4_242));
    }

    #[tokio::test]
    async fn concurrent_forks_at_one_block_still_only_fetch_it_once() {
        let (mgr, calls) = pinning_manager(DEFAULT_MAX_PINNED_BLOCKS);
        let mgr = Arc::new(mgr);

        // Sequential callers would collapse onto the cache entry trivially;
        // the guarantee has to survive N agents opening sessions at the
        // same block at once, which is the actual arrival pattern.
        let mut handles = Vec::new();
        for _ in 0..8 {
            let mgr = Arc::clone(&mgr);
            handles.push(tokio::spawn(async move { mgr.fork_at_block(77).await.unwrap() }));
        }
        for handle in handles {
            let id = handle.await.unwrap();
            assert_eq!(watched_balance(&mgr, id).await, U256::from(77));
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1, "a race to fork one block must not fetch it twice");
    }

    #[tokio::test]
    async fn an_unreachable_block_errors_instead_of_panicking() {
        let (mgr, calls) = pinning_manager(DEFAULT_MAX_PINNED_BLOCKS);

        let err = mgr.fork_at_block(UNMINED_BLOCK).await.unwrap_err();
        assert!(matches!(err, SessionError::BlockUnavailable(UNMINED_BLOCK, _)));
        assert_eq!(mgr.active_session_count(), 0, "a failed fork_at_block must not register a session");

        // The failure is not cached: a block that was unreachable a moment
        // ago (RPC hiccup, block not yet mined) can be asked for again.
        assert!(mgr.fork_at_block(UNMINED_BLOCK).await.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 2, "a failed block must stay retryable, not poison its slot");

        // The manager itself is unharmed.
        assert!(mgr.fork().await.is_ok());
    }

    #[tokio::test]
    async fn fork_at_block_without_a_factory_errors_rather_than_pretending() {
        let mgr = SessionManager::new(ValueFallback(1), BlockEnv::default(), 2, Duration::from_secs(60));
        let err = mgr.fork_at_block(100).await.unwrap_err();
        assert!(matches!(err, SessionError::BlockUnavailable(100, _)));
    }

    #[tokio::test]
    async fn a_tip_refresh_never_moves_an_explicitly_pinned_session() {
        let (mgr, _) = pinning_manager(DEFAULT_MAX_PINNED_BLOCKS);
        let pinned = mgr.fork_at_block(100).await.unwrap();

        // What `forkyard-ingest` does every time the chain produces a
        // block. It must move the *default* base only — a session
        // reproducing an incident at block 100 that silently jumps to the
        // tip is the exact failure this feature exists to prevent.
        mgr.refresh_fallback(ValueFallback(999), BlockEnv { number: U256::from(999), ..Default::default() });

        assert_eq!(watched_balance(&mgr, pinned).await, U256::from(100));
        assert_eq!(mgr.session_block_env(pinned).await.unwrap().number, U256::from(100));
        let after_refresh = mgr.fork_at_block(100).await.unwrap();
        assert_eq!(watched_balance(&mgr, after_refresh).await, U256::from(100), "the pinned block stays pinned for new sessions too");

        // The default fork does follow the tip, as before.
        let following = mgr.fork().await.unwrap();
        assert_eq!(watched_balance(&mgr, following).await, U256::from(999));
    }

    #[tokio::test]
    async fn evicting_a_pinned_block_leaves_its_live_sessions_working() {
        let (mgr, calls) = pinning_manager(1); // room for exactly one block
        let at_100 = mgr.fork_at_block(100).await.unwrap();

        // Block 200 pushes 100 out of the cache.
        let at_200 = mgr.fork_at_block(200).await.unwrap();
        assert_eq!(mgr.pinned_block_count(), 1, "the cache must stay at its cap");

        // Eviction drops the manager's handle on block 100's base and
        // fallback, not the session's: `at_100` holds its own `Arc` and its
        // own fallback clone, so it reads and executes exactly as before.
        assert_eq!(watched_balance(&mgr, at_100).await, U256::from(100));
        assert_eq!(mgr.session_block_env(at_100).await.unwrap().number, U256::from(100));
        mgr.set_account(at_100, WATCHED, AccountInfo { balance: U256::from(5), ..Default::default() })
            .await
            .unwrap();
        assert_eq!(watched_balance(&mgr, at_100).await, U256::from(5));
        assert_eq!(watched_balance(&mgr, at_200).await, U256::from(200));

        // What eviction actually costs: block 100's next session refetches
        // instead of sharing.
        let at_100_again = mgr.fork_at_block(100).await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 3, "an evicted block is refetched, not resurrected");
        assert_eq!(watched_balance(&mgr, at_100_again).await, U256::from(100));
        assert_eq!(
            watched_balance(&mgr, at_100).await,
            U256::from(5),
            "the refetch must not reach back into the session that was live across the eviction"
        );
    }

    #[tokio::test]
    async fn re_using_a_pinned_block_keeps_it_from_being_evicted() {
        let (mgr, calls) = pinning_manager(2);
        mgr.fork_at_block(100).await.unwrap();
        mgr.fork_at_block(200).await.unwrap();

        // Eviction is least-recently-*used*, not least-recently-inserted:
        // touching 100 again makes 200 the eviction candidate, so the block
        // an agent fleet is actively working at stays warm.
        mgr.fork_at_block(100).await.unwrap();
        mgr.fork_at_block(300).await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 3, "the re-used block must still have been cached");

        mgr.fork_at_block(100).await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 3, "100 was used most recently and must have survived");
        mgr.fork_at_block(200).await.unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 4, "200 was the least recently used and must have been evicted");
    }

    #[tokio::test]
    async fn a_branch_of_a_pinned_session_stays_on_that_session_block() {
        let (mgr, calls) = pinning_manager(DEFAULT_MAX_PINNED_BLOCKS);
        let parent = mgr.fork_at_block(100).await.unwrap();
        let child = mgr.fork_from(parent).await.unwrap();

        // `fork_from` carries the parent's whole context, block included —
        // "K what-ifs from where I got to" must not silently move the child
        // to a different chain height.
        assert_eq!(mgr.session_block_env(child).await.unwrap().number, U256::from(100));
        assert_eq!(watched_balance(&mgr, child).await, U256::from(100));
        assert_eq!(calls.load(Ordering::Relaxed), 1, "branching must not refetch the parent's block");
    }

    fn spend_funded_balance(recipient: Address) -> TxEnv {
        TxEnv::builder()
            .caller(FUNDED)
            .kind(TxKind::Call(recipient))
            .value(U256::from(FUNDED_BALANCE)) // exactly the whole balance
            .gas_limit(21_000)
            .gas_price(0) // isolate the balance question from gas accounting
            .nonce(0)
            .build_fill()
    }

    #[tokio::test]
    async fn sessions_are_independent_even_when_sharing_the_same_base_and_fallback() {
        let mgr = manager();
        let a = mgr.fork().await.unwrap();
        let b = mgr.fork().await.unwrap();
        assert_eq!(mgr.active_session_count(), 2);

        let recipient = Address::from([2u8; 20]);
        let tx = spend_funded_balance(recipient);

        // Session `a` spends FUNDED's entire balance on nonce 0.
        let result_a = mgr.advance(a, tx.clone()).await.unwrap();
        assert!(result_a.is_success(), "a's spend of a fully-funded balance must succeed");

        // If `a`'s write leaked into the shared base, `b` replaying the
        // exact same nonce-0, whole-balance transfer would now fail
        // (balance already spent, or nonce already used). It doesn't —
        // `b` sees its own fresh overlay over the same shared base.
        let result_b = mgr.advance(b, tx).await.unwrap();
        assert!(
            result_b.is_success(),
            "b must not observe a's overlay write — it shares the base and fallback, not a's session"
        );
    }

    /// Balance is the only session state `SessionManager` can read back
    /// directly (`basic`), so the branching tests express "did this state
    /// come along / stay isolated" as balances on distinct addresses.
    async fn fund(mgr: &SessionManager<FundedFallback>, id: SessionId, address: Address, balance: u64) {
        mgr.set_account(id, address, AccountInfo { balance: U256::from(balance), ..Default::default() })
            .await
            .unwrap();
    }

    async fn balance_of(mgr: &SessionManager<FundedFallback>, id: SessionId, address: Address) -> U256 {
        mgr.basic(id, address).await.unwrap().unwrap().balance
    }

    #[tokio::test]
    async fn fork_from_starts_the_child_where_the_parent_had_got_to() {
        let mgr = manager();
        let parent = mgr.fork().await.unwrap();
        let touched = Address::from([0x31; 20]);
        fund(&mgr, parent, touched, 500).await;

        let child = mgr.fork_from(parent).await.unwrap();

        assert_eq!(balance_of(&mgr, child, touched).await, U256::from(500));
        // The child is a session like any other, registered on its own
        // shard — not a view onto the parent.
        assert_ne!(child, parent);
        assert_eq!(mgr.active_session_count(), 2);

        // A session forked from the shared base instead of the parent
        // sees none of it — this is what fork_from adds over fork.
        let sibling = mgr.fork().await.unwrap();
        assert_eq!(balance_of(&mgr, sibling, touched).await, U256::ZERO);
    }

    #[tokio::test]
    async fn a_child_and_its_parent_never_see_each_others_later_writes() {
        let mgr = manager();
        let parent = mgr.fork().await.unwrap();
        let shared = Address::from([0x41; 20]);
        fund(&mgr, parent, shared, 1).await;

        let child = mgr.fork_from(parent).await.unwrap();
        fund(&mgr, child, shared, 999).await;
        fund(&mgr, parent, shared, 2).await;

        assert_eq!(balance_of(&mgr, parent, shared).await, U256::from(2), "the child's write must not reach the parent");
        assert_eq!(balance_of(&mgr, child, shared).await, U256::from(999), "the parent's later write must not reach the child");

        // Same thing through real execution, not just the cheatcode: the
        // whole-balance nonce-0 spend below can only succeed once per
        // session, so both sides succeeding proves neither replayed into
        // the other's state.
        let recipient = Address::from([2u8; 20]);
        assert!(mgr.advance(parent, spend_funded_balance(recipient)).await.unwrap().is_success());
        assert!(mgr.advance(child, spend_funded_balance(recipient)).await.unwrap().is_success());
    }

    #[tokio::test]
    async fn branching_a_branch_keeps_the_whole_chain_of_state() {
        let mgr = manager();
        let root = mgr.fork().await.unwrap();
        let from_root = Address::from([0x51; 20]);
        fund(&mgr, root, from_root, 10).await;

        let child = mgr.fork_from(root).await.unwrap();
        let from_child = Address::from([0x52; 20]);
        fund(&mgr, child, from_child, 20).await;

        let grandchild = mgr.fork_from(child).await.unwrap();
        let from_grandchild = Address::from([0x53; 20]);
        fund(&mgr, grandchild, from_grandchild, 30).await;

        assert_eq!(balance_of(&mgr, grandchild, from_root).await, U256::from(10));
        assert_eq!(balance_of(&mgr, grandchild, from_child).await, U256::from(20));
        assert_eq!(balance_of(&mgr, grandchild, from_grandchild).await, U256::from(30));

        // Depth doesn't leak upward either.
        assert_eq!(balance_of(&mgr, child, from_grandchild).await, U256::ZERO);
        assert_eq!(balance_of(&mgr, root, from_child).await, U256::ZERO);
    }

    #[tokio::test]
    async fn many_children_of_one_parent_are_mutually_isolated() {
        let mgr = manager();
        let parent = mgr.fork().await.unwrap();
        let shared = Address::from([0x61; 20]);
        fund(&mgr, parent, shared, 7).await;

        // Eight what-ifs off the same state — the case the whole feature
        // exists for, and more than the manager's two shards, so children
        // land on both.
        let mut children = Vec::new();
        for i in 0..8u64 {
            let child = mgr.fork_from(parent).await.unwrap();
            fund(&mgr, child, shared, 100 + i).await;
            children.push(child);
        }

        for (i, child) in children.iter().enumerate() {
            assert_eq!(balance_of(&mgr, *child, shared).await, U256::from(100 + i as u64));
        }
        assert_eq!(balance_of(&mgr, parent, shared).await, U256::from(7), "no sibling may write through to the parent");
    }

    #[tokio::test]
    async fn children_keep_working_after_their_parent_is_discarded() {
        let mgr = manager();
        let parent = mgr.fork().await.unwrap();
        let touched = Address::from([0x71; 20]);
        fund(&mgr, parent, touched, 64).await;
        let child = mgr.fork_from(parent).await.unwrap();
        let grandchild = mgr.fork_from(child).await.unwrap();

        // The agent tree outlives its root: nothing about the child's
        // state lives in the parent's session anymore.
        mgr.discard(parent).await.unwrap();

        assert_eq!(balance_of(&mgr, child, touched).await, U256::from(64));
        assert_eq!(balance_of(&mgr, grandchild, touched).await, U256::from(64));
        assert!(mgr.advance(child, spend_funded_balance(Address::from([2u8; 20]))).await.unwrap().is_success());
    }

    #[tokio::test]
    async fn fork_from_an_unknown_or_discarded_session_errors_instead_of_panicking() {
        let mgr = manager();
        assert!(matches!(mgr.fork_from(999).await, Err(SessionError::Unknown(999))));

        let id = mgr.fork().await.unwrap();
        mgr.discard(id).await.unwrap();
        assert!(matches!(mgr.fork_from(id).await, Err(SessionError::Unknown(_))));
        assert_eq!(mgr.active_session_count(), 0, "a failed fork_from must not register anything");
    }

    #[tokio::test]
    async fn unknown_session_errors_instead_of_panicking() {
        let mgr = manager();
        let tx = TxEnv::builder().build_fill();
        let err = mgr.simulate(999, tx).await.unwrap_err();
        assert!(matches!(err, SessionError::Unknown(999)));
    }

    #[tokio::test]
    async fn discard_removes_the_session() {
        let mgr = manager();
        let id = mgr.fork().await.unwrap();
        assert_eq!(mgr.active_session_count(), 1);
        mgr.discard(id).await.unwrap();
        assert_eq!(mgr.active_session_count(), 0);
        let tx = TxEnv::builder().build_fill();
        assert!(matches!(mgr.simulate(id, tx).await, Err(SessionError::Unknown(_))));
    }

    #[tokio::test]
    async fn set_storage_overrides_a_slot_in_the_sessions_overlay() {
        let mgr = manager();
        let id = mgr.fork().await.unwrap();
        let address = Address::from([0x22; 20]);
        let key = U256::from(9u64);
        let value = U256::from(123u64);

        mgr.set_storage(id, address, key, value).await.unwrap();

        assert_eq!(mgr.storage(id, address, key).await.unwrap(), value);
    }

    /// A slot nobody wrote resolves through to the fallback rather than
    /// erroring — the same overlay-then-base-then-fallback order `basic`
    /// already follows.
    #[tokio::test]
    async fn storage_falls_through_to_the_fallback_for_an_untouched_slot() {
        let mgr = manager();
        let id = mgr.fork().await.unwrap();

        let value = mgr.storage(id, Address::from([0x33; 20]), U256::from(1u64)).await.unwrap();

        assert_eq!(value, U256::ZERO);
    }

    /// Reads are per-session: writing a slot in one session must not be
    /// visible from another, the same isolation `set_balance` has.
    #[tokio::test]
    async fn storage_is_isolated_between_sessions() {
        let mgr = manager();
        let (a, b) = (mgr.fork().await.unwrap(), mgr.fork().await.unwrap());
        let address = Address::from([0x44; 20]);
        let key = U256::from(7u64);

        mgr.set_storage(a, address, key, U256::from(42u64)).await.unwrap();

        assert_eq!(mgr.storage(a, address, key).await.unwrap(), U256::from(42u64));
        assert_eq!(mgr.storage(b, address, key).await.unwrap(), U256::ZERO);
    }

    /// `code` answers with the account's deployed bytecode, resolved the
    /// same way execution resolves it (overlay, base, then fallback by
    /// code hash) — what an agent needs to tell a contract from an EOA.
    #[tokio::test]
    async fn code_returns_empty_bytes_for_an_account_with_no_code() {
        let mgr = manager();
        let id = mgr.fork().await.unwrap();

        let code = mgr.code(id, FUNDED).await.unwrap();

        assert!(code.is_empty(), "an EOA has no code, got {} bytes", code.len());
    }

    #[tokio::test]
    async fn ttl_expiry_reaps_idle_sessions_without_being_asked() {
        let mgr = manager(); // 200ms TTL
        let id = mgr.fork().await.unwrap();
        assert_eq!(mgr.active_session_count(), 1);
        tokio::time::sleep(Duration::from_millis(1_500)).await; // > TTL + 1s sweep tick
        assert_eq!(mgr.active_session_count(), 0, "idle session should have been reaped");
        let tx = TxEnv::builder().build_fill();
        assert!(matches!(mgr.simulate(id, tx).await, Err(SessionError::Unknown(_))));
    }

    #[tokio::test]
    async fn refresh_fallback_only_reaches_sessions_forked_afterward() {
        let mgr = SessionManager::new(ValueFallback(100), BlockEnv::default(), 2, Duration::from_secs(60));

        let before = mgr.fork().await.unwrap();
        assert_eq!(mgr.basic(before, WATCHED).await.unwrap().unwrap().balance, U256::from(100));

        // The chain "moved": a fresh fallback reports a different real
        // balance for the same address.
        mgr.refresh_fallback(ValueFallback(999), BlockEnv::default());

        let after = mgr.fork().await.unwrap();
        assert_eq!(
            mgr.basic(after, WATCHED).await.unwrap().unwrap().balance,
            U256::from(999),
            "a session forked after refresh_fallback must read through the new fallback"
        );
        assert_eq!(
            mgr.basic(before, WATCHED).await.unwrap().unwrap().balance,
            U256::from(100),
            "a session forked before refresh_fallback must keep reading its own old fallback, unaffected"
        );
    }

    /// Stands in for the fetch backend, counting every read that reaches
    /// it. That count is the whole point of the cache-seeding tests below:
    /// "the process restarted warm" is only provable as "a read that used
    /// to cost an upstream call no longer makes one."
    #[derive(Clone)]
    struct CountingFallback {
        balance: u64,
        reads: Arc<AtomicUsize>,
    }

    impl DatabaseRef for CountingFallback {
        type Error = FundedFallbackError;
        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let balance = if address == WATCHED { U256::from(self.balance) } else { U256::ZERO };
            Ok(Some(AccountInfo { balance, ..Default::default() }))
        }
        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(Bytecode::default())
        }
        fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(U256::ZERO)
        }
        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            Ok(B256::ZERO)
        }
    }

    /// The state a previous run of this process would have left in its
    /// cache file: `WATCHED` already resolved, at this block.
    fn cached_base(balance: u64) -> BaseSnapshot {
        BaseSnapshot::from_parts(
            [(WATCHED, AccountInfo { balance: U256::from(balance), ..Default::default() })],
            [],
            [],
            [],
        )
    }

    #[tokio::test]
    async fn a_seeded_base_answers_reads_that_would_otherwise_have_hit_the_fallback() {
        let reads = Arc::new(AtomicUsize::new(0));
        let fallback = CountingFallback { balance: 100, reads: Arc::clone(&reads) };
        let mgr = SessionManager::new(fallback, BlockEnv::default(), 2, Duration::from_secs(60))
            .with_base(cached_base(100));

        // Ten sessions, the multi-agent shape — cold, every one of these
        // reads is an upstream call for the first agent and a shared-cache
        // hit for the other nine. Warm, not even the first one costs
        // anything.
        for _ in 0..10 {
            let id = mgr.fork().await.unwrap();
            assert_eq!(mgr.basic(id, WATCHED).await.unwrap().unwrap().balance, U256::from(100));
        }
        assert_eq!(
            reads.load(Ordering::Relaxed),
            0,
            "a base seeded from a previous run's cache must not go back to the network at all"
        );

        // An address the cache file never held still resolves normally,
        // through the fallback — a seeded base is a cache, not a whitelist.
        let id = mgr.fork().await.unwrap();
        assert_eq!(mgr.basic(id, FUNDED).await.unwrap().unwrap().balance, U256::ZERO);
        assert_eq!(reads.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn a_tip_refresh_discards_a_seeded_base_instead_of_serving_its_old_block() {
        let reads = Arc::new(AtomicUsize::new(0));
        let mgr = SessionManager::new(
            CountingFallback { balance: 100, reads: Arc::clone(&reads) },
            BlockEnv::default(),
            2,
            Duration::from_secs(60),
        )
        .with_base(cached_base(100));

        // The seeded base describes the *old* block and is checked before
        // the fallback, so surviving the refresh would pin every later
        // session to the old balance forever.
        mgr.refresh_fallback(
            CountingFallback { balance: 999, reads: Arc::clone(&reads) },
            BlockEnv { number: U256::from(2), ..Default::default() },
        );

        let after = mgr.fork().await.unwrap();
        assert_eq!(mgr.basic(after, WATCHED).await.unwrap().unwrap().balance, U256::from(999));
        assert_eq!(reads.load(Ordering::Relaxed), 1, "the read had to reach the new fallback to be correct");
        assert!(mgr.base().is_empty());
    }

    /// The restart story end to end, minus the network: one manager
    /// resolves through its fallback, persists, and a second manager built
    /// from that file answers the same read without touching its fallback.
    #[tokio::test]
    async fn a_cache_file_written_by_one_run_warms_the_next_one() {
        use forkyard_engine::persist::{CacheKey, ForkCache};

        let dir = std::env::temp_dir().join(format!("forkyard-session-cache-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cache = ForkCache::new(&dir);
        let key = CacheKey::new(1, 23_000_000);

        // First run: a real read, paid upstream, then persisted.
        let first_reads = Arc::new(AtomicUsize::new(0));
        let first = SessionManager::new(
            CountingFallback { balance: 100, reads: Arc::clone(&first_reads) },
            BlockEnv::default(),
            2,
            Duration::from_secs(60),
        );
        let id = first.fork().await.unwrap();
        let info = first.basic(id, WATCHED).await.unwrap().unwrap();
        assert_eq!(first_reads.load(Ordering::Relaxed), 1, "a cold run pays for the read");
        cache.store(key, &BaseSnapshot::from_parts([(WATCHED, info)], [], [], [])).unwrap();

        // Second run: same block, same chain, seeded from the file.
        let second_reads = Arc::new(AtomicUsize::new(0));
        let second = SessionManager::new(
            CountingFallback { balance: 100, reads: Arc::clone(&second_reads) },
            BlockEnv::default(),
            2,
            Duration::from_secs(60),
        )
        .with_base(cache.load(key).unwrap());

        let id = second.fork().await.unwrap();
        assert_eq!(second.basic(id, WATCHED).await.unwrap().unwrap().balance, U256::from(100));
        assert_eq!(
            second_reads.load(Ordering::Relaxed),
            0,
            "the restarted run must answer from the file rather than refetching"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// revm rejects an underpriced transaction before executing it, and
    /// that rejection has to survive as a *type*: surfaces build their
    /// own advice from it (naming the basefee, the shortfall), which is
    /// impossible once it has been flattened into a string.
    #[tokio::test]
    async fn an_underpriced_transaction_fails_with_a_typed_rejection() {
        let mgr = SessionManager::new(
            FundedFallback,
            BlockEnv { basefee: 1_000_000_000, ..Default::default() },
            2,
            Duration::from_secs(60),
        );
        let id = mgr.fork().await.unwrap();

        let tx = TxEnv::builder()
            .caller(FUNDED)
            .kind(TxKind::Call(Address::ZERO))
            .gas_price(0)
            .build_fill();
        let error = mgr.simulate(id, tx).await.expect_err("underpriced must be rejected");

        assert!(
            matches!(
                &error,
                SessionError::InvalidTransaction(reason)
                    if matches!(**reason, InvalidTransaction::GasPriceLessThanBasefee)
            ),
            "should keep revm's typed rejection, got {error:?}"
        );
    }

    /// The funds rejection carries the numbers a caller needs, so a
    /// surface can say what the sender has and what it needed without
    /// looking anything up again.
    #[tokio::test]
    async fn an_unaffordable_fee_fails_with_the_balance_and_fee_attached() {
        let mgr = SessionManager::new(
            FundedFallback, // FUNDED holds 100 wei
            BlockEnv { basefee: 1_000_000_000, ..Default::default() },
            2,
            Duration::from_secs(60),
        );
        let id = mgr.fork().await.unwrap();

        let tx = TxEnv::builder()
            .caller(FUNDED)
            .kind(TxKind::Call(Address::ZERO))
            .gas_limit(21_000)
            .gas_price(1_000_000_000)
            .build_fill();
        let error = mgr.simulate(id, tx).await.expect_err("an unaffordable fee must be rejected");

        match error {
            SessionError::InvalidTransaction(reason) => match *reason {
                InvalidTransaction::LackOfFundForMaxFee { fee, balance } => {
                    assert_eq!(*balance, U256::from(FUNDED_BALANCE));
                    assert_eq!(*fee, U256::from(21_000u64) * U256::from(1_000_000_000u64));
                }
                other => panic!("expected LackOfFundForMaxFee, got {other:?}"),
            },
            other => panic!("expected a typed rejection, got {other:?}"),
        }
    }

    /// A snapshot directory unique to one test, removed afterwards.
    struct SnapshotDir(std::path::PathBuf);

    impl SnapshotDir {
        fn new() -> Self {
            static N: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "forkyard-session-snapshots-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&path);
            Self(path)
        }

        fn store(&self) -> SnapshotStore {
            SnapshotStore::new(&self.0, 1)
        }
    }

    impl Drop for SnapshotDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn a_snapshot_resumes_into_an_independent_session_with_the_same_state() {
        let dir = SnapshotDir::new();
        let mgr = manager().with_snapshots(dir.store());
        let original = mgr.fork().await.unwrap();
        let touched = Address::from([0x91; 20]);
        fund(&mgr, original, touched, 321).await;
        let slot = StorageKey::from(4u64);
        mgr.set_storage(original, touched, slot, U256::from(88u64)).await.unwrap();

        let info = mgr.snapshot(original).await.unwrap();
        let resumed = mgr.resume(&info.id).await.unwrap();

        assert_ne!(resumed, original);
        assert_eq!(balance_of(&mgr, resumed, touched).await, U256::from(321));
        assert_eq!(mgr.storage(resumed, touched, slot).await.unwrap(), U256::from(88u64));

        // Independent from here on, both ways.
        fund(&mgr, resumed, touched, 1).await;
        assert_eq!(balance_of(&mgr, original, touched).await, U256::from(321));
    }

    #[tokio::test]
    async fn a_snapshot_survives_the_process_that_took_it() {
        let dir = SnapshotDir::new();
        let touched = Address::from([0x92; 20]);
        let id = {
            let before = manager().with_snapshots(dir.store());
            let session = before.fork().await.unwrap();
            fund(&before, session, touched, 654).await;
            before.snapshot(session).await.unwrap().id
        };

        // A fresh manager sharing only the directory — a restart.
        let after = manager().with_snapshots(dir.store());
        let resumed = after.resume(&id).await.unwrap();
        assert_eq!(balance_of(&after, resumed, touched).await, U256::from(654));
    }

    #[tokio::test]
    async fn a_branch_snapshots_with_everything_it_inherited() {
        let dir = SnapshotDir::new();
        let mgr = manager().with_snapshots(dir.store());
        let parent = mgr.fork().await.unwrap();
        let from_parent = Address::from([0x93; 20]);
        fund(&mgr, parent, from_parent, 10).await;
        let child = mgr.fork_from(parent).await.unwrap();
        mgr.discard(parent).await.unwrap();

        let resumed = mgr.resume(&mgr.snapshot(child).await.unwrap().id).await.unwrap();
        assert_eq!(balance_of(&mgr, resumed, from_parent).await, U256::from(10));
    }

    #[tokio::test]
    async fn a_snapshot_at_a_pinned_block_resumes_at_that_block() {
        let dir = SnapshotDir::new();
        let (mgr, _calls) = pinning_manager(DEFAULT_MAX_PINNED_BLOCKS);
        let mgr = mgr.with_snapshots(dir.store());
        let at_100 = mgr.fork_at_block(100).await.unwrap();
        let written = Address::from([0x94; 20]);
        mgr.set_account(at_100, written, AccountInfo { balance: U256::from(5), ..Default::default() })
            .await
            .unwrap();

        let resumed = mgr.resume(&mgr.snapshot(at_100).await.unwrap().id).await.unwrap();

        assert_eq!(mgr.session_block_env(resumed).await.unwrap().number, U256::from(100));
        // Unwritten state comes from block 100's fallback, not the default's.
        assert_eq!(watched_balance(&mgr, resumed).await, U256::from(100));
        assert_eq!(mgr.basic(resumed, written).await.unwrap().unwrap().balance, U256::from(5));
    }

    #[tokio::test]
    async fn snapshots_without_a_store_or_with_a_bad_id_are_errors() {
        let mgr = manager();
        let id = mgr.fork().await.unwrap();
        assert!(matches!(mgr.snapshot(id).await, Err(SessionError::Snapshot(_))));

        let dir = SnapshotDir::new();
        let mgr = manager().with_snapshots(dir.store());
        assert!(matches!(mgr.resume("not-an-id").await, Err(SessionError::Snapshot(_))));
        assert!(matches!(mgr.resume(&"a".repeat(32)).await, Err(SessionError::Snapshot(_))));
        assert!(matches!(mgr.snapshot(12345).await, Err(SessionError::Unknown(12345))));
    }

    /// An upstream that takes `delay` per read and counts them — and fails
    /// every read with `fail` set. Wrapped in the real `ReadThrough`, so
    /// the worker's speculative path runs exactly as in production.
    #[derive(Clone)]
    struct SlowFallback {
        delay: Duration,
        reads: Arc<AtomicUsize>,
        fail: bool,
    }

    impl DatabaseRef for SlowFallback {
        type Error = FundedFallbackError;
        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(self.delay);
            if self.fail {
                return Err(FundedFallbackError);
            }
            let balance = if address == FUNDED { U256::from(FUNDED_BALANCE) } else { U256::ZERO };
            Ok(Some(AccountInfo { balance, ..Default::default() }))
        }
        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }
        fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            std::thread::sleep(self.delay);
            Ok(U256::ZERO)
        }
        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    type Slow = forkyard_fetch::ReadThrough<SlowFallback>;

    /// One worker, so every session below shares it — the case where a
    /// blocking read used to stall everyone.
    /// Every key set the resolver was handed, in order.
    type Rounds = Arc<Mutex<Vec<Vec<StateKey>>>>;

    fn slow_manager(delay: Duration, fail: bool) -> (Arc<SessionManager<Slow>>, Rounds) {
        let fallback = forkyard_fetch::ReadThrough::new(SlowFallback { delay, reads: Arc::default(), fail });
        let rounds: Rounds = Arc::default();
        let seen = Arc::clone(&rounds);
        let mgr = SessionManager::new(fallback, BlockEnv::default(), 1, Duration::from_secs(60)).with_resolver(
            move |fallback: &Slow, keys: &[StateKey]| {
                seen.lock().unwrap().push(keys.to_vec());
                fallback.resolve(keys)
            },
        );
        (Arc::new(mgr), rounds)
    }

    #[tokio::test]
    async fn a_session_waiting_on_upstream_does_not_stall_the_others_on_its_worker() {
        let (mgr, _) = slow_manager(Duration::from_millis(400), false);
        let cold = mgr.fork().await.unwrap();
        let warm = mgr.fork().await.unwrap();
        let known = Address::from([0xa1; 20]);
        mgr.set_account(warm, known, AccountInfo { balance: U256::from(9), ..Default::default() }).await.unwrap();

        let slow = {
            let mgr = Arc::clone(&mgr);
            tokio::spawn(async move { mgr.basic(cold, FUNDED).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;

        let start = Instant::now();
        assert_eq!(mgr.basic(warm, known).await.unwrap().unwrap().balance, U256::from(9));
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "a warm read waited {:?} behind another session's upstream fetch",
            start.elapsed()
        );

        let fetched = slow.await.unwrap().unwrap().unwrap();
        assert_eq!(fetched.balance, U256::from(FUNDED_BALANCE), "the parked read answers once its data is in");
    }

    #[tokio::test]
    async fn a_transaction_that_missed_commits_exactly_once_and_fetches_sender_and_recipient_together() {
        let (mgr, rounds) = slow_manager(Duration::from_millis(20), false);
        let id = mgr.fork().await.unwrap();
        let recipient = Address::from([0xa2; 20]);

        let result = mgr.advance(id, spend_funded_balance(recipient)).await.unwrap();
        assert!(result.is_success());
        assert_eq!(balance_of_slow(&mgr, id, recipient).await, U256::from(FUNDED_BALANCE));
        let sender = mgr.basic(id, FUNDED).await.unwrap().unwrap();
        assert_eq!((sender.balance, sender.nonce), (U256::ZERO, 1), "one commit, not one per pass");

        let first = rounds.lock().unwrap()[0].clone();
        assert!(
            first.contains(&StateKey::Account(FUNDED)) && first.contains(&StateKey::Account(recipient)),
            "sender and recipient must share one round trip: {first:?}"
        );
    }

    #[tokio::test]
    async fn jobs_for_one_session_keep_their_order_while_one_waits_on_upstream() {
        let (mgr, _) = slow_manager(Duration::from_millis(200), false);
        let id = mgr.fork().await.unwrap();
        let recipient = Address::from([0xa3; 20]);

        let advance = {
            let mgr = Arc::clone(&mgr);
            tokio::spawn(async move { mgr.advance(id, spend_funded_balance(recipient)).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        // Sent while the advance is parked: it must queue behind it and see
        // its result, not overtake it.
        assert_eq!(balance_of_slow(&mgr, id, recipient).await, U256::from(FUNDED_BALANCE));
        assert!(advance.await.unwrap().unwrap().is_success());
    }

    #[tokio::test]
    async fn an_upstream_failure_surfaces_as_the_jobs_error_instead_of_retrying_forever() {
        let (mgr, _) = slow_manager(Duration::from_millis(1), true);
        let id = mgr.fork().await.unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(5), mgr.basic(id, FUNDED)).await;
        assert!(matches!(outcome, Ok(Err(SessionError::Execution(_)))), "{outcome:?}");
    }

    async fn balance_of_slow(mgr: &SessionManager<Slow>, id: SessionId, address: Address) -> U256 {
        mgr.basic(id, address).await.unwrap().unwrap_or_default().balance
    }
}
