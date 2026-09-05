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

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use forkyard_engine::{BaseSnapshot, Session};
use revm::context::result::ExecutionResult;
use revm::context::{BlockEnv, TxEnv};
use revm::database_interface::DatabaseRef;
use revm::primitives::{Address, StorageKey, StorageValue};
use revm::state::AccountInfo;
use revm::{Database, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext};
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

#[derive(Debug)]
pub enum SessionError {
    Unknown(SessionId),
    Execution(String),
    /// The worker thread this session was assigned to is gone — a bug
    /// (a worker's own job loop panicked past `catch_unwind`, or the
    /// manager was dropped), not a normal runtime condition.
    WorkerGone,
    /// No fallback could be built for the requested block — upstream can't
    /// serve it, or no block-fork factory was configured. A caller error to
    /// report, not a panic to take a worker down with.
    BlockUnavailable(u64, String),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown(id) => write!(f, "unknown or expired session {id}"),
            Self::Execution(msg) => write!(f, "execution error: {msg}"),
            Self::WorkerGone => write!(f, "worker thread is gone"),
            Self::BlockUnavailable(number, reason) => {
                write!(f, "cannot open a session at block {number}: {reason}")
            }
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
        reply: oneshot::Sender<()>,
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
        let mut workers = Vec::with_capacity(num_workers);
        let mut counts = Vec::with_capacity(num_workers);
        for idx in 0..num_workers {
            let count = Arc::new(AtomicUsize::new(0));
            workers.push(spawn_worker(idx, ttl, Arc::clone(&count)));
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
        }
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
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::Fork {
                id,
                base: self.base(),
                fallback: self.fallback.read().unwrap().clone(),
                block_env: self.block_env(),
                reply,
            })
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

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::Fork { id, base, fallback, block_env, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?;
        Ok(id)
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
) -> std_mpsc::Sender<Job<F>>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let (tx, rx) = std_mpsc::channel::<Job<F>>();
    std::thread::Builder::new()
        .name(format!("forkyard-worker-{idx}"))
        .spawn(move || worker_loop(rx, ttl, count))
        .expect("failed to spawn forkyard worker thread");
    tx
}

fn worker_loop<F: Fallback>(rx: std_mpsc::Receiver<Job<F>>, ttl: Duration, count: Arc<AtomicUsize>)
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let mut sessions: HashMap<SessionId, (Session<F>, Instant)> = HashMap::new();
    loop {
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(job) => {
                // A panic here (e.g. a revm bug on malformed input) is
                // caught at this boundary — revm has no unsafe/FFI in its
                // hot path, so this is sound — and only poisons this one
                // worker's sessions, not the other shards.
                if std::panic::catch_unwind(AssertUnwindSafe(|| handle_job(&mut sessions, job))).is_err() {
                    tracing::error!("forkyard worker job panicked; that session's state may be inconsistent");
                }
                count.store(sessions.len(), Ordering::Relaxed);
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                let before = sessions.len();
                let now = Instant::now();
                sessions.retain(|_, (_, touched)| now.duration_since(*touched) < ttl);
                let removed = before - sessions.len();
                if removed > 0 {
                    tracing::info!(removed, "reaped expired sessions");
                    count.store(sessions.len(), Ordering::Relaxed);
                }
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn handle_job<F: Fallback>(sessions: &mut HashMap<SessionId, (Session<F>, Instant)>, job: Job<F>)
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    match job {
        Job::Fork { id, base, fallback, block_env, reply } => {
            sessions.insert(id, (Session::fork(base, fallback, block_env), Instant::now()));
            let _ = reply.send(());
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
            let _ = reply.send(());
        }
        Job::Simulate { id, tx, disable_checks, reply } => {
            let result = run(sessions, id, *tx, false, disable_checks);
            let _ = reply.send(result);
        }
        Job::Advance { id, tx, reply } => {
            let result = run(sessions, id, *tx, true, false);
            let _ = reply.send(result);
        }
        Job::Discard { id, reply } => {
            sessions.remove(&id);
            let _ = reply.send(());
        }
        Job::Basic { id, address, reply } => {
            let result = match sessions.get_mut(&id) {
                Some((session, touched)) => {
                    *touched = Instant::now();
                    Database::basic(session, address).map_err(|e| SessionError::Execution(format!("{e}")))
                }
                None => Err(SessionError::Unknown(id)),
            };
            let _ = reply.send(result);
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
    }
}

fn run<F: Fallback>(
    sessions: &mut HashMap<SessionId, (Session<F>, Instant)>,
    id: SessionId,
    tx: TxEnv,
    commit: bool,
    disable_checks: bool,
) -> Result<ExecutionResult, SessionError>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let (session, touched) = sessions.get_mut(&id).ok_or(SessionError::Unknown(id))?;
    *touched = Instant::now();
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
    if commit {
        evm.transact_commit(tx).map_err(|e| SessionError::Execution(format!("{e:?}")))
    } else {
        evm.transact(tx)
            .map(|result_and_state| result_and_state.result)
            .map_err(|e| SessionError::Execution(format!("{e:?}")))
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

        // No direct storage-read accessor exists on SessionManager today,
        // so this only proves set_storage doesn't error and reaches the
        // right session (the wrong-id case would surface as
        // SessionError::Unknown from the .unwrap() above). A full
        // write-then-read-back is covered at the forkyard-engine level
        // (Task 3's test).
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
}
