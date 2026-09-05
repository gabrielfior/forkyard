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
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc as std_mpsc;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use forkyard_engine::{BaseSnapshot, Session};
use revm::context::result::ExecutionResult;
use revm::context::{BlockEnv, TxEnv};
use revm::database_interface::DatabaseRef;
use revm::primitives::{Address, StorageKey, StorageValue};
use revm::state::AccountInfo;
use revm::{Database, ExecuteCommitEvm, ExecuteEvm, MainBuilder, MainContext};
use tokio::sync::oneshot;

pub type SessionId = u64;

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
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown(id) => write!(f, "unknown or expired session {id}"),
            Self::Execution(msg) => write!(f, "execution error: {msg}"),
            Self::WorkerGone => write!(f, "worker thread is gone"),
        }
    }
}
impl std::error::Error for SessionError {}

// The `DatabaseRef` bound is only here because `Branch`/`Adopt` carry a
// whole `Session<F>` between workers, and `Session` declares it on its own
// type parameter; every `F` this crate instantiates `Job` with is a
// `Fallback`, which already implies it.
enum Job<F: DatabaseRef> {
    Fork {
        id: SessionId,
        base: Arc<BaseSnapshot>,
        fallback: F,
        block_env: BlockEnv,
        reply: oneshot::Sender<()>,
    },
    /// Take a branch of `parent`'s current state on the worker that owns
    /// it, handing the ready-made child back for `fork_from` to register
    /// on whichever worker its own id lands on. Two hops, because parent
    /// and child are almost never on the same shard.
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
    base: Arc<BaseSnapshot>,
    block_env: Arc<RwLock<BlockEnv>>,
    workers: Vec<std_mpsc::Sender<Job<F>>>,
    counts: Vec<Arc<AtomicUsize>>,
    next_id: AtomicU64,
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
            base: Arc::new(BaseSnapshot::default()),
            block_env: Arc::new(RwLock::new(block_env)),
            workers,
            counts,
            next_id: AtomicU64::new(0),
        }
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
                base: Arc::clone(&self.base),
                fallback: self.fallback.read().unwrap().clone(),
                block_env: self.block_env(),
                reply,
            })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?;
        Ok(id)
    }

    /// Branch a new session off `parent`'s *current* state — everything
    /// `parent` has written or cached so far, not just the shared base
    /// `fork` starts from. This is the "K what-ifs from where this agent
    /// already got to" primitive: still no state copy (see
    /// `Session::branch`), where Anvil needs either K sequential
    /// snapshot/revert cycles in one process or K processes each redoing
    /// the setup.
    ///
    /// The child is an ordinary session from the registry's point of view:
    /// its own id, its own shard (`worker_for` hashes ids, so it usually
    /// isn't the parent's), its own TTL clock, discardable on its own. The
    /// parent stays live and independent — branching only reads it, and
    /// the two never see each other's later writes.
    ///
    /// Errors with `SessionError::Unknown` if `parent` is unknown or has
    /// already expired.
    pub async fn fork_from(&self, parent: SessionId) -> Result<SessionId, SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(parent)
            .send(Job::Branch { parent, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        let session = rx.await.map_err(|_| SessionError::WorkerGone)??;

        // The id is allocated only once the branch actually succeeded, so
        // a `fork_from` against a dead parent doesn't burn one.
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
                    // Branching counts as activity on the parent: a root
                    // session an agent only ever branches from must not be
                    // reaped out from under the tree mid-run.
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
}
