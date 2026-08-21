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
use revm::primitives::Address;
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

enum Job<F> {
    Fork {
        id: SessionId,
        base: Arc<BaseSnapshot>,
        fallback: F,
        block_env: BlockEnv,
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
}

/// A registry of sessions sharing `fallback` and `base`, sharded across
/// `num_workers` OS threads. Cloning `F` per session is cheap by
/// construction — see `forkyard_fetch::Fork`'s own doc comment — so every
/// session gets its own handle to the same underlying fetch cache.
pub struct SessionManager<F: Fallback>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    fallback: F,
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
            fallback,
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

    /// Swap the block context new sessions are forked against. Existing
    /// sessions are unaffected — each already has its own `BlockEnv` pinned
    /// at fork time. This is `forkyard-ingest`'s only write path into a
    /// `SessionManager`; see its module doc for what it does and does not
    /// keep fresh.
    pub fn set_block_env(&self, block_env: BlockEnv) {
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
                fallback: self.fallback.clone(),
                block_env: self.block_env(),
                reply,
            })
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
    async fn ttl_expiry_reaps_idle_sessions_without_being_asked() {
        let mgr = manager(); // 200ms TTL
        let id = mgr.fork().await.unwrap();
        assert_eq!(mgr.active_session_count(), 1);
        tokio::time::sleep(Duration::from_millis(1_500)).await; // > TTL + 1s sweep tick
        assert_eq!(mgr.active_session_count(), 0, "idle session should have been reaped");
        let tx = TxEnv::builder().build_fill();
        assert!(matches!(mgr.simulate(id, tx).await, Err(SessionError::Unknown(_))));
    }
}
