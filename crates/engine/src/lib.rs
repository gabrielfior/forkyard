//! Persistent state map, session overlay, and the revm `Database`
//! implementation that fuses them. See `docs/RESEARCH.md` ("System design")
//! for the full rationale — forking is a pointer copy over structurally
//! shared state, not a clone of it.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use imbl::HashMap as ImHashMap;
use revm::database_interface::{DBErrorMarker, DatabaseRef};
use revm::primitives::{Address, AddressMap, StorageKey, StorageValue, B256};
use revm::state::{Account, AccountInfo, Bytecode};
use revm::{Database, DatabaseCommit};

/// The shared, immutable working-set cache for one chain. Cloning it is a
/// pointer bump (`imbl`'s structural sharing) — O(1), not a copy of the
/// underlying state. Only accounts/slots some session has actually touched
/// live here; everything else is a miss, resolved by a fallback (in
/// production, `forkyard-fetch`'s `SharedBackend`).
#[derive(Clone, Default)]
pub struct BaseSnapshot {
    accounts: ImHashMap<Address, AccountInfo>,
    code: ImHashMap<B256, Bytecode>,
    storage: ImHashMap<(Address, StorageKey), StorageValue>,
    block_hashes: ImHashMap<u64, B256>,
}

/// A fallback that never resolves anything — for local/unit-test sessions
/// that only ever read what's already in the overlay or base.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoFallback;

#[derive(Debug)]
pub struct NoFallbackError(Address);

impl fmt::Display for NoFallbackError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no fallback configured for account {}", self.0)
    }
}
impl std::error::Error for NoFallbackError {}
impl DBErrorMarker for NoFallbackError {}

impl DatabaseRef for NoFallback {
    type Error = NoFallbackError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Err(NoFallbackError(address))
    }
    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        Err(NoFallbackError(Address::ZERO))
    }
    fn storage_ref(
        &self,
        address: Address,
        _index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        Err(NoFallbackError(address))
    }
    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        Err(NoFallbackError(Address::ZERO))
    }
}

/// One forked session: a pointer to the base it forked from, a private,
/// ordinary (non-persistent) overlay for whatever it writes, and a fallback
/// for whatever neither has seen yet. `simulate` never touches the overlay;
/// `advance` writes into it. Dropping the session discards the overlay —
/// nothing here ever reaches `base` or the real chain.
///
/// TODO(fetch): a resolved fallback read is currently cached only in this
/// session's own overlay, not pushed back into the shared `base` — so two
/// sibling sessions forked from the same base each pay for their own miss on
/// the same account. Closing that requires a versioned, atomically-swapped
/// base handle (see docs/RESEARCH.md layer 4); scoped out of this pass.
pub struct Session<F: DatabaseRef = NoFallback> {
    base: Arc<BaseSnapshot>,
    fallback: F,
    block_env: revm::context::BlockEnv,
    overlay_accounts: HashMap<Address, AccountInfo>,
    overlay_code: HashMap<B256, Bytecode>,
    overlay_storage: HashMap<(Address, StorageKey), StorageValue>,
}

impl<F: DatabaseRef> Session<F> {
    /// Fork a session from a shared base snapshot — an `Arc` clone, O(1) —
    /// with `fallback` resolving whatever the base hasn't cached yet, and
    /// `block_env` the real block (number, timestamp, base fee) this
    /// session's fork is pinned to. Every sibling session forked from the
    /// same base shares the identical `block_env` — they're all forked
    /// from the same block.
    pub fn fork(base: Arc<BaseSnapshot>, fallback: F, block_env: revm::context::BlockEnv) -> Self {
        Self {
            base,
            fallback,
            block_env,
            overlay_accounts: HashMap::new(),
            overlay_code: HashMap::new(),
            overlay_storage: HashMap::new(),
        }
    }

    /// The real block this session's fork is pinned to — what
    /// `forkyard_fetch::fork` actually fetched, not `BlockEnv::default()`.
    /// Callers building a revm `Context` for this session should seed its
    /// block from this, e.g. via `Context::mainnet().with_block(session.block_env().clone())`.
    pub fn block_env(&self) -> &revm::context::BlockEnv {
        &self.block_env
    }

    /// Override an account directly in this session's private overlay —
    /// e.g. funding a freshly generated test signer on top of real forked
    /// state, the same role Anvil's `anvil_setBalance` cheatcode plays.
    /// Never touches the shared base or the real chain.
    pub fn set_account(&mut self, address: Address, info: AccountInfo) {
        self.overlay_accounts.insert(address, info);
    }

    /// Override a single storage slot directly in this session's private
    /// overlay — the same test-only cheatcode role `set_account` plays for
    /// balance/nonce, here for arbitrary contract storage (e.g. writing an
    /// ERC-20 `balanceOf` mapping entry directly, since there's no faucet
    /// or impersonation to fund tokens the normal way). Never touches the
    /// shared base or the real chain. Mirrors Anvil's `anvil_setStorageAt`.
    pub fn set_storage(&mut self, address: Address, key: StorageKey, value: StorageValue) {
        self.overlay_storage.insert((address, key), value);
    }

    /// Branch a *new* session off this one's current state — base plus
    /// everything this session has written or resolved so far, folded into
    /// its own fresh `BaseSnapshot`, with an empty overlay on top. This is
    /// "explore K what-ifs from where I already got to": Anvil can only say
    /// it as a snapshot/revert stack (one branch live at a time) or by
    /// serializing the whole touched state through `anvil_dumpState`.
    ///
    /// Neither side is a copy of state and neither can reach the other
    /// afterwards. The fold clones `imbl` maps — structurally shared with
    /// this session's base, so it pays only for the overlay's own entries,
    /// not for the base's — into an `Arc` nothing else holds, and after
    /// that no `BaseSnapshot` is ever mutated again: this session keeps
    /// writing into *its* overlay (invisible to the child, whose base was
    /// frozen at this instant) and the child writes into the child's
    /// overlay (invisible to this session, which doesn't hold the child's
    /// base at all). Dropping either side leaves the other whole — the
    /// parent's own base survives on the child's `Arc` even if the parent
    /// itself is discarded, which is what lets an agent tree outlive its
    /// root.
    ///
    /// `block_env` and the fallback come along unchanged: a branch is
    /// another branch of the same fork at the same block, not a re-fork at
    /// head. The fallback clone is the same cheap shared-cache handle
    /// `SessionManager::fork` hands out (see `forkyard_fetch::Fork`), so
    /// the child keeps reading through the same warm cache.
    pub fn branch(&self) -> Self
    where
        F: Clone,
    {
        let mut accounts = self.base.accounts.clone();
        for (address, info) in &self.overlay_accounts {
            accounts.insert(*address, info.clone());
        }
        let mut code = self.base.code.clone();
        for (hash, bytecode) in &self.overlay_code {
            code.insert(*hash, bytecode.clone());
        }
        let mut storage = self.base.storage.clone();
        for (key, value) in &self.overlay_storage {
            storage.insert(*key, *value);
        }

        Self {
            // No overlay counterpart to fold in: `block_hash` reads are
            // served straight from base-or-fallback and never cached in
            // the overlay, so the base's map is already the whole picture.
            base: Arc::new(BaseSnapshot { accounts, code, storage, block_hashes: self.base.block_hashes.clone() }),
            fallback: self.fallback.clone(),
            block_env: self.block_env.clone(),
            overlay_accounts: HashMap::new(),
            overlay_code: HashMap::new(),
            overlay_storage: HashMap::new(),
        }
    }
}

/// Surfaced when a read misses the session overlay, the shared base, *and*
/// the fallback.
#[derive(Debug)]
pub enum SessionDbError<E> {
    CodeMiss { code_hash: B256 },
    Fallback(E),
}

impl<E: fmt::Display> fmt::Display for SessionDbError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CodeMiss { code_hash } => write!(f, "no cached code for hash {code_hash}"),
            Self::Fallback(e) => write!(f, "fallback lookup failed: {e}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for SessionDbError<E> {}
impl<E: fmt::Debug + fmt::Display + Send + Sync + 'static> DBErrorMarker for SessionDbError<E> {}

impl<F: DatabaseRef> Database for Session<F>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    type Error = SessionDbError<F::Error>;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(info) = self.overlay_accounts.get(&address) {
            return Ok(Some(info.clone()));
        }
        if let Some(info) = self.base.accounts.get(&address) {
            return Ok(Some(info.clone()));
        }
        let info = self
            .fallback
            .basic_ref(address)
            .map_err(SessionDbError::Fallback)?;
        if let Some(info) = &info {
            self.overlay_accounts.insert(address, info.clone());
        }
        Ok(info)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if let Some(code) = self.overlay_code.get(&code_hash) {
            return Ok(code.clone());
        }
        if let Some(code) = self.base.code.get(&code_hash) {
            return Ok(code.clone());
        }
        match self.fallback.code_by_hash_ref(code_hash) {
            Ok(code) => {
                self.overlay_code.insert(code_hash, code.clone());
                Ok(code)
            }
            // A missing fallback (NoFallback) is the common "no code, this
            // is an EOA" case for storage/basic lookups, but code lookups
            // have no such default — surface it distinctly.
            Err(_) => Err(SessionDbError::CodeMiss { code_hash }),
        }
    }

    fn storage(
        &mut self,
        address: Address,
        index: StorageKey,
    ) -> Result<StorageValue, Self::Error> {
        if let Some(value) = self.overlay_storage.get(&(address, index)) {
            return Ok(*value);
        }
        if let Some(value) = self.base.storage.get(&(address, index)) {
            return Ok(*value);
        }
        match self.fallback.storage_ref(address, index) {
            Ok(value) => {
                self.overlay_storage.insert((address, index), value);
                Ok(value)
            }
            // No fallback configured / slot genuinely empty both read as
            // "zero", which is the correct default for untouched storage.
            Err(_) => Ok(StorageValue::ZERO),
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        if let Some(hash) = self.base.block_hashes.get(&number) {
            return Ok(*hash);
        }
        self.fallback
            .block_hash_ref(number)
            .map_err(SessionDbError::Fallback)
    }
}

/// `advance`: write an execution's resulting diff into this session's
/// private overlay only. The shared base and the real network are never
/// touched — see `docs/RESEARCH.md`, "what simulate / advance actually do".
impl<F: DatabaseRef> DatabaseCommit for Session<F> {
    fn commit(&mut self, changes: AddressMap<Account>) {
        for (address, account) in changes {
            for (key, slot) in account.storage.iter() {
                self.overlay_storage.insert((address, *key), slot.present_value);
            }
            self.overlay_accounts.insert(address, account.info);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_is_a_pointer_copy_not_a_clone_of_state() {
        let base = Arc::new(BaseSnapshot::default());
        let a = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        let b = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        assert_eq!(Arc::strong_count(&base), 3); // base + a + b
        drop(a);
        drop(b);
        assert_eq!(Arc::strong_count(&base), 1);
    }

    #[test]
    fn overlay_override_is_session_private() {
        let base = Arc::new(BaseSnapshot::default());
        let mut a = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        let addr = Address::ZERO;
        a.set_account(
            addr,
            AccountInfo {
                balance: revm::primitives::U256::from(100u64),
                ..Default::default()
            },
        );
        assert_eq!(a.basic(addr).unwrap().unwrap().balance, revm::primitives::U256::from(100u64));

        let mut b = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        assert!(b.basic(addr).is_err(), "sibling session must not see a's overlay");
    }

    /// A fallback that serves one contract's code and counts how often it
    /// was asked for it — the only way to tell "the child read this out of
    /// the folded base" apart from "the child went back to the fallback,"
    /// since a branch inherits the parent's fallback and would otherwise
    /// resolve either way.
    #[derive(Clone)]
    struct CountingCodeFallback {
        code: Bytecode,
        hits: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DatabaseRef for CountingCodeFallback {
        type Error = NoFallbackError;
        fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(Some(AccountInfo::default()))
        }
        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(self.code.clone())
        }
        fn storage_ref(&self, _address: Address, _index: StorageKey) -> Result<StorageValue, Self::Error> {
            Ok(StorageValue::ZERO)
        }
        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    fn funded(balance: u64) -> AccountInfo {
        AccountInfo { balance: revm::primitives::U256::from(balance), ..Default::default() }
    }

    #[test]
    fn branch_starts_from_the_parents_current_state_not_the_parents_base() {
        let base = Arc::new(BaseSnapshot::default());
        let mut parent = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        let addr = Address::from([0x31; 20]);
        let key = StorageKey::from(3u64);
        parent.set_account(addr, funded(500));
        parent.set_storage(addr, key, StorageValue::from(77u64));

        let mut child = parent.branch();

        // NoFallback errors on every miss, so a read that resolves at all
        // proves the value came from the folded base — the child's own
        // overlay is empty and the base it forked from is empty too.
        assert_eq!(child.basic(addr).unwrap().unwrap().balance, revm::primitives::U256::from(500u64));
        assert_eq!(Database::storage(&mut child, addr, key).unwrap(), StorageValue::from(77u64));
        assert_eq!(child.block_env(), parent.block_env());
    }

    #[test]
    fn branch_carries_the_parents_cached_code_without_refetching_it() {
        let code = Bytecode::new_raw(revm::primitives::Bytes::from(vec![0x60, 0x00]));
        let code_hash = code.hash_slow();
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let fallback = CountingCodeFallback { code, hits: Arc::clone(&hits) };
        let mut parent = Session::fork(
            Arc::new(BaseSnapshot::default()),
            fallback,
            revm::context::BlockEnv::default(),
        );

        parent.code_by_hash(code_hash).unwrap();
        assert_eq!(hits.load(std::sync::atomic::Ordering::Relaxed), 1);

        let mut child = parent.branch();
        child.code_by_hash(code_hash).unwrap();
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the branch already holds the parent's cached code — it must not go back to the fallback"
        );
    }

    #[test]
    fn a_branch_and_its_parent_never_see_each_others_later_writes() {
        let base = Arc::new(BaseSnapshot::default());
        let mut parent = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        let shared = Address::from([0x41; 20]);
        parent.set_account(shared, funded(1));

        let mut child = parent.branch();

        // Both sides move the same account after the branch point.
        child.set_account(shared, funded(999));
        parent.set_account(shared, funded(2));

        assert_eq!(parent.basic(shared).unwrap().unwrap().balance, revm::primitives::U256::from(2u64));
        assert_eq!(child.basic(shared).unwrap().unwrap().balance, revm::primitives::U256::from(999u64));

        // An address only one side has ever heard of stays unknown to the
        // other — with NoFallback, "unknown" surfaces as an error.
        let child_only = Address::from([0x42; 20]);
        child.set_account(child_only, funded(5));
        assert!(parent.basic(child_only).is_err(), "the parent must not see the child's later writes");

        let parent_only = Address::from([0x43; 20]);
        parent.set_account(parent_only, funded(6));
        assert!(child.basic(parent_only).is_err(), "the child must not see the parent's later writes");
    }

    #[test]
    fn a_branch_outlives_the_parent_it_came_from() {
        let base = Arc::new(BaseSnapshot::default());
        let mut parent = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        let addr = Address::from([0x51; 20]);
        parent.set_account(addr, funded(42));

        let mut child = parent.branch();
        drop(parent); // the agent tree's root goes away mid-run

        assert_eq!(child.basic(addr).unwrap().unwrap().balance, revm::primitives::U256::from(42u64));
    }

    #[test]
    fn set_storage_overrides_the_overlay_and_reads_back() {
        let base = Arc::new(BaseSnapshot::default());
        let mut session = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        let address = Address::from([0x11; 20]);
        let key = StorageKey::from(7u64);
        let value = StorageValue::from(42u64);

        session.set_storage(address, key, value);

        assert_eq!(Database::storage(&mut session, address, key).unwrap(), value);
    }
}
