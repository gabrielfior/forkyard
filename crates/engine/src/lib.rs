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
    overlay_accounts: HashMap<Address, AccountInfo>,
    overlay_code: HashMap<B256, Bytecode>,
    overlay_storage: HashMap<(Address, StorageKey), StorageValue>,
}

impl<F: DatabaseRef> Session<F> {
    /// Fork a session from a shared base snapshot — an `Arc` clone, O(1) —
    /// with `fallback` resolving whatever the base hasn't cached yet.
    pub fn fork(base: Arc<BaseSnapshot>, fallback: F) -> Self {
        Self {
            base,
            fallback,
            overlay_accounts: HashMap::new(),
            overlay_code: HashMap::new(),
            overlay_storage: HashMap::new(),
        }
    }

    /// Override an account directly in this session's private overlay —
    /// e.g. funding a freshly generated test signer on top of real forked
    /// state, the same role Anvil's `anvil_setBalance` cheatcode plays.
    /// Never touches the shared base or the real chain.
    pub fn set_account(&mut self, address: Address, info: AccountInfo) {
        self.overlay_accounts.insert(address, info);
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
        let a = Session::fork(Arc::clone(&base), NoFallback);
        let b = Session::fork(Arc::clone(&base), NoFallback);
        assert_eq!(Arc::strong_count(&base), 3); // base + a + b
        drop(a);
        drop(b);
        assert_eq!(Arc::strong_count(&base), 1);
    }

    #[test]
    fn overlay_override_is_session_private() {
        let base = Arc::new(BaseSnapshot::default());
        let mut a = Session::fork(Arc::clone(&base), NoFallback);
        let addr = Address::ZERO;
        a.set_account(
            addr,
            AccountInfo {
                balance: revm::primitives::U256::from(100u64),
                ..Default::default()
            },
        );
        assert_eq!(a.basic(addr).unwrap().unwrap().balance, revm::primitives::U256::from(100u64));

        let mut b = Session::fork(Arc::clone(&base), NoFallback);
        assert!(b.basic(addr).is_err(), "sibling session must not see a's overlay");
    }
}
