//! On-disk persistence for a `BaseSnapshot`, keyed by (chain id, block
//! number).
//!
//! The warm cache used to live only in the process, so a restart re-paid
//! for everything — the one dimension Anvil won, since Foundry persists its
//! fork cache to `~/.foundry/cache/rpc/<chain>/<block>/storage.json`.
//!
//! The cache is an optimisation, never a dependency: every failure path
//! here is an ordinary `Err` the caller logs before starting cold, nothing
//! panics, and writes go through a temp file plus a rename.

use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use revm::context::BlockEnv;
use revm::primitives::{keccak256, Address, Bytes, StorageKey, StorageValue, B256, U256};
use revm::state::{AccountInfo, Bytecode};
use serde::{Deserialize, Serialize};

use crate::{BaseSnapshot, SessionState};

/// Written into every file and checked on load: a file without this tag is
/// someone else's JSON sitting at our path, and must be refused before any
/// of its fields are believed.
pub const CACHE_FORMAT: &str = "forkyard-fork-cache";

/// Bumped whenever the fields below change meaning. An old file is
/// rejected, not migrated: misreading it means stale state in a
/// simulation, where rejecting it costs one cold start.
pub const CACHE_FORMAT_VERSION: u32 = 1;

/// The chain and block a cache file holds. Carried inside the file as well
/// as in its path: paths get renamed and copied, and block X's state served
/// as block Y's is silently wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheKey {
    pub chain_id: u64,
    pub block_number: u64,
}

impl CacheKey {
    pub fn new(chain_id: u64, block_number: u64) -> Self {
        Self { chain_id, block_number }
    }
}

impl fmt::Display for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "chain {} block {}", self.chain_id, self.block_number)
    }
}

#[derive(Debug)]
pub enum CacheError {
    /// The ordinary first-run case, kept distinct so a caller can log it
    /// at debug rather than warn.
    Missing(PathBuf),
    Io(PathBuf, io::Error),
    /// Unparseable, truncated, or holding invalid bytecode — all of which
    /// mean the same thing to a caller: don't trust any of it.
    Malformed(PathBuf, String),
    NotAForkyardCache { path: PathBuf, found: Option<String> },
    VersionMismatch { path: PathBuf, found: Option<u32>, expected: u32 },
    /// The file describes a different chain or a different block than the
    /// one asked for.
    KeyMismatch { path: PathBuf, expected: CacheKey, found_chain_id: Option<u64>, found_block_number: Option<u64> },
    /// Not something `SnapshotStore::store` could have handed out. Checked
    /// before the id goes anywhere near a path: it comes from a client.
    InvalidSnapshotId(String),
}

impl CacheError {
    /// Lets a caller keep a first run quiet and still shout about a
    /// genuinely broken file.
    pub fn is_missing(&self) -> bool {
        matches!(self, Self::Missing(_))
    }
}

fn describe(value: &Option<impl fmt::Display>) -> String {
    match value {
        Some(v) => v.to_string(),
        None => "absent".to_string(),
    }
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing(path) => write!(f, "no cache file at {}", path.display()),
            Self::Io(path, e) => write!(f, "cannot read or write {}: {e}", path.display()),
            Self::Malformed(path, e) => write!(f, "malformed cache file {}: {e}", path.display()),
            Self::NotAForkyardCache { path, found } => write!(
                f,
                "{} is not a forkyard cache file (format tag {}, expected {CACHE_FORMAT})",
                path.display(),
                describe(found)
            ),
            Self::VersionMismatch { path, found, expected } => write!(
                f,
                "{} has cache format version {}, expected {expected}",
                path.display(),
                describe(found)
            ),
            Self::KeyMismatch { path, expected, found_chain_id, found_block_number } => write!(
                f,
                "{} holds chain {} block {}, but {expected} was asked for",
                path.display(),
                describe(found_chain_id),
                describe(found_block_number)
            ),
            Self::InvalidSnapshotId(id) => {
                write!(f, "{id:?} is not a snapshot id (expected {SNAPSHOT_ID_LEN} lowercase hex characters)")
            }
        }
    }
}

impl std::error::Error for CacheError {}

/// Primitive fields rather than a serialized `AccountInfo`: that carries a
/// runtime-only `account_id` and an inline copy of code the `code` list
/// already holds, and ties the file to revm's struct layout.
#[derive(Serialize, Deserialize)]
struct StoredAccount {
    address: Address,
    balance: U256,
    nonce: u64,
    code_hash: B256,
}

/// Original (unpadded) bytes, not a serialized `Bytecode`: that serde form
/// is revm's *analyzed* representation (padding, jump table, kind tag), an
/// interpreter detail. `Bytecode::new_raw_checked` rebuilds it on load.
#[derive(Serialize, Deserialize)]
struct StoredCode {
    hash: B256,
    bytes: Bytes,
}

/// The four fields `forkyard-fetch` reads off a block header, and so the
/// only four a `BlockEnv` built from one ever has set. Stored so a pinned
/// restart can skip that header fetch — the one upstream call a warm start
/// otherwise still makes.
#[derive(Serialize, Deserialize, Clone, Copy)]
struct StoredBlockEnv {
    number: u64,
    timestamp: U256,
    basefee: u64,
    gas_limit: u64,
}

impl StoredBlockEnv {
    fn from_env(env: &BlockEnv) -> Option<Self> {
        Some(Self {
            number: u64::try_from(env.number).ok()?,
            timestamp: env.timestamp,
            basefee: env.basefee,
            gas_limit: env.gas_limit,
        })
    }

    fn to_env(self) -> BlockEnv {
        BlockEnv {
            number: U256::from(self.number),
            timestamp: self.timestamp,
            basefee: self.basefee,
            gas_limit: self.gas_limit,
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
struct CacheFile {
    /// The self-describing fields are `Option` so an absent tag gets the
    /// same specific error as a wrong one, instead of serde's generic
    /// "missing field" parse failure.
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    version: Option<u32>,
    #[serde(default)]
    chain_id: Option<u64>,
    #[serde(default)]
    block_number: Option<u64>,
    #[serde(default)]
    accounts: Vec<StoredAccount>,
    #[serde(default)]
    code: Vec<StoredCode>,
    #[serde(default)]
    storage: Vec<(Address, StorageKey, StorageValue)>,
    #[serde(default)]
    block_hashes: Vec<(u64, B256)>,
    /// Absent in files written before it existed, which still load — they
    /// just can't skip the header fetch. Additive, so no version bump.
    #[serde(default)]
    block_env: Option<StoredBlockEnv>,
}

/// Rebuild analyzed bytecode from stored bytes, refusing the whole file on
/// the first blob that doesn't decode.
fn decode_code(path: &Path, stored: Vec<StoredCode>) -> Result<Vec<(B256, Bytecode)>, CacheError> {
    let mut code = Vec::with_capacity(stored.len());
    for entry in stored {
        // Undecodable code means the bytes on disk aren't what was
        // written, so refuse the whole file rather than serve a
        // snapshot with a hole in it.
        let bytecode = Bytecode::new_raw_checked(entry.bytes)
            .map_err(|e| CacheError::Malformed(path.to_path_buf(), format!("code {}: {e}", entry.hash)))?;
        code.push((entry.hash, bytecode));
    }
    Ok(code)
}

/// Re-attach each account's code inline: revm only calls `code_by_hash`
/// when `basic` returns `code: None`, so leaving it off costs a round trip
/// per contract read.
fn attach_code(accounts: Vec<StoredAccount>, code: &[(B256, Bytecode)]) -> Vec<(Address, AccountInfo)> {
    let by_hash: std::collections::HashMap<B256, &Bytecode> = code.iter().map(|(h, c)| (*h, c)).collect();
    accounts
        .into_iter()
        .map(|a| {
            let info = AccountInfo {
                balance: a.balance,
                nonce: a.nonce,
                code_hash: a.code_hash,
                code: by_hash.get(&a.code_hash).map(|c| (*c).clone()),
                ..Default::default()
            };
            (a.address, info)
        })
        .collect()
}

fn stored_account(address: &Address, info: &AccountInfo) -> StoredAccount {
    StoredAccount { address: *address, balance: info.balance, nonce: info.nonce, code_hash: info.code_hash }
}

/// Separates concurrent temp files within a process, as the pid does
/// between processes: two instances sharing a cache directory must not
/// write into one temp file and rename the mixture into place.
static TEMP_NONCE: AtomicU64 = AtomicU64::new(0);

/// Temp file in `path`'s directory, fsync, rename over `path`. A reader
/// sees the whole old file or the whole new one — an in-place write would
/// leave a crash's truncated prefix as a permanently poisoned entry.
fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), CacheError> {
    let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
    fs::create_dir_all(&dir).map_err(|e| CacheError::Io(dir.clone(), e))?;

    let stem = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
    let temp = dir.join(format!("{stem}.{}.{nonce}.tmp", std::process::id()));
    let write = (|| -> io::Result<()> {
        let mut handle = fs::File::create(&temp)?;
        handle.write_all(bytes)?;
        // Without this the rename can land before the data does: on a
        // crash the file exists, is named correctly, and is empty.
        handle.sync_all()
    })();
    if let Err(e) = write {
        let _ = fs::remove_file(&temp);
        return Err(CacheError::Io(temp, e));
    }
    if let Err(e) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(CacheError::Io(path.to_path_buf(), e));
    }
    Ok(())
}

/// `$HOME/.forkyard/cache` when `FORKYARD_CACHE_DIR` isn't set — alongside
/// `~/.foundry/cache`, never inside it, the formats being unrelated. With
/// no `$HOME`, the temp dir: warm within one boot, and never fatal.
pub fn default_cache_dir() -> PathBuf {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    match home {
        Some(home) if !home.is_empty() => PathBuf::from(home).join(".forkyard").join("cache"),
        _ => std::env::temp_dir().join("forkyard-cache"),
    }
}

/// `$HOME/.forkyard/snapshots` when `FORKYARD_SNAPSHOT_DIR` isn't set —
/// beside the fork cache, not in it: clearing a cache must never delete a
/// snapshot someone meant to come back to.
pub fn default_snapshot_dir() -> PathBuf {
    let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"));
    match home {
        Some(home) if !home.is_empty() => PathBuf::from(home).join(".forkyard").join("snapshots"),
        _ => std::env::temp_dir().join("forkyard-snapshots"),
    }
}

/// A directory of cache files, one per (chain id, block number).
#[derive(Debug, Clone)]
pub struct ForkCache {
    dir: PathBuf,
}

impl ForkCache {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// `<dir>/<chain_id>/<block_number>.json` — the chain-then-block shape
    /// Foundry uses, so both directories read the same way.
    pub fn path_for(&self, key: CacheKey) -> PathBuf {
        self.dir.join(key.chain_id.to_string()).join(format!("{}.json", key.block_number))
    }

    /// Read back the snapshot stored for `key`, or say why it can't be
    /// trusted. Every parse and field check is an `Err`, never a panic.
    pub fn load(&self, key: CacheKey) -> Result<BaseSnapshot, CacheError> {
        self.load_with_block_env(key).map(|(base, _)| base)
    }

    /// `load`, plus the block env the file was written with — `None` for a
    /// file from before that was recorded. What lets a pinned restart
    /// serve without asking upstream for a header it already had.
    pub fn load_with_block_env(&self, key: CacheKey) -> Result<(BaseSnapshot, Option<BlockEnv>), CacheError> {
        let path = self.path_for(key);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(CacheError::Missing(path)),
            Err(e) => return Err(CacheError::Io(path, e)),
        };

        // A truncated file lands here as a JSON syntax error — which is
        // the wanted outcome: reject, start cold.
        let file: CacheFile = serde_json::from_slice(&bytes)
            .map_err(|e| CacheError::Malformed(path.clone(), e.to_string()))?;

        if file.format.as_deref() != Some(CACHE_FORMAT) {
            return Err(CacheError::NotAForkyardCache { path, found: file.format });
        }
        if file.version != Some(CACHE_FORMAT_VERSION) {
            return Err(CacheError::VersionMismatch {
                path,
                found: file.version,
                expected: CACHE_FORMAT_VERSION,
            });
        }
        if file.chain_id != Some(key.chain_id) || file.block_number != Some(key.block_number) {
            return Err(CacheError::KeyMismatch {
                path,
                expected: key,
                found_chain_id: file.chain_id,
                found_block_number: file.block_number,
            });
        }
        // An env for another block than the file's own is a corrupt file,
        // not a usable hint: drop the hint, keep the (checked) state.
        let block_env = file.block_env.filter(|env| env.number == key.block_number).map(StoredBlockEnv::to_env);

        let code = decode_code(&path, file.code)?;
        let accounts = attach_code(file.accounts, &code);

        let base = BaseSnapshot::from_parts(
            accounts,
            code,
            file.storage.into_iter().map(|(address, key, value)| ((address, key), value)),
            file.block_hashes,
        );
        Ok((base, block_env))
    }

    /// Write `snapshot` as the cache for `key`, atomically.
    pub fn store(&self, key: CacheKey, snapshot: &BaseSnapshot) -> Result<(), CacheError> {
        self.store_inner(key, snapshot, None)
    }

    /// `store`, recording `block_env` too, so the next start at this block
    /// can skip fetching its header (`load_with_block_env`).
    pub fn store_with_block_env(
        &self,
        key: CacheKey,
        snapshot: &BaseSnapshot,
        block_env: &BlockEnv,
    ) -> Result<(), CacheError> {
        self.store_inner(key, snapshot, StoredBlockEnv::from_env(block_env))
    }

    fn store_inner(
        &self,
        key: CacheKey,
        snapshot: &BaseSnapshot,
        block_env: Option<StoredBlockEnv>,
    ) -> Result<(), CacheError> {
        let path = self.path_for(key);
        let file = CacheFile {
            format: Some(CACHE_FORMAT.to_string()),
            version: Some(CACHE_FORMAT_VERSION),
            chain_id: Some(key.chain_id),
            block_number: Some(key.block_number),
            accounts: snapshot.accounts().map(|(address, info)| stored_account(address, info)).collect(),
            code: snapshot
                .code()
                .map(|(hash, bytecode)| StoredCode { hash: *hash, bytes: bytecode.original_bytes() })
                .collect(),
            storage: snapshot.storage().map(|((address, key), value)| (*address, *key, *value)).collect(),
            block_hashes: snapshot.block_hashes().map(|(number, hash)| (*number, *hash)).collect(),
            block_env: block_env.filter(|env| env.number == key.block_number),
        };
        let bytes = serde_json::to_vec(&file)
            .map_err(|e| CacheError::Malformed(path.clone(), e.to_string()))?;
        write_atomically(&path, &bytes)
    }
}

/// Written into every session snapshot and checked on load, as
/// `CACHE_FORMAT` is for fork caches. A different tag: the two files hold
/// different things and must never be read as each other.
pub const SNAPSHOT_FORMAT: &str = "forkyard-session-snapshot";

pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// Hex characters in a snapshot id: the first 16 bytes of the file's
/// keccak — collision-free for any realistic number of snapshots, and
/// short enough for an agent to carry around in a prompt.
pub const SNAPSHOT_ID_LEN: usize = 32;

#[derive(Serialize, Deserialize, Default)]
struct SnapshotFile {
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    version: Option<u32>,
    #[serde(default)]
    chain_id: Option<u64>,
    #[serde(default)]
    block_number: Option<u64>,
    #[serde(default)]
    accounts: Vec<StoredAccount>,
    #[serde(default)]
    code: Vec<StoredCode>,
    #[serde(default)]
    storage: Vec<(Address, StorageKey, StorageValue)>,
}

/// What `SnapshotStore::store` wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// Hand this back to `SnapshotStore::load` to get the state again.
    pub id: String,
    pub block_number: u64,
    pub accounts: usize,
    pub storage_slots: usize,
    pub contracts: usize,
    /// Size of the file on disk.
    pub bytes: usize,
}

/// Session snapshots on disk, content-addressed: `<dir>/<chain_id>/<id>.json`,
/// where `id` is derived from the file's own bytes. So a snapshot is
/// immutable once written, snapshotting the same state twice writes the
/// same file, and any process sharing `dir` can resume any id another
/// wrote — the id *is* the handoff, no blob passes through an agent.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    dir: PathBuf,
    chain_id: u64,
}

impl SnapshotStore {
    pub fn new(dir: impl Into<PathBuf>, chain_id: u64) -> Self {
        Self { dir: dir.into(), chain_id }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where `id` lives, or `InvalidSnapshotId` for anything that isn't an
    /// id this store could have minted — which keeps a client-supplied
    /// string from naming any other path.
    pub fn path_for(&self, id: &str) -> Result<PathBuf, CacheError> {
        let valid = id.len() == SNAPSHOT_ID_LEN && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'));
        if !valid {
            return Err(CacheError::InvalidSnapshotId(id.to_string()));
        }
        Ok(self.dir.join(self.chain_id.to_string()).join(format!("{id}.json")))
    }

    /// Write `state`, a session at `block_number`, and return its id.
    pub fn store(&self, block_number: u64, state: &SessionState) -> Result<SnapshotInfo, CacheError> {
        let file = SnapshotFile {
            format: Some(SNAPSHOT_FORMAT.to_string()),
            version: Some(SNAPSHOT_FORMAT_VERSION),
            chain_id: Some(self.chain_id),
            block_number: Some(block_number),
            accounts: state.accounts.iter().map(|(address, info)| stored_account(address, info)).collect(),
            code: state
                .code
                .iter()
                .map(|(hash, bytecode)| StoredCode { hash: *hash, bytes: bytecode.original_bytes() })
                .collect(),
            storage: state.storage.iter().map(|((address, key), value)| (*address, *key, *value)).collect(),
        };
        let bytes = serde_json::to_vec(&file)
            .map_err(|e| CacheError::Malformed(self.dir.clone(), e.to_string()))?;
        let id = revm::primitives::hex::encode(&keccak256(&bytes)[..SNAPSHOT_ID_LEN / 2]);
        let path = self.path_for(&id)?;

        // Content-addressed: an existing file under this id already holds
        // these exact bytes, so rewriting it is pure cost.
        if !path.exists() {
            write_atomically(&path, &bytes)?;
        }
        Ok(SnapshotInfo {
            id,
            block_number,
            accounts: state.accounts.len(),
            storage_slots: state.storage.len(),
            contracts: state.code.len(),
            bytes: bytes.len(),
        })
    }

    /// The block and state stored under `id`, checked as strictly as a
    /// fork cache: format, version and chain must all be this store's.
    pub fn load(&self, id: &str) -> Result<(u64, SessionState), CacheError> {
        let path = self.path_for(id)?;
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(CacheError::Missing(path)),
            Err(e) => return Err(CacheError::Io(path, e)),
        };
        let file: SnapshotFile = serde_json::from_slice(&bytes)
            .map_err(|e| CacheError::Malformed(path.clone(), e.to_string()))?;

        if file.format.as_deref() != Some(SNAPSHOT_FORMAT) {
            return Err(CacheError::NotAForkyardCache { path, found: file.format });
        }
        if file.version != Some(SNAPSHOT_FORMAT_VERSION) {
            return Err(CacheError::VersionMismatch { path, found: file.version, expected: SNAPSHOT_FORMAT_VERSION });
        }
        let (Some(chain_id), Some(block_number)) = (file.chain_id, file.block_number) else {
            return Err(CacheError::Malformed(path, "snapshot names no chain or block".to_string()));
        };
        if chain_id != self.chain_id {
            return Err(CacheError::KeyMismatch {
                path,
                expected: CacheKey::new(self.chain_id, block_number),
                found_chain_id: Some(chain_id),
                found_block_number: Some(block_number),
            });
        }

        let code = decode_code(&path, file.code)?;
        let accounts = attach_code(file.accounts, &code);
        let state = SessionState {
            accounts,
            code,
            storage: file.storage.into_iter().map(|(address, key, value)| ((address, key), value)).collect(),
        };
        Ok((block_number, state))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    /// Hand-rolled rather than a `tempfile` dependency: nothing in this
    /// workspace's lockfile provides one.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static N: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "forkyard-cache-test-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        fn cache(&self) -> ForkCache {
            ForkCache::new(&self.0)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const KEY: CacheKey = CacheKey { chain_id: 1, block_number: 23_000_000 };

    fn contract_code() -> Bytecode {
        Bytecode::new_raw(Bytes::from(vec![0x60, 0x01, 0x60, 0x02, 0x01, 0x00]))
    }

    /// One of everything a real fork cache holds: an EOA, a contract with
    /// code, two of its slots, a block hash.
    fn populated() -> (BaseSnapshot, Address, Address, Bytecode) {
        let eoa = Address::from([0x11; 20]);
        let contract = Address::from([0x22; 20]);
        let code = contract_code();
        let code_hash = code.hash_slow();

        let snapshot = BaseSnapshot::from_parts(
            [
                (eoa, AccountInfo { balance: U256::from(5_000u64), nonce: 7, ..Default::default() }),
                (
                    contract,
                    AccountInfo {
                        balance: U256::from(1u64),
                        nonce: 1,
                        code_hash,
                        code: Some(code.clone()),
                        ..Default::default()
                    },
                ),
            ],
            [(code_hash, code.clone())],
            [
                ((contract, StorageKey::from(0u64)), StorageValue::from(42u64)),
                ((contract, StorageKey::from(1u64)), StorageValue::from(43u64)),
            ],
            [(22_999_999u64, B256::from([0xab; 32]))],
        );
        (snapshot, eoa, contract, code)
    }

    #[test]
    fn a_snapshot_round_trips_through_a_file() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, eoa, contract, code) = populated();

        cache.store(KEY, &snapshot).unwrap();
        let loaded = cache.load(KEY).unwrap();

        assert_eq!(loaded.account_count(), 2);
        assert_eq!(loaded.storage_count(), 2);
        assert_eq!(loaded.code_count(), 1);
        assert_eq!(loaded.block_hash_count(), 1);

        let eoa_info = loaded.account(&eoa).unwrap();
        assert_eq!(eoa_info.balance, U256::from(5_000u64));
        assert_eq!(eoa_info.nonce, 7);

        let contract_info = loaded.account(&contract).unwrap();
        assert_eq!(contract_info.code_hash, code.hash_slow());
        assert_eq!(
            contract_info.code.as_ref().map(|c| c.original_bytes()),
            Some(code.original_bytes()),
            "code must come back attached to the account, not just in the code map"
        );

        assert_eq!(loaded.code_by_hash(&code.hash_slow()).unwrap().original_bytes(), code.original_bytes());
        assert_eq!(loaded.storage_slot(&contract, &StorageKey::from(0u64)), Some(StorageValue::from(42u64)));
        assert_eq!(loaded.storage_slot(&contract, &StorageKey::from(1u64)), Some(StorageValue::from(43u64)));
        assert_eq!(loaded.block_hash(&22_999_999), Some(B256::from([0xab; 32])));
    }

    #[test]
    fn an_empty_snapshot_round_trips_too() {
        let scratch = Scratch::new();
        let cache = scratch.cache();

        cache.store(KEY, &BaseSnapshot::default()).unwrap();

        assert_eq!(cache.load(KEY).unwrap().account_count(), 0);
    }

    #[test]
    fn a_file_written_for_one_chain_or_block_is_refused_for_another() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();
        cache.store(KEY, &snapshot).unwrap();

        // Same file under a different key — a copied or renamed file, which
        // the path check alone would miss.
        let other_chain = CacheKey::new(137, KEY.block_number);
        fs::create_dir_all(cache.path_for(other_chain).parent().unwrap()).unwrap();
        fs::copy(cache.path_for(KEY), cache.path_for(other_chain)).unwrap();
        assert!(
            matches!(cache.load(other_chain), Err(CacheError::KeyMismatch { .. })),
            "chain 1's state must never be served as chain 137's"
        );

        let other_block = CacheKey::new(KEY.chain_id, KEY.block_number + 1);
        fs::copy(cache.path_for(KEY), cache.path_for(other_block)).unwrap();
        assert!(
            matches!(cache.load(other_block), Err(CacheError::KeyMismatch { .. })),
            "one block's balances are not another block's"
        );

        // The key it really was written for still loads.
        assert_eq!(cache.load(KEY).unwrap().account_count(), 2);
    }

    #[test]
    fn a_missing_file_is_a_cold_start_not_a_fault() {
        let scratch = Scratch::new();
        match scratch.cache().load(KEY) {
            Err(error) => {
                assert!(error.is_missing(), "the first run has no file and that is not an error to shout about")
            }
            Ok(_) => panic!("an empty cache directory cannot yield a snapshot"),
        }
    }

    #[test]
    fn a_corrupt_or_truncated_file_starts_cold_instead_of_erroring_out() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();
        cache.store(KEY, &snapshot).unwrap();
        let path = cache.path_for(KEY);

        // Truncated: what a write killed halfway would leave without the
        // temp file and rename.
        let full = fs::read(&path).unwrap();
        fs::write(&path, &full[..full.len() / 2]).unwrap();
        assert!(matches!(cache.load(KEY), Err(CacheError::Malformed { .. })));

        // Outright garbage, e.g. something else's file at our path.
        fs::write(&path, b"\x00\x01not json at all").unwrap();
        assert!(matches!(cache.load(KEY), Err(CacheError::Malformed { .. })));

        // A Foundry `storage.json` at our path: valid JSON, but `accounts`
        // is a map where ours is a list, so it fails before the format tag.
        fs::write(&path, br#"{"meta":{"chain":1},"accounts":{"0x00":{"balance":"0x0"}}}"#).unwrap();
        assert!(matches!(cache.load(KEY), Err(CacheError::Malformed { .. })));

        // Well-formed JSON with nothing of ours in it: only the format tag
        // refuses this, since serde would hand back an empty snapshot.
        fs::write(&path, br#"{"note":"some other tool's file"}"#).unwrap();
        assert!(matches!(cache.load(KEY), Err(CacheError::NotAForkyardCache { found: None, .. })));
    }

    #[test]
    fn an_undecodable_code_blob_rejects_the_whole_file() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();
        cache.store(KEY, &snapshot).unwrap();
        let path = cache.path_for(KEY);

        // 0xef01 is the EIP-7702 delegation prefix, valid only at exactly
        // 23 bytes — a stand-in for bytes that aren't what was written.
        let mut file: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        file["code"][0]["bytes"] = serde_json::json!("0xef0100");
        fs::write(&path, serde_json::to_vec(&file).unwrap()).unwrap();

        assert!(matches!(cache.load(KEY), Err(CacheError::Malformed { .. })));
    }

    #[test]
    fn a_wrong_or_absent_version_tag_is_refused() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();
        cache.store(KEY, &snapshot).unwrap();
        let path = cache.path_for(KEY);

        let original: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();

        let mut newer = original.clone();
        newer["version"] = serde_json::json!(CACHE_FORMAT_VERSION + 1);
        fs::write(&path, serde_json::to_vec(&newer).unwrap()).unwrap();
        assert!(matches!(cache.load(KEY), Err(CacheError::VersionMismatch { found: Some(_), .. })));

        let mut untagged = original.clone();
        untagged.as_object_mut().unwrap().remove("version");
        fs::write(&path, serde_json::to_vec(&untagged).unwrap()).unwrap();
        assert!(
            matches!(cache.load(KEY), Err(CacheError::VersionMismatch { found: None, .. })),
            "an untagged file predates the tag and cannot be interpreted"
        );

        let mut wrong_format = original.clone();
        wrong_format["format"] = serde_json::json!("foundry-rpc-cache");
        fs::write(&path, serde_json::to_vec(&wrong_format).unwrap()).unwrap();
        assert!(matches!(cache.load(KEY), Err(CacheError::NotAForkyardCache { found: Some(_), .. })));
    }

    #[test]
    fn writing_is_atomic_and_leaves_no_temp_file_behind() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();

        cache.store(KEY, &snapshot).unwrap();
        cache.store(KEY, &BaseSnapshot::default()).unwrap(); // smaller than the first

        let dir = cache.path_for(KEY).parent().unwrap().to_path_buf();
        let strays: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp"))
            .collect();
        assert!(strays.is_empty(), "a completed store must leave no temp file: {strays:?}");

        // The smaller second snapshot replaced the first whole: an in-place
        // write would leave the bigger file's tail appended to it.
        assert_eq!(cache.load(KEY).unwrap().account_count(), 0);
    }

    #[test]
    fn a_leftover_temp_file_never_gets_read_as_the_cache() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();
        cache.store(KEY, &snapshot).unwrap();

        // The debris a SIGKILL between `File::create` and `rename` leaves.
        // `load` only ever opens `path_for`, so it can't see this.
        let dir = cache.path_for(KEY).parent().unwrap().to_path_buf();
        fs::write(dir.join(format!("{}.99999.0.tmp", KEY.block_number)), b"half a fi").unwrap();

        assert_eq!(cache.load(KEY).unwrap().account_count(), 2);
    }

    #[test]
    fn storing_creates_the_directory_it_needs() {
        let scratch = Scratch::new();
        let cache = ForkCache::new(scratch.0.join("nested").join("not-yet-there"));
        let (snapshot, ..) = populated();

        cache.store(KEY, &snapshot).unwrap();

        assert_eq!(cache.load(KEY).unwrap().account_count(), 2);
    }

    #[test]
    fn an_unwritable_directory_errors_rather_than_panicking() {
        // A path whose parent is a file: `create_dir_all` fails, and must
        // surface as an ordinary Err the shutdown path can log past.
        let scratch = Scratch::new();
        let blocker = scratch.0.join("blocker");
        fs::write(&blocker, b"not a directory").unwrap();
        let cache = ForkCache::new(blocker.join("cache"));
        let (snapshot, ..) = populated();

        assert!(matches!(cache.store(KEY, &snapshot), Err(CacheError::Io(..))));
        assert!(cache.load(KEY).is_err());
    }

    fn block_env() -> BlockEnv {
        BlockEnv {
            number: U256::from(KEY.block_number),
            timestamp: U256::from(1_750_000_000u64),
            basefee: 3_000_000_000,
            gas_limit: 36_000_000,
            ..Default::default()
        }
    }

    #[test]
    fn the_block_env_round_trips_with_the_cache() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();

        cache.store_with_block_env(KEY, &snapshot, &block_env()).unwrap();
        let (loaded, env) = cache.load_with_block_env(KEY).unwrap();

        assert_eq!(loaded.account_count(), 2);
        assert_eq!(env, Some(block_env()), "a pinned restart needs every field the header fetch would have set");
    }

    #[test]
    fn a_file_from_before_block_envs_were_recorded_still_loads() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();
        cache.store(KEY, &snapshot).unwrap();

        let (loaded, env) = cache.load_with_block_env(KEY).unwrap();
        assert_eq!(loaded.account_count(), 2);
        assert_eq!(env, None, "no env means fetch the header, not refuse the file");
    }

    #[test]
    fn a_block_env_for_another_block_is_never_served() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();
        let mut wrong = block_env();
        wrong.number = U256::from(KEY.block_number + 1);

        cache.store_with_block_env(KEY, &snapshot, &wrong).unwrap();
        assert_eq!(cache.load_with_block_env(KEY).unwrap().1, None);
    }

    fn session_state() -> SessionState {
        let (_, eoa, contract, code) = populated();
        SessionState {
            accounts: vec![
                (eoa, AccountInfo { balance: U256::from(9u64), nonce: 3, ..Default::default() }),
                (contract, AccountInfo { code_hash: code.hash_slow(), code: None, ..Default::default() }),
            ],
            code: vec![(code.hash_slow(), code)],
            storage: vec![((contract, StorageKey::from(5u64)), StorageValue::from(6u64))],
        }
    }

    #[test]
    fn a_session_snapshot_round_trips_with_code_reattached() {
        let scratch = Scratch::new();
        let store = SnapshotStore::new(&scratch.0, 1);
        let state = session_state();

        let info = store.store(KEY.block_number, &state).unwrap();
        assert_eq!(info.id.len(), SNAPSHOT_ID_LEN);
        assert_eq!((info.accounts, info.storage_slots, info.contracts), (2, 1, 1));

        let (block_number, loaded) = store.load(&info.id).unwrap();
        assert_eq!(block_number, KEY.block_number);
        assert_eq!(loaded.accounts[0].1.balance, U256::from(9u64));
        assert_eq!(loaded.storage, state.storage);
        assert!(loaded.accounts[1].1.code.is_some(), "a restored contract must carry its code inline");
    }

    #[test]
    fn the_same_state_gets_the_same_id_and_other_blocks_do_not() {
        let scratch = Scratch::new();
        let store = SnapshotStore::new(&scratch.0, 1);
        let a = store.store(KEY.block_number, &session_state()).unwrap();
        let b = store.store(KEY.block_number, &session_state()).unwrap();
        let other_block = store.store(KEY.block_number + 1, &session_state()).unwrap();
        assert_eq!(a.id, b.id);
        assert_ne!(a.id, other_block.id);
    }

    #[test]
    fn a_snapshot_id_cannot_name_a_path() {
        let scratch = Scratch::new();
        let store = SnapshotStore::new(&scratch.0, 1);
        for id in ["../../etc/passwd", "", "ABCDEF00ABCDEF00ABCDEF00ABCDEF00", &"a".repeat(33)] {
            assert!(
                matches!(store.load(id), Err(CacheError::InvalidSnapshotId(_))),
                "{id:?} must be refused before it reaches the filesystem"
            );
        }
        assert!(store.load(&"a".repeat(SNAPSHOT_ID_LEN)).unwrap_err().is_missing());
    }

    #[test]
    fn a_snapshot_from_another_chain_is_refused() {
        let scratch = Scratch::new();
        let info = SnapshotStore::new(&scratch.0, 1).store(KEY.block_number, &session_state()).unwrap();

        // Same file in chain 137's directory, as a copy would leave it.
        let polygon = SnapshotStore::new(&scratch.0, 137);
        let target = polygon.path_for(&info.id).unwrap();
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(SnapshotStore::new(&scratch.0, 1).path_for(&info.id).unwrap(), &target).unwrap();

        assert!(matches!(polygon.load(&info.id), Err(CacheError::KeyMismatch { .. })));
    }

    #[test]
    fn a_fork_cache_is_not_a_session_snapshot() {
        let scratch = Scratch::new();
        let cache = scratch.cache();
        let (snapshot, ..) = populated();
        cache.store(KEY, &snapshot).unwrap();

        let store = SnapshotStore::new(&scratch.0, 1);
        let id = "0".repeat(SNAPSHOT_ID_LEN);
        fs::copy(cache.path_for(KEY), store.path_for(&id).unwrap()).unwrap();
        assert!(matches!(store.load(&id), Err(CacheError::NotAForkyardCache { .. })));
    }
}
