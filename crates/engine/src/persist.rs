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

use revm::primitives::{Address, Bytes, StorageKey, StorageValue, B256, U256};
use revm::state::{AccountInfo, Bytecode};
use serde::{Deserialize, Serialize};

use crate::BaseSnapshot;

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

/// A directory of cache files, one per (chain id, block number).
#[derive(Debug, Clone)]
pub struct ForkCache {
    dir: PathBuf,
}

/// Separates concurrent temp files within a process, as the pid does
/// between processes: two instances sharing a cache directory must not
/// write into one temp file and rename the mixture into place.
static TEMP_NONCE: AtomicU64 = AtomicU64::new(0);

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

        let mut code = Vec::with_capacity(file.code.len());
        for entry in file.code {
            // Undecodable code means the bytes on disk aren't what was
            // written, so refuse the whole file rather than serve a
            // snapshot with a hole in it.
            let bytecode = Bytecode::new_raw_checked(entry.bytes)
                .map_err(|e| CacheError::Malformed(path.clone(), format!("code {}: {e}", entry.hash)))?;
            code.push((entry.hash, bytecode));
        }

        // Re-attach each account's code inline: revm only calls
        // `code_by_hash` when `basic` returns `code: None`, so leaving it
        // off costs a round trip per contract read.
        let by_hash: std::collections::HashMap<B256, Bytecode> = code.iter().cloned().collect();
        let accounts = file.accounts.into_iter().map(|a| {
            let info = AccountInfo {
                balance: a.balance,
                nonce: a.nonce,
                code_hash: a.code_hash,
                code: by_hash.get(&a.code_hash).cloned(),
                ..Default::default()
            };
            (a.address, info)
        });

        Ok(BaseSnapshot::from_parts(
            accounts,
            code,
            file.storage.into_iter().map(|(address, key, value)| ((address, key), value)),
            file.block_hashes,
        ))
    }

    /// Write `snapshot` as the cache for `key`: temp file in the same
    /// directory, fsync, rename over the target. A reader sees the whole
    /// old file or the whole new one — an in-place write would leave a
    /// crash's truncated prefix as a permanently poisoned entry.
    pub fn store(&self, key: CacheKey, snapshot: &BaseSnapshot) -> Result<(), CacheError> {
        let path = self.path_for(key);
        let dir = path.parent().unwrap_or(&self.dir).to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| CacheError::Io(dir.clone(), e))?;

        let file = CacheFile {
            format: Some(CACHE_FORMAT.to_string()),
            version: Some(CACHE_FORMAT_VERSION),
            chain_id: Some(key.chain_id),
            block_number: Some(key.block_number),
            accounts: snapshot
                .accounts()
                .map(|(address, info)| StoredAccount {
                    address: *address,
                    balance: info.balance,
                    nonce: info.nonce,
                    code_hash: info.code_hash,
                })
                .collect(),
            code: snapshot
                .code()
                .map(|(hash, bytecode)| StoredCode { hash: *hash, bytes: bytecode.original_bytes() })
                .collect(),
            storage: snapshot.storage().map(|((address, key), value)| (*address, *key, *value)).collect(),
            block_hashes: snapshot.block_hashes().map(|(number, hash)| (*number, *hash)).collect(),
        };
        let bytes = serde_json::to_vec(&file)
            .map_err(|e| CacheError::Malformed(path.clone(), e.to_string()))?;

        let nonce = TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let temp = dir.join(format!("{}.{}.{nonce}.tmp", key.block_number, std::process::id()));
        let write = (|| -> io::Result<()> {
            let mut handle = fs::File::create(&temp)?;
            handle.write_all(&bytes)?;
            // Without this the rename can land before the data does: on a
            // crash the file exists, is named correctly, and is empty.
            handle.sync_all()
        })();
        if let Err(e) = write {
            let _ = fs::remove_file(&temp);
            return Err(CacheError::Io(temp, e));
        }
        if let Err(e) = fs::rename(&temp, &path) {
            let _ = fs::remove_file(&temp);
            return Err(CacheError::Io(path, e));
        }
        Ok(())
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
}
