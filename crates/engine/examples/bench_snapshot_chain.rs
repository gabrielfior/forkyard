//! What does checkpointing every step of an agent run cost — today, and with
//! incremental (delta) snapshots?
//!
//! The workload: K steps. Each step writes D fresh storage slots (mapping
//! slots, so the keys are hashes), rewrites a few existing ones, and
//! checkpoints. Four contracts of ~12 KB each are touched from the start —
//! their bytecode rides along in every snapshot today.
//!
//!   cargo run --release -p forkyard-engine --example bench_snapshot_chain -- [K] [D] [KEYFRAME_EVERY]
//!
//! Strategies compared, all on the same sequence of states:
//!   today      `SnapshotStore::store` — the real code, a full JSON file each time.
//!   delta      parent id + only what changed since the previous checkpoint;
//!              bytecode in its own content-addressed blob, written once.
//!   delta+kf   as `delta`, with a full keyframe every KEYFRAME_EVERY checkpoints
//!              so a resume reads a short chain.
//!   delta+geo  as `delta`, with a keyframe whenever the deltas since the last one
//!              add up to as many bytes as that keyframe — so write amplification
//!              stays bounded (every byte is rewritten O(1) times, amortised) and a
//!              resume never reads more than about twice the live state.
//!
//! What this does *not* measure: how the delta is found. Here it is a set
//! difference against the previous state (O(total), and untimed); the design
//! it stands in for tracks dirtiness in the session, making it O(delta).
//! Everything timed is serialise + write + fsync + rename, which is what a
//! real implementation pays after the delta is known.
//!
//! "gz" figures are `gzip -6` over each file, as a stand-in for a compressed
//! on-disk encoding; they show what compression buys on this data, which is
//! half hashes and so not very compressible.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use forkyard_engine::persist::SnapshotStore;
use forkyard_engine::SessionState;
use revm::primitives::{keccak256, Address, Bytes, B256, U256};
use revm::state::{AccountInfo, Bytecode};
use serde_json::{json, Value};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn word(&mut self) -> U256 {
        U256::from_limbs([self.next(), self.next(), self.next(), self.next()])
    }
}

fn median(mut v: Vec<Duration>) -> Duration {
    v.sort();
    v[v.len() / 2]
}

fn ms(d: Duration) -> String {
    format!("{:.2} ms", d.as_secs_f64() * 1e3)
}

fn kb(bytes: u64) -> String {
    if bytes >= 1 << 20 {
        format!("{:.2} MB", bytes as f64 / (1 << 20) as f64)
    } else {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    }
}

fn gzip_len(bytes: &[u8]) -> u64 {
    let mut child = Command::new("gzip").arg("-6").arg("-c").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().expect("gzip on PATH");
    let mut stdin = child.stdin.take().unwrap();
    let data = bytes.to_vec();
    let writer = std::thread::spawn(move || stdin.write_all(&data));
    let out = child.wait_with_output().unwrap();
    writer.join().unwrap().unwrap();
    out.stdout.len() as u64
}

fn write_durably(path: &Path, bytes: &[u8]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let temp = path.with_extension("tmp");
    let mut file = fs::File::create(&temp).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
    fs::rename(&temp, path).unwrap();
}

type StorageKey = (Address, U256);

/// One checkpoint's worth of state, in the shape a delta needs: what is
/// there now, keyed for diffing.
#[derive(Clone, Default)]
struct World {
    accounts: BTreeMap<Address, AccountInfo>,
    code: BTreeMap<B256, Bytecode>,
    storage: BTreeMap<StorageKey, U256>,
}

impl World {
    fn to_state(&self) -> SessionState {
        SessionState {
            accounts: self.accounts.iter().map(|(a, i)| (*a, i.clone())).collect(),
            code: self.code.iter().map(|(h, c)| (*h, c.clone())).collect(),
            storage: self.storage.iter().map(|(k, v)| (*k, *v)).collect(),
        }
    }
}

/// A delta checkpoint on disk: parent link, changed entries, and the hashes
/// of code it references (the bytes live in `code/<hash>.bin`).
struct DeltaStore {
    dir: PathBuf,
    code_written: std::collections::HashSet<B256>,
}

impl DeltaStore {
    fn new(dir: PathBuf) -> Self {
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self { dir, code_written: Default::default() }
    }

    fn account_json(address: &Address, info: &AccountInfo) -> Value {
        json!([address, info.balance, info.nonce, info.code_hash])
    }

    /// Writes `changed` on top of `parent` (or a full keyframe if `parent`
    /// is `None`, in which case `changed` is everything). Returns the id and
    /// the bytes written for this checkpoint — the file plus any code blob
    /// that was new.
    fn put(&mut self, parent: Option<&str>, block: u64, changed: &World) -> (String, Vec<Vec<u8>>) {
        let mut files = Vec::new();
        for (hash, code) in &changed.code {
            if self.code_written.insert(*hash) {
                let raw = code.original_bytes();
                write_durably(&self.dir.join("code").join(format!("{hash:x}.bin")), &raw);
                files.push(raw.to_vec());
            }
        }
        let body = json!({
            "format": "forkyard-session-snapshot-delta",
            "parent": parent,
            "block_number": block,
            "accounts": changed.accounts.iter().map(|(a, i)| Self::account_json(a, i)).collect::<Vec<_>>(),
            "storage": changed.storage.iter().map(|((a, k), v)| json!([a, k, v])).collect::<Vec<_>>(),
            "code": changed.code.keys().collect::<Vec<_>>(),
        });
        let bytes = serde_json::to_vec(&body).unwrap();
        let id = revm::primitives::hex::encode(&keccak256(&bytes)[..16]);
        write_durably(&self.dir.join(format!("{id}.json")), &bytes);
        files.push(bytes);
        (id, files)
    }

    /// Resume: walk parents to the nearest keyframe, then apply oldest first.
    /// Returns the rebuilt state and how many snapshot files were read.
    fn get(&self, id: &str) -> (SessionState, usize) {
        let mut chain = Vec::new();
        let mut next = Some(id.to_string());
        while let Some(id) = next {
            let bytes = fs::read(self.dir.join(format!("{id}.json"))).unwrap();
            let file: Value = serde_json::from_slice(&bytes).unwrap();
            next = file["parent"].as_str().map(str::to_string);
            chain.push(file);
        }
        let reads = chain.len();
        let mut world = World::default();
        for file in chain.into_iter().rev() {
            for entry in file["accounts"].as_array().unwrap() {
                let address: Address = serde_json::from_value(entry[0].clone()).unwrap();
                let info = AccountInfo {
                    balance: serde_json::from_value(entry[1].clone()).unwrap(),
                    nonce: entry[2].as_u64().unwrap(),
                    code_hash: serde_json::from_value(entry[3].clone()).unwrap(),
                    ..Default::default()
                };
                world.accounts.insert(address, info);
            }
            for entry in file["storage"].as_array().unwrap() {
                let address: Address = serde_json::from_value(entry[0].clone()).unwrap();
                let key: U256 = serde_json::from_value(entry[1].clone()).unwrap();
                let value: U256 = serde_json::from_value(entry[2].clone()).unwrap();
                world.storage.insert((address, key), value);
            }
            for hash in file["code"].as_array().unwrap() {
                let hash: B256 = serde_json::from_value(hash.clone()).unwrap();
                let raw = fs::read(self.dir.join("code").join(format!("{hash:x}.bin"))).unwrap();
                world.code.insert(hash, Bytecode::new_raw(Bytes::from(raw)));
            }
        }
        (world.to_state(), reads)
    }
}

/// `after` minus `before`: entries that are new or changed.
fn diff(before: &World, after: &World) -> World {
    let mut out = World::default();
    for (a, i) in &after.accounts {
        if before.accounts.get(a) != Some(i) {
            out.accounts.insert(*a, i.clone());
        }
    }
    for (h, c) in &after.code {
        if !before.code.contains_key(h) {
            out.code.insert(*h, c.clone());
        }
    }
    for (k, v) in &after.storage {
        if before.storage.get(k) != Some(v) {
            out.storage.insert(*k, *v);
        }
    }
    out
}

#[derive(Default)]
struct Tally {
    write_times: Vec<Duration>,
    total_bytes: u64,
    total_gz: u64,
    last_bytes: u64,
    last_gz: u64,
}

impl Tally {
    fn record(&mut self, elapsed: Duration, files: &[Vec<u8>]) {
        let bytes: u64 = files.iter().map(|f| f.len() as u64).sum();
        let gz: u64 = files.iter().map(|f| gzip_len(f)).sum();
        self.write_times.push(elapsed);
        self.total_bytes += bytes;
        self.total_gz += gz;
        self.last_bytes = bytes;
        self.last_gz = gz;
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let steps: usize = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(50);
    let per_step: usize = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(200);
    let keyframe_every: usize = args.get(3).and_then(|v| v.parse().ok()).unwrap_or(10);

    let root = std::env::temp_dir().join(format!("forkyard-snapshot-chain-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let mut rng = Rng(0x9e3779b97f4a7c15);

    // Four contracts of ~12 KB, touched from step 0.
    let mut world = World::default();
    let contracts: Vec<Address> = (1..=4u8).map(|n| Address::new([n; 20])).collect();
    for address in &contracts {
        let raw: Vec<u8> = (0..12_000).map(|_| (rng.next() >> 8) as u8).collect();
        let code = Bytecode::new_raw(Bytes::from(raw));
        let hash = code.hash_slow();
        world.code.insert(hash, code.clone());
        world.accounts.insert(*address, AccountInfo { balance: U256::ZERO, nonce: 1, code_hash: hash, code: Some(code), ..Default::default() });
    }

    let today = SnapshotStore::new(root.join("today"), 1);
    let mut delta = DeltaStore::new(root.join("delta"));
    let mut keyframed = DeltaStore::new(root.join("delta-kf"));
    let mut geometric = DeltaStore::new(root.join("delta-geo"));

    let (mut t_today, mut t_delta, mut t_kf, mut t_geo) =
        (Tally::default(), Tally::default(), Tally::default(), Tally::default());
    let (mut last_today, mut last_delta, mut last_kf, mut last_geo): (Option<String>, Option<String>, Option<String>, Option<String>) =
        (None, None, None, None);
    let (mut geo_keyframe_bytes, mut geo_delta_bytes) = (0u64, 0u64);
    let mut previous = World::default();
    let mut all_existing: Vec<StorageKey> = vec![];

    for step in 0..steps {
        for _ in 0..per_step {
            let key = (contracts[(rng.next() % 4) as usize], rng.word());
            // Half small values (balances, flags), half hash-like.
            let value = if rng.next().is_multiple_of(2) { U256::from(rng.next() % 1_000_000) } else { rng.word() };
            world.storage.insert(key, value);
            all_existing.push(key);
        }
        for _ in 0..5 {
            let key = all_existing[(rng.next() as usize) % all_existing.len()];
            world.storage.insert(key, rng.word());
        }
        let sender = Address::new([0xaa; 20]);
        world.accounts.insert(sender, AccountInfo { balance: U256::from(1_000_000 - step as u64), nonce: step as u64, ..Default::default() });

        let state = world.to_state();
        let block = 23_000_000u64;

        // today
        let started = Instant::now();
        let info = today.store(block, &state).unwrap();
        let elapsed = started.elapsed();
        let file = fs::read(today.path_for(&info.id).unwrap()).unwrap();
        t_today.record(elapsed, &[file]);
        last_today = Some(info.id);

        // delta
        let changed = diff(&previous, &world);
        let started = Instant::now();
        let (id, files) = delta.put(last_delta.as_deref(), block, &changed);
        let elapsed = started.elapsed();
        t_delta.record(elapsed, &files);
        last_delta = Some(id);

        // delta + keyframes
        let keyframe = step % keyframe_every == 0;
        let (parent, payload) = if keyframe { (None, world.clone()) } else { (last_kf.as_deref().map(str::to_string), changed.clone()) };
        let started = Instant::now();
        let (id, files) = keyframed.put(parent.as_deref(), block, &payload);
        let elapsed = started.elapsed();
        t_kf.record(elapsed, &files);
        last_kf = Some(id);

        // delta + geometric keyframes
        let keyframe = last_geo.is_none() || geo_delta_bytes >= geo_keyframe_bytes;
        let (parent, payload) = if keyframe { (None, world.clone()) } else { (last_geo.clone(), changed.clone()) };
        let started = Instant::now();
        let (id, files) = geometric.put(parent.as_deref(), block, &payload);
        let elapsed = started.elapsed();
        let written: u64 = files.iter().map(|f| f.len() as u64).sum();
        if keyframe {
            geo_keyframe_bytes = written;
            geo_delta_bytes = 0;
        } else {
            geo_delta_bytes += written;
        }
        t_geo.record(elapsed, &files);
        last_geo = Some(id);

        previous = world.clone();
    }

    // Resume the last checkpoint each way, checking the state is exact.
    let want = world.to_state();
    let resume = |f: &dyn Fn() -> (SessionState, usize)| {
        let mut times = vec![];
        let mut reads = 0;
        for _ in 0..21 {
            let started = Instant::now();
            let (state, n) = f();
            times.push(started.elapsed());
            reads = n;
            assert!(state == want, "a resumed state differs from the live one");
        }
        (median(times), reads)
    };
    let today_id = last_today.unwrap();
    let (r_today, n_today) = resume(&|| (today.load(&today_id).unwrap().1, 1));
    let delta_id = last_delta.unwrap();
    let (r_delta, n_delta) = resume(&|| delta.get(&delta_id));
    let kf_id = last_kf.unwrap();
    let (r_kf, n_kf) = resume(&|| keyframed.get(&kf_id));
    let geo_id = last_geo.unwrap();
    let (r_geo, n_geo) = resume(&|| geometric.get(&geo_id));

    println!("{steps} checkpoints, {per_step} new slots + 5 rewrites per step, 4 contracts x ~12 KB, keyframe every {keyframe_every}");
    println!("final state: {} slots, {} contracts\n", world.storage.len(), world.code.len());
    println!(
        "{:<10} {:>14} {:>12} {:>14} {:>12} {:>12} {:>12} {:>16}",
        "strategy", "total written", "(gz)", "last ckpt", "(gz)", "write p50", "write @end", "resume (files)"
    );
    for (name, tally, resume_time, reads) in [("today", &t_today, r_today, n_today), ("delta", &t_delta, r_delta, n_delta), ("delta+kf", &t_kf, r_kf, n_kf), ("delta+geo", &t_geo, r_geo, n_geo)] {
        println!(
            "{:<10} {:>14} {:>12} {:>14} {:>12} {:>12} {:>12} {:>16}",
            name,
            kb(tally.total_bytes),
            kb(tally.total_gz),
            kb(tally.last_bytes),
            kb(tally.last_gz),
            ms(median(tally.write_times.clone())),
            ms(median(tally.write_times[tally.write_times.len().saturating_sub(5)..].to_vec())),
            format!("{} ({reads})", ms(resume_time)),
        );
    }
    let _ = fs::remove_dir_all(&root);
}
