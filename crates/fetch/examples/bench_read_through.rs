//! Warm-read cost through `SharedBackend` alone vs through `ReadThrough`,
//! from several threads at once — the shape of N sessions on N workers
//! opening against the same hot contracts. No network: the backend's
//! cache is seeded up front and its provider points nowhere.
//!
//! `cargo run --release -p forkyard-fetch --example bench_read_through`
use std::time::{Duration, Instant};

use alloy_provider::ProviderBuilder;
use forkyard_fetch::{Backend, ReadThrough};
use foundry_fork_db::cache::BlockchainDbMeta;
use foundry_fork_db::{BlockchainDb, SharedBackend};
use revm::context::BlockEnv;
use revm::database_interface::{DatabaseRef, WrapDatabaseRef};
use revm::primitives::{Address, U256};
use revm::state::AccountInfo;

const ADDRESSES: usize = 200;
const READS_PER_THREAD: usize = 20_000;

fn seeded_backend() -> Backend {
    let block_env = BlockEnv { number: U256::from(1u64), ..Default::default() };
    let db = BlockchainDb::new(BlockchainDbMeta::new(block_env, "http://127.0.0.1:9".to_string()), None);
    {
        let mut accounts = db.accounts().write();
        let mut storage = db.storage().write();
        for n in 0..ADDRESSES {
            let address = address(n);
            accounts.insert(address, AccountInfo { balance: U256::from(n), ..Default::default() });
            storage.entry(address).or_default().insert(U256::ZERO, U256::from(n));
        }
    }
    let provider = ProviderBuilder::new().connect_http("http://127.0.0.1:9".parse().unwrap());
    WrapDatabaseRef(SharedBackend::spawn_backend_thread(provider, db, None))
}

fn address(n: usize) -> Address {
    let mut bytes = Address::with_last_byte(n as u8).into_array();
    bytes[0] = (n >> 8) as u8;
    Address::from(bytes)
}

fn run<D: DatabaseRef + Clone + Send + 'static>(db: D, threads: usize) -> Duration
where
    D::Error: std::fmt::Debug,
{
    let start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let db = db.clone();
            std::thread::spawn(move || {
                for i in 0..READS_PER_THREAD {
                    let n = (i * 7 + t) % ADDRESSES;
                    std::hint::black_box(db.basic_ref(address(n)).unwrap());
                    std::hint::black_box(db.storage_ref(address(n), U256::ZERO).unwrap());
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    start.elapsed()
}

fn main() {
    println!("threads | backend ns/read | read-through ns/read | speedup");
    for threads in [1, 4, 12] {
        let reads = (threads * READS_PER_THREAD * 2) as f64;
        let raw = run(seeded_backend(), threads);
        let cached = ReadThrough::new(seeded_backend());
        run(cached.clone(), 1); // warm the read-through layer, as a first session would
        let through = run(cached, threads);
        let raw_ns = raw.as_nanos() as f64 / reads;
        let through_ns = through.as_nanos() as f64 / reads;
        println!("{threads:>7} | {raw_ns:>15.0} | {through_ns:>20.0} | {:>6.1}x", raw_ns / through_ns);
    }
}
