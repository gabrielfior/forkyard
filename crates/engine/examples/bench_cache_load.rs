//! Times `ForkCache::load` on an existing cache file.
//! `cargo run --release -p forkyard-engine --example bench_cache_load -- <cache_dir> <chain_id> <block>`
use std::time::Instant;

use forkyard_engine::persist::{CacheKey, ForkCache};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cache = ForkCache::new(&args[1]);
    let key = CacheKey::new(args[2].parse().unwrap(), args[3].parse().unwrap());
    let mut times = Vec::new();
    for _ in 0..20 {
        let start = Instant::now();
        let base = cache.load(key).unwrap();
        times.push(start.elapsed());
        std::hint::black_box(base);
    }
    times.sort();
    println!("median load {:?}  min {:?}  max {:?}", times[10], times[0], times[19]);
}
