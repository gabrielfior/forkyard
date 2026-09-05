# Ten-idea measurement pass — 2026-09-04

Every CSV here comes from one serial run of `scratchpad/run_all.sh`-equivalent
commands (see each benchmark's `--help`), on an Apple M3 Pro (12 cores, 38 GB)
against a Tenderly mainnet archive gateway, block 25795072 unless the file says
otherwise. Nothing ran concurrently with anything else: these are timings, and a
second benchmark sharing the CPU, the ports or the RPC quota would corrupt them.

Two environment facts that make these numbers mean what they say:

- **`FORKYARD_CACHE_DISABLED=1` for every run except `warmstart.csv`.** forkyard
  now persists its fetch cache per (chain, block) the way Foundry does, so
  without the kill switch a run would be reading the previous run's work.
  `bench_warmstart.py` pops the variable deliberately — the cache is its subject.
- **Anvil runs with `--no-storage-caching`** everywhere except `warmstart.csv`,
  for the same reason and with the same exception.

These runs are the first on a binary where `FORKYARD_FORK_BLOCK_NUMBER` actually
pins state. Before commit `874dfd2`, `SharedBackend` was spawned with
`pin_block: None`, so forkyard's reads went to `latest` while Anvil's were
pinned — earlier results in `../2026-09-04/` compared two different chain states.

| File | Idea | What it measures |
| --- | --- | --- |
| `branching.csv` | 1 | K what-ifs from one starting state: forkyard branch vs Anvil's snapshot stack vs K Anvil processes. Wall clocks, no proxy. |
| `branching_proxied.csv` | 1 | The same sweep through the counting proxy — read the call counts here, the timings from the file above. |
| `checkpoint.csv` | 2 | Checkpoint/restore cost and blob size as touched state grows 100 → 10,000 slots. |
| `writers.csv` | 3 | Isolated concurrent writers per GB, with a per-writer isolation assertion. |
| `arrivals.csv`, `arrivals.summary.csv` | 4 | Poisson arrivals; p50/p95/p99 from arrival to first successful simulation. |
| `freshness.csv`, `freshness.summary.csv` | 5 | Staying at the chain tip: block lag, refresh latency, upstream calls. The 25-agent Anvil leg is absent — 25 tip-following Anvils could not all start (port 21005 refused after 30s); that failure is the result, not a gap. |
| `quota_delay.csv`, `quota_reject.csv` | 6 | Max sustainable agents under a token-bucket quota, in both queueing and refusing provider modes. |
| `upstream.csv`, `upstream.upstream.csv` | 7 | Cold upstream call counts feeding `cost_model.py` (CU and $/1k agent runs). |
| `warmstart.csv` | 9 | Restart cost, cold → warm, for **both** backends with **both** caches enabled. |
| `blocks.csv`, `blocks.summary.csv` | 10 | 24 agents spread over B ∈ {1,2,4,8} blocks, two rounds. Several Anvil rows hit the 120s cap with nonzero `block_mismatches` — those wall clocks are a timeout, not a measurement. |

Idea 8 (`fork_from`) has no file of its own: it is the capability the
`forkyard-branch` arm of `branching.csv` exercises.
