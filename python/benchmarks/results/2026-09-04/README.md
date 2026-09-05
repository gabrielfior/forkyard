# Raw benchmark runs — 2026-09-04

The CSVs behind the [Benchmark section](../../../../README.md#benchmark) of the
root README. Machine: Apple M3 Pro (12 cores, 38 GB), macOS. Endpoint: a
Tenderly mainnet gateway (archive) at block 25795072 unless noted.

**Anvil's RPC cache is disabled in every run here** (`--no-storage-caching`,
the harness default) except the three `anvilcache_*` files, which exist to
measure that cache. Foundry otherwise persists fork state per resolved block
and reuses it across processes and runs, while forkyard refills its in-memory
cache on every process start — an earlier round of these runs was
contaminated exactly that way, with whole sweeps in which Anvil made zero
upstream state calls.

| File | Workload | Agent tiers | Notes |
| --- | --- | --- | --- |
| `headline.csv` | 5 actions, one long-lived environment | 1, 5, 10, 25, 50, 100 | The headline elapsed-time and per-interaction tables. |
| `headline_workers12.csv` | same | 100 | `FORKYARD_NUM_WORKERS=12`; paired against the 100-agent row above. |
| `headline_100_memory_sampled.csv` | same | 100 | The run during which RSS was sampled: forkyard peaked at 25 MB in one process, the 100 Anvils at 3.3 GB combined. |
| `churn_A1_longlived_1x20.csv` | `--episodes 1 --actions-per-agent 20` | 1, 5, 10, 25 | One long-lived environment per agent. |
| `churn_A2_disposable_10x2.csv` | `--episodes 10 --actions-per-agent 2` | 1, 5, 10, 25 | The same 20 actions through ten disposable forks. Paired A/B with the row above. |
| `upstream_calls.csv` | 5 actions, `--count-upstream` | 1, 5, 10, 25, 50 | JSON-RPC calls that reached the provider, per combination. |
| `upstream_timings_via_proxy.csv` | same run | 1, 5, 10, 25, 50 | Timings from that proxied run — inflated by the proxy hop and **not** the source of any timing in the README. |
| `overlap_shared_calls.csv` / `overlap_shared_timed.csv` | `--state-overlap shared`, 8 reads | 1, 5, 10, 25, 50 | Every agent reads the same 8 Uniswap V2 pairs. |
| `overlap_disjoint_calls.csv` / `overlap_disjoint_timed.csv` | `--state-overlap disjoint`, 8 reads | 1, 5, 10, 25, 50 | Each agent reads its own 8 pairs — the control. |
| `publicnode_ratelimited.csv` / `_calls.csv` | 5 actions, `--count-upstream` | 1, 5, 10, 25 | `https://ethereum-rpc.publicnode.com`, pinned near the chain tip (block 25906xxx) because a full node won't serve deep historical state. Anvil's action success falls to 26.4% at 25 agents. |
| `anvilcache_C1_cold.csv` | `--state-overlap shared`, `--anvil-rpc-cache` | 10 | Anvil's cache **enabled**, `~/.foundry/cache/rpc/mainnet/25795072` deleted first. |
| `anvilcache_C2_warm.csv` | same | 10 | Immediately after C1, so Anvil's cache is populated — the run where Anvil (30 calls) beats forkyard (37). |
| `anvilcache_C3_disabled.csv` | `--state-overlap shared` | 10 | `--no-storage-caching`, the default everywhere else. |

Column meanings are in [`../../README.md`](../../README.md#csv-columns); the
`--count-upstream` files use the [upstream schema](../../README.md).
Regenerate any of these with the `run_benchmark.py` invocations shown in the
root README.
