# Benchmarks

[Anvil](https://book.getfoundry.sh/anvil/) is the standard tool for forked-state
simulation, and a very good one. It is what forkyard measures itself against
because it is what everyone already reaches for, and for a single agent it stays
the simpler choice — one command, the full Ethereum JSON-RPC surface, a mature
set of cheatcodes. Nothing here is an argument to stop using it.

What these benchmarks look for is narrower: forkyard and Anvil make a different
choice about the *unit of isolation*. Anvil's is an OS process, with its own
cache and its own copy of state; forkyard's is a session inside one process,
sharing one warm cache. That difference should be invisible for one agent and
should start to matter as agents multiply. These measurements are an attempt to
find out where, and by how much — including the places where the process model
is the better one.

Measured 2026-09-04/05 on an Apple M3 Pro (12 cores, 38 GB), macOS, against a
Tenderly mainnet archive gateway, block 25795072 unless stated. Result CSVs are
not tracked in git; [Reproducing](#reproducing) regenerates every one of them.

**Read [Measurement variance](#measurement-variance) before quoting a wall-clock
number from this page.** Counts and footprints here are reproducible; elapsed
times on this hardware were not.

## Contents

- [Methodology](#methodology)
- [Reproducing](#reproducing)
- [Structural results](#structural-results) — the reproducible ones
  - [Upstream RPC load](#upstream-rpc-load)
  - [Memory per isolated agent](#memory-per-isolated-agent)
  - [Acquiring an environment](#acquiring-an-environment)
  - [Checkpointing and branching](#checkpointing-and-branching)
  - [Restart cost](#restart-cost)
  - [Many blocks in one process](#many-blocks-in-one-process)
- [Timing results](#timing-results) — directional only
- [Where Anvil is the better tool](#where-anvil-is-the-better-tool)
- [Measurement variance](#measurement-variance)

## Methodology

**The workload.** Each simulated agent acquires its own environment (forkyard:
`POST /session`; Anvil: spawn a process and poll until it answers
`eth_blockNumber`), runs a randomised sequence of actions — `set_balance`, then
transfers, balance reads, ERC-20 funding by raw slot write, approvals and real
Uniswap V2 swaps — then discards it. Every state-changing action is a full
signed transaction waited to receipt, counted as failed unless the receipt
returns status 1. All N agents run concurrently in a thread pool.

**What the timer covers**, per agent: environment acquisition + actions +
teardown. It excludes forkyard's one-time process startup — a single shared cost
with no Anvil counterpart — while Anvil's per-agent spawn is inside the timer,
because Anvil pays it once per agent.

**Both tools have a persistent cache, and both are disabled by default here.**

- Foundry writes fetched fork state to
  `~/.foundry/cache/rpc/<chain>/<block>/storage.json` and reuses it across
  processes and runs — a genuinely useful feature. Left enabled, whole sweeps
  ran in which Anvil made *zero* upstream state calls and still answered every
  read, which measures previous sweeps rather than either architecture. Anvil
  therefore spawns with `--no-storage-caching`; `--anvil-rpc-cache` restores it.
- forkyard now persists its own cache per `(chain, block)`
  (`FORKYARD_CACHE_DIR`), so every run sets `FORKYARD_CACHE_DISABLED=1` for
  symmetry.

The exception is [Restart cost](#restart-cost), where both are switched **on**,
because they are the subject.

**One benchmark at a time.** A second benchmark sharing CPU, ports or RPC quota
corrupts the first. The whole pass is serial.

**forkyard runs on its shipped defaults**, in particular
`FORKYARD_NUM_WORKERS=4` — the thread pool sessions are sharded over. That
default is the concurrency ceiling these numbers run into: at 100 agents the
same sweep took 13.1s at 4 workers and 5.5s at 12 on a 12-core machine, and
session opens degrade from ~4ms to several hundred as agents pile up. Raising
it is the first thing to try before concluding anything about forkyard under
load; the tables here deliberately do not, because the default is what a user
gets.

**A bug that invalidated earlier numbers.** Until commit `874dfd2`,
`SharedBackend` was spawned with `pin_block: None`, so `FORKYARD_FORK_BLOCK_NUMBER`
labelled a fork without pinning its reads: forkyard read `latest` while Anvil
read the pinned block. Everything here is from the fixed binary.

## Reproducing

```bash
cargo build -p forkyard --release && export PATH="$PWD/target/release:$PATH"
cd python/benchmarks && uv sync
export RPC_URL=...                 # an archive endpoint, for historical blocks
export FORKYARD_CACHE_DISABLED=1   # except for `bench.py warmstart`
```

| Section | Command |
| --- | --- |
| Standard workload | `uv run python run_benchmark.py --agents 1,10,50,100 --block-heights 25795072 --actions-per-agent 5 --rpc-url $RPC_URL --out core.csv` |
| Long-lived vs churn | as above with `--actions-per-agent 20 --episodes 1`, then `--actions-per-agent 2 --episodes 10` |
| Upstream load | add `--count-upstream`; then `uv run python cost_model.py core.upstream.csv` |
| State sharing | `--actions-per-agent 8 --state-overlap shared --count-upstream`, then `--state-overlap disjoint` |
| Branching | `uv run python bench.py branching --branches 2,4,8,16,32 --prefix-actions 5 --branch-actions 3 --no-proxy --rpc-url $RPC_URL --out branching.csv` (drop `--no-proxy` for call counts) |
| Checkpoint | `uv run python bench.py checkpoint --state-sizes 100,1000,10000 --repeats 3 --rpc-url $RPC_URL --out checkpoint.csv` |
| Memory | `uv run python bench.py writers --writers 1,5,10,25,50 --rounds 10 --rpc-url $RPC_URL --out writers.csv` |
| Arrivals | `uv run python bench.py arrivals --arrival-rates 1,5,20 --duration 20 --rpc-url $RPC_URL --out arrivals.csv` |
| Freshness | `uv run python bench.py freshness --agents 5,25 --duration 120 --refresh-secs 30 --poll-secs 4 --anvil-base-port 21000 --rpc-url $RPC_URL --out freshness.csv` |
| Quota | `uv run python bench.py quota --quotas 10,50 --agents 5,25 --rpc-url $RPC_URL --out quota.csv` (add `--limit-mode reject --burst 200`) |
| Restart | `uv run python bench.py warmstart --agents 5 --contracts 8 --rpc-url $RPC_URL --out warmstart.csv` |
| Many blocks | `uv run python bench.py blocks --agents 24 --blocks 1,2,4,8 --base-block 25795072 --block-stride 1000 --rounds 2 --rpc-url $RPC_URL --out blocks.csv` |

Each command writes to `--out`; some also write a `.summary.csv` sibling, and
`--count-upstream` adds a `.upstream.csv`. Column meanings are in
`python/benchmarks/README.md`.

Results are not committed — `python/benchmarks/results/` is gitignored, so the
numbers on this page are reproduced by re-running the table above rather than
read out of the repo. Run one at a time: a second benchmark sharing CPU, ports
or RPC quota corrupts the first.

## Structural results

These are counts and footprints rather than elapsed times, and they reproduced
across independent runs — forkyard's call counts were byte-identical between
passes.

### Upstream RPC load

Standard workload, cold on both sides. Two independent runs shown where they
differ:

| Agents | forkyard calls | anvil calls | forkyard per agent | anvil per agent |
| --- | --- | --- | --- | --- |
| 1 | 33 | 43 – 97 | 33.0 | 43 – 97 |
| 10 | 108 | 422 – 834 | 10.8 | 42 – 83 |
| 50 | **387** | 1,321 – 3,062 | **7.7** | 26 – 61 |

forkyard's per-agent cost *falls* as agents are added, because they share one
cache; a process-per-agent design pays for its own fork every time. Note
Anvil's spread — its fetch volume varied more than 2× between runs, so treat
the ratio as "several times", not a precise multiple.

The clearest version isolates sharing from everything else. A read-only
workload where every agent reads the **same** 8 contracts, against one where
each reads its **own** 8:

| Agents | Shared: forkyard | Shared: anvil | Disjoint: forkyard | Disjoint: anvil |
| --- | --- | --- | --- | --- |
| 1 | 37 | 76 – 78 | 37 | 78 |
| 10 | **37** | 253 – 776 | 325 | 687 – 776 |
| 50 | **37** | 1,203 – 2,125 | 1,605 | 3,395 |

On shared state forkyard's upstream traffic is **flat** — 37 calls whether one
agent reads those contracts or fifty, identical across both runs. On disjoint
state, where nothing *can* be shared, the advantage narrows to roughly 2×, which
is per-fork overhead rather than caching. That second column is the control: it
shows the first column is measuring what it claims to.

Priced with Alchemy's published compute-unit table at $0.45/million CU, the
standard workload came to **$0.08 per 1,000 agent runs against $0.58**. True but
minor — cost only becomes an argument at 10^5–10^6 runs; the quota ceiling
matters more.

### Memory per isolated agent

Every writer writes a value only it uses, to the same account every other writer
targets, then reads it back. `isolation_violations` was 0 on every row, so these
are genuinely isolated agents.

| Concurrent writers | forkyard RSS | anvil RSS | forkyard per GB | anvil per GB |
| --- | --- | --- | --- | --- |
| 1 | 16.8 MB | 32.8 MB | 61 | 31 |
| 10 | 17.4 MB | 325 MB | 588 | 32 |
| 25 | 18.0 MB | 794 MB | 1,425 | 32 |
| 50 | **18.9 MB** | 1,546 MB | **2,708** | 33 |

forkyard's footprint moves 16.8 → 18.9 MB going from 1 to 50 concurrent
writers; Anvil's is linear at ~31 MB each, which is simply what a process costs.
The 24-agent multi-block sweep agrees independently: 15–19 MB against 610–770 MB.

### Acquiring an environment

| Concurrent agents | forkyard `POST /session` | anvil spawn → ready |
| --- | --- | --- |
| 1 | 3 – 5 ms | ~840 – 870 ms |
| 10 | 13 – 18 ms | ~860 – 875 ms |
| 25 | 16 – 153 ms | ~840 – 1,055 ms |
| 50 | 213 – 383 ms | 1,055 – 2,546 ms |
| 100 | 563 – 802 ms | 2,539 – 20,090 ms |

Under no contention the gap is about 200×. It narrows as concurrency rises,
because forkyard's session opens start queueing behind its worker pool — see
[Where Anvil is the better tool](#where-anvil-is-the-better-tool).

### Checkpointing and branching

**Anvil's `evm_snapshot`/`evm_revert` are excellent and this benchmark says so**:
~1 ms flat regardless of how much state is dirty. If you need to rewind one
timeline, that is the right primitive and forkyard offers nothing better.

What scales with state is the serializing path, `anvil_dumpState`/`loadState`:

| Dirty slots | anvil dump | anvil load | blob size | forkyard fork | forkyard discard |
| --- | --- | --- | --- | --- | --- |
| 100 | 1.7 ms | 1.3 ms | 3.3 KB | 0.6 ms | 1.0 ms |
| 1,000 | 2.1 ms | 2.2 ms | 8.4 KB | 0.7 ms | 1.0 ms |
| 10,000 | 5.8 ms | 6.2 ms | 56 KB | **0.6 ms** | 0.9 ms |

These are not the same operation — forkyard branches off a shared base and never
carries the writes, Anvil's dump does — so compare the *shape* of the columns,
flat against growing, not the absolute milliseconds.

Where the two models genuinely diverge is **K branches live at once**. Anvil's
snapshot stack is a single timeline: you explore a branch, revert, explore the
next. forkyard's `forkyard_forkFrom` hands each child the parent's state as a
pointer, and all K run concurrently. Creating one branch:

| K | forkyard `forkFrom` | anvil snapshot+revert | anvil spawn + replay prefix |
| --- | --- | --- | --- |
| 2 | 2.1 ms | 5.2 ms | 4,990 ms |
| 32 | **0.9 ms** | 4.5 ms | 6,561 ms |

Upstream calls for the whole K=32 sweep: forkyard **129**, anvil-snapshot 348,
anvil-processes 1,310. Zero isolation violations — every child's diverging write
stayed invisible to its siblings and to its parent.

### Restart cost

The one axis where Anvil was clearly ahead, and the reason forkyard now persists
its cache too. **Both caches enabled** — the only section where that is true.
5 agents, 8 contracts:

| Backend | Cold calls | Warm calls | Cold time | Warm time |
| --- | --- | --- | --- | --- |
| forkyard | 37 | **1** | 3.13 s | **0.14 s** |
| anvil | 90 | 15 | 9.24 s | 3.42 s |

forkyard's warm floor is one call — the fork's own block-header lookup. Anvil's
is 15, because each process re-resolves state that forkyard serves once from a
shared base. Before this feature the same measurement had forkyard refetching
everything on every start, and Anvil comfortably ahead.

### Many blocks in one process

`POST /session {"block_number": N}` pins a session to its own block with one
shared cache per block. Anvil's `--fork-block-number` is per process, so B
blocks means at least B processes — and since a process is Anvil's unit of
isolation, N isolated agents means N processes. 24 agents over B blocks, twice:

| B | forkyard calls (r1 / r2) | anvil calls (r1 / r2) | forkyard RSS | anvil RSS |
| --- | --- | --- | --- | --- |
| 1 | 20 / **0** | 861 / 1,236 | 15.5 MB | 610 MB |
| 2 | 40 / **0** | 1,462 / 1,463 | 16.2 MB | 675 MB |
| 4 | 80 / **0** | 1,459 / 1,399 | 17.1 MB | 770 MB |
| 8 | 160 / **0** | 1,438 / 1,228 | 18.5 MB | 730 MB |

forkyard's cost scales with **B, not N** — 20 calls per block regardless of how
many agents use it — and the second round costs nothing, because the per-block
bases are already warm in-process.

## Timing results

Directional only. Two independent passes of the same sweeps disagreed by up to
7× on this machine; see [Measurement variance](#measurement-variance).

**Standard workload, whole-sweep wall clock (range across two runs):**

| Agents | forkyard | anvil |
| --- | --- | --- |
| 1 | 2.7 – 2.9 s | 4.9 – 6.2 s |
| 10 | 3.6 – 3.8 s | 4.9 – 5.3 s |
| 50 | 7.6 – 8.4 s | 8.5 – 11.9 s |
| 100 | 12.8 – 13.7 s | 12.1 – 62.3 s |

**Branching, whole sweep** (one clean run): at K=32, forkyard **4.72 s**,
anvil-processes 9.81 s, anvil-snapshot 30.27 s — the last growing linearly in K
because it explores one branch at a time.

**Arrivals** (Poisson, time from scheduled arrival to first successful
simulation, clean run):

| λ (agents/s) | forkyard p50 | anvil p50 | forkyard failures | anvil failures |
| --- | --- | --- | --- | --- |
| 1 | 330 ms | 1,780 ms | 0 / 17 | 0 / 17 |
| 5 | 325 ms | 1,988 ms | 6 / 84 | 11 / 84 |
| 20 | 5,857 ms | 14,291 ms | 0 / 372 | 59 / 372 |

An earlier pass — since found to have been contaminated by leaked processes —
had forkyard *slower* at λ=20. The clean run reverses that, which is exactly why
the table above is labelled directional.

**Staying at the chain tip**, 5 agents refreshing every 30 s for two minutes:
forkyard refreshed in ~17–23 ms using 14–29 upstream calls total; Anvil in
~1,110 ms using 859. Both stayed current. At 25 agents forkyard used 28 calls;
the Anvil leg could not be measured — 25 tip-forked instances would not all
start on this machine, and in a later attempt even 5 would not.

**Under a provider quota**, forkyard's sustainable agent count was consistently
at or above Anvil's (Anvil: 0–5 across every configuration), but forkyard's own
ceiling moved between runs (5–25 agents), so no specific number is quotable yet.

## Where Anvil is the better tool

**Rewinding one timeline.** `evm_snapshot`/`evm_revert` cost ~1 ms flat no
matter how much state is dirty. forkyard has no equivalent primitive, and for
"try this, undo it, try the next" in a single agent, Anvil's is simply the right
design.

**Per-interaction latency at high concurrency.** forkyard shards sessions over
`FORKYARD_NUM_WORKERS` threads — 4 by default — and that queue becomes the
ceiling. Past a few tens of concurrent agents Anvil's per-interaction latency is
better, and forkyard's session opens degrade from ~4 ms to several hundred.
Raising the worker count helps materially (100 agents: 4 workers → 13.1 s,
12 workers → 5.5 s) but does not change the shape.

**Cold, unshared state.** When agents touch disjoint state, the shared cache has
nothing to share and forkyard is left with its worker queue. This is the regime
where it does worst.

**Maturity and surface area.** Anvil implements the whole Ethereum JSON-RPC
surface plus a large, well-documented cheatcode set, is battle-tested, and
integrates with the rest of Foundry. forkyard's HTTP surface is deliberately
small — no `eth_call`, no `eth_getCode`, no `eth_getStorageAt` (reads go through
`eth_estimateGas`), which several benchmarks here had to be written around.

**Anything with one agent.** Every advantage measured on this page begins at
"more than one". For a single agent, Anvil is one command and no new concepts.

## Measurement variance

Two independent passes of the same sweeps, on the same machine and endpoint,
disagreed substantially:

- forkyard's **upstream call counts were byte-identical** between passes
  (33/108/387; 37/325/1,605 in the overlap tests).
- Anvil's call counts varied by **more than 2×** (e.g. 1,321 vs 3,062 at 50
  agents), presumably from spawn-order and retry differences.
- **Wall clocks varied by up to 7×** on the heavier legs (forkyard's 25-agent
  long-lived sweep: 9.2 s in one pass, 69.2 s in another).

Two causes were identified and one was fixed. A crash while spawning the 25th
tip-forked Anvil leaked the other 24, which then sat resident under every later
benchmark for hours — fixed in `da5ce48`, with a regression test. The second is
environmental: the host carried a load average of 4–5 from unrelated work during
the second pass, and these benchmarks are sensitive to that.

Consequently: the counts and footprints above are safe to quote; the elapsed
times are not, until they have been repeated several times on an idle machine.
Anything published as a headline number should come from a repeated run with a
reported spread, not from a single pass.
