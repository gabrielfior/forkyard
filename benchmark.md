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

Measured 2026-09-05 on an Apple M3 Pro (12 cores, 38 GB), macOS, against a
Tenderly mainnet archive gateway, block 25795072 unless stated. Result CSVs are
not tracked in git; [Reproducing](#reproducing) regenerates every one of them.

Every number is the **median of five runs**, each sweep preceded by a discarded
warm-up run, with the max/min spread reported so you can see how stable it is.
Both tools run with their persistent caches **enabled** — warm against warm,
which is the comparison a returning user actually gets. The host was not idle
(it runs a desktop; load average 1.7–9.5 across the campaign, recorded per
repetition), which is part of why the spread column is here.

## Contents

- [Methodology](#methodology)
- [Reproducing](#reproducing)
- [Results](#results)
  - [Upstream RPC load](#upstream-rpc-load)
  - [Memory per isolated agent](#memory-per-isolated-agent)
  - [Acquiring an environment](#acquiring-an-environment)
  - [Branching: K what-ifs from one state](#branching-k-what-ifs-from-one-state)
  - [Checkpoint cost](#checkpoint-cost)
  - [Many blocks in one process](#many-blocks-in-one-process)
  - [Restart cost](#restart-cost)
  - [Whole-workload wall clock](#whole-workload-wall-clock)
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

**Both tools keep a persistent cache, and both are enabled.** Foundry writes
fetched fork state to `~/.foundry/cache/rpc/<chain>/<block>/storage.json`;
forkyard writes its own per `(chain, block)` under `FORKYARD_CACHE_DIR`. Both
survive restarts, so warm-against-warm is the like-for-like comparison and the
one a returning user gets. `--cold-caches` turns both off to measure a
first-ever run at a block instead.

This matters enough to state plainly: warm, Anvil serves most of its state from
disk, and the upstream-traffic gap that dominates a cold comparison largely
closes. Every sweep here is therefore preceded by a discarded warm-up run, so
no reported number is secretly a cold one.

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
export RPC_URL=...    # an archive endpoint, for historical blocks
```

Run each sweep once and discard it before measuring: the first run at a block
fills both caches, so including it reports a cold number. `aggregate_runs.py`
takes the median and spread over repeated runs and skips the warm-up for you.

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

## Results

Medians of five warm runs. **Spread** is max/min across those five: 1.0 means
every run agreed, and anything above ~1.5 is a number to treat as approximate.

### Upstream RPC load

The standard workload, both caches warm:

| Agents | forkyard calls | anvil calls | forkyard per agent | anvil per agent |
| --- | --- | --- | --- | --- |
| 1 | 9 | 13 | 9.0 | 13.0 |
| 10 | **84** | 142 | 8.4 | 14.2 |
| 50 | **363** | 666 | **7.3** | 13.3 |

forkyard's counts were *identical in all five runs* at every tier; Anvil's
varied (13 → 96 at one agent, 656 → 785 at fifty) as its cache filled unevenly
across processes.

The version that isolates sharing from everything else: a read-only workload
where every agent reads the **same** 8 contracts, against one where each agent
reads its **own** 8.

| Agents | Shared: forkyard | Shared: anvil | Disjoint: forkyard | Disjoint: anvil |
| --- | --- | --- | --- | --- |
| 1 | **1** | 3 | 1 | 58 |
| 10 | **1** | 30 | 1 | 327 |
| 50 | **1** | 300 | 1 | 1,734 |

Warm and sharing state, forkyard needs **one upstream call at any agent count**
— the fork's own block-header lookup — because the contracts are already in the
base every session reads from. Anvil, whose cache is per process, still pays
about six calls per agent.

**The disjoint column stops being a control once caches are warm**, and it is
worth saying why rather than quietly dropping it. Cold, it separates the two
things forkyard's cache does: *sharing* one copy between concurrent sessions,
and *persisting* it across runs. Warm, persistence alone answers the disjoint
reads too — a previous run already fetched those contracts — so forkyard reports
1 either way and the column no longer isolates anything. Cold, the same control
gives 37 shared against 1,605 disjoint for forkyard, which is where the claim
that sharing (not just persistence) is doing work actually comes from. Run
`--cold-caches` to reproduce that half.

Anvil's disjoint number is high for a warm run because 50 processes exiting at
once each write the same per-block cache file, so it ends up holding only part
of what was fetched — a per-process cache paying for a shared workload twice.

Priced with Alchemy's published compute-unit table at $0.45/million CU this is
cents per thousand agent runs either way. The cost argument only matters at
10^5–10^6 runs; the quota ceiling is the useful version of it.

### Memory per isolated agent

Every writer writes a value only it uses, to the same account every other writer
targets, then reads it back. Zero isolation violations across all five runs, so
these are genuinely isolated agents.

| Concurrent writers | forkyard RSS | anvil RSS | forkyard per GB | anvil per GB |
| --- | --- | --- | --- | --- |
| 1 | 21.2 MB | 30.7 MB | 48 | 33 |
| 10 | 21.5 MB | 292 MB | 476 | 35 |
| 50 | **23.2 MB** | 1,434 MB | **2,211** | 36 |

forkyard's footprint moves 21.2 → 23.2 MB going from 1 to 50 concurrent
writers. Anvil's is linear at ~29 MB each, which is what a process costs — its
own design decision, not a fault.

### Acquiring an environment

| Concurrent agents | forkyard `POST /session` | anvil spawn → ready |
| --- | --- | --- |
| 1 | **4.3 ms** | 627 ms |
| 10 | 16.2 ms | 662 ms |
| 50 | 215 ms | 686 ms |

Uncontended the gap is ~150×. It closes as concurrency rises, because forkyard's
session opens queue behind its four worker threads while Anvil's spawn cost
stays flat — the shape behind every high-concurrency result below.

### Branching: K what-ifs from one state

One prefix of 5 actions, then K branches of 3 actions each, run concurrently
where the architecture allows it. Whole-sweep seconds:

| K | forkyard | anvil-processes | anvil-snapshot |
| --- | --- | --- | --- |
| 2 | **0.08** | 0.73 | 0.72 |
| 8 | **0.18** | 1.97 | 10.67 |
| 32 | **0.54** | 2.06 | 9.87 |

Creating one branch: forkyard `forkyard_forkFrom` **0.7 ms**, Anvil
`evm_snapshot` + `evm_revert` 2.2 ms, spawning a process and replaying the
prefix 1,156 ms. The snapshot stack is fast per operation but serial by
construction — one branch at a time — which is what the K=8 and K=32 columns
show. Zero isolation violations: every child's diverging write stayed invisible
to its siblings and its parent.

### Checkpoint cost

**Anvil's `evm_snapshot`/`evm_revert` are excellent and this says so**: about a
millisecond flat, regardless of how much state is dirty. To rewind a single
timeline that is the right primitive, and forkyard has nothing better.

What grows with state is the serializing path, `anvil_dumpState`/`loadState`:

| Dirty slots | anvil dump | anvil load | blob | forkyard fork | anvil snapshot/revert |
| --- | --- | --- | --- | --- | --- |
| 100 | 1.2 ms | 1.1 ms | 3.3 KB | 0.7 ms | 0.8 / 1.1 ms |
| 1,000 | 2.0 ms | 2.0 ms | 8.4 KB | 0.7 ms | 1.0 / 1.2 ms |
| 10,000 | 5.9 ms | 6.7 ms | 56 KB | **0.7 ms** | 0.8 / 1.0 ms |

Dump and load grow with dirty state; snapshot, revert and forkyard's branch do
not. The branch and the dump are not the same operation — forkyard's branch
never carries the writes — so compare the shape of each column, flat against
growing, rather than the milliseconds.

### Many blocks in one process

`POST /session {"block_number": N}` pins a session to its own block with one
shared cache per block; Anvil's `--fork-block-number` is per process. Twelve
agents spread over B blocks, run twice:

| B | forkyard calls (r1 / r2) | anvil calls (r1 / r2) | forkyard RSS | anvil RSS |
| --- | --- | --- | --- | --- |
| 1 | 20 / **0** | 36 / 36 | 15.3 MB | 271 MB |
| 4 | 80 / **0** | 36 / 36 | 16.6 MB | 321 MB |

forkyard's cost scales with the number of *blocks*, not agents — 20 calls per
block — and the second round is free because those bases are already warm in
process. Anvil's is flat here because its own disk cache is warm too; what it
cannot amortise is memory, at roughly 25 MB per process against one 16 MB
process.

### Restart cost

Both persistent caches enabled, 5 agents reading 8 contracts, cold run then warm:

| Backend | Cold calls | Warm calls | Cold time | Warm time |
| --- | --- | --- | --- | --- |
| forkyard | 37 | **1** | 3.13 s | **0.14 s** |
| anvil | 90 | 15 | 9.24 s | 3.42 s |

forkyard's warm floor is one call, the fork's own block-header lookup. Anvil's
is 15, because each process re-resolves state that forkyard serves once from a
shared base. Before forkyard had a persistent cache at all, this was the one
axis where Anvil was clearly ahead.

### Whole-workload wall clock

The standard agent workload, warm, median of five:

| Agents | forkyard | spread | anvil | spread |
| --- | --- | --- | --- | --- |
| 1 | **0.62 s** | 1.09× | 1.51 s | 3.49× |
| 10 | **1.76 s** | 3.62× | 2.13 s | 1.10× |
| 50 | 6.34 s | 1.05× | **2.72 s** | 1.15× |

Ten disposable forks per agent instead of one long-lived environment, same total
work:

| Agents | forkyard | anvil |
| --- | --- | --- |
| 1 | **3.53 s** | 11.32 s |
| 10 | **9.18 s** | 13.57 s |

And agents arriving over time rather than all at once (p50 from scheduled
arrival to first successful simulation):

| Arrival rate | forkyard p50 | anvil p50 |
| --- | --- | --- |
| 1/s | **340 ms** | 1,082 ms |
| 5/s | **353 ms** | 1,092 ms |
| 20/s | 6,233 ms | **1,181 ms** |

The pattern across all three: forkyard is ahead while its worker pool is not the
constraint — single agents, churn, arrivals up to ~5/s — and behind once it is.
At 50 concurrent agents and at 20 arrivals/s, Anvil's flat per-process cost wins.

## Where Anvil is the better tool

**Concurrency past a few tens of agents.** This is the clearest one. forkyard
shards sessions over `FORKYARD_NUM_WORKERS` threads — 4 by default — and that
queue becomes the ceiling: at 50 concurrent agents the standard workload took
6.34 s against Anvil's 2.72 s, and at 20 arrivals/second forkyard's p50 was
6,233 ms against 1,181 ms. Anvil's per-process cost is flat, and warm it has no
fetch penalty left to pay. Raising the worker count helps a lot (100 agents:
13.1 s at 4 workers, 5.5 s at 12) but does not change the shape.

**Rewinding one timeline.** `evm_snapshot`/`evm_revert` cost about a millisecond
flat no matter how much state is dirty. forkyard has no equivalent primitive,
and for "try this, undo it, try the next" inside one agent, Anvil's design is
simply the right one.

**Unshared state.** When agents touch disjoint state the shared cache has
nothing to share, the upstream advantage falls to under 2×, and forkyard is left
carrying its worker queue.

**Maturity and surface area.** Anvil implements the whole Ethereum JSON-RPC
surface plus a large, documented cheatcode set, is battle-tested, and integrates
with the rest of Foundry. forkyard's HTTP surface is deliberately small — no
`eth_call`, no `eth_getCode`, no `eth_getStorageAt` (reads go through
`eth_estimateGas`), which several benchmarks here had to be written around.

**Anything with one agent.** Every advantage measured here begins at "more than
one". For a single agent Anvil is one command and no new concepts.

## Measurement variance

Timing benchmarks on a desktop are noisy, and earlier passes of this work were
noisy enough to reverse a conclusion. Two causes were found and fixed:

- A crash while spawning the 25th tip-forked Anvil leaked the other 24, which
  then sat resident under every later benchmark for hours (fixed in `da5ce48`,
  with a regression test).
- Consolidating the benchmark scripts renamed the *string* `"FORKYARD_PORT"`
  along with the constant of that name, so five sweeps told forkyard nothing
  about its port, it fell back to its default, and collided with whatever was
  already listening (fixed in `6ff71e3`, with a regression test).

What remains is the host itself: it runs a desktop, and load average moved
between 1.7 and 9.5 during the campaign. That is why every number here is a
median of five runs with the max/min spread beside it. Most spreads are between
1.0 and 1.4; two rows reach ~3.5× on the strength of a single slow repetition,
and are marked. Counts are steadier than times — forkyard's upstream call counts
were identical across all five runs at every tier, while Anvil's varied by up to
7× at one agent as its cache filled unevenly.

Reproduce with `aggregate_runs.py`, which takes the median and spread across
`<name>_rep*.csv` and excludes the warm-up run.
