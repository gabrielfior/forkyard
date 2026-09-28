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
- [Latency pass (2026-09-26)](#latency-pass-2026-09-26)
- [Fifty agents under a second (2026-09-27)](#fifty-agents-under-a-second-2026-09-27)
- [One warm HTTP/2 connection upstream (2026-09-27)](#one-warm-http2-connection-upstream-2026-09-27)
- [100 and 1,000 agents (2026-09-28)](#100-and-1000-agents-2026-09-28)
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
| Startup | `uv run python bench.py startup --runs 7 --rpc-url $RPC_URL --out startup.csv`; add `--binary <path> --label <name>` to compare two builds |
| Standard workload, Rust client | `cargo run --release -p forkyard-loadgen -- --agents 1,10,50 --block-heights 25795072 --rpc-url $RPC_URL --out rs_rep1.csv` (from the repo root; `--backends forkyard` or `anvil` for one side, `--forkyard-bin` for a specific build), then `uv run python aggregate_runs.py <dir> --name rs` |
| Resume | `uv run python bench.py resume --prefix-actions 5,20,50 --repeats 5 --rpc-url $RPC_URL --out resume.csv` |
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
timeline that is the right primitive, and forkyard's own — `forkyard_snapshot`
then `POST /session {"snapshot_id"}`, below — is slower, because it writes a
file rather than keeping a checkpoint in memory.

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

`forkyard_snapshot` *is* the same operation as a dump: it carries the writes,
and it grows with them. Re-run 2026-09-26 (median of five, same host, load
average ~10), both tools in one pass:

| Dirty slots | forkyard snapshot | forkyard resume | snapshot file | anvil dump | anvil load | blob | anvil snapshot / revert |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 100 | 3.3 ms | 1.7 ms | 6.4 KB | 0.8 ms | 0.7 ms | 3.3 KB | 0.6 / 0.6 ms |
| 1,000 | 4.6 ms | 1.9 ms | 63 KB | 2.2 ms | 2.8 ms | 8.4 KB | 1.2 / 1.4 ms |
| 10,000 | 9.5 ms | 6.9 ms | 642 KB | 6.2 ms | 8.3 ms | 56 KB | 1.4 / 1.3 ms |

Anvil wins this table. Its blob is compressed and forkyard's JSON is not (about
64 bytes a slot), and a forkyard snapshot is fsynced to disk before it answers —
most of its 3 ms floor. What that buys is in the next section: the snapshot
outlives the process, and any process sharing the directory can resume it by id.

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

## Latency pass (2026-09-26)

Three changes aimed at start time and per-call latency, each measured against
the build before it (`main` at `63957c6`) in the same session, on the same
Apple M3 Pro (12 cores), same Tenderly archive endpoint, block 25795072. The
host was busy — load average 8.3–10.8 throughout — so the spreads matter.

**1. A pinned warm start makes no upstream call.** The persisted cache now
records the block's header fields. With `FORKYARD_FORK_BLOCK_NUMBER` set, a warm
start reads the cache before touching the network and skips the header fetch
that used to be its one remaining upstream call. `bench.py startup`, spawn to
first usable session and to one contract read, seven warm runs after one cold:

| Build | Ready (median) | Range | First read | Upstream calls |
| --- | --- | --- | --- | --- |
| main | 208.0 ms | 192–257 ms | 214.4 ms | 1 |
| this branch | **11.5 ms** | 8.5–14.9 ms | **19.6 ms** | **0** |

Cold starts are unchanged (~0.9 s to ready, 21 upstream calls). Following the
tip instead of pinning still costs the one header fetch, since the block — and so
the cache file — isn't known until it's back.

Rewriting the cache in a binary format was the other half of the plan and was
dropped after measuring: loading the real 1.6 MB cache file for this block takes
**2.6 ms** as JSON (`cargo run --release -p forkyard-engine --example
bench_cache_load -- ~/.forkyard/cache 1 25795072`), about a quarter of the new
warm start, and not worth a format migration.

**2. Sessions save to disk and resume in under a millisecond.**
`forkyard_snapshot` / `POST /session {"snapshot_id"}` (MCP: `snapshot` /
`resume`). `bench.py resume` builds a session with N real actions (funding, DAI
approvals, Uniswap V2 swaps, transfers — each a signed transaction waited to
receipt), then times getting back to that state three ways. Every resumed
session's marker balance is checked; all 45 resumes were correct.

| Actions N | Replay on a fresh session | Snapshot | Resume, same process | Resume after restart |
| --- | --- | --- | --- | --- |
| 5 | 23.0 ms | 4.6 ms | 0.9 ms | 0.9 ms |
| 20 | 87.8 ms | 5.2 ms | 1.0 ms | 0.8 ms |
| 50 | 210.7 ms | 4.2 ms | **0.9 ms** | **0.8 ms** |

Median of five; replay is timed with the shared cache already warm, the fairest
case for it. Resume is flat in N because the snapshot is flat in N: ~90 KB here,
almost all of it bytecode of the contracts the session touched, not the
transactions.

**3. Concurrency: workers default to one per core, and repeat reads skip the
fetch thread.** `FORKYARD_NUM_WORKERS` now defaults to the core count instead
of 4. Separately, every read that missed a session's own state went to
`foundry-fork-db`'s single backend thread, *cache hits included*: one request
and one reply per read, with every session on every worker queued on that one
thread. A shared concurrent read cache now answers repeats on the calling
thread. `cargo run --release -p forkyard-fetch --example bench_read_through`,
warm reads with no network:

| Threads | Through the backend thread | Read-through | |
| --- | --- | --- | --- |
| 1 | 5,854 ns | 28 ns | 209× |
| 4 | 2,252 ns | 29 ns | 79× |
| 12 | 1,424 ns | 52 ns | 28× |

End to end, the standard workload (`run_benchmark.py --agents 1,10,50
--actions-per-agent 5`), median of five after a discarded warm-up, each build
and Anvil in the same pass:

| Agents | forkyard main | forkyard this branch | spread | anvil |
| --- | --- | --- | --- | --- |
| 1 | 0.63 s | 0.72 s | 1.05× | 1.65 s |
| 10 | 2.06 s | **1.27 s** | 1.31× | 2.27 s |
| 50 | 7.67 s | **3.72 s** | 1.22× | 3.92 s |

At 50 agents forkyard went from about half Anvil's speed to level with it. The
1-agent row did *not* improve and reads slightly worse. That gap held in six more
interleaved runs (0.66 s against 0.72 s, ranges 0.63–0.82 and 0.62–0.86), and
it sits in `set_balance` and `approve`: those spend most of their time on the
first upstream fetch of each agent's fresh random addresses, which this change
doesn't touch. The best reading is round-trip noise to the endpoint, not a
regression, but it has not been ruled out.

## Fifty agents under a second (2026-09-27)

The pass above left 50 agents at 3.7 s. Breaking that run down showed three
things in the way, and each got its own fix. Same host and endpoint as above,
block 25795072, load average 2.8–4.8.

**What was in the way.** The workload's upstream traffic is almost all fresh
state. Every agent's signer and every transfer recipient is a brand-new random
address, and every `approve` writes a fresh allowance slot, so about three reads
per agent miss whatever the cache holds and take a ~160 ms Tenderly round trip.
That part is unavoidable, for Anvil too. What wasn't unavoidable:

1. **A worker waited on each of those reads, and so did every session behind
   it.** Giving the pool 64 threads, a crude test, took 50 agents from 3.1 s to
   1.8 s.
2. **Each miss was found, and paid for, one at a time.** revm stops at the first
   read it can't answer.
3. **The Python client was the ceiling.** It was busy 1.6 s of a 1.8 s run: one
   process, 50 threads, one core, and five HTTP calls per transaction.

**What changed.**

- **A worker never waits on the network.** A job runs *speculatively*: a read
  the shared cache can't answer is recorded, answered with a placeholder (no
  account, a zero word), and execution carries on. If anything was missing, the
  pass is thrown away, including anything it cached, and nothing is committed.
  The job is then parked and its keys fetched on another thread while the worker
  serves other sessions. When the keys arrive the job runs again. Jobs for one
  session keep their order. An upstream error, or 16 passes without converging,
  falls back to running the job blocking, so errors still surface as errors.
- **One round trip per pass, not per read.**
  - Because a pass carries on past a miss, it finds most of what it needs at
    once.
  - A transaction's sender and recipient are read before it runs, so they are
    found together even when validation would have stopped at the sender.
  - Everything a pass missed goes upstream as one JSON-RPC batch (balance,
    nonce and code per account, plus any storage slots), written into the fetch
    backend's own cache so it is persisted like any other read.
  - Debug logs of a 50-agent run: every resolve was **one key in one round**,
    median 168 ms. No pass was ever wasted on a key a previous pass could have
    found.
- **A client that doesn't measure itself.**
  - The HTTP surface now implements `eth_sendRawTransactionSync` (EIP-7966),
    which puts the receipt in the reply. Anvil implements it too, so the
    harness uses it on both backends.
  - Chain id and gas price are fetched once per client.
  - A transaction is now one round trip, where it was five.
  - `run_benchmark.py --client-processes N` spreads agents over N processes.
  - `forkyard-loadgen` is the same workload in async Rust, action for action.
    Its RNG is a bit-exact port of CPython's, so agent *i* makes the same
    choices in both harnesses. Same timed regions, same CSV. The one difference:
    it polls Anvil's readiness every 10 ms instead of every 200 ms, which only
    ever flatters Anvil.

**Step by step, 50 agents** (median of five unless noted):

| Build | Client | Wall clock | Client CPU |
| --- | --- | --- | --- |
| previous pass | Python, 1 process, old send path | 3.10 s (2.91–3.39) | 1.5 s |
| this change | Python, 1 process, old send path | **1.66 s** (1.58–1.72) | 1.4 s |
| this change | Python, 1 process, sync send | 1.48 s (median of 3) | 1.2 s |
| this change | Python, 12 processes, sync send | 1.13 s (0.98–1.27) | — |
| this change | **Rust** | **0.93 s** (0.92–1.00) | — |

The second row is the forkyard change on its own, measured with the client held
fixed: 1.9× faster. The rest is the client getting out of the way.

**The standard workload through the Rust client**, both backends in the same
pass, median of five after a discarded warm-up, 0 failed actions in any run:

| Agents | forkyard | spread | anvil | spread |
| --- | --- | --- | --- | --- |
| 1 | **0.54 s** | 1.09× | 1.37 s | 1.13× |
| 10 | **0.86 s** | 1.03× | 1.40 s | 1.01× |
| 50 | **0.93 s** | 1.09× | 1.72 s | 1.12× |

Four of the five 50-agent runs finished under a second; the fifth took 1.002 s.
Anvil is faster through this client too (it was 3.9 s through the Python one).
The client was holding both back.

**The floor.** What's left is about three sequential upstream round trips per
agent at ~160 ms each — fresh addresses and slots that no cache can have seen —
plus the agent's own work. A slower-than-median agent sets the wall clock. Below
this takes either fewer sequential cold reads per agent or a nearer upstream
(Tenderly's round trip is mostly its own processing: ~20–50 ms to connect,
~155 ms per request). Answering a fresh read *before* upstream confirms it would
be faster and is ruled out here, since a simulator that is usually right isn't
one.

**Tried and dropped**, each A/B'd twice at 50 agents:
- Coalescing misses from all sessions into one batch within a 2 ms window:
  1.12–1.15 s against 1.11–1.17 s. Tenderly serves 50 concurrent small batches
  over HTTP/2 as fast as one large one (~240 ms either way, measured with curl).
- Pre-warming the upstream connection at startup: 1.06–1.11 s against
  1.13–1.18 s. Within noise, and it would have put an upstream call back into the
  zero-call warm start. (Reinstated in a different form in the next section,
  once the Rust client showed what it was actually for.)

## One warm HTTP/2 connection upstream (2026-09-27)

In the Rust client's 50-agent runs, every agent's `set_balance` took ~100 ms
longer than any other miss, including its fastest runs (p10 257 ms against
153 ms). With hyper's connection logging on, the cause was plain: **51 TLS
connections to Tenderly in one run**, 50 of them dialed in the same
millisecond. The shipped binary spoke HTTP/1.1. alloy builds reqwest without
its `http2` feature, and only builds that also compiled the test and loadgen
crates turned it on. So each of 50 concurrent requests needed a connection of
its own.

Two changes:
- `forkyard-fetch` enables reqwest's `http2` feature.
- Every fork in the process shares one upstream client per URL, opened in the
  background at startup and kept open with an `eth_chainId` every 20 s.

After both, the same run dials **one** connection.

Interleaved A/B, 50 agents, Rust client, six pairs, 1.5 s between forkyard
answering and the agents starting, so the connection exists, as it does in any
process that has been up for a moment:

| Build | Wall clock (median) | Range | `set_balance` p50 / p90 | `transfer` p50 |
| --- | --- | --- | --- | --- |
| before | 0.973 s | 0.913–1.066 s | 274 / 385 ms | 161 ms |
| after | **0.881 s** | 0.849–1.069 s | **196 / 256 ms** | 166 ms |

The standard benchmark starts agents ~10 ms after forkyard answers. There the
change does nothing (six pairs: 1.023 s against 1.168 s, overlapping ranges
and one 2.2 s outlier), because the burst beats the background handshake and
dials its own connections anyway. Making those requests wait for the
connection being opened would be slower for that first burst, and holding
readiness until it is open would give back most of the 10 ms warm start. So the
fix is for a process that has been running a while, which is how forkyard is
deployed. The warm start is unchanged: ready in 10.2 ms, first read 14.2 ms.
Its upstream call count goes from 0 to 1, the background keep-alive, made after
the server is already serving.

`forkyard-loadgen --settle-ms N` reproduces the steady-state rows.

## 100 and 1,000 agents (2026-09-28)

The standard workload at larger scale, through the Rust client, three reps
after a discarded warm-up, forkyard at `9db76cb` unless stated.

| | Wall clock | Failed actions | Peak memory |
| --- | --- | --- | --- |
| forkyard, 100 agents | **1.02 s** (0.97–1.02) | 0 | **55 MB** |
| Anvil, 100 agents | 5.74 s (5.42–7.33) | 0 | 2.7 GB across 100 processes |
| forkyard, 1,000 agents | 7.68 s (7.41–9.86) | 108–196 a run | 178 MB |
| Anvil, 1,000 agents | not run | | |

Anvil wasn't run at 1,000. At the ~29 MB a process it measured at 50 agents,
it needs about 29 GB, and the host had 15 GB free. At 100 agents upstream reads
cost both tools the same ~170 ms. Anvil loses its time before any work starts:
spawning a process per agent took a median of 1.2 s, against 3 ms to open a
forkyard session.

**At 1,000 agents the ceiling is Tenderly's rate limit.** In one run 427
batches came back `-32005: rate limit exceeded`. The worker retried each job
blocking, that was refused too, and the error went back to the agent. So every
failure was a surfaced upstream error, not a wrong answer. And every one traced
back to one root: `set_balance` refused for 3–5% of agents, which were then
never funded and failed every transaction after ("sender holds 0 wei").

**Fix:** a rate-limited batch is now retried with jittered exponential backoff
(100 ms doubling to a 2 s cap, six retries, ~6 s in all), asking only for
what's still missing each time. A cap on batches in flight to one upstream
(`FORKYARD_UPSTREAM_MAX_IN_FLIGHT`) is there to queue runaway bursts. At
1,000 agents, one run per cap:

| Cap | 16 | 32 | 64 | 128 | 1,024 |
| --- | --- | --- | --- | --- | --- |
| Wall clock | 32.6 s | 16.4 s | 8.5 s | 6.6 s | 6.8 s |
| Failed actions | 0 | 0 | 0 | 0 | 0 |

The retries alone removed the failures. A tight cap only slows things down,
since it throttles harder than the provider does. So the default is a loose 256,
a safety valve rather than a limiter. Interleaved with the previous build,
three pairs each:

| Agents | before: wall, failed | after: wall, failed |
| --- | --- | --- |
| 50 | 3.39\*, 0.94, 1.09 s; 0 | 1.02, 0.94, 1.07 s; 0 |
| 100 | 1.14, 0.92, 1.12 s; 0 | 1.23, 1.87\*, 1.10 s; 0 |
| 1,000 | 10.32, 10.40, 10.82 s; **117–178** | **6.53, 6.57, 6.51 s; 0** |

\* In both outliers dozens of agents stall at the same instant, ~2.5 s before
and ~1 s after. The build without any retry shows the same shape, so it's the
upstream, not a retry. At 1,000 agents the 6.5 s is what Tenderly's limit
allows. Going faster there takes a higher quota, not a client change.

## Where Anvil is the better tool

**Concurrency past a few tens of agents.** This was the clearest one; since
[the non-blocking workers](#fifty-agents-under-a-second-2026-09-27) the
standard workload no longer shows it (50 agents: 0.93 s against 1.72 s), but
the history is worth keeping. forkyard shards sessions over `FORKYARD_NUM_WORKERS` threads, and
with the old default of 4 that queue was the ceiling: at 50 concurrent agents
the standard workload took 6.34 s against Anvil's 2.72 s, and at 20
arrivals/second forkyard's p50 was 6,233 ms against 1,181 ms. Since the
[latency pass](#latency-pass-2026-09-26) workers default to one per core, and 50
agents were level (3.72 s against 3.92 s). The arrivals sweep has not been re-run
since either change.

**Rewinding one timeline.** `evm_snapshot`/`evm_revert` cost about a millisecond
flat no matter how much state is dirty. forkyard's equivalent, snapshot then
resume, costs 5 ms at 100 dirty slots and 16 ms at 10,000, because it writes a
durable file. For "try this, undo it, try the next" inside one agent, Anvil's
design is still the faster one.

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
