# forkyard benchmarks

Compares forkyard (one process, N concurrent forked sessions) against
running one standalone Anvil instance per agent, across agent counts and
fork block heights. See `docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md`
in the repo root for the full design.

## Requirements

Both binaries must be **discoverable on `PATH`** — `run_benchmark.py`
spawns them by bare name (`forkyard`, `anvil`) and refuses to start if
either is missing:

```bash
# anvil: install Foundry (https://book.getfoundry.sh/getting-started/installation)
cargo build -p forkyard --release          # from the repo root
export PATH="$PWD/target/release:$PATH"    # building alone isn't enough
```

`--rpc-url` must serve historical state at the block heights you sweep
(`--fork-block-number` on Anvil's side); some public endpoints only do
that on a paid tier.

## Running

```bash
uv sync
uv run pytest                     # unit tests (no subprocesses/network)
uv run python run_benchmark.py --agents 1,2,5 --block-heights 20000000 --actions-per-agent 5 --rpc-url $RPC_URL --out results.csv
uv run python plot_results.py results.csv
```

Two flags change what the sweep is measuring:

- `--episodes N` (default 1) — how many acquire → act → discard cycles each
  agent runs. `--episodes 1 --actions-per-agent 20` is one long-lived
  environment; `--episodes 10 --actions-per-agent 2` is the same 20 actions
  through ten disposable forks, which is what exercises the difference
  between opening a session and spawning a process. Anvil gets its own
  window of `episodes` ports per agent, since a killed instance's port can
  linger in TIME_WAIT.
- `--state-overlap {shared,disjoint}` — replaces the transaction mix with a
  read-only workload over real Uniswap V2 pairs (sourced from the factory at
  setup time, straight from the endpoint so it lands in neither the timings
  nor the counts). `shared` gives every agent the same contracts, `disjoint`
  gives each its own. Run both: the first measures what a shared cache is
  worth, the second is the control that shows how much of the gap is *not*
  sharing.
- `--anvil-rpc-cache` — let Anvil use `~/.foundry/cache` (**off by default**).
  Foundry persists fetched fork state per resolved block and reuses it across
  processes and runs; forkyard has no cross-process cache, so leaving it on
  makes a sweep partly a measurement of earlier sweeps — measured directly:
  whole sweeps in which Anvil made zero upstream state calls and still
  answered every read. Turn it on only to measure that cache itself.
- `--count-upstream` — routes **both** backends through `rpc_proxy.py`, a
  local proxy that forwards every call unchanged and counts it, then writes
  per-combination totals to `<out>.upstream.csv`. This is what measures the
  shared-cache claim: whether N agents cost the provider N forks' worth of
  traffic. It adds a local hop to every call, so read the counts from such a
  run and the timings from a direct one.

Rows are flushed to the CSV after each (block height, agent count,
backend) combination, so an interrupted or failing sweep still leaves
everything collected up to that point.

## What "total simulation time" measures

For **both** backends the timer covers, per agent and concurrently across
agents: acquiring its own environment (forkyard — opening a session;
Anvil — spawning an instance and waiting until it is ready), running its
action sequence, and discarding that environment. It deliberately does
**not** include forkyard's one-time `forkyard` process startup, which is a
single shared cost with no Anvil counterpart — so the comparison is
per-agent-cost against per-agent-cost, not "warm sessions" against "cold
process starts".

## Output

With `--episodes N > 1` each agent contributes N of these blocks, so an
agent's rows run acquire, set_balance, actions…, discard, acquire, … .

`plot_results.py results.csv` writes two PNGs next to the CSV:

- **`results_total_time.png`** — total simulation time (ms) against
  concurrent agent count, one line per (backend, block height). This is
  the headline chart: how each model's cost scales as agents are added.
- **`results_action_latency.png`** — a grouped bar chart of median
  per-action latency by backend, so a difference in the total can be
  attributed to specific actions rather than just "it's faster".

### CSV columns

| Column | Meaning |
| --- | --- |
| `backend` | `forkyard` or `anvil`. |
| `block_height` | Fork block the run was pinned to. |
| `num_agents` | Agents running concurrently in that sweep. |
| `agent_id` | Which agent produced the row; `-1` on the synthetic total row. |
| `action` | Action label (`acquire`, `read_contract`, `set_balance`, `transfer`, `get_balance`, `fund_token`, `approve`, `swap_eth_for_token`, `swap_token_for_token`, `discard`), or `__total__` for the one whole-sweep row per combination. `acquire` is the environment acquisition itself — a forkyard session open, or an Anvil spawn plus wait-until-ready — and appears once per episode, first. A failed `acquire` is the entire episode: there is no environment to run actions in, and the agent moves on to its next episode. |
| `elapsed_ms` | Wall-clock duration of that action — or of the whole sweep on a `__total__` row. |
| `ok` | Whether it succeeded. On a `__total__` row, whether *every* action in that combination succeeded. |
| `error` | `repr()` of the exception (truncated to 200 chars) when `ok` is False, empty otherwise — this is what distinguishes a revert from a nonce rejection from an HTTP timeout. |

### `<out>.upstream.csv` columns (`--count-upstream` only)

One row per (backend, block height, agent count) combination.

| Column | Meaning |
| --- | --- |
| `backend`, `block_height`, `num_agents`, `episodes` | Which combination the row is for. |
| `http_requests` | HTTP requests that reached the proxy. |
| `jsonrpc_calls` | JSON-RPC calls inside them — a batch is one request and many calls, and providers bill the calls. |
| `calls_per_agent` | `jsonrpc_calls / num_agents`. The number the two architectures actually differ on: flat for a per-agent cache, falling for a shared one. |
| `upstream_errors` | Forwarded requests the upstream refused or dropped; the agent sees a JSON-RPC error and the sweep continues. |
| `top_methods` | JSON object of the five busiest methods and their counts. |

## Architecture benchmarks

Two standalone scripts alongside the main sweep. Each one isolates a single
architectural claim, writes its own CSV, and uses its own port range so it
can never be confused with `run_benchmark.py`'s processes (18555/18556 and
19000+). Both take `--rpc-url` (defaulting to `$RPC_URL`), `--block-height`
(default 25795072) and `--out`; run them **one at a time** — concurrent load
corrupts both the timings and the RSS samples.

### `bench_checkpoint.py` — checkpoint cost vs. state size

```bash
uv run python bench_checkpoint.py --state-sizes 100,1000,10000 --repeats 3 --rpc-url $RPC_URL --out checkpoint.csv
```

Dirties X storage slots on a real mainnet contract, then times each
backend's checkpoint. Anvil's is a serialization — `evm_snapshot`/
`evm_revert` in memory, `anvil_dumpState`/`anvil_loadState` into a blob whose
size is recorded. forkyard has **no snapshot RPC at all**, so its equivalent
is branching another session off the shared base (`POST /session`) and
discarding it.

Read that asymmetry honestly: forkyard's new session does *not* carry the X
writes — it branches from the base, not from the dirtied session — so it has
less to move by construction. That is the architectural difference (forkyard's
unit of work is "another fork of the same base", not "a copy of my current
state"), not a measurement trick, but the two `elapsed_ms` columns are not
the same operation. The sweep over `--state-sizes` is what makes it legible:
what matters is the *shape* of each curve, Anvil's bending upward with X and
forkyard's staying flat. Ports: forkyard 18600/18601, anvil 19200+.

| Column | Meaning |
| --- | --- |
| `backend` | `forkyard` or `anvil`. |
| `operation` | `snapshot`, `revert`, `dump`, `load` (Anvil) or `fork`, `discard` (forkyard). |
| `state_size` | Storage slots dirtied before the checkpoint ran. |
| `elapsed_ms` | Wall clock for that one operation. |
| `blob_bytes` | Bytes the operation moved: the `anvil_dumpState` blob, and the same blob on the matching `load`. **0 means no blob exists** — `evm_snapshot`/`evm_revert` keep state in memory and forkyard's fork/discard serializes nothing. |
| `ok` / `error` | Whether it succeeded, and `repr()` of the exception (truncated to 200 chars) if not. |

`--repeats N` (default 3) emits N rows per (backend, operation, state_size)
so a consumer can take a median instead of trusting one sample. Dirtying
10000 slots is one RPC call per slot on both backends and is the dominant
part of the runtime; it sits outside every timed region.

### `bench_writers.py` — isolated concurrent writers per GB

```bash
uv run python bench_writers.py --writers 1,5,10,25,50 --rounds 10 --rpc-url $RPC_URL --out writers.csv
```

K writers run concurrently — forkyard as K sessions in one process, Anvil as
K processes. Every writer writes a value only it uses to the **same** account
and the **same** storage slot as every other writer, then reads it back and
asserts it sees its own. That read-back is the isolation check: if the
environments leak, `isolation_violations` is non-zero and the memory number
means nothing, so such a row is reported `ok=False` however fast it was. The
assertion travels over `eth_getBalance` because forkyard's per-session RPC
has no `eth_getStorageAt`; the shared-slot write is part of the write load
but cannot itself be read back. RSS is sampled every 100 ms
(`--sample-interval`) by summing `ps` over every process of that name this
run started — pids already running are captured before the sweep and
excluded. Ports: forkyard 18610/18611, anvil 19300+ (never reused across
sweeps; a killed Anvil's port lingers in TIME_WAIT).

| Column | Meaning |
| --- | --- |
| `backend` | `forkyard` or `anvil`. |
| `writers` | Concurrent isolated writers in that sweep. |
| `peak_rss_mb` | Highest sampled sum of resident memory across this run's processes — for Anvil the moment all K coexisted, for forkyard the single process holding K sessions. |
| `wall_clock_ms` | The whole concurrent region, **including** each writer acquiring its environment (a session open, or an Anvil spawn plus wait-until-ready) — the same timed region `run_benchmark.py` uses. |
| `writes_per_sec` | Total writes (two per round: balance + storage) over that wall clock. At low `--rounds` this is mostly acquisition cost; raise `--rounds` to push it toward steady state. |
| `writers_per_gb` | `writers * 1024 / peak_rss_mb`. The headline number. `0.0` means RSS could not be sampled — not "infinitely many". |
| `isolation_violations` | Read-backs that returned someone else's (or a stale) value. **Must be 0.** |
| `ok` | No violations, and every writer completed. |

Read `writers_per_gb` off the *sweep*, not off one row. A single K conflates
forkyard's fixed cost (one process plus one shared base cache, tens of MB
before any session exists) with its marginal cost per session, so small K
understates it badly — at K=2 forkyard measured ~122 writers/GB against
Anvil's ~32, a 4x gap that should widen as K grows and the fixed cost
amortises. The slope across K is the number the claim is actually about. One
more caveat: `_wait_for_forkyard` probes readiness by opening a session it
never discards, so forkyard is holding K+1 sessions, not K.

## Arrival and freshness benchmarks

`run_benchmark.py` asks two questions that both assume a thundering herd at a
pinned block. These two scripts ask the other two.

### `bench_arrivals.py` — time-to-first-simulation

Agents do not all start at once; they trickle in. This script makes them
arrive as a Poisson process of rate λ and measures, per arrival, the
wall-clock from the **scheduled arrival instant** to the receipt of its first
transfer — acquire an environment, fund a signer, transact, discard.

```bash
uv run python bench_arrivals.py --arrival-rates 1,5,20 --duration 30 \
  --rpc-url $RPC_URL --out arrivals.csv
```

Measuring from the scheduled instant (not from when a worker thread got
around to it) is the whole design: an arrival that lands while the machine is
still spawning earlier Anvils pays for that backlog, and the backlog is the
point. Arrivals are dispatched by a scheduler thread that sleeps to each
instant and starts a thread there, never a bounded pool, which would hide it.
`--max-concurrent-envs` (default 64) is the one exception — a machine guard,
since λ=20 over 30s is ~600 arrivals and 600 simultaneous Anvils is tens of
GB — and the time it makes an arrival wait is deliberately counted *inside*
that arrival's latency. Past the cap, a row measures the cap.

Both backends replay the identical seeded schedule. Each Anvil arrival gets a
port that is never reused for the whole run (a killed instance's port lingers
in TIME_WAIT).

`arrivals.csv`: `backend`, `arrival_rate` (λ), `agent_id`, `arrival_s`
(scheduled offset into the run), `time_to_first_success_ms`, `ok`, `error`.
`arrivals.summary.csv` adds one row per (backend, λ) with `arrivals`,
`completed`, `failures`, `p50_ms`/`p95_ms`/`p99_ms`/`max_ms` and
`peak_concurrent_envs`. Percentiles cover successful arrivals only, with
failures counted beside them — a 20s Anvil startup timeout in the tail would
blame the architecture for this machine's resource ceiling. A row with a
large `failures` count describes only the arrivals that got served at all.

### `bench_freshness.py` — chain-tip freshness at fleet scale

Pinning a block makes forking a one-time cost. Against the live tip it is a
recurring bill, and that is where the shared base pays: forkyard re-forks
**one** base per new block for every session at once (`ChainTipFollower`),
while each Anvil must `anvil_reset` and refetch for itself.

```bash
uv run python bench_freshness.py --agents 5,25 --duration 120 --refresh-secs 30 \
  --rpc-url $RPC_URL --out freshness.csv
```

Both backends run **unpinned** — forkyard without `FORKYARD_FORK_BLOCK_NUMBER`
(with `FORKYARD_INGEST_POLL_SECS` set from `--poll-secs`, default 4s, so a new
block lands inside the run), Anvil without `--fork-block-number`. Every
`--refresh-secs`, all N agents demand current state at the same instant:
forkyard by opening a new session, Anvil by `anvil_reset` with
`{"forking": {"jsonRpcUrl": …}}` and no block number. Both then read
`eth_blockNumber`.

Both go through `rpc_proxy.CountingProxy`, and counting is reset once every
environment is up: the initial fork is setup, the question is what *staying*
fresh costs. A separate poller reads the real endpoint **directly**, never
through the proxy — a yardstick inside the proxy would add its own traffic to
the number being compared. The tip is read *after* each refresh, so a slow
refresh is scored against where the chain is when the agent finally gets its
answer.

`freshness.csv`: `backend`, `agents`, `agent_id`, `refresh_index`,
`observed_block`, `true_tip`, `block_lag` (= `true_tip − observed_block`),
`refresh_ms`, `ok`, `error`; `-1` in `observed_block`/`true_tip`/`block_lag`
means "no answer to report", not zero. `freshness.summary.csv` adds one row
per (backend, N): `lag_p50`/`lag_p95`, `refresh_ms_p50`/`refresh_ms_p95`,
`http_requests`, `jsonrpc_calls`, `calls_per_agent_refresh`,
`upstream_errors`, `top_methods`.

`calls_per_agent_refresh` is the number the two architectures differ on. Read
it knowing that a large part of Anvil's per-reset bill is re-fetching its ten
prefunded dev accounts, not the agent's own state — intrinsic to
`anvil_reset`, but not a cost the agent asked for.

## Quota and cost

Call counts say how much traffic each backend sends the provider. Two
things follow from that number and neither is visible in it: what the
traffic **costs**, and what happens when the provider **caps the rate**.

### Rate limiting in the counting proxy

`rpc_proxy.py` takes an optional token bucket:

```bash
uv run python rpc_proxy.py --upstream $RPC_URL --rate-limit-rps 25 --limit-mode delay
```

The budget is in JSON-RPC **calls** per second and a batch of M costs M,
because that is how providers meter. `--burst` (default: one second of the
rate) is the bucket capacity. The two modes are the two things real
providers do, and they produce completely different-looking benchmarks:

- `--limit-mode delay` — over-budget calls are **queued**, never dropped.
  Excess volume turns into latency: `wall_clock_ms` and `total_delay_ms`
  climb, success stays at 100%.
- `--limit-mode reject` — over-budget calls get JSON-RPC error `-32005`
  ("limit exceeded") over HTTP 429, the code Infura/Alchemy/QuickNode
  return. Excess volume turns into **failed actions**.

`ProxyStats` gains `throttled_calls` (calls not let straight through),
`rejected_calls` (the subset that never reached the upstream — a delayed
call still costs money and still returns data, a rejected one does
neither), `total_delay_ms` and `max_delay_ms`. `total_delay_ms` is summed
across handler threads, so with N agents in flight it can far exceed the
run's wall clock — that ratio is how oversubscribed the quota was, not a
wall-clock claim. With no limit configured every one of these stays 0 and
the request path skips the bucket entirely.

### `bench_quota.py` — how many agents survive a quota

```bash
uv run python bench_quota.py --quotas 10,25,100 --agents 5,10,25,50 \
    --block-height 25795072 --actions-per-agent 5 --rpc-url $RPC_URL --out quota.csv
```

Sweeps quota × agent count for both backends through one rate-limited
proxy, and reports for each (backend, quota) the largest agent count still
completing ≥99% of its actions. Ports are fixed and distinct from
`run_benchmark.py`'s (forkyard 18640/18641, Anvil 19600+, proxy 18700) so a
long quota sweep and an ordinary sweep can coexist. `--limit-mode reject`
measures the failure-mode version of the same question; `--full-curve`
keeps testing agent counts after one has already failed (off by default:
those points cost a full sweep each and cannot raise the verdict).

| Column | Meaning |
| --- | --- |
| `backend`, `quota_rps`, `num_agents` | Which point the row is for. `quota_rps` is the upstream budget in JSON-RPC calls/sec. |
| `action_success_rate` | Fraction of that point's recorded actions that succeeded. `acquire` and `discard` count: under a tight quota the first thing that fails is Anvil *forking at all*, and an agent that never got an environment has failed. |
| `wall_clock_ms` | Wall clock for the whole point, same timed region as `run_benchmark.py`'s `__total__` row. |
| `jsonrpc_calls` | Calls the point sent upstream (a batch is many). |
| `throttled_calls` | Calls the limiter did not pass straight through — delayed under `delay`, refused under `reject`. |
| `total_delay_ms` | Time parked in the limiter, **summed across agents**; may exceed `wall_clock_ms`. |
| `max_sustainable_agents` | Per (backend, quota), repeated on each of its rows: the largest tested agent count that met the threshold *with every smaller tested count also meeting it*. A pass sitting above a failure is luck, not capacity — the per-point rows keep that evidence visible. `0` means even the smallest count failed. |

### `cost_model.py` — what a run cost the provider

```bash
uv run python run_benchmark.py --count-upstream ... --out results.csv
uv run python cost_model.py results.upstream.csv --usd-per-million-cu 0.45
```

Weights each method by the provider's published compute-unit price and
prints per-combination CU and USD plus **$/1,000 agent runs** per backend
(an *agent run* = one agent's whole workload in that combination, i.e. its
`episodes` acquire→act→discard cycles). Weighting matters because a call is
not a call: `eth_chainId` is free, `eth_call` is 26 CU, `eth_sendRawTransaction`
is 40.

Two caveats travel with every number it prints:

- **The table is a snapshot, not a lookup.** Weights are transcribed from
  Alchemy's public [Compute Unit Costs](https://www.alchemy.com/docs/reference/compute-unit-costs)
  list and the default price ($0.45/million CU) from its pay-as-you-go
  page, both read 2026-09-04. Provider tables drift, and other providers
  meter in different units entirely — so the CU **ratios** here are
  Alchemy's, not universal. Anything not on the published list (e.g.
  `eth_getAccountInfo`, ~13% of Anvil's measured traffic) is charged at a
  clearly labelled `DEFAULT_CU` stand-in and named in the output.
- **`top_methods` is truncated.** The upstream CSV keeps only the five
  busiest methods, so summing it is a strict lower bound. Where it does not
  cover a row's `jsonrpc_calls`, the remainder is priced at the covered
  calls' average weight and the row is labelled `extrapolated`, with a
  `cover` column showing how much of the call count the breakdown actually
  saw.

## Branching benchmark

### `bench_branching.py` — exploring K what-ifs from one starting state

```bash
# wall clocks (no proxy hop in the path)
uv run python bench_branching.py --branches 2,4,8,16,32 --prefix-actions 5 \
  --branch-actions 3 --rpc-url $RPC_URL --no-proxy --out branching.csv

# upstream call counts (same sweep, through the counting proxy)
uv run python bench_branching.py --branches 2,4,8,16,32 --prefix-actions 5 \
  --branch-actions 3 --rpc-url $RPC_URL --out branching.proxied.csv
```

An agent gets somewhere interesting — fund an account, seed a token balance,
approve, swap — and then wants to try K mutually-exclusive continuations from
exactly that point. Three ways to do it, all doing the **same total work**
(one prefix plus K x B branch actions):

| Arm | How it branches | Branches coexist? |
| --- | --- | --- |
| `forkyard-branch` | prefix once, then `forkyard_forkFrom` K times; the K children run concurrently | yes, all K at once |
| `anvil-snapshot` | one Anvil, `evm_snapshot`, then per branch: run the actions, `evm_revert` back | **no — one at a time** |
| `anvil-processes` | K Anvils, each spawning and replaying the whole prefix, then diverging | yes, at K spawns + K prefixes |

`anvil-snapshot` is **sequential by construction, and that is the finding
rather than a flaw in the harness**: `evm_revert` invalidates every snapshot
taken after the one it restores, so the K branches share a single mutable EVM
and can only be visited one after another. Putting them on a thread pool would
not make two of them coexist, it would make them corrupt each other. The two
Anvil arms are therefore the two halves of the same trade: serialise the
exploration, or pay for the prefix K times.

Isolation is asserted, not assumed. The prefix writes a known marker balance;
every branch first reads it back — proving the branch really inherited the
parent's overlay, since a fork that had silently started from the shared base
would read 0 — and then overwrites it with a value only that branch uses. For
the forkyard arm the markers are re-read **after** every branch has finished,
with all K children and the parent still alive, which is the check the
snapshot stack cannot even be asked to perform. Any disagreement lands in
`isolation_violations`, and a row with a non-zero count is reported `ok=False`
however fast it was.

Ports: forkyard 18650/18651, anvil 19700+ (a fresh port for every Anvil in a
run — a killed Anvil's port lingers in TIME_WAIT). The forkyard process is
restarted **per K**, not once for the sweep: its base cache is shared across
sessions and would otherwise stay warm from K=2 all the way to K=32, so every
later K would be reading state an earlier K paid for while the Anvil arms
(running `--no-storage-caching`) refetch from cold every time.

| Column | Meaning |
| --- | --- |
| `arm` | `forkyard-branch`, `anvil-snapshot` or `anvil-processes`. |
| `branches` | K for that sweep. |
| `phase` | `prefix`, `branch_create`, `branch_action` or `total`. |
| `branch_id` | Which branch the row belongs to; `-1` for the shared prefix and for the arm total. |
| `elapsed_ms` | Wall clock for that row. For `branch_create`: `forkyard_forkFrom` (forkyard), `evm_snapshot` + `evm_revert` for that branch (anvil-snapshot), or spawn + prefix replay (anvil-processes). |
| `ok` / `error` | Whether it succeeded, and `repr()` of the exception truncated to 200 chars. |
| `isolation_violations` | Marker read-backs that returned the wrong value. On a `branch_action` row: that branch's own inline checks. On the `total` row: the arm's whole count, forkyard's post-hoc sweep across all live children included. **Must be 0.** |
| `jsonrpc_calls` | Upstream JSON-RPC calls the whole arm made, from `CountingProxy`. Only the `total` row carries it — the proxy is per-process, so a call cannot be attributed to one branch of a concurrent arm. Empty means "not attributable here", which is not the same claim as 0. |

Three things to know before quoting a number:

* **`total` is the only row comparable across arms.** `anvil-processes` emits
  its per-branch `prefix` rows *and* a `branch_create` row that spans spawn +
  that same prefix, so its phases deliberately overlap.
* **Timings and call counts come from two different runs.** The counting proxy
  (on by default) inserts a local hop into every upstream call. Quote wall
  clocks from a `--no-proxy` run and `jsonrpc_calls` from a proxied one. A
  proxied run also prints `ConnectionResetError`/`BrokenPipeError` tracebacks
  from `rpc_proxy.py` as killed Anvils drop their connections; they are noise
  and do not affect the counts.
* **The CSV has no per-action label.** Rows are written in execution order and
  the action cycles are fixed, so the i-th `prefix` row is
  `PREFIX_STEPS[i % 5]` (with a trailing `parent_marker` write, which is
  instrumentation and is not counted against `--prefix-actions`) and the i-th
  `branch_action` row is `BRANCH_STEPS[i % 3]` (after one leading
  `inherit_check`).

A K=2 smoke run (P=5, B=3, `--no-proxy`, block 25795072) gives the shape:
`branch_create` was **1.1-1.4 ms** for `forkyard_forkFrom`, **4.5-5.1 ms** for
`evm_snapshot`+`evm_revert`, and **4.6 s** for an Anvil spawn plus prefix
replay — and totals of 2.9 s / 6.1 s / 5.5 s with zero isolation violations
everywhere. Through the proxy at P=3, B=2 the upstream cost was 21 / 75 / 138
JSON-RPC calls. Read the *slope* across K rather than one row: forkyard's
branch cost is O(parent overlay) and is paid K times, `anvil-snapshot`'s total
grows linearly in K because the branches cannot overlap at all, and
`anvil-processes` pays K spawns and K prefixes but does at least run them
concurrently — so at small K it can beat the snapshot stack, and the arm to
watch as K grows is which of the two Anvil failure modes gets worse faster.

## Multi-block benchmark

`bench_blocks.py` — N agents spread over B **different** fork blocks at once.

Every other benchmark here pins the whole sweep to one block. This one exists
because per-session block pinning (`POST /session {"block_number": N}`) made a
new question answerable: what does it cost to serve a fleet whose agents each
need a *different* historical block?

* **forkyard**: one process. Sessions naming the same block share one fetched
  base and one fallback, so upstream cost tracks the number of distinct
  *blocks*, not the number of agents.
* **anvil**: `--fork-block-number` is a process-level flag, so B blocks forces
  at least B processes — and because an Anvil instance *is* Anvil's unit of
  isolation, N isolated agents forces **N processes**, grouped B ways by
  block. This is the only apples-to-apples arm.
* **anvil-shared-unsafe** (opt-in, `--arms ...,anvil-shared-unsafe`): B
  processes total, with the N/B agents at a block all pointing at one Anvil.
  Recorded because it is the cheap thing an operator would actually try, and
  labelled `unsafe` because **those agents are not isolated from each other** —
  one agent's `anvil_setBalance` or landed transaction is visible to every
  other agent in its group. Never quote it beside the other two without that
  sentence.

Two rounds are run against the same blocks. Round 1 is cold; round 2 opens
fresh sessions (forkyard, same process) or spawns fresh processes (Anvil) at
the same blocks. forkyard's per-block bases are still resident; every new
Anvil refetches from scratch. That gap is the point of the file.

```bash
cd python/benchmarks
PATH=../../target/release:$PATH uv run python bench_blocks.py \
  --rpc-url "$RPC_URL" \
  --agents 24 --blocks 1,2,4,8 \
  --base-block 25795072 --block-stride 1000 \
  --rounds 2 --out results/blocks.csv
```

Expect roughly 15-25 minutes: the forkyard arm is seconds per round, and
essentially all of the wall clock is Anvil spawning 24 forking processes eight
times over (4 values of B x 2 rounds). `--arms forkyard` alone runs in about a
minute. Do **not** set `FORKYARD_FORK_BLOCK_NUMBER`: it pins the whole process
and disables the tip follower, which is a different feature.

Correctness is checked rather than assumed, because a cheap benchmark that
forked everything at the same block would look identical to a correct one:

* `eth_blockNumber` on every agent's environment must equal the block it asked
  for — `block_mismatches`, which **must be 0** for a row to mean anything (an
  environment whose block could not be read counts as a mismatch, not as a
  pass);
* environments at different blocks must see different state —
  `distinct_state_verified`, which is also `no` if two agents at the *same*
  block disagree, since that would falsify the one-cache-per-block claim.

### Output

`--out` gets one row per agent phase; `<out>.summary.csv` gets the per-fleet
numbers, which cannot be attributed to a single agent in a concurrent arm.

| Column | Meaning |
| --- | --- |
| `arm` | `forkyard`, `anvil` or `anvil-shared-unsafe`. |
| `blocks` | B — how many distinct fork blocks the fleet spans. |
| `agents` | N — total agents, spread round-robin over those blocks. |
| `round` | 1 = cold, 2 = warm (same blocks, fresh sessions/processes). |
| `agent_id` | 0..N-1; agent i is pinned to `blocks[i % B]`. |
| `block_number` | The block that agent asked for. |
| `phase` | `acquire` (session open, or Anvil spawn + wait-until-ready), `read` (one row per Uniswap V2 pair), `discard`. |
| `elapsed_ms` / `ok` / `error` | Wall clock, success, and `repr()` of the exception truncated to 200 chars. |

Summary CSV, one row per (`arm`, `blocks`, `round`): `jsonrpc_calls` (upstream
JSON-RPC calls that whole round made, from `CountingProxy`), `peak_rss_mb`
(sampled across every process this run started, excluding pre-existing ones),
`wall_clock_ms`, `block_mismatches`, `distinct_state_verified`
(`yes`/`no`/`n/a`).

A smoke run at N=2, B=2, one round, blocks 25795072 and 25794072 gives the
shape: forkyard **32** upstream JSON-RPC calls / 14.5 MB peak / 1.3 s, against
**114** calls / 67.4 MB / 8.0 s for `anvil` — 0 block mismatches and
`distinct_state_verified=yes` everywhere. Probed directly, a session asking for
25795072 reports `eth_blockNumber` 25795072 and a WETH balance of
2136162.610202074692260002 ETH, one asking for 25794072 reports 25794072 and
2132325.280900491438384367 ETH, and a second session at 25795072 reports the
first one's numbers exactly — different blocks really are different state, and
the same block really is one shared base.

Five things that make the claim weaker than it looks:

* **The state fingerprint is WETH's account balance, not a pair's reserves.**
  forkyard's per-session RPC has no `eth_call`, `eth_getCode` or
  `eth_getStorageAt`, so the only state a session can hand back is an
  account's balance and nonce — and a Uniswap V2 pair holds its reserves in
  storage and no ETH at all, making its account state byte-identical at every
  block. WETH's balance is the ETH backing every one of those pairs' WETH side
  and moves essentially every block, so it is the strongest cross-block signal
  this surface can return. The pairs are still what the timed reads touch.
* **At N = B the two Anvil arms are the same thing.** One agent per block means
  one process per agent either way; `anvil-shared-unsafe` only diverges once
  N/B > 1. The smoke run above is exactly that degenerate case, which is why
  both arms cost 114 calls there.
* **Round 2 is deliberately asymmetric, and that asymmetry is the finding, not
  a bug.** forkyard keeps its process (and so its per-block bases) across
  rounds; Anvil cannot, because a fork block is chosen at spawn. Anvil's own
  on-disk cache is left disabled (`--no-storage-caching`, `backend.py`'s
  default) so the sweep does not measure earlier sweeps.
* **`FORKYARD_MAX_PINNED_BLOCKS` is set above B on purpose** (`max(B) + 2` by
  default, `--max-pinned-blocks` to change it). At or below B the LRU starts
  evicting a base mid-round and the arm would measure eviction-and-refetch
  instead of sharing. Eviction is worth benchmarking; it is not what this file
  claims.
* **Timings and call counts come from two different runs.** The counting proxy
  (on by default) adds a local hop to every upstream call — quote wall clocks
  from `--no-proxy` and `jsonrpc_calls` from a proxied run. A proxied run also
  prints `BrokenPipeError` tracebacks from `rpc_proxy.py` as killed Anvils drop
  their connections; they are noise and do not affect the counts.
