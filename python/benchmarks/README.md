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
