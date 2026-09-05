# forkyard benchmarks

Compares forkyard (one process, N concurrent sessions on one shared cache)
against one standalone Anvil per agent. What each benchmark measures, and the
results, are in [`benchmark.md`](../../benchmark.md); this file covers layout,
flags and CSV schemas.

## Requirements

Both binaries must be on `PATH` — they are spawned by bare name:

```bash
# anvil: install Foundry (https://book.getfoundry.sh/getting-started/installation)
cargo build -p forkyard --release          # from the repo root
export PATH="$PWD/target/release:$PATH"
uv sync
uv run pytest                              # tests/ — no subprocesses, no network
```

`--rpc-url` must serve historical state at the blocks you sweep; some public
endpoints only do that on a paid tier.

## Layout

| File | Contents |
| --- | --- |
| `bench.py` | entry point for every benchmark: `bench.py <command> [flags]` |
| `bench_architecture.py` | `branching`, `checkpoint`, `writers` |
| `bench_load.py` | `arrivals`, `freshness`, `quota` |
| `bench_cache.py` | `warmstart`, `blocks` |
| `bench_common.py` | process lifecycle, port allocation, RSS sampling, percentiles, CSV output |
| `run_benchmark.py` | the standard agent-workload sweep (its own CLI) |
| `agent.py`, `actions.py`, `backend.py`, `contracts.py` | the workload itself |
| `rpc_proxy.py` | counting/rate-limiting JSON-RPC proxy |
| `cost_model.py` | upstream calls → provider compute units and dollars |
| `plot_results.py` | PNGs from a `run_benchmark.py` CSV |
| `aggregate_runs.py` | median and spread across repeated runs of a sweep |
| `tests/` | the unit tests; `pythonpath = ["."]` in `pyproject.toml` is what lets them import the modules above |

Results are written where `--out` says and are **not** tracked in git.

## Running

```bash
uv run python bench.py                     # list the commands
uv run python bench.py branching --help    # flags for one of them

uv run python run_benchmark.py --agents 1,10,50 --block-heights 25795072 \
  --actions-per-agent 5 --rpc-url $RPC_URL --out results.csv
uv run python plot_results.py results.csv
```

Run one benchmark at a time: a second one sharing CPU, ports or RPC quota
corrupts the first.

### `run_benchmark.py` flags worth knowing

- `--episodes N` — acquire → act → discard cycles per agent. `--episodes 1
  --actions-per-agent 20` is one long-lived environment; `--episodes 10
  --actions-per-agent 2` is the same work through ten disposable forks.
- `--state-overlap {shared,disjoint}` — read-only workload over real Uniswap V2
  pairs, either the same contracts for every agent or its own. Run both: the
  first measures cache sharing, the second is the control.
- `--count-upstream` — routes both backends through `rpc_proxy.py` and writes
  `<out>.upstream.csv`. Adds a local hop, so take counts from such a run and
  timings from a direct one.
- `--cold-caches` — turn **both** persistent caches off: Anvil's
  `~/.foundry/cache` and forkyard's `FORKYARD_CACHE_DIR`. Both are on by
  default, because both survive restarts and warm-against-warm is the
  like-for-like comparison. Use this to measure a first-ever run at a block,
  and warm up once before measuring otherwise — the first run at a block fills
  both caches.

## CSV columns

| Column | Meaning |
| --- | --- |
| `backend` | `forkyard` or `anvil`. |
| `block_height`, `num_agents`, `agent_id` | Which sweep and which agent; `agent_id` is `-1` on the synthetic total row. |
| `action` | `acquire`, `set_balance`, `transfer`, `get_balance`, `fund_token`, `approve`, `swap_eth_for_token`, `swap_token_for_token`, `read_contract`, `discard`, or `__total__` for the one whole-sweep row. `acquire` is the environment acquisition — a forkyard session open, or an Anvil spawn plus wait-until-ready — and appears once per episode, first. A failed `acquire` is the whole episode. |
| `elapsed_ms` | Duration of that action, or of the sweep on a `__total__` row. |
| `ok` | Whether it succeeded; on `__total__`, whether everything did. |
| `error` | `repr()` of the exception (200 chars) when `ok` is false — what distinguishes a revert from a nonce rejection from a timeout. |

### `<out>.upstream.csv` (`--count-upstream` only)

One row per (backend, block height, agent count): `http_requests`,
`jsonrpc_calls` (batch-aware — a batch is one request and many calls, and
providers bill calls), `calls_per_agent` (the number the two architectures
differ on: flat for a per-agent cache, falling for a shared one),
`upstream_errors`, `throttled_calls`/`rejected_calls`/`total_delay_ms` under a
rate limit, and `top_methods` — the five busiest methods only, so summing it
under-counts.

Each `bench.py` command documents its own columns in `--help`; several also
write a `.summary.csv` sibling with per-sweep aggregates.
