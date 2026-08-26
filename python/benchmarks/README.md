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
| `action` | Action label (`set_balance`, `transfer`, `get_balance`, `fund_token`, `approve`, `swap_eth_for_token`, `swap_token_for_token`, `discard`), or `__total__` for the one whole-sweep row per combination. |
| `elapsed_ms` | Wall-clock duration of that action — or of the whole sweep on a `__total__` row. |
| `ok` | Whether it succeeded. On a `__total__` row, whether *every* action in that combination succeeded. |
| `error` | `repr()` of the exception (truncated to 200 chars) when `ok` is False, empty otherwise — this is what distinguishes a revert from a nonce rejection from an HTTP timeout. |
