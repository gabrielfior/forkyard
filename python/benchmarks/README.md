# forkyard benchmarks

Compares forkyard (one process, N concurrent forked sessions) against
running one standalone Anvil instance per agent, across agent counts and
fork block heights. See `docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md`
in the repo root for the full design.

Requires the `anvil` binary (Foundry) on `PATH`, and a `forkyard` binary
built from this repo (`cargo build -p forkyard --release`).

## What "total simulation time" measures

For **both** backends the timer covers, per agent and concurrently across
agents: acquiring its own environment (forkyard — opening a session;
Anvil — spawning an instance and waiting until it is ready), running its
action sequence, and discarding that environment. It deliberately does
**not** include forkyard's one-time `forkyard` process startup, which is a
single shared cost with no Anvil counterpart — so the comparison is
per-agent-cost against per-agent-cost, not "warm sessions" against "cold
process starts".

```bash
uv sync
uv run pytest                     # unit tests (no subprocesses/network)
uv run python run_benchmark.py --agents 1,2,5 --block-heights 20000000 --actions-per-agent 5 --rpc-url $RPC_URL --out results.csv
uv run python plot_results.py results.csv
```