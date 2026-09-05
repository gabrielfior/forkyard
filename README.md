<img src="assets/logo.svg" alt="" width="72" height="72">

# Forkyard

Instant, disposable forks of live EVM chain state — for AI agents that need to simulate a transaction before committing gas or capital.

For a single agent, [Anvil](https://book.getfoundry.sh/anvil/) is simpler and already enough — use it. Forkyard is for once there's more than one concurrent agent/session: one process, one shared warm cache across many isolated sessions, with MCP (stdio), MCP (Streamable HTTP), and JSON-RPC (HTTP) surfaces all running side by side out of the box.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/gabrielfior/forkyard/main/install.sh | bash
```

Installs a prebuilt `forkyard` binary (x86_64 Linux or arm64 macOS) into `/usr/local/bin` if writable, else `~/.local/bin` — usually already on `PATH`, no Rust toolchain required. No release for your platform yet? [Build from source](#build-from-source) below.

## Get started

Set an RPC endpoint and run it:

```bash
export RPC_URL=https://your-mainnet-rpc
forkyard
```

That starts all three surfaces on one shared cache:

- **MCP over stdio** — point your agent framework at the `forkyard` binary, e.g.:
  ```json
  { "mcpServers": { "forkyard": { "command": "forkyard", "env": { "RPC_URL": "https://your-mainnet-rpc" } } } }
  ```
  Tools: `fork`, `simulate`, `advance`, `get_balance`, `set_balance`, `set_storage`, `discard`.
- **MCP over HTTP** (Streamable HTTP transport), default `http://127.0.0.1:8556/mcp` (`FORKYARD_MCP_HTTP_PORT` to change the port) — for any client that isn't launching forkyard as a subprocess: a browser-based agent, a teammate's [mcp-cli](https://github.com/philschmid/mcp-cli), a client on another machine. Same tools as above. mcp-cli config:
  ```json
  { "mcpServers": { "forkyard": { "url": "http://127.0.0.1:8556/mcp" } } }
  ```
  Unlike the stdio config, this points at an already-running `forkyard` process — start it first, then run `mcp-cli` against it. That also means the session survives across separate `mcp-cli call` invocations, since `fork()`'s session lives in that one long-running process rather than a fresh one spawned per call.
- **HTTP JSON-RPC**, default `http://127.0.0.1:8555` (`FORKYARD_PORT` to change it) — `POST /session` opens a session (optional body `{"block_number": N}` pins it to block N; no body means the block the process is currently on), then `POST /session/{id}` speaks normal Ethereum JSON-RPC (`eth_call`, `eth_sendRawTransaction`, etc.) against it (the `{id}` path segment selects the session). Works with `cast`, `alloy`, `web3.py`, or any wallet/client — see `python/examples` for a working `web3.py` demo.

The model: `fork()` → `simulate(tx)` (read-only) or `advance(tx)` (commits, but only into that session's own private overlay) → `discard()` or let the session's idle TTL expire (default 1 hour, `FORKYARD_SESSION_TTL_SECS` to change it). The real chain and the shared cache are never written to: `get_balance` after `simulate(tx)` is unchanged, after `advance(tx)` it reflects the transfer.

## Configuration

| Variable | Default | What it does |
| --- | --- | --- |
| `RPC_URL` | *(required)* | Upstream endpoint the fork reads real chain state from. |
| `FORKYARD_PORT` | `8555` | HTTP JSON-RPC port. |
| `FORKYARD_MCP_HTTP_PORT` | `8556` | MCP-over-Streamable-HTTP port. |
| `FORKYARD_SESSION_TTL_SECS` | `3600` | Idle lifetime of a session before it's reaped. |
| `FORKYARD_NUM_WORKERS` | `4` | OS threads sessions are sharded across. The default is deliberately small; under heavy concurrency it is the first thing to raise — see [Benchmark](#benchmark), where 100 concurrent agents ran 2.4x faster at `12` (13.06s → 5.49s). |
| `FORKYARD_FORK_BLOCK_NUMBER` | *(unset)* | Pin the fork to an explicit historical block instead of following the chain tip. When set, the background chain-tip follower is **disabled** — re-forking to a newer block would defeat the point of pinning — so every session sees exactly that block for the process's whole lifetime. Reproducible runs (benchmarks, regression tests) want this; note that your `RPC_URL` must actually serve historical state at that height, which some public endpoints only do on a paid tier. |
| `FORKYARD_MAX_PINNED_BLOCKS` | `8` | How many *per-session* pinned blocks (`POST /session` with `{"block_number": N}`) stay warm at once. Sessions at the same block share one cache, so B blocks cost B fetch backends, not one per session; past the cap the least-recently-used block is evicted — sessions already open at it keep working untouched, only the *next* session at that block refetches. Unlike `FORKYARD_FORK_BLOCK_NUMBER` this is per session, not per process, and pinned blocks are never moved by the chain-tip follower. |

## Gotchas

- **`gas_price` vs the fork's real basefee** — `advance`/`simulate` reject a `gas_price` below the forked block's basefee (the schema defaults it to `0`, which will fail on almost any live chain). There's no MCP tool for reading basefee — use `eth_gasPrice` on the JSON-RPC surface, running alongside MCP on the same process, which already includes a usable priority-fee margin.
- **Nonces aren't tracked for you** — `advance`'s `nonce` defaults to `0` and isn't auto-incremented; each successful call bumps the sender's nonce by 1. A reused nonce fails with `NonceTooLow`, a skipped-ahead one with `NonceTooHigh` — neither corrupts state. Check the current value via `get_balance`, which returns `nonce` alongside `balance`.
- **Balances aren't zeroed by default, and `set_balance` overwrites rather than credits** — an address you haven't called `set_balance` on keeps its real forked-chain balance; explicitly set every address you use (sender and receiver) for deterministic tests, and remember calling `set_balance` twice with the same value doesn't double it.
- **Addresses aren't checksum-validated** — mixed-case (EIP-55) and lowercase hex are both accepted as the same address.
- **Gas fees are burned to the zero address** — forkyard never sets a block beneficiary, so `advance`'s `gas_used * gas_price` debit lands on `0x0000…0000`, not a miner/validator.
- **Three cheatcodes live on the JSON-RPC surface, not just MCP** — alongside `forkyard_setBalance` there's `forkyard_setStorageAt(address, slot, value)`, which writes one raw storage slot in that session's overlay (how you mint yourself an ERC-20 balance: compute the `balanceOf` mapping slot and set it — see `python/benchmarks/backend.py`), and `forkyard_discard()`, the HTTP counterpart to the `discard` MCP tool for tearing a session down ahead of its TTL. All three affect only the calling session's overlay.
- **Branching a session — `forkyard_forkFrom()`** — opens a *new* session whose starting state is the calling session's current state (the shared base plus everything that session has written or cached), and answers with `{"session_id": …}`, the same shape `POST /session` returns. From that moment the two are independent: neither sees the other's later writes, a child can be branched again, and discarding the parent leaves its children fully working. This is the one thing `evm_snapshot`/`evm_revert` can't express — a snapshot stack has one live branch at a time — and it costs a fold of that session's overlay into a fresh structurally-shared base, not a state dump.

## Benchmark

`python/benchmarks/` runs one workload against both architectures — forkyard (one process, N concurrent forked sessions) and N standalone Anvil processes, one per agent — pinned to the same fork block and pointed at the same upstream RPC endpoint.

### What one agent does

Every agent runs this sequence, all N of them concurrently in a thread pool:

1. **Acquire its own environment**, timed as an `acquire` interaction. forkyard: `POST /session`. Anvil: spawn `anvil --fork-url … --fork-block-number …` and poll until it answers `eth_blockNumber`.
2. **Run 7 timed interactions.** A `set_balance` to fund itself, then 5 actions drawn at random from `transfer`, `get_balance`, `fund_token` (mints an ERC-20 balance by writing the `balanceOf` storage slot), `approve`, `swap_eth_for_token` and `swap_token_for_token` — the two swaps are real Uniswap V2 router calls, and the router-dependent ones only become eligible once their `fund_token`/`approve` prerequisite has run — then a `discard`.
3. **Tear the environment down.** forkyard: `forkyard_discard`. Anvil: kill the process.

Each state-changing interaction is a full signed transaction, not an `eth_call`: read `eth_gasPrice` and `eth_chainId`, sign locally, `eth_sendRawTransaction`, then wait for the receipt — and it counts as a failure unless the receipt comes back with status 1. So a per-interaction number below is the round trip an agent actually waits on, execution included.

`--episodes N` repeats the whole cycle N times per agent, each episode acquiring a fresh environment — an agent that holds one fork open versus one that treats forks as disposable. `--state-overlap` swaps the transaction mix for a read-only workload over chosen contracts. Both change the answer substantially; see below.

The timer covers, per agent: environment acquisition + its interactions + its teardown. It excludes forkyard's one-time process startup — a single shared cost with no Anvil counterpart — while Anvil's per-agent process spawn is inside the timer, because Anvil pays it once per agent.

### Caches, and why both sides start cold

Anvil has a caching mechanism forkyard does not: Foundry **persists fetched fork state to disk**, at `~/.foundry/cache/rpc/<chain>/<block>/storage.json` — a zstd-compressed JSON of accounts (balance, nonce, code), contract storage slots and block hashes. It survives process exit and is shared by every `anvil`, `forge` and `cast` process on the machine at that block. forkyard's cache is in-memory and per-process: it is shared by every session in that process, and refilled from the endpoint on the next start.

The two caches win in different situations, and the difference is large. Ten agents each reading the same eight contracts:

| Anvil cache state | forkyard upstream calls | anvil upstream calls |
| --- | --- | --- |
| Cold (first run at this block) | 37 | 778 |
| **Warm (second run, same pinned block)** | 37 | **30** |
| Disabled (`--no-storage-caching`) | 37 | 638 |

So on a *repeat* run at a pinned block Anvil is the more economical of the two — its 100 processes read state that an earlier run already paid for, while forkyard refetches on every process start. That advantage is bounded in one important way: Foundry keys the cache by the block the fork resolved to, so agents forking at the **chain tip** — forkyard's default mode — only reuse it within the ~12s that block stays the tip, while a pinned, reproducible workload reuses it forever.

Because forkyard has no cross-process cache to match it, every measurement below spawns Anvil with `--no-storage-caching`, so both backends start cold and a sweep measures the two architectures rather than the machine's benchmark history. `--anvil-rpc-cache` puts Anvil's cache back if you want to measure it. **This matters more than any other methodological choice here** — with it left on, whole sweeps ran in which Anvil made zero upstream state calls and still answered every read.

### Elapsed time

Same fork block, same endpoint, one long-lived environment per agent, 7 interactions in it, default settings on both sides. Columns: whole-sweep wall-clock; one agent's own end-to-end time, averaged; median environment acquisition; mean latency of a single post-acquisition interaction.

| Concurrent agents | forkyard sweep | anvil sweep | forkyard per agent | anvil per agent | forkyard acquire | anvil acquire | forkyard per interaction | anvil per interaction |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | **2.62s** | 4.68s | 2.61s | 4.67s | 6ms | 840ms | 372ms | 547ms |
| 5 | **2.74s** | 5.03s | 2.21s | 4.14s | 4ms | 851ms | 315ms | 469ms |
| 10 | **3.15s** | 5.06s | 3.02s | 4.07s | 16ms | 879ms | 430ms | 455ms |
| 25 | **4.79s** | 5.08s | 4.09s | 3.63s | 17ms | 861ms | 578ms | 395ms |
| 50 | **7.55s** | 8.53s | 6.80s | 5.96s | 216ms | 3331ms | 932ms | 466ms |
| 100 | **13.06s** | 14.02s | 11.93s | 10.65s | 443ms | 5588ms | 1632ms | 668ms |

All 1,528 interactions per backend succeeded at every tier. forkyard is ahead everywhere, but for shrinking reasons: at 1-10 agents it wins on both halves, while from 25 agents up Anvil's *interactions* are faster and forkyard stays ahead only because Anvil's per-agent process spawn keeps getting more expensive under load (0.86s at 25 agents, 5.6s at 100).

Median latency per interaction type:

| Interaction | forkyard @ 10 agents | anvil @ 10 agents | forkyard @ 100 agents | anvil @ 100 agents |
| --- | --- | --- | --- | --- |
| `acquire` | 16ms | 879ms | 443ms | 5588ms |
| `get_balance` | 11ms | 5ms | 1965ms | 3ms |
| `set_balance` | 491ms | 155ms | 4680ms | 149ms |
| `fund_token` (raw slot write) | 8ms | 10ms | 981ms | 3ms |
| `transfer` | 277ms | 513ms | 1179ms | 517ms |
| `approve` | 220ms | 501ms | 1138ms | 504ms |
| `swap_eth_for_token` | 327ms | 2190ms | 1170ms | 2491ms |
| `discard` | 3ms | 4ms | 426ms | 4ms |

### Disposable forks (session churn)

Acquisition is where the two architectures differ most — 0.3ms against ~630ms measured in isolation, 16ms against 879ms under ten-way concurrency. A long-lived environment amortises that away; a *disposable* fork does not. Same total work per agent (20 actions), split two ways:

| Concurrent agents | One long fork: forkyard | One long fork: anvil | 10 disposable forks: forkyard | 10 disposable forks: anvil |
| --- | --- | --- | --- | --- |
| 1 | 3.32s | 8.00s | **5.33s** | 31.29s |
| 5 | 3.88s | 8.66s | **6.41s** | 31.78s |
| 10 | 4.88s | 8.62s | **9.35s** | 30.84s |
| 25 | 8.64s | 10.65s | **20.24s** | 31.70s |

Churn is the workload forkyard is built for and it wins by 1.6-5.9×. Anvil's ten re-acquisitions cost it a flat ~8.5s per agent no matter the concurrency — 28-35% of its wall-clock — against forkyard's 0.5-14%.

### Upstream RPC load

The claim underneath the architecture is that N agents should not cost the provider N forks' worth of traffic. `--count-upstream` routes both backends through a counting proxy (`rpc_proxy.py`) and tallies what reaches the endpoint, batch-aware, since providers bill per call:

| Concurrent agents | forkyard calls | anvil calls | forkyard per agent | anvil per agent |
| --- | --- | --- | --- | --- |
| 1 | 33 | 95 | 33.0 | 95.0 |
| 5 | 73 | 452 | 14.6 | 90.4 |
| 10 | 108 | 891 | 10.8 | 89.1 |
| 25 | 204 | 2,105 | 8.2 | 84.2 |
| 50 | 387 | 4,204 | 7.7 | 84.1 |

Anvil's per-agent cost is flat at ~84-95, i.e. every agent pays for its own fork in full; forkyard's *falls* from 33 to 7.7 as agents are added. At 50 agents that is **10.9× less traffic**, and the ratio grows with N.

### How much of that is sharing? (shared vs disjoint state)

The control that isolates it. A read-only workload where every agent reads the same 8 Uniswap V2 pairs, against one where each agent reads its own 8 — same instrument, same tiers, only the sharing changes:

| Concurrent agents | Shared: forkyard | Shared: anvil | Shared ratio | Disjoint: forkyard | Disjoint: anvil | Disjoint ratio |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 37 | 78 | 2.1× | 37 | 78 | 2.1× |
| 5 | 37 | 390 | 10.5× | 165 | 388 | 2.4× |
| 10 | 37 | 780 | 21.1× | 325 | 780 | 2.4× |
| 25 | 37 | 1,732 | 46.8× | 805 | 995 | 1.2× |
| 50 | **37** | 3,201 | **86.5×** | 1,605 | 3,775 | 2.4× |

On shared state forkyard's upstream traffic is **completely flat** — 37 calls whether one agent reads those pairs or fifty — while Anvil pays 78 per agent. On disjoint state the advantage collapses to a constant ~2×, which is per-fork setup overhead, not sharing. That is the shared cache doing exactly, and only, what it claims.

Wall-clock in the same runs tells the other half of the story:

| Concurrent agents | Shared: forkyard | Shared: anvil | Disjoint: forkyard | Disjoint: anvil |
| --- | --- | --- | --- | --- |
| 10 | **3.08s** | 4.52s | 7.73s | **3.46s** |
| 50 | **3.71s** | 7.94s | 31.70s | **7.91s** |

Reading shared state, forkyard is flat and faster. Reading cold, unshared state at 50-way concurrency it is 4× *slower*, because every miss queues behind the same worker pool — the ceiling described below.

### Under a rate-limited endpoint

Repeating the headline sweep against `https://ethereum-rpc.publicnode.com` (a free public endpoint, pinned near the chain tip since a full node won't serve deep historical state) is where the upstream numbers turn into outcomes:

| Concurrent agents | forkyard sweep | forkyard action success | anvil sweep | anvil action success |
| --- | --- | --- | --- | --- |
| 1 | 2.79s | 100% | 8.05s | 100% |
| 5 | 2.75s | 100% | 20.17s | 97.0% |
| 10 | 3.29s | 100% | 13.41s | 100% |
| 25 | **5.68s** | **100%** | 20.42s | **26.4%** |

At 25 agents Anvil's failures are the quota wall arriving: instances that never became ready, connection errors, and — the most misleading failure mode — `Insufficient funds for gas * price + value`, i.e. a balance fetch that quietly failed and left the account looking empty. forkyard, fetching once for all 25 sessions, never reached the wall.

### How to read this

**forkyard is faster on wall-clock at every tier measured, and the advantage is largest exactly where the design says it should be**: disposable forks (1.6-5.9×), shared state (flat traffic, 86× less at 50 agents), and constrained upstream quota (100% vs 26% action success at 25 agents on a free endpoint). It is also far smaller: sampled during a 100-agent run, one **25 MB** process against 100 Anvil instances peaking at **3.3 GB** combined.

**Anvil is not behind on everything.** Its per-interaction latency is better from 25 agents up (395ms vs 578ms at 25; 668ms vs 1632ms at 100) — forkyard stays ahead on the total only because Anvil's spawn cost grows faster. Its persistent disk cache beats forkyard outright on a repeat run at a pinned block. And on cold *unshared* state at high concurrency it is several times faster.

**What limits forkyard is contention, not architecture.** Sessions are sharded over `FORKYARD_NUM_WORKERS` threads, 4 by default. At 100 agents, cold, in a paired run: 4 workers → 13.06s, **12 workers → 5.49s**, against Anvil's 12.43s. The same ceiling turns a 16ms session-open into 443ms at 100 agents and a 3ms discard into 426ms. Raise it before drawing conclusions — and treat the contention itself as the thing to fix.

Measured 2026-09-04 on an Apple M3 Pro (12 cores, 38 GB) against a Tenderly mainnet gateway, block 25795072, default settings on both backends and Anvil's RPC cache disabled throughout. Reproduce with:

```bash
cargo build -p forkyard --release && export PATH="$PWD/target/release:$PATH"
cd python/benchmarks && uv sync

# headline table: long-lived environments
uv run python run_benchmark.py --agents 1,5,10,25,50,100 --block-heights 25795072 \
  --actions-per-agent 5 --rpc-url $RPC_URL --out results.csv
uv run python plot_results.py results.csv

# disposable forks: same 20 actions per agent, ten short episodes instead of one long one
uv run python run_benchmark.py --agents 1,5,10,25 --block-heights 25795072 \
  --actions-per-agent 2 --episodes 10 --rpc-url $RPC_URL --out churn.csv

# upstream call counts, and how much of the gap is state sharing
uv run python run_benchmark.py --agents 1,5,10,25,50 --block-heights 25795072 \
  --actions-per-agent 5 --count-upstream --rpc-url $RPC_URL --out upstream.csv
uv run python run_benchmark.py --agents 1,5,10,25,50 --block-heights 25795072 \
  --actions-per-agent 8 --state-overlap shared --count-upstream --rpc-url $RPC_URL --out shared.csv
uv run python run_benchmark.py --agents 1,5,10,25,50 --block-heights 25795072 \
  --actions-per-agent 8 --state-overlap disjoint --count-upstream --rpc-url $RPC_URL --out disjoint.csv
```

See `python/benchmarks/README.md` for the CSV schema and what each column means; the raw CSVs behind every number above are archived in `python/benchmarks/results/2026-09-04/`.

## Build from source

```bash
git clone https://github.com/gabrielfior/forkyard
cd forkyard
RPC_URL=... cargo run -p forkyard
```
