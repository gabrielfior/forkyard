<img src="assets/logo.svg" alt="" width="72" height="72">

# Forkyard

Instant, disposable forks of live EVM chain state — for AI agents that need to simulate a transaction before committing gas or capital.

For a single agent, [Anvil](https://book.getfoundry.sh/anvil/) is simpler and already enough — it is the standard for good reason, and forkyard is measured against it throughout. Forkyard is for once there's more than one concurrent agent/session: one process, one shared warm cache across many isolated sessions, with MCP (stdio), MCP (Streamable HTTP), and JSON-RPC (HTTP) surfaces all running side by side out of the box.

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
| `FORKYARD_CACHE_DIR` | `$HOME/.forkyard/cache` | Where the fork cache is persisted between runs, as `<dir>/<chain_id>/<block_number>.json` — accounts, contract code, storage slots and block hashes, so a restart at the same block starts warm instead of refetching everything. Written atomically on shutdown (including SIGTERM) and loaded at startup; a file that's missing, truncated, from another chain or block, or in an older format is logged and ignored, and the process starts cold rather than failing. This is the equivalent of Foundry's `~/.foundry/cache/rpc/<chain>/<block>/storage.json`, and it's a separate directory on purpose — the two formats are unrelated. |
| `FORKYARD_CACHE_DISABLED` | *(unset)* | Set to `1`/`true` to neither load nor save the persisted cache — every start is a cold start. What a benchmark toggles to measure cold and warm in one run; measured here, 5 agents reading 4 contracts at a pinned block cost 14 upstream calls cold and **1** on a restart with the cache in place. |
| `FORKYARD_CACHE_FLUSH_SECS` | `0` (only at shutdown) | Also write the cache every N seconds. Shutdown alone covers Ctrl-C and SIGTERM; this buys back what a SIGKILL or a power loss would cost, at the price of re-serializing the whole snapshot that often. |
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

[Anvil](https://book.getfoundry.sh/anvil/) is the standard tool for forked-state
simulation and a very good one — it is what forkyard measures itself against
because it is what everyone already reaches for, and for a single agent it stays
the simpler choice. The difference the benchmarks probe is narrow: Anvil's unit
of isolation is an OS process with its own cache; forkyard's is a session inside
one process sharing one warm cache. That should be invisible for one agent, and
start to matter as agents multiply.

Three results, median of five runs on an Apple M3 Pro against a mainnet archive
endpoint, with **both tools' persistent caches warm**:

**Reading shared state costs one upstream call, at any scale.** With every agent
reading the same contracts, forkyard sent **1 upstream JSON-RPC call whether 1
agent or 50 were running** — identical in all five runs — against 3 → 300 for a
cache-per-process design. When agents read *disjoint* state the gap falls under
2×, which is the control showing the first number really is cache sharing.

**Isolated agents are cheap in memory.** At 50 agents writing concurrently, each
verifying it reads back its own value, forkyard used **23 MB in one process**
against **1,434 MB across 50 Anvil instances** — its footprint moved 21 → 23 MB
going from 1 agent to 50, because a session is not a process.

**Branching from a live state is ~1,600× cheaper than re-forking.** Opening a
branch from another session's current state takes **0.7 ms**, against 1,156 ms
to spawn a process and replay the setup; 32 branches from one prefix finish in
0.54s against 2.06s.

Anvil is the better tool in other regimes — notably past a few tens of
concurrent agents, where forkyard's four-thread ceiling decides it (50 agents:
2.72s against 6.34s), and for rewinding a single timeline, where `evm_snapshot`
costs about a millisecond regardless of dirty state.

**[benchmark.md](benchmark.md)** has the full set: methodology, every result
including those that favour Anvil, the measured run-to-run spread, and commands
to reproduce each one.

## Build from source

```bash
git clone https://github.com/gabrielfior/forkyard
cd forkyard
RPC_URL=... cargo run -p forkyard
```
