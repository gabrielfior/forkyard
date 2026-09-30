<img src="assets/logo.svg" alt="" width="72" height="72">

# Forkyard

Instant, disposable forks of live EVM chain state — for AI agents that need to simulate a transaction before committing gas or capital.

One process, one shared warm cache, many isolated sessions — with MCP (stdio), MCP (Streamable HTTP) and JSON-RPC (HTTP) surfaces running side by side out of the box. [Anvil](https://book.getfoundry.sh/anvil/) is the standard here and stays the simpler choice for a single agent; [when each tool wins](#forkyard-or-anvil) is measured below.

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
  Tools: `fork`, `simulate`, `advance`, `get_balance`, `get_storage`, `get_code`, `set_balance`, `set_storage`, `snapshot`, `resume`, `discard`.
- **MCP over HTTP** (Streamable HTTP transport), default `http://127.0.0.1:8556/mcp` (`FORKYARD_MCP_HTTP_PORT` to change the port) — for any client that isn't launching forkyard as a subprocess: a browser-based agent, a teammate's [mcp-cli](https://github.com/philschmid/mcp-cli), a client on another machine. Same tools as above. mcp-cli config:
  ```json
  { "mcpServers": { "forkyard": { "url": "http://127.0.0.1:8556/mcp" } } }
  ```
  Unlike the stdio config, this points at an already-running `forkyard` process — start it first, then run `mcp-cli` against it. That also means the session survives across separate `mcp-cli call` invocations, since `fork()`'s session lives in that one long-running process rather than a fresh one spawned per call.
- **HTTP JSON-RPC**, default `http://127.0.0.1:8555` (`FORKYARD_PORT` to change it) — `POST /session` opens a session (optional body `{"block_number": N}` pins it to block N, `{"snapshot_id": "…"}` reopens a saved one; no body means the block the process is currently on), then `POST /session/{id}` speaks normal Ethereum JSON-RPC (`eth_call`, `eth_sendRawTransaction`, `eth_sendRawTransactionSync` — EIP-7966, the receipt in the reply — etc.) against it (the `{id}` path segment selects the session). Works with `cast`, `alloy`, `web3.py`, or any wallet/client — see `python/examples` for a working `web3.py` demo.

The model: `fork()` → `simulate(tx)` (read-only) or `advance(tx)` (commits, but only into that session's own private overlay) → `discard()` or let the session's idle TTL expire (default 1 hour, `FORKYARD_SESSION_TTL_SECS` to change it). To keep a session past its TTL or a restart, `snapshot(session_id)` writes its state to disk and returns a short `snapshot_id`; `resume(snapshot_id)` reopens it as a new session in under a millisecond, instead of replaying the transactions that built it. The real chain and the shared cache are never written to: `get_balance` after `simulate(tx)` is unchanged, after `advance(tx)` it reflects the transfer.

Contract calls, not just ETH transfers: `simulate`/`advance` take `data` (`0x`-prefixed calldata) and report the call's return data in `output` — which is also where a revert's data lands, so a failed call tells you *why*. Omit `to` and the transaction becomes a deploy: `data` is the init code and the new address comes back in `contract_address`. To read contract state without executing anything, `get_storage(address, slot)` reads one raw slot and `get_code(address)` distinguishes a contract from an EOA. A misspelled or unknown argument is rejected outright rather than dropped, so a typo can't quietly turn a contract call back into a bare transfer.

## Configuration

| Variable | Default | What it does |
| --- | --- | --- |
| `RPC_URL` | *(required)* | Upstream endpoint the fork reads real chain state from. |
| `FORKYARD_PORT` | `8555` | HTTP JSON-RPC port. |
| `FORKYARD_MCP_HTTP_PORT` | `8556` | MCP-over-Streamable-HTTP port. |
| `FORKYARD_SESSION_TTL_SECS` | `3600` | Idle lifetime of a session before it's reaped. |
| `FORKYARD_NUM_WORKERS` | number of CPU cores | OS threads sessions are sharded across. A worker never waits on upstream — a job that needs uncached state is parked while its reads are fetched in one batch, and the worker serves other sessions meanwhile — so this bounds EVM parallelism, not how many agents can be waiting on the network at once. See [benchmark.md](benchmark.md#fifty-agents-under-a-second-2026-09-27). |
| `FORKYARD_FORK_BLOCK_NUMBER` | *(unset)* | Pin the fork to an explicit historical block instead of following the chain tip. When set, the background chain-tip follower is **disabled** — re-forking to a newer block would defeat the point of pinning — so every session sees exactly that block for the process's whole lifetime. Reproducible runs (benchmarks, regression tests) want this; note that your `RPC_URL` must actually serve historical state at that height, which some public endpoints only do on a paid tier. |
| `FORKYARD_CACHE_DIR` | `$HOME/.forkyard/cache` | Where the fork cache is persisted between runs, as `<dir>/<chain_id>/<block_number>.json` — accounts, contract code, storage slots, block hashes and the block's header fields, so a restart at the same block starts warm instead of refetching everything. With `FORKYARD_FORK_BLOCK_NUMBER` set, a warm start serves its first session about 10 ms after launch without waiting on upstream at all; its only upstream call is a background `eth_chainId` that opens the shared connection (repeated every 20 s to keep it open). Written atomically on shutdown (including SIGTERM) and loaded at startup; a file that's missing, truncated, from another chain or block, or in an older format is logged and ignored, and the process starts cold rather than failing. This is the equivalent of Foundry's `~/.foundry/cache/rpc/<chain>/<block>/storage.json` (`storage-<keccak(rpc_url)>.json` on current Foundry `main`), which Anvil writes only on a clean shutdown, as a whole-file overwrite of that one process's cache; it's a separate directory on purpose — the two formats are unrelated. |
| `FORKYARD_CACHE_DISABLED` | *(unset)* | Set to `1`/`true` to neither load nor save the persisted cache — every start is a cold start. What a benchmark toggles to measure cold and warm in one run; measured here, 5 agents reading 4 contracts at a pinned block cost 21 upstream calls cold and **1** on a restart with the cache in place. |
| `FORKYARD_CACHE_FLUSH_SECS` | `0` (only at shutdown) | Also write the cache every N seconds. Shutdown alone covers Ctrl-C and SIGTERM; this buys back what a SIGKILL or a power loss would cost, at the price of re-serializing the whole snapshot that often. |
| `FORKYARD_UPSTREAM_MAX_IN_FLIGHT` | `256` | Batches in flight to the upstream RPC at once, shared by every fork in the process. Past it, a burst queues inside forkyard instead of landing on the provider all at once. A rate-limited reply (`-32005`, HTTP 429) is retried with jittered backoff, about 6 s in all, before its error goes back to the caller. Lower this only if your plan's limit is tight: a cap below the provider's own throttle costs more than it saves (1,000 agents took 32.6 s at 16, 6.6 s at 128). |
| `FORKYARD_SNAPSHOT_DIR` | `$HOME/.forkyard/snapshots` | Where `snapshot` / `forkyard_snapshot` write sessions, as `<dir>/<chain_id>/<snapshot_id>.json`. Separate from the cache directory on purpose — clearing a cache must never delete a session someone meant to come back to — and on even with `FORKYARD_CACHE_DISABLED`. Ids are derived from the file's content, so any process sharing this directory can resume any snapshot another wrote. |
| `FORKYARD_MAX_PINNED_BLOCKS` | `8` | How many *per-session* pinned blocks (`POST /session` with `{"block_number": N}`) stay warm at once. Sessions at the same block share one cache, so B blocks cost B fetch backends, not one per session; past the cap the least-recently-used block is evicted — sessions already open at it keep working untouched, only the *next* session at that block refetches. Unlike `FORKYARD_FORK_BLOCK_NUMBER` this is per session, not per process, and pinned blocks are never moved by the chain-tip follower. |

## Gotchas

- **`gas_price` defaults to the fork's basefee** — omit it and `advance`/`simulate` price the transaction at the basefee of the block that session is pinned to, which is what makes it valid on a live chain. Pass a value explicitly to ask whether a transaction at *that* price would work, and it's still rejected below the basefee — but the error now names the basefee and the fix rather than just `GasPriceLessThanBasefee`. An explicit `0` still means literally zero, so zero-basefee forks keep costing nothing. Two consequences of pricing working: the sender needs `gas_limit * gas_price` in balance on top of the value sent (`set_balance` it), and that gas is really debited. For a priority-fee margin over the basefee, read `eth_gasPrice` off the JSON-RPC surface running alongside MCP in the same process.
- **Nonces aren't tracked for you** — `advance`'s `nonce` defaults to `0` and isn't auto-incremented; each successful call bumps the sender's nonce by 1. A reused nonce fails with `NonceTooLow`, a skipped-ahead one with `NonceTooHigh` — neither corrupts state. Check the current value via `get_balance`, which returns `nonce` alongside `balance`.
- **Balances aren't zeroed by default, and `set_balance` overwrites rather than credits** — an address you haven't called `set_balance` on keeps its real forked-chain balance; explicitly set every address you use (sender and receiver) for deterministic tests, and remember calling `set_balance` twice with the same value doesn't double it.
- **Addresses aren't checksum-validated** — mixed-case (EIP-55) and lowercase hex are both accepted as the same address.
- **Gas fees are burned to the zero address** — forkyard never sets a block beneficiary, so `advance`'s `gas_used * gas_price` debit lands on `0x0000…0000`, not a miner/validator.
- **Three cheatcodes live on the JSON-RPC surface, not just MCP** — alongside `forkyard_setBalance` there's `forkyard_setStorageAt(address, slot, value)`, which writes one raw storage slot in that session's overlay (how you mint yourself an ERC-20 balance: compute the `balanceOf` mapping slot and set it — see `python/benchmarks/backend.py`), and `forkyard_discard()`, the HTTP counterpart to the `discard` MCP tool for tearing a session down ahead of its TTL. All three affect only the calling session's overlay.
- **Saving a session — `forkyard_snapshot()`** — writes the calling session's own state (what it wrote, what it read, and everything it inherited from a branch) to `FORKYARD_SNAPSHOT_DIR` and answers `{"snapshot_id": …, "block_number": …, "bytes": …}`; the session stays live. `POST /session` with `{"snapshot_id": "…"}` reopens it at the block it was taken, in this process or a later one; `snapshot`/`resume` are the MCP equivalents. A snapshot holds the bytecode of every contract the session touched, so one that called Uniswap is ~90 KB, not a few hundred bytes. What it doesn't carry is the JSON-RPC surface's own bookkeeping: a resumed session's synthetic `eth_blockNumber` counter and receipt log start fresh, the way any new session's do.
- **Branching a session — `forkyard_forkFrom()`** — opens a *new* session whose starting state is the calling session's current state (the shared base plus everything that session has written or cached), and answers with `{"session_id": …}`, the same shape `POST /session` returns. From that moment the two are independent: neither sees the other's later writes, a child can be branched again, and discarding the parent leaves its children fully working. This is the one thing `evm_snapshot`/`evm_revert` can't express — a snapshot stack has one live branch at a time — and it costs a fold of that session's overlay into a fresh structurally-shared base, not a state dump.

## forkyard or Anvil?

[Anvil](https://book.getfoundry.sh/anvil/) is the standard for forked-state
simulation and the right default: for one agent, or a handful of long-lived
ones, it is simpler and — with both caches warm — faster. forkyard is for the
shapes below, where making a *session* rather than a *process* the unit of
isolation changes the numbers.

| Reach for | When | Measured |
| --- | --- | --- |
| **forkyard** | Exploring K what-ifs from one state | branch in **0.6 ms** vs 1,409 ms to respawn and replay; 32 branches in **0.44 s** vs 2.44 s (12.85 s through `evm_snapshot`, which is serial by design) |
| **forkyard** | Forks are disposable — one per hypothesis | acquire in **2 ms** vs 634 ms; 10 agents × 10 forks in **4.3 s** vs 13.9 s |
| **forkyard** | Dozens of isolated agents at once | 50 concurrent writers in **21 MB, one process** vs 1,291 MB across 50 — plus whatever forkyard's persisted warm cache holds, which it loads at startup (~100 MB with a 14 MB cache file) |
| **forkyard** | Agents read the same hot contracts, or upstream is metered | cold: **37 upstream calls at any agent count** vs ~78 per Anvil process (778 at 10 agents); warm: **1** vs 3 startup calls per process, each Anvil reading its state from disk |
| **forkyard** | Agents need different fork blocks | one process, cost scaling with blocks not agents; Anvil needs a process per block |
| **forkyard** | State must outlive a process — restarts, handoffs, parked sessions | resume a 50-transaction session in **1.1 ms** after a restart, instead of 143 ms replaying it into a fresh forkyard session — Anvil's `anvil_dumpState`/`anvil_loadState` is on par here (5–6 ms at 10,000 slots, [benchmark.md](benchmark.md)); pinned warm start is ready in **8 ms** without waiting on upstream |
| **forkyard** | Dozens of concurrent agents, each with its own state | 50 agents through the standard workload in **1.1 s** vs 2.4 s — a worker never waits on upstream, so one agent's cold read can't stall another's (was 7.7 s) |
| **Anvil** | Rewinding a single timeline, fast | `evm_snapshot`/`evm_revert` under **1 ms** each, flat, in memory; forkyard's `snapshot` + `resume` is 6 ms at 100 dirty slots and 10 ms at 10,000, because it writes a durable file |
| **Anvil** | You need the full RPC surface or cheatcodes | forkyard covers `eth_call`, `eth_getCode` and `eth_getStorageAt`, but not tracing, logs/filters, or `evm_*` time travel |
| **Anvil** | One agent | every forkyard advantage here begins at "more than one" |

Medians of five warm runs on an Apple M3 Pro against a mainnet archive endpoint, all re-run 2026-09-30.
**[benchmark.md](benchmark.md)** has the methodology, the run-to-run spread, the
results that did not favour forkyard, and commands to reproduce each one.

## Build from source

```bash
git clone https://github.com/gabrielfior/forkyard
cd forkyard
RPC_URL=... cargo run -p forkyard
```
