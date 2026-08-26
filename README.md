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
- **HTTP JSON-RPC**, default `http://127.0.0.1:8555` (`FORKYARD_PORT` to change it) — `POST /session` opens a session, then `POST /session/{id}` speaks normal Ethereum JSON-RPC (`eth_call`, `eth_sendRawTransaction`, etc.) against it (the `{id}` path segment selects the session). Works with `cast`, `alloy`, `web3.py`, or any wallet/client — see `python/examples` for a working `web3.py` demo.

The model: `fork()` → `simulate(tx)` (read-only) or `advance(tx)` (commits, but only into that session's own private overlay) → `discard()` or let the session's idle TTL expire (default 1 hour, `FORKYARD_SESSION_TTL_SECS` to change it). The real chain and the shared cache are never written to: `get_balance` after `simulate(tx)` is unchanged, after `advance(tx)` it reflects the transfer.

## Configuration

| Variable | Default | What it does |
| --- | --- | --- |
| `RPC_URL` | *(required)* | Upstream endpoint the fork reads real chain state from. |
| `FORKYARD_PORT` | `8555` | HTTP JSON-RPC port. |
| `FORKYARD_MCP_HTTP_PORT` | `8556` | MCP-over-Streamable-HTTP port. |
| `FORKYARD_SESSION_TTL_SECS` | `3600` | Idle lifetime of a session before it's reaped. |
| `FORKYARD_FORK_BLOCK_NUMBER` | *(unset)* | Pin the fork to an explicit historical block instead of following the chain tip. When set, the background chain-tip follower is **disabled** — re-forking to a newer block would defeat the point of pinning — so every session sees exactly that block for the process's whole lifetime. Reproducible runs (benchmarks, regression tests) want this; note that your `RPC_URL` must actually serve historical state at that height, which some public endpoints only do on a paid tier. |

## Gotchas

- **`gas_price` vs the fork's real basefee** — `advance`/`simulate` reject a `gas_price` below the forked block's basefee (the schema defaults it to `0`, which will fail on almost any live chain). There's no MCP tool for reading basefee — use `eth_gasPrice` on the JSON-RPC surface, running alongside MCP on the same process, which already includes a usable priority-fee margin.
- **Nonces aren't tracked for you** — `advance`'s `nonce` defaults to `0` and isn't auto-incremented; each successful call bumps the sender's nonce by 1. A reused nonce fails with `NonceTooLow`, a skipped-ahead one with `NonceTooHigh` — neither corrupts state. Check the current value via `get_balance`, which returns `nonce` alongside `balance`.
- **Balances aren't zeroed by default, and `set_balance` overwrites rather than credits** — an address you haven't called `set_balance` on keeps its real forked-chain balance; explicitly set every address you use (sender and receiver) for deterministic tests, and remember calling `set_balance` twice with the same value doesn't double it.
- **Addresses aren't checksum-validated** — mixed-case (EIP-55) and lowercase hex are both accepted as the same address.
- **Gas fees are burned to the zero address** — forkyard never sets a block beneficiary, so `advance`'s `gas_used * gas_price` debit lands on `0x0000…0000`, not a miner/validator.
- **Three cheatcodes live on the JSON-RPC surface, not just MCP** — alongside `forkyard_setBalance` there's `forkyard_setStorageAt(address, slot, value)`, which writes one raw storage slot in that session's overlay (how you mint yourself an ERC-20 balance: compute the `balanceOf` mapping slot and set it — see `python/benchmarks/backend.py`), and `forkyard_discard()`, the HTTP counterpart to the `discard` MCP tool for tearing a session down ahead of its TTL. All three affect only the calling session's overlay.

## Build from source

```bash
git clone https://github.com/gabrielfior/forkyard
cd forkyard
RPC_URL=... cargo run -p forkyard
```
