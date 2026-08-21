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
  Tools: `fork`, `simulate`, `advance`, `get_balance`, `set_balance`, `discard`.
- **MCP over HTTP** (Streamable HTTP transport), default `http://127.0.0.1:8556/mcp` (`FORKYARD_MCP_HTTP_PORT` to change the port) — for any client that isn't launching forkyard as a subprocess: a browser-based agent, a teammate's [mcp-cli](https://github.com/philschmid/mcp-cli), a client on another machine. Same tools as above. mcp-cli config:
  ```json
  { "mcpServers": { "forkyard": { "url": "http://127.0.0.1:8556/mcp" } } }
  ```
  Unlike the stdio config, this points at an already-running `forkyard` process — start it first, then run `mcp-cli` against it. That also means the session survives across separate `mcp-cli call` invocations, since `fork()`'s session lives in that one long-running process rather than a fresh one spawned per call.
- **HTTP JSON-RPC**, default `http://127.0.0.1:8555` (`FORKYARD_PORT` to change it) — `POST /session` opens a session, then `POST /session/{id}` speaks normal Ethereum JSON-RPC (`eth_call`, `eth_sendRawTransaction`, etc.) against it. Works with `cast`, `alloy`, `web3.py`, or any wallet/client — see `python/examples` for a working `web3.py` demo.

The model: `fork()` → `simulate(tx)` (read-only) or `advance(tx)` (commits, but only into that session's own private overlay) → `discard()` or let the TTL expire. The real chain and the shared cache are never written to.

## Build from source

```bash
git clone https://github.com/gabrielfior/forkyard
cd forkyard
RPC_URL=... cargo run -p forkyard
```
