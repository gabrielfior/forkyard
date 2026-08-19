# Forkyard

Instant, disposable forks of live EVM chain state, priced per second, built for AI agents that need to simulate before they act.

## Problem

Agents acting on-chain need to simulate a transaction before committing gas or capital. What exists today:

- **RPC bolt-ons** (Alchemy `eth_simulateV1`, QuickNode) — cheap, but shallow: single-transaction only, no sequential multi-tx bundle support against moving state.
- **Dedicated platforms** (Tenderly) — deep, but enterprise-sales-gated with opaque pricing, and built as persistent per-customer environments, not a cheap per-call primitive.
- **Wallet-native** (MetaMask Agent Wallet, Coinbase AgentKit/Agentic Wallets) — simulation bundled into a closed custody stack you don't control.

Nobody sells a cheap, transparent, disposable fork built for a machine calling it hundreds of times an hour mid-reasoning-loop.

## Why EVM, not Solana

Solana carries far more raw agent transaction volume (65–77% of on-chain agent payment volume via x402), but its sub-penny fees cap what anyone can charge per simulation call, and the fork/simulation gap there is already covered by Surfpool (free, Solana Foundation-backed) and RPC Fast (paid, ~20ms, already selling to AI agents). EVM's higher gas costs are exactly why Tenderly is a real, paying business — and why this is too.

## Solution

Sub-second, disposable, copy-on-write forks of EVM state:

```
fork(chain, block?) → simulate(tx) → advance(tx) [repeat] → discard() / TTL expiry
```

- `simulate` runs a transaction through revm read-only — nothing persists.
- `advance` runs it and writes the resulting diff into that session's *private* overlay only — the shared base cache and the real network are never touched. Broadcasting the real transaction is always a separate step the caller does with their own wallet.
- Every response reports the exact block it ran against. Staleness (the real chain moving past the forked block, or an invisible pending mempool tx) is an inherent limit of any simulate-then-broadcast pattern — not something engineering can fully close, only shrink via short TTLs and re-simulating right before a real broadcast.

## Architecture

1. **Chain-tip ingestion** — subscribes to `newHeads` via a single upstream RPC provider (configured via `.env`; target 2–3 providers with failover once volume justifies it), keeps one warm base snapshot per chain current to the latest block.
2. **Persistent state map** — a bounded working-set cache, not a chain mirror. Only touched accounts/storage live here; anything else is fetched live via `eth_getProof` and cached, exactly like Anvil's fork mode. Immutable and structurally shared, so forking a session is a pointer copy — O(1).
3. **Execution: revm, embedded directly** — not Anvil-as-a-process. Anvil *is* revm wrapped in a single-process node; spawning one Anvil per session loses the shared warm cache (the real cost advantage) and, at scale, means managing thousands of OS processes with none of the benefit. We embed revm's own `CacheDB` primitive — the same one Anvil's fork mode already uses — inside a thin custom node layer instead.
4. **Lazy remote fetch** — cache misses pulled live from the upstream RPC, cached into the shared base for the next fork.
5. **Session lifecycle** — TTL-keyed sessions, sharded across a fixed pool of worker processes (one per core) rather than one process per session. revm's own sandboxing is the correctness boundary within a worker; sharding bounds a crash's blast radius to a fraction of active sessions instead of all of them or exactly one. (Chosen after a prior process-per-fork attempt broke on exactly this: thousands of processes to manage, `--dump-state` hard to track, and spawn latency on every fork.)
6. **Edge API + MCP server** — auth, per-fork-second metering (Modal-style: metered by wall-clock seconds a session stays alive, no idle charge beyond the TTL window), a thin TS/Python SDK, and an MCP tool definition so agent frameworks call it directly.

Launch chains: Ethereum mainnet, Base, Arbitrum.

## Competitive position & moat

Tenderly already ships an MCP server with 43 tools — including fork/branch, snapshot/revert, and simulate/send. Architecture alone isn't a moat; they could rebuild a shared-cache engine. What's actually defensible:

- A cross-tenant warm cache that gets cheaper as aggregate traffic grows — structurally awkward for a product built around siloed per-customer environments (real, but unproven against their internals).
- An enterprise/TU pricing model that structurally disincentivizes the incumbent from chasing a cheap, self-serve, transparent segment even if they technically can build it.
- Being simpler and cheaper specifically for the throwaway-many-forks-per-turn pattern their dashboard-first product wasn't built around.

A head start to compound, not a wall.

## Go-to-market

Sequenced by reach:

1. **MCP server** — framework-agnostic, reaches Claude Code, Cursor, LangChain (via its MCP adapter) at once; the most honest test of the price/latency claim against Tenderly's real product.
2. **ElizaOS plugin** — 17,600+ stars, 200+ plugins, the largest existing on-chain-agent audience. A `plugin-forkyard` next to the existing `plugin-evm`.
3. **GOAT SDK plugin** — ~1k stars, provider-agnostic tool catalog, examples already ship for LangChain and OpenAI's Agents SDK.
4. **Coinbase AgentKit action provider** — 1.3k stars, official backing, direct line to CDP wallet users.

## Docs

- [`docs/RESEARCH.md`](docs/RESEARCH.md) — full market research: landscape, gaps, EVM-vs-Solana decision, sources.
- [`docs/PITCH_OUTLINE.md`](docs/PITCH_OUTLINE.md) — slide-by-slide pitch deck outline.
