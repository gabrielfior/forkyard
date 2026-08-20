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

Rust all the way — fork engine and API layer both. One Cargo workspace:

- `engine` — the persistent state map (`im`/`imbl`, structural sharing), session overlay, revm wiring, per-chain hardfork config.
- `fetch` — lazy remote fetch, built on `foundry-fork-db` (the crate Anvil/Forge actually depend on) rather than reinvented: its `SharedBackend` already bridges revm's synchronous execution with an async fetch task over channels. Our own code adds what it doesn't have — many sessions cheaply branching off one shared base.
- `ingest` — chain-tip follower, keeps the shared base snapshot current to the latest block.
- `session` — TTL lifecycle, thread-pool sharding. The only crate the API layer is allowed to call into.
- `api-mcp` — built on `rmcp`, the official Rust MCP SDK (async on tokio, spec-conformant, derives `inputSchema` from typed structs) — chosen specifically to foreclose the handshake bug that broke a prior MCP server on the Hermes dogfood target.
- `api-http` — a thin `axum` REST surface for the SDK case.
- `bin` — wires it together, loads `.env`.

1. **Chain-tip ingestion** — subscribes to `newHeads` via a single upstream RPC provider (configured via `.env`; target 2–3 providers with failover once volume justifies it).
2. **Persistent state map** — a bounded working-set cache, not a chain mirror. Only touched accounts/storage live here; anything else is fetched live and cached. Immutable and structurally shared, so forking a session is a pointer copy — O(1).
3. **Execution: revm, embedded directly** — not Anvil-as-a-process. Anvil *is* revm wrapped in a single-process node; spawning one per session loses the shared warm cache (the real cost advantage) and, at scale, means managing thousands of OS processes with none of the benefit.
4. **Lazy remote fetch** — via `foundry-fork-db`'s `SharedBackend`, cached into the shared base for the next fork.
5. **Session lifecycle & isolation** — TTL-keyed sessions, sharded across a fixed pool of worker **threads** (not processes) within one process. Corrected from an earlier pass that said "processes": OS processes don't share an address space, and the O(1)-fork trick depends on every worker holding the same `Arc` to the shared state map — threads share that for free, processes would force duplicating the cache per worker. Blast radius is handled by `catch_unwind` around every job (safe — revm has no unsafe/FFI in its hot path) plus per-session gas/time ceilings; a genuine process crash is covered by running multiple replicas per chain for capacity anyway, not by artificial process-per-worker isolation.
6. **Edge API + MCP server** — per-fork-second metering (Modal-style), `rmcp` for MCP, `axum` for HTTP.

**Scaling topology:** one instance (or small replica set) per chain — Ethereum mainnet, Base, Arbitrum each independent, since their caches don't share anything anyway. Within a chain, start with a single vertically-scaled instance to maximize the shared-cache benefit; split only once memory or cores actually bottleneck, not before.

**Compute model:** long-running Rust processes, not Lambda — a stateless invocation can't hold the `Arc` the whole speed advantage depends on. "Not a single EC2" is solved by a fleet of replicas behind a load balancer instead.

**Cross-replica caching:** local memory only for v1, no Redis on the hot path. An in-process read (~10–100ns) beats even same-host Redis (~0.1–1ms) by three-plus orders of magnitude, and revm does dozens to hundreds of reads per transaction — routing them through Redis would cost more latency than the speed differentiator can afford. Redis as a write-through warm-start feed for cold replicas is a reasonable later optimization, not a v1 need.

Launch chains: Ethereum mainnet, Base, Arbitrum.

## Competitive position & moat

Tenderly already ships an MCP server with 43 tools — including fork/branch, snapshot/revert, and simulate/send. Architecture alone isn't a moat; they could rebuild a shared-cache engine. What's actually defensible:

- A cross-tenant warm cache that gets cheaper as aggregate traffic grows — structurally awkward for a product built around siloed per-customer environments (real, but unproven against their internals).
- An enterprise/TU pricing model that structurally disincentivizes the incumbent from chasing a cheap, self-serve, transparent segment even if they technically can build it.
- Being simpler and cheaper specifically for the throwaway-many-forks-per-turn pattern their dashboard-first product wasn't built around.

A head start to compound, not a wall.

## Go-to-market

**Step zero:** dogfood on the founder's own Hermes (Nous Research) deployment — it already loads MCP servers via `mcp_servers.<name>` in `config.yaml`. Zero new infra, fastest feedback loop. Get the MCP handshake right from day one: a prior misconfigured MCP server on that same setup (no `initialize` handler, no JSON-RPC envelope, wrong schema key) cost 60s per agent init before anyone noticed.

Then, sequenced by public reach:

1. **MCP server** — framework-agnostic, reaches Claude Code, Cursor, LangChain (via its MCP adapter) at once; the most honest test of the price/latency claim against Tenderly's real product.
2. **ElizaOS plugin** — 17,600+ stars, 200+ plugins, the largest existing on-chain-agent audience. A `plugin-forkyard` next to the existing `plugin-evm`.
3. **GOAT SDK plugin** — ~1k stars, provider-agnostic tool catalog, examples already ship for LangChain and OpenAI's Agents SDK.
4. **Coinbase AgentKit action provider** — 1.3k stars, official backing, direct line to CDP wallet users.

## Cloud & deployment

Three production designs considered (full detail in `docs/RESEARCH.md`): bare VMs + systemd, **ECS Fargate** (chosen for least maintenance, ~15.6% cost premium over EC2 at a 6-replica fleet — worth it for no OS to patch), and Kubernetes (likely premature at this scale). Non-negotiable in any of them: single-sourced secrets, readiness checks that verify the chain-tip ingestion feed is actually live (not just "process up"), and leaning on the static-binary deploy story Rust already gives for free.

**Right now, though, this is a hobby project, not production** — budget ceiling $10/mo. Phase 0: **Hetzner CAX11** (2 vCPU/4 GiB ARM, $4.99/mo) running all three chain processes as systemd units, plus **Supabase free tier** ($0/mo) for the control plane only (auth, usage records) — Supabase Edge Functions don't fit the engine itself, same reason Lambda didn't. Total ~$5/mo. Scaling to the Fargate design later is a redeploy, not a rewrite — nothing in the software is cloud-specific.

## Docs

- [`docs/RESEARCH.md`](docs/RESEARCH.md) — full market research: landscape, gaps, EVM-vs-Solana decision, sources.
- [`docs/PITCH_OUTLINE.md`](docs/PITCH_OUTLINE.md) — slide-by-slide pitch deck outline.
