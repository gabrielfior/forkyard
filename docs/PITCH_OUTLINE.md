# Pitch deck outline

Seed-stage structure, drafted from `RESEARCH.md`. Slides 11 and 12 are placeholders — team composition and the actual ask aren't something to draft without the founder's input.

1. **Title** — Forkyard: instant, disposable forks of live chain state, for agents. *Per-second, self-serve, built for the "simulate before you act" pattern agents already need.*

2. **Problem** — Agents acting on-chain need to simulate before committing gas or capital. What exists is shallow (RPC bolt-on `eth_simulateV1`), deep but enterprise-gated with opaque pricing (Tenderly), or bundled into a closed wallet stack you don't control (MetaMask, Coinbase). Nobody sells a cheap, transparent, disposable fork built for a machine calling it hundreds of times an hour.

3. **Why now** — 65–77% of agent payment volume already runs through Solana via x402; ERC-8004 (agent identity, backed by the Ethereum Foundation, MetaMask, Google, Coinbase) went live on EVM mainnet January 2026. Agents are becoming a first-class on-chain actor class needing infra built for machine pacing, not human dashboards.

4. **Solution** — Sub-second, disposable, copy-on-write forks of EVM state. Billed per second, Modal-style. Called directly by agent frameworks via an MCP tool / SDK — no dashboard in the loop.

5. **How it works** — The 6-layer stack: chain-tip ingestion → shared, bounded working-set cache → revm embedded per session, sharded across a fixed worker pool → lazy remote fetch → TTL session lifecycle → MCP/edge API surface. Lifecycle: `fork → simulate → advance → discard`.

6. **Market & chain choice** — EVM over Solana, deliberately: Solana carries more raw agent volume, but sub-penny fees cap what anyone can charge per simulation, and that gap is already covered by Surfpool (free) and RPC Fast (paid, ~20ms). EVM's higher gas costs are exactly why Tenderly is a real business — and why this is too.

7. **Competitive landscape** — Three tiers: RPC bolt-ons (shallow), dedicated platforms (Tenderly — deep, enterprise-gated, already runs an MCP server with 43 tools), wallet-native (MetaMask/Coinbase — bundled, closed). Position: cheaper, transparent, ephemeral-native, self-serve.

8. **Why we can win** — Technique alone isn't a moat, and Tenderly already reaches agents via MCP. What's left: a cross-tenant warm cache that gets cheaper as aggregate traffic grows, an enterprise/TU pricing model that disincentivizes the incumbent from chasing this segment, and being simpler/cheaper for the throwaway-many-forks-per-turn pattern specifically. A head start to compound, not a wall.

9. **Business model** — Per-second billing, self-serve, transparent, vs. Tenderly's opaque TU units and sales-gated pricing. Single RPC provider at launch, roadmap to 2–3 for resilience.

10. **Go-to-market** — MCP server first (widest reach, tests the price/latency claim directly) → ElizaOS plugin (17,600+ stars, largest on-chain-agent audience) → GOAT SDK (~1k stars, multi-framework) → Coinbase AgentKit (1.3k stars, institutional channel).

11. **Team** — *placeholder.*

12. **The ask** — *placeholder.*
