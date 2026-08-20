# Research memo: forking chain state on demand

Origin: prompted by Cursor's "Git at any scale" retrospective on treating Git hosting as a database problem — the question was whether other systems heavily used by AI agents are similarly under-optimized. Transaction simulation / chain-state forking was the candidate that held up under scrutiny.

## The landscape, three tiers deep

**1. RPC bolt-on** — cheap, shallow, bundled into a node subscription.
- Alchemy — `eth_simulateV1`, bundled with the API subscription.
- QuickNode — same primitive, same limitation.

Fine for "will this one transaction revert," useless for a sequence of transactions against state that's moving block to block.

**2. Dedicated fork platform** — deep simulation, gated behind a sales call.
- Tenderly — Virtual TestNets + Simulation API, 400 TU per call. Public self-serve pricing was pulled in favor of "send us your stack."
- Foundry / Anvil — free and local, but single-tenant. No multi-agent, multi-tenant fork-per-second story.

**3. Wallet-native** — simulation as a default, bundled feature, not a product.
- MetaMask — Agent Wallet: mandatory simulate → Blockaid threat-scan → MEV-protect pipeline, already wired into Claude Code / Cursor.
- Coinbase — AgentKit / Agentic Wallets, moving the same direction.
- Blowfish / Blockaid — the "is this a scam" layer, acquired by Phantom (absorbed into a wallet too).

## Where it actually breaks

- **pricing-opacity** — Tenderly, the deepest offering, no longer publishes self-serve dollar pricing. An agent calling a fork API programmatically has no clean price to build a cost model against.
- **no-ephemeral-primitive** — nobody sells "cheap, disposable, copy-on-write fork of live state, forked mid-reasoning-loop and thrown away a second later." Virtual TestNets are priced and shaped like a persistent environment.
- **multi-tx-bundle-gap** — simulating one transaction against a snapshot is solved. Simulating a sequence against state that's itself changing block by block — what a trading or arb agent needs — is still bolted on via a third party.
- **category-fragmentation** — dev-simulation ("will it revert") and safety-simulation ("is this a drain") are sold by different vendors to different buyers. An autonomous agent needs both from one call; nobody fuses them outside a closed wallet stack.
- **evm-only** — every *paid, dedicated* simulation platform is EVM-first, which first looked like Solana whitespace. It isn't: Surfpool (Solana Foundation, free) and RPC Fast (paid, ~20ms, already selling to AI agents) already cover most of that ground. The real gap stays on the EVM side.

## Four directions considered

- **A — Forkyard: ephemeral forks as infrastructure.** *Chosen.* Attacks no-ephemeral-primitive and pricing-opacity. First buyer: teams building agent frameworks on MCP/LangChain. Biggest risk: Tenderly already ships an MCP server with 43 tools covering fork/branch/snapshot/revert/simulate/send — the distribution channel is contested today, not just a future threat.
- **B — Bundle-native simulator for trading agents.** Multi-tx, mempool-aware simulation for arb/trading agents. Narrow, sophisticated buyer pool.
- **C — One call: simulate + screen.** Fuses dev-simulation and safety-screening into one open API. Competes with free, bundled incumbents on their home turf.
- **D — Fork-as-a-primitive for non-EVM chains.** *Not chosen.* Solana's sub-second, sub-penny transactions are exactly why agents already route most on-chain agent volume there — but that same cheapness caps what anyone can charge per simulation call, and Surfpool/RPC Fast already cover the gap.
- *(A cross-chain combination of A+D was considered and rejected — the two backends are architecturally unrelated, roughly doubling v1 engineering cost for a synergy that turned out not to exist once D's competitive picture came into focus.)*

## The moat, honestly

Architecture alone isn't defensible — Tenderly could rebuild a shared-cache engine, and their MCP server already exposes fork/simulate/snapshot/revert to agents today. What's left:

- A cross-tenant warm cache that gets cheaper as aggregate traffic grows — real, but unproven against their internals, and structurally awkward for a product built around siloed per-customer environments.
- A self-serve, per-second price their enterprise/TU business model is structurally reluctant to chase, even if they technically can build the tech.
- Being simpler and cheaper specifically for the throwaway-many-forks-per-turn pattern their dashboard-first product wasn't built around.

A head start to compound, not a wall.

## System design decisions (resolved)

- **Pricing unit** — per-second, Modal-style. Metered by wall-clock seconds a session stays alive, not a coarse per-call unit like Tenderly's TU. No idle charge beyond the TTL window.
- **Upstream RPC dependency** — single provider at launch, configured via `.env`. Target 2–3 providers with failover once real traffic justifies the cost and complexity.
- **Isolation boundary** — *resolved, then corrected.* A fixed pool of worker **threads** within one process, not worker processes as first stated: OS processes don't share an address space, and the O(1)-fork trick depends on every worker holding the same `Arc` to the shared immutable state map. Threads share that memory for free; processes would force either duplicating the warm cache per worker (the exact problem being solved) or a custom shared-memory scheme. Blast radius: `catch_unwind` around every job (safe, since revm has no unsafe/FFI in its hot path) plus per-session gas/time ceilings. A genuine process-level crash still takes the instance down — covered by running multiple replicas per chain for capacity anyway, not by artificially isolating workers that need to share memory to do their job. (Originally chosen in response to a prior process-per-fork attempt that broke on `--dump-state` being hard to track, thousands of processes to manage, and spawn latency — that reasoning still holds against *process-per-session*, just not against threads-vs-processes for the worker pool itself.)
- **Language & stack** — Rust all the way, fork engine and API layer both, one Cargo workspace (`engine`, `fetch`, `ingest`, `session`, `api-mcp`, `api-http`, `bin`). `fetch` builds on `foundry-fork-db` (the crate Anvil/Forge actually depend on) rather than reinventing the sync-revm/async-fetch bridge. `api-mcp` builds on `rmcp`, the official Rust MCP SDK — chosen specifically to foreclose the handshake bug that broke a prior MCP server on the Hermes dogfood target (missing `initialize`, no JSON-RPC envelope, wrong schema key). `api-mcp`/`api-http` only call into `session`'s public interface — "thin" as a compile-time boundary, not just a stated intention.
- **Scaling topology** — one instance (or small replica set) per chain (Ethereum mainnet, Base, Arbitrum independently), since their caches share nothing anyway. Within a chain, start with one vertically-scaled instance to maximize the shared-cache benefit; split only once memory or cores actually bottleneck.
- **Compute model** — long-running Rust processes, not Lambda. The entire speed advantage rests on forking being an in-memory `Arc` clone; a stateless Lambda invocation can't hold that across calls, so every state read would become either a per-call round trip to an external DB or a cold bulk-fetch at invocation start — reintroducing the exact "cold, siloed cache" problem already rejected for Anvil-per-process, just serverless-shaped. "Not a single EC2" is solved by a small fleet of replicas behind a load balancer instead — redundancy without giving up in-memory speed.
- **Cross-replica cache sharing** — local memory only for v1, no Redis on the hot path. Replicas already don't share memory with each other (only worker threads inside one process do), so Redis wouldn't be fixing a safety risk, it would be syncing already-independent caches. An in-process read is ~10–100 nanoseconds; even same-host Redis is roughly 0.1–1ms — a thousand-plus times slower — and revm makes dozens to hundreds of state reads per transaction, so routing all of them through Redis could plausibly cost more latency than the "faster than Tenderly" pitch can afford. The real, smaller cost this would fix — redundant upstream fetches and cold-start latency on new replicas — is worth solving later with Redis as a write-through warm-start feed for each replica's local cache (populated lazily on a miss, never on the per-transaction read path itself). Not needed at current scale.

## State semantics (the part easy to get wrong)

- `simulate` runs a transaction through revm read-only — the result is returned, the session's overlay is untouched, nothing persists.
- `advance` runs the same execution but writes the resulting diff into that session's private overlay only. The shared base cache and the real network are never touched. Broadcasting the real transaction for real is always a separate step the caller does with their own wallet and RPC — this service never submits anything itself.
- What this doesn't guarantee: revm's execution is faithful to whatever state it's given, but a session is pinned to the block it forked at. By the time a caller broadcasts for real, the chain may have moved, and a pending mempool transaction neither we nor any other simulator can see may land first. Every response reports the exact block it ran against; the default SDK pattern re-simulates against current head immediately before a real broadcast. This shrinks the staleness window to seconds — it cannot close it to zero, and no simulator (Tenderly, Anvil, `eth_simulateV1` included) can.

## Integration path

**Step zero, before any public reach — dogfood on Hermes.** Gabriel's own Nous Research Hermes deployment (EC2, `hermes-gateway`) already loads MCP servers via `mcp_servers.<name>` blocks in `config.yaml` — the same mechanism Claude Code and Cursor use. The moment the fork-engine MCP server exists, point Hermes at it, restart `hermes-gateway`, confirm via the `MCP: registered N tool(s)` log line. Zero new infra, fastest possible feedback loop. Real gotcha already hit once on this exact setup: a broken handshake (missing `initialize` handler, no JSON-RPC envelope, wrong `inputSchema` key instead of MCP's required key) cost 60 seconds per agent init and 1,593 failed connects before anyone noticed — get the protocol handshake right from day one, or this is the first thing that breaks.

Ranked by public reach, not by how open the lane is (Tenderly's own MCP server means the protocol-level channel is already contested):

1. **MCP server** (protocol-level) — framework-agnostic, reaches Claude Code, Cursor, LangChain (via its MCP adapter) at once. Widest reach per unit of effort, and the most honest test of the price/latency claim against Tenderly's real product.
2. **ElizaOS plugin** (largest audience) — 17,600+ stars, 5,300+ forks, 200+ plugins, the closest thing to a standard for on-chain agents. A `plugin-forkyard` next to the existing `plugin-evm`.
3. **GOAT SDK plugin** (multi-framework) — ~1k stars, provider-agnostic tool catalog with first-class third-party plugins; examples ship for LangChain and OpenAI's Agents SDK.
4. **Coinbase AgentKit action provider** (institutional) — 1.3k stars, official backing, direct line to CDP wallet users.

Sequence: MCP server first, then ElizaOS. GOAT and AgentKit follow once one of the first two proves the economics work.

## Cloud & deployment: three designs

These differ in how much operational machinery gets taken on for what's currently one stateful Rust process per chain — not in cloud provider.

1. **Bare VMs + systemd** — *fits current scale.* One systemd-managed Rust binary per chain-shard instance, a small fixed fleet (say two per chain) behind a basic load balancer for availability, not cache-sharing — each instance's warm cache is independent. Deploy is: build the binary, restart the service. Ops model: the same pattern already running the Hermes deployment on EC2 — systemd unit, SSH to debug, nothing new to learn. Downside: scaling past a handful of instances per chain becomes real toil, fine today, not indefinitely.
2. **Managed container platform** (Fly.io / ECS Fargate) — a minimal container image (static Rust binary, distroless base) on a platform handling rolling deploys, health checks, and secrets without owning a Kubernetes control plane. Downside: a new platform's abstractions to learn, less transparent to debug than one process over SSH.
3. **Kubernetes** (EKS/GKE) — full container orchestration, HPA per chain-service. The standard shape once this grows into many services around the core engine (billing, dashboard, multi-region). Downside: real overhead (upgrades, RBAC, networking policy) a handful of stateful instances per chain doesn't yet justify — likely premature today.

**Non-negotiable regardless of which design:**
- Secrets single-sourced — one place for the RPC provider key, referenced everywhere else, not duplicated across config files (exactly how a token got leaked into a transcript and left in backup files on the Hermes box before).
- Readiness checks that verify the chain-tip ingestion websocket is actually connected and current, not just "process is up" — an instance can look healthy while silently serving increasingly stale forks, the same failure shape as a model-provider chain dying quietly with nothing alerting on it.
- A static Rust binary means no interpreter/virtualenv drift — none of the "an upgrade silently reverted my patch" or "python3 vs the venv" class of deploy failure.

## Sources

- [Tenderly Virtual TestNet pricing](https://docs.tenderly.co/virtual-testnets/pricing) · [Tenderly pricing](https://tenderly.co/pricing)
- [Multi-tx bundle simulation with Anvil](https://www.degencode.com/p/multi-transaction-bundle-simulation-with-anvil)
- [MetaMask Agent Wallet](https://metamask.io/agent-wallet) · [architecture](https://docs.metamask.io/agent-wallet/reference/architecture/)
- [Phantom acquires Blowfish](https://phantom.com/learn/blog/phantom-acquires-blowfish) · [Blowfish simulation-challenge case study](https://www.coinspect.com/blog/transaction-simulation-challenges/)
- [ZenGo red-pill simulation findings](https://www.techtarget.com/searchsecurity/news/365533432/ZenGo-finds-transaction-simulation-flaw-in-Coinbase-others)
- [Alchemy eth_simulateV1](https://www.alchemy.com/docs/chains/ethereum/ethereum-api-endpoints/eth-simulate-v-1) · [QuickNode eth_simulateV1](https://www.quicknode.com/docs/ethereum/eth_simulateV1)
- [Nava seed funding](https://fortune.com/2026/04/14/nava-seed-funding-ai-financial-agents/)
- [Coinbase Agentic Wallets](https://www.coinbase.com/developer-platform/discover/launches/agentic-wallets)
- [65% of agentic payments on Solana](https://cryptonews.net/news/blockchain/32917016/) · [Solana AI Agents 2026 Playbook](https://solanareport.com/solana-ai-agents-2026-playbook)
- [Solana Agent Kit (SendAI)](https://github.com/sendaifun/solana-agent-kit)
- [ERC-8004 launch](https://www.gate.com/learn/articles/erc-8004-launches-on-ethereum-to-power-identity-and-trust-for-autonomous-ai-agents)
- [Surfpool docs](https://solana.com/docs/tools/surfpool) · [LiteSVM / Bankrun deprecation](https://kevinheavey.github.io/solana-bankrun/)
- [RPC Fast transaction simulator](https://rpcfast.com/transaction-simulator)
- [Tenderly MCP server (43 tools)](https://claudemarketplaces.com/mcp/co.tenderly/tenderly-mcp)
- [ElizaOS plugin-evm](https://github.com/elizaos-plugins/plugin-evm) · [GOAT SDK](https://github.com/goat-sdk/goat) · [Coinbase AgentKit](https://github.com/coinbase/agentkit)
- [evm-mcp-server](https://github.com/mcpdotdirect/evm-mcp-server) · [awesome-blockchain-mcps](https://github.com/royyannick/awesome-blockchain-mcps)
