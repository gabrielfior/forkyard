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
- **Isolation boundary** — a fixed pool of worker processes (one per core), sessions hashed across it — not one process per session. revm's own sandboxing is the correctness boundary within a worker; sharding bounds a crash's blast radius. Chosen directly in response to a prior process-per-fork attempt that broke on `--dump-state` being hard to track, thousands of processes to manage, and spawn latency.

## State semantics (the part easy to get wrong)

- `simulate` runs a transaction through revm read-only — the result is returned, the session's overlay is untouched, nothing persists.
- `advance` runs the same execution but writes the resulting diff into that session's private overlay only. The shared base cache and the real network are never touched. Broadcasting the real transaction for real is always a separate step the caller does with their own wallet and RPC — this service never submits anything itself.
- What this doesn't guarantee: revm's execution is faithful to whatever state it's given, but a session is pinned to the block it forked at. By the time a caller broadcasts for real, the chain may have moved, and a pending mempool transaction neither we nor any other simulator can see may land first. Every response reports the exact block it ran against; the default SDK pattern re-simulates against current head immediately before a real broadcast. This shrinks the staleness window to seconds — it cannot close it to zero, and no simulator (Tenderly, Anvil, `eth_simulateV1` included) can.

## Integration path

Ranked by reach, not by how open the lane is (Tenderly's own MCP server means the protocol-level channel is already contested):

1. **MCP server** (protocol-level) — framework-agnostic, reaches Claude Code, Cursor, LangChain (via its MCP adapter) at once. Widest reach per unit of effort, and the most honest test of the price/latency claim against Tenderly's real product.
2. **ElizaOS plugin** (largest audience) — 17,600+ stars, 5,300+ forks, 200+ plugins, the closest thing to a standard for on-chain agents. A `plugin-forkyard` next to the existing `plugin-evm`.
3. **GOAT SDK plugin** (multi-framework) — ~1k stars, provider-agnostic tool catalog with first-class third-party plugins; examples ship for LangChain and OpenAI's Agents SDK.
4. **Coinbase AgentKit action provider** (institutional) — 1.3k stars, official backing, direct line to CDP wallet users.

Sequence: MCP server first, then ElizaOS. GOAT and AgentKit follow once one of the first two proves the economics work.

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
