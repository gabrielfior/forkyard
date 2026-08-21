# Release / GTM plan

Positioning: **use this after Anvil.** For a single agent, Anvil is easier and enough — don't
compete with it there. The wedge is what breaks once there's more than one concurrent
agent/session: N Anvil processes, N cold RPC syncs, no shared warm cache, no native MCP
surface. Forkyard is one process, one shared warm base + fetch cache, session-per-agent
isolation, and both an HTTP JSON-RPC surface and an MCP-stdio surface out of the box.

## Landscape (GitHub stars, checked 2026-08-20)

| Project | Stars | Relevance |
|---|---|---|
| browser-use/browser-use | 109,965 | proof agent-tooling repos can go viral fast (GTM pattern reference, not EVM) |
| modelcontextprotocol/servers | 89,732 | the directory/registry layer — discovery surface, not a competitor |
| ethereum/go-ethereum | 51,305 | baseline EVM client, not a direct competitor |
| modelcontextprotocol/typescript-sdk | 13,219 | ecosystem infra |
| elizaOS/eliza | 19,112 | biggest "crypto AI agent" framework — real GTM channel |
| **foundry-rs/foundry (Anvil)** | **10,568** | **the incumbent "good enough" answer we're positioned after, not against** |
| NomicFoundation/hardhat | 8,505 | adjacent dev tooling, not agent-native |
| paradigmxyz/reth | 5,745 | infra, not comparable use case |
| bluealloy/revm | 2,220 | forkyard's own dependency — warm, technical audience |
| alloy-rs/alloy | 1,322 | infra |
| coinbase/agentkit | 1,284 | agent-wallet tooling, adjacent |
| goat-sdk/goat | 1,008 | multi-chain agent action library, adjacent |
| vyperlang/titanoboa | 313 | niche |
| matter-labs/era-test-node | 4 | shows how little traction a fork-node tool gets *without* an agent story |

Nothing in this list is "Anvil, but for concurrent AI-agent sessions." That's the gap.

## GTM moves

1. **Get listed in the MCP directories** — `mcp.so`, `mcpservers.org`, `glama.ai`, and the
   `awesome-blockchain-mcps` / `awesome-crypto-mcp-servers` GitHub lists. PR-sized effort,
   direct exposure to the actual target audience. Do this first.
2. **Publish "When Anvil stops being enough."** Target the exact search moment: someone
   already on Anvil for one agent, now hitting the N-processes wall building a swarm/eval
   harness. Post to r/ethdev, Farcaster crypto-dev channels, Hacker News. Frame as "the next
   tool after Anvil," not "better than Anvil."
3. **Ship an elizaOS plugin/example** (19k stars) — trading/DeFi agent builders there already
   need a dry-run sandbox before mainnet execution. A working example beats any post.
4. **Dogfood the revm community** (2,220 stars) — a short technical write-up on why forkyard
   needed the session/worker-pool architecture on top of revm. Cross-posts naturally to
   r/rust and Paradigm-adjacent circles.
5. **Target the multi-agent/eval crowd directly** — backtesting/eval harnesses, DeFi-agent
   red-teaming/safety researchers, hackathon organizers needing many cheap isolated sandboxes.
   Smaller, higher-intent audience; targeted outreach over broad content.

## Distribution / install (the "single binary" story)

Confirmed 2026-08-21: `forkyard-bin` is already architecturally one process — HTTP + MCP-stdio
simultaneously against one shared `SessionManager`. What's missing is *easy install*:
`Cargo.toml` has `publish = false`, no release workflow, no prebuilt binaries — today it's
`git clone` + `cargo build --release`, which requires a Rust toolchain. That's the actual gap
between "single binary" (done) and "simpler than Anvil to get" (not done).

Anvil's bar is `foundryup`: one curl command, no toolchain, prebuilt binary per platform.
Priority order to match/beat it for agent developers specifically:

1. **Prebuilt release binaries + one-line installer.** GitHub Actions matrix build (macOS
   arm64/x64, Linux x64/arm64) → GitHub Releases → a tiny `curl -fsSL .../install.sh | bash`.
   Removes the Rust-toolchain requirement — the single biggest friction point today. Do this
   first; everything else builds on it.
2. **npm wrapper package** (`@forkyard/cli`, `npx forkyard`). Eliza plugins live in npm-land.
   A thin package whose postinstall fetches the right binary from (1) — same trick as esbuild/
   swc/turbo/biome — lets someone add forkyard with zero Rust awareness. Highest-leverage step
   for the Eliza audience specifically.
3. **Zero-config startup that prints the MCP config block.** No args → sane defaults (free
   port, default poll interval) + a ready-to-paste `mcpServers` JSON snippet printed to stderr
   on boot. Makes "simpler than Anvil" a literal, demonstrable claim — Anvil never tells you
   how to wire itself into an MCP client.
4. **A first-class Eliza plugin**, not just raw MCP config — `@elizaos/plugin-forkyard` (or
   equivalent for whichever framework comes first) so adding forkyard is a character-file
   one-liner. Generic MCP support gets you "usable"; a native plugin gets you "path of least
   resistance."
5. **Docker image**, mainly for people who already containerize their agent stack. Lower
   priority than 1–2 (doesn't solve the Eliza-dev-laptop case), but cheap once release
   binaries exist.

Sequencing: 1 → 2 → 3, then 4, with 5 opportunistic.

## Engine correctness prerequisite (closed 2026-08-21)

Before any of the above matters, the chain-tip ingestion story had to actually hold up: v1
only relabeled the shared `BlockEnv`'s block number and left `forkyard-fetch`'s cached
account/storage state pinned forever. Fixed by having `forkyard-ingest`'s `ChainTipFollower`
re-fork an entirely fresh fallback backend (`SessionManager::refresh_fallback`) whenever the
polled block number actually advances, not just re-stamp the label — live-verified against
real mainnet blocks. See `crates/ingest/src/lib.rs` and `crates/session/src/lib.rs` for the
exact bound that remains (poll-interval staleness for new forks; already-forked sessions keep
their own snapshot for their lifetime, by design).
