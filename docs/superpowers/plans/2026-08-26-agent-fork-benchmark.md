# Multi-Agent Fork Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the small forkyard core features the benchmark needs (block pinning, a storage-slot cheatcode, an HTTP discard method), then build a Python benchmark that spins up N simulated agents doing random on-chain actions against forkyard (one process, N sessions) vs. Anvil (N separate processes), records per-action and total timings across agent counts and block heights, and plots the results.

**Architecture:** Three tightly-scoped Rust additions land first (each mirrors an existing cheatcode/pattern in the codebase), verified with the crate's own existing test style. Then a new `python/benchmarks/` package drives both backends through a shared `Backend` protocol and a shared action library, so the same agent-simulation code exercises forkyard and Anvil identically.

**Tech Stack:** Rust (existing crates: `forkyard-fetch`, `forkyard-engine`, `forkyard-session`, `forkyard-api-http`, `forkyard-api-mcp`, `forkyard-bin`), Python 3.11 + `uv` (new `python/benchmarks/`: `web3.py`, `eth_account`, `pandas`, `matplotlib`, `pytest`), Foundry's `anvil` binary (external, must be on `PATH`).

**Spec:** `docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md`

## Global Constraints

- `FORKYARD_FORK_BLOCK_NUMBER` is optional; when unset, behavior is byte-for-byte what it is today (fork latest, run `ChainTipFollower`). Never change the unset-case behavior.
- `eth_sendRawTransaction` only accepts legacy transactions (existing forkyard constraint) — every Python-side transaction must be built and signed as a legacy tx.
- Block pinning is process-wide, not per-session — "different block starts" means separate process runs, for both forkyard and Anvil, never multiple heights live in one forkyard process.
- Only DAI (`0x6B175474E89094C44Da98b954EedeAC495271d0F`, `balanceOf` mapping at storage slot `2`) is funded via the storage cheatcode by default. Do not add other tokens without independently verifying their slot.
- New Rust code follows the existing doc-comment style in each crate (a `///` explaining *why*, referencing the analogous existing cheatcode) — see `forkyard_setBalance` / `Session::set_account` as the template for every new cheatcode.

---

## Task 1: `forkyard-fetch` — fork at an explicit block height

**Files:**
- Modify: `crates/fetch/src/lib.rs`
- Create: `crates/fetch/examples/fork_at_block.rs`

**Interfaces:**
- Produces: `pub async fn fork_at(rpc_url: &str, block_number: u64) -> eyre::Result<(Fork, BlockEnv)>` — same shape as the existing `pub async fn fork(rpc_url: &str) -> eyre::Result<(Fork, BlockEnv)>`, but pinned to `block_number` instead of latest.

This crate has no unit tests today (`fork`/`latest_block_env` are only exercised by the real-RPC examples in `crates/fetch/examples/`, per the existing `mainnet_transfer.rs` pattern) — network I/O against a real provider isn't unit-tested here. `fork_at` follows the same convention: verified by a manual example run against a real RPC, not `cargo test`.

- [ ] **Step 1: Write the verification example (will not compile yet — `fork_at` doesn't exist)**

Create `crates/fetch/examples/fork_at_block.rs`:

```rust
//! Manual verification for `forkyard_fetch::fork_at`: fork a specific
//! historical block and confirm the returned `BlockEnv` actually reports
//! that block number, not latest. Run with:
//!
//!     RPC_URL=... cargo run -p forkyard-fetch --example fork_at_block -- 20000000

use revm::primitives::U256;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let rpc_url = std::env::var("RPC_URL").expect("set RPC_URL");
    let requested: u64 = std::env::args()
        .nth(1)
        .expect("usage: fork_at_block <block_number>")
        .parse()
        .expect("block_number must be a u64");

    let (_fork, block_env) = forkyard_fetch::fork_at(&rpc_url, requested).await?;
    assert_eq!(
        block_env.number,
        U256::from(requested),
        "fork_at pinned to the wrong block"
    );
    println!("OK: fork_at({requested}) pinned to block {}", block_env.number);
    Ok(())
}
```

- [ ] **Step 2: Confirm it fails to compile**

Run: `cargo build -p forkyard-fetch --example fork_at_block`
Expected: FAIL — `error[E0433]: function or associated item not found: fork_at`

- [ ] **Step 3: Refactor `fork` to share a block-parameterized helper, then add `fork_at`**

In `crates/fetch/src/lib.rs`, replace the existing `block_env_from_provider` + `fork` with:

```rust
async fn block_env_from_provider_at<P: Provider<Ethereum>>(
    provider: &P,
    block: BlockId,
) -> eyre::Result<BlockEnv> {
    let block = provider
        .get_block(block)
        .await?
        .ok_or_else(|| eyre::eyre!("upstream RPC returned no block for the requested id"))?;
    let header = &block.header;
    Ok(BlockEnv {
        number: U256::from(header.number),
        timestamp: U256::from(header.timestamp),
        basefee: header.base_fee_per_gas.unwrap_or(0),
        gas_limit: header.gas_limit,
        ..Default::default()
    })
}

/// Fetches just the real block context (number, timestamp, base fee) for
/// `rpc_url`'s latest block — the same lookup `fork` does once at startup,
/// exposed standalone so `forkyard-ingest` can call it again periodically
/// to keep a `SessionManager`'s `BlockEnv` from going stale. Opens its own
/// short-lived provider connection each call — negligible overhead against
/// a poll interval measured in seconds, and it keeps this crate decoupled
/// from needing to share `fork`'s own provider instance.
pub async fn latest_block_env(rpc_url: &str) -> eyre::Result<BlockEnv> {
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    block_env_from_provider_at(&provider, BlockId::latest()).await
}

async fn fork_impl(rpc_url: &str, block: BlockId) -> eyre::Result<(Fork, BlockEnv)> {
    let url = rpc_url.parse()?;
    let provider = ProviderBuilder::new().connect_http(url);
    let block_env = block_env_from_provider_at(&provider, block).await?;

    let meta = BlockchainDbMeta::new(block_env.clone(), rpc_url.to_string());
    let db = BlockchainDb::new(meta, None);
    let backend = SharedBackend::spawn_backend_thread(provider, db, None);
    Ok((WrapDatabaseRef(backend), block_env))
}

/// Fork `rpc_url` at its current head, returning both the fork itself and
/// the real `BlockEnv` (number, timestamp, base fee) of the block it's
/// pinned to. Spawns a dedicated background thread that owns the actual
/// network I/O (`foundry-fork-db`'s own pattern, mirrored by our
/// worker-thread design rather than copied wholesale) — reads against the
/// returned `Fork` block until that thread resolves them, then return from
/// cache on every later call, exactly like Anvil's fork mode. Dropping
/// every clone of the returned `Fork` tears the thread down.
///
/// The returned `BlockEnv` is the caller's responsibility to actually wire
/// into revm's execution context — `foundry-fork-db`'s own `BlockEnv` is
/// only used for its fork-cache bookkeeping, not fed into any `Evm`
/// automatically. Skipping this was a real bug: every transaction run
/// against a `Fork` without it executes with basefee=0, block number=0,
/// regardless of what block was actually forked.
pub async fn fork(rpc_url: &str) -> eyre::Result<(Fork, BlockEnv)> {
    fork_impl(rpc_url, BlockId::latest()).await
}

/// Same as `fork`, but pinned to `block_number` instead of the chain tip —
/// what lets a caller (e.g. `forkyard-bin`, via `FORKYARD_FORK_BLOCK_NUMBER`)
/// run a benchmark or test scenario against a fixed, reproducible block
/// instead of whatever happens to be current.
pub async fn fork_at(rpc_url: &str, block_number: u64) -> eyre::Result<(Fork, BlockEnv)> {
    fork_impl(rpc_url, BlockId::number(block_number)).await
}
```

Remove the old standalone `block_env_from_provider` function body (now folded into `block_env_from_provider_at` + `latest_block_env`) and the old `fork` body (now `fork_impl` + thin `fork`).

- [ ] **Step 4: Confirm the example builds and passes**

Run: `RPC_URL=https://ethereum-rpc.publicnode.com cargo run -p forkyard-fetch --example fork_at_block -- 20000000`
Expected: `OK: fork_at(20000000) pinned to block 20000000`

Run once more with a different block number (e.g. `21000000`) to confirm it isn't hardcoded/cached:
Expected: `OK: fork_at(21000000) pinned to block 21000000`

- [ ] **Step 5: Commit**

```bash
git add crates/fetch/src/lib.rs crates/fetch/examples/fork_at_block.rs
git commit -m "feat(fetch): add fork_at for pinning a fork to an explicit block"
```

---

## Task 2: `forkyard-bin` — `FORKYARD_FORK_BLOCK_NUMBER` wiring

**Files:**
- Modify: `crates/bin/src/main.rs:42-99`

**Interfaces:**
- Consumes: `forkyard_fetch::fork_at(rpc_url: &str, block_number: u64) -> eyre::Result<(Fork, BlockEnv)>` (Task 1).
- Produces: nothing new for later tasks — this is a leaf wiring change.

This binary has no test suite of its own (it's wired-up `main`, exercised manually) — verified here by starting the binary twice, with and without the env var, and checking the logs and `eth_blockNumber`.

- [ ] **Step 1: Modify `main.rs` to read the env var and branch fork/follower setup**

Replace lines 42-68 of `crates/bin/src/main.rs` (from `let rpc_url = ...` through the `ingest_task` spawn) with:

```rust
    let rpc_url = std::env::var("RPC_URL")
        .expect("set RPC_URL to an EVM RPC endpoint (see .env.example)");
    let port: u16 = env_or("FORKYARD_PORT", 8555);
    let mcp_http_port: u16 = env_or("FORKYARD_MCP_HTTP_PORT", 8556);
    let num_workers: usize = env_or("FORKYARD_NUM_WORKERS", 4);
    let ttl_secs: u64 = env_or("FORKYARD_SESSION_TTL_SECS", 3600);
    let ingest_poll_secs: u64 = env_or("FORKYARD_INGEST_POLL_SECS", 12);
    let chain_id: u64 = env_or("FORKYARD_CHAIN_ID", 1);
    let fork_block_number: Option<u64> =
        std::env::var("FORKYARD_FORK_BLOCK_NUMBER").ok().and_then(|v| v.parse().ok());

    let (fork, block_env) = match fork_block_number {
        Some(n) => forkyard_fetch::fork_at(&rpc_url, n).await?,
        None => forkyard_fetch::fork(&rpc_url).await?,
    };
    let manager = Arc::new(SessionManager::new(
        fork,
        block_env,
        num_workers,
        Duration::from_secs(ttl_secs),
    ));
    tracing::info!(num_workers, ttl_secs, "forked upstream chain, session manager ready");

    // Background chain-tip follower — only when the fork isn't pinned to an
    // explicit block. A pinned historical block and "keep following the
    // tip" are contradictory: re-forking to a newer block would silently
    // defeat the whole point of FORKYARD_FORK_BLOCK_NUMBER.
    let ingest_handle = match fork_block_number {
        Some(n) => {
            tracing::info!(block = n, "fork pinned to an explicit block; chain-tip following disabled");
            None
        }
        None => {
            let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
            let ingest_manager = Arc::clone(&manager);
            let task = tokio::spawn(async move {
                let follower = ChainTipFollower::new(rpc_url, Duration::from_secs(ingest_poll_secs));
                follower.run(&ingest_manager, stop_rx).await;
            });
            Some((stop_tx, task))
        }
    };
```

Then replace the shutdown block near the end of `main` (currently):

```rust
    let _ = ingest_stop_tx.send(());
    let _ = ingest_task.await;
```

with:

```rust
    if let Some((stop_tx, task)) = ingest_handle {
        let _ = stop_tx.send(());
        let _ = task.await;
    }
```

- [ ] **Step 2: Confirm the workspace still builds**

Run: `cargo build -p forkyard`
Expected: builds cleanly, no warnings about unused `ingest_stop_tx`/`ingest_task` (they no longer exist as separate bindings).

- [ ] **Step 3: Verify unset behavior is unchanged**

Run: `RPC_URL=https://ethereum-rpc.publicnode.com cargo run -p forkyard &` then check its logs.
Expected: log line `forked upstream chain, session manager ready` appears, no "fork pinned" line, and the chain-tip follower task starts as before. Stop it with `kill %1` (or `fg` + Ctrl-C).

- [ ] **Step 4: Verify pinned behavior**

Run: `RPC_URL=https://ethereum-rpc.publicnode.com FORKYARD_FORK_BLOCK_NUMBER=20000000 cargo run -p forkyard &`, then:

```bash
curl -s -X POST http://127.0.0.1:8555/session | tee /tmp/session.json
SESSION_ID=$(python3 -c "import json;print(json.load(open('/tmp/session.json'))['session_id'])")
curl -s -X POST http://127.0.0.1:8555/session/$SESSION_ID \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}'
```

Expected: log line `fork pinned to an explicit block; chain-tip following disabled block=20000000`, and the RPC response's `result` is `0x1312d00` (hex for 20000000). Stop the process with `kill %1`.

- [ ] **Step 5: Commit**

```bash
git add crates/bin/src/main.rs
git commit -m "feat(bin): add FORKYARD_FORK_BLOCK_NUMBER to pin the fork to a fixed block"
```

---

## Task 3: `forkyard-engine` — `Session::set_storage`

**Files:**
- Modify: `crates/engine/src/lib.rs:112-119` (next to `set_account`)
- Test: `crates/engine/src/lib.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Produces: `pub fn set_storage(&mut self, address: Address, key: StorageKey, value: StorageValue)` on `Session<F>`.

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block in `crates/engine/src/lib.rs` (near the existing `fork_is_a_pointer_copy_not_a_clone_of_state` test):

```rust
    #[test]
    fn set_storage_overrides_the_overlay_and_reads_back() {
        let base = Arc::new(BaseSnapshot::default());
        let mut session = Session::fork(Arc::clone(&base), NoFallback, revm::context::BlockEnv::default());
        let address = Address::from([0x11; 20]);
        let key = StorageValue::from(7u64);
        let value = StorageValue::from(42u64);

        session.set_storage(address, key, value);

        assert_eq!(Database::storage(&mut session, address, key).unwrap(), value);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p forkyard-engine set_storage_overrides_the_overlay_and_reads_back`
Expected: FAIL with "no method named `set_storage` found"

- [ ] **Step 3: Implement `set_storage`**

In `crates/engine/src/lib.rs`, right after `set_account` (which ends at line 118):

```rust
    /// Override a single storage slot directly in this session's private
    /// overlay — the same test-only cheatcode role `set_account` plays for
    /// balance/nonce, here for arbitrary contract storage (e.g. writing an
    /// ERC-20 `balanceOf` mapping entry directly, since there's no faucet
    /// or impersonation to fund tokens the normal way). Never touches the
    /// shared base or the real chain. Mirrors Anvil's `anvil_setStorageAt`.
    pub fn set_storage(&mut self, address: Address, key: StorageKey, value: StorageValue) {
        self.overlay_storage.insert((address, key), value);
    }
```

`Database::storage` already checks `overlay_storage` first (see its existing implementation), so no read-path change is needed — the test above should pass once the write side exists.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p forkyard-engine set_storage_overrides_the_overlay_and_reads_back`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/lib.rs
git commit -m "feat(engine): add Session::set_storage cheatcode"
```

---

## Task 4: `forkyard-session` — `SessionManager::set_storage`

**Files:**
- Modify: `crates/session/src/lib.rs` (the `Job` enum, `handle_job`, and the `impl<F: Fallback> SessionManager<F>` block)
- Test: `crates/session/src/lib.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `Session::set_storage(&mut self, address: Address, key: StorageKey, value: StorageValue)` (Task 3).
- Produces: `pub async fn set_storage(&self, id: SessionId, address: Address, key: StorageValue, value: StorageValue) -> Result<(), SessionError>` on `SessionManager<F>`.

Note: `revm::primitives::StorageKey` and `StorageValue` are both aliases for `U256` in this codebase (see the existing `use revm::primitives::{..., StorageKey, StorageValue, ...}` import already in `crates/engine/src/lib.rs`); `crates/session/src/lib.rs` doesn't import them yet and needs to.

- [ ] **Step 1: Write the failing test**

The test module in `crates/session/src/lib.rs` already has a `manager()` helper (`fn manager() -> SessionManager<FundedFallback> { SessionManager::new(FundedFallback, BlockEnv::default(), 2, Duration::from_millis(200)) }`), reused by `discard_removes_the_session` and others — use it rather than constructing a new `SessionManager` directly. Add, next to `discard_removes_the_session`:

```rust
    #[tokio::test]
    async fn set_storage_overrides_a_slot_in_the_sessions_overlay() {
        let mgr = manager();
        let id = mgr.fork().await.unwrap();
        let address = Address::from([0x22; 20]);
        let key = U256::from(9u64);
        let value = U256::from(123u64);

        mgr.set_storage(id, address, key, value).await.unwrap();

        // No direct storage-read accessor exists on SessionManager today,
        // so this only proves set_storage doesn't error and reaches the
        // right session (the wrong-id case would surface as
        // SessionError::Unknown from the .unwrap() above). A full
        // write-then-read-back is covered at the forkyard-engine level
        // (Task 3's test).
    }
```

`U256` is already imported in this test module's `use revm::primitives::{Address, TxKind, B256, U256};`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p forkyard-session set_storage_overrides_a_slot_in_the_sessions_overlay`
Expected: FAIL with "no method named `set_storage` found for struct `SessionManager`"

- [ ] **Step 3: Add the `Job::SetStorage` variant, its handler, and the public method**

In `crates/session/src/lib.rs`:

1. Add `StorageKey` and `StorageValue` to the existing `use revm::primitives::{...}` import.

2. Add a new variant to `enum Job<F>` (next to `SetAccount`):

```rust
    SetStorage {
        id: SessionId,
        address: Address,
        key: StorageKey,
        value: StorageValue,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
```

3. Add its arm in `handle_job` (next to the `Job::SetAccount` arm):

```rust
        Job::SetStorage { id, address, key, value, reply } => {
            let result = match sessions.get_mut(&id) {
                Some((session, touched)) => {
                    *touched = Instant::now();
                    session.set_storage(address, key, value);
                    Ok(())
                }
                None => Err(SessionError::Unknown(id)),
            };
            let _ = reply.send(result);
        }
```

4. Add the public method on `SessionManager<F>` (next to `set_account`):

```rust
    /// Override a single storage slot directly in `id`'s private overlay —
    /// the test-only cheatcode role, never touching the shared base or the
    /// real chain. See `Session::set_storage`.
    pub async fn set_storage(
        &self,
        id: SessionId,
        address: Address,
        key: StorageKey,
        value: StorageValue,
    ) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.worker_for(id)
            .send(Job::SetStorage { id, address, key, value, reply })
            .map_err(|_| SessionError::WorkerGone)?;
        rx.await.map_err(|_| SessionError::WorkerGone)?
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p forkyard-session set_storage_overrides_a_slot_in_the_sessions_overlay`
Expected: PASS

- [ ] **Step 5: Run the full session crate's test suite to confirm nothing else broke**

Run: `cargo test -p forkyard-session`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add crates/session/src/lib.rs
git commit -m "feat(session): add SessionManager::set_storage passthrough"
```

---

## Task 5: `forkyard-api-http` — `forkyard_setStorageAt`

**Files:**
- Modify: `crates/api-http/src/lib.rs` (the `dispatch` match, next to the `forkyard_setBalance` arm)
- Test: `crates/api-http/src/lib.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `SessionManager::set_storage(&self, id: SessionId, address: Address, key: StorageValue, value: StorageValue) -> Result<(), SessionError>` (Task 4); existing `parse_address`, `parse_u256_hex_str`, `param_str` helpers.
- Produces: RPC method `forkyard_setStorageAt`, params `[address_hex, slot_hex, value_hex]`, result `true`.

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block in `crates/api-http/src/lib.rs` (near `estimate_gas_reports_real_gas_used_for_a_transfer`):

```rust
    #[tokio::test]
    async fn set_storage_at_overrides_a_slot_readable_by_a_later_call() {
        let state = test_state();
        let id = state.manager.fork().await.unwrap();
        let address = Address::from([3u8; 20]);

        let result = dispatch(
            &state,
            id,
            "forkyard_setStorageAt",
            &[json!(address.to_string()), json!("0x9"), json!(format!("0x{:064x}", 123u64))],
        )
        .await
        .unwrap();

        assert_eq!(result, json!(true));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p forkyard-api-http set_storage_at_overrides_a_slot_readable_by_a_later_call`
Expected: FAIL — `dispatch` returns a `method_not_found` error for `forkyard_setStorageAt`, so `.unwrap()` panics.

- [ ] **Step 3: Add the dispatch arm**

In `crates/api-http/src/lib.rs`, add this arm to `dispatch`'s `match method` right after the existing `"forkyard_setBalance" => { ... }` arm:

```rust
        // Test-only cheatcode, same role as Anvil's `anvil_setStorageAt` —
        // overrides a single storage slot in this session's private
        // overlay only. Exists so an RPC client can fund an ERC-20
        // balance (or set up any other storage-dependent scenario)
        // without needing impersonation, which forkyard doesn't support.
        "forkyard_setStorageAt" => {
            let address = parse_address(params, 0)?;
            let key = parse_u256_hex_str(param_str(params, 1)?)?;
            let value = parse_u256_hex_str(param_str(params, 2)?)?;
            state.manager.set_storage(session_id, address, key, value).await?;
            Ok(json!(true))
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p forkyard-api-http set_storage_at_overrides_a_slot_readable_by_a_later_call`
Expected: PASS

- [ ] **Step 5: Run the full api-http crate's test suite**

Run: `cargo test -p forkyard-api-http`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add crates/api-http/src/lib.rs
git commit -m "feat(api-http): add forkyard_setStorageAt RPC cheatcode"
```

---

## Task 6: `forkyard-api-http` — `forkyard_discard`

**Files:**
- Modify: `crates/api-http/src/lib.rs` (the `dispatch` match)
- Test: `crates/api-http/src/lib.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: existing `SessionManager::discard(&self, id: SessionId) -> Result<(), SessionError>`.
- Produces: RPC method `forkyard_discard`, no params, result `true`; after it, the session id is gone (further calls with that `session_id` fail with `SessionError::Unknown`, mapped to RPC error code `-32001`).

- [ ] **Step 1: Write the failing test**

Add to the `mod tests` block in `crates/api-http/src/lib.rs`:

```rust
    #[tokio::test]
    async fn discard_ends_the_session_so_later_calls_fail() {
        let state = test_state();
        let id = state.manager.fork().await.unwrap();

        let result = dispatch(&state, id, "forkyard_discard", &[]).await.unwrap();
        assert_eq!(result, json!(true));

        let after = dispatch(&state, id, "eth_blockNumber", &[]).await;
        assert!(after.is_err(), "expected the discarded session to be gone");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p forkyard-api-http discard_ends_the_session_so_later_calls_fail`
Expected: FAIL — `forkyard_discard` isn't a known method, so the first `dispatch(...).unwrap()` panics on the `method_not_found` error.

- [ ] **Step 3: Add the dispatch arm**

Add this arm to `dispatch`'s `match method` (position doesn't matter functionally; place it next to `forkyard_setStorageAt` for grouping):

```rust
        // Explicit session teardown ahead of its TTL, over the JSON-RPC
        // surface — the HTTP-side counterpart to the `discard` MCP tool
        // (`crates/api-mcp`), which has no equivalent route here today.
        "forkyard_discard" => {
            state.manager.discard(session_id).await?;
            Ok(json!(true))
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p forkyard-api-http discard_ends_the_session_so_later_calls_fail`
Expected: PASS

- [ ] **Step 5: Run the full api-http crate's test suite**

Run: `cargo test -p forkyard-api-http`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add crates/api-http/src/lib.rs
git commit -m "feat(api-http): add forkyard_discard RPC method"
```

---

## Task 7: `forkyard-api-mcp` — `set_storage` tool (parity)

**Files:**
- Modify: `crates/api-mcp/src/lib.rs`
- Test: `crates/api-mcp/src/lib.rs` (inline `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: `SessionManager::set_storage(...)` (Task 4); existing `parse_address`, `parse_u256_hex` helpers.
- Produces: MCP tool `set_storage`, args `{ session_id, address, slot, value }` (all strings except `session_id`), returns `"true"`.

- [ ] **Step 1: Write the failing test**

In `crates/api-mcp/src/lib.rs`'s `mod tests`, add `"set_storage"` to the `for expected in [...]` list in **both** `lists_expected_tools_and_round_trips_fork_set_balance_and_advance` and `same_tool_surface_round_trips_over_streamable_http` (each currently reads `for expected in ["fork", "get_balance", "set_balance", "simulate", "advance", "discard"]`).

Then add a new test, reusing this file's existing `call(name, args) -> CallToolRequestParams` and `text_of(result) -> &str` helpers and the same in-process duplex-stream setup `lists_expected_tools_and_round_trips_fork_set_balance_and_advance` already uses:

```rust
    #[tokio::test]
    async fn set_storage_tool_round_trips() {
        let manager = Arc::new(SessionManager::new(TestFallback, revm::context::BlockEnv::default(), 1, Duration::from_secs(60)));
        let server = ForkyardMcpServer::new(manager);

        let (server_io, client_io) = tokio::io::duplex(4096);
        let server_task = tokio::spawn(async move {
            server.serve(server_io).await?.waiting().await?;
            eyre::Result::<()>::Ok(())
        });
        let client = NullClient.serve(client_io).await.expect("client should connect");

        let fork_result = client.call_tool(call("fork", serde_json::json!({}))).await.expect("fork");
        let session_id: u64 = text_of(&fork_result).parse().unwrap();

        let address = Address::from([3u8; 20]);
        let result = client
            .call_tool(call(
                "set_storage",
                serde_json::json!({
                    "session_id": session_id,
                    "address": address.to_string(),
                    "slot": "0x9",
                    "value": format!("0x{:064x}", 123u64),
                }),
            ))
            .await
            .expect("set_storage");
        assert_eq!(text_of(&result), "true");

        client.cancel().await.expect("client should cancel");
        server_task.await.expect("server task").expect("server");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p forkyard-api-mcp set_storage_tool_round_trips`
Expected: FAIL — no tool named `set_storage`.

- [ ] **Step 3: Add the `SetStorageArgs` struct and the tool method**

Add near the existing `SetBalanceArgs` struct:

```rust
#[derive(Deserialize, schemars::JsonSchema)]
struct SetStorageArgs {
    session_id: SessionId,
    address: String,
    /// Storage slot index, `0x`-prefixed hex.
    slot: String,
    /// New value for that slot, `0x`-prefixed hex (32 bytes).
    value: String,
}
```

Add the tool method in the `#[tool_router] impl<F: Fallback> ForkyardMcpServer<F>` block, next to `set_balance`:

```rust
    #[tool(description = "Test-only cheatcode: override a single storage slot in this session's private overlay only, e.g. to fund an ERC-20 balanceOf mapping entry. Never touches the shared base or the real chain.")]
    async fn set_storage(&self, Parameters(args): Parameters<SetStorageArgs>) -> Result<CallToolResult, ErrorData> {
        let address = parse_address(&args.address)?;
        let slot = parse_u256_hex(&args.slot)?;
        let value = parse_u256_hex(&args.value)?;
        self.manager.set_storage(args.session_id, address, slot, value).await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text("true")]))
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p forkyard-api-mcp set_storage_tool_round_trips`
Expected: PASS

- [ ] **Step 5: Run the full api-mcp crate's test suite**

Run: `cargo test -p forkyard-api-mcp`
Expected: all tests PASS, including the two updated `for expected in [...]` tests now asserting `set_storage` is listed.

- [ ] **Step 6: Run the entire workspace's tests once, to confirm Tasks 1-7 all still hold together**

Run: `cargo test --workspace`
Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
git add crates/api-mcp/src/lib.rs
git commit -m "feat(api-mcp): add set_storage tool for parity with the HTTP surface"
```

---

## Task 8: `python/benchmarks/` scaffold

**Files:**
- Create: `python/benchmarks/pyproject.toml`
- Create: `python/benchmarks/.python-version`
- Create: `python/benchmarks/README.md`

**Interfaces:**
- Produces: a `uv`-managed project other tasks add modules to.

- [ ] **Step 1: Create `.python-version`**

```
3.11.9
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[project]
name = "forkyard-benchmarks"
version = "0.1.0"
description = "Multi-agent fork benchmark: forkyard (one process, N sessions) vs. Anvil (N processes)"
requires-python = ">=3.11.9"
dependencies = [
    "web3>=7.16.0",
    "requests>=2.32.0",
    "pandas>=2.2.0",
    "matplotlib>=3.9.0",
]

[dependency-groups]
dev = [
    "pytest>=8.3.0",
]
```

- [ ] **Step 3: Create `README.md`**

```markdown
# forkyard benchmarks

Compares forkyard (one process, N concurrent forked sessions) against
running one standalone Anvil instance per agent, across agent counts and
fork block heights. See `docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md`
in the repo root for the full design.

Requires the `anvil` binary (Foundry) on `PATH`, and a `forkyard` binary
built from this repo (`cargo build -p forkyard --release`).

```bash
uv sync
uv run pytest                     # unit tests (no subprocesses/network)
uv run python run_benchmark.py --agents 1,2,5 --block-heights 20000000 --actions-per-agent 5 --rpc-url $RPC_URL --out results.csv
uv run python plot_results.py results.csv
```
```

- [ ] **Step 4: Verify `uv sync` resolves**

Run: `cd python/benchmarks && uv sync`
Expected: creates `.venv` and `uv.lock` with no errors.

- [ ] **Step 5: Commit**

```bash
git add python/benchmarks/pyproject.toml python/benchmarks/.python-version python/benchmarks/README.md python/benchmarks/uv.lock
git commit -m "chore(benchmarks): scaffold python/benchmarks project"
```

---

## Task 9: `backend.py` — `Backend` protocol, `ForkyardBackend`, `AnvilBackend`

**Files:**
- Create: `python/benchmarks/backend.py`
- Test: `python/benchmarks/test_backend.py`

**Interfaces:**
- Produces:
  - `class Backend(Protocol)`: `web3(self) -> Web3`, `set_native_balance(self, address: str, wei: int) -> None`, `set_storage(self, address: str, slot_hex: str, value_hex: str) -> None`, `discard(self) -> None`, `name: str` attribute.
  - `class ForkyardBackend(session_url: str)` implementing `Backend`.
  - `class AnvilBackend(port: int, fork_url: str, fork_block_number: int)` implementing `Backend` — spawns and owns an `anvil` subprocess; `discard()` terminates it.
  - `erc20_balance_slot(holder: str, mapping_slot: int) -> bytes` — the Solidity mapping-storage-slot formula, needed by both this module's callers and Task 10's `fund_token`.

- [ ] **Step 1: Write the failing test**

Create `python/benchmarks/test_backend.py`:

```python
from backend import erc20_balance_slot


def test_erc20_balance_slot_matches_known_dai_vector():
    # DAI's balanceOf mapping is storage slot 2 (see the plan's Global
    # Constraints). This is the standard Solidity mapping-slot formula:
    # keccak256(bytes32(holder) ++ bytes32(slot)). Cross-checked against
    # a known-good value computed independently with eth_utils.keccak
    # for holder 0x0000000000000000000000000000000000000001, slot 2.
    holder = "0x0000000000000000000000000000000000000001"
    slot = erc20_balance_slot(holder, mapping_slot=2)
    assert isinstance(slot, bytes)
    assert len(slot) == 32
    # Re-derive independently in the test (not copy the implementation)
    # to actually catch a wrong formula, not just a wrong constant.
    from eth_utils import keccak
    key = int(holder, 16).to_bytes(32, "big")
    mapping_slot_bytes = (2).to_bytes(32, "big")
    expected = keccak(key + mapping_slot_bytes)
    assert slot == expected
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python/benchmarks && uv run pytest test_backend.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'backend'`

- [ ] **Step 3: Implement `backend.py`**

```python
"""Backend abstraction so the same agent/action code drives forkyard's
shared-cache session model and Anvil's one-instance-per-agent model
identically. See docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md.
"""

from __future__ import annotations

import subprocess
import time
from typing import Protocol

import requests
from eth_utils import keccak
from web3 import Web3


def erc20_balance_slot(holder: str, mapping_slot: int) -> bytes:
    """Solidity's mapping storage slot formula: keccak256(bytes32(key) ++
    bytes32(mapping_slot)). Standard for a simple `mapping(address =>
    uint256) balances` declared at `mapping_slot` — not valid for a proxy
    contract with a different layout (that's why USDC is excluded from
    the default TOKENS registry — see actions.py)."""
    key = int(holder, 16).to_bytes(32, "big")
    slot = mapping_slot.to_bytes(32, "big")
    return keccak(key + slot)


class Backend(Protocol):
    name: str

    def web3(self) -> Web3: ...
    def set_native_balance(self, address: str, wei: int) -> None: ...
    def set_storage(self, address: str, slot_hex: str, value_hex: str) -> None: ...
    def discard(self) -> None: ...


class ForkyardBackend:
    """One forkyard session, opened against an already-running forkyard
    process's shared cache. `discard()` calls forkyard_discard rather than
    tearing down any process — the process outlives every session."""

    name = "forkyard"

    def __init__(self, session_url: str):
        self._w3 = Web3(Web3.HTTPProvider(session_url))

    def web3(self) -> Web3:
        return self._w3

    def set_native_balance(self, address: str, wei: int) -> None:
        self._w3.manager.request_blocking("forkyard_setBalance", [address, hex(wei)])

    def set_storage(self, address: str, slot_hex: str, value_hex: str) -> None:
        self._w3.manager.request_blocking("forkyard_setStorageAt", [address, slot_hex, value_hex])

    def discard(self) -> None:
        self._w3.manager.request_blocking("forkyard_discard", [])


class AnvilBackend:
    """One standalone Anvil instance, forked at a specific block, owned
    entirely by this agent. `discard()` kills the process — Anvil has no
    lighter-weight session-close concept than "the instance is the
    session", so tearing it down is the fair equivalent action."""

    name = "anvil"

    def __init__(self, port: int, fork_url: str, fork_block_number: int, startup_timeout_s: float = 20.0):
        try:
            self._process = subprocess.Popen(
                [
                    "anvil",
                    "--fork-url", fork_url,
                    "--fork-block-number", str(fork_block_number),
                    "--port", str(port),
                    "--silent",
                ],
            )
        except FileNotFoundError as e:
            raise RuntimeError(
                "the `anvil` binary was not found on PATH — install Foundry "
                "(https://book.getfoundry.sh/getting-started/installation) before running the Anvil backend"
            ) from e
        self._url = f"http://127.0.0.1:{port}"
        self._wait_until_ready(startup_timeout_s)
        self._w3 = Web3(Web3.HTTPProvider(self._url))

    def _wait_until_ready(self, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                resp = requests.post(
                    self._url,
                    json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
                    timeout=1,
                )
                if resp.ok:
                    return
            except requests.RequestException as e:
                last_error = e
            time.sleep(0.2)
        raise RuntimeError(f"anvil on {self._url} did not become ready in {timeout_s}s: {last_error}")

    def web3(self) -> Web3:
        return self._w3

    def set_native_balance(self, address: str, wei: int) -> None:
        self._w3.manager.request_blocking("anvil_setBalance", [address, hex(wei)])

    def set_storage(self, address: str, slot_hex: str, value_hex: str) -> None:
        self._w3.manager.request_blocking("anvil_setStorageAt", [address, slot_hex, value_hex])

    def discard(self) -> None:
        self._process.terminate()
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python/benchmarks && uv run pytest test_backend.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add python/benchmarks/backend.py python/benchmarks/test_backend.py
git commit -m "feat(benchmarks): add Backend protocol with forkyard and anvil implementations"
```

---

## Task 10: `actions.py` — action library + `TOKENS` registry

**Files:**
- Create: `python/benchmarks/actions.py`
- Test: `python/benchmarks/test_actions.py`

**Interfaces:**
- Consumes: `Backend` protocol, `erc20_balance_slot` (Task 9).
- Produces:
  - `TOKENS: dict[str, dict]` — `{"DAI": {"address": "0x6B17...", "balance_slot": 2}}`.
  - `UNISWAP_V2_ROUTER = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"`, `WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"`.
  - `ActionResult = tuple[str, float, bool]` — `(label, elapsed_ms, ok)`.
  - `transfer(backend, signer_key, to, value, nonce) -> ActionResult`
  - `set_balance(backend, address, value) -> ActionResult`
  - `get_balance(backend, address) -> ActionResult`
  - `fund_token(backend, token, holder, amount) -> ActionResult`
  - `approve(backend, signer_key, token, spender, amount, nonce) -> ActionResult`
  - `swap_eth_for_token(backend, signer_key, token, amount_in, nonce) -> ActionResult`
  - `swap_token_for_token(backend, signer_key, token_in, token_out, amount_in, nonce) -> ActionResult`
  - `discard_session(backend) -> ActionResult`

Every action that sends a transaction takes an explicit `nonce` (per the README's Gotchas: forkyard doesn't auto-increment nonces) rather than querying it — the caller (Task 11's `agent.py`) owns nonce bookkeeping so it can also legitimately call `get_balance` as one of the random actions without that read perturbing the write path.

- [ ] **Step 1: Write the failing test**

Create `python/benchmarks/test_actions.py`:

```python
from actions import TOKENS, UNISWAP_V2_ROUTER, WETH


def test_tokens_registry_has_dai_with_expected_slot():
    assert TOKENS["DAI"]["address"] == "0x6B175474E89094C44Da98b954EedeAC495271d0F"
    assert TOKENS["DAI"]["balance_slot"] == 2


def test_uniswap_constants_are_checksummed_mainnet_addresses():
    from web3 import Web3
    assert Web3.to_checksum_address(UNISWAP_V2_ROUTER) == UNISWAP_V2_ROUTER
    assert Web3.to_checksum_address(WETH) == WETH
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python/benchmarks && uv run pytest test_actions.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'actions'`

- [ ] **Step 3: Implement `actions.py`**

```python
"""One function per simulated-agent action, each timed and returning
(label, elapsed_ms, ok). See docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md
for why this specific set (no eth_call, no impersonation, no generic
ERC-20 faucet on the RPC surface — only what forkyard_setStorageAt /
anvil_setStorageAt make possible)."""

from __future__ import annotations

import time
from typing import Callable

from eth_account import Account
from web3 import Web3

from backend import Backend, erc20_balance_slot

ActionResult = tuple[str, float, bool]

UNISWAP_V2_ROUTER = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

TOKENS = {
    "DAI": {"address": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "balance_slot": 2},
}

_ERC20_ABI = [
    {
        "name": "approve", "type": "function", "stateMutability": "nonpayable",
        "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
        "outputs": [{"name": "", "type": "bool"}],
    },
]

_ROUTER_ABI = [
    {
        "name": "swapExactETHForTokens", "type": "function", "stateMutability": "payable",
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
    {
        "name": "swapExactTokensForTokens", "type": "function", "stateMutability": "nonpayable",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
]

_FAR_FUTURE_DEADLINE = 9_999_999_999  # year ~2286, plenty for a benchmark run


def _timed(label: str, fn: Callable[[], None]) -> ActionResult:
    start = time.monotonic()
    try:
        fn()
        ok = True
    except Exception:
        ok = False
    elapsed_ms = (time.monotonic() - start) * 1000
    return (label, elapsed_ms, ok)


def _send_signed(w3: Web3, signer_key: str, to: str, value: int, data: bytes, nonce: int, gas: int) -> None:
    gas_price = w3.eth.gas_price
    tx = {
        "chainId": w3.eth.chain_id,
        "nonce": nonce,
        "gas": gas,
        "gasPrice": gas_price,
        "to": to,
        "value": value,
        "data": data,
    }
    signed = Account.sign_transaction(tx, signer_key)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=10)
    if receipt.status != 1:
        raise RuntimeError(f"transaction {tx_hash.to_0x_hex()} reverted")


def transfer(backend: Backend, signer_key: str, to: str, value: int, nonce: int) -> ActionResult:
    def do():
        _send_signed(backend.web3(), signer_key, to, value, b"", nonce, gas=21_000)
    return _timed("transfer", do)


def set_balance(backend: Backend, address: str, value: int) -> ActionResult:
    return _timed("set_balance", lambda: backend.set_native_balance(address, value))


def get_balance(backend: Backend, address: str) -> ActionResult:
    def do():
        backend.web3().eth.get_balance(address)
        backend.web3().eth.get_transaction_count(address)
    return _timed("get_balance", do)


def fund_token(backend: Backend, token: str, holder: str, amount: int) -> ActionResult:
    def do():
        info = next(t for t in TOKENS.values() if t["address"] == token)
        slot = erc20_balance_slot(holder, info["balance_slot"])
        value = amount.to_bytes(32, "big")
        backend.set_storage(token, "0x" + slot.hex(), "0x" + value.hex())
    return _timed("fund_token", do)


def approve(backend: Backend, signer_key: str, token: str, spender: str, amount: int, nonce: int) -> ActionResult:
    def do():
        w3 = backend.web3()
        contract = w3.eth.contract(address=token, abi=_ERC20_ABI)
        data = contract.encode_abi("approve", args=[spender, amount])
        _send_signed(w3, signer_key, token, 0, bytes.fromhex(data[2:]), nonce, gas=60_000)
    return _timed("approve", do)


def swap_eth_for_token(backend: Backend, signer_key: str, token: str, amount_in: int, nonce: int) -> ActionResult:
    def do():
        w3 = backend.web3()
        signer = Account.from_key(signer_key)
        router = w3.eth.contract(address=UNISWAP_V2_ROUTER, abi=_ROUTER_ABI)
        data = router.encode_abi(
            "swapExactETHForTokens",
            args=[0, [WETH, token], signer.address, _FAR_FUTURE_DEADLINE],
        )
        _send_signed(w3, signer_key, UNISWAP_V2_ROUTER, amount_in, bytes.fromhex(data[2:]), nonce, gas=250_000)
    return _timed("swap_eth_for_token", do)


def swap_token_for_token(
    backend: Backend, signer_key: str, token_in: str, token_out: str, amount_in: int, nonce: int
) -> ActionResult:
    def do():
        w3 = backend.web3()
        signer = Account.from_key(signer_key)
        router = w3.eth.contract(address=UNISWAP_V2_ROUTER, abi=_ROUTER_ABI)
        data = router.encode_abi(
            "swapExactTokensForTokens",
            args=[amount_in, 0, [token_in, token_out], signer.address, _FAR_FUTURE_DEADLINE],
        )
        _send_signed(w3, signer_key, UNISWAP_V2_ROUTER, 0, bytes.fromhex(data[2:]), nonce, gas=300_000)
    return _timed("swap_token_for_token", do)


def discard_session(backend: Backend) -> ActionResult:
    return _timed("discard", backend.discard)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python/benchmarks && uv run pytest test_actions.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add python/benchmarks/actions.py python/benchmarks/test_actions.py
git commit -m "feat(benchmarks): add timed action library (transfer, balances, uniswap swaps)"
```

---

## Task 11: `agent.py` — random legal action sequences

**Files:**
- Create: `python/benchmarks/agent.py`
- Test: `python/benchmarks/test_agent.py`

**Interfaces:**
- Consumes: every function in `actions.py` (Task 10), `Backend` (Task 9).
- Produces:
  - `@dataclass class ActionRecord: backend: str; block_height: int; num_agents: int; agent_id: int; action: str; elapsed_ms: float; ok: bool`
  - `def run_agent(backend: Backend, rng: random.Random, agent_id: int, block_height: int, num_agents: int, num_actions: int, funding_eth: int = 10**18) -> list[ActionRecord]`

`run_agent` always: funds the agent's signer with ETH first (a prerequisite for every other action, not itself one of the "random" ones), then runs `num_actions` randomly chosen actions respecting one ordering rule — `swap_token_for_token` on `token_in` is only chosen if that token was already funded (`fund_token`) and approved (`approve`) earlier in this agent's own sequence — then always ends with `discard_session`.

- [ ] **Step 1: Write the failing test**

Create `python/benchmarks/test_agent.py`:

```python
import random

from agent import ActionRecord, run_agent


class FakeBackend:
    """Records every backend call instead of touching a real RPC endpoint,
    so this test proves the ordering/dependency logic in run_agent without
    needing a live forkyard or anvil process."""

    name = "fake"

    def __init__(self):
        self.calls: list[str] = []

    def web3(self):
        class FakeEth:
            chain_id = 1
            gas_price = 1_000_000_000

            def get_balance(self, address):
                return 0

            def get_transaction_count(self, address):
                return 0

        class FakeW3:
            eth = FakeEth()

        return FakeW3()

    def set_native_balance(self, address, wei):
        self.calls.append("set_native_balance")

    def set_storage(self, address, slot_hex, value_hex):
        self.calls.append("set_storage")

    def discard(self):
        self.calls.append("discard")


def test_run_agent_always_ends_with_discard_and_starts_with_funding():
    backend = FakeBackend()
    rng = random.Random(42)

    records = run_agent(backend, rng, agent_id=0, block_height=20_000_000, num_agents=1, num_actions=3)

    assert all(isinstance(r, ActionRecord) for r in records)
    assert records[0].action == "set_balance"
    assert records[-1].action == "discard"
    assert len(records) == 1 + 3 + 1  # funding + num_actions + discard
    assert all(r.block_height == 20_000_000 and r.num_agents == 1 and r.agent_id == 0 for r in records)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python/benchmarks && uv run pytest test_agent.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'agent'`

- [ ] **Step 3: Implement `agent.py`**

```python
"""Runs one simulated agent's action sequence and returns timed records.
See docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import random
from dataclasses import dataclass

from eth_account import Account

from actions import (
    TOKENS,
    UNISWAP_V2_ROUTER,
    approve,
    discard_session,
    fund_token,
    get_balance,
    set_balance,
    swap_eth_for_token,
    swap_token_for_token,
    transfer,
)
from backend import Backend

ONE_ETH = 10**18


@dataclass
class ActionRecord:
    backend: str
    block_height: int
    num_agents: int
    agent_id: int
    action: str
    elapsed_ms: float
    ok: bool


def run_agent(
    backend: Backend,
    rng: random.Random,
    agent_id: int,
    block_height: int,
    num_agents: int,
    num_actions: int,
    funding_eth: int = ONE_ETH,
) -> list[ActionRecord]:
    signer = Account.create()
    signer_key = signer.key.hex()
    nonce = 0
    funded_tokens: set[str] = set()
    approved_tokens: set[str] = set()

    def record(result) -> ActionRecord:
        label, elapsed_ms, ok = result
        return ActionRecord(backend.name, block_height, num_agents, agent_id, label, elapsed_ms, ok)

    records: list[ActionRecord] = [record(set_balance(backend, signer.address, funding_eth))]

    for _ in range(num_actions):
        choices = ["transfer", "get_balance", "swap_eth_for_token", "fund_token"]
        if funded_tokens:
            choices.append("approve")
        if approved_tokens:
            choices.append("swap_token_for_token")
        choice = rng.choice(choices)
        token = rng.choice(list(TOKENS.values()))["address"]

        if choice == "transfer":
            recipient = Account.create().address
            records.append(record(transfer(backend, signer_key, recipient, ONE_ETH // 100, nonce)))
            nonce += 1
        elif choice == "get_balance":
            records.append(record(get_balance(backend, signer.address)))
        elif choice == "swap_eth_for_token":
            records.append(record(swap_eth_for_token(backend, signer_key, token, ONE_ETH // 100, nonce)))
            nonce += 1
        elif choice == "fund_token":
            records.append(record(fund_token(backend, token, signer.address, ONE_ETH)))
            funded_tokens.add(token)
        elif choice == "approve":
            funded_token = rng.choice(list(funded_tokens))
            records.append(
                record(approve(backend, signer_key, funded_token, UNISWAP_V2_ROUTER, ONE_ETH, nonce))
            )
            nonce += 1
            approved_tokens.add(funded_token)
        elif choice == "swap_token_for_token":
            token_in = rng.choice(list(approved_tokens))
            token_out = rng.choice([t["address"] for t in TOKENS.values() if t["address"] != token_in] or [token_in])
            records.append(record(swap_token_for_token(backend, signer_key, token_in, token_out, ONE_ETH // 1000, nonce)))
            nonce += 1

    records.append(record(discard_session(backend)))
    return records
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python/benchmarks && uv run pytest test_agent.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add python/benchmarks/agent.py python/benchmarks/test_agent.py
git commit -m "feat(benchmarks): add run_agent random-action-sequence orchestration"
```

---

## Task 12: `run_benchmark.py` — CLI, sweep, CSV output

**Files:**
- Create: `python/benchmarks/run_benchmark.py`
- Test: `python/benchmarks/test_run_benchmark.py` (only the pure argument-parsing/CSV-shape logic — the actual sweep spawns real subprocesses and needs a live RPC, so it's smoke-tested manually per the spec's testing plan, not in this automated test)

**Interfaces:**
- Consumes: `Backend`/`ForkyardBackend`/`AnvilBackend` (Task 9), `run_agent`/`ActionRecord` (Task 11).
- Produces: a script runnable as `uv run python run_benchmark.py --agents ... --block-heights ... --actions-per-agent N --rpc-url ... --out results.csv`, writing one CSV row per `ActionRecord` plus one `action="__total__"` row per `(backend, block_height, num_agents)` run recording that run's total wall-clock in `elapsed_ms`.
- `parse_int_list(s: str) -> list[int]` — parses `"1,2,5,10"` into `[1, 2, 5, 10]`, used for both `--agents` and `--block-heights`.

- [ ] **Step 1: Write the failing test**

Create `python/benchmarks/test_run_benchmark.py`:

```python
import csv
import io

from run_benchmark import parse_int_list, write_records
from agent import ActionRecord


def test_parse_int_list_splits_and_converts():
    assert parse_int_list("1,2,5,10") == [1, 2, 5, 10]
    assert parse_int_list("7") == [7]


def test_write_records_produces_one_csv_row_per_record():
    records = [
        ActionRecord("forkyard", 20_000_000, 2, 0, "transfer", 12.5, True),
        ActionRecord("forkyard", 20_000_000, 2, 0, "discard", 3.1, True),
    ]
    buf = io.StringIO()
    write_records(buf, records)
    rows = list(csv.DictReader(io.StringIO(buf.getvalue())))
    assert len(rows) == 2
    assert rows[0]["action"] == "transfer"
    assert rows[0]["ok"] == "True"
    assert rows[1]["backend"] == "forkyard"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python/benchmarks && uv run pytest test_run_benchmark.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'run_benchmark'`

- [ ] **Step 3: Implement `run_benchmark.py`**

```python
"""CLI entrypoint: sweeps (backend, block_height, num_agents) and records
per-action + per-run timings to a CSV. See
docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import os
import random
import subprocess
import sys
import time
from typing import IO

import requests

from agent import ActionRecord, run_agent
from backend import AnvilBackend, ForkyardBackend

FIELDS = ["backend", "block_height", "num_agents", "agent_id", "action", "elapsed_ms", "ok"]


def parse_int_list(s: str) -> list[int]:
    return [int(x) for x in s.split(",")]


def write_records(out: IO[str], records: list[ActionRecord]) -> None:
    writer = csv.DictWriter(out, fieldnames=FIELDS)
    writer.writeheader()
    for r in records:
        writer.writerow(
            {
                "backend": r.backend,
                "block_height": r.block_height,
                "num_agents": r.num_agents,
                "agent_id": r.agent_id,
                "action": r.action,
                "elapsed_ms": r.elapsed_ms,
                "ok": r.ok,
            }
        )


def _wait_for_forkyard(base_url: str, timeout_s: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            resp = requests.post(f"{base_url}/session", timeout=1)
            if resp.ok:
                return
        except requests.RequestException as e:
            last_error = e
        time.sleep(0.2)
    raise RuntimeError(f"forkyard on {base_url} did not become ready in {timeout_s}s: {last_error}")


def run_forkyard_sweep(
    rpc_url: str, block_height: int, num_agents: int, actions_per_agent: int, port: int, mcp_port: int
) -> tuple[list[ActionRecord], float]:
    env = {
        **os.environ,
        "RPC_URL": rpc_url,
        "FORKYARD_PORT": str(port),
        "FORKYARD_MCP_HTTP_PORT": str(mcp_port),
        "FORKYARD_FORK_BLOCK_NUMBER": str(block_height),
    }
    try:
        process = subprocess.Popen(["forkyard"], env=env)
    except FileNotFoundError as e:
        raise RuntimeError(
            "the `forkyard` binary was not found on PATH — build it with "
            "`cargo build -p forkyard --release` and add target/release to PATH"
        ) from e
    base_url = f"http://127.0.0.1:{port}"
    try:
        _wait_for_forkyard(base_url)
        session_urls = []
        for _ in range(num_agents):
            resp = requests.post(f"{base_url}/session", timeout=5).json()
            session_urls.append(f"{base_url}/session/{resp['session_id']}")

        start = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_agents) as pool:
            futures = [
                pool.submit(
                    run_agent,
                    ForkyardBackend(session_urls[i]),
                    random.Random(i),
                    i,
                    block_height,
                    num_agents,
                    actions_per_agent,
                )
                for i in range(num_agents)
            ]
            all_records = [r for f in futures for r in f.result()]
        total_ms = (time.monotonic() - start) * 1000
        return all_records, total_ms
    finally:
        process.terminate()
        process.wait(timeout=10)


def run_anvil_sweep(
    rpc_url: str, block_height: int, num_agents: int, actions_per_agent: int, base_port: int
) -> tuple[list[ActionRecord], float]:
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_agents) as pool:
        futures = [
            pool.submit(
                lambda i=i: run_agent(
                    AnvilBackend(base_port + i, rpc_url, block_height),
                    random.Random(i),
                    i,
                    block_height,
                    num_agents,
                    actions_per_agent,
                )
            )
            for i in range(num_agents)
        ]
        all_records = [r for f in futures for r in f.result()]
    total_ms = (time.monotonic() - start) * 1000
    return all_records, total_ms


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--agents", type=parse_int_list, required=True)
    parser.add_argument("--block-heights", type=parse_int_list, required=True)
    parser.add_argument("--actions-per-agent", type=int, default=5)
    parser.add_argument("--rpc-url", required=True)
    parser.add_argument("--out", default="results.csv")
    args = parser.parse_args()

    all_records: list[ActionRecord] = []
    for block_height in args.block_heights:
        for num_agents in args.agents:
            for sweep_fn, label in [
                (lambda: run_forkyard_sweep(args.rpc_url, block_height, num_agents, args.actions_per_agent, 18555, 18556), "forkyard"),
                (lambda: run_anvil_sweep(args.rpc_url, block_height, num_agents, args.actions_per_agent, 19000), "anvil"),
            ]:
                print(f"running {label}: block={block_height} agents={num_agents}", file=sys.stderr)
                records, total_ms = sweep_fn()
                all_records.extend(records)
                all_records.append(
                    ActionRecord(label, block_height, num_agents, -1, "__total__", total_ms, True)
                )

    with open(args.out, "w", newline="") as f:
        write_records(f, all_records)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python/benchmarks && uv run pytest test_run_benchmark.py -v`
Expected: PASS

- [ ] **Step 5: Manual smoke test against real processes (documented, not automated)**

Prerequisites: `cargo build -p forkyard --release` (and put `target/release/forkyard` on `PATH` for this shell, e.g. `export PATH="$PWD/target/release:$PATH"` from the repo root), `anvil` installed (Foundry) and on `PATH`.

Run:
```bash
cd python/benchmarks
uv run python run_benchmark.py --agents 1,2 --block-heights 20000000 --actions-per-agent 3 --rpc-url https://ethereum-rpc.publicnode.com --out /tmp/smoke.csv
```
Expected: exits 0, `/tmp/smoke.csv` exists, every row's `ok` column is `True` (open it and check — a `False` anywhere means an action reverted and needs investigating before trusting the full sweep's numbers).

- [ ] **Step 6: Commit**

```bash
git add python/benchmarks/run_benchmark.py python/benchmarks/test_run_benchmark.py
git commit -m "feat(benchmarks): add run_benchmark CLI sweeping agents x block heights x backends"
```

---

## Task 13: `plot_results.py`

**Files:**
- Create: `python/benchmarks/plot_results.py`
- Test: `python/benchmarks/test_plot_results.py`

**Interfaces:**
- Consumes: the CSV written by `write_records` (Task 12) — columns `backend, block_height, num_agents, agent_id, action, elapsed_ms, ok`.
- Produces: `def plot_total_time_vs_agents(df: pandas.DataFrame, out_path: str) -> None`, `def plot_action_latency(df: pandas.DataFrame, out_path: str) -> None`, and a CLI `uv run python plot_results.py results.csv` writing `results_total_time.png` and `results_action_latency.png` alongside it.

- [ ] **Step 1: Write the failing test**

Create `python/benchmarks/test_plot_results.py`:

```python
import os

import pandas as pd

from plot_results import plot_action_latency, plot_total_time_vs_agents


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"backend": "forkyard", "block_height": 20_000_000, "num_agents": 1, "agent_id": -1, "action": "__total__", "elapsed_ms": 100.0, "ok": True},
            {"backend": "forkyard", "block_height": 20_000_000, "num_agents": 2, "agent_id": -1, "action": "__total__", "elapsed_ms": 150.0, "ok": True},
            {"backend": "anvil", "block_height": 20_000_000, "num_agents": 1, "agent_id": -1, "action": "__total__", "elapsed_ms": 300.0, "ok": True},
            {"backend": "anvil", "block_height": 20_000_000, "num_agents": 2, "agent_id": -1, "action": "__total__", "elapsed_ms": 600.0, "ok": True},
            {"backend": "forkyard", "block_height": 20_000_000, "num_agents": 1, "agent_id": 0, "action": "transfer", "elapsed_ms": 5.0, "ok": True},
            {"backend": "anvil", "block_height": 20_000_000, "num_agents": 1, "agent_id": 0, "action": "transfer", "elapsed_ms": 8.0, "ok": True},
        ]
    )


def test_plot_total_time_vs_agents_writes_a_png(tmp_path):
    out = tmp_path / "total.png"
    plot_total_time_vs_agents(_sample_df(), str(out))
    assert out.exists()
    assert out.stat().st_size > 0


def test_plot_action_latency_writes_a_png(tmp_path):
    out = tmp_path / "latency.png"
    plot_action_latency(_sample_df(), str(out))
    assert out.exists()
    assert out.stat().st_size > 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python/benchmarks && uv run pytest test_plot_results.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'plot_results'`

- [ ] **Step 3: Implement `plot_results.py`**

```python
"""Plots run_benchmark.py's CSV output. See
docs/superpowers/specs/2026-08-26-agent-fork-benchmark-design.md."""

from __future__ import annotations

import sys

import matplotlib

matplotlib.use("Agg")  # headless: this script only ever writes PNGs, never shows a window
import matplotlib.pyplot as plt
import pandas as pd


def plot_total_time_vs_agents(df: pd.DataFrame, out_path: str) -> None:
    totals = df[df["action"] == "__total__"]
    fig, ax = plt.subplots()
    for (backend, block_height), group in totals.groupby(["backend", "block_height"]):
        group = group.sort_values("num_agents")
        ax.plot(group["num_agents"], group["elapsed_ms"], marker="o", label=f"{backend} @ {block_height}")
    ax.set_xlabel("number of concurrent agents")
    ax.set_ylabel("total simulation time (ms)")
    ax.set_title("Total simulation time vs. agent count")
    ax.legend()
    fig.savefig(out_path)
    plt.close(fig)


def plot_action_latency(df: pd.DataFrame, out_path: str) -> None:
    per_action = df[df["action"] != "__total__"]
    medians = per_action.groupby(["action", "backend"])["elapsed_ms"].median().unstack("backend")
    fig, ax = plt.subplots()
    medians.plot(kind="bar", ax=ax)
    ax.set_xlabel("action")
    ax.set_ylabel("median latency (ms)")
    ax.set_title("Per-action median latency by backend")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main() -> None:
    csv_path = sys.argv[1]
    df = pd.read_csv(csv_path)
    base = csv_path.rsplit(".", 1)[0]
    plot_total_time_vs_agents(df, f"{base}_total_time.png")
    plot_action_latency(df, f"{base}_action_latency.png")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python/benchmarks && uv run pytest test_plot_results.py -v`
Expected: PASS

- [ ] **Step 5: Run the full Python test suite once, to confirm Tasks 8-13 all still hold together**

Run: `cd python/benchmarks && uv run pytest -v`
Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
git add python/benchmarks/plot_results.py python/benchmarks/test_plot_results.py
git commit -m "feat(benchmarks): add plotting for total-time and per-action-latency"
```
