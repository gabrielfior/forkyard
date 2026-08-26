# Multi-agent fork benchmark: design

Date: 2026-08-26
Status: approved for planning

## Goal

Measure how forkyard's "one process, many forked sessions" model scales as
the number of concurrent simulated agents grows, and compare it against the
alternative of running one standalone Anvil instance per agent — across
different block heights. Each simulated agent runs a random sequence of
realistic actions (ETH transfer, balance funding, a Uniswap swap, etc.)
against its own fork/instance; every action's wall-clock time and the run's
total wall-clock time are recorded, so results can be plotted as agent count
increases.

## Non-goals

- Not a correctness/regression test suite for forkyard (existing crate
  tests cover that).
- Not measuring gas cost or on-chain economics, only wall-clock latency of
  driving the RPC surface.
- Not adding a general-purpose ERC-20 faucet or impersonation feature —
  just enough of a storage cheatcode to fund one well-understood token
  (DAI) for the swap actions.
- Not making forkyard support *per-session* block pinning (multiple block
  heights live in the same process at once). Block pinning stays
  process-wide, matching today's shared-base architecture; different block
  heights are compared across separate process runs, for both backends.

## Background / constraints established during exploration

- forkyard's `SessionManager` holds one shared `BaseSnapshot` + `BlockEnv`
  for the whole process; every session forks from that same base
  (`crates/session/src/lib.rs`). There is no per-`fork()` block parameter
  anywhere in the stack (`crates/fetch`, `crates/session`,
  `crates/api-http`, `crates/api-mcp`) — `forkyard_fetch::fork` always
  calls `BlockId::latest()`, and `forkyard-ingest`'s `ChainTipFollower`
  continuously re-forks to the new tip whenever the chain advances.
- The JSON-RPC surface (`crates/api-http/src/lib.rs`, `dispatch`) only
  implements: `eth_chainId`, `eth_blockNumber`, `eth_gasPrice`,
  `eth_getBalance`, `eth_getTransactionCount`, `forkyard_setBalance`,
  `eth_sendRawTransaction` (legacy transactions only),
  `eth_getTransactionReceipt`, `eth_estimateGas`. No `eth_call`, no
  storage-slot setter, no impersonation, no session-close endpoint. Any
  unrecognized method returns `method_not_found`.
- `Session` (`crates/engine/src/lib.rs`) already carries an
  `overlay_storage: HashMap<(Address, StorageKey), StorageValue>` field,
  populated by transaction execution via `DatabaseCommit`, but nothing
  currently writes to it directly the way `set_account` does for balances.
- `SessionManager::discard` (`crates/session/src/lib.rs`) exists and is
  wired as an MCP tool, but has no HTTP JSON-RPC or REST route.
- `python/examples/transfer_demo.py` is the existing precedent for driving
  forkyard from Python: `web3.py` `HTTPProvider` pointed at a session URL,
  `eth_account` for local signing, `forkyard_setBalance` via
  `w3.manager.request_blocking`, `uv` for dependency management.

## Component 1 — `FORKYARD_FORK_BLOCK_NUMBER` (block pinning)

**`crates/fetch/src/lib.rs`**
- Extract `block_env_from_provider`'s body into
  `block_env_from_provider_at(provider, block: BlockId)`; keep
  `block_env_from_provider` as a thin wrapper calling it with
  `BlockId::latest()`.
- Add `pub async fn fork_at(rpc_url: &str, block_number: u64) -> eyre::Result<(Fork, BlockEnv)>`,
  identical to `fork` but resolving `BlockId::number(block_number)` and
  passing that block number through to `BlockchainDbMeta::new`.

**`crates/bin/src/main.rs`**
- Read `FORKYARD_FORK_BLOCK_NUMBER` as `Option<u64>` (absent by default —
  behavior for existing users is unchanged).
- If set: call `forkyard_fetch::fork_at(&rpc_url, n)` instead of `fork`,
  and **do not** spawn the `ChainTipFollower` task (log at `info` that
  chain-tip following is disabled because the fork is pinned).
- If unset: existing behavior (fork latest, run `ChainTipFollower`).

## Component 2 — `forkyard_setStorageAt` cheatcode

**`crates/engine/src/lib.rs`**
- Add `Session::set_storage(&mut self, address: Address, key: StorageKey, value: StorageValue)`
  writing into `overlay_storage`, mirroring `set_account`'s doc comment
  style (references Anvil's `anvil_setStorageAt`).
- `Database::storage` read path already checks `overlay_storage` before
  falling back — confirm during implementation; if it doesn't, add the
  same overlay-then-base-then-fallback lookup order `basic` already uses.

**`crates/session/src/lib.rs`**
- Add `SessionManager::set_storage(&self, id: SessionId, address: Address, key: StorageKey, value: StorageValue) -> Result<(), SessionError>`,
  same shape as the existing `set_account` passthrough.

**`crates/api-http/src/lib.rs`**
- Add `"forkyard_setStorageAt"` to `dispatch`: params
  `[address, slot_hex, value_hex]`, calls
  `state.manager.set_storage(...)`, returns `json!(true)` — same pattern
  as `forkyard_setBalance`.

**`crates/api-mcp/src/lib.rs`**
- Add a matching `set_storage` MCP tool for parity with the other
  cheatcodes (not required by the Python benchmark, but keeps the two
  surfaces consistent — low cost given `set_balance`'s tool already
  exists as a template).

## Component 3 — `forkyard_discard` RPC method

**`crates/api-http/src/lib.rs`**
- Add `"forkyard_discard"` to `dispatch`: no params, calls
  `state.manager.discard(session_id).await` and returns `json!(true)`.
  Chose a same-dispatch RPC method over a new `DELETE /session/{id}`
  route to stay inside the existing "`forkyard_*` cheatcode" convention
  used by `forkyard_setBalance`, and to keep the benchmark on one
  request shape.

## Component 4 — Python action library (`python/benchmarks/`)

New directory, sibling to `python/examples/`, own `pyproject.toml` (uv,
`web3.py` + `matplotlib` + `pandas`).

**`backend.py`** — a `Backend` protocol with two implementations:

```python
class Backend(Protocol):
    def web3(self) -> Web3: ...          # a ready HTTPProvider-backed Web3
    def set_native_balance(self, address, wei) -> None: ...
    def set_storage(self, address, slot, value) -> None: ...
    def discard(self) -> None: ...        # ends this agent's session/instance
```

- `ForkyardBackend(session_url)` — wraps `forkyard_setBalance`,
  `forkyard_setStorageAt`, `forkyard_discard` over the session's own RPC
  endpoint.
- `AnvilBackend(port, fork_url, fork_block_number)` — spawns
  `anvil --fork-url <fork_url> --fork-block-number <n> --port <port> --silent`
  as a subprocess in `__init__`, polls until it accepts connections;
  `set_native_balance`/`set_storage` map to `anvil_setBalance` /
  `anvil_setStorageAt`; `discard()` terminates the subprocess (Anvil has
  no lighter-weight session-close concept — the instance *is* the
  session, so tearing it down is the fair equivalent action).

**`actions.py`** — one function per action, each returning
`(label: str, elapsed_ms: float)`, matching `transfer_demo.py`'s `timed()`
helper:

- `transfer(backend, sender, recipient, value)`
- `set_balance(backend, address, value)`
- `get_balance(backend, address)` — `eth_getBalance` + `eth_getTransactionCount`
- `fund_token(backend, token, holder, amount)` — writes `holder`'s
  balance directly via `set_storage` on the token's `balanceOf` mapping
  slot (`TOKENS` registry below); real fund flows don't exist without
  impersonation, so this is the acknowledged cheat, same role
  `forkyard_setBalance` already plays for ETH
- `approve(backend, owner_key, token, spender, amount)` — real
  `approve()` transaction
- `swap_eth_for_token(backend, signer_key, token, amount_in)` — Uniswap
  V2 `swapExactETHForTokens`, router `0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D`
- `swap_token_for_token(backend, signer_key, token_in, token_out, amount_in)` —
  `swapExactTokensForTokens`; only legal after `fund_token` + `approve`
  on `token_in`
- `discard(backend)`

```python
TOKENS = {
    "DAI": {"address": "0x6B175474E89094C44Da98b954EedeAC495271d0F", "balance_slot": 2},
    # USDC intentionally omitted from the default set: it's a proxy
    # contract, and its storage layout doesn't follow the simple
    # `keccak256(abi.encode(holder, slot))` rule reliably enough to trust
    # without per-run verification. Add it later if needed.
}
```

**`agent.py`** — `run_agent(backend, rng, num_actions) -> list[ActionRecord]`:
picks a random legal sequence (respecting the fund → approve → swap
dependency), runs each action, always ends with `discard`, returns
per-action records: `(backend_name, block_height, num_agents, agent_id,
action, elapsed_ms)`.

**`run_benchmark.py`** — CLI entrypoint:
- args: `--agents 1,2,5,10,20,50`, `--block-heights <list>`,
  `--actions-per-agent N`, `--rpc-url`, `--out results.csv`
- for each `(backend_kind, block_height, num_agents)` combination:
  - **forkyard**: start one forkyard subprocess with
    `FORKYARD_FORK_BLOCK_NUMBER=<height>`, poll until ready, open
    `num_agents` sessions, run them concurrently via
    `concurrent.futures.ThreadPoolExecutor(max_workers=num_agents)`,
    record total wall-clock, tear the process down.
  - **anvil**: spawn `num_agents` Anvil subprocesses (sequential ports
    from a base), run one agent per instance concurrently in the same
    thread-pool shape, record total wall-clock, tear all instances down.
  - append every action record plus one run-total record to the CSV.

**`plot_results.py`** — reads the CSV (pandas), produces:
1. Total simulation time vs. number of agents — one line per
   `(backend, block_height)`.
2. Per-action-type median/p95 latency, backend side-by-side, as a bar
   chart.

## Error handling

- Anvil subprocess failing to bind/start: fail that run with a clear
  message naming the port, not a hang — poll with a bounded timeout.
- A single agent's action raising (e.g. a swap revert): caught inside
  `run_agent`, recorded as a failed action row (`elapsed_ms` still
  recorded, `ok=False`), does not abort the rest of that run.
- Missing `anvil` binary on `PATH`: fail fast at startup with an
  actionable message (Foundry install instructions), not a subprocess
  spawn traceback.

## Testing plan

- Rust: unit tests for `fork_at` (mirroring existing `fork` tests, if
  any exist against a real or mocked provider), `Session::set_storage`
  read-back, `SessionManager::set_storage`, and dispatch tests for
  `forkyard_setStorageAt` / `forkyard_discard` (mirroring the existing
  `forkyard_setBalance` dispatch tests in `crates/api-http/src/lib.rs`).
- Python: a small smoke test (or manual run) with `--agents 1 --block-heights <one height>`
  against both backends, asserting every action in the CSV has `ok=True`,
  before running the full sweep.
- Full sweep is run manually (it spawns real subprocesses and hits a
  live/forked mainnet RPC) rather than wired into CI.

## Known limitations (explicitly out of scope, not deferred silently)

- Block pinning is process-wide; testing N agents at N *different*
  heights within a single forkyard process is not possible with this
  design (would require the multi-base architecture noted as a TODO in
  `crates/engine/src/lib.rs`).
- Only DAI is funded via the storage cheatcode by default; other tokens
  need their slot verified before being added to `TOKENS`.
- `eth_sendRawTransaction` only accepts legacy transactions today (an
  existing forkyard constraint), so all actions build legacy-shaped txs.
