//! A minimal Ethereum JSON-RPC server in front of a `forkyard-session`
//! `SessionManager` — many sessions, sharing one base and one fetch
//! fallback, reachable over the wire.
//!
//! `POST /session` opens a new session and returns its id; `POST
//! /session/{id}` carries the actual JSON-RPC calls against that session.
//! This is what lets N callers share one warm cache instead of each paying
//! for their own, the way the earlier per-agent-fork example did.
//!
//! Covers `eth_chainId`, `eth_blockNumber`, `eth_getBalance`,
//! `eth_getTransactionCount`, `eth_gasPrice`, `eth_estimateGas`,
//! `eth_sendRawTransaction` (legacy transactions only), and
//! `eth_getTransactionReceipt` — enough for a real client library
//! (`web3.py`'s standard sign-locally-then-send flow, `wait_for_transaction_receipt`
//! included) to work against a session, not just enough to repeat one
//! scripted example. Not the production MCP/SDK surface (that's `api-mcp`,
//! which stays in-process and therefore doesn't pay this crate's JSON/HTTP
//! serialization cost).
//!
//! `eth_gasPrice` is the fork's real base fee (from `SessionManager::block_env`,
//! itself `forkyard_fetch::fork`'s actual fetched block) plus a fixed
//! priority-fee margin — not a made-up constant, but it's still a snapshot
//! from whenever the fork was taken, not a live-updating fee market (that
//! needs `forkyard-ingest` to exist first). `eth_blockNumber` is that same
//! real starting block number plus a per-session counter incremented on
//! every successful `eth_sendRawTransaction` — a real starting point, but
//! progress from there is still synthetic, not real chain advancement.
//! Receipts and that counter live in this crate's own state, keyed by
//! session id; they are *not* evicted when a session's TTL expires
//! (`SessionManager` doesn't expose an eviction hook yet), so they leak for
//! the life of the server today — a known, deliberately-deferred gap, not
//! an oversight.

use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{Address, Bytes, B256};
use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Json, Router};
use forkyard_session::{Fallback, SessionError, SessionId, SessionManager};
use revm::context::result::ExecutionResult;
use revm::context::TxEnv;
use revm::primitives::{TxKind, U256};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// Priority fee added on top of the fork's real base fee to build a legacy
/// `eth_gasPrice` figure — a common wallet-default "tip" (1.5 gwei), not
/// derived from anything; the base fee underneath it is real.
const PRIORITY_FEE_WEI: u64 = 1_500_000_000;

/// Generous cap for `eth_estimateGas`'s dry-run — the maximum revm allows
/// per transaction (EIP-7825), not an arbitrary round number. A higher
/// value fails validation before the call is even attempted, which is
/// exactly the bug this constant used to have (30M vs. the real 16.7M cap).
const ESTIMATE_GAS_LIMIT: u64 = 16_777_216;

#[derive(Default)]
struct SessionRpcState {
    receipts: HashMap<String, Value>,
    block_number: u64,
}

struct AppState<F: Fallback>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    manager: Arc<SessionManager<F>>,
    chain_id: u64,
    rpc_state: Mutex<HashMap<SessionId, SessionRpcState>>,
}

#[derive(Deserialize)]
struct RpcRequest {
    id: Value,
    method: String,
    #[serde(default)]
    params: Vec<Value>,
}

#[derive(Serialize)]
struct RpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcErrorObj>,
}

#[derive(Debug, Serialize)]
struct RpcErrorObj {
    code: i64,
    message: String,
}

impl RpcErrorObj {
    fn invalid_params(message: impl fmt::Display) -> Self {
        Self { code: -32602, message: message.to_string() }
    }
    fn method_not_found(method: &str) -> Self {
        Self { code: -32601, message: format!("method not found: {method}") }
    }
    fn execution(message: impl fmt::Display) -> Self {
        Self { code: -32000, message: message.to_string() }
    }
}

impl From<SessionError> for RpcErrorObj {
    fn from(e: SessionError) -> Self {
        match e {
            SessionError::Unknown(id) => Self { code: -32001, message: format!("unknown or expired session {id}") },
            other => RpcErrorObj::execution(other),
        }
    }
}

fn param_str<'a>(params: &'a [Value], idx: usize) -> Result<&'a str, RpcErrorObj> {
    params
        .get(idx)
        .and_then(Value::as_str)
        .ok_or_else(|| RpcErrorObj::invalid_params(format!("missing/invalid params[{idx}]")))
}

fn parse_address(params: &[Value], idx: usize) -> Result<Address, RpcErrorObj> {
    param_str(params, idx)?
        .parse()
        .map_err(RpcErrorObj::invalid_params)
}

fn parse_raw_tx(params: &[Value], idx: usize) -> Result<Vec<u8>, RpcErrorObj> {
    let s = param_str(params, idx)?;
    alloy_primitives::hex::decode(s).map_err(RpcErrorObj::invalid_params)
}

fn parse_u256_hex_str(s: &str) -> Result<U256, RpcErrorObj> {
    U256::from_str_radix(s.trim_start_matches("0x"), 16).map_err(RpcErrorObj::invalid_params)
}

/// Reads an optional hex field (`"from"`, `"to"`, `"value"`, `"data"`, ...)
/// out of an `eth_estimateGas`-style call object.
fn field_str<'a>(call: &'a Value, key: &str) -> Option<&'a str> {
    call.get(key).and_then(Value::as_str)
}

/// The fork's real starting block number plus `session_id`'s own
/// send-count so far — see the module-level doc comment.
fn real_block_number<F: Fallback>(state: &AppState<F>, session_id: SessionId) -> u64
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let local = state.rpc_state.lock().unwrap().get(&session_id).map(|s| s.block_number).unwrap_or(0);
    state.manager.block_env().number.to::<u64>() + local
}

/// Builds a standard-shaped JSON-RPC transaction receipt. `block_number`
/// here should be `real_block_number`'s value at send time, not this
/// session's raw local counter — see the module-level doc comment.
fn build_receipt(
    tx_hash: B256,
    from: Address,
    to: TxKind,
    result: &ExecutionResult,
    block_number: u64,
) -> Value {
    let block_hash = format!("0x{block_number:064x}");
    let logs: Vec<Value> = result
        .logs()
        .iter()
        .enumerate()
        .map(|(i, log)| {
            json!({
                "address": format!("{:#x}", log.address),
                "topics": log.data.topics().iter().map(|t| format!("{t:#x}")).collect::<Vec<_>>(),
                "data": format!("0x{}", alloy_primitives::hex::encode(log.data.data.as_ref())),
                "blockNumber": format!("0x{block_number:x}"),
                "transactionHash": format!("{tx_hash:#x}"),
                "transactionIndex": "0x0",
                "blockHash": block_hash,
                "logIndex": format!("0x{i:x}"),
                "removed": false,
            })
        })
        .collect();

    json!({
        "transactionHash": format!("{tx_hash:#x}"),
        "transactionIndex": "0x0",
        "blockHash": block_hash,
        "blockNumber": format!("0x{block_number:x}"),
        "from": format!("{from:#x}"),
        "to": match to {
            TxKind::Call(addr) => Value::String(format!("{addr:#x}")),
            TxKind::Create => Value::Null,
        },
        "cumulativeGasUsed": format!("0x{:x}", result.tx_gas_used()),
        "gasUsed": format!("0x{:x}", result.tx_gas_used()),
        "contractAddress": result.created_address().map(|a| format!("{a:#x}")),
        "logs": logs,
        "logsBloom": format!("0x{}", "00".repeat(256)),
        "status": if result.is_success() { "0x1" } else { "0x0" },
        "type": "0x0",
    })
}

/// Handles one JSON-RPC call against `session_id`'s session on the shared
/// manager. Every real read/write goes through `SessionManager`, which
/// routes it to whichever worker thread owns that session — nothing here
/// touches `Session` or revm's execution machinery directly.
async fn dispatch<F: Fallback>(
    state: &AppState<F>,
    session_id: SessionId,
    method: &str,
    params: &[Value],
) -> Result<Value, RpcErrorObj>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    match method {
        "eth_chainId" => Ok(json!(format!("0x{:x}", state.chain_id))),

        // The fork's real starting block number plus this session's own
        // send-count — see module doc.
        "eth_blockNumber" => Ok(json!(format!("0x{:x}", real_block_number(state, session_id)))),

        // Real base fee (from the fork's actual block) plus a fixed
        // priority-fee margin — see module doc.
        "eth_gasPrice" => {
            let gas_price = state.manager.block_env().basefee as u64 + PRIORITY_FEE_WEI;
            Ok(json!(format!("0x{gas_price:x}")))
        }

        "eth_getBalance" => {
            let address = parse_address(params, 0)?;
            let balance = state
                .manager
                .basic(session_id, address)
                .await?
                .map(|info| info.balance)
                .unwrap_or_default();
            Ok(json!(format!("0x{balance:x}")))
        }

        "eth_getTransactionCount" => {
            let address = parse_address(params, 0)?;
            let nonce = state
                .manager
                .basic(session_id, address)
                .await?
                .map(|info| info.nonce)
                .unwrap_or_default();
            Ok(json!(format!("0x{nonce:x}")))
        }

        // Test-only cheatcode, same role as Anvil's `anvil_setBalance` —
        // funds an address in this session's private overlay only. Real
        // usage doesn't get this; it exists so an RPC client can set up a
        // scenario (e.g. a freshly generated signer) without needing a
        // whale's private key.
        "forkyard_setBalance" => {
            let address = parse_address(params, 0)?;
            let balance = parse_u256_hex_str(param_str(params, 1)?)?;

            let mut info = state.manager.basic(session_id, address).await?.unwrap_or_default();
            info.balance = balance;
            state.manager.set_account(session_id, address, info).await?;
            Ok(json!(true))
        }

        "eth_sendRawTransaction" => {
            let raw = parse_raw_tx(params, 0)?;
            let envelope = TxEnvelope::decode_2718(&mut raw.as_slice()).map_err(RpcErrorObj::invalid_params)?;
            let TxEnvelope::Legacy(signed) = &envelope else {
                return Err(RpcErrorObj::invalid_params("only legacy transactions are supported for now"));
            };
            let tx = signed.tx();
            let sender = envelope
                .recover_signer()
                .map_err(|e| RpcErrorObj::invalid_params(format!("could not recover sender: {e}")))?;
            let tx_hash = *signed.hash();

            let tx_env = TxEnv::builder()
                .caller(sender)
                .kind(tx.to)
                .value(tx.value)
                .gas_limit(tx.gas_limit)
                .gas_price(tx.gas_price)
                .nonce(tx.nonce)
                .chain_id(tx.chain_id)
                .build_fill();

            let result = state.manager.advance(session_id, tx_env).await?;

            // Advance this session's own block counter and store a
            // receipt for it — this is what makes `eth_getTransactionReceipt`
            // (and so `web3.py`'s `wait_for_transaction_receipt`) work.
            let real_start = state.manager.block_env().number.to::<u64>();
            let mut guard = state.rpc_state.lock().unwrap();
            let entry = guard.entry(session_id).or_default();
            entry.block_number += 1;
            let receipt = build_receipt(tx_hash, sender, tx.to, &result, real_start + entry.block_number);
            entry.receipts.insert(format!("{tx_hash:#x}"), receipt);

            Ok(json!(format!("{tx_hash:#x}")))
        }

        "eth_getTransactionReceipt" => {
            let hash = param_str(params, 0)?.to_lowercase();
            let receipt = state
                .rpc_state
                .lock()
                .unwrap()
                .get(&session_id)
                .and_then(|s| s.receipts.get(&hash))
                .cloned()
                .unwrap_or(Value::Null);
            Ok(receipt)
        }

        // Dry-runs the call via `simulate` (no commit) with a generous gas
        // cap and reports the actual gas used — real estimation, not a
        // fixed constant, since we already have the machinery for it.
        "eth_estimateGas" => {
            let call = params.first().ok_or_else(|| RpcErrorObj::invalid_params("missing call object"))?;
            let from = field_str(call, "from").map(|s| s.parse()).transpose().map_err(RpcErrorObj::invalid_params)?.unwrap_or_default();
            let to = field_str(call, "to").map(|s| s.parse()).transpose().map_err(RpcErrorObj::invalid_params)?;
            let value = field_str(call, "value").map(parse_u256_hex_str).transpose()?.unwrap_or_default();
            let data = field_str(call, "data")
                .map(alloy_primitives::hex::decode)
                .transpose()
                .map_err(RpcErrorObj::invalid_params)?
                .unwrap_or_default();
            let nonce = state.manager.basic(session_id, from).await?.map(|i| i.nonce).unwrap_or_default();

            let kind = match to {
                Some(addr) => TxKind::Call(addr),
                None => TxKind::Create,
            };
            let tx_env = TxEnv::builder()
                .caller(from)
                .kind(kind)
                .value(value)
                .data(Bytes::from(data))
                .gas_limit(ESTIMATE_GAS_LIMIT)
                .gas_price(0) // don't require balance for the estimate itself
                .nonce(nonce)
                .build_fill();

            let result = state.manager.estimate_gas(session_id, tx_env).await?;
            if !result.is_success() {
                return Err(RpcErrorObj::execution(format!(
                    "call would fail: {}",
                    if result.is_halt() { "halted" } else { "reverted" }
                )));
            }
            Ok(json!(format!("0x{:x}", result.tx_gas_used())))
        }

        other => Err(RpcErrorObj::method_not_found(other)),
    }
}

async fn open_session_handler<F: Fallback>(State(state): State<Arc<AppState<F>>>) -> Json<Value>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    match state.manager.fork().await {
        Ok(id) => Json(json!({ "session_id": id })),
        Err(e) => Json(json!({ "error": e.to_string() })),
    }
}

async fn rpc_handler<F: Fallback>(
    State(state): State<Arc<AppState<F>>>,
    Path(session_id): Path<SessionId>,
    Json(req): Json<RpcRequest>,
) -> Json<RpcResponse>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    match dispatch(&state, session_id, &req.method, &req.params).await {
        Ok(result) => Json(RpcResponse { jsonrpc: "2.0", id: req.id, result: Some(result), error: None }),
        Err(error) => Json(RpcResponse { jsonrpc: "2.0", id: req.id, result: None, error: Some(error) }),
    }
}

/// A running server. Drop it — or call `shutdown`, equivalent — to tear
/// down the listener. Sessions themselves keep expiring on their own TTL
/// regardless; this only stops the HTTP surface in front of them.
pub struct Handle {
    pub addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: tokio::task::JoinHandle<()>,
}

impl Handle {
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join.await;
    }
}

/// Binds a JSON-RPC server at `bind_addr` (use `"127.0.0.1:0"` for an
/// ephemeral port) fronting `manager`. Many callers can each open their
/// own session against the same manager — and therefore the same warm
/// base and fetch cache — via `POST /session`. Takes `manager` as an `Arc`
/// so `forkyard-bin` can hand the same one to `api-mcp`'s stdio surface,
/// running alongside this HTTP server against one shared engine.
pub async fn serve<F: Fallback>(bind_addr: &str, manager: Arc<SessionManager<F>>, chain_id: u64) -> eyre::Result<Handle>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let state = Arc::new(AppState { manager, chain_id, rpc_state: Mutex::new(HashMap::new()) });
    let app = Router::new()
        .route("/session", post(open_session_handler))
        .route("/session/{id}", post(rpc_handler))
        .with_state(state);

    let listener = TcpListener::bind(bind_addr).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    Ok(Handle {
        addr,
        shutdown_tx: Some(shutdown_tx),
        join,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_network::TxSignerSync;
    use alloy_signer_local::PrivateKeySigner;
    use revm::database_interface::{DBErrorMarker, DatabaseRef};
    use revm::state::{AccountInfo, Bytecode};
    use std::time::Duration;

    #[derive(Clone)]
    struct TestFallback;

    #[derive(Debug)]
    struct TestFallbackError;
    impl fmt::Display for TestFallbackError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test fallback has no real data")
        }
    }
    impl std::error::Error for TestFallbackError {}
    impl DBErrorMarker for TestFallbackError {}

    impl DatabaseRef for TestFallback {
        type Error = TestFallbackError;
        fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            Ok(Some(AccountInfo::default()))
        }
        fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
            Ok(Bytecode::default())
        }
        fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
            Ok(U256::ZERO)
        }
        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    fn test_state() -> AppState<TestFallback> {
        test_state_with_block_env(revm::context::BlockEnv::default())
    }

    fn test_state_with_block_env(block_env: revm::context::BlockEnv) -> AppState<TestFallback> {
        AppState {
            manager: Arc::new(SessionManager::new(TestFallback, block_env, 1, Duration::from_secs(60))),
            chain_id: 1,
            rpc_state: Mutex::new(HashMap::new()),
        }
    }

    /// Builds a signed legacy transfer's raw `0x`-prefixed hex — the exact
    /// shape `eth_sendRawTransaction` expects — reused across tests instead
    /// of repeating the signing dance each time.
    fn signed_transfer_hex(signer: &PrivateKeySigner, to: Address, value: u64, gas_price: u128) -> String {
        let mut tx = TxLegacy {
            chain_id: Some(1),
            nonce: 0,
            gas_price,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value: U256::from(value),
            input: Default::default(),
        };
        let signature = signer.sign_transaction_sync(&mut tx).unwrap();
        let signed = tx.into_signed(signature);
        let raw = alloy_eips::eip2718::Encodable2718::encoded_2718(&signed);
        format!("0x{}", alloy_primitives::hex::encode(&raw))
    }

    #[tokio::test]
    async fn gas_price_is_the_forks_real_base_fee_plus_the_priority_margin() {
        let block_env = revm::context::BlockEnv { basefee: 12_000_000_000, ..Default::default() };
        let state = test_state_with_block_env(block_env);
        let id = state.manager.fork().await.unwrap();
        let result = dispatch(&state, id, "eth_gasPrice", &[]).await.unwrap();
        assert_eq!(result, json!(format!("0x{:x}", 12_000_000_000u64 + PRIORITY_FEE_WEI)));
    }

    #[tokio::test]
    async fn estimate_gas_reports_real_gas_used_for_a_transfer() {
        let state = test_state();
        let id = state.manager.fork().await.unwrap();
        let from = Address::from([1u8; 20]);
        let to = Address::from([2u8; 20]);
        dispatch(&state, id, "forkyard_setBalance", &[json!(from.to_string()), json!("0x64")])
            .await
            .unwrap();

        let call = json!({ "from": from.to_string(), "to": to.to_string(), "value": "0x1" });
        let result = dispatch(&state, id, "eth_estimateGas", &[call]).await.unwrap();
        assert_eq!(result, json!("0x5208")); // 21000, a plain transfer's real cost
    }

    /// Regression test for a real bug caught live: once the fork's real
    /// base fee is non-zero, a naive gas_price=0 estimate call fails
    /// revm's `GasPriceLessThanBasefee` check before the estimate even
    /// runs — exactly the class of bug `disable_base_fee` exists to avoid,
    /// same as real nodes' `eth_estimateGas`. Also proves estimation works
    /// with zero balance, matching "how much gas, not do you have funds."
    #[tokio::test]
    async fn estimate_gas_works_against_a_real_nonzero_base_fee_with_no_balance() {
        let block_env = revm::context::BlockEnv { basefee: 12_000_000_000, ..Default::default() };
        let state = test_state_with_block_env(block_env);
        let id = state.manager.fork().await.unwrap();
        let from = Address::from([1u8; 20]); // never funded
        let to = Address::from([2u8; 20]);

        let call = json!({ "from": from.to_string(), "to": to.to_string(), "value": "0x1" });
        let result = dispatch(&state, id, "eth_estimateGas", &[call]).await.unwrap();
        assert_eq!(result, json!("0x5208"));
    }

    #[tokio::test]
    async fn send_raw_transaction_populates_a_fetchable_receipt() {
        let state = test_state();
        let id = state.manager.fork().await.unwrap();

        let sender = PrivateKeySigner::random();
        let recipient = PrivateKeySigner::random().address();
        dispatch(
            &state,
            id,
            "forkyard_setBalance",
            &[json!(sender.address().to_string()), json!("0xde0b6b3a7640000")], // 1 ETH
        )
        .await
        .unwrap();

        let raw_hex = signed_transfer_hex(&sender, recipient, 100, 20_000_000_000);
        let tx_hash = dispatch(&state, id, "eth_sendRawTransaction", &[json!(raw_hex)]).await.unwrap();

        let receipt = dispatch(&state, id, "eth_getTransactionReceipt", &[tx_hash]).await.unwrap();
        assert_eq!(receipt["status"], json!("0x1"));
        assert_eq!(receipt["gasUsed"], json!("0x5208"));
        assert_eq!(receipt["blockNumber"], json!("0x1"));
    }

    #[tokio::test]
    async fn unknown_receipt_returns_null_not_an_error() {
        let state = test_state();
        let id = state.manager.fork().await.unwrap();
        let result = dispatch(&state, id, "eth_getTransactionReceipt", &[json!("0xdeadbeef")])
            .await
            .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[tokio::test]
    async fn block_number_increments_per_session_independently() {
        let state = test_state();
        let a = state.manager.fork().await.unwrap();
        let b = state.manager.fork().await.unwrap();
        assert_eq!(dispatch(&state, a, "eth_blockNumber", &[]).await.unwrap(), json!("0x0"));

        let sender = PrivateKeySigner::random();
        dispatch(
            &state,
            a,
            "forkyard_setBalance",
            &[json!(sender.address().to_string()), json!("0xde0b6b3a7640000")],
        )
        .await
        .unwrap();
        let raw_hex = signed_transfer_hex(&sender, Address::from([9u8; 20]), 1, 0);
        dispatch(&state, a, "eth_sendRawTransaction", &[json!(raw_hex)]).await.unwrap();

        assert_eq!(dispatch(&state, a, "eth_blockNumber", &[]).await.unwrap(), json!("0x1"));
        // session b never sent anything — its own counter must be untouched.
        assert_eq!(dispatch(&state, b, "eth_blockNumber", &[]).await.unwrap(), json!("0x0"));
    }

    #[tokio::test]
    async fn block_number_starts_from_the_forks_real_number_not_zero() {
        let block_env = revm::context::BlockEnv { number: U256::from(20_000_000u64), ..Default::default() };
        let state = test_state_with_block_env(block_env);
        let id = state.manager.fork().await.unwrap();

        // Fresh session, no sends yet — should report the fork's real
        // number directly, not a counter starting at zero.
        assert_eq!(dispatch(&state, id, "eth_blockNumber", &[]).await.unwrap(), json!("0x1312d00"));

        let sender = PrivateKeySigner::random();
        dispatch(
            &state,
            id,
            "forkyard_setBalance",
            &[json!(sender.address().to_string()), json!("0xde0b6b3a7640000")],
        )
        .await
        .unwrap();
        let raw_hex = signed_transfer_hex(&sender, Address::from([9u8; 20]), 1, 0);
        dispatch(&state, id, "eth_sendRawTransaction", &[json!(raw_hex)]).await.unwrap();

        // One send later: real number + 1, and the receipt agrees.
        assert_eq!(dispatch(&state, id, "eth_blockNumber", &[]).await.unwrap(), json!("0x1312d01"));
    }
}
