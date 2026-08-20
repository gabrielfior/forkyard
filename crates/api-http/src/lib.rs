//! A minimal Ethereum JSON-RPC server in front of one `forkyard-engine`
//! `Session`. This is the piece that was missing from the original
//! `mainnet_transfer` example: that example called `Session` directly,
//! in-process — this crate is what lets an *external* client (`cast`,
//! `alloy`'s own HTTP provider, MetaMask, anything that speaks Ethereum
//! JSON-RPC) talk to a fork over the wire instead.
//!
//! Deliberately narrow: `eth_chainId`, `eth_blockNumber` (stubbed),
//! `eth_getBalance`, `eth_getTransactionCount`, and `eth_sendRawTransaction`
//! for legacy transactions only — exactly enough to repeat the
//! fork/transfer/assert-balances/discard flow over RPC. Not the production
//! MCP/SDK surface (that's `api-mcp`, which stays in-process and therefore
//! doesn't pay this crate's JSON/HTTP serialization cost).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use alloy_consensus::transaction::SignerRecoverable;
use alloy_consensus::TxEnvelope;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::Address;
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use forkyard_engine::{BaseSnapshot, Session};
use forkyard_fetch::Fork;
use revm::context::TxEnv;
use revm::{Database, ExecuteCommitEvm, MainBuilder, MainContext};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

struct AppState {
    session: Mutex<Session<Fork>>,
    chain_id: u64,
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

#[derive(Serialize)]
struct RpcErrorObj {
    code: i64,
    message: String,
}

impl RpcErrorObj {
    fn invalid_params(message: impl std::fmt::Display) -> Self {
        Self { code: -32602, message: message.to_string() }
    }
    fn method_not_found(method: &str) -> Self {
        Self { code: -32601, message: format!("method not found: {method}") }
    }
    fn execution(message: impl std::fmt::Display) -> Self {
        Self { code: -32000, message: message.to_string() }
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

/// Handles one JSON-RPC call against the shared session. Locking is
/// synchronous and held only across the (in-memory, non-blocking) call —
/// nothing here awaits while the lock is live.
fn dispatch(state: &AppState, method: &str, params: &[Value]) -> Result<Value, RpcErrorObj> {
    match method {
        "eth_chainId" => Ok(json!(format!("0x{:x}", state.chain_id))),

        // TODO: track the fork's real pinned block number instead of a
        // constant, once `forkyard-ingest` (or the fork's own metadata) is
        // wired through to this layer.
        "eth_blockNumber" => Ok(json!("0x0")),

        "eth_getBalance" => {
            let address = parse_address(params, 0)?;
            let mut session = state.session.lock().unwrap();
            let balance = session
                .basic(address)
                .map_err(RpcErrorObj::execution)?
                .map(|info| info.balance)
                .unwrap_or_default();
            Ok(json!(format!("0x{balance:x}")))
        }

        "eth_getTransactionCount" => {
            let address = parse_address(params, 0)?;
            let mut session = state.session.lock().unwrap();
            let nonce = session
                .basic(address)
                .map_err(RpcErrorObj::execution)?
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
            let balance_hex = param_str(params, 1)?;
            let balance = alloy_primitives::U256::from_str_radix(
                balance_hex.trim_start_matches("0x"),
                16,
            )
            .map_err(RpcErrorObj::invalid_params)?;

            let mut session = state.session.lock().unwrap();
            let mut info = session.basic(address).map_err(RpcErrorObj::execution)?.unwrap_or_default();
            info.balance = balance;
            session.set_account(address, info);
            Ok(json!(true))
        }

        "eth_sendRawTransaction" => {
            let raw = parse_raw_tx(params, 0)?;
            let envelope = TxEnvelope::decode_2718(&mut raw.as_slice())
                .map_err(RpcErrorObj::invalid_params)?;
            let TxEnvelope::Legacy(signed) = &envelope else {
                return Err(RpcErrorObj::invalid_params(
                    "only legacy transactions are supported for now",
                ));
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

            let mut session = state.session.lock().unwrap();
            revm::Context::mainnet()
                .with_db(&mut *session)
                .build_mainnet()
                .transact_commit(tx_env)
                .map_err(RpcErrorObj::execution)?;

            Ok(json!(format!("{tx_hash:#x}")))
        }

        other => Err(RpcErrorObj::method_not_found(other)),
    }
}

async fn rpc_handler(State(state): State<Arc<AppState>>, Json(req): Json<RpcRequest>) -> Json<RpcResponse> {
    match dispatch(&state, &req.method, &req.params) {
        Ok(result) => Json(RpcResponse {
            jsonrpc: "2.0",
            id: req.id,
            result: Some(result),
            error: None,
        }),
        Err(error) => Json(RpcResponse {
            jsonrpc: "2.0",
            id: req.id,
            result: None,
            error: Some(error),
        }),
    }
}

/// A running server. Drop it — or call `shutdown`, equivalent — to tear
/// down the listener and the underlying fork's background fetch thread,
/// same "discard" semantics as dropping a `Session` directly.
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
/// ephemeral port) fronting a single fork/session built from `fork`.
pub async fn serve(bind_addr: &str, fork: Fork, chain_id: u64) -> eyre::Result<Handle> {
    let session = Session::fork(Arc::new(BaseSnapshot::default()), fork);
    let state = Arc::new(AppState {
        session: Mutex::new(session),
        chain_id,
    });
    let app = Router::new().route("/", post(rpc_handler)).with_state(state);

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
