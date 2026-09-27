//! The production surface: an MCP server exposing `fork` / `simulate` /
//! `advance` / `discard`, the reads `get_balance` / `get_storage` /
//! `get_code`, the same `set_balance` and `set_storage` test
//! cheatcodes `forkyard-api-http` has, and `snapshot` / `resume` for
//! saving a session to disk and reopening it later, as tools backed
//! directly by a `forkyard-session::SessionManager`.
//!
//! `simulate` and `advance` carry `data`, so they express a contract call
//! rather than only an ETH transfer, and answer with the call's return
//! data (or a revert's) — without both halves an agent cannot read a
//! contract at all. Omitting `to` makes the transaction a deploy. Every
//! argument struct is `deny_unknown_fields`: an unknown field used to be
//! dropped in silence, which made a contract call come back as a
//! plausible-looking bare transfer instead of an error. This is what an agent framework
//! (Claude Code, Cursor, ElizaOS, ...) actually calls — in-process via
//! stdio, so none of `api-http`'s JSON/HTTP serialization cost applies
//! here (see docs/RESEARCH.md, "Integration path").
//!
//! Built on `rmcp`, the official Rust MCP SDK, specifically to foreclose
//! the handshake bug that broke a prior MCP server on the Hermes dogfood
//! target — its typed `Parameters<T>` derives the tool schema instead of
//! hand-writing one.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use forkyard_session::{Fallback, SessionError, SessionId, SessionManager};
use revm::context::result::{ExecutionResult, InvalidTransaction};
use revm::context::TxEnv;
use revm::primitives::{hex, Address, Bytes, TxKind, U256};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{schemars, tool, tool_handler, tool_router, ErrorData, ServerHandler};
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn tool_err(e: impl fmt::Display) -> ErrorData {
    ErrorData::internal_error(e.to_string(), None)
}

fn parse_address(s: &str) -> Result<Address, ErrorData> {
    s.parse().map_err(|e| ErrorData::invalid_params(format!("bad address: {e}"), None))
}

/// Calldata, as `0x`-prefixed hex. Rejects odd-length and non-hex input
/// rather than truncating — silently mangled calldata is the failure mode
/// this whole argument exists to avoid.
fn parse_bytes_hex(s: &str) -> Result<Bytes, ErrorData> {
    let body = s.strip_prefix("0x").unwrap_or(s);
    hex::decode(body)
        .map(Bytes::from)
        .map_err(|e| ErrorData::invalid_params(format!("bad hex data: {e}"), None))
}

fn parse_u256_hex(s: &str) -> Result<U256, ErrorData> {
    U256::from_str_radix(s.trim_start_matches("0x"), 16)
        .map_err(|e| ErrorData::invalid_params(format!("bad hex value: {e}"), None))
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SessionArgs {
    /// Session id returned by `fork`.
    session_id: SessionId,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ResumeArgs {
    /// Snapshot id returned by `snapshot`.
    snapshot_id: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct BalanceArgs {
    session_id: SessionId,
    /// Account address, `0x`-prefixed hex.
    address: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetBalanceArgs {
    session_id: SessionId,
    address: String,
    /// New balance in wei, `0x`-prefixed hex.
    balance: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SetStorageArgs {
    session_id: SessionId,
    address: String,
    /// Storage slot index, `0x`-prefixed hex.
    slot: String,
    /// New value for that slot, `0x`-prefixed hex (32 bytes).
    value: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct GetStorageArgs {
    session_id: SessionId,
    address: String,
    /// Storage slot index, `0x`-prefixed hex.
    slot: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct TransferArgs {
    session_id: SessionId,
    /// Sender address — must already be funded in this session's overlay
    /// (see `set_balance`) or hold real forked balance.
    from: String,
    /// Recipient, `0x`-prefixed hex. Omit it to deploy instead: the init
    /// code in `data` runs and the new contract's address comes back in
    /// `contract_address`.
    #[serde(default)]
    to: Option<String>,
    /// Value in wei, `0x`-prefixed hex.
    value: String,
    #[serde(default = "default_gas_limit")]
    gas_limit: u64,
    /// Gas price in wei. Omit it to price the transaction at this fork's
    /// own basefee, which is what makes it valid on a live chain — the old
    /// default of `0` is below every real chain's basefee and so always
    /// failed. Pass a value explicitly to ask whether a transaction at
    /// *that* price would work; `0` means literally zero, valid only on a
    /// zero-basefee fork.
    #[serde(default)]
    gas_price: Option<u64>,
    #[serde(default)]
    nonce: u64,
    /// Calldata, `0x`-prefixed hex — an ABI-encoded function call such as
    /// `balanceOf(address)`. Omit it for a plain ETH transfer. Required
    /// for any contract interaction: without it the transaction reaches
    /// the callee with an empty input, which most contracts reject.
    #[serde(default)]
    data: Option<String>,
}

fn default_gas_limit() -> u64 {
    21_000
}

/// MCP tool surface for one `SessionManager<F>`. Every tool call routes
/// straight into the manager — no HTTP, no JSON-RPC envelope, no session
/// mutex to contend with the way `forkyard-api-http` needs one. Takes the
/// manager as an `Arc` so `forkyard-bin` can hand the same one, still owned
/// by an `api-http` server running alongside it, to this stdio surface too.
pub struct ForkyardMcpServer<F: Fallback>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    manager: Arc<SessionManager<F>>,
    #[allow(dead_code)] // read by the tool_handler macro's generated code
    tool_router: ToolRouter<Self>,
}

impl<F: Fallback> ForkyardMcpServer<F>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    pub fn new(manager: Arc<SessionManager<F>>) -> Self {
        Self { manager, tool_router: Self::tool_router() }
    }

    /// Serves this tool surface over stdio — the transport an agent
    /// framework (Claude Code, Cursor, Hermes) launches as a subprocess.
    pub async fn serve_stdio(self) -> eyre::Result<()>
    where
        F: 'static,
    {
        use rmcp::ServiceExt;
        self.serve(rmcp::transport::io::stdio()).await?.waiting().await?;
        Ok(())
    }

    /// Serves the same tool surface over MCP's Streamable HTTP transport at
    /// `http://{bind_addr}/mcp` — for any client that isn't launching this
    /// as a subprocess over stdio (a browser-based agent, a teammate's
    /// mcp-cli pointed at a `"url"` config entry, a client on another
    /// machine). Each HTTP client gets its own `ForkyardMcpServer` instance
    /// (rmcp's per-session factory model) sharing the same underlying
    /// `manager` — cheap, since a fresh instance here is just another `Arc`
    /// clone plus a tool-router rebuild, not a new session-manager.
    pub async fn serve_http(manager: Arc<SessionManager<F>>, bind_addr: &str) -> eyre::Result<Handle>
    where
        F: 'static,
    {
        let service = StreamableHttpService::new(
            move || Ok(Self::new(Arc::clone(&manager))),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default(),
        );
        let app = axum::Router::new().nest_service("/mcp", service);

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

        Ok(Handle { addr, shutdown_tx: Some(shutdown_tx), join })
    }
}

/// Handle to a running `serve_http` server — mirrors `forkyard-api-http`'s
/// own `Handle` shape, since both surfaces get shut down the same way from
/// `forkyard-bin`.
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

#[tool_router]
impl<F: Fallback> ForkyardMcpServer<F>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    #[tool(description = "Fork a new session off the shared base and fetch cache. Returns the session_id to use with every other tool.")]
    async fn fork(&self) -> Result<CallToolResult, ErrorData> {
        let id = self.manager.fork().await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(id.to_string())]))
    }

    #[tool(description = "Read an account's balance and nonce in a session — overlay, then base, then the fetch fallback.")]
    async fn get_balance(&self, Parameters(args): Parameters<BalanceArgs>) -> Result<CallToolResult, ErrorData> {
        let address = parse_address(&args.address)?;
        let info = self.manager.basic(args.session_id, address).await.map_err(tool_err)?.unwrap_or_default();
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "{{\"balance\":\"0x{:x}\",\"nonce\":{}}}",
            info.balance, info.nonce
        ))]))
    }

    #[tool(description = "Test-only cheatcode: override an account's balance in this session's private overlay only, e.g. to fund a freshly generated signer. Never touches the shared base or the real chain.")]
    async fn set_balance(&self, Parameters(args): Parameters<SetBalanceArgs>) -> Result<CallToolResult, ErrorData> {
        let address = parse_address(&args.address)?;
        let balance = parse_u256_hex(&args.balance)?;
        let mut info = self.manager.basic(args.session_id, address).await.map_err(tool_err)?.unwrap_or_default();
        info.balance = balance;
        self.manager.set_account(args.session_id, address, info).await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text("true")]))
    }

    #[tool(description = "Test-only cheatcode: override a single storage slot in this session's private overlay only, e.g. to fund an ERC-20 balanceOf mapping entry. Never touches the shared base or the real chain.")]
    async fn set_storage(&self, Parameters(args): Parameters<SetStorageArgs>) -> Result<CallToolResult, ErrorData> {
        let address = parse_address(&args.address)?;
        let slot = parse_u256_hex(&args.slot)?;
        let value = parse_u256_hex(&args.value)?;
        self.manager.set_storage(args.session_id, address, slot, value).await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text("true")]))
    }

    #[tool(description = "Read one raw storage slot of a contract in a session — overlay, then base, then the fetch fallback. Returns the 32-byte value as 0x-prefixed hex. Use with a computed mapping slot to read e.g. an ERC-20 balanceOf entry without executing a transaction.")]
    async fn get_storage(&self, Parameters(args): Parameters<GetStorageArgs>) -> Result<CallToolResult, ErrorData> {
        let address = parse_address(&args.address)?;
        let slot = parse_u256_hex(&args.slot)?;
        let value = self.manager.storage(args.session_id, address, slot).await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!("0x{value:064x}"))]))
    }

    #[tool(description = "Read an account's deployed bytecode in a session, as 0x-prefixed hex — `0x` for an externally owned account. Use to tell a contract from an EOA, or to confirm a deploy landed.")]
    async fn get_code(&self, Parameters(args): Parameters<BalanceArgs>) -> Result<CallToolResult, ErrorData> {
        let address = parse_address(&args.address)?;
        let code = self.manager.code(args.session_id, address).await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!("0x{}", hex::encode(code)))]))
    }

    #[tool(description = "Run a transfer read-only against a session — no commit, nothing persists. Use to preview a transaction's effect.")]
    async fn simulate(&self, Parameters(args): Parameters<TransferArgs>) -> Result<CallToolResult, ErrorData> {
        run_transfer(&self.manager, args, false).await
    }

    #[tool(description = "Run a transfer and commit the diff into this session's private overlay only. Never broadcasts to the real chain.")]
    async fn advance(&self, Parameters(args): Parameters<TransferArgs>) -> Result<CallToolResult, ErrorData> {
        run_transfer(&self.manager, args, true).await
    }

    #[tool(description = "Save a session's current state to disk and return a short snapshot_id. The session stays live. Pass the id to `resume` — later, after a restart, or from another agent — to reopen that exact state as a new, independent session, without replaying the transactions that built it.")]
    async fn snapshot(&self, Parameters(args): Parameters<SessionArgs>) -> Result<CallToolResult, ErrorData> {
        let info = self.manager.snapshot(args.session_id).await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "{{\"snapshot_id\":\"{}\",\"block_number\":{},\"accounts\":{},\"storage_slots\":{},\"contracts\":{},\"bytes\":{}}}",
            info.id, info.block_number, info.accounts, info.storage_slots, info.contracts, info.bytes
        ))]))
    }

    #[tool(description = "Open a new session from a snapshot_id returned by `snapshot`, at the block the snapshot was taken. Returns the new session_id. Resuming one id twice gives two independent sessions.")]
    async fn resume(&self, Parameters(args): Parameters<ResumeArgs>) -> Result<CallToolResult, ErrorData> {
        let id = self.manager.resume(&args.snapshot_id).await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(id.to_string())]))
    }

    #[tool(description = "Discard a session ahead of its TTL. Not required — an idle session expires on its own — but available once a caller knows it's done.")]
    async fn discard(&self, Parameters(args): Parameters<SessionArgs>) -> Result<CallToolResult, ErrorData> {
        self.manager.discard(args.session_id).await.map_err(tool_err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text("true")]))
    }
}

async fn run_transfer<F: Fallback>(
    manager: &SessionManager<F>,
    args: TransferArgs,
    commit: bool,
) -> Result<CallToolResult, ErrorData>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let from = parse_address(&args.from)?;
    // No `to` is a deploy: the EVM runs `data` as init code and the diff
    // lands at a freshly derived address.
    let kind = match &args.to {
        Some(to) => TxKind::Call(parse_address(to)?),
        None => TxKind::Create,
    };
    let value = parse_u256_hex(&args.value)?;
    let data = match &args.data {
        Some(hex) => parse_bytes_hex(hex)?,
        None => Bytes::new(),
    };
    // An omitted gas_price is priced at *this session's* basefee — the same
    // value the fee check compares against (a session's block env is fixed
    // at fork, so this cannot go stale), never the manager's default, which
    // diverges for a pinned session.
    let gas_price = match args.gas_price {
        Some(price) => price,
        None => basefee_of(manager, args.session_id).await?,
    };
    let tx = TxEnv::builder()
        .caller(from)
        .kind(kind)
        .value(value)
        .gas_limit(args.gas_limit)
        .gas_price(gas_price as u128)
        .nonce(args.nonce)
        .data(data)
        .build_fill();

    let outcome = if commit {
        manager.advance(args.session_id, tx).await
    } else {
        manager.simulate(args.session_id, tx).await
    };
    let result = match outcome {
        Ok(result) => result,
        Err(e) => {
            return Err(explain_fee_error(manager, args.session_id, e, from, gas_price, args.gas_limit).await)
        }
    };

    Ok(CallToolResult::success(vec![ContentBlock::text(render_result(&result))]))
}

async fn basefee_of<F: Fallback>(manager: &SessionManager<F>, id: SessionId) -> Result<u64, ErrorData>
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    manager.session_block_env(id).await.map(|env| env.basefee).map_err(tool_err)
}

/// Turns revm's two fee rejections into advice an agent can act on,
/// naming this surface's own tools. Matched on the typed rejection
/// `SessionError::InvalidTransaction` carries, so the numbers come from
/// the error itself rather than from parsing its text. Every other error
/// passes through untouched.
async fn explain_fee_error<F: Fallback>(
    manager: &SessionManager<F>,
    id: SessionId,
    error: SessionError,
    caller: Address,
    gas_price: u64,
    gas_limit: u64,
) -> ErrorData
where
    F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    let SessionError::InvalidTransaction(reason) = &error else {
        return tool_err(error);
    };
    match &**reason {
        InvalidTransaction::GasPriceLessThanBasefee => {
            let basefee = manager.session_block_env(id).await.map(|e| e.basefee).unwrap_or_default();
            ErrorData::invalid_params(
                format!(
                    "gas_price {gas_price} is below this fork's basefee of {basefee} - pass \
                     gas_price of at least {basefee}, or omit gas_price entirely to price at \
                     the basefee automatically"
                ),
                None,
            )
        }
        // The rejection carries the balance and the total, so there is
        // nothing to look up. `fee` is revm's `max_balance_spending`: the
        // value sent *plus* the gas, which is why the gas cost is computed
        // and labelled separately rather than presented as the total.
        InvalidTransaction::LackOfFundForMaxFee { fee, balance } => {
            let gas_cost = (gas_limit as u128) * (gas_price as u128);
            ErrorData::invalid_params(
                format!(
                    "sender {caller:#x} holds {balance} wei, but this transaction needs {fee} \
                     wei in total: gas_limit * gas_price = {gas_limit} * {gas_price} = \
                     {gas_cost} wei of gas, plus the value sent; fund it with set_balance, or \
                     lower gas_limit"
                ),
                None,
            )
        }
        _ => tool_err(error),
    }
}

/// The JSON one `simulate`/`advance` call reports back. `output` carries
/// the call's return data on success and the revert data on failure — a
/// read like `balanceOf` is answered entirely by that field, so omitting
/// it would leave calldata support useless.
fn render_result(result: &ExecutionResult) -> String {
    let output = result.output().map(|b| format!("0x{}", hex::encode(b))).unwrap_or_default();
    // Only a deploy has one, and an agent cannot use the contract it just
    // created without it.
    let created = result
        .created_address()
        .map(|a| format!(",\"contract_address\":\"{a:#x}\""))
        .unwrap_or_default();
    format!(
        "{{\"success\":{},\"gas_used\":{},\"output\":\"{}\"{}}}",
        result.is_success(),
        result.tx_gas_used(),
        output,
        created
    )
}

#[tool_handler]
impl<F: Fallback> ServerHandler for ForkyardMcpServer<F> where F::Error: fmt::Debug + fmt::Display + Send + Sync + 'static {}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::database_interface::{DBErrorMarker, DatabaseRef};
    use revm::primitives::B256;
    use revm::state::{AccountInfo, Bytecode};
    use rmcp::model::CallToolRequestParams;
    use rmcp::{ClientHandler, ServiceExt};
    use std::time::Duration;

    /// A minimal real contract, hand-assembled so the test needs no
    /// compiler: called with no calldata it reverts with the word `0xbb`;
    /// called with calldata it stores the first word in slot 0 and returns
    /// it. That makes three things observable that a plain ETH transfer
    /// cannot show — that calldata reached execution, that return data
    /// comes back, and that a revert carries its data.
    const COUNTER_RUNTIME: &str =
        "36600e5760bb60005260206000fd5b6000358060005560005260206000f3";
    /// The same contract's init code: `CODECOPY` the runtime out and return
    /// it, which is what a real deploy transaction carries.
    const COUNTER_INIT: &str =
        "601e600c600039601e6000f336600e5760bb60005260206000fd5b6000358060005560005260206000f3";
    /// Address the test fallback pretends `COUNTER_RUNTIME` is deployed at,
    /// for the cases that shouldn't have to deploy it first.
    const CONTRACT: Address = Address::new([0x11; 20]);

    fn unhex(s: &str) -> Vec<u8> {
        let s = s.strip_prefix("0x").unwrap_or(s);
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    fn counter_bytecode() -> Bytecode {
        Bytecode::new_raw(unhex(COUNTER_RUNTIME).into())
    }

    /// Every account exists with zero balance/nonce — enough to prove the
    /// MCP tool surface actually works end to end without live network —
    /// except `CONTRACT`, which carries `COUNTER_RUNTIME`.
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
        fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
            if address == CONTRACT {
                let code = counter_bytecode();
                return Ok(Some(AccountInfo {
                    code_hash: code.hash_slow(),
                    code: Some(code),
                    ..Default::default()
                }));
            }
            Ok(Some(AccountInfo::default()))
        }
        fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
            let code = counter_bytecode();
            if code_hash == code.hash_slow() {
                return Ok(code);
            }
            Ok(Bytecode::default())
        }
        fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
            Ok(U256::ZERO)
        }
        fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
            Ok(B256::ZERO)
        }
    }

    #[derive(Clone, Default)]
    struct NullClient;
    impl ClientHandler for NullClient {}

    fn call(name: &'static str, args: serde_json::Value) -> CallToolRequestParams {
        let object = match args {
            serde_json::Value::Object(map) => map,
            _ => Default::default(),
        };
        CallToolRequestParams::new(name).with_arguments(object)
    }

    fn text_of(result: &CallToolResult) -> &str {
        result.content[0].as_text().expect("tool result should be text").text.as_str()
    }

    #[tokio::test]
    async fn lists_expected_tools_and_round_trips_fork_set_balance_and_advance() {
        let manager = Arc::new(SessionManager::new(TestFallback, revm::context::BlockEnv::default(), 1, Duration::from_secs(60)));
        let server = ForkyardMcpServer::new(manager);

        let (server_io, client_io) = tokio::io::duplex(4096);
        let server_task = tokio::spawn(async move {
            server.serve(server_io).await?.waiting().await?;
            eyre::Result::<()>::Ok(())
        });

        let client = NullClient.serve(client_io).await.expect("client should connect");

        let tools = client.list_tools(None).await.expect("tools/list should succeed");
        let names: Vec<&str> = tools.tools.iter().map(|t| t.name.as_ref()).collect();
        for expected in ["fork", "get_balance", "set_balance", "set_storage", "simulate", "advance", "discard", "snapshot", "resume"] {
            assert!(names.contains(&expected), "missing tool {expected:?}, got {names:?}");
        }

        // fork -> set_balance -> advance -> get_balance, entirely over the
        // MCP protocol, not by calling the struct's methods directly.
        let fork_result = client.call_tool(call("fork", serde_json::json!({}))).await.expect("fork");
        let session_id: u64 = text_of(&fork_result).parse().unwrap();

        let sender = Address::from([7u8; 20]);
        let recipient = Address::from([8u8; 20]);
        client
            .call_tool(call(
                "set_balance",
                serde_json::json!({ "session_id": session_id, "address": sender.to_string(), "balance": "0x64" }),
            ))
            .await
            .expect("set_balance");

        let advance_result = client
            .call_tool(call(
                "advance",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": recipient.to_string(),
                    "value": "0x64",
                    "gas_price": 0,
                }),
            ))
            .await
            .expect("advance");
        assert!(text_of(&advance_result).contains("\"success\":true"));

        let balance_result = client
            .call_tool(call(
                "get_balance",
                serde_json::json!({ "session_id": session_id, "address": recipient.to_string() }),
            ))
            .await
            .expect("get_balance");
        assert!(text_of(&balance_result).contains("\"balance\":\"0x64\""));

        client.cancel().await.expect("client should cancel");
        server_task.await.expect("server task").expect("server");
    }

    #[tokio::test]
    async fn same_tool_surface_round_trips_over_streamable_http() {
        use rmcp::transport::StreamableHttpClientTransport;

        let manager = Arc::new(SessionManager::new(TestFallback, revm::context::BlockEnv::default(), 1, Duration::from_secs(60)));
        let handle = ForkyardMcpServer::serve_http(manager, "127.0.0.1:0").await.expect("serve_http should bind");
        let url = format!("http://{}/mcp", handle.addr);

        let client = NullClient
            .serve(StreamableHttpClientTransport::from_uri(url))
            .await
            .expect("client should connect over HTTP");

        let tools = client.list_tools(None).await.expect("tools/list should succeed");
        let names: Vec<&str> = tools.tools.iter().map(|t| t.name.as_ref()).collect();
        for expected in ["fork", "get_balance", "set_balance", "set_storage", "simulate", "advance", "discard", "snapshot", "resume"] {
            assert!(names.contains(&expected), "missing tool {expected:?}, got {names:?}");
        }

        // Same fork -> set_balance -> advance -> get_balance round trip as
        // the stdio test, this time over a real TCP connection — proves
        // the HTTP transport isn't just reachable, it drives the exact same
        // SessionManager correctly.
        let fork_result = client.call_tool(call("fork", serde_json::json!({}))).await.expect("fork");
        let session_id: u64 = text_of(&fork_result).parse().unwrap();

        let sender = Address::from([9u8; 20]);
        let recipient = Address::from([10u8; 20]);
        client
            .call_tool(call(
                "set_balance",
                serde_json::json!({ "session_id": session_id, "address": sender.to_string(), "balance": "0x64" }),
            ))
            .await
            .expect("set_balance");

        let advance_result = client
            .call_tool(call(
                "advance",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": recipient.to_string(),
                    "value": "0x64",
                    "gas_price": 0,
                }),
            ))
            .await
            .expect("advance");
        assert!(text_of(&advance_result).contains("\"success\":true"));

        let balance_result = client
            .call_tool(call(
                "get_balance",
                serde_json::json!({ "session_id": session_id, "address": recipient.to_string() }),
            ))
            .await
            .expect("get_balance");
        assert!(text_of(&balance_result).contains("\"balance\":\"0x64\""));

        client.cancel().await.expect("client should cancel");
        handle.shutdown().await;
    }

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

    /// Shared setup: a live MCP client over stdio plus a funded sender in a
    /// fresh session — every contract test below starts here.
    async fn client_with_funded_session() -> (
        rmcp::service::RunningService<rmcp::service::RoleClient, NullClient>,
        tokio::task::JoinHandle<eyre::Result<()>>,
        u64,
        Address,
    ) {
        client_with_basefee(0, 10u128.pow(18)).await
    }

    /// Same, but on a fork whose basefee is `basefee` — a live chain's
    /// shape rather than a zero-fee fixture — with the sender holding
    /// `balance` wei.
    async fn client_with_basefee(
        basefee: u64,
        balance: u128,
    ) -> (
        rmcp::service::RunningService<rmcp::service::RoleClient, NullClient>,
        tokio::task::JoinHandle<eyre::Result<()>>,
        u64,
        Address,
    ) {
        let manager = Arc::new(SessionManager::new(
            TestFallback,
            revm::context::BlockEnv { basefee, ..Default::default() },
            1,
            Duration::from_secs(60),
        ));
        let server = ForkyardMcpServer::new(manager);
        let (server_io, client_io) = tokio::io::duplex(1 << 16);
        let server_task = tokio::spawn(async move {
            server.serve(server_io).await?.waiting().await?;
            eyre::Result::<()>::Ok(())
        });
        let client = NullClient.serve(client_io).await.expect("client should connect");
        let fork = client.call_tool(call("fork", serde_json::json!({}))).await.expect("fork");
        let session_id: u64 = text_of(&fork).parse().unwrap();
        let sender = Address::from([7u8; 20]);
        client
            .call_tool(call(
                "set_balance",
                serde_json::json!({
                    "session_id": session_id,
                    "address": sender.to_string(),
                    "balance": format!("0x{balance:x}"),
                }),
            ))
            .await
            .expect("set_balance");
        (client, server_task, session_id, sender)
    }

    /// A 32-byte word, the calldata the counter contract expects.
    fn word(n: u64) -> String {
        format!("0x{n:064x}")
    }

    #[tokio::test]
    async fn simulate_delivers_calldata_to_the_contract() {
        let (client, task, session_id, sender) = client_with_funded_session().await;
        let args = serde_json::json!({
            "session_id": session_id,
            "from": sender.to_string(),
            "to": CONTRACT.to_string(),
            "value": "0x0",
            "gas_limit": 200_000,
            "gas_price": 0,
        });

        // Control: the contract reverts when it receives no calldata, so a
        // passing assertion below can only mean the bytes got through.
        let without = client.call_tool(call("simulate", args.clone())).await.expect("simulate");
        assert!(
            text_of(&without).contains("\"success\":false"),
            "no calldata should revert, got {}",
            text_of(&without)
        );

        let mut with_data = args.as_object().unwrap().clone();
        with_data.insert("data".into(), serde_json::json!(word(42)));
        let with = client
            .call_tool(call("simulate", serde_json::Value::Object(with_data)))
            .await
            .expect("simulate");

        assert!(
            text_of(&with).contains("\"success\":true"),
            "calldata should reach the contract, got {}",
            text_of(&with)
        );

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    #[tokio::test]
    async fn simulate_returns_the_contracts_return_data() {
        let (client, task, session_id, sender) = client_with_funded_session().await;

        let result = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": CONTRACT.to_string(),
                    "value": "0x0",
                    "data": word(42),
                    "gas_limit": 200_000,
                    "gas_price": 0,
                }),
            ))
            .await
            .expect("simulate");

        // The contract echoes its input word back, so 42 must appear in the
        // returned output — without it a read like `balanceOf` is unusable.
        assert!(
            text_of(&result).contains(&format!("\"output\":\"{}\"", word(42))),
            "expected the returned word in output, got {}",
            text_of(&result)
        );

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    #[tokio::test]
    async fn a_reverting_call_surfaces_its_revert_data() {
        let (client, task, session_id, sender) = client_with_funded_session().await;

        // No calldata: the contract reverts with the word 0xbb.
        let result = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": CONTRACT.to_string(),
                    "value": "0x0",
                    "gas_limit": 200_000,
                    "gas_price": 0,
                }),
            ))
            .await
            .expect("simulate");

        let text = text_of(&result);
        assert!(text.contains("\"success\":false"), "should have reverted, got {text}");
        assert!(
            text.contains(&format!("\"output\":\"{}\"", word(0xbb))),
            "revert data should be surfaced, got {text}"
        );

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// The bug behind "there's no data argument": an unknown field used to
    /// be dropped in silence, so a caller passing `data` got back a result
    /// identical to a bare transfer and no hint anything was ignored.
    #[tokio::test]
    async fn an_unknown_argument_is_rejected_rather_than_silently_dropped() {
        let (client, task, session_id, sender) = client_with_funded_session().await;

        let result = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": CONTRACT.to_string(),
                    "value": "0x0",
                    "gas_limit": 200_000,
                    "gas_price": 0,
                    "dtaa": word(42),
                }),
            ))
            .await
            .expect("the call itself should complete");

        // rmcp reports a schema rejection as a tool-level error, not a
        // transport error — what matters is that it is flagged as a failure
        // and names the offending field, instead of coming back as a
        // plausible-looking `{"success":false}` transfer result.
        assert_eq!(result.is_error, Some(true), "should be flagged an error: {result:?}");
        assert!(
            text_of(&result).contains("unknown field `dtaa`"),
            "should name the bad field, got {}",
            text_of(&result)
        );

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    #[tokio::test]
    async fn get_storage_reads_a_slot_a_contract_call_wrote() {
        let (client, task, session_id, sender) = client_with_funded_session().await;

        client
            .call_tool(call(
                "advance",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": CONTRACT.to_string(),
                    "value": "0x0",
                    "data": word(42),
                    "gas_limit": 200_000,
                    "gas_price": 0,
                }),
            ))
            .await
            .expect("advance");

        let result = client
            .call_tool(call(
                "get_storage",
                serde_json::json!({
                    "session_id": session_id,
                    "address": CONTRACT.to_string(),
                    "slot": "0x0",
                }),
            ))
            .await
            .expect("get_storage");

        assert_eq!(text_of(&result), word(42));

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// The read-back half of `set_storage`: a slot forced into the overlay
    /// must be visible without running a transaction at all.
    #[tokio::test]
    async fn get_storage_reads_back_what_set_storage_wrote() {
        let (client, task, session_id, _sender) = client_with_funded_session().await;
        let address = Address::from([0x55; 20]);

        client
            .call_tool(call(
                "set_storage",
                serde_json::json!({
                    "session_id": session_id,
                    "address": address.to_string(),
                    "slot": "0x9",
                    "value": word(7),
                }),
            ))
            .await
            .expect("set_storage");

        let result = client
            .call_tool(call(
                "get_storage",
                serde_json::json!({
                    "session_id": session_id,
                    "address": address.to_string(),
                    "slot": "0x9",
                }),
            ))
            .await
            .expect("get_storage");

        assert_eq!(text_of(&result), word(7));

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    #[tokio::test]
    async fn get_code_returns_the_deployed_bytecode() {
        let (client, task, session_id, _sender) = client_with_funded_session().await;

        let result = client
            .call_tool(call(
                "get_code",
                serde_json::json!({ "session_id": session_id, "address": CONTRACT.to_string() }),
            ))
            .await
            .expect("get_code");

        assert_eq!(text_of(&result), format!("0x{COUNTER_RUNTIME}"));

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    #[tokio::test]
    async fn get_code_returns_empty_for_an_account_with_no_code() {
        let (client, task, session_id, sender) = client_with_funded_session().await;

        let result = client
            .call_tool(call(
                "get_code",
                serde_json::json!({ "session_id": session_id, "address": sender.to_string() }),
            ))
            .await
            .expect("get_code");

        assert_eq!(text_of(&result), "0x");

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// Omitting `to` means "deploy": the init code in `data` runs and the
    /// new contract's address comes back, so an agent can put a throwaway
    /// test contract on its fork without leaving MCP.
    #[tokio::test]
    async fn advance_without_a_to_deploys_the_contract_in_data() {
        let (client, task, session_id, sender) = client_with_funded_session().await;

        let result = client
            .call_tool(call(
                "advance",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "value": "0x0",
                    "data": format!("0x{COUNTER_INIT}"),
                    "gas_limit": 500_000,
                    "gas_price": 0,
                }),
            ))
            .await
            .expect("advance");

        let text = text_of(&result);
        assert!(text.contains("\"success\":true"), "deploy should succeed, got {text}");
        let parsed: serde_json::Value = serde_json::from_str(text).expect("result should be json");
        let address = parsed["contract_address"].as_str().expect("deploy should report an address");

        // Proof it is really deployed: its code is readable at that address.
        let code = client
            .call_tool(call(
                "get_code",
                serde_json::json!({ "session_id": session_id, "address": address }),
            ))
            .await
            .expect("get_code");
        assert_eq!(text_of(&code), format!("0x{COUNTER_RUNTIME}"));

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// The flow the MCP surface exists to serve, end to end over the
    /// protocol with nothing pre-seeded: deploy a contract, call it with
    /// real calldata, read the value it returned, read the slot it wrote,
    /// and confirm `simulate` left that slot alone. Before calldata
    /// support this was not expressible at all — `simulate` could only
    /// move ETH — so this is the regression test for the whole gap.
    #[tokio::test]
    async fn deploy_call_and_read_a_contract_end_to_end_over_mcp() {
        let (client, task, session_id, sender) = client_with_funded_session().await;

        // 1. Deploy: no `to`, init code in `data`.
        let deployed = client
            .call_tool(call(
                "advance",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "value": "0x0",
                    "data": format!("0x{COUNTER_INIT}"),
                    "gas_limit": 500_000,
                    "gas_price": 0,
                    "nonce": 0,
                }),
            ))
            .await
            .expect("deploy");
        let deployed: serde_json::Value = serde_json::from_str(text_of(&deployed)).unwrap();
        assert_eq!(deployed["success"], true, "deploy failed: {deployed}");
        let contract = deployed["contract_address"].as_str().expect("address").to_string();

        // 2. Its code is really there.
        let code = client
            .call_tool(call(
                "get_code",
                serde_json::json!({ "session_id": session_id, "address": contract }),
            ))
            .await
            .expect("get_code");
        assert_eq!(text_of(&code), format!("0x{COUNTER_RUNTIME}"), "deployed code mismatch");

        // 3. Call it with calldata and commit. The contract stores the word
        //    it is given in slot 0 and returns it.
        let called = client
            .call_tool(call(
                "advance",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": contract,
                    "value": "0x0",
                    "data": word(42),
                    "gas_limit": 200_000,
                    "gas_price": 0,
                    "nonce": 1, // the deploy above consumed nonce 0
                }),
            ))
            .await
            .expect("call");
        let called: serde_json::Value = serde_json::from_str(text_of(&called)).unwrap();
        assert_eq!(called["success"], true, "call failed: {called}");
        assert_eq!(called["output"], word(42), "return data should echo the input word");

        // 4. The write it performed is readable as raw storage.
        let slot = client
            .call_tool(call(
                "get_storage",
                serde_json::json!({ "session_id": session_id, "address": contract, "slot": "0x0" }),
            ))
            .await
            .expect("get_storage");
        assert_eq!(text_of(&slot), word(42), "the committed write should be visible");

        // 5. `simulate` of a *different* value returns that value but must
        //    not persist it — the read-only guarantee, now observable on
        //    contract state rather than only on balances.
        let simulated = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": contract,
                    "value": "0x0",
                    "data": word(99),
                    "gas_limit": 200_000,
                    "gas_price": 0,
                    "nonce": 2,
                }),
            ))
            .await
            .expect("simulate");
        let simulated: serde_json::Value = serde_json::from_str(text_of(&simulated)).unwrap();
        assert_eq!(simulated["success"], true, "simulate failed: {simulated}");
        assert_eq!(simulated["output"], word(99), "simulate should return the new value");

        let slot_after = client
            .call_tool(call(
                "get_storage",
                serde_json::json!({ "session_id": session_id, "address": contract, "slot": "0x0" }),
            ))
            .await
            .expect("get_storage");
        assert_eq!(text_of(&slot_after), word(42), "simulate must not have committed");

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// The footgun: `gas_price` defaulted to `0`, which is below the
    /// basefee of every live chain, so an agent that omitted it got
    /// `GasPriceLessThanBasefee` every time. Omitting it now means "price
    /// this at the fork's own basefee".
    #[tokio::test]
    async fn omitting_gas_price_uses_the_forks_basefee() {
        let basefee = 1_000_000_000; // 1 gwei
        let (client, task, session_id, sender) = client_with_basefee(basefee, 10u128.pow(18)).await;

        let result = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": CONTRACT.to_string(),
                    "value": "0x0",
                    "data": word(42),
                    "gas_limit": 200_000,
                    // no gas_price
                }),
            ))
            .await
            .expect("simulate");

        assert!(
            text_of(&result).contains("\"success\":true"),
            "omitted gas_price should price at the basefee, got {}",
            text_of(&result)
        );

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// The escape hatch stays open: an explicit `0` still means literally
    /// zero, so a zero-basefee fork keeps costing nothing and "would this
    /// underpriced transaction fail?" stays askable. If `0` were treated
    /// as "auto", both would be lost.
    #[tokio::test]
    async fn an_explicit_zero_gas_price_still_means_zero() {
        let (client, task, session_id, sender) = client_with_funded_session().await; // basefee 0

        let result = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": CONTRACT.to_string(),
                    "value": "0x0",
                    "data": word(42),
                    "gas_limit": 200_000,
                    "gas_price": 0,
                }),
            ))
            .await
            .expect("simulate");

        let text = text_of(&result);
        assert!(text.contains("\"success\":true"), "explicit zero should execute, got {text}");
        // Priced at zero, so the sender pays no gas at all.
        let balance = client
            .call_tool(call(
                "get_balance",
                serde_json::json!({ "session_id": session_id, "address": sender.to_string() }),
            ))
            .await
            .expect("get_balance");
        assert!(
            text_of(&balance).contains(&format!("0x{:x}", 10u128.pow(18))),
            "a zero gas price must not debit the sender, got {}",
            text_of(&balance)
        );

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// An explicitly underpriced transaction is still rejected — that is
    /// `simulate`'s job — but the error now names the basefee and the way
    /// out, instead of the bare `GasPriceLessThanBasefee` an agent cannot
    /// act on.
    #[tokio::test]
    async fn an_underpriced_gas_price_names_the_basefee_and_the_fix() {
        let basefee = 1_000_000_000;
        let (client, task, session_id, sender) = client_with_basefee(basefee, 10u128.pow(18)).await;

        let result = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": CONTRACT.to_string(),
                    "value": "0x0",
                    "data": word(42),
                    "gas_limit": 200_000,
                    "gas_price": 1,
                }),
            ))
            .await
            .expect_err("an underpriced transaction must be rejected");

        let text = result.to_string();
        assert!(text.contains(&basefee.to_string()), "should name the basefee, got {text}");
        assert!(text.contains("omit"), "should say omitting it works, got {text}");

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// The failure that replaces the basefee one once pricing works: a
    /// sender who cannot cover `gas_limit * gas_price`. The message has to
    /// say so in those terms, or it reads as another mystery rejection.
    #[tokio::test]
    async fn an_underfunded_sender_gets_an_error_naming_the_gas_cost() {
        let basefee = 1_000_000_000;
        let (client, task, session_id, sender) = client_with_basefee(basefee, 1_000).await;

        let result = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": CONTRACT.to_string(),
                    "value": "0x0",
                    "data": word(42),
                    "gas_limit": 200_000,
                }),
            ))
            .await
            .expect_err("an underfunded sender must be rejected");

        let text = result.to_string();
        assert!(
            text.contains("gas_limit") && text.contains("set_balance"),
            "should explain the gas cost and how to fund it, got {text}"
        );
        assert!(text.contains("1000"), "should name the balance the sender has, got {text}");
        // Stated in the caller's terms, not revm's: a message carrying an
        // internal enum name alongside the explanation reads as two errors
        // glued together.
        assert!(
            !text.contains("LackOfFundForMaxFee"),
            "should not leak revm's internal variant name, got {text}"
        );

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    /// revm's `fee` is the *total* it needs — the value sent plus the gas
    /// — so a message that prints it as `gas_limit * gas_price` is simply
    /// wrong once any value is attached. Both numbers have to appear, each
    /// labelled as what it is.
    #[tokio::test]
    async fn the_funds_error_separates_the_gas_cost_from_the_value_sent() {
        let basefee = 1_000_000_000u64;
        let gas_limit = 21_000u64;
        let (client, task, session_id, sender) = client_with_basefee(basefee, 1_000).await;

        let result = client
            .call_tool(call(
                "simulate",
                serde_json::json!({
                    "session_id": session_id,
                    "from": sender.to_string(),
                    "to": Address::from([0x31; 20]).to_string(),
                    "value": "0x64", // 100 wei, so total != gas cost
                    "gas_limit": gas_limit,
                }),
            ))
            .await
            .expect_err("an unaffordable transaction must be rejected");

        let text = result.to_string();
        let gas_cost = (gas_limit as u128) * (basefee as u128);
        assert!(text.contains(&gas_cost.to_string()), "should state the gas cost, got {text}");
        assert!(
            text.contains(&(gas_cost + 100).to_string()),
            "should state the true total, which includes the value sent, got {text}"
        );

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
    }

    #[tokio::test]
    async fn snapshot_and_resume_round_trip_a_session_over_mcp() {
        let dir = std::env::temp_dir().join(format!("forkyard-mcp-snapshots-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let manager = Arc::new(
            SessionManager::new(TestFallback, revm::context::BlockEnv::default(), 1, Duration::from_secs(60))
                .with_snapshots(forkyard_engine::persist::SnapshotStore::new(&dir, 1)),
        );
        let server = ForkyardMcpServer::new(manager);
        let (server_io, client_io) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            server.serve(server_io).await?.waiting().await?;
            eyre::Result::<()>::Ok(())
        });
        let client = NullClient.serve(client_io).await.expect("client should connect");

        let session_id: u64 =
            text_of(&client.call_tool(call("fork", serde_json::json!({}))).await.expect("fork")).parse().unwrap();
        let funded = Address::from([0x66; 20]);
        client
            .call_tool(call(
                "set_balance",
                serde_json::json!({ "session_id": session_id, "address": funded.to_string(), "balance": "0x2a" }),
            ))
            .await
            .expect("set_balance");

        let snapshot = client
            .call_tool(call("snapshot", serde_json::json!({ "session_id": session_id })))
            .await
            .expect("snapshot");
        let info: serde_json::Value = serde_json::from_str(text_of(&snapshot)).unwrap();
        let snapshot_id = info["snapshot_id"].as_str().unwrap().to_string();

        // The original is gone; the snapshot is all that's left of it.
        client.call_tool(call("discard", serde_json::json!({ "session_id": session_id }))).await.expect("discard");

        let resumed: u64 = text_of(
            &client.call_tool(call("resume", serde_json::json!({ "snapshot_id": snapshot_id }))).await.expect("resume"),
        )
        .parse()
        .unwrap();
        let balance = client
            .call_tool(call(
                "get_balance",
                serde_json::json!({ "session_id": resumed, "address": funded.to_string() }),
            ))
            .await
            .expect("get_balance");
        assert!(text_of(&balance).contains("\"balance\":\"0x2a\""), "{}", text_of(&balance));

        let bad = client.call_tool(call("resume", serde_json::json!({ "snapshot_id": "../../etc/passwd" }))).await;
        assert!(bad.is_err(), "a malformed id must be refused, not resolved to a path");

        client.cancel().await.expect("cancel");
        task.await.expect("server task").expect("server");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
