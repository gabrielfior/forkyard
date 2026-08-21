//! The production surface: an MCP server exposing `fork` / `simulate` /
//! `advance` / `discard` (plus the same `set_balance` test cheatcode
//! `forkyard-api-http` has) as tools, backed directly by a
//! `forkyard-session::SessionManager`. This is what an agent framework
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

use forkyard_session::{Fallback, SessionId, SessionManager};
use revm::context::TxEnv;
use revm::primitives::{Address, TxKind, U256};
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

fn parse_u256_hex(s: &str) -> Result<U256, ErrorData> {
    U256::from_str_radix(s.trim_start_matches("0x"), 16)
        .map_err(|e| ErrorData::invalid_params(format!("bad hex value: {e}"), None))
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SessionArgs {
    /// Session id returned by `fork`.
    session_id: SessionId,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct BalanceArgs {
    session_id: SessionId,
    /// Account address, `0x`-prefixed hex.
    address: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct SetBalanceArgs {
    session_id: SessionId,
    address: String,
    /// New balance in wei, `0x`-prefixed hex.
    balance: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct TransferArgs {
    session_id: SessionId,
    /// Sender address — must already be funded in this session's overlay
    /// (see `set_balance`) or hold real forked balance.
    from: String,
    to: String,
    /// Value in wei, `0x`-prefixed hex.
    value: String,
    #[serde(default = "default_gas_limit")]
    gas_limit: u64,
    #[serde(default)]
    gas_price: u64,
    #[serde(default)]
    nonce: u64,
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

    #[tool(description = "Run a transfer read-only against a session — no commit, nothing persists. Use to preview a transaction's effect.")]
    async fn simulate(&self, Parameters(args): Parameters<TransferArgs>) -> Result<CallToolResult, ErrorData> {
        run_transfer(&self.manager, args, false).await
    }

    #[tool(description = "Run a transfer and commit the diff into this session's private overlay only. Never broadcasts to the real chain.")]
    async fn advance(&self, Parameters(args): Parameters<TransferArgs>) -> Result<CallToolResult, ErrorData> {
        run_transfer(&self.manager, args, true).await
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
    let to = parse_address(&args.to)?;
    let value = parse_u256_hex(&args.value)?;
    let tx = TxEnv::builder()
        .caller(from)
        .kind(TxKind::Call(to))
        .value(value)
        .gas_limit(args.gas_limit)
        .gas_price(args.gas_price as u128)
        .nonce(args.nonce)
        .build_fill();

    let result = if commit {
        manager.advance(args.session_id, tx).await.map_err(tool_err)?
    } else {
        manager.simulate(args.session_id, tx).await.map_err(tool_err)?
    };

    Ok(CallToolResult::success(vec![ContentBlock::text(format!(
        "{{\"success\":{},\"gas_used\":{}}}",
        result.is_success(),
        result.tx_gas_used()
    ))]))
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

    /// Every account exists with zero balance/nonce — enough to prove the
    /// MCP tool surface actually works end to end without live network.
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
        for expected in ["fork", "get_balance", "set_balance", "simulate", "advance", "discard"] {
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
        for expected in ["fork", "get_balance", "set_balance", "simulate", "advance", "discard"] {
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
}
