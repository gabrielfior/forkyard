//! The standard agent workload of `python/benchmarks/run_benchmark.py`,
//! ported action for action, against forkyard and Anvil alike.
//!
//! Why a second harness: one Python process driving 50 agents' signing and
//! JSON-RPC encoding saturates a core — measured, the client was busy 1.6 s
//! of a 1.8 s forkyard run — so past that point the Python numbers time the
//! harness. This one is async Rust on every core, and costs microseconds a
//! request.
//!
//! What is kept identical, so rows compare with the Python harness's:
//! - the action mix, drawn from a port of CPython's Mersenne Twister seeded
//!   with the agent id, so agent 7 makes the same choices in both;
//! - each action's RPC calls, gas limits, amounts and calldata;
//! - the timed regions: acquire (forkyard: `POST /session`; Anvil: spawn and
//!   poll until it answers) → actions → discard, with forkyard's one-time
//!   process start excluded, and every state-changing action waited to its
//!   receipt via `eth_sendRawTransactionSync` (EIP-7966, both backends);
//! - the CSV columns, including the `__total__` row `aggregate_runs.py` reads.
//!
//! One deliberate difference: Anvil's readiness is polled every 10 ms, not
//! every 200 ms, which only ever makes Anvil's acquire look faster.

use std::fmt::Write as _;
use std::io::Write as _;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{address, hex, keccak256, Address, Bytes, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;
use alloy_sol_types::{sol, SolCall};
use serde_json::{json, Value};

const UNISWAP_V2_ROUTER: Address = address!("7a250d5630B4cF539739dF2C5dAcb4c659F2488D");
const WETH: Address = address!("C02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2");
/// `actions.TOKENS`: DAI, its `balanceOf` mapping at slot 2.
const TOKENS: [(Address, u64); 1] = [(address!("6B175474E89094C44Da98b954EedeAC495271d0F"), 2)];
const ONE_ETH: u128 = 1_000_000_000_000_000_000;
const FAR_FUTURE_DEADLINE: u64 = 9_999_999_999;
const MAX_ERROR_CHARS: usize = 200;

sol! {
    function approve(address spender, uint256 amount) returns (bool);
    function swapExactETHForTokens(uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[] amounts);
    function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline) returns (uint256[] amounts);
}

/// CPython's `random.Random`: MT19937 seeded by `init_by_array`, and
/// `choice` via `_randbelow_with_getrandbits`, rejection loop included —
/// only exact, not merely equivalent, keeps the two harnesses' agents on
/// the same action sequence.
struct PyRandom {
    mt: [u32; 624],
    index: usize,
}

impl PyRandom {
    fn new(seed: u64) -> Self {
        let mut rng = Self { mt: [0; 624], index: 624 };
        rng.init_genrand(19_650_218);
        let key: Vec<u32> = if seed == 0 {
            vec![0]
        } else {
            let mut words = Vec::new();
            let mut n = seed;
            while n > 0 {
                words.push(n as u32);
                n >>= 32;
            }
            words
        };
        let (mut i, mut j) = (1usize, 0usize);
        for _ in 0..624.max(key.len()) {
            let prev = rng.mt[i - 1] ^ (rng.mt[i - 1] >> 30);
            rng.mt[i] = (rng.mt[i] ^ prev.wrapping_mul(1_664_525)).wrapping_add(key[j]).wrapping_add(j as u32);
            i += 1;
            j += 1;
            if i >= 624 {
                rng.mt[0] = rng.mt[623];
                i = 1;
            }
            if j >= key.len() {
                j = 0;
            }
        }
        for _ in 0..623 {
            let prev = rng.mt[i - 1] ^ (rng.mt[i - 1] >> 30);
            rng.mt[i] = (rng.mt[i] ^ prev.wrapping_mul(1_566_083_941)).wrapping_sub(i as u32);
            i += 1;
            if i >= 624 {
                rng.mt[0] = rng.mt[623];
                i = 1;
            }
        }
        rng.mt[0] = 0x8000_0000;
        rng
    }

    fn init_genrand(&mut self, seed: u32) {
        self.mt[0] = seed;
        for i in 1..624 {
            let prev = self.mt[i - 1] ^ (self.mt[i - 1] >> 30);
            self.mt[i] = 1_812_433_253u32.wrapping_mul(prev).wrapping_add(i as u32);
        }
        self.index = 624;
    }

    fn next_u32(&mut self) -> u32 {
        if self.index >= 624 {
            for i in 0..624 {
                let y = (self.mt[i] & 0x8000_0000) | (self.mt[(i + 1) % 624] & 0x7fff_ffff);
                let mut next = self.mt[(i + 397) % 624] ^ (y >> 1);
                if y & 1 != 0 {
                    next ^= 0x9908_b0df;
                }
                self.mt[i] = next;
            }
            self.index = 0;
        }
        let mut y = self.mt[self.index];
        self.index += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^ (y >> 18)
    }

    fn randbelow(&mut self, n: usize) -> usize {
        let k = usize::BITS - n.leading_zeros();
        loop {
            let r = (self.next_u32() >> (32 - k)) as usize;
            if r < n {
                return r;
            }
        }
    }

    fn choice<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.randbelow(items.len())]
    }
}

/// One row of the output CSV — `agent.ActionRecord`'s columns.
struct Record {
    backend: &'static str,
    block_height: u64,
    num_agents: usize,
    agent_id: i64,
    action: &'static str,
    elapsed_ms: f64,
    ok: bool,
    error: String,
}

fn truncate(error: impl std::fmt::Display) -> String {
    error.to_string().chars().take(MAX_ERROR_CHARS).collect()
}

#[derive(Clone)]
struct Rpc {
    http: reqwest::Client,
}

impl Rpc {
    async fn call(&self, url: &str, method: &str, params: Value) -> Result<Value, String> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let response: Value = self
            .http
            .post(url)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{method}: {e}"))?
            .json()
            .await
            .map_err(|e| format!("{method}: {e}"))?;
        if let Some(error) = response.get("error") {
            return Err(format!("{method}: {error}"));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }
}

fn hex_u256(value: &Value) -> Result<U256, String> {
    let s = value.as_str().ok_or_else(|| format!("expected a hex string, got {value}"))?;
    U256::from_str_radix(s.trim_start_matches("0x"), 16).map_err(|e| e.to_string())
}

/// One agent's environment: a forkyard session, or an Anvil process.
enum Backend {
    Forkyard { url: String },
    Anvil { url: String, child: tokio::process::Child },
}

impl Backend {
    fn url(&self) -> &str {
        match self {
            Backend::Forkyard { url } | Backend::Anvil { url, .. } => url,
        }
    }

    fn cheat(&self, forkyard: &'static str, anvil: &'static str) -> &'static str {
        match self {
            Backend::Forkyard { .. } => forkyard,
            Backend::Anvil { .. } => anvil,
        }
    }

    async fn discard(self, rpc: &Rpc) -> Result<(), String> {
        match self {
            Backend::Forkyard { url } => rpc.call(&url, "forkyard_discard", json!([])).await.map(drop),
            // SIGTERM, not SIGKILL: Anvil writes its fork cache on the way
            // out, which is what keeps the next run warm — as the Python
            // harness's `terminate()` does.
            Backend::Anvil { mut child, .. } => {
                terminate(&child);
                match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
                    Ok(_) => Ok(()),
                    Err(_) => {
                        let _ = child.kill().await;
                        Ok(())
                    }
                }
            }
        }
    }
}

/// SIGTERM, which tokio's `Child` has no call for.
fn terminate(child: &tokio::process::Child) {
    if let Some(pid) = child.id() {
        // SAFETY: `kill` takes no pointers; a stale pid just fails.
        unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
    }
}

#[derive(Clone)]
enum Target {
    Forkyard { base_url: String },
    Anvil { rpc_url: String, block_height: u64, base_port: u16, episodes: usize },
}

async fn acquire(target: &Target, rpc: &Rpc, agent_id: usize, episode: usize) -> Result<Backend, String> {
    match target {
        Target::Forkyard { base_url } => {
            let response: Value = rpc
                .http
                .post(format!("{base_url}/session"))
                .send()
                .await
                .map_err(|e| e.to_string())?
                .json()
                .await
                .map_err(|e| e.to_string())?;
            let id = response["session_id"].as_u64().ok_or_else(|| format!("no session: {response}"))?;
            Ok(Backend::Forkyard { url: format!("{base_url}/session/{id}") })
        }
        Target::Anvil { rpc_url, block_height, base_port, episodes } => {
            let port = base_port + (agent_id * episodes + episode) as u16;
            let child = tokio::process::Command::new("anvil")
                .args(["--fork-url", rpc_url, "--fork-block-number", &block_height.to_string()])
                .args(["--port", &port.to_string(), "--silent"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true)
                .spawn()
                .map_err(|e| format!("spawning anvil: {e}"))?;
            let backend = Backend::Anvil { url: format!("http://127.0.0.1:{port}"), child };
            let deadline = Instant::now() + Duration::from_secs(60);
            while Instant::now() < deadline {
                if rpc.call(backend.url(), "eth_blockNumber", json!([])).await.is_ok() {
                    return Ok(backend);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(format!("anvil on port {port} was not ready within 60 s"))
        }
    }
}

/// One transaction's contents, less what `Sender` fills in.
struct Call {
    to: Address,
    value: U256,
    input: Bytes,
    gas_limit: u64,
}

/// What `actions._send_signed` does: chain id and gas price fetched once
/// per environment, a legacy EIP-155 transaction, sent and waited to its
/// receipt in one call.
struct Sender {
    signer: PrivateKeySigner,
    chain: Option<(u64, u128)>,
}

impl Sender {
    async fn send(&mut self, rpc: &Rpc, backend: &Backend, call: Call, nonce: u64) -> Result<(), String> {
        let (chain_id, gas_price) = match self.chain {
            Some(chain) => chain,
            None => {
                let chain_id = hex_u256(&rpc.call(backend.url(), "eth_chainId", json!([])).await?)?.to::<u64>();
                let gas_price = hex_u256(&rpc.call(backend.url(), "eth_gasPrice", json!([])).await?)?.to::<u128>();
                *self.chain.insert((chain_id, gas_price))
            }
        };
        let Call { to, value, input, gas_limit } = call;
        let mut tx = TxLegacy { chain_id: Some(chain_id), nonce, gas_price, gas_limit, to: TxKind::Call(to), value, input };
        let signature = self.signer.sign_transaction_sync(&mut tx).map_err(|e| e.to_string())?;
        let raw = tx.into_signed(signature).encoded_2718();
        let receipt = rpc
            .call(backend.url(), "eth_sendRawTransactionSync", json!([format!("0x{}", hex::encode(raw))]))
            .await?;
        if hex_u256(&receipt["status"])? != U256::from(1) {
            return Err(format!("transaction {} reverted", receipt["transactionHash"]));
        }
        Ok(())
    }
}

fn erc20_balance_slot(holder: Address, mapping_slot: u64) -> String {
    let mut preimage = [0u8; 64];
    preimage[12..32].copy_from_slice(holder.as_slice());
    preimage[32..].copy_from_slice(&U256::from(mapping_slot).to_be_bytes::<32>());
    format!("0x{}", hex::encode(keccak256(preimage)))
}

async fn timed<F>(label: &'static str, future: F) -> (&'static str, f64, bool, String)
where
    F: std::future::Future<Output = Result<(), String>>,
{
    let start = Instant::now();
    let outcome = future.await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    match outcome {
        Ok(()) => (label, elapsed_ms, true, String::new()),
        Err(e) => (label, elapsed_ms, false, truncate(e)),
    }
}

struct Sweep {
    backend: &'static str,
    block_height: u64,
    num_agents: usize,
    actions_per_agent: usize,
    episodes: usize,
}

/// `agent._run_episode`, one call per episode.
async fn run_episode(sweep: &Sweep, target: &Target, rpc: &Rpc, rng: &mut PyRandom, agent_id: usize, episode: usize) -> Vec<Record> {
    let mut records = Vec::new();
    let mut record = |(action, elapsed_ms, ok, error): (&'static str, f64, bool, String)| {
        records.push(Record {
            backend: sweep.backend,
            block_height: sweep.block_height,
            num_agents: sweep.num_agents,
            agent_id: agent_id as i64,
            action,
            elapsed_ms,
            ok,
            error,
        });
        ok
    };

    let start = Instant::now();
    let backend = acquire(target, rpc, agent_id, episode).await;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    let backend = match backend {
        Ok(backend) => {
            record(("acquire", elapsed_ms, true, String::new()));
            backend
        }
        Err(e) => {
            record(("acquire", elapsed_ms, false, truncate(e)));
            return records;
        }
    };

    let mut sender = Sender { signer: PrivateKeySigner::random(), chain: None };
    let me = sender.signer.address();
    let mut nonce = 0u64;
    let mut funded: Vec<Address> = Vec::new();
    let mut approved: Vec<Address> = Vec::new();
    let url = backend.url().to_string();

    let set_balance = backend.cheat("forkyard_setBalance", "anvil_setBalance");
    record(
        timed("set_balance", async {
            rpc.call(&url, set_balance, json!([me, format!("{:#x}", ONE_ETH)])).await.map(drop)
        })
        .await,
    );

    for _ in 0..sweep.actions_per_agent {
        let mut choices = vec!["transfer", "get_balance", "swap_eth_for_token", "fund_token"];
        if !funded.is_empty() {
            choices.push("approve");
        }
        if !approved.is_empty() {
            choices.push("swap_token_for_token");
        }
        let choice = *rng.choice(&choices);
        let token = rng.choice(&TOKENS).0;

        // Every transaction-sending action advances the local nonce, and a
        // failed one resyncs it from the chain: a rejected transaction uses
        // no nonce, a revert does.
        let sent = match choice {
            "transfer" => {
                let recipient = PrivateKeySigner::random().address();
                let result = timed("transfer", sender.send(rpc, &backend, Call { to: recipient, value: U256::from(ONE_ETH / 100), input: Bytes::new(), gas_limit: 21_000 }, nonce)).await;
                Some(record(result))
            }
            "get_balance" => {
                record(
                    timed("get_balance", async {
                        rpc.call(&url, "eth_getBalance", json!([me, "latest"])).await?;
                        rpc.call(&url, "eth_getTransactionCount", json!([me, "latest"])).await.map(drop)
                    })
                    .await,
                );
                None
            }
            "swap_eth_for_token" => {
                let data = swapExactETHForTokensCall {
                    amountOutMin: U256::ZERO,
                    path: vec![WETH, token],
                    to: me,
                    deadline: U256::from(FAR_FUTURE_DEADLINE),
                }
                .abi_encode();
                let result = timed("swap_eth_for_token", sender.send(rpc, &backend, Call { to: UNISWAP_V2_ROUTER, value: U256::from(ONE_ETH / 100), input: data.into(), gas_limit: 250_000 }, nonce)).await;
                Some(record(result))
            }
            "fund_token" => {
                let slot_index = TOKENS.iter().find(|(t, _)| *t == token).map(|(_, s)| *s).unwrap_or_default();
                let set_storage = backend.cheat("forkyard_setStorageAt", "anvil_setStorageAt");
                let value = format!("0x{}", hex::encode(U256::from(ONE_ETH).to_be_bytes::<32>()));
                record(
                    timed("fund_token", async {
                        rpc.call(&url, set_storage, json!([token, erc20_balance_slot(me, slot_index), value])).await.map(drop)
                    })
                    .await,
                );
                if !funded.contains(&token) {
                    funded.push(token);
                }
                None
            }
            "approve" => {
                let funded_token = *rng.choice(&funded);
                let data = approveCall { spender: UNISWAP_V2_ROUTER, amount: U256::from(ONE_ETH) }.abi_encode();
                let result = timed("approve", sender.send(rpc, &backend, Call { to: funded_token, value: U256::ZERO, input: data.into(), gas_limit: 60_000 }, nonce)).await;
                if !approved.contains(&funded_token) {
                    approved.push(funded_token);
                }
                Some(record(result))
            }
            "swap_token_for_token" => {
                let token_in = *rng.choice(&approved);
                let candidates: Vec<Address> =
                    TOKENS.iter().map(|(t, _)| *t).chain([WETH]).filter(|t| *t != token_in).collect();
                let token_out = *rng.choice(&candidates);
                let data = swapExactTokensForTokensCall {
                    amountIn: U256::from(ONE_ETH / 1000),
                    amountOutMin: U256::ZERO,
                    path: vec![token_in, token_out],
                    to: me,
                    deadline: U256::from(FAR_FUTURE_DEADLINE),
                }
                .abi_encode();
                let result = timed("swap_token_for_token", sender.send(rpc, &backend, Call { to: UNISWAP_V2_ROUTER, value: U256::ZERO, input: data.into(), gas_limit: 300_000 }, nonce)).await;
                Some(record(result))
            }
            _ => unreachable!("choices only holds the names above"),
        };
        if let Some(ok) = sent {
            nonce += 1;
            if !ok {
                if let Ok(count) = rpc.call(&url, "eth_getTransactionCount", json!([me, "latest"])).await {
                    nonce = hex_u256(&count).map(|n| n.to::<u64>()).unwrap_or(nonce);
                }
            }
        }
    }

    let result = timed("discard", backend.discard(rpc)).await;
    record(result);
    records
}

/// All agents at once, each on its own task; the wall clock they took together.
async fn run_agents(sweep: Arc<Sweep>, target: Target, rpc: Rpc) -> (Vec<Record>, f64) {
    let start = Instant::now();
    let tasks: Vec<_> = (0..sweep.num_agents)
        .map(|agent_id| {
            let (sweep, target, rpc) = (Arc::clone(&sweep), target.clone(), rpc.clone());
            tokio::spawn(async move {
                let mut rng = PyRandom::new(agent_id as u64);
                let mut records = Vec::new();
                for episode in 0..sweep.episodes {
                    records.extend(run_episode(&sweep, &target, &rpc, &mut rng, agent_id, episode).await);
                }
                records
            })
        })
        .collect();
    let mut records = Vec::new();
    for task in tasks {
        records.extend(task.await.expect("agent task panicked"));
    }
    (records, start.elapsed().as_secs_f64() * 1000.0)
}

struct Args {
    agents: Vec<usize>,
    block_heights: Vec<u64>,
    actions_per_agent: usize,
    episodes: usize,
    rpc_url: String,
    backends: Vec<String>,
    out: String,
    forkyard: String,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        agents: vec![],
        block_heights: vec![],
        actions_per_agent: 5,
        episodes: 1,
        rpc_url: std::env::var("RPC_URL").unwrap_or_default(),
        backends: vec!["forkyard".into(), "anvil".into()],
        out: "results.csv".into(),
        forkyard: "forkyard".into(),
    };
    let list = |v: &str| v.split(',').map(|x| x.trim().parse::<u64>().map_err(|e| format!("{v}: {e}"))).collect::<Result<Vec<_>, _>>();
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--agents" => args.agents = list(&value()?)?.into_iter().map(|n| n as usize).collect(),
            "--block-heights" => args.block_heights = list(&value()?)?,
            "--actions-per-agent" => args.actions_per_agent = value()?.parse().map_err(|e| format!("{e}"))?,
            "--episodes" => args.episodes = value()?.parse().map_err(|e| format!("{e}"))?,
            "--rpc-url" => args.rpc_url = value()?,
            "--backends" => args.backends = value()?.split(',').map(str::to_string).collect(),
            "--out" => args.out = value()?,
            "--forkyard-bin" => args.forkyard = value()?,
            "-h" | "--help" => {
                return Err("usage: forkyard-loadgen --agents 1,10,50 --block-heights 25795072 \
                            [--actions-per-agent 5] [--episodes 1] [--rpc-url URL] \
                            [--backends forkyard,anvil] [--forkyard-bin forkyard] [--out results.csv]"
                    .into())
            }
            other => return Err(format!("unknown flag {other}")),
        }
    }
    if args.agents.is_empty() || args.block_heights.is_empty() || args.rpc_url.is_empty() {
        return Err("--agents, --block-heights and --rpc-url (or $RPC_URL) are required".into());
    }
    Ok(args)
}

const FORKYARD_PORT: u16 = 18555;

/// One forkyard process for the sweep, as `run_forkyard_sweep` starts it:
/// pinned block, persistent cache on, stopped with SIGTERM so it saves it.
async fn start_forkyard(bin: &str, rpc_url: &str, block_height: u64, rpc: &Rpc) -> Result<tokio::process::Child, String> {
    let child = tokio::process::Command::new(bin)
        .env("RPC_URL", rpc_url)
        .env("FORKYARD_PORT", FORKYARD_PORT.to_string())
        .env("FORKYARD_MCP_HTTP_PORT", (FORKYARD_PORT + 1).to_string())
        .env("FORKYARD_FORK_BLOCK_NUMBER", block_height.to_string())
        .env_remove("FORKYARD_CACHE_DISABLED")
        .env("RUST_LOG", std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".into()))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawning {bin}: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        if let Ok(response) = rpc.http.post(format!("http://127.0.0.1:{FORKYARD_PORT}/session")).send().await {
            if response.status().is_success() {
                return Ok(child);
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err("forkyard did not become ready within 60 s".into())
}

async fn stop(mut child: tokio::process::Child) {
    terminate(&child);
    if tokio::time::timeout(Duration::from_secs(10), child.wait()).await.is_err() {
        let _ = child.kill().await;
    }
}

fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[tokio::main]
async fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let rpc = Rpc {
        http: reqwest::Client::builder()
            .pool_max_idle_per_host(1024)
            .timeout(Duration::from_secs(60))
            .build()
            .expect("http client"),
    };
    let mut out = std::fs::File::create(&args.out).expect("cannot create --out");
    writeln!(out, "backend,block_height,num_agents,agent_id,action,elapsed_ms,ok,error").unwrap();

    for &block_height in &args.block_heights {
        for &num_agents in &args.agents {
            for backend in &args.backends {
                let (name, target, process): (&'static str, Target, Option<tokio::process::Child>) = match backend.as_str() {
                    "forkyard" => {
                        let child = start_forkyard(&args.forkyard, &args.rpc_url, block_height, &rpc).await.unwrap_or_else(|e| {
                            eprintln!("{e}");
                            std::process::exit(1)
                        });
                        let base_url = format!("http://127.0.0.1:{FORKYARD_PORT}");
                        ("forkyard", Target::Forkyard { base_url }, Some(child))
                    }
                    "anvil" => (
                        "anvil",
                        Target::Anvil { rpc_url: args.rpc_url.clone(), block_height, base_port: 19000, episodes: args.episodes },
                        None,
                    ),
                    other => {
                        eprintln!("unknown backend {other}");
                        std::process::exit(2)
                    }
                };
                eprintln!("running {name}: block={block_height} agents={num_agents} episodes={}", args.episodes);
                let sweep = Arc::new(Sweep {
                    backend: name,
                    block_height,
                    num_agents,
                    actions_per_agent: args.actions_per_agent,
                    episodes: args.episodes,
                });
                let (mut records, total_ms) = run_agents(Arc::clone(&sweep), target, rpc.clone()).await;
                if let Some(child) = process {
                    stop(child).await;
                }
                let all_ok = records.iter().all(|r| r.ok);
                records.push(Record {
                    backend: name,
                    block_height,
                    num_agents,
                    agent_id: -1,
                    action: "__total__",
                    elapsed_ms: total_ms,
                    ok: all_ok,
                    error: String::new(),
                });
                let failed = records.iter().filter(|r| !r.ok && r.agent_id >= 0).count();
                eprintln!("  {name} {num_agents} agents: {:.3} s, {failed} failed actions", total_ms / 1000.0);
                let mut buf = String::new();
                for r in &records {
                    let ok = if r.ok { "True" } else { "False" };
                    writeln!(
                        buf,
                        "{},{},{},{},{},{},{},{}",
                        r.backend, r.block_height, r.num_agents, r.agent_id, r.action, r.elapsed_ms, ok, csv_field(&r.error)
                    )
                    .unwrap();
                }
                out.write_all(buf.as_bytes()).unwrap();
                out.flush().unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference values from CPython 3.11's `random.Random`.
    #[test]
    fn the_rng_matches_cpython_bit_for_bit() {
        for (seed, expected) in [
            (0u64, [3_626_764_237u32, 1_654_615_998, 3_255_389_356]),
            (1, [577_090_037, 2_444_712_010, 3_639_700_191]),
            (49, [287_264_509, 1_478_885_609, 1_774_864_359]),
        ] {
            let mut rng = PyRandom::new(seed);
            assert_eq!([rng.next_u32(), rng.next_u32(), rng.next_u32()], expected, "seed {seed}");
        }
    }

    #[test]
    fn choice_matches_cpython_including_the_rejection_loop() {
        let mut rng = PyRandom::new(7);
        let letters = ["a", "b", "c", "d", "e"];
        let picked: Vec<&str> = (0..8).map(|_| *rng.choice(&letters)).collect();
        assert_eq!(picked, ["c", "b", "d", "a", "a", "e", "a", "c"]);
        // A one-element choice still consumes randomness in CPython.
        assert_eq!([*rng.choice(&[1]), *rng.choice(&[1])], [1, 1]);
        assert_eq!(rng.next_u32(), 161_042_648);
    }

    #[test]
    fn the_erc20_slot_matches_the_python_harness() {
        // `backend.erc20_balance_slot("0x…01", 2)`, computed by the Python harness.
        assert_eq!(erc20_balance_slot(Address::with_last_byte(1), 2), "0xe90b7bceb6e7df5418fb78d8ee546e97c83a08bbccc01a0644d599ccd2a7c2e0");
    }

    #[test]
    fn a_field_with_a_comma_is_quoted() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a, \"b\""), "\"a, \"\"b\"\"\"");
    }
}
