//! Red-team suite: attacks a real `forkyard-api-http` server over TCP, the
//! way a hostile or confused agent would, against a stub chain — so it
//! needs no RPC key and runs in CI.
//!
//! Each test states the attack in its name. A test that passes is a
//! property that held. A test marked `#[ignore = "KNOWN: …"]` is a gap the
//! suite found and the project has not closed yet: run
//! `cargo test -p forkyard-api-http --test red_team -- --ignored` to see
//! each one fail, and delete the `ignore` when its fix lands.
//!
//! The canary technique: every isolation test plants a recognisable value
//! (`CANARY`) in one session and then tries, by every route the API
//! offers, to read it from another. Seeing it anywhere but its own session
//! is an escape.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use forkyard_engine::persist::SnapshotStore;
use forkyard_session::{Limits, SessionManager};
use revm::database_interface::{DBErrorMarker, DatabaseRef};
use revm::primitives::{Address, B256, U256};
use revm::state::{AccountInfo, Bytecode};
use serde_json::{json, Value};

const CANARY: &str = "0xc0ffee00000000000000000000000000000000000000000000000000000000ff";
const CANARY_SLOT: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
const VICTIM: &str = "0x00000000000000000000000000000000000000aa";

/// A chain with nothing on it: any non-zero value an agent reads back came
/// from another agent, not from upstream.
#[derive(Clone)]
struct EmptyChain;

#[derive(Debug)]
struct Never;
impl std::fmt::Display for Never {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "empty chain")
    }
}
impl std::error::Error for Never {}
impl DBErrorMarker for Never {}

impl DatabaseRef for EmptyChain {
    type Error = Never;
    fn basic_ref(&self, _: Address) -> Result<Option<AccountInfo>, Never> {
        Ok(Some(AccountInfo::default()))
    }
    fn code_by_hash_ref(&self, _: B256) -> Result<Bytecode, Never> {
        Ok(Bytecode::default())
    }
    fn storage_ref(&self, _: Address, _: U256) -> Result<U256, Never> {
        Ok(U256::ZERO)
    }
    fn block_hash_ref(&self, _: u64) -> Result<B256, Never> {
        Ok(B256::ZERO)
    }
}

struct Target {
    base: String,
    http: reqwest::Client,
    manager: Arc<SessionManager<EmptyChain>>,
    snapshot_dir: std::path::PathBuf,
    _handle: forkyard_api_http::Handle,
}

fn scratch_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "forkyard-redteam-{tag}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

impl Target {
    async fn start(limits: Limits) -> Self {
        Self::start_in(limits, scratch_dir("snap"), Duration::from_secs(60)).await
    }

    async fn start_in(limits: Limits, snapshot_dir: std::path::PathBuf, ttl: Duration) -> Self {
        let manager = Arc::new(
            SessionManager::new(EmptyChain, Default::default(), 2, ttl)
                .with_snapshots(SnapshotStore::new(&snapshot_dir, 1))
                .with_limits(limits),
        );
        let handle = forkyard_api_http::serve("127.0.0.1:0", Arc::clone(&manager), 1)
            .await
            .unwrap();
        Self {
            base: format!("http://{}", handle.addr),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            manager,
            snapshot_dir,
            _handle: handle,
        }
    }

    /// `POST /session` with an optional body; the parsed reply.
    async fn open(&self, body: Option<Value>) -> Value {
        let request = self.http.post(format!("{}/session", self.base));
        let request = match body {
            Some(body) => request.json(&body),
            None => request,
        };
        request.send().await.unwrap().json().await.unwrap()
    }

    async fn session(&self) -> u64 {
        self.open(None).await["session_id"]
            .as_u64()
            .expect("a session opens")
    }

    /// One JSON-RPC call; the whole reply object.
    async fn rpc(&self, session: u64, method: &str, params: Value) -> Value {
        self.http
            .post(format!("{}/session/{session}", self.base))
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{method} got no reply — the server may have died: {e}"))
            .json()
            .await
            .unwrap_or_else(|e| panic!("{method} reply was not JSON: {e}"))
    }

    /// Raw bytes to a path; status and body text. For input that isn't
    /// well-formed JSON-RPC at all.
    async fn raw(&self, path: &str, body: Vec<u8>) -> (u16, String) {
        let reply = self
            .http
            .post(format!("{}{path}", self.base))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap_or_else(|e| panic!("POST {path} got no reply — the server may have died: {e}"));
        (
            reply.status().as_u16(),
            reply.text().await.unwrap_or_default(),
        )
    }

    /// Like `raw`, but a dropped connection is an answer too: a server that
    /// refuses a huge body may hang up before the client finishes sending it.
    async fn raw_or_hangup(&self, path: &str, body: Vec<u8>) -> Option<u16> {
        let reply = self
            .http
            .post(format!("{}{path}", self.base))
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await;
        reply.ok().map(|r| r.status().as_u16())
    }

    /// The server still serves an ordinary request — the bar after any
    /// hostile input.
    async fn assert_alive(&self) {
        let id = self.session().await;
        let chain = self.rpc(id, "eth_chainId", json!([])).await;
        assert_eq!(
            chain["result"], "0x1",
            "server stopped answering normally: {chain}"
        );
        self.rpc(id, "forkyard_discard", json!([])).await;
    }

    async fn storage(&self, session: u64, slot: &str) -> String {
        self.rpc(session, "eth_getStorageAt", json!([VICTIM, slot, "latest"]))
            .await["result"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }

    async fn balance(&self, session: u64, address: &str) -> String {
        self.rpc(session, "eth_getBalance", json!([address, "latest"]))
            .await["result"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }
}

fn is_zero_word(word: &str) -> bool {
    word.trim_start_matches("0x").chars().all(|c| c == '0')
}

fn error_text(reply: &Value) -> String {
    reply["error"]["message"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| reply["error"].to_string())
}

// ───────────────────────── isolation ─────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_canary_written_in_one_session_is_invisible_to_every_other() {
    let t = Target::start(Limits::unlimited()).await;
    let attacker = t.session().await;
    let victim = t.session().await;

    t.rpc(
        victim,
        "forkyard_setStorageAt",
        json!([VICTIM, CANARY_SLOT, CANARY]),
    )
    .await;
    t.rpc(victim, "forkyard_setBalance", json!([VICTIM, "0xdeadbeef"]))
        .await;
    assert!(
        !is_zero_word(&t.storage(victim, CANARY_SLOT).await),
        "setup: the canary is in the victim session"
    );

    // Concurrent sibling, and one opened *after* the write.
    let late = t.session().await;
    for (name, id) in [("sibling", attacker), ("late-opened", late)] {
        assert!(
            is_zero_word(&t.storage(id, CANARY_SLOT).await),
            "{name} session read the canary slot"
        );
        assert!(
            is_zero_word(&t.balance(id, VICTIM).await),
            "{name} session read the canary balance"
        );
    }

    // After the victim is gone the shared base must still be clean.
    t.rpc(victim, "forkyard_discard", json!([])).await;
    let after = t.session().await;
    assert!(
        is_zero_word(&t.storage(after, CANARY_SLOT).await),
        "the canary leaked into the shared cache"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn advance_commits_into_the_callers_overlay_only() {
    let t = Target::start(Limits::unlimited()).await;
    let a = t.session().await;
    let b = t.session().await;
    t.rpc(
        a,
        "forkyard_setBalance",
        json!([VICTIM, "0x3635c9adc5dea00000"]),
    )
    .await;
    assert!(is_zero_word(&t.balance(b, VICTIM).await));
    assert!(!is_zero_word(&t.balance(a, VICTIM).await));
}

#[tokio::test(flavor = "multi_thread")]
async fn branch_writes_stay_private_in_both_directions() {
    let t = Target::start(Limits::unlimited()).await;
    let parent = t.session().await;
    t.rpc(
        parent,
        "forkyard_setStorageAt",
        json!([VICTIM, CANARY_SLOT, CANARY]),
    )
    .await;
    let child = t.rpc(parent, "forkyard_forkFrom", json!([])).await["result"]["session_id"]
        .as_u64()
        .unwrap();
    assert!(
        !is_zero_word(&t.storage(child, CANARY_SLOT).await),
        "the child inherits the parent's state"
    );

    let slot2 = "0x0000000000000000000000000000000000000000000000000000000000000002";
    t.rpc(
        child,
        "forkyard_setStorageAt",
        json!([VICTIM, slot2, CANARY]),
    )
    .await;
    t.rpc(parent, "forkyard_setBalance", json!([VICTIM, "0x1"]))
        .await;
    assert!(
        is_zero_word(&t.storage(parent, slot2).await),
        "a child write reached its parent"
    );
    assert!(
        is_zero_word(&t.balance(child, VICTIM).await),
        "a parent write after the branch reached its child"
    );

    t.rpc(parent, "forkyard_discard", json!([])).await;
    assert!(
        !is_zero_word(&t.storage(child, CANARY_SLOT).await),
        "discarding the parent damaged the child"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resumed_snapshot_is_a_private_copy_not_a_shared_session() {
    let t = Target::start(Limits::unlimited()).await;
    let origin = t.session().await;
    t.rpc(
        origin,
        "forkyard_setStorageAt",
        json!([VICTIM, CANARY_SLOT, CANARY]),
    )
    .await;
    let snapshot_id = t.rpc(origin, "forkyard_snapshot", json!([])).await["result"]["snapshot_id"]
        .as_str()
        .unwrap()
        .to_string();

    let one = t.open(Some(json!({"snapshot_id": snapshot_id}))).await["session_id"]
        .as_u64()
        .unwrap();
    let two = t.open(Some(json!({"snapshot_id": snapshot_id}))).await["session_id"]
        .as_u64()
        .unwrap();
    t.rpc(one, "forkyard_setBalance", json!([VICTIM, "0x99"]))
        .await;
    assert!(
        is_zero_word(&t.balance(two, VICTIM).await),
        "two resumes of one snapshot share state"
    );
    assert!(
        is_zero_word(&t.balance(origin, VICTIM).await),
        "a resumed session wrote back into the original"
    );
}

// ───────────────────── malformed and hostile input ─────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn malformed_bodies_never_kill_the_server() {
    let t = Target::start(Limits::unlimited()).await;
    let id = t.session().await;
    let deep = format!("{}{}", "[".repeat(5_000), "]".repeat(5_000));
    let bodies: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("not json", b"\x00\xff\xfe garbage".to_vec()),
        (
            "truncated",
            br#"{"jsonrpc":"2.0","id":1,"method":"eth_ch"#.to_vec(),
        ),
        (
            "array batch",
            br#"[{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}]"#.to_vec(),
        ),
        (
            "params is an object",
            br#"{"id":1,"method":"eth_getBalance","params":{"a":1}}"#.to_vec(),
        ),
        (
            "params is null",
            br#"{"id":1,"method":"eth_chainId","params":null}"#.to_vec(),
        ),
        (
            "method is a number",
            br#"{"id":1,"method":42,"params":[]}"#.to_vec(),
        ),
        ("no method", br#"{"id":1}"#.to_vec()),
        (
            "method with NUL",
            "{\"id\":1,\"method\":\"eth_\\u0000chainId\",\"params\":[]}"
                .as_bytes()
                .to_vec(),
        ),
        ("deeply nested", deep.into_bytes()),
        (
            "huge id",
            format!(
                r#"{{"id":"{}","method":"eth_chainId","params":[]}}"#,
                "x".repeat(100_000)
            )
            .into_bytes(),
        ),
    ];
    for (name, body) in bodies {
        let (status, text) = t.raw(&format!("/session/{id}"), body).await;
        assert!(status < 500, "{name}: server error {status}: {text}");
    }
    t.assert_alive().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hex_and_number_edge_cases_are_errors_not_panics() {
    let t = Target::start(Limits::unlimited()).await;
    let id = t.session().await;
    let too_wide = format!("0x{}", "f".repeat(80)); // > 256 bits
    let calls: Vec<(&str, Value)> = vec![
        ("forkyard_setBalance", json!([VICTIM, too_wide])),
        ("forkyard_setBalance", json!([VICTIM, "0x"])),
        ("forkyard_setBalance", json!([VICTIM, "0xzz"])),
        ("forkyard_setBalance", json!([VICTIM, "-1"])),
        ("forkyard_setBalance", json!(["0x1234", "0x1"])),
        ("forkyard_setBalance", json!([VICTIM])),
        ("forkyard_setStorageAt", json!([VICTIM, too_wide, CANARY])),
        ("forkyard_setStorageAt", json!([VICTIM, CANARY_SLOT, "0x"])),
        ("eth_getStorageAt", json!([VICTIM, too_wide, "latest"])),
        ("eth_getBalance", json!([42, "latest"])),
        ("eth_getBalance", json!([])),
        ("eth_getCode", json!([null])),
        ("eth_call", json!([{"to": VICTIM, "data": "0xabc"}])), // odd-length hex
        ("eth_call", json!([{"to": VICTIM, "gas": too_wide}])),
        ("eth_call", json!([{"to": "not an address"}])),
        ("eth_call", json!([{"value": too_wide}])),
        ("eth_call", json!(["not an object"])),
        (
            "eth_estimateGas",
            json!([{"to": VICTIM, "value": "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"}]),
        ),
        ("eth_sendRawTransaction", json!(["0x"])),
        ("eth_sendRawTransaction", json!(["0xdeadbeef"])),
        ("eth_sendRawTransaction", json!(["0x02f8"])), // typed, truncated
        (
            "eth_sendRawTransaction",
            json!([format!("0x{}", "ab".repeat(100_000))]),
        ),
        ("eth_getTransactionReceipt", json!([null])),
        ("eth_getTransactionReceipt", json!(["0x00"])),
    ];
    for (method, params) in calls {
        let reply = t.rpc(id, method, params.clone()).await;
        assert!(
            reply.get("error").is_some() || reply.get("result").is_some(),
            "{method} {params}: unusable reply {reply}"
        );
        let message = reply.get("error").map(error_text).unwrap_or_default();
        assert!(
            !message.contains("worker thread is gone"),
            "{method} {params} took a worker down: {message}"
        );
    }
    // A worker panic is caught and logged, so the symptom is that the same
    // session stops answering — check it still does.
    assert_eq!(t.rpc(id, "eth_chainId", json!([])).await["result"], "0x1");
    t.assert_alive().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn odd_session_ids_are_errors_not_panics() {
    let t = Target::start(Limits::unlimited()).await;
    for path in [
        "abc",
        "-1",
        "18446744073709551615",
        "18446744073709551616",
        "1.5",
        "%00",
        "0x1",
        "",
    ] {
        let (status, body) = t
            .raw(
                &format!("/session/{path}"),
                br#"{"id":1,"method":"eth_chainId"}"#.to_vec(),
            )
            .await;
        assert!(status < 500, "/session/{path:?}: {status} {body}");
    }
    t.assert_alive().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_id_that_is_not_ours_never_touches_the_filesystem() {
    let t = Target::start(Limits::unlimited()).await;
    let ids = [
        "../../../../etc/passwd".to_string(),
        "/etc/passwd".to_string(),
        "..".to_string(),
        "A".repeat(32), // uppercase hex
        "g".repeat(32), // right length, not hex
        "a".repeat(31),
        "a".repeat(33),
        format!("{}\u{0}", "a".repeat(31)),
        "ａ".repeat(32), // full-width letters
        String::new(),
    ];
    for id in ids {
        let reply = t.open(Some(json!({"snapshot_id": id}))).await;
        assert!(
            reply.get("session_id").is_none(),
            "{id:?} opened a session: {reply}"
        );
        assert!(reply["error"].is_string(), "{id:?}: {reply}");
    }
    t.assert_alive().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_request_body_is_refused_before_it_is_parsed() {
    let t = Target::start(Limits::unlimited()).await;
    let id = t.session().await;
    let body = format!(
        r#"{{"id":1,"method":"eth_chainId","params":["{}"]}}"#,
        "x".repeat(8 * 1024 * 1024)
    );
    // `None` is the server hanging up mid-upload: also a refusal.
    let refused_with = t.raw_or_hangup(&format!("/session/{id}"), body.into_bytes()).await;
    if let Some(status) = refused_with {
        assert!((400..500).contains(&status), "an 8 MB body was not refused: HTTP {status}");
    }
    t.assert_alive().await;
}

// ───────────────────────── resource limits ─────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn the_session_cap_holds_over_http_and_a_discard_frees_a_slot() {
    let t = Target::start(Limits {
        max_sessions: Some(3),
        ..Limits::unlimited()
    })
    .await;
    let mut opened = vec![];
    let mut refused = 0;
    for _ in 0..6 {
        let reply = t.open(None).await;
        match reply["session_id"].as_u64() {
            Some(id) => opened.push(id),
            None => {
                refused += 1;
                assert!(
                    reply["error"]
                        .as_str()
                        .unwrap()
                        .contains("FORKYARD_MAX_SESSIONS"),
                    "{reply}"
                );
            }
        }
    }
    assert_eq!((opened.len(), refused), (3, 3));
    // Branching and resuming are sessions too: neither is a way around it.
    assert!(t.rpc(opened[0], "forkyard_forkFrom", json!([])).await["error"].is_object());

    t.rpc(opened[0], "forkyard_discard", json!([])).await;
    assert!(
        t.open(None).await["session_id"].is_u64(),
        "a discard freed no slot"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_flood_of_concurrent_session_opens_cannot_exceed_the_cap() {
    let t = Arc::new(
        Target::start(Limits {
            max_sessions: Some(10),
            ..Limits::unlimited()
        })
        .await,
    );
    let attempts: Vec<_> = (0..200)
        .map(|_| {
            let t = Arc::clone(&t);
            tokio::spawn(async move { t.open(None).await["session_id"].as_u64() })
        })
        .collect();
    let mut ids = HashSet::new();
    for attempt in attempts {
        ids.extend(attempt.await.unwrap());
    }
    assert_eq!(ids.len(), 10);
    assert_eq!(t.manager.active_session_count(), 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_that_fills_its_entry_allowance_is_stopped_with_the_knob_named() {
    let t = Target::start(Limits {
        max_session_entries: Some(20),
        ..Limits::unlimited()
    })
    .await;
    let id = t.session().await;
    let mut stopped_at = None;
    for n in 0..100u32 {
        let slot = format!("0x{n:064x}");
        let reply = t
            .rpc(id, "forkyard_setStorageAt", json!([VICTIM, slot, CANARY]))
            .await;
        if reply.get("error").is_some() {
            assert!(
                error_text(&reply).contains("FORKYARD_MAX_SESSION_ENTRIES"),
                "{reply}"
            );
            stopped_at = Some(n);
            break;
        }
    }
    assert_eq!(
        stopped_at,
        Some(20),
        "writes should stop exactly at the allowance"
    );
    // A full session can still be saved and torn down.
    assert!(t.rpc(id, "forkyard_snapshot", json!([])).await["result"]["snapshot_id"].is_string());
    assert_eq!(
        t.rpc(id, "forkyard_discard", json!([])).await["result"],
        true
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn gas_above_the_cap_is_refused_and_defaults_stay_under_it() {
    let t = Target::start(Limits {
        max_tx_gas_limit: Some(1_000_000),
        ..Limits::unlimited()
    })
    .await;
    let id = t.session().await;

    let greedy = t
        .rpc(id, "eth_call", json!([{"to": VICTIM, "gas": "0x2faf080"}]))
        .await; // 50M
    assert!(
        error_text(&greedy).contains("FORKYARD_MAX_TX_GAS_LIMIT"),
        "{greedy}"
    );

    // The server's own defaults must not trip its own limit, or lowering
    // the cap would break every bare eth_call and every wallet's estimate.
    let bare = t.rpc(id, "eth_call", json!([{"to": VICTIM}])).await;
    assert!(
        bare.get("error").is_none(),
        "a bare eth_call was refused by the server's own default: {bare}"
    );
    let estimate = t.rpc(id, "eth_estimateGas", json!([{"to": VICTIM}])).await;
    assert!(
        estimate.get("error").is_none(),
        "eth_estimateGas was refused by the server's own default: {estimate}"
    );
}

// ───────────────────── snapshot integrity and bypasses ─────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_edited_on_disk_is_refused_not_resumed() {
    let t = Target::start(Limits::unlimited()).await;
    let a = t.session().await;
    t.rpc(
        a,
        "forkyard_setStorageAt",
        json!([VICTIM, CANARY_SLOT, "0x01"]),
    )
    .await;
    let id = t.rpc(a, "forkyard_snapshot", json!([])).await["result"]["snapshot_id"]
        .as_str()
        .unwrap()
        .to_string();

    let path = SnapshotStore::new(&t.snapshot_dir, 1)
        .path_for(&id)
        .unwrap();
    let original = std::fs::read_to_string(&path).unwrap();
    let forged = original.replace(r#""0x1"]]"#, r#""0x2"]]"#);
    assert_ne!(original, forged, "setup: the edit changed the file");
    std::fs::write(&path, forged).unwrap();

    let reply = t.open(Some(json!({"snapshot_id": id}))).await;
    assert!(
        reply.get("session_id").is_none(),
        "a tampered snapshot was resumed: {reply}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn resuming_a_big_snapshot_cannot_get_past_the_entry_limit() {
    let shared = scratch_dir("shared");
    // One process with no limit writes a 50-slot snapshot into a shared dir…
    let loose =
        Target::start_in(Limits::unlimited(), shared.clone(), Duration::from_secs(60)).await;
    let big = loose.session().await;
    for n in 0..50u32 {
        loose
            .rpc(
                big,
                "forkyard_setStorageAt",
                json!([VICTIM, format!("0x{n:064x}"), CANARY]),
            )
            .await;
    }
    let id = loose.rpc(big, "forkyard_snapshot", json!([])).await["result"]["snapshot_id"]
        .as_str()
        .unwrap()
        .to_string();

    // …and a strict one is asked to resume it.
    let strict = Target::start_in(
        Limits {
            max_session_entries: Some(10),
            ..Limits::unlimited()
        },
        shared,
        Duration::from_secs(60),
    )
    .await;
    let reply = strict.open(Some(json!({"snapshot_id": id}))).await;
    assert!(
        reply.get("session_id").is_none(),
        "resume created a session 5x over its limit: {reply}"
    );
    assert!(
        reply["error"]
            .as_str()
            .unwrap()
            .contains("FORKYARD_MAX_SESSION_ENTRIES"),
        "{reply}"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "KNOWN: nothing bounds how many snapshots (or bytes) accumulate in FORKYARD_SNAPSHOT_DIR"]
async fn snapshots_cannot_fill_the_disk() {
    let t = Target::start(Limits::unlimited()).await;
    let id = t.session().await;
    let mut refused = false;
    for n in 0..300u32 {
        // Distinct state each time, so content addressing can't dedupe it.
        t.rpc(
            id,
            "forkyard_setStorageAt",
            json!([VICTIM, format!("0x{n:064x}"), CANARY]),
        )
        .await;
        let reply = t.rpc(id, "forkyard_snapshot", json!([])).await;
        if reply.get("error").is_some() {
            refused = true;
            break;
        }
    }
    assert!(
        refused,
        "300 distinct snapshots were all accepted; the directory has no cap"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "KNOWN: session ids are sequential integers and there is no authentication, so any client can drive or discard any session"]
async fn one_client_cannot_drive_or_destroy_anothers_session_by_guessing_its_id() {
    let t = Target::start(Limits::unlimited()).await;
    let victim = t.session().await;
    t.rpc(
        victim,
        "forkyard_setStorageAt",
        json!([VICTIM, CANARY_SLOT, CANARY]),
    )
    .await;

    // The attacker never opened `victim`; it just counts up from 0.
    let stolen = t.storage(victim, CANARY_SLOT).await;
    let killed = t.rpc(victim, "forkyard_discard", json!([])).await;
    assert!(
        is_zero_word(&stolen) && killed.get("error").is_some(),
        "guessed id {victim}: read {stolen}, discard -> {killed}"
    );
}
