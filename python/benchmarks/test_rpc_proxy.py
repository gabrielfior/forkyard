import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from rpc_proxy import RATE_LIMIT_ERROR_CODE, CountingProxy, TokenBucket


class _StubUpstream:
    """A minimal JSON-RPC endpoint, so these tests never touch a real
    provider (and so the counts asserted below can only come from the
    proxy)."""

    def __init__(self):
        self.received: list[object] = []
        received = self.received

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):  # noqa: N802
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                received.append(json.loads(body))
                payload = json.dumps({"jsonrpc": "2.0", "id": 1, "result": "0x1"}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def upstream():
    stub = _StubUpstream()
    yield stub
    stub.stop()


def _call(url, method="eth_getBalance"):
    return requests.post(
        url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": []}, timeout=5
    )


def test_proxy_forwards_the_response_untouched(upstream):
    with CountingProxy(upstream.url) as proxy:
        resp = _call(proxy.url)
    assert resp.json() == {"jsonrpc": "2.0", "id": 1, "result": "0x1"}
    assert upstream.received, "the request never reached the upstream"


def test_proxy_counts_calls_by_method(upstream):
    with CountingProxy(upstream.url) as proxy:
        _call(proxy.url, "eth_getBalance")
        _call(proxy.url, "eth_getBalance")
        _call(proxy.url, "eth_getStorageAt")
        stats = proxy.snapshot()

    assert stats.http_requests == 3
    assert stats.jsonrpc_calls == 3
    assert stats.by_method == {"eth_getBalance": 2, "eth_getStorageAt": 1}
    assert stats.upstream_errors == 0


def test_a_batch_is_one_http_request_but_many_jsonrpc_calls(upstream):
    """Providers bill per call, not per HTTP request, and both backends
    batch. Counting only requests would understate whichever backend
    batches harder."""
    batch = [
        {"jsonrpc": "2.0", "id": 1, "method": "eth_getCode", "params": []},
        {"jsonrpc": "2.0", "id": 2, "method": "eth_getCode", "params": []},
        {"jsonrpc": "2.0", "id": 3, "method": "eth_getStorageAt", "params": []},
    ]
    with CountingProxy(upstream.url) as proxy:
        requests.post(proxy.url, json=batch, timeout=5)
        stats = proxy.snapshot()

    assert stats.http_requests == 1
    assert stats.jsonrpc_calls == 3
    assert stats.by_method == {"eth_getCode": 2, "eth_getStorageAt": 1}


def test_reset_zeroes_the_counters_between_combinations(upstream):
    with CountingProxy(upstream.url) as proxy:
        _call(proxy.url)
        proxy.reset()
        assert proxy.snapshot().jsonrpc_calls == 0
        _call(proxy.url)
        assert proxy.snapshot().jsonrpc_calls == 1


def test_an_unreachable_upstream_is_counted_not_raised():
    """A sweep must not die because the provider blipped: the agent sees a
    JSON-RPC error like any other, and the blip is visible in the stats."""
    with CountingProxy("http://127.0.0.1:9") as proxy:  # port 9 = discard
        resp = _call(proxy.url)
        stats = proxy.snapshot()

    assert resp.status_code == 502
    assert "error" in resp.json()
    assert stats.upstream_errors == 1
    assert stats.jsonrpc_calls == 1


# ---------------------------------------------------------------------------
# Rate limiting. The bucket maths is tested directly against an injected
# clock (no sleeping, so no flakiness); only the two end-to-end tests below
# actually wait, and they wait tenths of a second.
# ---------------------------------------------------------------------------


class _FakeClock:
    """A hand-cranked monotonic clock, so "one second later" costs nothing."""

    def __init__(self, t: float = 0.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def test_bucket_lets_the_first_burst_through_and_delays_the_next_call():
    """The headline property: a limiter at N rps passes N calls and makes
    the N+1st wait. Without it the "quota" in bench_quota.py would be
    decorative."""
    clock = _FakeClock()
    bucket = TokenBucket(rate_per_s=10, clock=clock)

    assert [bucket.reserve(1) for _ in range(10)] == [0.0] * 10
    # 11th call: one token short at 10/s, so a tenth of a second behind.
    assert bucket.reserve(1) == pytest.approx(0.1)


def test_bucket_queues_rather_than_drops_so_delays_accumulate():
    """Two calls past the budget wait twice as long as one: the debt is
    charged under the lock before anyone sleeps, which is what makes
    concurrent callers serialise instead of all waking together."""
    clock = _FakeClock()
    bucket = TokenBucket(rate_per_s=10, burst=1, clock=clock)

    assert bucket.reserve(1) == 0.0
    assert bucket.reserve(1) == pytest.approx(0.1)
    assert bucket.reserve(1) == pytest.approx(0.2)


def test_bucket_refills_at_the_configured_rate_and_stops_at_the_burst():
    clock = _FakeClock()
    bucket = TokenBucket(rate_per_s=10, burst=10, clock=clock)

    for _ in range(10):
        bucket.reserve(1)
    assert bucket.available() == pytest.approx(0.0)

    clock.advance(0.5)
    assert bucket.available() == pytest.approx(5.0)
    clock.advance(100)  # a long idle stretch must not bank 1000 tokens
    assert bucket.available() == pytest.approx(10.0)


def test_bucket_charges_a_batch_of_m_calls_m_tokens():
    """Providers bill and throttle per call, so a batch of M must cost M
    tokens. Charging one per HTTP request would let a batching client walk
    straight through any quota."""
    clock = _FakeClock()
    bucket = TokenBucket(rate_per_s=10, burst=10, clock=clock)

    assert bucket.reserve(8) == 0.0
    assert bucket.available() == pytest.approx(2.0)
    assert bucket.reserve(4) == pytest.approx(0.2)  # 2 tokens short at 10/s


def test_try_consume_refuses_instead_of_queueing():
    clock = _FakeClock()
    bucket = TokenBucket(rate_per_s=10, burst=2, clock=clock)

    assert bucket.try_consume(2) is True
    assert bucket.try_consume(1) is False
    assert bucket.available() == pytest.approx(0.0), "a refused call must not be charged"
    clock.advance(0.1)
    assert bucket.try_consume(1) is True


def test_a_batch_bigger_than_the_burst_can_never_be_consumed():
    """Documented behaviour, not an accident: under a per-window quota a
    single request larger than the window is always refused."""
    bucket = TokenBucket(rate_per_s=10, burst=5, clock=_FakeClock())
    assert bucket.try_consume(6) is False


def test_bucket_rejects_a_nonsensical_rate():
    with pytest.raises(ValueError):
        TokenBucket(rate_per_s=0)
    with pytest.raises(ValueError):
        TokenBucket(rate_per_s=10, burst=0)


def test_unlimited_proxy_records_no_throttling(upstream):
    """The no-limit path must stay exactly what it was: no bucket, no clock
    read, no delay columns moving."""
    with CountingProxy(upstream.url) as proxy:
        for _ in range(5):
            _call(proxy.url)
        stats = proxy.snapshot()

    assert stats.jsonrpc_calls == 5
    assert stats.throttled_calls == 0
    assert stats.rejected_calls == 0
    assert stats.total_delay_ms == 0.0


def test_delay_mode_makes_the_over_budget_call_wait_and_still_serves_it(upstream):
    """A queueing provider turns excess volume into latency, not errors:
    the call is slow, but it is answered and it still costs money."""
    with CountingProxy(upstream.url, rate_limit_rps=4, limit_mode="delay") as proxy:
        for _ in range(4):  # the initial burst, straight through
            _call(proxy.url)
        start = time.monotonic()
        resp = _call(proxy.url)
        waited_ms = (time.monotonic() - start) * 1000
        stats = proxy.snapshot()

    assert resp.status_code == 200, "delay mode must never fail a call"
    assert resp.json()["result"] == "0x1"
    assert waited_ms > 150, f"the 5th call at 4/s should wait ~250ms, waited {waited_ms:.0f}ms"
    assert stats.jsonrpc_calls == 5
    assert stats.throttled_calls == 1
    assert stats.rejected_calls == 0
    assert stats.total_delay_ms > 150
    assert stats.max_delay_ms == pytest.approx(stats.total_delay_ms)
    assert len(upstream.received) == 5, "a delayed call is still forwarded and still billed"


def test_reject_mode_answers_minus_32005_and_never_reaches_the_upstream(upstream):
    """The other failure mode: the provider refuses. This is what collapses
    an agent's success rate, and it costs the provider nothing — which is
    why rejected calls are counted apart from delayed ones."""
    with CountingProxy(upstream.url, rate_limit_rps=2, burst=2, limit_mode="reject") as proxy:
        _call(proxy.url)
        _call(proxy.url)
        resp = _call(proxy.url)
        stats = proxy.snapshot()

    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == RATE_LIMIT_ERROR_CODE
    assert resp.json()["error"]["message"] == "limit exceeded"
    assert len(upstream.received) == 2, "the refused call must not be forwarded"
    assert stats.jsonrpc_calls == 3, "it is still a call the agent made"
    assert stats.throttled_calls == 1
    assert stats.rejected_calls == 1
    assert stats.total_delay_ms == 0.0


def test_a_rejected_batch_answers_one_error_per_call_with_matching_ids(upstream):
    batch = [
        {"jsonrpc": "2.0", "id": 7, "method": "eth_getCode", "params": []},
        {"jsonrpc": "2.0", "id": 8, "method": "eth_getCode", "params": []},
    ]
    with CountingProxy(upstream.url, rate_limit_rps=1, burst=1, limit_mode="reject") as proxy:
        resp = requests.post(proxy.url, json=batch, timeout=5)
        stats = proxy.snapshot()

    body = resp.json()
    assert [e["id"] for e in body] == [7, 8]
    assert all(e["error"]["code"] == RATE_LIMIT_ERROR_CODE for e in body)
    assert stats.rejected_calls == 2, "a batch of M costs M tokens, not one"
    assert not upstream.received


def test_reset_clears_the_throttling_counters_too(upstream):
    with CountingProxy(upstream.url, rate_limit_rps=1, burst=1, limit_mode="reject") as proxy:
        _call(proxy.url)
        _call(proxy.url)  # refused
        assert proxy.snapshot().rejected_calls == 1
        proxy.reset()
        stats = proxy.snapshot()

    assert stats.rejected_calls == 0
    assert stats.throttled_calls == 0
    assert stats.total_delay_ms == 0.0
    assert stats.max_delay_ms == 0.0


def test_an_unknown_limit_mode_is_refused_at_construction():
    """Better here than three hours into a sweep that silently limited
    nothing."""
    with pytest.raises(ValueError, match="limit_mode"):
        CountingProxy("http://127.0.0.1:9", rate_limit_rps=10, limit_mode="drop")
