import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from rpc_proxy import CountingProxy


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
