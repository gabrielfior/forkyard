"""A counting JSON-RPC proxy: forwards every request upstream unchanged and
tallies what went past.

This is what measures forkyard's actual claim. Wall-clock says which
backend answers an agent faster; only an upstream count says whether N
agents cost the provider N forks' worth of traffic or one. Anvil gives
each agent its own independent fetch cache, so that traffic should grow
with the agent count; forkyard shares one, so past the first agent it
should barely move.

Counts are per HTTP request *and* per JSON-RPC call, because a batched
request is one of the former and many of the latter — and providers bill
the latter.
"""

from __future__ import annotations

import argparse
import json
import threading
from collections import Counter
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests


@dataclass
class ProxyStats:
    http_requests: int = 0
    jsonrpc_calls: int = 0
    by_method: dict[str, int] = field(default_factory=dict)
    upstream_errors: int = 0


class CountingProxy:
    """Owns the listening socket and the counters. Start one per sweep and
    call `snapshot()`/`reset()` around each (backend, agent-count)
    combination."""

    def __init__(self, upstream: str, port: int = 0, host: str = "127.0.0.1"):
        self.upstream = upstream
        self._lock = threading.Lock()
        self._http_requests = 0
        self._jsonrpc_calls = 0
        self._by_method: Counter[str] = Counter()
        self._upstream_errors = 0
        # One pooled session shared by every handler thread: without it each
        # forwarded call would pay a fresh TLS handshake to the upstream and
        # the proxy itself would dominate the latency it is measuring.
        self._session = requests.Session()
        self._session.mount(
            "https://", requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=256)
        )
        self._session.mount(
            "http://", requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=256)
        )

        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_POST(self):  # noqa: N802 — BaseHTTPRequestHandler's naming
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                proxy._count(body)
                try:
                    resp = proxy._session.post(
                        proxy.upstream,
                        data=body,
                        headers={"content-type": "application/json"},
                        timeout=30,
                    )
                except requests.RequestException as e:
                    proxy._count_upstream_error()
                    payload = json.dumps(
                        {"jsonrpc": "2.0", "id": None, "error": {"code": -32000, "message": repr(e)[:200]}}
                    ).encode()
                    self._respond(502, payload)
                    return
                self._respond(resp.status_code, resp.content)

            def _respond(self, status: int, payload: bytes) -> None:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass  # a sweep makes tens of thousands of these

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self.port = self._server.server_address[1]
        self.url = f"http://{host}:{self.port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def _count(self, body: bytes) -> None:
        methods: list[str] = []
        try:
            parsed = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            parsed = None
        if isinstance(parsed, list):  # JSON-RPC batch
            methods = [c.get("method", "?") for c in parsed if isinstance(c, dict)]
        elif isinstance(parsed, dict):
            methods = [parsed.get("method", "?")]
        with self._lock:
            self._http_requests += 1
            self._jsonrpc_calls += max(len(methods), 1)
            self._by_method.update(methods)

    def _count_upstream_error(self) -> None:
        with self._lock:
            self._upstream_errors += 1

    def start(self) -> "CountingProxy":
        self._thread.start()
        return self

    def snapshot(self) -> ProxyStats:
        with self._lock:
            return ProxyStats(
                http_requests=self._http_requests,
                jsonrpc_calls=self._jsonrpc_calls,
                by_method=dict(self._by_method),
                upstream_errors=self._upstream_errors,
            )

    def reset(self) -> None:
        with self._lock:
            self._http_requests = 0
            self._jsonrpc_calls = 0
            self._by_method.clear()
            self._upstream_errors = 0

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._session.close()

    def __enter__(self) -> "CountingProxy":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--port", type=int, default=18700)
    args = parser.parse_args()
    proxy = CountingProxy(args.upstream, args.port).start()
    print(f"counting proxy on {proxy.url} -> {args.upstream}; Ctrl-C for totals")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        stats = proxy.snapshot()
        print(json.dumps(stats.__dict__, indent=2))
    finally:
        proxy.stop()


if __name__ == "__main__":
    main()
