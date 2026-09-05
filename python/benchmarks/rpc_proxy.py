"""A counting JSON-RPC proxy: forwards every request upstream unchanged and
tallies what went past — per HTTP request and per JSON-RPC call, since a
batch is one of the former and many of the latter. With `rate_limit_rps`
set it also enforces a provider-style budget, as latency (`delay`) or as
refusals (`reject`)."""

from __future__ import annotations

import argparse
import json
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

import requests
from pydantic import BaseModel, Field, NonNegativeFloat, NonNegativeInt

# What Infura, Alchemy and QuickNode all return for throughput exhaustion.
RATE_LIMIT_ERROR_CODE = -32005


class ProxyStats(BaseModel):
    """A snapshot of the counters. Everything stays 0 when no rate limit is
    configured."""

    http_requests: NonNegativeInt = 0
    jsonrpc_calls: NonNegativeInt = 0
    by_method: dict[str, NonNegativeInt] = Field(default_factory=dict)
    upstream_errors: NonNegativeInt = 0
    throttled_calls: NonNegativeInt = 0
    # The subset of `throttled_calls` that never reached the upstream: a
    # delayed call still costs money and returns data, a rejected one does
    # neither and is the only one that sinks an agent's success rate.
    rejected_calls: NonNegativeInt = 0
    # Summed across handler threads, so *not* wall-clock: the excess over
    # the run's duration is how oversubscribed the quota was.
    total_delay_ms: NonNegativeFloat = 0.0
    max_delay_ms: NonNegativeFloat = 0.0


class TokenBucket:
    """A token bucket that can be *reserved* past empty, modelling a
    provider queue rather than a drop. The debt is charged under the lock
    before the caller sleeps, so concurrent callers serialise in arrival
    order instead of stampeding the same refill. `clock` is injectable so
    the maths can be tested without sleeping."""

    def __init__(
        self,
        rate_per_s: float,
        burst: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        if rate_per_s <= 0:
            raise ValueError(f"rate_per_s must be positive, got {rate_per_s}")
        # Default burst = one second of budget, which is what a "Q calls
        # per second" plan feels like.
        self.rate = float(rate_per_s)
        self.burst = float(burst) if burst is not None else float(rate_per_s)
        if self.burst <= 0:
            raise ValueError(f"burst must be positive, got {burst}")
        self._clock = clock
        self._tokens = self.burst
        self._updated = clock()
        self._lock = threading.Lock()

    def _refill_locked(self) -> None:
        now = self._clock()
        # max(0.0, ...) only guards a non-monotonic injected test clock.
        self._tokens = min(self.burst, self._tokens + max(0.0, now - self._updated) * self.rate)
        self._updated = now

    def reserve(self, tokens: float) -> float:
        """Charge `tokens`, return the seconds to wait. The balance may go
        negative — that negative balance *is* the queue."""
        with self._lock:
            self._refill_locked()
            self._tokens -= tokens
            deficit = -self._tokens
            return deficit / self.rate if deficit > 0 else 0.0

    def try_consume(self, tokens: float) -> bool:
        """Charge `tokens` only if available now. A batch bigger than
        `burst` therefore never succeeds, as under a real per-window quota."""
        with self._lock:
            self._refill_locked()
            if self._tokens < tokens:
                return False
            self._tokens -= tokens
            return True

    def available(self) -> float:
        with self._lock:
            self._refill_locked()
            return self._tokens


class CountingProxy:
    """Owns the listening socket and the counters. Start one per sweep and
    call `snapshot()`/`reset()` around each combination. `rate_limit_rps` is
    in JSON-RPC *calls* per second, so a batch of M costs M tokens."""

    def __init__(
        self,
        upstream: str,
        port: int = 0,
        host: str = "127.0.0.1",
        rate_limit_rps: float | None = None,
        burst: float | None = None,
        limit_mode: str = "delay",
    ):
        if limit_mode not in ("delay", "reject"):
            raise ValueError(f"limit_mode must be 'delay' or 'reject', got {limit_mode!r}")
        self.upstream = upstream
        self.rate_limit_rps = rate_limit_rps
        self.limit_mode = limit_mode
        # None on the unlimited path, so counting-only sweeps skip the
        # bucket, the clock read and the sleep entirely.
        self._limiter = TokenBucket(rate_limit_rps, burst) if rate_limit_rps else None
        self._lock = threading.Lock()
        self._http_requests = 0
        self._jsonrpc_calls = 0
        self._by_method: Counter[str] = Counter()
        self._upstream_errors = 0
        self._throttled_calls = 0
        self._rejected_calls = 0
        self._total_delay_ms = 0.0
        self._max_delay_ms = 0.0
        # Pooled across handler threads; without it every forwarded call
        # pays a TLS handshake and the proxy dominates what it measures.
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
                parsed = _parse_body(body)
                methods = _methods_of(parsed)
                # An unparseable body still costs the provider one call.
                cost = max(len(methods), 1)
                proxy._count(methods, cost)
                if proxy._limiter is not None and not proxy._admit(cost):
                    self._respond(429, _limit_exceeded_payload(parsed))
                    return
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

    def _admit(self, cost: int) -> bool:
        """Whether a request worth `cost` calls may be forwarded; in `delay`
        mode blocks this handler thread first. Safe because
        ThreadingHTTPServer gives every connection its own thread."""
        assert self._limiter is not None
        if self.limit_mode == "reject":
            if self._limiter.try_consume(cost):
                return True
            with self._lock:
                self._throttled_calls += cost
                self._rejected_calls += cost
            return False

        wait_s = self._limiter.reserve(cost)
        if wait_s > 0:
            time.sleep(wait_s)
            delay_ms = wait_s * 1000
            with self._lock:
                self._throttled_calls += cost
                self._total_delay_ms += delay_ms
                self._max_delay_ms = max(self._max_delay_ms, delay_ms)
        return True

    def _count(self, methods: list[str], cost: int) -> None:
        with self._lock:
            self._http_requests += 1
            self._jsonrpc_calls += cost
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
                throttled_calls=self._throttled_calls,
                rejected_calls=self._rejected_calls,
                total_delay_ms=self._total_delay_ms,
                max_delay_ms=self._max_delay_ms,
            )

    def reset(self) -> None:
        """Zero the counters between combinations. The token bucket is
        deliberately *not* refilled: a fresh full bucket would credit
        whichever backend happens to run second."""
        with self._lock:
            self._http_requests = 0
            self._jsonrpc_calls = 0
            self._by_method.clear()
            self._upstream_errors = 0
            self._throttled_calls = 0
            self._rejected_calls = 0
            self._total_delay_ms = 0.0
            self._max_delay_ms = 0.0

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._session.close()

    def __enter__(self) -> "CountingProxy":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


def _parse_body(body: bytes) -> object:
    try:
        return json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None


def _methods_of(parsed: object) -> list[str]:
    if isinstance(parsed, list):  # JSON-RPC batch
        return [c.get("method", "?") for c in parsed if isinstance(c, dict)]
    if isinstance(parsed, dict):
        return [parsed.get("method", "?")]
    return []


def _limit_exceeded_payload(parsed: object) -> bytes:
    """Mirror the request's shape: a batch gets one error per call, carrying
    the original ids, so a client correlating by id loses nothing."""
    error = {"code": RATE_LIMIT_ERROR_CODE, "message": "limit exceeded"}
    if isinstance(parsed, list):
        return json.dumps(
            [
                {"jsonrpc": "2.0", "id": c.get("id") if isinstance(c, dict) else None, "error": error}
                for c in parsed
            ]
        ).encode()
    rpc_id = parsed.get("id") if isinstance(parsed, dict) else None
    return json.dumps({"jsonrpc": "2.0", "id": rpc_id, "error": error}).encode()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True)
    parser.add_argument("--port", type=int, default=18700)
    parser.add_argument(
        "--rate-limit-rps", type=float, default=None,
        help="JSON-RPC calls per second allowed upstream (a batch of M costs M). "
             "Unset means count only, throttle nothing.",
    )
    parser.add_argument(
        "--burst", type=float, default=None,
        help="bucket capacity in calls (default: one second of the rate).",
    )
    parser.add_argument(
        "--limit-mode", choices=["delay", "reject"], default="delay",
        help="what happens over budget: 'delay' queues the call (the client "
             "sees latency), 'reject' answers JSON-RPC -32005 / HTTP 429 (the "
             "client sees a failure). Providers do both, and they look nothing "
             "alike in the results.",
    )
    args = parser.parse_args()
    proxy = CountingProxy(
        args.upstream, args.port,
        rate_limit_rps=args.rate_limit_rps, burst=args.burst, limit_mode=args.limit_mode,
    ).start()
    limit = f" @ {args.rate_limit_rps}/s ({args.limit_mode})" if args.rate_limit_rps else ""
    print(f"counting proxy on {proxy.url} -> {args.upstream}{limit}; Ctrl-C for totals")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        stats = proxy.snapshot()
        print(stats.model_dump_json(indent=2))
    finally:
        proxy.stop()


if __name__ == "__main__":
    main()
