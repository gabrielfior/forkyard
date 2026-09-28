import subprocess
import sys

import pytest

import bench_latency
from bench_latency import marker_for, wait_ready


def test_every_repeat_snapshots_different_state():
    # The snapshot store is content-addressed: two repeats with the same
    # marker would make the second snapshot a no-op write.
    assert len({marker_for(r) for r in range(-1, 10)}) == 11


def test_wait_ready_fails_fast_when_the_process_dies():
    process = subprocess.Popen([sys.executable, "-c", "raise SystemExit(3)"])
    process.wait()
    with pytest.raises(RuntimeError, match="exited with 3"):
        wait_ready("http://127.0.0.1:1", process, timeout_s=5)


def test_wait_ready_returns_the_session_it_opened(monkeypatch):
    class Response:
        ok = True

        def json(self):
            return {"session_id": 42}

    monkeypatch.setattr(bench_latency.requests, "post", lambda url, timeout: Response())

    class Alive:
        def poll(self):
            return None

    assert wait_ready("http://h:1", Alive()) == "http://h:1/session/42"
