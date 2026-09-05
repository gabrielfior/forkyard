from pathlib import Path

import bench_warmstart
from bench_warmstart import FIELDS, clear_dir, foundry_cache_dir


class _FakeBackend:
    def web3(self):
        class Eth:
            def estimate_gas(self, tx):
                return 21_000

            def get_balance(self, address):
                return 0

        class W3:
            eth = Eth()

        return W3()

    def discard(self):
        pass


class _FakeProcess:
    pid = 1

    def terminate(self):
        pass

    def wait(self, timeout=None):
        return 0

    def kill(self):
        pass


def test_foundry_cache_is_cleared_per_block_not_wholesale():
    """Clearing all of ~/.foundry/cache would throw away the user's own
    unrelated work; the benchmark only owns the block it pinned."""
    path = foundry_cache_dir(25_795_072)

    assert path.name == "25795072"
    assert path.parent.name == "mainnet"
    assert "cache" in path.parts and ".foundry" in path.parts


def test_clear_dir_is_a_noop_on_a_missing_directory(tmp_path):
    clear_dir(tmp_path / "never-existed")  # the cold run must not need one to exist


def test_clear_dir_removes_a_populated_cache(tmp_path):
    block_dir = tmp_path / "1"
    block_dir.mkdir()
    (block_dir / "25795072.json").write_text("{}")

    clear_dir(tmp_path)

    assert not tmp_path.exists()


def test_forkyard_run_never_inherits_the_cache_kill_switch(monkeypatch):
    """The measurement pass exports FORKYARD_CACHE_DISABLED=1 so every other
    benchmark stays cold-vs-cold. Leaking it in here would make the warm row
    a second cold row — the finding would read as "persistence does nothing"."""
    captured: dict[str, dict[str, str]] = {}

    monkeypatch.setenv("FORKYARD_CACHE_DISABLED", "1")
    monkeypatch.setattr(
        bench_warmstart.subprocess, "Popen",
        lambda argv, env=None, **kw: (captured.__setitem__("env", env), _FakeProcess())[1],
    )
    monkeypatch.setattr(bench_warmstart, "_wait_for_forkyard", lambda url, **kw: None)
    monkeypatch.setattr(bench_warmstart, "_terminate", lambda process: None)
    monkeypatch.setattr(bench_warmstart, "ForkyardBackend", lambda **kw: _FakeBackend())

    bench_warmstart.run_forkyard("http://rpc", 1, 1, ["0xabc"], Path("/tmp/x"))

    assert "FORKYARD_CACHE_DISABLED" not in captured["env"]
    assert captured["env"]["FORKYARD_CACHE_DIR"] == "/tmp/x"


def test_forkyard_is_stopped_politely_so_its_cache_actually_lands(monkeypatch):
    """SIGKILL would skip the save path, leaving the warm run cold."""
    terminated: list[object] = []

    monkeypatch.setattr(
        bench_warmstart.subprocess, "Popen", lambda argv, env=None, **kw: _FakeProcess()
    )
    monkeypatch.setattr(bench_warmstart, "_wait_for_forkyard", lambda url, **kw: None)
    monkeypatch.setattr(bench_warmstart, "_terminate", lambda process: terminated.append(process))
    monkeypatch.setattr(bench_warmstart, "ForkyardBackend", lambda **kw: _FakeBackend())

    bench_warmstart.run_forkyard("http://rpc", 1, 2, ["0xabc"], Path("/tmp/x"))

    assert len(terminated) == 1, "the process must go through _terminate, not be leaked or killed"


def test_anvil_runs_with_foundrys_cache_enabled(monkeypatch):
    """Every other benchmark passes --no-storage-caching. Here the cache is
    the subject, so this arm must opt back in or the warm row is meaningless."""
    seen: list[bool] = []

    def fake_anvil(port, fork_url, block, rpc_cache=False):
        seen.append(rpc_cache)
        return _FakeBackend()

    monkeypatch.setattr(bench_warmstart, "AnvilBackend", fake_anvil)

    bench_warmstart.run_anvil("http://rpc", 1, 3, ["0xabc"])

    assert seen == [True, True, True]


def test_fields_cover_both_the_cost_and_the_outcome_columns():
    assert FIELDS[:4] == ["backend", "condition", "agents", "contracts"]
    assert "jsonrpc_calls" in FIELDS
    assert FIELDS[-2:] == ["ok", "error"]
