from backend import erc20_balance_slot


def test_erc20_balance_slot_matches_known_dai_vector():
    holder = "0x0000000000000000000000000000000000000001"
    slot = erc20_balance_slot(holder, mapping_slot=2)
    assert isinstance(slot, bytes)
    assert len(slot) == 32
    # Re-derived here rather than copied, so a wrong formula fails too.
    from eth_utils import keccak
    key = int(holder, 16).to_bytes(32, "big")
    mapping_slot_bytes = (2).to_bytes(32, "big")
    expected = keccak(key + mapping_slot_bytes)
    assert slot == expected


def test_anvil_uses_foundrys_cache_by_default(monkeypatch):
    """Without it Anvil serves fork state from ~/.foundry/cache written by
    earlier runs, while forkyard refills from the endpoint every start."""
    import backend as backend_module

    spawned: dict[str, list[str]] = {}

    class FakeProcess:
        pid = 1234

        def terminate(self):
            pass

        def wait(self, timeout=None):
            return 0

        def kill(self):
            pass

    def fake_popen(argv, *args, **kwargs):
        spawned["argv"] = argv
        return FakeProcess()

    monkeypatch.setattr(backend_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(backend_module.AnvilBackend, "_wait_until_ready", lambda self, timeout: None)

    backend_module.AnvilBackend(8545, "http://rpc.example", 20_000_000)

    assert "--no-storage-caching" not in spawned["argv"]
    assert spawned["argv"][:2] == ["anvil", "--fork-url"]


def test_anvil_rpc_cache_can_be_disabled_to_measure_a_cold_start(monkeypatch):
    import backend as backend_module

    spawned: dict[str, list[str]] = {}

    class FakeProcess:
        pid = 1234

        def terminate(self):
            pass

        def wait(self, timeout=None):
            return 0

        def kill(self):
            pass

    monkeypatch.setattr(
        backend_module.subprocess, "Popen",
        lambda argv, *a, **k: (spawned.__setitem__("argv", argv), FakeProcess())[1],
    )
    monkeypatch.setattr(backend_module.AnvilBackend, "_wait_until_ready", lambda self, timeout: None)

    backend_module.AnvilBackend(8545, "http://rpc.example", 20_000_000, rpc_cache=False)

    assert "--no-storage-caching" in spawned["argv"]
