import os

import pandas as pd

from plot_results import plot_action_latency, plot_total_time_vs_agents


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"backend": "forkyard", "block_height": 20_000_000, "num_agents": 1, "agent_id": -1, "action": "__total__", "elapsed_ms": 100.0, "ok": True, "error": ""},
            {"backend": "forkyard", "block_height": 20_000_000, "num_agents": 2, "agent_id": -1, "action": "__total__", "elapsed_ms": 150.0, "ok": True, "error": ""},
            {"backend": "anvil", "block_height": 20_000_000, "num_agents": 1, "agent_id": -1, "action": "__total__", "elapsed_ms": 300.0, "ok": True, "error": ""},
            {"backend": "anvil", "block_height": 20_000_000, "num_agents": 2, "agent_id": -1, "action": "__total__", "elapsed_ms": 600.0, "ok": True, "error": ""},
            {"backend": "forkyard", "block_height": 20_000_000, "num_agents": 1, "agent_id": 0, "action": "transfer", "elapsed_ms": 5.0, "ok": True, "error": ""},
            {"backend": "anvil", "block_height": 20_000_000, "num_agents": 1, "agent_id": 0, "action": "transfer", "elapsed_ms": 8.0, "ok": True, "error": ""},
        ]
    )


def test_plot_total_time_vs_agents_writes_a_png(tmp_path):
    out = tmp_path / "total.png"
    plot_total_time_vs_agents(_sample_df(), str(out))
    assert out.exists()
    assert out.stat().st_size > 0


def test_plot_action_latency_writes_a_png(tmp_path):
    out = tmp_path / "latency.png"
    plot_action_latency(_sample_df(), str(out))
    assert out.exists()
    assert out.stat().st_size > 0
