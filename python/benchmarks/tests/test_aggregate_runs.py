import pandas as pd
import pytest

from aggregate_runs import load_reps, summarize, totals_by_group


def _frame(backend: str, agents: int, elapsed: float) -> pd.DataFrame:
    return pd.DataFrame([
        {"backend": backend, "num_agents": agents, "action": "transfer", "elapsed_ms": 1.0},
        {"backend": backend, "num_agents": agents, "action": "__total__", "elapsed_ms": elapsed},
    ])


def test_only_total_rows_feed_the_summary():
    series = totals_by_group([_frame("forkyard", 10, 5000.0)], ["backend", "num_agents"], "elapsed_ms")

    assert series == {("forkyard", 10): [5000.0]}


def test_summary_reports_median_and_the_full_range():
    series = {("forkyard", 10): [1000.0, 4000.0, 2000.0]}

    row = summarize(series, scale=1000.0).iloc[0]  # ms -> s, as the CLI does

    assert row["n"] == 3
    assert row["median"] == 2.0
    assert row["min"] == 1.0 and row["max"] == 4.0
    assert row["spread_x"] == 4.0


def test_spread_is_what_says_whether_a_number_is_quotable():
    """A 4x spread across repetitions is the whole reason this script exists:
    a single run of these sweeps has been off by that much."""
    tight = summarize({("a", 1): [10.0, 10.5, 10.2]}).iloc[0]
    loose = summarize({("a", 1): [10.0, 70.0, 12.0]}).iloc[0]

    assert tight["spread_x"] < 1.1
    assert loose["spread_x"] > 5


def test_warmup_run_is_never_counted(tmp_path):
    """It is the run that fills both persistent caches, so it is a cold
    number wearing a warm label."""
    _frame("forkyard", 1, 9999.0).to_csv(tmp_path / "core_warmup.csv", index=False)
    _frame("forkyard", 1, 100.0).to_csv(tmp_path / "core_rep1.csv", index=False)

    frames = load_reps(tmp_path, "core")

    assert len(frames) == 1
    assert frames[0][frames[0].action == "__total__"].elapsed_ms.iloc[0] == 100.0


def test_a_missing_sweep_is_an_empty_list_not_a_crash(tmp_path):
    assert load_reps(tmp_path, "nothing-here") == []


def test_zero_median_does_not_divide_by_zero():
    row = summarize({("a", 1): [0.0, 0.0]}).iloc[0]

    assert row["spread_x"] == pytest.approx(float("inf"))
