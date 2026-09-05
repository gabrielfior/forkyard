import csv
import json

import pytest

import cost_model
from cost_model import (
    CU_BY_METHOD,
    DEFAULT_CU,
    average_cu,
    cost_for,
    cu_for,
    estimate_calls,
    is_priced,
    report,
    usd_per_1k_agent_runs,
)


def test_cu_for_returns_the_published_weight():
    assert cu_for("eth_call") == 26
    assert cu_for("eth_getStorageAt") == 20
    assert cu_for("eth_chainId") == 0, "free on the published list"


def test_an_unknown_method_falls_back_to_the_labelled_default():
    """A method we could not verify must be priced *and* flagged, never
    silently folded into a number someone then quotes."""
    assert cu_for("eth_getAccountInfo") == DEFAULT_CU
    assert is_priced("eth_getAccountInfo") is False
    assert is_priced("eth_call") is True


def test_the_table_holds_the_methods_the_backends_actually_hammer():
    """These five are what a fork backend spends its upstream budget on;
    if one drops out of the table the whole cost report silently becomes
    an average-of-defaults."""
    for method in [
        "eth_getBalance", "eth_getCode", "eth_getStorageAt",
        "eth_getTransactionCount", "eth_getBlockByNumber",
    ]:
        assert method in CU_BY_METHOD


def test_cost_for_weights_each_method_by_its_own_price():
    """The reason not to bill a flat rate per call: 100 eth_chainIds and
    100 eth_sendRawTransactions are the same call count and nowhere near
    the same bill."""
    total_cu, usd = cost_for({"eth_call": 2, "eth_chainId": 10}, usd_per_million_cu=1.0)
    assert total_cu == 52  # 2*26 + 10*0
    assert usd == pytest.approx(52 / 1_000_000)


def test_cost_for_scales_with_the_configured_price():
    cu_a, usd_a = cost_for({"eth_getBalance": 1_000_000}, usd_per_million_cu=0.45)
    cu_b, usd_b = cost_for({"eth_getBalance": 1_000_000}, usd_per_million_cu=0.90)
    assert cu_a == cu_b == 20_000_000
    assert usd_b == pytest.approx(2 * usd_a)
    assert usd_a == pytest.approx(9.0)


def test_average_cu_is_call_weighted_not_method_weighted():
    # 9 free calls and 1 expensive one average near zero, not near 13.
    assert average_cu({"eth_chainId": 9, "eth_call": 1}) == pytest.approx(2.6)


def test_average_cu_of_an_empty_breakdown_is_the_default():
    assert average_cu({}) == float(DEFAULT_CU)


def test_a_complete_breakdown_is_priced_exactly():
    est = estimate_calls(30, {"eth_getBalance": 20, "eth_call": 10}, usd_per_million_cu=1.0)
    assert est.basis == "exact"
    assert est.total_cu == 20 * 20 + 10 * 26
    assert est.coverage == 1.0


def test_a_truncated_breakdown_is_extrapolated_and_says_so():
    """`top_methods` keeps only the five busiest methods, so summing it is
    a lower bound. Pricing the uncovered calls at the covered average is a
    better estimate than that bound, but it is an estimate — the label is
    the load-bearing part."""
    top5 = {"eth_getBalance": 30, "eth_getCode": 20, "eth_chainId": 10}  # 60 calls
    est = estimate_calls(100, top5, usd_per_million_cu=1.0)

    covered_cu = 30 * 20 + 20 * 20 + 10 * 0  # 1000 CU over 60 calls -> 16.67 avg
    assert est.basis == "extrapolated"
    assert est.covered_calls == 60
    assert est.coverage == pytest.approx(0.6)
    assert est.total_cu == pytest.approx(covered_cu + 40 * (covered_cu / 60))
    assert est.total_cu > covered_cu, "the lower bound must not be reported as the answer"


def test_the_lower_bound_and_the_extrapolation_bracket_each_other():
    top5 = {"eth_getStorageAt": 50}
    lower_bound, _ = cost_for(top5)
    est = estimate_calls(200, top5)
    assert lower_bound < est.total_cu


def test_unpriced_methods_are_reported_per_row():
    est = estimate_calls(10, {"eth_getAccountInfo": 6, "eth_call": 4})
    assert est.unpriced_methods == ["eth_getAccountInfo"]
    assert est.basis == "exact"


def test_a_row_with_no_calls_does_not_divide_by_zero():
    est = estimate_calls(0, {})
    assert est.total_cu == 0
    assert est.usd == 0
    assert est.coverage == 1.0
    assert est.avg_cu == 0.0


def test_usd_per_1k_agent_runs_divides_the_row_among_its_agents():
    assert usd_per_1k_agent_runs(usd=0.5, num_agents=50) == pytest.approx(10.0)
    assert usd_per_1k_agent_runs(usd=0.5, num_agents=0) == 0.0


def _row(backend: str, agents: int, calls: int, methods: dict[str, int]) -> dict[str, str]:
    return {
        "backend": backend,
        "block_height": "25795072",
        "num_agents": str(agents),
        "episodes": "1",
        "http_requests": str(calls),
        "jsonrpc_calls": str(calls),
        "calls_per_agent": str(calls / agents),
        "upstream_errors": "0",
        "top_methods": json.dumps(methods),
    }


def test_report_prices_both_backends_and_states_the_ratio():
    """The measured 50-agent numbers: forkyard 387 calls against Anvil's
    4,204. Cost has to preserve that gap, not average it away."""
    rows = [
        _row("forkyard", 50, 387, {"eth_getBalance": 100, "eth_getStorageAt": 287}),
        _row("anvil", 50, 4204, {"eth_getBalance": 1500, "eth_getCode": 1500, "eth_getStorageAt": 1204}),
    ]
    text = report(rows, usd_per_million_cu=0.45)

    assert "forkyard" in text and "anvil" in text
    assert "$/1k runs" in text
    assert "1,000 agent runs" in text
    ratio_line = [line for line in text.splitlines() if "x forkyard" in line]
    assert ratio_line, text
    assert "10.9x" in ratio_line[0]


def test_report_flags_extrapolated_rows_and_unpriced_methods():
    rows = [_row("anvil", 10, 778, {"eth_getAccountInfo": 98, "eth_getBalance": 190})]
    text = report(rows, usd_per_million_cu=0.45)

    assert "extrapolated" in text
    assert "LOWER BOUND" in text
    assert "eth_getAccountInfo" in text
    assert f"DEFAULT_CU={DEFAULT_CU}" in text


def test_report_does_not_claim_extrapolation_when_the_breakdown_is_complete():
    rows = [_row("forkyard", 5, 20, {"eth_getBalance": 20})]
    text = report(rows, usd_per_million_cu=0.45)

    assert "exact" in text
    assert "extrapolated" not in text


def test_cli_reads_an_upstream_csv_and_prints_costs(tmp_path, monkeypatch, capsys):
    """End to end over the real file format `run_benchmark.py
    --count-upstream` writes, so a column rename there fails here."""
    from run_benchmark import UPSTREAM_FIELDS

    path = tmp_path / "results.upstream.csv"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=UPSTREAM_FIELDS)
        writer.writeheader()
        writer.writerow(_row("forkyard", 10, 37, {"eth_getBalance": 9, "eth_getCode": 9}))
        writer.writerow(_row("anvil", 10, 778, {"eth_getBalance": 190, "eth_getCode": 190}))

    monkeypatch.setattr("sys.argv", ["cost_model.py", str(path), "--usd-per-million-cu", "0.45"])
    cost_model.main()

    out = capsys.readouterr().out
    assert "forkyard" in out and "anvil" in out
    assert "0.45 per million compute units" in out


def test_cli_exits_nonzero_on_an_empty_csv(tmp_path, monkeypatch):
    path = tmp_path / "empty.upstream.csv"
    path.write_text("backend,block_height,num_agents,episodes,jsonrpc_calls,top_methods\n")
    monkeypatch.setattr("sys.argv", ["cost_model.py", str(path)])
    with pytest.raises(SystemExit):
        cost_model.main()
