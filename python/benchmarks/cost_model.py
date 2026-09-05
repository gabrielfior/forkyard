"""Prices `<out>.upstream.csv`'s call counts in provider compute units and
dollars, weighting each method by what the provider charges for it.

Weights and price are a hardcoded snapshot of Alchemy's published tables
(compute-unit reference and pay-as-you-go pricing, both read 2026-09-04),
chosen because they are public; other providers meter differently. Provider
tables drift, so re-check before quoting a dollar figure."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from typing import Literal

from pydantic import BaseModel, Field, NonNegativeFloat, NonNegativeInt

# Alchemy compute units per JSON-RPC call, limited to the methods a fork
# backend plausibly issues.
# https://www.alchemy.com/docs/reference/compute-unit-costs, read 2026-09-04.
CU_BY_METHOD: dict[str, int] = {
    # State reads: the bulk of every fork's traffic.
    "eth_getBalance": 20,
    "eth_getCode": 20,
    "eth_getStorageAt": 20,
    "eth_getTransactionCount": 20,
    "eth_getProof": 20,
    # Execution.
    "eth_call": 26,
    "eth_estimateGas": 20,
    "eth_sendRawTransaction": 40,
    # Chain/block metadata.
    "eth_blockNumber": 10,
    "eth_getBlockByNumber": 20,
    "eth_getBlockByHash": 20,
    "eth_getTransactionByHash": 20,
    "eth_getTransactionReceipt": 20,
    "eth_getBlockReceipts": 20,
    "eth_gasPrice": 20,
    "eth_feeHistory": 10,
    "eth_maxPriorityFeePerGas": 10,
    "eth_getLogs": 60,
    # Free on the published list, though still counted against throughput
    # by rpc_proxy.py's limiter — the two limits are separate.
    "eth_chainId": 0,
    "eth_syncing": 0,
    "net_version": 0,
    "eth_accounts": 10,
    "web3_clientVersion": 20,
}

# Charged for any method not in the table, and always reported as unpriced.
# 20 is the modal weight of the priced state reads. It errs against
# forkyard's claim: Anvil's `eth_getAccountInfo` (~13% of its measured
# calls, unpublished) replaces three 20-CU reads, so 20 understates it.
DEFAULT_CU = 20

# A list price, not yours — override with --usd-per-million-cu.
# https://www.alchemy.com/pricing, read 2026-09-04.
DEFAULT_USD_PER_MILLION_CU = 0.45


def cu_for(method: str) -> int:
    """Unknown methods get DEFAULT_CU; `is_priced()` says which those are."""
    return CU_BY_METHOD.get(method, DEFAULT_CU)


def is_priced(method: str) -> bool:
    return method in CU_BY_METHOD


def cost_for(
    by_method: dict[str, int], usd_per_million_cu: float = DEFAULT_USD_PER_MILLION_CU
) -> tuple[int, float]:
    """(total compute units, USD) for a method→count breakdown."""
    total_cu = sum(cu_for(method) * count for method, count in by_method.items())
    return total_cu, total_cu * usd_per_million_cu / 1_000_000


def average_cu(by_method: dict[str, int]) -> float:
    """Call-weighted mean CU. Used to extrapolate a truncated breakdown: the
    five busiest methods are a biased-but-close sample of the whole."""
    calls = sum(by_method.values())
    if calls == 0:
        return float(DEFAULT_CU)
    return sum(cu_for(m) * c for m, c in by_method.items()) / calls


class CostEstimate(BaseModel):
    """One combination's cost, plus how much of it was actually measured."""

    total_cu: NonNegativeFloat
    usd: NonNegativeFloat
    # "exact" when the breakdown accounts for every call — the difference
    # matters more than the number does.
    basis: Literal["exact", "extrapolated"]
    jsonrpc_calls: NonNegativeInt
    # Calls the breakdown named; under `top_methods`, never the total.
    covered_calls: NonNegativeInt
    avg_cu: NonNegativeFloat
    unpriced_methods: list[str] = Field(default_factory=list)

    @property
    def coverage(self) -> float:
        return self.covered_calls / self.jsonrpc_calls if self.jsonrpc_calls else 1.0


def estimate_calls(
    jsonrpc_calls: int,
    by_method: dict[str, int],
    usd_per_million_cu: float = DEFAULT_USD_PER_MILLION_CU,
) -> CostEstimate:
    """Cost a combination given its total call count and a possibly
    *truncated* method breakdown — `top_methods` holds only the five busiest
    methods, so summing it is a lower bound. Uncovered calls are priced at
    the covered calls' average weight and the row labelled "extrapolated"."""
    covered_calls = sum(by_method.values())
    covered_cu, _ = cost_for(by_method, usd_per_million_cu)
    unpriced = sorted(m for m in by_method if not is_priced(m))

    if covered_calls >= jsonrpc_calls:
        total_cu = float(covered_cu)
        basis = "exact"
    else:
        avg = average_cu(by_method)
        total_cu = covered_cu + (jsonrpc_calls - covered_calls) * avg
        basis = "extrapolated"

    calls = max(jsonrpc_calls, covered_calls)
    return CostEstimate(
        total_cu=total_cu,
        usd=total_cu * usd_per_million_cu / 1_000_000,
        basis=basis,
        jsonrpc_calls=calls,
        covered_calls=covered_calls,
        avg_cu=total_cu / calls if calls else 0.0,
        unpriced_methods=unpriced,
    )


def read_upstream_csv(path: str) -> list[dict[str, str]]:
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def usd_per_1k_agent_runs(usd: float, num_agents: int) -> float:
    """An *agent run* is one agent's whole workload in a combination, i.e.
    its `episodes` acquire→act→discard cycles — so keep `episodes` fixed
    across rows quoted together."""
    if num_agents <= 0:
        return 0.0
    return usd / num_agents * 1000


def _summarise(rows: list[dict[str, object]]) -> list[str]:
    """Per-backend roll-up. Summing before dividing weights the large-N
    combinations more heavily, which is the right way round."""
    lines: list[str] = []
    by_backend: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_backend.setdefault(str(row["backend"]), []).append(row)

    per_1k: dict[str, float] = {}
    for backend, group in sorted(by_backend.items()):
        usd = sum(float(r["usd"]) for r in group)
        cu = sum(float(r["total_cu"]) for r in group)
        agent_runs = sum(int(r["num_agents"]) for r in group)
        rate = usd / agent_runs * 1000 if agent_runs else 0.0
        per_1k[backend] = rate
        lines.append(
            f"  {backend:<9} {cu:>14,.0f} CU  ${usd:>10.4f}  over {agent_runs:>5} agent runs"
            f"  =  ${rate:.2f} / 1,000 agent runs"
        )

    if "anvil" in per_1k and "forkyard" in per_1k and per_1k["forkyard"] > 0:
        ratio = per_1k["anvil"] / per_1k["forkyard"]
        lines.append(f"\n  anvil costs {ratio:.1f}x forkyard per agent run (same workload, same endpoint)")
    return lines


def report(rows: list[dict[str, str]], usd_per_million_cu: float) -> str:
    out: list[str] = []
    priced: list[dict[str, object]] = []
    any_extrapolated = False
    unpriced_seen: set[str] = set()

    header = (
        f"{'backend':<9} {'block':>10} {'agents':>7} {'eps':>4} {'calls':>8} "
        f"{'CU':>12} {'USD':>10} {'$/1k runs':>10} {'basis':>13} {'cover':>6}"
    )
    out.append(header)
    out.append("-" * len(header))

    for row in rows:
        by_method = json.loads(row.get("top_methods") or "{}")
        calls = int(row["jsonrpc_calls"])
        num_agents = int(row["num_agents"])
        est = estimate_calls(calls, by_method, usd_per_million_cu)
        any_extrapolated |= est.basis == "extrapolated"
        unpriced_seen.update(est.unpriced_methods)
        out.append(
            f"{row['backend']:<9} {int(row['block_height']):>10} {num_agents:>7} "
            f"{int(row.get('episodes', 1)):>4} {est.jsonrpc_calls:>8} "
            f"{est.total_cu:>12,.0f} {est.usd:>10.5f} "
            f"{usd_per_1k_agent_runs(est.usd, num_agents):>10.2f} "
            f"{est.basis:>13} {est.coverage:>5.0%}"
        )
        priced.append(
            {
                "backend": row["backend"],
                "num_agents": num_agents,
                "total_cu": est.total_cu,
                "usd": est.usd,
            }
        )

    out.append("")
    out.append(f"At ${usd_per_million_cu:g} per million compute units:")
    out.extend(_summarise(priced))
    out.append("")
    if any_extrapolated:
        out.append(
            "note: rows marked 'extrapolated' had a truncated method breakdown — "
            "`top_methods` holds only the five busiest methods, so the uncovered "
            "calls were priced at the covered calls' average weight. Summing "
            "`top_methods` alone would instead give a strict LOWER BOUND "
            "(the 'cover' column is how much of the call count it saw)."
        )
    if unpriced_seen:
        out.append(
            "note: not on the provider's published CU list, charged at the "
            f"DEFAULT_CU={DEFAULT_CU} stand-in: {', '.join(sorted(unpriced_seen))}"
        )
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Price a --count-upstream run: turns the JSON-RPC call counts in "
            "<out>.upstream.csv into provider compute units and dollars, per "
            "(backend, block, agent count) combination and as $/1,000 agent "
            "runs per backend. Weights come from a hardcoded snapshot of "
            "Alchemy's published compute-unit table (see the module docstring "
            "for date and caveats); the dollar rate is a flag because yours "
            "differs."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Caveat carried into every row: `top_methods` in the input holds "
            "only the FIVE busiest methods. Where that does not cover the "
            "row's `jsonrpc_calls`, the remainder is priced at the covered "
            "calls' average CU and the row is labelled 'extrapolated'."
        ),
    )
    parser.add_argument("upstream_csv", help="an <out>.upstream.csv from run_benchmark.py --count-upstream")
    parser.add_argument(
        "--usd-per-million-cu", type=float, default=DEFAULT_USD_PER_MILLION_CU,
        help=f"provider price per million compute units (default {DEFAULT_USD_PER_MILLION_CU}, "
             "Alchemy pay-as-you-go list price as of 2026-09-04)",
    )
    args = parser.parse_args()

    rows = read_upstream_csv(args.upstream_csv)
    if not rows:
        print(f"{args.upstream_csv} has no rows", file=sys.stderr)
        raise SystemExit(1)
    print(report(rows, args.usd_per_million_cu))


if __name__ == "__main__":
    main()
