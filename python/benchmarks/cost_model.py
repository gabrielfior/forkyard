"""What a benchmark run cost the RPC provider, in compute units and dollars.

`rpc_proxy.py` answers "how many calls"; this answers "so what". Providers
do not bill calls uniformly — an `eth_call` is worth more than an
`eth_chainId` and much less than an `eth_sendRawTransaction` — so a raw
call count can flatter or slander a backend depending on which methods it
leans on. Weighting by the provider's own published compute-unit table
turns the call counts from `<out>.upstream.csv` into the number a team
actually budgets against: dollars per thousand agent runs.

Provenance of the numbers below — read this before quoting any figure:

* Source: Alchemy's public "Compute Unit Costs" reference
  (https://www.alchemy.com/docs/reference/compute-unit-costs), read on
  2026-09-04. Alchemy is used because its per-method table is public;
  Infura and QuickNode meter differently (credits/method-classes), so the
  *ratios* here are Alchemy's, not a universal truth.
* Price: Alchemy's pay-as-you-go rate was $0.45 per million CU for the
  first 300M CU/month and $0.40 per million after
  (https://www.alchemy.com/pricing, same date). `--usd-per-million-cu`
  exists because your contract is probably not that rate; the CU totals
  are the durable part of the output, the dollars are a multiplication.
* This is a hardcoded snapshot of a published list, not a live lookup.
  Provider tables drift — treat every number as "as of the date above"
  and re-check before putting a dollar figure in front of anyone.
* Anything not in the table gets `DEFAULT_CU` and is reported as
  *unpriced* rather than silently folded in, so a method we could not
  verify can never masquerade as a measured cost.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import dataclass, field

# Alchemy compute units per JSON-RPC call. Snapshot of the published list
# (see module docstring for source and date). Only methods that actually
# appear in — or plausibly appear in — a fork backend's upstream traffic
# are listed; the point is not to mirror the whole table.
CU_BY_METHOD: dict[str, int] = {
    # State reads: the bulk of every fork's traffic, and the methods the
    # two architectures differ on.
    "eth_getBalance": 20,
    "eth_getCode": 20,
    "eth_getStorageAt": 20,
    "eth_getTransactionCount": 20,
    "eth_getProof": 20,
    # Execution.
    "eth_call": 26,
    "eth_estimateGas": 20,
    "eth_sendRawTransaction": 40,
    # Chain/block metadata: fetched on every fork start.
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
    # Free on the published list (they still count against throughput,
    # which is what rpc_proxy.py's rate limiter models — the two limits
    # are separate and a method can be free on one and not the other).
    "eth_chainId": 0,
    "eth_syncing": 0,
    "net_version": 0,
    "eth_accounts": 10,
    "web3_clientVersion": 20,
}

# Charged for any method not in the table above. 20 is the modal weight of
# the priced state-read methods, so it is a defensible stand-in rather
# than a guess dressed as data — but every estimate that leans on it says
# so. This is not academic: `eth_getAccountInfo` (a combined
# balance+nonce+code read, which Anvil uses against endpoints that support
# it) made up ~13% of Anvil's measured upstream calls and is *not* on the
# published list. Where an endpoint lacks it, Anvil issues the three
# separate reads instead — 60 CU rather than 20 — so pricing it at
# DEFAULT_CU understates Anvil's cost, i.e. errs against forkyard's claim.
DEFAULT_CU = 20

# See the module docstring. Override it: this is a list price, not yours.
DEFAULT_USD_PER_MILLION_CU = 0.45


def cu_for(method: str) -> int:
    """Compute units one call of `method` costs. Unknown methods get
    DEFAULT_CU — use `is_priced()` when you need to know which."""
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
    """Call-weighted mean CU of a breakdown. This is what extrapolates a
    truncated breakdown to a full call count: the five busiest methods are
    a biased-but-close sample of the whole, since the tail is small by
    definition of being the tail."""
    calls = sum(by_method.values())
    if calls == 0:
        return float(DEFAULT_CU)
    return sum(cu_for(m) * c for m, c in by_method.items()) / calls


@dataclass
class CostEstimate:
    """One combination's cost, plus how much of it was actually measured."""

    total_cu: float
    usd: float
    # "exact" when the method breakdown accounts for every call;
    # "extrapolated" when it does not (see `estimate_calls` below) — the
    # difference matters more than the number does.
    basis: str
    jsonrpc_calls: int
    # Calls the breakdown named. Under `top_methods` this is the five
    # busiest methods' calls, never the total.
    covered_calls: int
    avg_cu: float
    unpriced_methods: list[str] = field(default_factory=list)

    @property
    def coverage(self) -> float:
        return self.covered_calls / self.jsonrpc_calls if self.jsonrpc_calls else 1.0


def estimate_calls(
    jsonrpc_calls: int,
    by_method: dict[str, int],
    usd_per_million_cu: float = DEFAULT_USD_PER_MILLION_CU,
) -> CostEstimate:
    """Cost a combination given its total call count and a possibly
    *truncated* method breakdown.

    `run_benchmark.py` writes only the five busiest methods to
    `top_methods`, so summing that column alone is a lower bound — it can
    miss a third of the calls and there is no way to tell from the file
    whether the missing ones are free `eth_chainId`s or 40-CU
    `eth_sendRawTransaction`s. When the breakdown does not cover the total
    we therefore price the covered calls exactly and the remainder at the
    covered calls' average weight, and label the whole row
    "extrapolated". Rerun with a full breakdown if a dollar figure has to
    be defended."""
    covered_calls = sum(by_method.values())
    covered_cu, _ = cost_for(by_method, usd_per_million_cu)
    unpriced = sorted(m for m in by_method if not is_priced(m))

    if covered_calls >= jsonrpc_calls:
        # Also the path for a row whose breakdown happens to be complete.
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
    """An *agent run* is one agent's whole workload in a combination — its
    `episodes` acquire→act→discard cycles. So this is the row's cost
    divided among its agents, scaled to a thousand. Comparing rows with
    different `episodes` compares different-sized runs; keep episodes
    fixed across the rows you quote together."""
    if num_agents <= 0:
        return 0.0
    return usd / num_agents * 1000


def _summarise(rows: list[dict[str, object]]) -> list[str]:
    """Per-backend roll-up. Costs are summed and agent runs are summed
    before dividing, so a big combination weighs more than a small one —
    which is the right way round: the large-N rows are the ones the claim
    is about."""
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
