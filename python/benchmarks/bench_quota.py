"""How many agents each backend can keep alive under a fixed upstream quota.

The call-count benchmark (`run_benchmark.py --count-upstream`) says forkyard
asks the provider for roughly a tenth of what one-Anvil-per-agent asks for.
That is a bill. This is the other half of the same fact: a provider plan is
also a *rate*, and the backend that needs 10x the calls hits the ceiling at
a fraction of the agent count — where "hits the ceiling" means either the
provider queues you (every agent gets slower) or refuses you (agents start
failing). `rpc_proxy.py` models both; this sweeps them.

For each (backend, quota) it reports the largest agent count that still
completes ≥99% of its actions — max sustainable agents — together with the
success rate, wall-clock and throttling at every point, so the shape of the
collapse is visible and not just the final number.

Not a synthetic claim: on a free public endpoint at 25 agents, Anvil's
action success fell to 26.4% while forkyard stayed at 100%. This harness
reproduces that on any endpoint by making the quota an input instead of an
accident of which provider you happened to point at.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time

from agent import ActionRecord
from rpc_proxy import CountingProxy
from run_benchmark import (
    _check_binaries_on_path,
    parse_int_list,
    run_anvil_sweep,
    run_forkyard_sweep,
)

# Fixed, and deliberately not the ports run_benchmark.py uses: a quota
# sweep is long, and it must be possible to leave one running without it
# colliding with an ordinary sweep started in another shell.
FORKYARD_PORT = 18640
FORKYARD_MCP_PORT = 18641
ANVIL_BASE_PORT = 19600
PROXY_PORT = 18700

# "Sustainable" has to mean something an operator would accept. 99% is one
# failed action in a hundred — already generous for a fleet of agents whose
# work is worthless if a transaction silently didn't land.
DEFAULT_THRESHOLD = 0.99

FIELDS = [
    "backend", "quota_rps", "num_agents", "action_success_rate", "wall_clock_ms",
    "jsonrpc_calls", "throttled_calls", "total_delay_ms", "max_sustainable_agents",
]


def action_success_rate(records: list[ActionRecord]) -> float:
    """Fraction of an agent's recorded actions that succeeded.

    `acquire` and `discard` count like everything else, on purpose: under a
    tight quota the first thing that fails is Anvil *forking at all* — it
    cannot even fetch the block it is pinned to — and an agent that never
    got an environment has failed at its job as surely as one whose swap
    reverted. An empty record list is a run that produced nothing, which is
    a 0, not a 1."""
    if not records:
        return 0.0
    return sum(1 for r in records if r.ok) / len(records)


def max_sustainable_agents(
    points: list[tuple[int, float]], threshold: float = DEFAULT_THRESHOLD
) -> int:
    """Largest tested agent count that met `threshold` **with every smaller
    tested count also meeting it**.

    The stricter "and everything below it" rule is what the word
    *sustainable* has to mean: if 10 agents fail and 25 pass, that is noise
    or a lucky retry, not a capacity of 25. The per-point rows keep the
    non-monotonic evidence visible so it can be judged rather than hidden.
    Returns 0 when even the smallest tested count fails."""
    best = 0
    for num_agents, rate in sorted(points):
        if rate < threshold:
            break
        best = num_agents
    return best


def quota_row(
    backend: str,
    quota_rps: float,
    num_agents: int,
    success_rate: float,
    wall_clock_ms: float,
    stats,
    max_sustainable: int,
) -> dict[str, object]:
    return {
        "backend": backend,
        "quota_rps": quota_rps,
        "num_agents": num_agents,
        "action_success_rate": round(success_rate, 4),
        "wall_clock_ms": round(wall_clock_ms, 1),
        "jsonrpc_calls": stats.jsonrpc_calls,
        "throttled_calls": stats.throttled_calls,
        "total_delay_ms": round(stats.total_delay_ms, 1),
        "max_sustainable_agents": max_sustainable,
    }


def _run_point(
    backend: str, rpc_url: str, block_height: int, num_agents: int,
    actions_per_agent: int, episodes: int,
) -> tuple[list[ActionRecord], float]:
    """One (backend, agent count) point, through the already-throttled
    proxy URL. The workload itself is `run_benchmark`'s — reimplementing it
    here would let the two benchmarks drift apart and make their numbers
    incomparable, which is the only reason either is interesting."""
    if backend == "forkyard":
        return run_forkyard_sweep(
            rpc_url, block_height, num_agents, actions_per_agent,
            FORKYARD_PORT, FORKYARD_MCP_PORT, episodes,
        )
    return run_anvil_sweep(
        rpc_url, block_height, num_agents, actions_per_agent, ANVIL_BASE_PORT, episodes,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Measure how many concurrent agents each backend sustains under a "
            "capped upstream RPC rate. Both backends run the same workload "
            "through one rate-limited counting proxy; for every (backend, "
            "quota) the sweep reports the largest agent count still completing "
            f"at least {DEFAULT_THRESHOLD:.0%} of its actions, plus the success "
            "rate, wall-clock and throttling at each point."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "The quota is in JSON-RPC calls per second and a batch of M calls "
            "costs M — that is how providers meter. --limit-mode delay models a "
            "provider that queues you (excess volume becomes latency); "
            "--limit-mode reject models one that answers -32005 (excess volume "
            "becomes failed actions). Anvil runs with its cross-process fork "
            "cache disabled, as everywhere else in this harness, so a run does "
            "not measure the machine's benchmark history."
        ),
    )
    parser.add_argument(
        "--quotas", type=parse_int_list, default=[10, 25, 100],
        help="upstream budgets to test, in JSON-RPC calls/sec (default 10,25,100)",
    )
    parser.add_argument(
        "--agents", type=parse_int_list, default=[5, 10, 25, 50],
        help="concurrent agent counts to test at each quota (default 5,10,25,50)",
    )
    parser.add_argument("--block-height", type=int, default=25795072)
    parser.add_argument("--actions-per-agent", type=int, default=5)
    parser.add_argument("--episodes", type=int, default=1)
    parser.add_argument(
        "--threshold", type=float, default=DEFAULT_THRESHOLD,
        help=f"action success rate a point must reach to count as sustained "
             f"(default {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--limit-mode", choices=["delay", "reject"], default="delay",
        help="how the proxy enforces the quota (default delay). 'delay' shows up "
             "as wall-clock and total_delay_ms; 'reject' shows up as a collapsing "
             "action_success_rate.",
    )
    parser.add_argument(
        "--burst", type=float, default=None,
        help="token bucket capacity in calls (default: one second of the quota)",
    )
    parser.add_argument(
        "--full-curve", action="store_true",
        help="keep testing larger agent counts after one has already failed the "
             "threshold. Off by default because every extra point costs a full "
             "sweep and cannot raise max_sustainable_agents — turn it on to show "
             "the whole collapse, not just where it starts.",
    )
    parser.add_argument(
        "--settle-s", type=float, default=1.0,
        help="pause between points so the token bucket refills (default 1.0). "
             "Without it a backend would inherit the queue the previous one left "
             "behind, and whichever ran second would look worse than it is.",
    )
    parser.add_argument("--rpc-url", required=True)
    parser.add_argument("--out", default="quota.csv")
    args = parser.parse_args()

    _check_binaries_on_path()

    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        f.flush()

        for quota in args.quotas:
            # One proxy per quota, on a fixed port, torn down before the
            # next: the bucket is the provider, and a provider does not
            # change its plan halfway through.
            proxy = CountingProxy(
                args.rpc_url, PROXY_PORT,
                rate_limit_rps=quota, burst=args.burst, limit_mode=args.limit_mode,
            ).start()
            try:
                for backend in ("forkyard", "anvil"):
                    points: list[tuple[int, float]] = []
                    pending: list[dict[str, object]] = []
                    for num_agents in sorted(args.agents):
                        if args.settle_s:
                            time.sleep(args.settle_s)
                        proxy.reset()
                        print(
                            f"quota={quota}/s {backend}: {num_agents} agents "
                            f"({args.actions_per_agent} actions, {args.episodes} episodes)",
                            file=sys.stderr,
                        )
                        records, total_ms = _run_point(
                            backend, proxy.url, args.block_height, num_agents,
                            args.actions_per_agent, args.episodes,
                        )
                        stats = proxy.snapshot()
                        rate = action_success_rate(records)
                        points.append((num_agents, rate))
                        pending.append(
                            quota_row(backend, quota, num_agents, rate, total_ms, stats, -1)
                        )
                        print(
                            f"  success={rate:.1%} wall={total_ms:.0f}ms "
                            f"calls={stats.jsonrpc_calls} throttled={stats.throttled_calls} "
                            f"delay={stats.total_delay_ms:.0f}ms",
                            file=sys.stderr,
                        )
                        if rate < args.threshold and not args.full_curve:
                            print(
                                f"  below {args.threshold:.0%}: stopping this "
                                f"({backend}, {quota}/s) curve here",
                                file=sys.stderr,
                            )
                            break

                    # max_sustainable_agents is a property of the whole
                    # curve, so the rows can only be written once the curve
                    # is done. Per (backend, quota) is still fine-grained
                    # enough that an interrupted sweep keeps what it has.
                    sustained = max_sustainable_agents(points, args.threshold)
                    for row in pending:
                        row["max_sustainable_agents"] = sustained
                        writer.writerow(row)
                    f.flush()
                    print(
                        f"  -> {backend} sustains {sustained} agents at {quota} calls/s",
                        file=sys.stderr,
                    )
            finally:
                proxy.stop()


if __name__ == "__main__":
    main()
