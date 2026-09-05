"""Entry point for every benchmark: `uv run python bench.py <name> [flags]`.

Each subcommand parses its own flags, so `bench.py branching --help` shows
what branching takes. See benchmark.md for what each one measures.
"""

from __future__ import annotations

import sys

from bench_architecture import branching_main, checkpoint_main, writers_main
from bench_cache import blocks_main, warmstart_main
from bench_load import arrivals_main, freshness_main, quota_main

COMMANDS = {
    "branching": (branching_main, "K what-ifs from one starting state"),
    "checkpoint": (checkpoint_main, "checkpoint/restore cost against dirty-state size"),
    "writers": (writers_main, "isolated concurrent writers per GB"),
    "arrivals": (arrivals_main, "time to first simulation under Poisson arrivals"),
    "freshness": (freshness_main, "cost of staying at the chain tip"),
    "quota": (quota_main, "sustainable agents under a provider rate limit"),
    "warmstart": (warmstart_main, "restart cost, cold against warm"),
    "blocks": (blocks_main, "agents spread across many fork blocks"),
}


def usage() -> str:
    width = max(len(name) for name in COMMANDS)
    lines = [f"  {name.ljust(width)}  {desc}" for name, (_, desc) in COMMANDS.items()]
    return "usage: bench.py <command> [flags]\n\n" + "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if not args or args[0] in ("-h", "--help"):
        print(usage())
        return 0 if args else 2
    command, *rest = args
    if command not in COMMANDS:
        print(f"unknown command {command!r}\n\n{usage()}", file=sys.stderr)
        return 2
    # Each subcommand owns an argparse parser reading sys.argv, so hand it
    # everything after the command name.
    sys.argv = [f"bench.py {command}", *rest]
    COMMANDS[command][0]()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
