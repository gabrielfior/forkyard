"""Unit tests for the branching benchmark.

Everything here runs against fakes: the arms' orchestration (how many
forks, in what order, what gets discarded, how a failure is recorded) is
the part that can be wrong in a way a live run would silently hide, and it
is the part that must not need a network or a subprocess to check.
"""

from __future__ import annotations

import csv
import io
import sys

import pytest

import bench_branching
from bench_branching import (
    ARMS,
    BRANCH_STEPS,
    FIELDS,
    MARKER_ACCOUNT,
    PARENT_MARKER_WEI,
    PREFIX_STEPS,
    PortAllocator,
    Record,
    _row,
    _total_row,
    branch_marker,
    branch_recipient,
    branch_swap_wei,
    branch_transfer_wei,
    check_inherited_marker,
    fork_from,
    marker_wei,
    run_anvil_processes_arm,
    run_anvil_snapshot_arm,
    run_branch_actions,
    run_branch_sweep,
    run_forkyard_arm,
    run_prefix,
    verify_isolation,
    write_records,
)

OK = ("stub", 1.0, True, "")


class FakeEth:
    def __init__(self, backend: "FakeBackend"):
        self._backend = backend

    def get_balance(self, address):
        if self._backend.unreachable:
            raise RuntimeError("session gone")
        return self._backend.balances.get(address, 0)

    def get_transaction_count(self, address):
        self._backend.nonce_reads += 1
        return self._backend.chain_nonce


class FakeManager:
    def __init__(self, backend: "FakeBackend"):
        self._backend = backend

    def request_blocking(self, method, params):
        self._backend.calls.append((method, list(params)))
        if method in self._backend.fail_on:
            raise RuntimeError(f"{method} exploded")
        if method == "evm_snapshot":
            self._backend.snapshots += 1
            return f"0x{self._backend.snapshots:x}"
        if method == "forkyard_forkFrom":
            self._backend.snapshots += 1
            return {"session_id": self._backend.snapshots}
        return True


class FakeWeb3:
    def __init__(self, backend: "FakeBackend"):
        self.eth = FakeEth(backend)
        self.manager = FakeManager(backend)


class FakeBackend:
    """A Backend that keeps its balances in a dict, so the marker
    read-backs the isolation checks depend on are real reads of real
    per-backend state."""

    name = "fake"

    def __init__(self, label: str = "fake"):
        self.label = label
        self.balances: dict[str, int] = {}
        self.storage: list[tuple[str, str, str]] = []
        self.calls: list[tuple[str, list]] = []
        self.fail_on: set[str] = set()
        self.snapshots = 0
        self.chain_nonce = 0
        self.nonce_reads = 0
        self.unreachable = False
        self.discarded = 0

    def web3(self):
        return FakeWeb3(self)

    def set_native_balance(self, address, wei):
        self.balances[address] = wei

    def set_storage(self, address, slot, value):
        self.storage.append((address, slot, value))

    def discard(self):
        self.discarded += 1


def stub_actions(monkeypatch, *, failing: set[str] = frozenset()) -> list[tuple]:
    """Replace the real transaction actions with recorders. The marker
    writes deliberately stay real — they go through
    `backend.set_native_balance`, which is what the isolation checks
    read back."""
    log: list[tuple] = []

    def make(name):
        def fn(backend, *args, **kwargs):
            log.append((name, backend, args))
            ok = name not in failing
            return (name, 1.0, ok, "" if ok else "boom")
        return fn

    for name in ("set_balance", "fund_token", "approve", "swap_eth_for_token", "transfer"):
        monkeypatch.setattr(bench_branching, name, make(name))
    return log


# --- schema -----------------------------------------------------------------


def test_fields_and_row_stay_in_lockstep():
    """main() drives a DictWriter with FIELDS directly, so a column added
    to one and not the other only raises mid-sweep, after real forks."""
    assert list(_row(Record("forkyard-branch", 8, "total", -1, 1.0, True)).keys()) == FIELDS


def test_write_records_round_trips_and_leaves_unmeasured_calls_empty():
    """An empty `jsonrpc_calls` cell means "not attributable to this row",
    which is a different claim from 0 upstream calls."""
    buf = io.StringIO()
    write_records(buf, [
        Record("forkyard-branch", 4, "branch_create", 2, 0.4, True),
        Record("forkyard-branch", 4, "total", -1, 91.0, False, "x", 3, 120),
    ])
    reader = csv.DictReader(io.StringIO(buf.getvalue()))
    rows = list(reader)
    assert reader.fieldnames == FIELDS
    assert rows[0]["jsonrpc_calls"] == ""
    assert rows[1]["jsonrpc_calls"] == "120"
    assert rows[1]["isolation_violations"] == "3"


def test_total_row_fails_a_run_that_leaked_however_fast_it_was():
    passing = [Record("forkyard-branch", 2, "branch_action", 0, 1.0, True)]
    assert _total_row("forkyard-branch", 2, passing, 10.0, 0).ok is True
    assert _total_row("forkyard-branch", 2, passing, 10.0, 1).ok is False


def test_default_ports_do_not_collide_with_the_other_scripts():
    """run_benchmark.py owns 18555/18556 + 19000+, bench_checkpoint 18600/
    18601 + 19200+, bench_writers 18610/18611 + 19300+."""
    assert (bench_branching.FORKYARD_PORT, bench_branching.FORKYARD_MCP_PORT) == (18650, 18651)
    assert bench_branching.ANVIL_BASE_PORT == 19700


def test_port_allocator_never_hands_out_a_port_twice():
    """A killed Anvil leaves its port in TIME_WAIT; reusing it inside one
    run either fails to bind or connects to the corpse."""
    ports = PortAllocator(19700)
    handed = ports.take(3) + ports.take() + ports.take(2)
    assert handed == [19700, 19701, 19702, 19703, 19704, 19705]
    assert len(set(handed)) == len(handed)


# --- divergence -------------------------------------------------------------


def test_every_branch_writes_values_only_it_uses():
    """If two branches wrote the same value, a leak between them would be
    invisible — the read-back would still return the expected number."""
    for fn in (marker_wei, branch_recipient, branch_transfer_wei, branch_swap_wei):
        values = [fn(i) for i in range(32)]
        assert len(set(values)) == 32, fn.__name__
    assert PARENT_MARKER_WEI not in {marker_wei(i) for i in range(32)}


def test_fork_from_targets_the_parents_own_session_endpoint():
    parent = FakeBackend()
    url = fork_from(parent, "http://127.0.0.1:18650")
    assert parent.calls == [("forkyard_forkFrom", [])]
    assert url == "http://127.0.0.1:18650/session/1"


# --- workload ---------------------------------------------------------------


def test_run_prefix_walks_the_step_cycle_and_ends_with_the_parent_marker(monkeypatch):
    log = stub_actions(monkeypatch)
    backend = FakeBackend()

    results, nonce = run_prefix(backend, 7)

    # P steps off the cycle, then the marker write (also a set_balance).
    assert [name for name, _, _ in log] == list(PREFIX_STEPS) + list(PREFIX_STEPS[:2]) + ["set_balance"]
    assert log[-1][2][0] == MARKER_ACCOUNT
    # The trailing marker is instrumentation, not one of the P actions, so
    # it is recorded but relabelled rather than counted.
    assert [r[0] for r in results] == list(PREFIX_STEPS) + list(PREFIX_STEPS[:2]) + ["parent_marker"]
    # approve/swap/transfer send transactions; set_balance and fund_token
    # are cheats that consume no nonce.
    assert nonce == 3


def test_run_prefix_starts_from_the_signers_nonce_on_the_fork(monkeypatch):
    """A deterministic key is a real mainnet address that may already have
    sent transactions; the fork inherits that history, and a prefix
    assuming 0 gets every transaction rejected `nonce too low` — which is
    exactly what the first live run of this file did."""
    log = stub_actions(monkeypatch)
    backend = FakeBackend()
    backend.chain_nonce = 1926

    _, nonce = run_prefix(backend, 5)

    assert backend.nonce_reads == 1, "read once, not once per transaction"
    sent = [args[-1] for name, _, args in log if name in ("approve", "swap_eth_for_token", "transfer")]
    assert sent == [1926, 1927, 1928]
    assert nonce == 1929


def test_run_prefix_resyncs_the_nonce_only_when_a_transaction_failed(monkeypatch):
    """A reverted tx burns its nonce so the local increment is right; a tx
    rejected before execution burns nothing and would leave the counter
    permanently ahead."""
    stub_actions(monkeypatch)
    backend = FakeBackend()
    run_prefix(backend, 5)
    assert backend.nonce_reads == 1, "the starting read only"

    stub_actions(monkeypatch, failing={"approve"})
    backend = FakeBackend()
    backend.chain_nonce = 1
    _, nonce = run_prefix(backend, 5)
    assert backend.nonce_reads == 2, "the starting read plus one resync"
    assert nonce == 3, "resync at approve, then swap and transfer increment from there"


def test_run_branch_actions_leads_with_the_inheritance_check(monkeypatch):
    stub_actions(monkeypatch)
    backend = FakeBackend()
    backend.balances[MARKER_ACCOUNT] = PARENT_MARKER_WEI

    results, violations = run_branch_actions(backend, 3, len(BRANCH_STEPS), start_nonce=4)

    assert [r[0] for r in results] == ["inherit_check", "branch_marker", "transfer", "swap_eth_for_token"]
    assert violations == 0
    assert backend.balances[MARKER_ACCOUNT] == marker_wei(3)


def test_run_branch_actions_continues_from_the_prefix_nonce(monkeypatch):
    """Every arm's branch begins at the state the prefix ended in, so they
    all continue from the same nonce — a branch that restarted at 0 would
    have every transaction rejected."""
    log = stub_actions(monkeypatch)
    backend = FakeBackend()
    backend.balances[MARKER_ACCOUNT] = PARENT_MARKER_WEI

    run_branch_actions(backend, 0, 3, start_nonce=4)

    nonces = [args[-1] for name, _, args in log if name in ("transfer", "swap_eth_for_token")]
    assert nonces == [4, 5]


def test_inheritance_check_catches_a_fork_that_started_from_the_base():
    """A `forkyard_forkFrom` that branched off the shared base instead of
    the parent's overlay reads 0 here, and every latency below it would be
    measuring the wrong workload."""
    backend = FakeBackend()  # marker never written -> reads 0
    result, violations = check_inherited_marker(backend)
    assert result[2] is True and violations == 1

    backend.balances[MARKER_ACCOUNT] = PARENT_MARKER_WEI
    _, violations = check_inherited_marker(backend)
    assert violations == 0


def test_inheritance_check_does_not_count_an_unreachable_session_as_a_leak():
    """ok=False already carries "could not be reached"; counting it as a
    violation too would make a network blip look like a state leak."""
    backend = FakeBackend()
    backend.unreachable = True
    result, violations = check_inherited_marker(backend)
    assert result[2] is False and violations == 0


def test_branch_marker_flags_a_read_back_that_is_not_its_own():
    backend = FakeBackend()
    result, violations = branch_marker(backend, 5)
    assert result[2] is True and violations == 0

    class Leaky(FakeBackend):
        def set_native_balance(self, address, wei):
            super().set_native_balance(address, marker_wei(99))

    result, violations = branch_marker(Leaky(), 5)
    assert violations == 1


def test_verify_isolation_catches_a_sibling_leak_and_a_lost_session():
    parent = FakeBackend("parent")
    parent.balances[MARKER_ACCOUNT] = PARENT_MARKER_WEI
    children = {}
    for i in range(3):
        child = FakeBackend(f"child{i}")
        child.balances[MARKER_ACCOUNT] = marker_wei(i)
        children[i] = child
    assert verify_isolation(parent, children) == 0

    # Branch 1 sees branch 2's write, and the parent has been clobbered by
    # a child — the two failure modes forkyard's Arc-of-base model must not
    # have.
    children[1].balances[MARKER_ACCOUNT] = marker_wei(2)
    parent.balances[MARKER_ACCOUNT] = marker_wei(0)
    assert verify_isolation(parent, children) == 2

    # A session that cannot answer at this point is indistinguishable from
    # one that lost its state.
    children[0].unreachable = True
    assert verify_isolation(None, {0: children[0]}) == 1


# --- arms -------------------------------------------------------------------


def install_fake_arm_workload(monkeypatch, *, branch_violations: int = 0, prefix_nonce: int = 4):
    """Swap the real prefix/branch workload for recorders that still move
    the markers, so the arms' own isolation sweep is exercised for real."""
    calls: dict[str, list] = {"prefix": [], "branch": []}

    def fake_prefix(backend, count):
        calls["prefix"].append((backend, count))
        backend.set_native_balance(MARKER_ACCOUNT, PARENT_MARKER_WEI)
        return [("set_balance", 1.0, True, "")] * count, prefix_nonce

    def fake_branch(backend, branch_id, count, start_nonce):
        calls["branch"].append((backend, branch_id, count, start_nonce))
        backend.set_native_balance(MARKER_ACCOUNT, marker_wei(branch_id))
        return [("transfer", 2.0, True, "")] * count, branch_violations

    monkeypatch.setattr(bench_branching, "run_prefix", fake_prefix)
    monkeypatch.setattr(bench_branching, "run_branch_actions", fake_branch)
    return calls


def install_fake_forkyard(monkeypatch) -> dict[str, FakeBackend]:
    made: dict[str, FakeBackend] = {}

    def factory(session_url=None, *, base_url=None):
        key = session_url or f"{base_url}#parent"
        made[key] = FakeBackend(key)
        return made[key]

    monkeypatch.setattr(bench_branching, "ForkyardBackend", factory)
    return made


def test_forkyard_arm_forks_once_per_branch_and_runs_them_all_at_once(monkeypatch):
    made = install_fake_forkyard(monkeypatch)
    calls = install_fake_arm_workload(monkeypatch)

    records = run_forkyard_arm("http://base", branches=4, prefix_actions=3, branch_actions=2)

    phases = [(r.phase, r.branch_id) for r in records]
    assert phases.count(("branch_create", 0)) == 1
    assert sorted(b for p, b in phases if p == "branch_create") == [0, 1, 2, 3]
    assert len([p for p, _ in phases if p == "prefix"]) == 3, "the prefix runs once, not K times"
    assert len([p for p, _ in phases if p == "branch_action"]) == 4 * 2
    assert phases[-1] == ("total", -1)
    # Every child was driven through its own session, not the parent's.
    assert len(calls["branch"]) == 4
    assert len({id(b) for b, *_ in calls["branch"]}) == 4
    assert all(nonce == 4 for *_, nonce in calls["branch"]), "branches continue the prefix's nonce"
    # 1 parent + 4 children, each discarded exactly once.
    assert len(made) == 5 and all(b.discarded == 1 for b in made.values())


def test_forkyard_arm_passes_the_post_hoc_isolation_sweep(monkeypatch):
    """The children are all still alive when this runs — the check the
    snapshot-stack arm cannot even be asked to perform."""
    install_fake_forkyard(monkeypatch)
    install_fake_arm_workload(monkeypatch)

    records = run_forkyard_arm("http://base", branches=3, prefix_actions=1, branch_actions=1)

    total = records[-1]
    assert total.isolation_violations == 0 and total.ok is True


def test_forkyard_arm_reports_a_leak_between_live_children(monkeypatch):
    made = install_fake_forkyard(monkeypatch)
    install_fake_arm_workload(monkeypatch)

    real_verify = bench_branching.verify_isolation

    def leaky_verify(parent, children):
        # Simulate two sessions sharing one overlay: the last child's write
        # lands in every child.
        for child in children.values():
            child.balances[MARKER_ACCOUNT] = marker_wei(max(children))
        return real_verify(parent, children)

    monkeypatch.setattr(bench_branching, "verify_isolation", leaky_verify)
    records = run_forkyard_arm("http://base", branches=3, prefix_actions=1, branch_actions=1)

    total = records[-1]
    assert total.isolation_violations == 2
    assert total.ok is False
    assert all(b.discarded == 1 for b in made.values()), "a leaking run still tears down"


def test_forkyard_arm_records_a_failed_fork_without_driving_a_missing_child(monkeypatch):
    install_fake_forkyard(monkeypatch)
    install_fake_arm_workload(monkeypatch)

    def flaky_fork(parent, base_url):
        flaky_fork.n += 1
        if flaky_fork.n == 2:
            raise RuntimeError("session cap reached")
        return f"{base_url}/session/{flaky_fork.n}"

    flaky_fork.n = 0
    monkeypatch.setattr(bench_branching, "fork_from", flaky_fork)

    records = run_forkyard_arm("http://base", branches=3, prefix_actions=1, branch_actions=1)

    creates = [r for r in records if r.phase == "branch_create"]
    assert [r.ok for r in creates] == [True, False, True]
    assert "session cap reached" in creates[1].error
    assert len([r for r in records if r.phase == "branch_action"]) == 2
    assert records[-1].ok is False


def test_anvil_snapshot_arm_is_sequential_and_pairs_a_revert_with_every_snapshot(monkeypatch):
    """Sequential is the finding, not a flaw: `evm_revert` invalidates
    every snapshot taken after it, so the branches share one mutable EVM."""
    backend = FakeBackend("anvil")
    monkeypatch.setattr(bench_branching, "AnvilBackend", lambda *a, **k: backend)
    calls = install_fake_arm_workload(monkeypatch)

    records = run_anvil_snapshot_arm("http://rpc", 25_795_072, 19700, 3, 2, 2)

    assert [m for m, _ in backend.calls] == ["evm_snapshot", "evm_revert"] * 3
    # Snapshot for branch i is reverted before branch i+1 is snapshotted:
    # only one branch's state exists at any moment.
    assert [p for m, p in backend.calls if m == "evm_revert"] == [["0x1"], ["0x2"], ["0x3"]]
    assert len(calls["prefix"]) == 1, "the prefix runs once for the whole stack"
    assert [b for _, b, *_ in calls["branch"]] == [0, 1, 2]
    assert len([r for r in records if r.phase == "branch_create"]) == 3
    assert backend.discarded == 1


def test_anvil_snapshot_arm_does_not_revert_a_stale_id(monkeypatch):
    """Reverting to whatever id was lying around would roll the chain back
    to some earlier branch and record it as a success."""
    backend = FakeBackend("anvil")
    backend.fail_on = {"evm_snapshot"}
    monkeypatch.setattr(bench_branching, "AnvilBackend", lambda *a, **k: backend)
    install_fake_arm_workload(monkeypatch)

    records = run_anvil_snapshot_arm("http://rpc", 25_795_072, 19700, 2, 1, 1)

    creates = [r for r in records if r.phase == "branch_create"]
    assert all(r.ok is False for r in creates)
    assert all("evm_snapshot" in r.error for r in creates)
    assert [m for m, _ in backend.calls] == ["evm_snapshot"] * 2, "no revert was attempted"
    assert records[-1].ok is False


def test_anvil_processes_arm_replays_the_whole_prefix_per_branch(monkeypatch):
    made: list[tuple[int, FakeBackend]] = []

    def factory(port, *a, **k):
        backend = FakeBackend(f"anvil:{port}")
        made.append((port, backend))
        return backend

    monkeypatch.setattr(bench_branching, "AnvilBackend", factory)
    calls = install_fake_arm_workload(monkeypatch)

    ports = [19700, 19701, 19702, 19703]
    records = run_anvil_processes_arm("http://rpc", 25_795_072, ports, 4, 3, 2)

    assert [p for p, _ in made] == ports, "one process per branch, on its own port"
    assert len(calls["prefix"]) == 4, "the prefix is paid K times, not once"
    # Per branch: 3 prefix rows + 1 branch_create + 2 branch_action rows.
    assert len([r for r in records if r.phase == "prefix"]) == 12
    assert len([r for r in records if r.phase == "branch_create"]) == 4
    assert len([r for r in records if r.phase == "branch_action"]) == 8
    assert all(b.discarded == 1 for _, b in made)


def test_anvil_processes_arm_survives_a_branch_whose_process_never_came_up(monkeypatch):
    """At K=32 a spawn timeout must cost one branch, not the sweep."""
    def factory(port, *a, **k):
        if port == 19701:
            raise RuntimeError("anvil did not become ready in 120s")
        return FakeBackend(f"anvil:{port}")

    monkeypatch.setattr(bench_branching, "AnvilBackend", factory)
    install_fake_arm_workload(monkeypatch)

    records = run_anvil_processes_arm("http://rpc", 25_795_072, [19700, 19701], 2, 1, 1)

    failed = [r for r in records if r.phase == "branch_create" and not r.ok]
    assert len(failed) == 1 and "did not become ready" in failed[0].error
    assert len([r for r in records if r.phase == "branch_action"]) == 1
    assert records[-1].phase == "total" and records[-1].ok is False


def test_arm_totals_are_the_only_rows_that_may_be_compared_across_arms(monkeypatch):
    """anvil-processes' branch_create spans spawn + prefix and therefore
    overlaps its own prefix rows; summing phases across arms would double
    count it."""
    monkeypatch.setattr(bench_branching, "AnvilBackend", lambda port, *a, **k: FakeBackend())
    install_fake_arm_workload(monkeypatch)
    records = run_anvil_processes_arm("http://rpc", 1, [19700], 1, 2, 1)

    create = next(r for r in records if r.phase == "branch_create")
    prefix_sum = sum(r.elapsed_ms for r in records if r.phase == "prefix")
    assert create.elapsed_ms >= 0 and prefix_sum > 0


# --- sweep + CLI ------------------------------------------------------------


def test_sweep_gives_every_anvil_in_a_run_its_own_port(monkeypatch):
    seen_ports: list[int] = []

    def factory(port, *a, **k):
        seen_ports.append(port)
        return FakeBackend()

    monkeypatch.setattr(bench_branching, "AnvilBackend", factory)
    install_fake_arm_workload(monkeypatch)
    ports = PortAllocator(19700)

    for branches in (2, 4):
        run_branch_sweep(
            "http://rpc", 1, branches, 1, 1, ports,
            arms=["anvil-snapshot", "anvil-processes"],
        )

    assert len(seen_ports) == (1 + 2) + (1 + 4)
    assert len(set(seen_ports)) == len(seen_ports)


def test_sweep_starts_a_fresh_forkyard_per_k(monkeypatch):
    """forkyard's base cache is shared across sessions, so one long-lived
    process would let K=32 read state K=2 paid for while the Anvil arms
    (running --no-storage-caching) refetch from cold every time."""
    starts: list[int] = []
    monkeypatch.setattr(bench_branching, "start_forkyard", lambda url, bh: starts.append(bh) or object())
    monkeypatch.setattr(bench_branching, "_terminate", lambda p: None)
    install_fake_forkyard(monkeypatch)
    install_fake_arm_workload(monkeypatch)

    for branches in (2, 4):
        run_branch_sweep("http://rpc", 77, branches, 1, 1, PortAllocator(), arms=["forkyard-branch"])

    assert starts == [77, 77]


def test_sweep_rejects_an_unknown_arm():
    with pytest.raises(ValueError, match="unknown arm"):
        run_branch_sweep("http://rpc", 1, 2, 1, 1, PortAllocator(), arms=["anvil-fork"])


def test_arm_names_are_the_ones_the_cli_advertises():
    assert ARMS == ("forkyard-branch", "anvil-snapshot", "anvil-processes")


def test_cli_help_says_the_snapshot_arm_is_sequential_by_construction(monkeypatch, capsys):
    """Without that sentence a reader takes anvil-snapshot's wall clock for
    a threading bug in the harness rather than the shape of the API."""
    monkeypatch.setattr(sys, "argv", ["bench_branching.py", "--help"])
    with pytest.raises(SystemExit):
        bench_branching.main()
    help_text = " ".join(capsys.readouterr().out.split())
    assert "sequential by construction" in help_text
    assert "isolation_violations must be 0" in help_text
    assert "--no-proxy" in help_text


def test_cli_refuses_to_run_without_an_endpoint(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_branching.py"])
    monkeypatch.delenv("RPC_URL", raising=False)
    with pytest.raises(SystemExit):
        bench_branching.main()
    assert "--rpc-url" in capsys.readouterr().err


def test_cli_rejects_an_unknown_arm_before_forking_anything(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_branching.py", "--rpc-url", "http://rpc", "--arms", "nope"])
    with pytest.raises(SystemExit):
        bench_branching.main()
    assert "unknown arm" in capsys.readouterr().err
