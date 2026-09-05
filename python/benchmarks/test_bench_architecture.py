import bench_architecture
import bench_common
import csv
import io
import pytest
import sys
import types

from bench_architecture import (
    ARMS,
    BRANCHING_FIELDS,
    BRANCH_STEPS,
    CHECKPOINT_FIELDS,
    MARKER_ACCOUNT,
    PARENT_MARKER_WEI,
    PREFIX_STEPS,
    Record,
    Sample,
    SweepResult,
    WRITERS_FIELDS,
    WriterOutcome,
    _branching_row,
    _checkpoint_row,
    _measure,
    _total_row,
    _writers_row,
    blob_size_bytes,
    branch_marker,
    branch_recipient,
    branch_swap_wei,
    branch_transfer_wei,
    check_inherited_marker,
    fork_from,
    marker_wei,
    measure_anvil,
    measure_forkyard,
    run_anvil_processes_arm,
    run_anvil_snapshot_arm,
    run_branch_actions,
    run_branch_sweep,
    run_forkyard_arm,
    run_prefix,
    run_writer,
    run_writers,
    slot_hex,
    summarize,
    touch_slots,
    value_hex,
    verify_isolation,
    write_records,
    write_results,
    write_samples,
    writer_value,
)
from bench_common import PortAllocator, RssSampler, process_pids, total_rss_mb
from web3 import Web3


# --- from test_bench_branching

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


class branching_FakeManager:
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


class branching_FakeWeb3:
    def __init__(self, backend: "FakeBackend"):
        self.eth = FakeEth(backend)
        self.manager = branching_FakeManager(backend)


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
        return branching_FakeWeb3(self)

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
        monkeypatch.setattr(bench_architecture, name, make(name))
    return log


def test_branching_fields_and_row_stay_in_lockstep():
    """branching_main() drives a DictWriter with BRANCHING_FIELDS directly, so a column added
    to one and not the other only raises mid-sweep, after real forks."""
    assert list(_branching_row(Record("forkyard-branch", 8, "total", -1, 1.0, True)).keys()) == BRANCHING_FIELDS


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
    assert reader.fieldnames == BRANCHING_FIELDS
    assert rows[0]["jsonrpc_calls"] == ""
    assert rows[1]["jsonrpc_calls"] == "120"
    assert rows[1]["isolation_violations"] == "3"


def test_total_row_fails_a_run_that_leaked_however_fast_it_was():
    passing = [Record("forkyard-branch", 2, "branch_action", 0, 1.0, True)]
    assert _total_row("forkyard-branch", 2, passing, 10.0, 0).ok is True
    assert _total_row("forkyard-branch", 2, passing, 10.0, 1).ok is False


def test_default_ports_do_not_collide_with_the_other_scripts():
    """run_benchmark.py owns 18555/18556 + 19000+, bench_architecture 18600/
    18601 + 19200+, bench_architecture 18610/18611 + 19300+."""
    assert (bench_architecture.BRANCHING_FORKYARD_PORT, bench_architecture.BRANCHING_FORKYARD_MCP_PORT) == (18650, 18651)
    assert bench_architecture.BRANCHING_ANVIL_BASE_PORT == 19700


def test_port_allocator_never_hands_out_a_port_twice():
    """A killed Anvil leaves its port in TIME_WAIT; reusing it inside one
    run either fails to bind or connects to the corpse."""
    ports = PortAllocator(19700)
    handed = ports.take(3) + ports.take() + ports.take(2)
    assert handed == [19700, 19701, 19702, 19703, 19704, 19705]
    assert len(set(handed)) == len(handed)


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

    monkeypatch.setattr(bench_architecture, "run_prefix", fake_prefix)
    monkeypatch.setattr(bench_architecture, "run_branch_actions", fake_branch)
    return calls


def install_fake_forkyard(monkeypatch) -> dict[str, FakeBackend]:
    made: dict[str, FakeBackend] = {}

    def factory(session_url=None, *, base_url=None):
        key = session_url or f"{base_url}#parent"
        made[key] = FakeBackend(key)
        return made[key]

    monkeypatch.setattr(bench_architecture, "ForkyardBackend", factory)
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

    real_verify = bench_architecture.verify_isolation

    def leaky_verify(parent, children):
        # Simulate two sessions sharing one overlay: the last child's write
        # lands in every child.
        for child in children.values():
            child.balances[MARKER_ACCOUNT] = marker_wei(max(children))
        return real_verify(parent, children)

    monkeypatch.setattr(bench_architecture, "verify_isolation", leaky_verify)
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
    monkeypatch.setattr(bench_architecture, "fork_from", flaky_fork)

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
    monkeypatch.setattr(bench_architecture, "AnvilBackend", lambda *a, **k: backend)
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
    monkeypatch.setattr(bench_architecture, "AnvilBackend", lambda *a, **k: backend)
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

    monkeypatch.setattr(bench_architecture, "AnvilBackend", factory)
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

    monkeypatch.setattr(bench_architecture, "AnvilBackend", factory)
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
    monkeypatch.setattr(bench_architecture, "AnvilBackend", lambda port, *a, **k: FakeBackend())
    install_fake_arm_workload(monkeypatch)
    records = run_anvil_processes_arm("http://rpc", 1, [19700], 1, 2, 1)

    create = next(r for r in records if r.phase == "branch_create")
    prefix_sum = sum(r.elapsed_ms for r in records if r.phase == "prefix")
    assert create.elapsed_ms >= 0 and prefix_sum > 0


def test_sweep_gives_every_anvil_in_a_run_its_own_port(monkeypatch):
    seen_ports: list[int] = []

    def factory(port, *a, **k):
        seen_ports.append(port)
        return FakeBackend()

    monkeypatch.setattr(bench_architecture, "AnvilBackend", factory)
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
    monkeypatch.setattr(bench_architecture, "start_forkyard", lambda url, bh: starts.append(bh) or object())
    monkeypatch.setattr(bench_architecture, "_terminate", lambda p: None)
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
    monkeypatch.setattr(sys, "argv", ["bench_architecture.py", "--help"])
    with pytest.raises(SystemExit):
        bench_architecture.branching_main()
    help_text = " ".join(capsys.readouterr().out.split())
    assert "sequential by construction" in help_text
    assert "isolation_violations must be 0" in help_text
    assert "--no-proxy" in help_text


def test_branching_cli_refuses_to_run_without_an_endpoint(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_architecture.py"])
    monkeypatch.delenv("RPC_URL", raising=False)
    with pytest.raises(SystemExit):
        bench_architecture.branching_main()
    assert "--rpc-url" in capsys.readouterr().err


def test_cli_rejects_an_unknown_arm_before_forking_anything(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_architecture.py", "--rpc-url", "http://rpc", "--arms", "nope"])
    with pytest.raises(SystemExit):
        bench_architecture.branching_main()
    assert "unknown arm" in capsys.readouterr().err


# --- from test_bench_checkpoint

class checkpoint_FakeManager:
    """Records every JSON-RPC method the measurement drives, and answers
    the two that return something the script depends on."""

    def __init__(self, dump_blob="0x" + "ab" * 64, fail_on=()):
        self.calls: list[tuple[str, list]] = []
        self._dump_blob = dump_blob
        self._fail_on = set(fail_on)

    def request_blocking(self, method, params):
        self.calls.append((method, params))
        if method in self._fail_on:
            raise RuntimeError(f"{method} exploded")
        if method == "evm_snapshot":
            return "0x1"
        if method == "anvil_dumpState":
            return self._dump_blob
        return True


class checkpoint_FakeWeb3:
    def __init__(self, manager):
        self.manager = manager


class FakeAnvil:
    def __init__(self, manager):
        self._manager = manager
        self.stored: list[tuple[str, str, str]] = []
        self.discarded = False

    def web3(self):
        return checkpoint_FakeWeb3(self._manager)

    def set_storage(self, address, slot, value):
        self.stored.append((address, slot, value))

    def discard(self):
        self.discarded = True


def test_checkpoint_fields_and_row_stay_in_lockstep():
    """checkpoint_main() drives a DictWriter with CHECKPOINT_FIELDS directly, so a column added
    to one and not the other only raises mid-run, after real forks."""
    sample = Sample("anvil", "dump", 100, 1.0, 4096, True, "")
    assert list(_checkpoint_row(sample).keys()) == CHECKPOINT_FIELDS


def test_blob_size_bytes_measures_the_encoded_dump():
    assert blob_size_bytes("0x" + "ff" * 10) == 10
    assert blob_size_bytes("ff" * 10) == 10  # a build that omits the prefix
    assert blob_size_bytes(b"\x00" * 7) == 7
    assert blob_size_bytes(None) == 0


def test_blob_size_bytes_handles_a_json_object_dump():
    """Older Foundry builds returned a state object rather than a hex
    string; the column is about magnitude, so measure it either way."""
    assert blob_size_bytes({"accounts": {}}) == len(str({"accounts": {}}).encode())


def test_slot_and_value_words_are_full_32_byte_words():
    assert len(slot_hex(0)) == 66 and slot_hex(0).startswith("0x")
    assert len(value_hex(0)) == 66
    # A zero word is a storage delete on some backends, which would shrink
    # the state this script is trying to grow.
    assert int(value_hex(0), 16) != 0


def test_touch_slots_writes_one_distinct_slot_per_unit_of_state_size():
    written: list[tuple[str, str, str]] = []
    touch_slots(lambda a, s, v: written.append((a, s, v)), 5)
    assert len(written) == 5
    assert len({s for _, s, _ in written}) == 5, "duplicate slots would not grow the state"
    assert {a for a, _, _ in written} == {bench_architecture.DIRTY_CONTRACT}


def test_measure_records_the_blob_size_the_operation_moved():
    sample = _measure("anvil", "dump", 1000, lambda: 4096)
    assert (sample.backend, sample.operation, sample.state_size) == ("anvil", "dump", 1000)
    assert (sample.blob_bytes, sample.ok, sample.error) == (4096, True, "")
    assert sample.elapsed_ms >= 0


def test_measure_captures_the_failure_reason_and_reports_no_blob():
    def boom():
        raise RuntimeError("x" * 5_000)

    sample = _measure("anvil", "load", 100, boom)
    assert sample.ok is False
    assert sample.blob_bytes == 0
    assert len(sample.error) == bench_architecture.MAX_ERROR_CHARS


def test_measure_anvil_times_both_checkpoint_mechanisms_per_repeat(monkeypatch):
    manager = checkpoint_FakeManager(dump_blob="0x" + "cd" * 500)
    backend = FakeAnvil(manager)
    monkeypatch.setattr(bench_architecture, "AnvilBackend", lambda *a, **k: backend)

    samples = measure_anvil("http://rpc.example", 25_795_072, 3, 19200, repeats=2)

    assert [s.operation for s in samples] == [
        "snapshot", "revert", "dump", "load", "snapshot", "revert", "dump", "load",
    ]
    assert all(s.ok for s in samples), [s.error for s in samples]
    assert all(s.state_size == 3 for s in samples)
    assert len(backend.stored) == 3, "the state must be dirtied before any checkpoint is timed"
    assert backend.discarded, "the instance must not outlive the measurement"


def test_measure_anvil_reports_blob_bytes_only_where_a_blob_exists(monkeypatch):
    """evm_snapshot/evm_revert keep the state in memory; only dumpState
    materialises bytes, and that is the number the claim rests on."""
    manager = checkpoint_FakeManager(dump_blob="0x" + "cd" * 500)
    backend = FakeAnvil(manager)
    monkeypatch.setattr(bench_architecture, "AnvilBackend", lambda *a, **k: backend)

    samples = measure_anvil("http://rpc.example", 1, 2, 19200, repeats=1)

    by_op = {s.operation: s for s in samples}
    assert by_op["snapshot"].blob_bytes == 0
    assert by_op["revert"].blob_bytes == 0
    assert by_op["dump"].blob_bytes == 500
    assert by_op["load"].blob_bytes == 500, "load moves the same blob dump produced"


def test_measure_anvil_records_a_failed_snapshot_without_reverting_a_stale_id(monkeypatch):
    """Reverting to whatever id happened to be lying around would time a
    revert of some earlier snapshot and record it as a success."""
    manager = checkpoint_FakeManager(fail_on={"evm_snapshot"})
    backend = FakeAnvil(manager)
    monkeypatch.setattr(bench_architecture, "AnvilBackend", lambda *a, **k: backend)

    samples = measure_anvil("http://rpc.example", 1, 1, 19200, repeats=1)

    by_op = {s.operation: s for s in samples}
    assert by_op["snapshot"].ok is False
    assert by_op["revert"].ok is False
    assert "evm_snapshot" in by_op["revert"].error
    assert ("evm_revert", ["0x1"]) not in manager.calls


def test_measure_forkyard_times_a_branch_off_the_base_and_its_discard(monkeypatch):
    opened: list[str] = []
    discarded: list[str] = []

    class FakeForkyard:
        def __init__(self, session_url=None, *, base_url=None):
            self.session_url = session_url or f"{base_url}/session/base"
            self.stored: list[tuple[str, str, str]] = []

        def set_storage(self, address, slot, value):
            self.stored.append((address, slot, value))
            dirty_writes.append(slot)

        def discard(self):
            discarded.append(self.session_url)

    dirty_writes: list[str] = []

    def fake_open(base_url, timeout_s=30.0):
        opened.append(base_url)
        return f"{base_url}/session/{len(opened)}"

    monkeypatch.setattr(bench_architecture, "ForkyardBackend", FakeForkyard)
    monkeypatch.setattr(bench_architecture, "open_forkyard_session", fake_open)

    samples = measure_forkyard("http://127.0.0.1:18600", state_size=4, repeats=3)

    assert [s.operation for s in samples] == ["fork", "discard"] * 3
    assert all(s.ok for s in samples), [s.error for s in samples]
    # Every forkyard row is blob-free by construction: branching off the
    # shared base serializes nothing, which is the whole claim.
    assert all(s.blob_bytes == 0 for s in samples)
    assert len(dirty_writes) == 4, "the dirty session must be written before forking"
    assert len(opened) == 3, "one fresh session per repeat"
    assert len(discarded) == 4, "three branched sessions plus the dirty base"


def test_measure_forkyard_does_not_discard_a_session_it_failed_to_open(monkeypatch):
    class FakeForkyard:
        def __init__(self, session_url=None, *, base_url=None):
            self.session_url = session_url
            self.discards = discards

        def set_storage(self, address, slot, value):
            pass

        def discard(self):
            discards.append(self.session_url)

    discards: list[str] = []

    def fake_open(base_url, timeout_s=30.0):
        raise RuntimeError("session pool exhausted")

    monkeypatch.setattr(bench_architecture, "ForkyardBackend", FakeForkyard)
    monkeypatch.setattr(bench_architecture, "open_forkyard_session", fake_open)

    samples = measure_forkyard("http://127.0.0.1:18600", state_size=1, repeats=1)

    by_op = {s.operation: s for s in samples}
    assert by_op["fork"].ok is False
    assert by_op["discard"].ok is False
    assert discards == [None], "only the dirty base session was ever opened"


def test_write_samples_round_trips_through_csv():
    buf = io.StringIO()
    write_samples(buf, [
        Sample("forkyard", "fork", 10000, 0.9, 0, True, ""),
        Sample("anvil", "load", 10000, 812.0, 5_000_000, False, "RuntimeError('nope')"),
    ])
    reader = csv.DictReader(io.StringIO(buf.getvalue()))
    rows = list(reader)
    assert reader.fieldnames == CHECKPOINT_FIELDS
    assert rows[0]["operation"] == "fork" and rows[0]["blob_bytes"] == "0"
    assert rows[1]["blob_bytes"] == "5000000"
    assert rows[1]["error"] == "RuntimeError('nope')"


def test_default_ports_do_not_collide_with_the_main_sweep():
    """run_benchmark.py owns 18555/18556 and 19000+; a collision would make
    one script silently measure the other's processes."""
    assert bench_architecture.CHECKPOINT_FORKYARD_PORT == 18600
    assert bench_architecture.CHECKPOINT_FORKYARD_MCP_PORT == 18601
    assert bench_architecture.CHECKPOINT_ANVIL_BASE_PORT == 19200


def test_cli_help_says_what_is_being_compared(monkeypatch, capsys):
    """--help is where a reader learns this is not a like-named API
    comparison; without that sentence the two elapsed_ms columns look
    like the same operation measured twice."""
    monkeypatch.setattr(sys, "argv", ["bench_architecture.py", "--help"])
    with pytest.raises(SystemExit):
        bench_architecture.checkpoint_main()
    # argparse rewraps the description, so compare on collapsed whitespace
    # rather than on wherever the terminal width happened to break a line.
    help_text = " ".join(capsys.readouterr().out.split())
    assert "anvil_dumpState" in help_text
    assert "forkyard has no snapshot RPC" in help_text


def test_checkpoint_cli_refuses_to_run_without_an_endpoint(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_architecture.py"])
    monkeypatch.delenv("RPC_URL", raising=False)
    with pytest.raises(SystemExit):
        bench_architecture.checkpoint_main()
    assert "--rpc-url" in capsys.readouterr().err


# --- from test_bench_writers

class FakeWeb3Eth:
    def __init__(self, balances, address_key):
        self._balances = balances
        self._key = address_key

    def get_balance(self, address):
        return self._balances.get(address, 0)


class IsolatedBackend:
    """Its own private state, like a real session or a real Anvil."""

    def __init__(self, shared_state=None):
        # Passing a dict in makes every "environment" the *same* dict — the
        # leak this benchmark exists to rule out.
        self._state = {} if shared_state is None else shared_state
        self.storage_writes: list[tuple[str, str, str]] = []
        self.discarded = False

    def web3(self):
        return types.SimpleNamespace(eth=FakeWeb3Eth(self._state, None))

    def set_native_balance(self, address, wei):
        self._state[address] = wei

    def set_storage(self, address, slot, value):
        self.storage_writes.append((address, slot, value))

    def discard(self):
        self.discarded = True


def test_writers_fields_and_row_stay_in_lockstep():
    result = SweepResult("forkyard", 10, 90.0, 1200.0, 160.0, 113.7, 0, True)
    assert list(_writers_row(result).keys()) == WRITERS_FIELDS


def test_writer_value_is_unique_per_writer_and_per_round():
    """A value shared between two writers would make a leak look like a
    correct read; a value reused across rounds would hide a stale read."""
    values = [writer_value(w, r) for w in range(50) for r in range(20)]
    assert len(set(values)) == len(values)


def test_run_writer_writes_twice_per_round_and_verifies_its_own_value():
    backend = IsolatedBackend()
    outcome = run_writer(lambda: backend, writer_index=3, rounds=4)

    assert outcome.ok and outcome.error == ""
    # Two writes per round: one balance, one shared storage slot.
    assert outcome.writes == 8
    assert len(backend.storage_writes) == 4
    assert outcome.violations == 0
    assert backend.discarded, "an undiscarded environment would inflate the next sweep's RSS"


def test_run_writer_targets_the_same_account_and_slot_for_every_writer():
    """Writers aiming at different keys could never collide, so the
    isolation check would pass even on a completely shared environment."""
    first, second = IsolatedBackend(), IsolatedBackend()
    run_writer(lambda: first, 0, 1)
    run_writer(lambda: second, 1, 1)
    assert [w[:2] for w in first.storage_writes] == [w[:2] for w in second.storage_writes]
    assert first.storage_writes[0][0] == bench_architecture.SHARED_CONTRACT


def test_run_writer_counts_a_violation_when_it_reads_someone_elses_value():
    class LeakyBackend(IsolatedBackend):
        def web3(self):
            # Whatever this writer wrote, it reads back a value that is not
            # its own — exactly what a shared environment would produce.
            return types.SimpleNamespace(
                eth=FakeWeb3Eth({bench_architecture.SHARED_ACCOUNT: 1}, None)
            )

    outcome = run_writer(lambda: LeakyBackend(), writer_index=0, rounds=3)
    assert outcome.violations == 3
    assert outcome.ok is True, "the writes themselves succeeded; the isolation did not"


def test_two_writers_sharing_one_state_are_detected_as_a_leak():
    """The real failure mode, made deterministic: two environments over one
    state, and the other writer lands its write between this writer's write
    and its read-back. Concurrency is what makes this happen in the sweep;
    here it is scripted so the test cannot be flaky."""
    shared: dict[str, int] = {}
    other = IsolatedBackend(shared)

    class InterleavedBackend(IsolatedBackend):
        def web3(self):
            other.set_native_balance(bench_architecture.SHARED_ACCOUNT, writer_value(1, 0))
            return super().web3()

    outcome = run_writer(lambda: InterleavedBackend(shared), writer_index=0, rounds=2)

    assert outcome.violations == 2, "every read-back saw writer 1's value, not its own"
    assert summarize("forkyard", 1, [outcome], 50.0, 1000.0).ok is False


def test_run_writer_records_an_acquisition_failure_as_zero_writes():
    def boom():
        raise RuntimeError("anvil did not become ready")

    outcome = run_writer(boom, writer_index=7, rounds=5)
    assert (outcome.ok, outcome.writes, outcome.writer_index) == (False, 0, 7)
    assert "did not become ready" in outcome.error


def test_run_writer_keeps_the_writes_it_managed_before_failing():
    class FlakyBackend(IsolatedBackend):
        def set_storage(self, address, slot, value):
            raise RuntimeError("connection reset")

    outcome = run_writer(lambda: FlakyBackend(), writer_index=0, rounds=3)
    assert outcome.ok is False
    assert outcome.writes == 1, "the balance write landed before the storage write failed"


def test_run_writer_survives_a_failing_discard():
    """Anvil's discard kills a process that may already be gone; that is a
    teardown problem, not a measurement failure."""
    class UndiscardableBackend(IsolatedBackend):
        def discard(self):
            raise RuntimeError("no such process")

    outcome = run_writer(lambda: UndiscardableBackend(), writer_index=0, rounds=1)
    assert outcome.ok is True


def test_run_writers_gives_every_writer_its_own_index():
    seen: list[int] = []
    backends = [IsolatedBackend() for _ in range(6)]

    def factory(i):
        seen.append(i)
        return lambda: backends[i]

    outcomes, wall_ms = run_writers(factory, writers=6, rounds=1)
    assert sorted(o.writer_index for o in outcomes) == list(range(6))
    assert sorted(seen) == list(range(6))
    assert wall_ms > 0


def test_summarize_computes_throughput_and_the_headline_density():
    outcomes = [WriterOutcome(i, writes=20, violations=0, ok=True) for i in range(10)]
    result = summarize("forkyard", 10, outcomes, peak_rss_mb=100.0, wall_clock_ms=2000.0)

    assert result.writes_per_sec == 100.0  # 200 writes in 2 s
    assert result.writers_per_gb == 102.4  # 10 writers per 100 MB
    assert result.ok is True


def test_summarize_fails_the_row_on_any_isolation_violation():
    """A leak makes the memory number meaningless — K environments that
    share state are not K isolated writers, however cheap they were."""
    outcomes = [
        WriterOutcome(0, writes=20, violations=0, ok=True),
        WriterOutcome(1, writes=20, violations=1, ok=True),
    ]
    result = summarize("forkyard", 2, outcomes, peak_rss_mb=50.0, wall_clock_ms=1000.0)
    assert result.isolation_violations == 1
    assert result.ok is False


def test_summarize_fails_the_row_when_a_writer_never_got_an_environment():
    outcomes = [
        WriterOutcome(0, writes=20, violations=0, ok=True),
        WriterOutcome(1, writes=0, violations=0, ok=False, error="RuntimeError('timeout')"),
    ]
    assert summarize("anvil", 2, outcomes, 60.0, 1000.0).ok is False


def test_summarize_reports_zero_density_rather_than_inventing_one():
    """Dividing by an unsampled RSS would print a spectacular number that
    means nothing."""
    outcomes = [WriterOutcome(0, writes=2, violations=0, ok=True)]
    result = summarize("forkyard", 1, outcomes, peak_rss_mb=0.0, wall_clock_ms=100.0)
    assert result.writers_per_gb == 0.0


def test_total_rss_mb_sums_ps_output_in_megabytes(monkeypatch):
    monkeypatch.setattr(
        bench_architecture.subprocess, "run",
        lambda *a, **k: types.SimpleNamespace(stdout=" 30720\n 61440\n", returncode=0),
    )
    assert total_rss_mb({1, 2}) == pytest.approx(90.0)  # ps reports KiB


def test_total_rss_mb_is_zero_without_pids(monkeypatch):
    def fail(*a, **k):
        raise AssertionError("ps must not be spawned for an empty pid set")

    monkeypatch.setattr(bench_architecture.subprocess, "run", fail)
    assert total_rss_mb(set()) == 0.0


def test_process_pids_parses_pgrep_and_ignores_noise(monkeypatch):
    monkeypatch.setattr(
        bench_architecture.subprocess, "run",
        lambda *a, **k: types.SimpleNamespace(stdout="101\n102\nnot-a-pid\n", returncode=0),
    )
    assert process_pids("anvil") == {101, 102}


def test_process_pids_survives_a_machine_without_pgrep(monkeypatch):
    def missing(*a, **k):
        raise FileNotFoundError("pgrep")

    monkeypatch.setattr(bench_architecture.subprocess, "run", missing)
    assert process_pids("anvil") == set()


def test_rss_sampler_excludes_processes_that_were_already_running(monkeypatch):
    """A forkyard the developer left running on another port would
    otherwise be charged to this sweep's memory."""
    sampled: list[set[int]] = []
    monkeypatch.setattr(bench_common, "process_pids", lambda name: {1, 2, 3})
    monkeypatch.setattr(bench_common, "total_rss_mb",
        lambda pids: (sampled.append(set(pids)), 10.0 * len(pids))[1],
    )

    sampler = RssSampler("forkyard", exclude_pids={1, 2})
    assert sampler.sample_once() == 10.0
    assert sampled == [{3}]


def test_rss_sampler_keeps_the_peak_not_the_last_reading(monkeypatch):
    """Anvil's processes are killed by their own writers as they finish, so
    the final reading is near zero; the peak is when they all coexisted."""
    readings = iter([30.0, 1500.0, 20.0])
    monkeypatch.setattr(bench_common, "process_pids", lambda name: {9})
    monkeypatch.setattr(bench_common, "total_rss_mb", lambda pids: next(readings))

    sampler = RssSampler("anvil", exclude_pids=set())
    for _ in range(3):
        sampler.sample_once()
    assert sampler.peak_mb == 1500.0


def test_rss_sampler_records_a_sample_before_the_first_tick(monkeypatch):
    """A one-writer sweep can finish inside a single sampling interval, and
    a zero there would silently become an infinite writers-per-GB."""
    monkeypatch.setattr(bench_common, "process_pids", lambda name: {9})
    monkeypatch.setattr(bench_common, "total_rss_mb", lambda pids: 42.0)

    sampler = RssSampler("forkyard", exclude_pids=set(), interval_s=60.0).start()
    assert sampler.stop() == 42.0


def test_write_results_round_trips_through_csv():
    buf = io.StringIO()
    write_results(buf, [
        SweepResult("forkyard", 50, 180.0, 4000.0, 250.0, 284.4, 0, True),
        SweepResult("anvil", 50, 1600.0, 30000.0, 33.3, 32.0, 0, True),
    ])
    reader = csv.DictReader(io.StringIO(buf.getvalue()))
    rows = list(reader)
    assert reader.fieldnames == WRITERS_FIELDS
    assert rows[0]["writers_per_gb"] == "284.4"
    assert rows[1]["backend"] == "anvil"


def test_shared_targets_are_checksummed():
    """web3.py rejects lowercase hex outright, which would turn every
    writer into an instant client-side failure."""
    assert Web3.to_checksum_address(bench_architecture.SHARED_ACCOUNT) == bench_architecture.SHARED_ACCOUNT
    assert Web3.to_checksum_address(bench_architecture.SHARED_CONTRACT) == bench_architecture.SHARED_CONTRACT


def test_no_two_benchmarks_share_a_default_port():
    """A collision only shows up as one benchmark talking to another's
    leftover process, which reads as a plausible-looking measurement."""
    import bench_cache
    import bench_load

    ports = [
        value
        for module in (bench_architecture, bench_load, bench_cache)
        for name, value in vars(module).items()
        if name.endswith(("_PORT", "_BASE_PORT")) and isinstance(value, int)
    ]
    assert len(ports) == len(set(ports)), sorted(ports)


def test_cli_help_states_the_isolation_requirement(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_architecture.py", "--help"])
    with pytest.raises(SystemExit):
        bench_architecture.writers_main()
    help_text = " ".join(capsys.readouterr().out.split())
    assert "isolation" in help_text
    assert "must be 0 for a row to mean anything" in help_text


def test_writers_cli_refuses_to_run_without_an_endpoint(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["bench_architecture.py"])
    monkeypatch.delenv("RPC_URL", raising=False)
    with pytest.raises(SystemExit):
        bench_architecture.writers_main()
    assert "--rpc-url" in capsys.readouterr().err
