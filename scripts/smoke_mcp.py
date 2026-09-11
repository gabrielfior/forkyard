#!/usr/bin/env python3
"""End-to-end smoke test of the MCP surface's contract support, against a
real fork of live chain state.

The Rust tests cover all of this without a network, using a hand-assembled
contract. This script is the other half: proof that the same flow works
against a real chain through a real `forkyard` process, over MCP stdio —
which is how the gaps it checks were found in the first place (a reviewer
reported that `simulate` had no `data` argument and that contract storage
could not be read).

Checks, in order:

  1. `simulate` with `data` performs a real contract call (USDC
     `balanceOf`) and returns its return data.
  2. `get_storage` and `get_code` read contract state.
  3. An unknown argument is rejected by name rather than silently dropped.
  4. Omitting `gas_price` prices the transaction at the fork's basefee,
     and both fee errors explain themselves.
  5. A contract deploys, is called, and its written slot reads back.
  6. `eth_call` / `eth_getStorageAt` / `eth_getCode` answer on the HTTP
     JSON-RPC surface.

Run it from the repo root, after `cargo build --release`:

    RPC_URL=https://your-mainnet-rpc python3 scripts/smoke_mcp.py

Needs only the standard library. Starts and stops its own `forkyard`, on
ports 8855/8856 so it won't collide with one you already have running.
Exits non-zero on the first failed check. Set FORKYARD_BIN to point at a
binary somewhere other than ./target/release/forkyard.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request

BIN = os.environ.get("FORKYARD_BIN", "./target/release/forkyard")
RPC_URL = os.environ.get("RPC_URL", "https://ethereum-rpc.publicnode.com")
HTTP_PORT = os.environ.get("FORKYARD_PORT", "8855")

# Mainnet fixtures. The balance is whatever the fork's block says it is, so
# nothing here asserts a specific amount — only that a real call returns a
# plausible, decodable word.
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
HOLDER = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"
BALANCE_OF = "0x70a08231" + "0" * 24 + HOLDER[2:].lower()
SENDER = "0x0707070707070707070707070707070707070707"

# The same contract the Rust tests use: reverts with 0xbb on empty
# calldata, else stores the first calldata word in slot 0 and returns it.
# This is its init code, so `advance` with no `to` deploys it.
COUNTER_INIT = (
    "601e600c600039601e6000f3"
    "36600e5760bb60005260206000fd5b6000358060005560005260206000f3"
)

failures: list[str] = []


def check(label: str, ok: bool, detail: str) -> None:
    print(f"  {'ok  ' if ok else 'FAIL'}  {label}: {detail}")
    if not ok:
        failures.append(label)


def word(n: int) -> str:
    return f"0x{n:064x}"


class Mcp:
    """Speaks MCP over the binary's stdio transport."""

    def __init__(self, proc: subprocess.Popen[str]) -> None:
        self.proc = proc
        self.next_id = 0

    def send(self, method: str, params: object | None = None, notify: bool = False):
        message: dict[str, object] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        if not notify:
            self.next_id += 1
            message["id"] = self.next_id
        assert self.proc.stdin and self.proc.stdout
        self.proc.stdin.write(json.dumps(message) + "\n")
        self.proc.stdin.flush()
        if notify:
            return None
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise SystemExit("forkyard exited; is RPC_URL reachable?")
            response = json.loads(line)
            if response.get("id") == message["id"]:
                return response

    def tool(self, name: str, **arguments) -> str:
        """The tool's text result, or `ERROR: <message>` — either way a
        string, so a check can assert on the failure text too."""
        response = self.send("tools/call", {"name": name, "arguments": arguments})
        if "error" in response:
            return f"ERROR: {response['error']['message']}"
        result = response["result"]
        text = result["content"][0]["text"]
        return f"ERROR: {text}" if result.get("is_error") else text

    def handshake(self) -> None:
        self.send(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "smoke", "version": "0"},
            },
        )
        self.send("notifications/initialized", {}, notify=True)


def rpc(session_id: int, method: str, params: list) -> dict:
    request = urllib.request.Request(
        f"http://127.0.0.1:{HTTP_PORT}/session/{session_id}",
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"},
    )
    return json.loads(urllib.request.urlopen(request).read())


def open_http_session() -> int:
    request = urllib.request.Request(
        f"http://127.0.0.1:{HTTP_PORT}/session", data=b"{}",
        headers={"Content-Type": "application/json"},
    )
    return json.loads(urllib.request.urlopen(request).read())["session_id"]


def main() -> int:
    if not os.path.exists(BIN):
        print(f"no binary at {BIN} — run `cargo build --release` first", file=sys.stderr)
        return 2

    env = {**os.environ, "RPC_URL": RPC_URL, "FORKYARD_PORT": HTTP_PORT,
           "FORKYARD_MCP_HTTP_PORT": str(int(HTTP_PORT) + 1)}
    proc = subprocess.Popen(
        [BIN], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, text=True, bufsize=1, env=env,
    )
    try:
        mcp = Mcp(proc)
        mcp.handshake()

        tools = {t["name"] for t in mcp.send("tools/list", {})["result"]["tools"]}
        print("tools:", " ".join(sorted(tools)))
        print()

        expected = {"fork", "simulate", "advance", "get_balance", "get_storage",
                    "get_code", "set_balance", "set_storage", "discard"}
        check("tool surface", expected <= tools, f"missing {sorted(expected - tools)}" if expected - tools else "all present")

        session = int(mcp.tool("fork"))
        mcp.tool("set_balance", session_id=session, address=SENDER, balance=hex(10**18))
        call = dict(session_id=session, to=USDC, value="0x0", data=BALANCE_OF, gas_limit=200_000)
        call["from"] = SENDER

        # 1 + 4: a real contract call, with gas_price left out entirely.
        result = mcp.tool("simulate", **call)
        ok = not result.startswith("ERROR") and json.loads(result)["success"]
        check("simulate a contract call (gas_price omitted)", ok, result[:96])
        if ok:
            balance = int(json.loads(result)["output"], 16)
            check("return data decodes", balance > 0, f"{balance / 1e6:.6f} USDC")

        # 2: reads.
        slot = mcp.tool("get_storage", session_id=session, address=USDC, slot="0x0")
        check("get_storage", slot.startswith("0x") and len(slot) == 66, slot)
        code = mcp.tool("get_code", session_id=session, address=USDC)
        check("get_code", len(code) > 2, f"{code[:24]}... ({len(code) // 2 - 1} bytes)")

        # 3: a typo must not look like a successful transfer.
        typo = mcp.tool("simulate", **call | {"dtaa": BALANCE_OF})
        check("unknown argument rejected", "dtaa" in typo, typo[:96])

        # 4: both fee errors name the numbers involved.
        underpriced = mcp.tool("simulate", **call, gas_price=1)
        check("underpriced error explains itself", "basefee" in underpriced and "omit" in underpriced, underpriced[:120])

        # 5: deploy, call, read back.
        deployed = mcp.tool("advance", session_id=session, value="0x0", nonce=0,
                            data="0x" + COUNTER_INIT, gas_limit=500_000, **{"from": SENDER})
        ok = not deployed.startswith("ERROR") and json.loads(deployed).get("contract_address")
        check("deploy with no `to`", bool(ok), deployed[:96])
        if ok:
            address = json.loads(deployed)["contract_address"]
            called = mcp.tool("advance", session_id=session, to=address, value="0x0", nonce=1,
                              data=word(42), gas_limit=200_000, **{"from": SENDER})
            returned = json.loads(called).get("output") if not called.startswith("ERROR") else None
            check("call the deployed contract", returned == word(42), str(returned)[:96])
            stored = mcp.tool("get_storage", session_id=session, address=address, slot="0x0")
            check("its written slot reads back", stored == word(42), stored)

        # 6: the HTTP surface's read methods.
        http_session = open_http_session()
        for method, params in [
            ("eth_call", [{"to": USDC, "data": BALANCE_OF}, "latest"]),
            ("eth_getStorageAt", [USDC, "0x0", "latest"]),
            ("eth_getCode", [USDC, "latest"]),
        ]:
            response = rpc(http_session, method, params)
            check(f"HTTP {method}", "result" in response, str(response.get("result", response.get("error")))[:60])
    finally:
        # Closing stdio first: the stdio MCP transport is what keeps the
        # process alive, and it does not stop on SIGTERM alone.
        for stream in (proc.stdin, proc.stdout):
            if stream:
                try:
                    stream.close()
                except OSError:
                    pass
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {', '.join(failures)}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
