#!/usr/bin/env python3
"""Explicit macOS/Linux fault smoke for the four-child example2 deployment.

    python3 test/supervised_fault_smoke.py
    python3 test/supervised_fault_smoke.py --no-build --artifacts /tmp/mft-smoke

Uses only Python's standard library. Builds examples unless --no-build is given;
never uses example2/data. Retains logs and test databases in the printed artifacts
folder. Run separately from timing-sensitive benchmarks.
"""
import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import random
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import traceback

ROLES = ("procA", "procB", "procC", "apply")


def free_port_range():
    for _ in range(100):
        base = random.SystemRandom().randrange(20000, 55000)
        sockets = []
        try:
            for offset in (0, 1, 2, 10, 11, 12, 20, 21, 22):
                sock = socket.socket()
                sockets.append(sock)
                sock.bind(("127.0.0.1", base + offset))
            return base
        except OSError:
            pass
        finally:
            for sock in sockets:
                sock.close()
    raise RuntimeError("cannot find unused cluster ports")


def processes():
    lines = subprocess.check_output(
        ["ps", "-axo", "pid=,ppid=,stat=,command="], text=True
    ).splitlines()
    table = {}
    for line in lines:
        fields = line.strip().split(None, 3)
        if len(fields) == 4:
            pid, parent, status, command = fields
            table[int(pid)] = (int(parent), status, command)
    return table


class Smoke:
    def __init__(self, binaries, artifacts, base):
        self.binaries = binaries
        self.artifacts = artifacts
        self.data = artifacts / "data"
        self.data.mkdir()
        self.base = base
        self.parents = {}
        self.logs = {}
        self.owned = set()
        self.events = []
        self.started = time.monotonic()

    def record(self, event, **details):
        event = dict(seconds=round(time.monotonic() - self.started, 3),
                     event=event, **details)
        self.events.append(event)
        print(json.dumps(event), flush=True)
        (self.artifacts / "events.json").write_text(json.dumps(self.events, indent=2))

    def wait(self, description, predicate, timeout=25):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            for node, parent in self.parents.items():
                if node != "1" and parent.poll() is not None:
                    raise AssertionError(f"node {node} exited during {description}")
            time.sleep(.02)
        raise TimeoutError(description)

    def start(self, node):
        logfile = open(self.artifacts / f"node{node}.log", "wb")
        self.logs[node] = logfile
        env = dict(os.environ, MONSTERFT_BASE_PORT=str(self.base),
                   MONSTERFT_DATA_DIR=str(self.data))
        parent = subprocess.Popen(
            [str(self.binaries / "example2-entry"), node], env=env,
            stdout=logfile, stderr=subprocess.STDOUT, start_new_session=True,
        )
        self.parents[node] = parent
        self.owned.add(parent.pid)
        self.record("parent_started", node=node, pid=parent.pid)

    def children(self, node):
        found = {}
        for pid, (parent, _, command) in processes().items():
            if parent != self.parents[node].pid:
                continue
            for role in ROLES:
                prefix = str(self.binaries / f"example2-{role}") + f" {node} "
                if command.startswith(prefix):
                    found[role] = pid
                    self.owned.add(pid)
        return found

    def query(self, node, suffix, query):
        # Close every probe promptly. Opening a read-only WAL database without
        # sidecars can fail; an ordinary connection issuing SELECT is sufficient.
        path = self.data / (node + suffix)
        if not path.is_file():
            raise FileNotFoundError(path)
        with closing(sqlite3.connect(str(path), timeout=.2)) as db:
            return db.execute(query).fetchall()

    def state(self, node):
        try:
            n = self.query(node, "2", "SELECT n FROM counter WHERE id=1")[0][0]
            seq, generation = self.query(node, "2", "SELECT seq,hex(generation) FROM monsterft_meta_apply")[0]
            term, vote = self.query(node, "1a", "SELECT current_term,voted_for FROM raft_election")[0]
            applied, pending, repair = self.query(node, "1a", "SELECT applied_seq,pending_cmd_seq,repair_state FROM monsterft_meta")[0]
            return dict(n=n, receipt_seq=seq, generation=generation, term=term,
                        vote=vote, applied_seq=applied, pending=pending, repair=repair)
        except (sqlite3.Error, OSError, IndexError):
            return None

    def stable_children(self, node):
        candidate, since = {}, time.monotonic()

        def check():
            nonlocal candidate, since
            current = self.children(node)
            if current != candidate:
                candidate, since = current, time.monotonic()
            return current if len(current) == 4 and time.monotonic() - since > .8 else None

        # Do not inspect retained databases while initial generation claims and
        # journal-mode configuration may still require SQLite's opening locks.
        children = self.wait(f"node {node}: four stable owners", check)
        self.wait(f"node {node}: initialized receipt", lambda: self.state(node))
        self.record("owners_ready", node=node, children=children, state=self.state(node))
        return children

    def replacement(self, node, previous):
        reaped = False

        def check():
            nonlocal reaped
            existing = processes()
            alive = set(previous.values()).intersection(existing)
            current = self.children(node)
            new = set(current.values()).difference(previous.values())
            assert not (new and alive), ("replacement before old owner exits", alive, new)
            if not alive and not reaped:
                reaped = True
                self.record("old_owners_reaped", node=node, pids=list(previous.values()))
            return current if len(current) == 4 and not alive else None

        current = self.wait(f"node {node}: replacement", check)
        self.record("replacement_started", node=node, children=current)
        return self.stable_children(node)

    def kill_apply(self, node, previous):
        assert previous["apply"] in self.owned
        os.kill(previous["apply"], signal.SIGKILL)
        self.record("apply_sigkill", node=node, pid=previous["apply"])
        return self.replacement(node, previous)

    def run(self):
        self.start("3")
        self.start("2")
        old = self.stable_children("2")
        self.stable_children("3")
        current = self.kill_apply("2", old)
        # Freeze a follower's Apply so the healthy leader can return command
        # results before the follower's five-second execution timeout expires.
        for _ in range(5):
            a, b = self.state("2"), self.state("3")
            if a and b and a["vote"] == b["vote"] == "3" and a["term"] == b["term"]:
                break
            current = self.kill_apply("2", self.children("2"))
        else:
            raise AssertionError("could not establish node3 leadership")
        assert self.state("2")["n"] == self.state("3")["n"] == 0
        os.kill(current["apply"], signal.SIGSTOP)
        self.record("apply_sigstop", node="2", pid=current["apply"])
        self.start("1")
        self.wait("ten-command demo completion", lambda: self.parents["1"].poll() is not None, 35)
        assert self.parents["1"].returncode == 0, (self.artifacts / "node1.log").read_text()
        output = (self.artifacts / "node1.log").read_text()
        assert [int(line.split(": ")[1]) for line in output.splitlines() if line.startswith("CMD ")] == list(range(1, 11)), output
        self.record("demo_completed", output=output)
        current = self.replacement("2", current)
        assert "apply request timeout" in (self.artifacts / "node2.log").read_text()
        self.wait("three counters reach exactly ten", lambda: all(
            (s := self.state(node)) and s["n"] == 10 for node in ("1", "2", "3")
        ))
        before = {node: self.state(node) for node in ("1", "2", "3")}
        assert all(s["repair"] == 0 for s in before.values()), before
        self.record("consistent_once_only_results", states=before)
        # A second crash starts from committed work. Receipt import must not run
        # any of those ten application callbacks again.
        self.kill_apply("2", current)
        after = {node: self.state(node) for node in ("1", "2", "3")}
        assert all(s["n"] == 10 and s["receipt_seq"] == before[node]["receipt_seq"]
                   for node, s in after.items()), after
        def copies_agree(node):
            query = "SELECT applied_seq,hex(applied_entry_hash),repair_state,pending_cmd_seq,hex(applied_local_digest) FROM monsterft_meta"
            copies = [self.query(node, suffix, query) for suffix in ("1a", "1b", "1c")]
            return copies[0] == copies[1] == copies[2]

        for node in ("2", "3"):
            self.wait(f"node {node}: committed DB1 agreement", lambda: copies_agree(node))
        self.record("retained_receipts_not_reexecuted", states=after)
        for node in ("2", "3"):
            self.parents[node].terminate()
        for node in ("2", "3"):
            assert self.parents[node].wait(timeout=15) == 0
        time.sleep(.2)
        assert not self.owned.intersection(processes()), "orphaned process"
        self.record("success_no_orphans")

    def cleanup(self):
        for parent in self.parents.values():
            if parent.poll() is None:
                parent.terminate()
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline and any(p.poll() is None for p in self.parents.values()):
            time.sleep(.05)
        table = processes()
        for pid in self.owned.intersection(table):
            if table[pid][2].startswith(str(self.binaries / "example2-")):
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for parent in self.parents.values():
            parent.wait(timeout=5)
        for logfile in self.logs.values():
            logfile.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--base-port", type=int)
    parser.add_argument("--binary-dir", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    if not args.no_build:
        subprocess.run(["cargo", "build", "--locked", "--examples"], cwd=root, check=True)
    binaries = (args.binary_dir or root / "target/debug/examples").resolve()
    for role in ("entry",) + ROLES:
        assert (binaries / f"example2-{role}").is_file(), f"missing example2-{role} binary"
    artifacts = args.artifacts or Path(tempfile.mkdtemp(prefix="monsterft-supervised-"))
    artifacts.mkdir(parents=True, exist_ok=True)
    assert not any(artifacts.iterdir()), "artifacts directory must be empty"
    print(f"Artifacts: {artifacts}", flush=True)
    smoke = Smoke(binaries, artifacts, args.base_port or free_port_range())
    try:
        smoke.run()
    except BaseException as error:
        smoke.record("failure", error=str(error), traceback=traceback.format_exc())
        raise
    finally:
        smoke.cleanup()


if __name__ == "__main__":
    main()
