#!/usr/bin/env python3
"""Abrupt SQLite process-death probes for the proposed DB2 owner-page schema.

Children signal an exact SQL boundary over a pipe and then block. The parent
sends SIGKILL; no sleeps, orderly SQLite close, or patchset application is used.
This models the Apply transaction/receipt ordering with the SQLite C session
API; it is not a production MonsterApply process integration test.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile

from sqlite_probe import DB, OWNER_SQL, Session, canonical_pages, metadata


def token(label):
    return hashlib.sha256(label.encode()).digest()[:16]


def blob(data):
    return "x'%s'" % data.hex()


def configure(db):
    db.sql("PRAGMA cache_size=8; PRAGMA cache_spill=ON; PRAGMA wal_autocheckpoint=0; PRAGMA secure_delete=OFF")


def claim(db, value):
    db.sql("BEGIN IMMEDIATE; UPDATE monsterft_local_owner SET generation=%s WHERE id=1; COMMIT" % blob(value))


def wal_state(path):
    path = Path(str(path) + "-wal")
    if not path.exists():
        return {"exists": False, "bytes": 0, "frames": 0}
    contents = path.read_bytes()
    if len(contents) < 32:
        return {"exists": True, "bytes": len(contents), "frames": 0}
    page_size = int.from_bytes(contents[8:12], "big")
    frame_size = 24 + page_size
    count = (len(contents) - 32) // frame_size
    markers = [int.from_bytes(contents[32 + i * frame_size + 4:40 + i * frame_size], "big")
               for i in range(count)]
    return {"exists": True, "bytes": len(contents), "pageSize": page_size,
            "frames": count, "commitFrames": sum(marker != 0 for marker in markers),
            "lastFrameDatabaseSize": markers[-1] if markers else None}


def mutate(db, seq):
    start = (seq - 1) * 512
    for i in range(512):
        payload = hashlib.sha256(("payload:%d:%d" % (seq, i)).encode()).digest()
        payload = (payload * 100)[:2048 + i % 7 * 97]
        db.sql("INSERT INTO app(id,bucket,payload) VALUES(%d,%d,%s)" %
               (start + i, i * 17 % 23, blob(payload)))
    for i in range(0, 512, 7):
        db.sql("UPDATE app SET payload=%s WHERE id=%d" %
               (blob(token("short:%d:%d" % (seq, i))), start + i))
    db.sql("DELETE FROM app WHERE id >= %d AND id < %d AND id %% 11 = 0" % (start, start + 512))


def command(db, seq, expected_token, commit=True):
    db.sql("BEGIN IMMEDIATE")
    assert db.query("SELECT generation FROM monsterft_local_owner WHERE id=1")[0][0] == expected_token
    receipt = db.query("SELECT seq,entry_hash,local_digest FROM monsterft_meta_apply WHERE id=1")[0]
    entry_hash = token("entry:%d" % seq)
    if receipt[0] == seq:
        assert receipt[1] == entry_hash
        db.sql("COMMIT")
        return {"duplicateSkipped": True, "seq": seq, "receiptDigest": receipt[2].hex()}
    assert receipt[0] == seq - 1, (receipt[0], seq)
    session = Session(db)
    try:
        mutate(db, seq)
        patchset = session.patchset()
    finally:
        session.close()
    patch_hash = hashlib.sha256(patchset).hexdigest()
    if commit:
        db.sql("UPDATE monsterft_meta_apply SET seq=%d,entry_hash=%s,local_digest=%s WHERE id=1; COMMIT" %
               (seq, blob(entry_hash), blob(bytes.fromhex(patch_hash)[:16])))
    return {"seq": seq, "patchsetBytes": len(patchset), "patchsetSha256": patch_hash,
            "committed": commit}


def child(path, seq, phase, expected_token):
    db = DB(path)
    configure(db)
    result = command(db, seq, expected_token, phase == "after-commit")
    result.update({"phase": phase, "pid": os.getpid(), "wal": wal_state(path)})
    print(json.dumps(result), flush=True)
    # This is a control signal to the test parent, not an Apply ACK. Neither
    # normal close nor a second SQL statement occurs before the parent kills us.
    sys.stdin.buffer.read(1)
    raise AssertionError("parent must SIGKILL the child")


def kill_at_boundary(path, seq, phase, expected_token):
    process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                                "--child", str(path), str(seq), phase, expected_token.hex()],
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True)
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    try:
        if not selector.select(timeout=45):
            raise AssertionError("child did not reach SQL boundary")
        line = process.stdout.readline()
        if not line:
            raise AssertionError("child failed before boundary: " + process.stderr.read())
        ready = json.loads(line)
        assert ready["phase"] == phase
        os.kill(process.pid, signal.SIGKILL)
        _, errors = process.communicate(timeout=10)
        assert process.returncode == -signal.SIGKILL, (process.returncode, errors)
        ready["exitSignal"] = "SIGKILL"
        return ready
    finally:
        selector.close()
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=10)


def different_pages(left, right):
    return [p for p in sorted(left.keys() | right.keys()) if left.get(p) != right.get(p)]


def run(work, cycles):
    seed_path = work / "seed.db"
    seed = DB(seed_path)
    configure(seed)
    seed.sql("BEGIN IMMEDIATE")
    seed.sql(OWNER_SQL)
    seed.sql("INSERT INTO monsterft_local_owner VALUES(1,%s)" % blob(token("seed")))
    seed.sql("CREATE TABLE monsterft_meta_apply(id INTEGER PRIMARY KEY CHECK(id=1),seq INTEGER NOT NULL,entry_hash BLOB,local_digest BLOB) STRICT")
    seed.sql("INSERT INTO monsterft_meta_apply VALUES(1,0,%s,%s)" % (blob(token("entry:0")), blob(bytes(16))))
    seed.sql("CREATE TABLE app(id INTEGER PRIMARY KEY,bucket INTEGER NOT NULL,payload BLOB NOT NULL) STRICT; CREATE INDEX app_bucket ON app(bucket); COMMIT")
    info = metadata(seed)
    seed.close()
    paths = [work / (name + ".db") for name in ["persistent-a", "uncommitted-crash-b", "committed-crash-c"]]
    tokens = [token("initial:%d" % i) for i in range(3)]
    for path, value in zip(paths, tokens):
        shutil.copyfile(seed_path, path)
        db = DB(path)
        configure(db)
        claim(db, value)
        db.close()
    a = DB(paths[0])
    configure(a)
    results = []
    for seq in range(1, cycles + 1):
        baseline = canonical_pages(a)
        result_a = command(a, seq, tokens[0])
        killed_b = kill_at_boundary(paths[1], seq, "uncommitted", tokens[1])
        assert killed_b["wal"]["frames"] > 100, killed_b
        assert killed_b["wal"]["lastFrameDatabaseSize"] == 0, killed_b
        b = DB(paths[1])
        configure(b)
        previous_receipt = b.query("SELECT seq FROM monsterft_meta_apply WHERE id=1")[0][0]
        assert previous_receipt == seq - 1
        recovered_before_replay = different_pages(baseline, canonical_pages(b))
        assert not recovered_before_replay, recovered_before_replay
        tokens[1] = token("recovered:b:%d" % seq)
        claim(b, tokens[1])
        result_b = command(b, seq, tokens[1])

        killed_c = kill_at_boundary(paths[2], seq, "after-commit", tokens[2])
        assert killed_c["wal"]["frames"] > 100, killed_c
        assert killed_c["wal"]["lastFrameDatabaseSize"] > 0, killed_c
        c = DB(paths[2])
        configure(c)
        current_receipt = c.query("SELECT seq FROM monsterft_meta_apply WHERE id=1")[0][0]
        assert current_receipt == seq
        tokens[2] = token("recovered:c:%d" % seq)
        claim(c, tokens[2])
        skipped_c = command(c, seq, tokens[2])
        assert skipped_c["duplicateSkipped"]

        ap, bp, cp = canonical_pages(a), canonical_pages(b), canonical_pages(c)
        diff_ab, diff_ac = different_pages(ap, bp), different_pages(ap, cp)
        patch_hashes = [result_a["patchsetSha256"], killed_b["patchsetSha256"],
                        result_b["patchsetSha256"], killed_c["patchsetSha256"]]
        integrity = [db.query("PRAGMA integrity_check") for db in [a, b, c]]
        assert not diff_ab and not diff_ac, (diff_ab, diff_ac)
        assert len(set(patch_hashes)) == 1, patch_hashes
        assert integrity == [[("ok",)]] * 3, integrity
        results.append({"seq": seq, "pass": True, "pageCount": len(ap),
                        "uncommittedCrash": killed_b, "committedCrash": killed_c,
                        "bRecoveredReceiptBeforeReplay": previous_receipt,
                        "bRecoveredPageDifferencesBeforeReplay": recovered_before_replay,
                        "cRecoveredReceiptBeforeDuplicate": current_receipt,
                        "cDuplicate": skipped_c,
                        "canonicalDifferentPagesAB": diff_ab, "canonicalDifferentPagesAC": diff_ac,
                        "patchsetsAllEqual": True, "patchsetSha256": patch_hashes[0],
                        "integrityCheck": [rows[0][0] for rows in integrity]})
        b.close()
        c.close()
    a.close()
    return {"metadata": info, "workDirectory": str(work), "cycles": cycles,
            "scope": "SQLite C session API model of Apply; captures/hashes patchsets only",
            "allPassed": all(result["pass"] for result in results), "results": results}


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        child(Path(sys.argv[2]), int(sys.argv[3]), sys.argv[4], bytes.fromhex(sys.argv[5]))
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--output", type=Path)
        parser.add_argument("--work-dir", type=Path)
        parser.add_argument("--cycles", type=int, default=3)
        args = parser.parse_args()
        assert args.cycles >= 1
        work = Path(tempfile.mkdtemp(prefix="monsterft-db2-crash-", dir=str(args.work_dir) if args.work_dir else None))
        report = run(work, args.cycles)
        rendered = json.dumps(report, indent=2) + "\n"
        if args.output:
            args.output.write_text(rendered)
        print(rendered)
