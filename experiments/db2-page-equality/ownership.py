#!/usr/bin/env python3
"""Probe the proposed local ownership page; never modifies production databases.

Run with SQLITE_PROBE_LIB pointing at a SQLite library compiled with DBPAGE.
All page comparisons use sqlite_dbpage in a read transaction, with WAL/FULL.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tempfile

from sqlite_probe import DB, lib


OWNER = "monsterft_local_owner"
OWNER_SQL = """CREATE TABLE monsterft_local_owner (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  generation BLOB NOT NULL CHECK (length(generation) = 16)
) STRICT"""
RECEIPT_SQL = """CREATE TABLE monsterft_meta_apply (
  id INTEGER PRIMARY KEY CHECK(id = 1), seq INTEGER NOT NULL,
  entry_hash BLOB, local_digest BLOB
) STRICT"""


def blob(value):
    return "x'%s'" % value.hex()


def token(label):
    return hashlib.sha256(label.encode()).digest()[:16]


def claim(db, value):
    db.sql("BEGIN IMMEDIATE; UPDATE %s SET generation=%s WHERE id=1; COMMIT" %
           (OWNER, blob(value)))


def varint(buf, start):
    value = 0
    for index in range(9):
        byte = buf[start + index]
        value = (value << (8 if index == 8 else 7)) | (byte if index == 8 else byte & 127)
        if index == 8 or byte < 128:
            return value, index + 1
    raise AssertionError("invalid varint")


def owner_snapshot(db):
    """Validate row and actual one-cell leaf in the same read snapshot."""
    db.sql("BEGIN")
    try:
        schema = db.query("SELECT type,rootpage,sql FROM sqlite_schema WHERE name='%s'" % OWNER)
        if len(schema) != 1 or schema[0][0] != "table":
            raise ValueError("owner schema missing")
        if " ".join(schema[0][2].lower().split()) != " ".join(OWNER_SQL.lower().split()):
            raise ValueError("owner schema changed")
        root = schema[0][1]
        if db.query("PRAGMA index_list('%s')" % OWNER):
            raise ValueError("owner indexes are not allowed")
        if db.query("SELECT name FROM sqlite_schema WHERE type='trigger' AND tbl_name='%s'" % OWNER):
            raise ValueError("owner triggers are not allowed")
        rows = db.query("SELECT id,generation FROM %s" % OWNER)
        if len(rows) != 1 or rows[0][0] != 1:
            raise ValueError("owner row missing or not singleton")
        value = rows[0][1]
        if not isinstance(value, bytes) or len(value) != 16 or not any(value):
            raise ValueError("invalid local token")
        pages = dict(db.query("SELECT pgno,data FROM sqlite_dbpage ORDER BY pgno"))
        page = pages[root]
        if root <= 1 or page[0] != 13 or int.from_bytes(page[3:5], "big") != 1:
            raise ValueError("owner is not a singleton table leaf")
        cell = int.from_bytes(page[8:10], "big")
        payload, count = varint(page, cell)
        rowid, size = varint(page, cell + count)
        if rowid != 1 or payload != 19 or page[cell + count + size:cell + count + size + 3] != bytes([3, 0, 44]):
            raise ValueError("owner record shape changed")
        if page[cell + count + size + 3:cell + count + size + payload] != value:
            raise ValueError("owner record/token mismatch")
        if payload + count + size > len(page) - cell:
            raise ValueError("owner overflow not allowed")
        return root, value, pages
    finally:
        db.sql("ROLLBACK")


def canon(pages, root, template):
    result = dict(pages)
    header = bytearray(result[1])
    header[24:28] = b"\0" * 4
    header[92:96] = b"\0" * 4
    result[1] = bytes(header)
    result[root] = template
    return result


def differences(left, right):
    return [page for page in sorted(left.keys() | right.keys()) if left.get(page) != right.get(page)]


def compare(left, right, template):
    lr, lt, lp = owner_snapshot(left)
    rr, rt, rp = owner_snapshot(right)
    return {
        "ownerRoots": [lr, rr], "pageCounts": [len(lp), len(rp)],
        "rawDifferentPages": differences(lp, rp),
        "canonicalDifferentPages": differences(canon(lp, lr, template), canon(rp, rr, template)),
    }


def create(db, value, leading=False):
    db.sql("BEGIN IMMEDIATE")
    if leading:
        db.sql("CREATE TABLE temporary_predecessor(id INTEGER PRIMARY KEY,v BLOB)")
    db.sql(OWNER_SQL)
    db.sql("INSERT INTO %s VALUES(1,%s)" % (OWNER, blob(value)))
    db.sql(RECEIPT_SQL)
    db.sql("INSERT INTO monsterft_meta_apply VALUES(1,-1,NULL,NULL)")
    db.sql("CREATE TABLE app(id INTEGER PRIMARY KEY,v BLOB); CREATE INDEX app_v ON app(v)")
    db.sql("COMMIT")


def update_receipt(db, seq):
    digest = token("receipt:%d" % seq)
    db.sql("BEGIN IMMEDIATE; UPDATE monsterft_meta_apply SET seq=%d,entry_hash=%s,local_digest=%s WHERE id=1 AND seq < %d" %
           (seq, blob(digest), blob(digest), seq))
    assert db.query("SELECT changes()") == [(1,)]
    db.sql("COMMIT")


def run(work):
    results = []
    opened = []

    def connect(path):
        db = DB(path)
        opened.append(db)
        return db

    template_db = connect(work / "template.db")
    template_db.sql(OWNER_SQL)
    template_db.sql("INSERT INTO %s VALUES(1,zeroblob(16))" % OWNER)
    template = template_db.pages()[2]

    seed = connect(work / "seed.db")
    create(seed, token("seed"))
    info = {
        "sqliteVersion": lib.sqlite3_libversion().decode(),
        "sourceId": seed.query("SELECT sqlite_source_id()")[0][0],
        "compileOptions": [row[0] for row in seed.query("PRAGMA compile_options")],
        "settings": {name: seed.query("PRAGMA " + name)[0][0] for name in
                     ["journal_mode", "synchronous", "page_size", "secure_delete", "auto_vacuum", "wal_autocheckpoint"]},
        "pageReader": "sqlite_dbpage inside explicit read transactions",
        "ownerShapeCheck": "one table-leaf page, one cell, exact 19-byte record payload; no indexes",
    }
    seed.close()
    opened.remove(seed)

    def pair(name, seed_path=work / "seed.db"):
        a_path, b_path = work / (name + "-a.db"), work / (name + "-b.db")
        shutil.copyfile(seed_path, a_path)
        shutil.copyfile(seed_path, b_path)
        return connect(a_path), connect(b_path)

    a, b = pair("rotations")
    c_path = work / "rotations-c.db"
    shutil.copyfile(work / "seed.db", c_path)
    c = connect(c_path)
    tokens = []
    tail = [255, 256, 32767, 32768, 65535, 65536, 8388607, 8388608,
            2147483647, 2147483648, 140737488355327, 140737488355328,
            72057594037927935, 72057594037927936, 4611686018427387904,
            9223372036854775807]
    boundaries = [0, 1, 127, 128] + tail
    c_restarts = {3, 11, 23, 79, 180}
    checks = 0
    max_pages = 0
    raw_pages = set()
    for index in range(256):
        for name, db in [("a", a), ("b", b), ("c", c)]:
            current = token("rotation:%s:%d" % (name, index))
            tokens.append(current)
            claim(db, current)
            update_receipt(db, index if index < 240 else tail[index - 240])
            # Shared changes grow/split the application table and index.
            db.sql("BEGIN IMMEDIATE; INSERT INTO app VALUES(%d,%s); COMMIT" %
                   (index, blob(bytes([index % 251]) * (90 + index % 13))))
        if index % 7 == 0:
            path = b.path
            b.close()
            opened.remove(b)
            b = connect(path)
            new_token = token("restart:b:%d" % index)
            tokens.append(new_token)
            claim(b, new_token)
        if index in c_restarts:
            path = c.path
            c.close()
            opened.remove(c)
            c = connect(path)
            new_token = token("restart:c:%d" % index)
            tokens.append(new_token)
            claim(c, new_token)
        if index % 17 == 0:
            b.sql("PRAGMA wal_checkpoint(TRUNCATE)")
        result = compare(a, b, template)
        assert not result["canonicalDifferentPages"], result
        assert result["ownerRoots"] == [2, 2], result
        result_c = compare(a, c, template)
        assert not result_c["canonicalDifferentPages"], result_c
        assert result_c["ownerRoots"] == [2, 2], result_c
        raw_pages.update(result["rawDifferentPages"])
        raw_pages.update(result_c["rawDifferentPages"])
        max_pages = max(max_pages, *result["pageCounts"])
        checks += 1
    leaked = []
    for name, db in [("a", a), ("b", b), ("c", c)]:
        root, _, pages = owner_snapshot(db)
        leaked += [(name, page) for page, value in pages.items() if page != root and any(t in value for t in tokens)]
    assert not leaked, leaked
    results.append({"case": "256 shared updates with persistent, repeatedly reopened, and staggered-restart replicas",
                    "pass": True, "threeReplicaComparisons": checks, "reopens": [0, 37, 5],
                    "ownerClaims": [256, 293, 261],
                    "receiptBoundaryValues": boundaries, "maxPageCount": max_pages,
                    "rawDifferentPageUnion": sorted(raw_pages), "tokensOutsideOwnerPage": leaked,
                    "finalAB": result, "finalAC": result_c})

    # Check generation using the same write transaction as the guarded command.
    path = work / "fencing.db"
    shutil.copyfile(work / "seed.db", path)
    old, new = connect(path), connect(path)
    old_token = token("fence:old")
    new_token = token("fence:new")
    claim(old, old_token)
    old.sql("BEGIN IMMEDIATE")
    assert old.query("SELECT generation FROM %s WHERE id=1" % OWNER)[0][0] == old_token
    busy = None
    try:
        new.sql("BEGIN IMMEDIATE")
    except RuntimeError as error:
        busy = str(error)
    assert busy and ("locked" in busy or "busy" in busy.lower()), busy
    old.sql("INSERT INTO app VALUES(1,x'aa'); COMMIT")
    claim(new, new_token)
    old.sql("BEGIN IMMEDIATE")
    stale_rejected = old.query("SELECT generation FROM %s WHERE id=1" % OWNER)[0][0] != old_token
    old.sql("ROLLBACK")
    assert stale_rejected
    assert new.query("SELECT id FROM app") == [(1,)]
    results.append({"case": "writer exclusion and stale owner fence", "pass": True,
                    "competingClaimBlocked": busy, "staleOwnerRejected": stale_rejected})

    # Install a valid template, then claim the recipient token before committing.
    install, source = pair("install")
    recipient_token = token("installation recipient")
    claim(install, token("installation old"))
    install.sql("BEGIN IMMEDIATE; INSERT INTO sqlite_dbpage(pgno,data) VALUES(2,%s); "
                "UPDATE %s SET generation=%s WHERE id=1; COMMIT" %
                (blob(template), OWNER, blob(recipient_token)))
    assert owner_snapshot(install)[1] == recipient_token
    result = compare(install, source, template)
    assert not result["canonicalDifferentPages"], result
    # Rollback also restores the prior ownership page instead of exposing placeholder.
    install.sql("BEGIN IMMEDIATE; INSERT INTO sqlite_dbpage(pgno,data) VALUES(2,%s); ROLLBACK" % blob(template))
    assert owner_snapshot(install)[1] == recipient_token
    results.append({"case": "template installation, fresh recipient claim and rollback", "pass": True,
                    "sameTransactionClaim": True, "result": result})

    corruption_cases = [
        ("zero token", "UPDATE %s SET generation=zeroblob(16)" % OWNER),
        ("wrong token length", "PRAGMA ignore_check_constraints=ON; UPDATE %s SET generation=x'aa'" % OWNER),
        ("wrong singleton id", "PRAGMA ignore_check_constraints=ON; UPDATE %s SET id=2" % OWNER),
        ("extra index", "CREATE INDEX owner_generation ON %s(generation)" % OWNER),
        ("owner trigger", "CREATE TRIGGER owner_side_effect AFTER UPDATE ON %s BEGIN INSERT INTO app VALUES(999,NEW.generation); END" % OWNER),
        ("missing row", "DELETE FROM %s" % OWNER),
        ("broken page header", "INSERT INTO sqlite_dbpage(pgno,data) VALUES(2,zeroblob(%d))" % len(template)),
    ]
    errors = {}
    for index, (name, sql) in enumerate(corruption_cases):
        damaged, _ = pair("damage%d" % index)
        damaged.sql(sql)
        try:
            owner_snapshot(damaged)
        except (ValueError, RuntimeError) as error:
            errors[name] = str(error)
        else:
            raise AssertionError("corrupt owner masked: " + name)
    results.append({"case": "validate owner before masking", "pass": True, "rejected": errors})

    shifted_seed = connect(work / "shifted-seed.db")
    create(shifted_seed, token("shifted seed"), leading=True)
    shifted_seed.sql("DROP TABLE temporary_predecessor")
    shifted_seed.close()
    opened.remove(shifted_seed)
    va, vb = pair("vacuum", work / "shifted-seed.db")
    claim(va, token("vacuum-a"))
    claim(vb, token("vacuum-b"))
    before = compare(va, vb, template)
    assert not before["canonicalDifferentPages"]
    va.sql("VACUUM")
    one_sided = compare(va, vb, template)
    assert one_sided["canonicalDifferentPages"]
    vb.sql("VACUUM")
    both = compare(va, vb, template)
    assert not both["canonicalDifferentPages"], both
    assert before["ownerRoots"] == [3, 3] and both["ownerRoots"] == [2, 2]
    results.append({"case": "VACUUM root relocation", "pass": True, "before": before,
                    "oneSidedVacuum": one_sided, "bothVacuum": both,
                    "rule": "Resolve owner root from validated schema. Local-only VACUUM can break canonical equality."})

    # FULL auto-vacuum moves the last root when an earlier table is dropped.
    av = []
    av_tokens = [token("auto-vacuum-a"), token("auto-vacuum-b")]
    for index in range(2):
        db = connect(work / ("autovacuum%d.db" % index))
        db.sql("PRAGMA auto_vacuum=FULL; VACUUM")
        assert db.query("PRAGMA auto_vacuum") == [(1,)]
        db.sql("CREATE TABLE predecessor(id INTEGER PRIMARY KEY)")
        db.sql(OWNER_SQL)
        db.sql("INSERT INTO %s VALUES(1,%s)" % (OWNER, blob(av_tokens[index])))
        av.append(db)
    initial = compare(av[0], av[1], template)
    for db in av:
        db.sql("DROP TABLE predecessor")
    after = compare(av[0], av[1], template)
    assert not after["canonicalDifferentPages"], after
    assert initial["ownerRoots"] == [4, 4] and after["ownerRoots"] == [3, 3]
    for db in av:
        root, _, pages = owner_snapshot(db)
        assert not any(value in page for number, page in pages.items() if number != root for value in av_tokens)
    results.append({"case": "FULL auto-vacuum moves owner root during shared DROP", "pass": True,
                    "before": initial, "after": after})

    for db in opened:
        db.close()
    return {"metadata": info, "workDirectory": str(work), "results": results,
            "allPassed": all(result["pass"] for result in results)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--work-dir", type=Path)
    args = parser.parse_args()
    work = Path(tempfile.mkdtemp(prefix="monsterft-db2-owner-", dir=str(args.work_dir) if args.work_dir else None))
    report = run(work)
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered)
