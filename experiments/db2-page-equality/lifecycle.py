#!/usr/bin/env python3
"""SQLite-only lifecycle experiments; uses DBPAGE and never applies patchsets.

Run with the experiment's SQLITE_PROBE_LIB setting and sqlite_probe.py.
Outputs JSON observations; temporary databases are retained for inspection.
"""
import argparse
import ctypes as C
import hashlib
import json
import pathlib
import random
import shutil
import tempfile

from sqlite_probe import DB, LIB_PATH, lib


OWNER = "monsterft_local_owner"
SCHEMA = """
CREATE TABLE monsterft_local_owner (
  id INTEGER PRIMARY KEY CHECK(id=1),
  generation BLOB NOT NULL CHECK(length(generation)=16)
) STRICT;
INSERT INTO monsterft_local_owner VALUES(1,x'77777777777777777777777777777777');
CREATE TABLE monsterft_meta_apply (
  id INTEGER PRIMARY KEY CHECK(id=1), seq INTEGER NOT NULL,
  entry_hash BLOB, local_digest BLOB
) STRICT;
INSERT INTO monsterft_meta_apply VALUES(1,0,zeroblob(16),zeroblob(16));
"""


def capture(db, template):
    """Capture metadata and pager bytes in one actual read transaction."""
    # Separate readers avoid warming the writer's page cache or query history.
    db = DB(db.path, configure=False)
    db.sql("BEGIN")
    try:
        root = db.query("SELECT rootpage FROM sqlite_schema WHERE name='%s'" % OWNER)[0][0]
        rows = db.query("SELECT id,generation FROM %s" % OWNER)
        assert len(rows) == 1 and rows[0][0] == 1 and len(rows[0][1]) == 16
        assert any(rows[0][1]), "live owner token must be nonzero"
        raw = dict(db.query("SELECT pgno,data FROM sqlite_dbpage ORDER BY pgno"))
        assert raw[root][0] == 13 and int.from_bytes(raw[root][3:5], 'big') == 1
        assert len(raw[root]) == len(template)
        canonical = dict(raw)
        header = bytearray(raw[1])
        header[24:28] = bytes(4)
        header[92:96] = bytes(4)
        canonical[1] = bytes(header)
        canonical[root] = template
        return {"raw": raw, "canonical": canonical, "owner_root": root}
    finally:
        db.sql("COMMIT")
        db.close()


def page_diff(left, right):
    different = []
    for number in sorted(set(left) | set(right)):
        if left.get(number) == right.get(number):
            continue
        if number not in left or number not in right:
            different.append({"page": number, "missing": "left" if number not in left else "right"})
        else:
            offsets = [i for i, (a, b) in enumerate(zip(left[number], right[number])) if a != b]
            different.append({"page": number, "different_bytes": len(offsets), "first_offsets": offsets[:24]})
    return different


def view_digest(pages):
    h = hashlib.sha256()
    for number, page in sorted(pages.items()):
        h.update(number.to_bytes(8, 'big'))
        h.update(page)
    return h.hexdigest()


def free_pages(pages):
    trunk = int.from_bytes(pages[1][32:36], 'big')
    trunks, leaves = [], []
    while trunk:
        assert trunk not in trunks
        trunks.append(trunk)
        page = pages[trunk]
        count = int.from_bytes(page[4:8], 'big')
        leaves.extend(int.from_bytes(page[8+i*4:12+i*4], 'big') for i in range(count))
        trunk = int.from_bytes(page[:4], 'big')
    assert len(trunks) + len(leaves) == int.from_bytes(pages[1][36:40], 'big')
    return {'trunks': trunks, 'leaves': leaves}


class Suite:
    def __init__(self, root):
        self.root = root
        self.cases = []

    def seed(self, name, extra='', auto_vacuum=0):
        path = self.root / (name + '-seed.db')
        db = DB(path)
        db.sql('PRAGMA auto_vacuum=%d; VACUUM; PRAGMA secure_delete=OFF;' % auto_vacuum)
        db.sql(SCHEMA + extra)
        template = db.pages()[db.query("SELECT rootpage FROM sqlite_schema WHERE name='%s'" % OWNER)[0][0]]
        db.close()
        return path, template

    def copies(self, name, seed, count=2):
        result = []
        for index in range(count):
            path = self.root / ('%s-%d.db' % (name, index))
            shutil.copyfile(seed, path)
            db = DB(path)
            db.sql('PRAGMA secure_delete=OFF;')
            self.claim(db, index, 0)
            result.append(db)
        return result

    @staticmethod
    def claim(db, member, rotation):
        token = hashlib.sha256(('%d:%d' % (member, rotation)).encode()).digest()[:16]
        db.sql("BEGIN IMMEDIATE; UPDATE %s SET generation=x'%s' WHERE id=1; COMMIT;" % (OWNER, token.hex()))

    def compare(self, name, dbs, template, **extra):
        views = [capture(db, template) for db in dbs]
        comparisons = []
        for other in views[1:]:
            canonical_diff = page_diff(views[0]['canonical'], other['canonical'])
            common_free_leaves = set(free_pages(views[0]['raw'])['leaves']) & set(free_pages(other['raw'])['leaves'])
            comparisons.append({
                "raw_differences": page_diff(views[0]['raw'], other['raw']),
                "canonical_differences": canonical_diff,
                "canonical_differences_only_free_leaves": all(d['page'] in common_free_leaves for d in canonical_diff),
            })
        result = {
            'name': name,
            'canonical_equal': all(not item['canonical_differences'] for item in comparisons),
            'page_counts': [len(view['raw']) for view in views],
            'owner_roots': [view['owner_root'] for view in views],
            'canonical_hashes': [view_digest(view['canonical']) for view in views],
            'freelists': [free_pages(view['raw']) for view in views],
            'comparisons': comparisons,
            **extra,
        }
        self.cases.append(result)
        return result

    @staticmethod
    def close(dbs):
        for db in dbs:
            db.close()


def setup_sessions():
    p = C.c_void_p
    for name, args, restype in [
        ('sqlite3session_create', [p, C.c_char_p, C.POINTER(p)], C.c_int),
        ('sqlite3session_attach', [p, C.c_char_p], C.c_int),
        ('sqlite3session_patchset', [p, C.POINTER(C.c_int), C.POINTER(p)], C.c_int),
        ('sqlite3session_delete', [p], None),
        ('sqlite3_free', [p], None),
    ]:
        fn = getattr(lib, name)
        fn.argtypes = args
        fn.restype = restype


def session_start(db):
    session = C.c_void_p()
    assert lib.sqlite3session_create(db.p, b'main', C.byref(session)) == 0
    assert lib.sqlite3session_attach(session, None) == 0
    return session


def session_finish(session):
    length, blob = C.c_int(), C.c_void_p()
    assert lib.sqlite3session_patchset(session, C.byref(length), C.byref(blob)) == 0
    try:
        return C.string_at(blob, length.value)
    finally:
        lib.sqlite3_free(blob)
        lib.sqlite3session_delete(session)


def workload_statements(seq, rng):
    statements = []
    for offset in range(9):
        ident = seq * 9 + offset
        size = rng.choice([0, 1, 50, 270, 1100, 5000, 12000])
        blob = bytes([ident % 251 + 1]) * size
        statements.append("INSERT INTO items VALUES(%d,%d,x'%s');" % (ident, ident % 19, blob.hex()))
    if seq % 2 == 0:
        statements.append("UPDATE items SET v=x'6170706c69636174696f6e' WHERE id%%17=%d;" % (seq % 17))
    if seq % 3 == 0:
        statements.append('DELETE FROM items WHERE id%%%d=0;' % (seq % 7 + 3))
    statements.append("INSERT OR REPLACE INTO labels VALUES('key%03d',x'%s');" % (seq % 27, (bytes([seq % 255 + 1]) * (seq % 51)).hex()))
    statements.append("INSERT INTO history(v) VALUES('seq-%d');" % seq)
    return statements


def lifecycle_workload(suite, auto_vacuum, profile='split', secure_delete=False,
                       max_commands=160, stop_on_mismatch=True, observer=None,
                       restart_periods=(7,11), checkpoints=True, held_reads=True,
                       name_suffix='', before_commit=None):
    name = 'unequal-restarts-autovacuum-%d-%s-secure-%d%s' % (auto_vacuum, profile, secure_delete, name_suffix)
    seed, template = suite.seed(name, '''
CREATE TABLE items(id INTEGER PRIMARY KEY,k INTEGER NOT NULL,v BLOB NOT NULL);
CREATE INDEX items_k ON items(k);
CREATE TABLE labels(k TEXT PRIMARY KEY,v BLOB) WITHOUT ROWID;
CREATE TABLE history(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT);
CREATE TABLE unique_values(id INTEGER PRIMARY KEY,v TEXT UNIQUE);
INSERT INTO unique_values VALUES(1,'occupied');
''', auto_vacuum=auto_vacuum)
    dbs = suite.copies(name, seed, 3)
    rotations = [0, 0, 0]
    counters = {'checkpoints': 0, 'canonical_checks': 0, 'patchset_checks': 0, 'savepoint_rollbacks': 0}
    failures = []
    base = suite.compare(name + '-baseline', dbs, template)
    assert base['canonical_equal']
    held_reader = None
    held_pages = None
    pinned_stable = True
    rng = random.Random(0xDB2)
    mismatch_count = 0
    for seq in range(1, max_commands + 1):
        for member, period in [(1, restart_periods[0]), (2, restart_periods[1])]:
            if period and seq % period == 0:
                dbs[member].close()
                dbs[member] = DB(suite.root / ('%s-%d.db' % (name, member)))
                rotations[member] += 1
                suite.claim(dbs[member], member, rotations[member])
        profiles = {'split': [(2000,0),(5,1),(17,1)],
                    'on-split': [(2000,1),(5,1),(17,1)],
                    'on-equal': [(2000,1)]*3,
                    'on-small-equal': [(5,1)]*3,
                    'off-equal': [(2000,0)]*3}
        for db, (cache, spill) in zip(dbs, profiles[profile]):
            db.sql('PRAGMA cache_size=%d; PRAGMA cache_spill=%d; PRAGMA secure_delete=%d;' % (cache,spill,secure_delete))
        if seq == 17 and held_reads:
            held_reader = DB(suite.root / ('%s-2.db' % name))
            held_reader.sql('BEGIN')
            held_pages = dict(held_reader.query('SELECT pgno,data FROM sqlite_dbpage ORDER BY pgno'))
        if held_reader is not None:
            pinned_stable &= held_pages == dict(held_reader.query('SELECT pgno,data FROM sqlite_dbpage ORDER BY pgno'))
        if seq == 61 and held_reader is not None:
            held_reader.sql('COMMIT')
            held_reader.close()
            held_reader = None
        statements = workload_statements(seq, rng)
        patchsets = []
        for db in dbs:
            db.sql('BEGIN IMMEDIATE')
            session = session_start(db)
            db.sql('SAVEPOINT item;' + ''.join(statements) + 'RELEASE item;')
            if seq % 5 == 0:
                db.sql('SAVEPOINT rejected; UPDATE items SET v=zeroblob(15000);')
                try:
                    db.sql("INSERT INTO unique_values VALUES(2,'occupied');")
                    raise AssertionError('constraint did not reject')
                except RuntimeError:
                    db.sql('ROLLBACK TO rejected; RELEASE rejected;')
                    counters['savepoint_rollbacks'] += 1
            patchset = session_finish(session)
            patchsets.append(patchset)
            db.sql("UPDATE monsterft_meta_apply SET seq=%d,entry_hash=x'%s',local_digest=x'%s' WHERE id=1;" % (
                seq, hashlib.sha256(str(seq).encode()).digest()[:16].hex(), hashlib.sha256(patchset).digest()[:16].hex()))
            if before_commit is not None:
                before_commit(db)
            db.sql('COMMIT')
        counters['patchset_checks'] += 1
        if not all(item == patchsets[0] for item in patchsets):
            failures.append({'seq': seq, 'reason': 'patchset mismatch'})
        if checkpoints and seq % 13 == 0:
            dbs[0].query('PRAGMA wal_checkpoint(TRUNCATE)')
            counters['checkpoints'] += 1
        if checkpoints and seq % 5 == 0:
            dbs[1].query('PRAGMA wal_checkpoint(PASSIVE)')
            counters['checkpoints'] += 1
        views = [capture(db, template) for db in dbs]
        if observer is not None:
            views = observer(seq, dbs, template, views) or views
        counters['canonical_checks'] += 1
        if any(view['canonical'] != views[0]['canonical'] for view in views[1:]):
            mismatch_count += 1
            if not any(f['reason'] == 'canonical mismatch' for f in failures):
                failures.append({'seq': seq, 'reason': 'canonical mismatch', 'diffs': [page_diff(views[0]['canonical'], v['canonical']) for v in views[1:]]})
            if stop_on_mismatch:
                break
    if held_reader is not None:
        held_reader.sql('COMMIT')
        held_reader.close()
    result = suite.compare(name + '-final', dbs, template, restart_counts=rotations,
                           counters=counters, pinned_reader_stable=pinned_stable,
                           failures=failures, mismatch_checkpoints=mismatch_count,
                           cache_profile=profiles[profile], secure_delete=secure_delete,
                           integrity=[db.query('PRAGMA integrity_check') for db in dbs],
                           app_rows_equal=all(dbs[0].query('SELECT id,k,v FROM items ORDER BY id') == db.query('SELECT id,k,v FROM items ORDER BY id') for db in dbs[1:]))
    # No writes occur between these captures. This distinguishes the writer's
    # cached pager bytes from a fresh reader at the same committed receipt.
    result['writer_vs_fresh_reader'] = []
    for member, db in enumerate(dbs):
        fresh = capture(db, template)
        db.sql('BEGIN')
        try:
            writer = dict(db.query('SELECT pgno,data FROM sqlite_dbpage ORDER BY pgno'))
            receipt = db.query('SELECT seq FROM monsterft_meta_apply')[0][0]
        finally:
            db.sql('COMMIT')
        diff = page_diff(writer, fresh['raw'])
        result['writer_vs_fresh_reader'].append({
            'member': member, 'receipt_seq': receipt,
            'differences': diff,
            'only_free_leaves': all(d['page'] in free_pages(fresh['raw'])['leaves'] for d in diff),
            'first_bytes': [{'page': d['page'], 'writer': writer[d['page']][:16].hex(),
                             'fresh_reader': fresh['raw'][d['page']][:16].hex()} for d in diff],
        })
    assert pinned_stable
    print(json.dumps({'name':name,'checks':counters['canonical_checks'],'equal':result['canonical_equal']}),flush=True)
    suite.close(dbs)
    return result


def optimization_cases(suite):
    seed, template = suite.seed('optimization', '''
CREATE TABLE items(id INTEGER PRIMARY KEY,k INTEGER,v TEXT);
CREATE INDEX items_k ON items(k);
BEGIN;
''' + ''.join("INSERT INTO items VALUES(%d,%d,'value');" % (i, i % 5) for i in range(200)) + 'COMMIT; ANALYZE;')
    for suffix, maintenance in [('bare-optimize', 'PRAGMA optimize'),
                                ('all-tables-optimize', 'PRAGMA optimize(0x10002)'),
                                ('explicit-analyze', 'ANALYZE')]:
        name = 'restart-' + suffix
        dbs = suite.copies(name, seed)
        for db in dbs:
            db.query('SELECT * FROM items WHERE k=3')
            db.sql('BEGIN;' + ''.join("INSERT INTO items VALUES(%d,%d,'value');" % (i, i % 5) for i in range(200, 4200)) + 'COMMIT;')
        dbs[1].close()
        dbs[1] = DB(suite.root / (name + '-1.db'))
        suite.claim(dbs[1], 1, 1)
        before = suite.compare(name + '-before', dbs, template)
        assert before['canonical_equal']
        planned = [db.query('PRAGMA optimize(0x0003)') for db in dbs]
        patches = []
        for db in dbs:
            session = session_start(db)
            db.sql(maintenance)
            patches.append(session_finish(session))
        result = suite.compare(name + '-after', dbs, template, sql=maintenance,
                               optimize_debug_before=planned,
                               patchsets_equal=patches[0] == patches[1], patchset_lengths=[len(p) for p in patches],
                               statistics=[db.query('SELECT * FROM sqlite_stat1 ORDER BY tbl,idx') for db in dbs],
                               app_rows_equal=dbs[0].query('SELECT * FROM items ORDER BY id') == dbs[1].query('SELECT * FROM items ORDER BY id'))
        assert result['canonical_equal'] == (suffix != 'bare-optimize')
        suite.close(dbs)


def maintenance_cases(suite):
    seed, template = suite.seed('maintenance', '''
CREATE TABLE items(id INTEGER PRIMARY KEY,k INTEGER,v BLOB);
CREATE INDEX items_k ON items(k);
BEGIN;
''' + ''.join("INSERT INTO items VALUES(%d,%d,zeroblob(500));" % (i, i % 11) for i in range(400)) + 'DELETE FROM items WHERE id%3=0; COMMIT;')
    for name, sql in [('vacuum', 'VACUUM'), ('reindex', 'REINDEX'), ('analyze', 'ANALYZE')]:
        dbs = suite.copies('both-' + name, seed)
        dbs[1].close()
        dbs[1] = DB(suite.root / ('both-' + name + '-1.db'))
        suite.claim(dbs[1], 1, 1)
        for db in dbs:
            db.sql(sql)
        result = suite.compare('both-' + name, dbs, template,
                               sql=sql, integrity=[db.query('PRAGMA integrity_check') for db in dbs])
        assert result['canonical_equal']
        suite.close(dbs)
        dbs = suite.copies('local-' + name, seed)
        dbs[0].sql(sql)
        suite.compare('local-' + name, dbs, template,
                      sql_on_first_member_only=sql,
                      app_rows_equal=dbs[0].query('SELECT * FROM items ORDER BY id') == dbs[1].query('SELECT * FROM items ORDER BY id'))
        suite.close(dbs)


def ordered_mutation_cases(suite):
    seed, template = suite.seed('mutation-order', '''
CREATE TABLE source(id INTEGER PRIMARY KEY,k TEXT);
CREATE TABLE sink(k TEXT PRIMARY KEY,v TEXT);
BEGIN;
''' + ''.join("INSERT INTO source VALUES(%d,'key%04d');" % (i, i) for i in range(300)) + 'COMMIT;')
    for ordered in [False, True]:
        name = 'insert-select-' + ('ordered' if ordered else 'unordered')
        dbs = suite.copies(name, seed)
        dbs[1].sql('PRAGMA reverse_unordered_selects=ON')
        sql = "INSERT INTO sink SELECT k,'same' FROM source" + (' ORDER BY id' if ordered else '')
        patches = []
        for db in dbs:
            db.sql('BEGIN IMMEDIATE')
            session = session_start(db)
            db.sql(sql)
            patches.append(session_finish(session))
            db.sql('COMMIT')
        suite.compare(name, dbs, template, sql=sql,
                      deliberately_different_setting='reverse_unordered_selects',
                      app_rows_equal=dbs[0].query('SELECT * FROM sink ORDER BY k') == dbs[1].query('SELECT * FROM sink ORDER BY k'),
                      patchsets_equal=patches[0] == patches[1],
                      hidden_rowids_equal=dbs[0].query('SELECT rowid,k FROM sink ORDER BY k') == dbs[1].query('SELECT rowid,k FROM sink ORDER BY k'))
        suite.close(dbs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=pathlib.Path, default=pathlib.Path(__file__).with_name('lifecycle-results.json'))
    parser.add_argument('--work-dir', type=pathlib.Path)
    args = parser.parse_args()
    root = args.work_dir or pathlib.Path(tempfile.mkdtemp(prefix='monsterft-db2-lifecycle-'))
    root.mkdir(parents=True, exist_ok=True)
    info = DB(root / 'runtime.db')
    metadata = {'sqlite_version': lib.sqlite3_libversion().decode(),
                'sqlite_source_id': lib.sqlite3_sourceid().decode(),
                'library': LIB_PATH,
                'compile_options': [row[0] for row in info.query('PRAGMA compile_options')],
                'journal_mode': info.query('PRAGMA journal_mode')[0][0],
                'synchronous': info.query('PRAGMA synchronous')[0][0],
                'page_size': info.query('PRAGMA page_size')[0][0],
                'work_dir': str(root)}
    info.close()
    suite = Suite(root)
    setup_sessions()
    for profile in ['split','on-split','on-equal','on-small-equal','off-equal']:
        lifecycle_workload(suite, 0, profile)
    lifecycle_workload(suite, 0, 'split', True)
    lifecycle_workload(suite, 0, 'on-equal', True)
    lifecycle_workload(suite, 1)
    lifecycle_workload(suite, 2)
    for name, restarts, checkpoints, readers in [
        ('no-lifecycle', (0,0), False, False),
        ('restart-only', (7,11), False, False),
        ('checkpoint-only', (0,0), True, False),
        ('reader-only', (0,0), False, True),
    ]:
        lifecycle_workload(suite, 0, 'on-equal', restart_periods=restarts,
                           checkpoints=checkpoints, held_reads=readers,
                           name_suffix='-' + name)
    optimization_cases(suite)
    maintenance_cases(suite)
    ordered_mutation_cases(suite)
    result = {'runtime': metadata, 'cases': suite.cases}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    for case in suite.cases:
        print(json.dumps({key: case[key] for key in ('name', 'canonical_equal', 'page_counts')}), flush=True)
    print('results: ' + str(args.output), flush=True)


if __name__ == '__main__':
    main()
