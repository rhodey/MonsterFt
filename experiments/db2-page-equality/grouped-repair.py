#!/usr/bin/env python3
"""Disposable typed-group DBPAGE repair experiment, not the Raft protocol.

Uses the existing custom SQLite library. WAL/FULL throughout. No patchset is
ever applied. Simulated messages are Python values; databases/pages are real.
Full-copy fallback deletes only this run's disposable target files. Repair
lifecycle, interruption recovery, and voting/rejoin are deliberately not modeled.
"""
import argparse
import ctypes as C
import hashlib
import json
import pathlib
import shutil
import struct
import tempfile

from sqlite_probe import DB as SQLiteDB, P, lib, metadata, owner_root, owner_template, OWNER_SQL
from freelist import classify


ORDINARY, HEADER, OWNER, FREE = range(4)
GROUP_SIZE = 4
OPEN_HANDLES = []


class DB(SQLiteDB):
    def __init__(self, *args, **kwargs):
        try:
            super().__init__(*args, **kwargs)
        finally:
            if getattr(self, 'p', None):
                OPEN_HANDLES.append(self)


def close_remaining_handles():
    # Negative controls intentionally fail during a transaction. Release their
    # writers/readers before the next independent case rather than at process exit.
    for db in OPEN_HANDLES:
        if db.p:
            if not lib.sqlite3_get_autocommit(db.p):
                db.sql('ROLLBACK')
            db.close()
    OPEN_HANDLES.clear()


def token(label):
    return hashlib.sha256(label.encode()).digest()[:16]


def claim(db, label):
    db.sql("BEGIN IMMEDIATE; UPDATE monsterft_local_owner SET generation=x'%s'; COMMIT"
           % token(label).hex())


def owner_page(size, value):
    db = DB(':memory:', configure=False)
    try:
        db.sql('PRAGMA page_size=%d;' % size + OWNER_SQL + ';'
               "INSERT INTO monsterft_local_owner VALUES(1,x'%s')" % value.hex())
        return db.pages()[2]
    finally:
        db.close()


def view(db):
    own = bool(lib.sqlite3_get_autocommit(db.p))
    if own:
        db.sql('BEGIN')
    try:
        raw = db.pages()
        size = len(raw[1])
        assert raw[1][:16] == b'SQLite format 3\0' and raw[1][20] == 0
        assert sorted(raw) == list(range(1, len(raw) + 1))
        root = owner_root(db, raw)
        assert not db.query("SELECT name FROM sqlite_schema WHERE type='trigger' "
                            "AND tbl_name='monsterft_local_owner'")
        sql = db.query("SELECT sql FROM sqlite_schema WHERE name='monsterft_local_owner'")[0][0]
        assert ' '.join(sql.lower().split()) == ' '.join(OWNER_SQL.lower().split())
        free = classify(raw)
        assert root not in free['leaves'] and root not in free['trunks']
        modes = {p: ORDINARY for p in raw}
        modes[1], modes[root] = HEADER, OWNER
        canonical = dict(raw)
        header = bytearray(raw[1])
        header[24:28] = header[92:96] = bytes(4)
        canonical[1] = bytes(header)
        canonical[root] = owner_template(size)
        for p in free['leaves']:
            modes[p], canonical[p] = FREE, bytes(size)
        return {'raw': raw, 'canonical': canonical, 'modes': modes,
                'owner': root, 'free': free, 'size': size, 'count': len(raw)}
    finally:
        if own:
            db.sql('ROLLBACK')


def digest_group(rows, size, typed=True):
    # SHA-256 here is an experiment codec, not a production hash/API decision.
    h = hashlib.sha256(b'monsterft-dbpage-repair-experiment-v1')
    h.update(struct.pack('>III', size, rows[0][0], len(rows)))
    for p, mode, data in rows:
        h.update(struct.pack('>I', p))
        if typed:
            h.update(bytes([mode]))
        h.update(data)
    return h.hexdigest()


def groups(v, typed=True):
    result = {}
    for start in range(1, v['count'] + 1, GROUP_SIZE):
        rows = [(p, v['modes'][p], v['canonical'][p])
                for p in range(start, min(start + GROUP_SIZE, v['count'] + 1))]
        result[start] = {'rows': rows, 'hash': digest_group(rows, v['size'], typed)}
    return result


def comparable(v):
    return {p: (v['modes'][p], b) for p, b in v['canonical'].items()}


def write_pages(db, pages, truncate=None, profile='prepared_ascending'):
    schema = getattr(db, 'repair_schema', 'main')
    ordered = sorted(pages.items(), key=lambda row: (row[0] == 1, row[0])
                     if profile.endswith('page1_last') else (False, row[0]))
    if profile.startswith('exec_'):
        for p, data in ordered:
            db.sql("INSERT INTO main.sqlite_dbpage(pgno,data,schema) VALUES(%d,x'%s','%s')" % (p, data.hex(), schema))
        if truncate is not None:
            db.sql("INSERT INTO main.sqlite_dbpage(pgno,data,schema) VALUES(%d,NULL,'%s')" % (truncate + 1, schema))
        return len(ordered)
    for name, args in (
        ('sqlite3_bind_int64', [P, C.c_int, C.c_int64]),
        ('sqlite3_bind_blob', [P, C.c_int, P, C.c_int, P]),
        ('sqlite3_reset', [P]),
    ):
        getattr(lib, name).argtypes = args
        getattr(lib, name).restype = C.c_int
    statement = P()
    sql = ("INSERT INTO main.sqlite_dbpage(pgno,data,schema) VALUES(?1,?2,'%s')" % schema).encode()
    rc = lib.sqlite3_prepare_v2(db.p, sql, -1, C.byref(statement), None)
    if rc:
        raise RuntimeError(('prepare page write', rc, lib.sqlite3_errmsg(db.p).decode()))
    count = 0
    try:
        for p, data in ordered:
            buf = C.create_string_buffer(data)
            assert lib.sqlite3_bind_int64(statement, 1, p) == 0
            assert lib.sqlite3_bind_blob(statement, 2, C.cast(buf, P), len(data), None) == 0
            rc = lib.sqlite3_step(statement)
            if rc != 101:
                raise RuntimeError(('write page', p, rc, lib.sqlite3_errmsg(db.p).decode()))
            assert lib.sqlite3_reset(statement) == 0
            count += 1
    finally:
        lib.sqlite3_finalize(statement)
    if truncate is not None:
        db.sql("INSERT INTO main.sqlite_dbpage(pgno,data,schema) VALUES(%d,NULL,'%s')" % (truncate + 1, schema))
    return count


def mutate_pages(db, pages):
    db.sql('BEGIN IMMEDIATE')
    try:
        write_pages(db, pages)
        db.sql('COMMIT')
    except BaseException:
        if not lib.sqlite3_get_autocommit(db.p):
            db.sql('ROLLBACK')
        raise


def seed(path, pad_owner=False):
    db = DB(path)
    db.sql('PRAGMA secure_delete=OFF; PRAGMA cache_size=8;')
    if pad_owner:
        db.sql('CREATE TABLE pad_before_owner(x)')
    db.sql(OWNER_SQL + "; INSERT INTO monsterft_local_owner VALUES(1,x'%s');" % token('seed').hex())
    db.sql('''CREATE TABLE monsterft_meta_apply(id INTEGER PRIMARY KEY,seq INTEGER NOT NULL);
              INSERT INTO monsterft_meta_apply VALUES(1,0);
              CREATE TABLE items(id INTEGER PRIMARY KEY,k INTEGER NOT NULL,v BLOB);
              CREATE INDEX items_k ON items(k);
              CREATE TABLE zero_payload(id INTEGER PRIMARY KEY,v BLOB);
              INSERT INTO zero_payload VALUES(1,zeroblob(21000));
              CREATE TABLE scratch(id INTEGER PRIMARY KEY,v BLOB);''')
    db.sql('BEGIN IMMEDIATE')
    for i in range(1, 55):
        db.sql("INSERT INTO items VALUES(%d,%d,CAST('%s' AS BLOB));" % (i, i % 7, 'a' * (80 + i)))
    for i in range(1, 27):
        db.sql('INSERT INTO scratch VALUES(%d,zeroblob(10000))' % i)
    db.sql('DELETE FROM scratch; COMMIT')
    assert db.query('PRAGMA integrity_check') == [('ok',)]
    db.close()


def copy(seed_path, path, label):
    shutil.copyfile(seed_path, path)  # Seed is closed/checkpointed before copies.
    db = DB(path)
    db.sql('PRAGMA secure_delete=OFF; PRAGMA cache_size=8;')
    claim(db, label)
    return db


def grow_schema(db):
    db.sql('BEGIN IMMEDIATE; CREATE TABLE added(id INTEGER PRIMARY KEY,v BLOB);'
           'CREATE INDEX added_size ON added(length(v));')
    for i in range(1, 90):
        db.sql('INSERT INTO added VALUES(%d,zeroblob(11000))' % i)
    db.sql('COMMIT')


def command(db, seq):
    db.sql("BEGIN IMMEDIATE; INSERT INTO items VALUES(%d,%d,zeroblob(%d)); "
           "UPDATE items SET v=zeroblob(%d) WHERE id=%d; "
           "UPDATE monsterft_meta_apply SET seq=%d; COMMIT" %
           (1000 + seq, seq % 7, 200 + seq * 31, 90 + seq * 17, 1 + seq % 40, seq))


def errors(db):
    try:
        return [r[0] for r in db.query('PRAGMA integrity_check')]
    except Exception as exc:
        return [str(exc)]


def scenario(root, seed_path, name, steps, profile):
    directory = root / name
    directory.mkdir()
    paths = [directory / (member + '.db') for member in ('leader', 'witness', 'target')]
    dbs = [copy(seed_path, p, name + str(i)) for i, p in enumerate(paths)]
    result = {'case': name, 'groupSize': GROUP_SIZE, 'writeProfile': profile}
    donor0 = view(dbs[0])
    zero_pages = [p for p, b in donor0['raw'].items()
                  if not any(b) and donor0['modes'][p] == ORDINARY]
    assert zero_pages, 'fixture needs a zero-filled active overflow page'
    chosen = zero_pages[0]
    if name == 'ordinary_corruption':
        b = bytearray(donor0['raw'][chosen]); b[100:108] = b'DAMAGED!'
        mutate_pages(dbs[2], {chosen: bytes(b)})
    elif name == 'index_corruption':
        p = dbs[2].query("SELECT rootpage FROM sqlite_schema WHERE name='items_k'")[0][0]
        b = bytearray(donor0['raw'][p]); b[-1] ^= 1
        mutate_pages(dbs[2], {p: bytes(b)})
    elif name == 'wrong_free_classification':
        trunk = donor0['free']['trunks'][0]
        assert (trunk - 1) // GROUP_SIZE != (chosen - 1) // GROUP_SIZE
        b = bytearray(donor0['raw'][trunk])
        lost_free_page = int.from_bytes(b[8:12], 'big')
        assert donor0['modes'][lost_free_page] == FREE
        b[8:12] = chosen.to_bytes(4, 'big')
        damaged = bytearray(donor0['raw'][chosen]); damaged[100:108] = b'DAMAGED!'
        mutate_pages(dbs[2], {trunk: bytes(b), chosen: bytes(damaged)})
        result['misclassifiedActivePage'] = chosen
    elif name == 'schema_growth':
        for db in dbs[:2]:
            grow_schema(db)
    elif name == 'target_extra_pages':
        grow_schema(dbs[2])
    elif name == 'owner_root_change':
        other_seed = directory / 'other-seed.db'
        seed(other_seed, pad_owner=True)
        dbs[2].close()
        for suffix in ('', '-wal', '-shm'):
            pathlib.Path(str(paths[2]) + suffix).unlink(missing_ok=True)
        dbs[2] = copy(other_seed, paths[2], name)
        assert view(dbs[2])['owner'] != donor0['owner']
    elif name in ('bad_owner_fallback', 'bad_freelist_fallback', 'bad_schema_fallback', 'bad_header_fallback'):
        p = donor0['owner'] if name == 'bad_owner_fallback' else (
            donor0['free']['trunks'][0] if name == 'bad_freelist_fallback' else 1)
        b = bytearray(donor0['raw'][p])
        if name == 'bad_freelist_fallback':
            b[:4] = p.to_bytes(4, 'big')
        elif name == 'bad_header_fallback':
            b[:16] = b'NOT_A_SQLITE_DB!'
            assert len(b) == donor0['size']
        else:
            b[0 if p != 1 else 100] = 0xff
        mutate_pages(dbs[2], {p: bytes(b)})
    elif name == 'witness_disagrees':
        for i in (1, 2):
            b = bytearray(donor0['raw'][chosen]); b[100] = i
            mutate_pages(dbs[i], {chosen: bytes(b)})
    elif name == 'witness_unreadable':
        for i in (1, 2):
            p = donor0['owner'] if i == 1 else chosen
            b = bytearray(donor0['raw'][p]); b[0 if i == 1 else 100] = 0xff
            mutate_pages(dbs[i], {p: bytes(b)})
    elif name == 'pinned_snapshot':
        b = bytearray(donor0['raw'][chosen]); b[100] = 9
        mutate_pages(dbs[2], {chosen: bytes(b)})
    else:
        raise AssertionError(name)

    # Keep actual read transactions pinned for both healthy snapshots.
    readers = [DB(p, configure=False) for p in paths[:2]]
    for reader in readers:
        reader.sql('BEGIN')
    leader = view(readers[0])
    leader_groups = groups(leader)
    # Repair startup must not inherit the mutator's cached schema/metadata.
    dbs[2].close()
    dbs[2] = DB(paths[2], configure=False)
    try:
        target = view(dbs[2])
        target_groups = groups(target)
        result['fullCopyFallback'] = False
        result['targetOwnerBefore'] = target['owner']
        result['targetPagesBefore'] = target['count']
    except Exception as exc:
        target, target_groups = None, {}
        result['fullCopyFallback'] = True
        result['targetReadError'] = str(exc)
    assert result['fullCopyFallback'] == name.endswith('_fallback'), result

    requested = [g for g, v in leader_groups.items()
                 if target_groups.get(g, {}).get('hash') != v['hash']]
    result['donorPages'] = leader['count']
    result['partialFinalGroup'] = leader['count'] % GROUP_SIZE
    result['groupCount'] = len(leader_groups)
    result['requestedGroups'] = requested
    result['modeledTargetSubmissionCount'] = 1
    result['modeledFollowerCheckRequestCount'] = 1 if requested else 0
    if name == 'wrong_free_classification':
        g = 1 + ((chosen - 1) // GROUP_SIZE) * GROUP_SIZE
        result['byteOnlyHashWouldMatch'] = groups(target, False)[g]['hash'] == groups(leader, False)[g]['hash']
        assert result['byteOnlyHashWouldMatch'] and g in requested

    try:
        witness = view(readers[1])
        witness_groups = groups(witness)
        mismatch = [g for g in requested if witness_groups.get(g, {}).get('hash') != leader_groups[g]['hash']]
        if witness['count'] != leader['count']:
            mismatch.append('size')
        result['followerDisagreements'] = mismatch
        if mismatch:
            result.update(aborted=True, pagesWritten=0)
        else:
            result['aborted'] = False
    except Exception as exc:
        result.update(aborted=True, pagesWritten=0, followerReadError=str(exc))

    if result['aborted']:
        assert name in ('witness_disagrees', 'witness_unreadable')
        for reader in readers:
            reader.sql('ROLLBACK'); reader.close()
        for db in dbs:
            db.close()
        result['pass'] = True
        return result

    if name == 'pinned_snapshot':
        # Healthy writers advance while the donated snapshot remains pinned.
        for db in dbs[:2]:
            command(db, 1)
        assert comparable(view(readers[0])) == comparable(leader)
        assert comparable(view(readers[1])) == comparable(witness)
        result['healthyWritersAdvancedWhilePinned'] = True

    # One simulated response per selected group, preserving donor type codes.
    wire = [leader_groups[g] for g in requested]
    replacement = {}
    for group in wire:
        assert digest_group(group['rows'], leader['size']) == group['hash']
        for p, mode, data in group['rows']:
            replacement[p] = data
    result['pagesExpectedToWrite'] = len(replacement)
    if target is None:
        dbs[2].close()
        for suffix in ('', '-wal', '-shm'):
            pathlib.Path(str(paths[2]) + suffix).unlink(missing_ok=True)
        dbs[2] = DB(paths[2])
    # A fresh owner is generated as a valid leaf before any schema is replaced.
    # If its group is unchanged, claim it after reopening but before any modeled
    # application command; no Raft/activation transition is modeled here.
    recipient = token(name + '-repaired')
    owner_written = leader['owner'] in replacement
    if owner_written:
        replacement[leader['owner']] = owner_page(leader['size'], recipient)
    if profile.startswith('attached_'):
        dbs[2].close()
        dbs[2] = DB(':memory:', configure=False)
        dbs[2].sql("ATTACH DATABASE '%s' AS repair" % str(paths[2]).replace("'", "''"))
        dbs[2].sql('PRAGMA repair.journal_mode=WAL; PRAGMA repair.synchronous=FULL')
        assert dbs[2].query('PRAGMA repair.journal_mode') == [('wal',)]
        assert dbs[2].query('PRAGMA repair.synchronous') == [(2,)]
        dbs[2].repair_schema = 'repair'
    else:
        dbs[2].sql('PRAGMA synchronous=FULL')
    if 'writable' in profile:
        dbs[2].sql('PRAGMA writable_schema=ON')
    dbs[2].sql('BEGIN IMMEDIATE')
    try:
        if 'aligncookie' in profile:
            cookie = int.from_bytes(leader['raw'][1][40:44], 'big')
            if dbs[2].query('PRAGMA schema_version') != [(cookie,)]:
                dbs[2].sql('PRAGMA schema_version=%d' % cookie)
            dbs[2].query('SELECT count(*) FROM sqlite_schema')
        result['pagesWritten'] = write_pages(dbs[2], replacement, truncate=leader['count'], profile=profile)
        dbs[2].sql('COMMIT')
    except BaseException:
        if not lib.sqlite3_get_autocommit(dbs[2].p):
            dbs[2].sql('ROLLBACK')
        raise
    assert result['pagesWritten'] == result['pagesExpectedToWrite']
    dbs[2].close()
    result['targetMainBytesAfterInstallerClose'] = paths[2].stat().st_size
    assert paths[2].stat().st_size == leader['count'] * leader['size']
    dbs[2] = DB(paths[2])
    assert dbs[2].query('PRAGMA writable_schema') == [(0,)]
    if not owner_written:
        claim(dbs[2], name + '-repaired')
    assert dbs[2].query('SELECT generation FROM monsterft_local_owner') == [(recipient,)]
    result['integrityAfterRepair'] = errors(dbs[2])
    assert result['integrityAfterRepair'] == ['ok'], result
    result['canonicalEqualAfterReopen'] = comparable(view(dbs[2])) == comparable(leader)
    assert result['canonicalEqualAfterReopen'], result
    result['recipientOwnerAfter'] = view(dbs[2])['owner']
    for reader in readers:
        reader.sql('ROLLBACK'); reader.close()
    continued_checks = 0
    if name == 'pinned_snapshot':
        command(dbs[2], 1)
        views = [view(db) for db in dbs]
        assert comparable(views[0]) == comparable(views[1]) == comparable(views[2])
        continued_checks += 1
    restarts = [0, 0, 0]
    for seq in range(2 if name == 'pinned_snapshot' else 1, steps + 1):
        for member in (1, 2):
            if seq % (3 if member == 1 else 5) == 0:
                dbs[member].close(); dbs[member] = DB(paths[member])
                restarts[member] += 1
                claim(dbs[member], '%s-%d-%d' % (name, member, seq))
        for db in dbs:
            command(db, seq)
        views = [view(db) for db in dbs]
        assert comparable(views[0]) == comparable(views[1]) == comparable(views[2]), (name, seq)
        continued_checks += 1
    result.update(continuedCommandCheckpoints=continued_checks, restartCounts=restarts,
                  integrityAfterContinuedCommands=[errors(db) for db in dbs], **{'pass': True})
    assert all(r == ['ok'] for r in result['integrityAfterContinuedCommands'])
    for db in dbs:
        db.close()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=pathlib.Path, default=pathlib.Path(__file__).with_name('grouped-repair-results.json'))
    parser.add_argument('--work-dir', type=pathlib.Path)
    parser.add_argument('--commands', type=int, default=30)
    parser.add_argument('--profiles', nargs='+', default=[
        'prepared_ascending', 'prepared_page1_last', 'exec_ascending', 'exec_page1_last',
        'prepared_writable_ascending', 'prepared_aligncookie_ascending',
        'attached_ascending', 'attached_page1_last'])
    args = parser.parse_args()
    root = args.work_dir or pathlib.Path(tempfile.mkdtemp(prefix='monsterft-grouped-repair-'))
    root.mkdir(parents=True, exist_ok=True)
    seed_path = root / 'seed.db'
    seed(seed_path)
    db = DB(seed_path)
    info = metadata(db); db.close()
    result = {'runtime': info, 'workDir': str(root), 'scope': 'SQLite storage and simulated messages; no Raft, lifecycle, or interruption recovery', 'cases': []}
    names = ('ordinary_corruption', 'index_corruption', 'wrong_free_classification',
             'schema_growth', 'target_extra_pages', 'owner_root_change',
             'bad_owner_fallback', 'bad_freelist_fallback', 'bad_schema_fallback',
             'bad_header_fallback',
             'witness_disagrees', 'witness_unreadable', 'pinned_snapshot')
    for profile in args.profiles:
        profile_dir = root / profile
        profile_dir.mkdir()
        for name in names:
            try:
                case = scenario(profile_dir, seed_path, name, args.commands, profile)
            except Exception as exc:
                case = {'case': name, 'writeProfile': profile, 'pass': False, 'error': repr(exc)}
            finally:
                close_remaining_handles()
            result['cases'].append(case)
            args.output.write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps({k: case[k] for k in ('case', 'writeProfile', 'pass', 'error', 'pagesWritten', 'aborted') if k in case}), flush=True)
    required = [case for case in result['cases'] if 'aligncookie' in case['writeProfile'] or case['writeProfile'].startswith('attached_')]
    assert required and all(case['pass'] for case in required), 'repair controls must pass; see results'
    known_failure_cases = {'target_extra_pages', 'owner_root_change',
                          'bad_owner_fallback', 'bad_freelist_fallback', 'bad_schema_fallback',
                          'bad_header_fallback'}
    for case in result['cases']:
        if not case['pass']:
            assert case['case'] in known_failure_cases and 'malformed' in case['error'], case


if __name__ == '__main__':
    main()
