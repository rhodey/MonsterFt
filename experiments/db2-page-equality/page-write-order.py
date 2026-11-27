#!/usr/bin/env python3
"""Disposable DBPAGE full-image write-order/growth/truncation experiments.

Uses WAL/FULL and a pinned donor reader while the donor continues writing.
No changeset/patchset application. This does not implement repair lifecycle.
"""
import argparse
import ctypes as C
import hashlib
import json
import pathlib
import random
import shutil
import tempfile

from sqlite_probe import DB, P, lib, OWNER_SQL, canonical_pages, metadata
from lifecycle import free_pages

CONTINUED = {}


def continue_commands(db):
    """Resume ordinary SQL and grow past the old larger target's physical tail."""
    db.sql("BEGIN IMMEDIATE; UPDATE monsterft_local_owner SET generation=x'%s'; "
           "UPDATE app SET v='continued-'||v WHERE k%%9=0;" % ('c7' * 16))
    for k in range(1000, 1040):
        db.sql("INSERT INTO app VALUES(%d,'continued-%d',zeroblob(6000))" % (k, k))
    db.sql('DELETE FROM app WHERE k%11=0; CREATE TABLE after_repair(k INTEGER PRIMARY KEY,v TEXT);'
           "INSERT INTO after_repair VALUES(1,'continued'); COMMIT")
    value = digest(canonical(db))
    CONTINUED.setdefault('digest', value)
    return {'canonical_digest': value,
            'matches_other_repaired_images': value == CONTINUED['digest'],
            'page_count': db.query('PRAGMA page_count')[0][0],
            'integrity': db.query('PRAGMA integrity_check')}


def digest(pages):
    h = hashlib.sha256()
    for number, page in sorted(pages.items()):
        h.update(number.to_bytes(8, 'big'))
        h.update(page)
    return h.hexdigest()


def canonical(db):
    db.sql('BEGIN')
    try:
        raw = db.pages()
        result = canonical_pages(db)
        for number in free_pages(raw)['leaves']:
            result[number] = bytes(len(result[number]))
        return result
    finally:
        db.sql('ROLLBACK')


def seed(path, count, old_schema=False, pad_owner=False):
    db = DB(path)
    db.sql('PRAGMA secure_delete=OFF; PRAGMA auto_vacuum=NONE;')
    if count is not None:
        table = 'old_app' if old_schema else 'app'
        if pad_owner:
            db.sql('CREATE TABLE owner_padding(x)')
        db.sql(OWNER_SQL + "; INSERT INTO monsterft_local_owner VALUES(1,x'" +
               '73' * 16 + "'); CREATE TABLE " + table +
               '(k INTEGER PRIMARY KEY,v TEXT NOT NULL,b BLOB NOT NULL); '
               'CREATE INDEX app_v ON ' + table + '(v); BEGIN IMMEDIATE;')
        for k in range(count):
            db.sql("INSERT INTO %s VALUES(%d,'row-%04d',zeroblob(%d))" %
                   (table, k, k, 400 + (k % 4) * 700))
        db.sql('COMMIT')
    db.close()


def safe(fn):
    try:
        return {'ok': True, 'value': fn()}
    except Exception as error:
        return {'ok': False, 'error': str(error)[:500]}


def probe(db, expected):
    result = {
        'page_count': safe(lambda: db.query('PRAGMA page_count')[0][0]),
        'integrity': safe(lambda: db.query('PRAGMA integrity_check')[:8]),
        'application': safe(lambda: db.query('SELECT count(*),sum(k),sum(length(b)) FROM app')),
        'canonical': safe(lambda: digest(canonical(db))),
    }
    result['canonical_equal'] = result['canonical'].get('value') == expected
    return result


def attached_case(root, seed_path, label, source_pages, expected, order,
                  write_api='exec', warm_schema=False):
    """Probe an in-memory main database controlling an attached repair target."""
    name = label + '-' + order + '-attached-controller-' + write_api + ('-warm' if warm_schema else '-cold')
    path = root / (name + '.db')
    shutil.copyfile(seed_path, path)
    db = DB(':memory:', configure=False)
    db.sql("ATTACH DATABASE '%s' AS repair; PRAGMA repair.journal_mode=WAL; "
           'PRAGMA repair.synchronous=FULL; PRAGMA repair.wal_autocheckpoint=0; '
           'PRAGMA repair.cache_size=5; PRAGMA repair.cache_spill=ON;' % path)
    result = {'case': name, 'target': label, 'order': order,
              'write_api': 'attached-controller-' + write_api, 'warm_schema': warm_schema,
              'transaction': 'all-pages',
              'initial_pages': db.query('PRAGMA repair.page_count')[0][0],
              'donor_pages': len(source_pages), 'written_pages': 0,
              'settings': {'journal_mode': db.query('PRAGMA repair.journal_mode')[0][0],
                           'synchronous': db.query('PRAGMA repair.synchronous')[0][0]}}
    if warm_schema:
        db.query('SELECT name,rootpage,sql FROM repair.sqlite_schema')
        db.query('PRAGMA repair.table_info(app)')
        if label != 'empty':
            db.query('SELECT count(*) FROM repair.app')
    statement = P()
    try:
        db.sql('BEGIN IMMEDIATE')
        if write_api == 'prepared':
            for fn, args in (
                ('sqlite3_bind_int64', [P, C.c_int, C.c_int64]),
                ('sqlite3_bind_blob', [P, C.c_int, P, C.c_int, P]),
                ('sqlite3_reset', [P]),
            ):
                getattr(lib, fn).argtypes = args
                getattr(lib, fn).restype = C.c_int
            assert lib.sqlite3_prepare_v2(db.p,
                b"INSERT INTO main.sqlite_dbpage(pgno,data,schema) VALUES(?1,?2,'repair')", -1,
                C.byref(statement), None) == 0
        for p in ordered_pages(source_pages, order):
            if write_api == 'prepared':
                buf = C.create_string_buffer(source_pages[p])
                assert lib.sqlite3_bind_int64(statement, 1, p) == 0
                assert lib.sqlite3_bind_blob(statement, 2, C.cast(buf, P), len(source_pages[p]), None) == 0
                rc = lib.sqlite3_step(statement)
                if rc != 101:
                    raise RuntimeError(('attached write page', p, rc, lib.sqlite3_errmsg(db.p).decode()))
                assert lib.sqlite3_reset(statement) == 0
            else:
                db.sql("INSERT INTO main.sqlite_dbpage(pgno,data,schema) VALUES(%d,x'%s','repair')"
                       % (p, source_pages[p].hex()))
            result['written_pages'] += 1
        if statement:
            lib.sqlite3_finalize(statement)
            statement = P()
        db.sql("INSERT INTO main.sqlite_dbpage(pgno,data,schema) VALUES(%d,NULL,'repair')"
               % (len(source_pages) + 1))
        db.sql('COMMIT')
        result['install_ok'] = True
    except Exception as error:
        result['install_ok'] = False
        result['install_error'] = str(error)[:500]
        result['rollback'] = safe(lambda: db.sql('ROLLBACK'))
    finally:
        if statement:
            lib.sqlite3_finalize(statement)
    result['same_connection'] = {'canonical_equal': None,
        'application': safe(lambda: db.query('SELECT count(*) FROM repair.app'))}
    result['close'] = safe(db.close)
    result['main_file_pages_after_close'] = path.stat().st_size // len(source_pages[1])
    opened = DB(path)
    result['reopened'] = probe(opened, expected)
    if result['reopened']['canonical_equal']:
        result['continued_commands'] = safe(lambda: continue_commands(opened))
    opened.close()
    return result


def ordered_pages(pages, order):
    numbers = sorted(pages)
    if order == 'descending':
        numbers.reverse()
    elif order == 'page1-last':
        numbers = numbers[1:] + [1]
    elif order == 'random':
        random.Random(0xDBFACE).shuffle(numbers)
    return numbers


def install_case(root, seed_path, label, source_pages, expected, order,
                 truncate, counters, transaction='all-pages', cache_size=2000,
                 write_api='exec'):
    case_name = '-'.join((label, order, truncate, counters, transaction, str(cache_size), write_api))
    path = root / (case_name + '.db')
    shutil.copyfile(seed_path, path)
    db = DB(path)
    db.sql('PRAGMA cache_size=%d; PRAGMA cache_spill=ON;' % cache_size)
    if label == 'schema-different-same-cookie':
        cookie = int.from_bytes(source_pages[1][40:44], 'big')
        db.sql('PRAGMA schema_version=%d' % cookie)
        assert db.query('SELECT count(*) FROM old_app')
    initial_count = db.query('PRAGMA page_count')[0][0]
    result = {
        'case': case_name, 'target': label, 'order': order,
        'truncate': truncate, 'counters': counters, 'transaction': transaction,
        'cache_size': cache_size, 'initial_pages': initial_count,
        'donor_pages': len(source_pages), 'written_pages': 0, 'write_api': write_api,
    }
    n = len(source_pages)
    statement = P()
    try:
        if transaction == 'all-pages':
            db.sql('BEGIN IMMEDIATE')
        if truncate == 'before':
            db.sql('INSERT INTO sqlite_dbpage(pgno,data) VALUES(%d,NULL)' % (n + 1))
        if write_api == 'prepared':
            for name, args in (
                ('sqlite3_bind_int64', [P, C.c_int, C.c_int64]),
                ('sqlite3_bind_blob', [P, C.c_int, P, C.c_int, P]),
                ('sqlite3_reset', [P]),
            ):
                getattr(lib, name).argtypes = args
                getattr(lib, name).restype = C.c_int
            assert lib.sqlite3_prepare_v2(db.p,
                b'INSERT INTO sqlite_dbpage(pgno,data) VALUES(?1,?2)', -1,
                C.byref(statement), None) == 0
        for number in ordered_pages(source_pages, order):
            page = source_pages[number]
            if number == 1 and counters == 'normalized':
                page = bytearray(page)
                page[24:28] = bytes(4)
                page[92:96] = bytes(4)
            if write_api == 'prepared':
                buf = C.create_string_buffer(bytes(page))
                assert lib.sqlite3_bind_int64(statement, 1, number) == 0
                assert lib.sqlite3_bind_blob(statement, 2, C.cast(buf, P), len(page), None) == 0
                rc = lib.sqlite3_step(statement)
                if rc != 101:
                    raise RuntimeError(('write page', number, rc, lib.sqlite3_errmsg(db.p).decode()))
                assert lib.sqlite3_reset(statement) == 0
            else:
                db.sql("INSERT INTO sqlite_dbpage(pgno,data) VALUES(%d,x'%s')" %
                       (number, page.hex()))
            result['written_pages'] += 1
        if statement:
            lib.sqlite3_finalize(statement)
            statement = P()
        if truncate == 'after':
            db.sql('INSERT INTO sqlite_dbpage(pgno,data) VALUES(%d,NULL)' % (n + 1))
        if transaction == 'all-pages':
            # Capture stale in-transaction counts without requiring valid schema SQL.
            result['before_commit_page_count'] = safe(lambda: db.query('PRAGMA page_count')[0][0])
            db.sql('COMMIT')
        result['install_ok'] = True
    except Exception as error:
        result['install_ok'] = False
        result['install_error'] = str(error)[:500]
        result['rollback'] = safe(lambda: db.sql('ROLLBACK'))
    finally:
        if statement:
            lib.sqlite3_finalize(statement)
    result['same_connection'] = probe(db, expected)
    result['close'] = safe(db.close)
    result['main_file_pages_after_close'] = path.stat().st_size // len(source_pages[1])
    reopened = None
    try:
        reopened = DB(path)
        result['reopened'] = probe(reopened, expected)
        if result['reopened']['canonical_equal']:
            result['continued_commands'] = safe(lambda: continue_commands(reopened))
    except Exception as error:
        result['reopened'] = {'open_error': str(error)[:500], 'canonical_equal': False}
    finally:
        if reopened:
            reopened.close()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=pathlib.Path,
                        default=pathlib.Path(__file__).with_name('page-write-order-results.json'))
    parser.add_argument('--work-dir', type=pathlib.Path)
    args = parser.parse_args()
    root = args.work_dir or pathlib.Path(tempfile.mkdtemp(prefix='monsterft-page-write-order-'))
    root.mkdir(parents=True, exist_ok=True)
    source_path = root / 'source.db'
    seed(source_path, 80)
    writer = DB(source_path)
    writer.sql("UPDATE app SET v='pinned-'||v WHERE k%3=0")
    reader = DB(source_path, configure=False)
    reader.sql('BEGIN')
    pages = reader.pages()
    expected_pages = canonical_pages(reader)
    for number in free_pages(pages)['leaves']:
        expected_pages[number] = bytes(len(pages[number]))
    expected = digest(expected_pages)
    runtime = metadata(writer)
    runtime['work_dir'] = str(root)
    source_main_size = source_path.stat().st_size
    source_wal_size = pathlib.Path(str(source_path) + '-wal').stat().st_size
    writer.sql("UPDATE app SET v='newer-'||v WHERE k%5=0")
    assert reader.pages() == pages
    current_reader = DB(source_path, configure=False)
    assert current_reader.pages() != pages
    current_reader.close()
    seeds = {}
    for label, count in [('same', 80), ('smaller', 7), ('larger', 210),
                         ('empty', None), ('schema-different-same-cookie', 80),
                         ('extra-schema', 210), ('owner-shift', 80)]:
        seeds[label] = root / (label + '-seed.db')
        seed(seeds[label], count, old_schema=label.startswith('schema-'),
             pad_owner=label == 'owner-shift')
        if label == 'extra-schema':
            extra = DB(seeds[label])
            extra.sql('CREATE TABLE extra(x); INSERT INTO extra VALUES(123)')
            extra.close()
    explain_main = DB(seeds['same'])
    main_transactions = [row for row in explain_main.query(
        'EXPLAIN INSERT INTO sqlite_dbpage(pgno,data) VALUES(2,zeroblob(4096))')
        if row[1] == 'Transaction']
    explain_main.close()
    explain_attached = DB(':memory:', configure=False)
    explain_attached.sql("ATTACH DATABASE '%s' AS repair" % seeds['same'])
    attached_transactions = [row for row in explain_attached.query(
        "EXPLAIN INSERT INTO main.sqlite_dbpage(pgno,data,schema) "
        "VALUES(2,zeroblob(4096),'repair')") if row[1] == 'Transaction']
    explain_attached.close()
    results = []
    for label, seed_path in seeds.items():
        for order in ('ascending', 'descending', 'page1-last', 'random'):
            for truncate in ('none', 'before', 'after'):
                for counters in ('raw', 'normalized'):
                    results.append(install_case(root, seed_path, label, pages, expected,
                                                order, truncate, counters))
    # A smaller cache exercises spilling; per-page autocommit is a negative/control
    # comparison, not the planned transactional repair method.
    for label in ('smaller', 'larger', 'empty'):
        for order in ('ascending', 'descending', 'page1-last', 'random'):
            for transaction, cache_size in [('all-pages', 5), ('per-page', 2000)]:
                results.append(install_case(root, seeds[label], label, pages, expected,
                                            order, 'after', 'raw', transaction, cache_size))
    for label, seed_path in seeds.items():
        for order in ('ascending', 'descending', 'page1-last', 'random'):
            for cache_size in (2000, 5):
                results.append(install_case(root, seed_path, label, pages, expected,
                                            order, 'after', 'normalized', 'all-pages',
                                            cache_size, 'prepared'))
    for label in ('smaller', 'extra-schema', 'owner-shift', 'empty'):
        for order in ('ascending', 'descending', 'page1-last', 'random'):
            for write_api in ('exec', 'prepared'):
                for warm_schema in (False, True):
                    results.append(attached_case(root, seeds[label], label, pages, expected,
                                                 order, write_api, warm_schema))
    assert reader.pages() == pages
    reader.sql('ROLLBACK')
    reader.close()
    writer.close()
    summary = {
        'cases': len(results),
        'install_ok': sum(r['install_ok'] for r in results),
        'reopened_canonical_equal': sum(r['reopened'].get('canonical_equal', False) for r in results),
        'same_connection_canonical_equal': sum(bool(r['same_connection']['canonical_equal']) for r in results),
        'source_pages': len(pages),
        'source_main_size_at_pin': source_main_size,
        'source_wal_size_at_pin': source_wal_size,
        'pinned_source_stable_during_later_writes_and_repairs': True,
        'continued_commands_equal': sum(r.get('continued_commands', {}).get('value', {})
                                       .get('matches_other_repaired_images', False) for r in results),
    }
    output = {
        'runtime': runtime,
        'method': 'Raw full-image DBPAGE page writes; WAL/FULL; pinned donor while writer advances; '
                  'page1 counters raw or normalized; no changeset/patchset application. '
                  'Expected negative controls are recorded, not treated as successes.',
        'schemaVerification': {
            'description': 'EXPLAIN Transaction rows: address, opcode, P1 database index, '
                           'P2 write mode, P3 schema cookie, P4 generation, P5 schema-check flag, comment. '
                           'Attached controller checks the unchanged main schema (index0). '
                           'DBPAGE xUpdate itself opens a write transaction on the attached target.',
            'directMain': main_transactions,
            'attachedController': attached_transactions,
        },
        'summary': summary, 'results': results,
    }
    args.output.write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps({'output': str(args.output), **summary}))
    attached = [r for r in results if r['write_api'].startswith('attached-controller')]
    assert len(attached) == 64
    assert all(r['install_ok'] and r['reopened']['canonical_equal'] for r in attached)
    ordinary = [r for r in results if r['transaction'] == 'all-pages'
                and r['write_api'] in ('exec', 'prepared')
                and r['target'] not in ('extra-schema', 'owner-shift')]
    assert all(r['install_ok'] and r['reopened']['canonical_equal'] for r in ordinary)
    negative = [r for r in results if r['write_api'] in ('exec', 'prepared')
                and r['target'] in ('extra-schema', 'owner-shift')]
    assert negative and all(not r['install_ok'] for r in negative)
    for result in results:
        if result['install_ok']:
            assert result['reopened']['canonical_equal'], result['case']
            assert result['reopened']['integrity']['value'] == [('ok',)], result['case']
            continued = result['continued_commands']['value']
            assert continued['matches_other_repaired_images'], result['case']
            assert continued['integrity'] == [('ok',)], result['case']


if __name__ == '__main__':
    main()
