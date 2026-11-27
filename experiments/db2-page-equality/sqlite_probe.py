"""Experimental SQLite pager reader. Never applies changesets or patchsets.

Set SQLITE_PROBE_LIB to a SQLite shared library compiled with DBPAGE, SESSION,
and PREUPDATE_HOOK. The default is the earlier local exploratory build.
"""
import base64
import ctypes as C
import hashlib
import json
import os
import pathlib
import sys

LIB_PATH = os.environ.get('SQLITE_PROBE_LIB',
    '/private/tmp/monsterft-page-identity/libsqlite3.dylib')
lib = C.CDLL(LIB_PATH)
P = C.c_void_p
for name, args, result in [
    ('sqlite3_open', [C.c_char_p, C.POINTER(P)], C.c_int),
    ('sqlite3_close', [P], C.c_int),
    ('sqlite3_errmsg', [P], C.c_char_p),
    ('sqlite3_exec', [P, C.c_char_p, P, P, C.POINTER(P)], C.c_int),
    ('sqlite3_prepare_v2', [P, C.c_char_p, C.c_int, C.POINTER(P), P], C.c_int),
    ('sqlite3_step', [P], C.c_int), ('sqlite3_finalize', [P], C.c_int),
    ('sqlite3_column_count', [P], C.c_int),
    ('sqlite3_column_type', [P, C.c_int], C.c_int),
    ('sqlite3_column_int64', [P, C.c_int], C.c_int64),
    ('sqlite3_column_double', [P, C.c_int], C.c_double),
    ('sqlite3_column_text', [P, C.c_int], P),
    ('sqlite3_column_blob', [P, C.c_int], P),
    ('sqlite3_column_bytes', [P, C.c_int], C.c_int),
    ('sqlite3_libversion', [], C.c_char_p),
    ('sqlite3_sourceid', [], C.c_char_p),
    ('sqlite3_get_autocommit', [P], C.c_int),
    ('sqlite3session_create', [P, C.c_char_p, C.POINTER(P)], C.c_int),
    ('sqlite3session_attach', [P, C.c_char_p], C.c_int),
    ('sqlite3session_patchset', [P, C.POINTER(C.c_int), C.POINTER(P)], C.c_int),
    ('sqlite3session_delete', [P], None), ('sqlite3_free', [P], None),
]:
    fn = getattr(lib, name)
    fn.argtypes, fn.restype = args, result

OWNER_SQL = '''CREATE TABLE monsterft_local_owner (
 id INTEGER PRIMARY KEY CHECK (id = 1),
 generation BLOB NOT NULL CHECK (length(generation) = 16)
) STRICT'''


class DB:
    def __init__(self, path, configure=True):
        self.p, self.path = P(), pathlib.Path(path)
        rc = lib.sqlite3_open(str(path).encode(), C.byref(self.p))
        if rc:
            raise RuntimeError((rc, lib.sqlite3_errmsg(self.p)))
        if configure:
            self.sql('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; '
                     'PRAGMA wal_autocheckpoint=0;')
            assert self.query('PRAGMA journal_mode') == [('wal',)]
            assert self.query('PRAGMA synchronous') == [(2,)]

    def sql(self, sql):
        rc = lib.sqlite3_exec(self.p, sql.encode(), None, None, None)
        if rc:
            raise RuntimeError((rc, lib.sqlite3_errmsg(self.p).decode(), sql[:240]))

    def query(self, sql):
        stmt = P()
        rc = lib.sqlite3_prepare_v2(self.p, sql.encode(), -1, C.byref(stmt), None)
        if rc:
            raise RuntimeError((rc, lib.sqlite3_errmsg(self.p).decode(), sql[:240]))
        rows = []
        try:
            while True:
                rc = lib.sqlite3_step(stmt)
                if rc == 101:
                    return rows
                if rc != 100:
                    raise RuntimeError((rc, lib.sqlite3_errmsg(self.p).decode()))
                row = []
                for i in range(lib.sqlite3_column_count(stmt)):
                    typ = lib.sqlite3_column_type(stmt, i)
                    if typ == 1:
                        value = lib.sqlite3_column_int64(stmt, i)
                    elif typ == 2:
                        value = lib.sqlite3_column_double(stmt, i)
                    elif typ in (3, 4):
                        size = lib.sqlite3_column_bytes(stmt, i)
                        ptr = (lib.sqlite3_column_text if typ == 3
                               else lib.sqlite3_column_blob)(stmt, i)
                        value = C.string_at(ptr, size) if ptr else b''
                        if typ == 3:
                            value = value.decode()
                    elif typ == 5:
                        value = None
                    else:
                        raise AssertionError(typ)
                    row.append(value)
                rows.append(tuple(row))
        finally:
            lib.sqlite3_finalize(stmt)

    def pages(self):
        own = bool(lib.sqlite3_get_autocommit(self.p))
        if own:
            self.sql('BEGIN')
        try:
            return dict(self.query('SELECT pgno,data FROM sqlite_dbpage ORDER BY pgno'))
        finally:
            if own:
                self.sql('COMMIT')

    def close(self):
        if self.p:
            rc = lib.sqlite3_close(self.p)
            if rc:
                raise RuntimeError(('close', rc))
            self.p = P()


class Session:
    def __init__(self, db):
        self.p = P()
        assert lib.sqlite3session_create(db.p, b'main', C.byref(self.p)) == 0
        assert lib.sqlite3session_attach(self.p, None) == 0

    def patchset(self):
        n, ptr = C.c_int(), P()
        assert lib.sqlite3session_patchset(self.p, C.byref(n), C.byref(ptr)) == 0
        try:
            return C.string_at(ptr, n.value)
        finally:
            lib.sqlite3_free(ptr)

    def close(self):
        if self.p:
            lib.sqlite3session_delete(self.p)
            self.p = P()


_templates = {}


def owner_template(page_size):
    if page_size not in _templates:
        db = DB(':memory:', configure=False)
        try:
            db.sql('PRAGMA page_size=%d' % page_size)
            db.sql(OWNER_SQL)
            db.sql("INSERT INTO monsterft_local_owner VALUES(1,x'%s')" % ('42' * 16))
            root = db.query("SELECT rootpage FROM sqlite_schema "
                            "WHERE name='monsterft_local_owner'")[0][0]
            _templates[page_size] = db.pages()[root]
        finally:
            db.close()
    return _templates[page_size]


def owner_root(db, pages):
    schema = db.query("SELECT type,rootpage FROM sqlite_schema "
                      "WHERE name='monsterft_local_owner'")
    assert len(schema) == 1 and schema[0][0] == 'table', 'owner schema missing'
    root = schema[0][1]
    columns = db.query('PRAGMA table_info(monsterft_local_owner)')
    assert [(c[1], c[2], c[5]) for c in columns] == [
        ('id', 'INTEGER', 1), ('generation', 'BLOB', 0)], 'owner columns changed'
    assert not db.query('PRAGMA index_list(monsterft_local_owner)'), 'owner index added'
    rows = db.query('SELECT id,generation FROM monsterft_local_owner')
    assert len(rows) == 1 and rows[0][0] == 1, 'owner not singleton'
    token = rows[0][1]
    assert isinstance(token, bytes) and len(token) == 16 and any(token), 'invalid owner token'
    page = pages[root]
    assert root > 1 and page[0] == 13, 'owner is not a leaf root'
    assert int.from_bytes(page[3:5], 'big') == 1, 'owner has multiple cells'
    # The validated row has only a tiny fixed payload; it cannot need overflow.
    return root


def canonical_pages(db):
    own = bool(lib.sqlite3_get_autocommit(db.p))
    if own:
        db.sql('BEGIN')
    try:
        pages = db.pages()
        root = owner_root(db, pages)
        header = bytearray(pages[1])
        assert header[:16] == b'SQLite format 3\x00'
        assert header[20] == 0, 'reserved bytes not covered by experiment'
        header[24:28] = b'\x00' * 4
        header[92:96] = b'\x00' * 4
        pages[1] = bytes(header)
        pages[root] = owner_template(len(header))
        return pages
    finally:
        if own:
            db.sql('COMMIT')


def metadata(db):
    return {
        'sqlite': lib.sqlite3_libversion().decode(),
        'sourceId': lib.sqlite3_sourceid().decode(),
        'library': LIB_PATH,
        'compileOptions': [r[0] for r in db.query('PRAGMA compile_options')],
        'settings': {p: db.query('PRAGMA ' + p)[0][0] for p in (
            'journal_mode', 'synchronous', 'page_size', 'auto_vacuum',
            'secure_delete', 'cache_size', 'cache_spill', 'wal_autocheckpoint')},
    }


def snapshot(db_or_path, include_pages=False):
    external = not isinstance(db_or_path, DB)
    db = DB(db_or_path, configure=False) if external else db_or_path
    own = bool(lib.sqlite3_get_autocommit(db.p))
    if own:
        db.sql('BEGIN')
    try:
        raw = db.pages()
        canonical = canonical_pages(db)
        root = owner_root(db, raw)
        info = {
            'readerSQLite': lib.sqlite3_libversion().decode(),
            'pageSize': len(raw[1]), 'pageCount': len(raw), 'ownerPage': root,
            'headerCounters': [int.from_bytes(raw[1][i:i + 4], 'big') for i in (24, 92)],
            'writerSQLiteNumber': int.from_bytes(raw[1][96:100], 'big'),
            'schema': db.query('SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema ORDER BY name'),
            'rawHashes': {str(p): hashlib.sha256(b).hexdigest() for p, b in raw.items()},
            'canonicalHashes': {str(p): hashlib.sha256(b).hexdigest() for p, b in canonical.items()},
        }
        if include_pages:
            info['pages'] = {str(p): base64.b64encode(b).decode() for p, b in raw.items()}
        return info
    finally:
        if own:
            db.sql('COMMIT')
        if external:
            db.close()


if __name__ == '__main__':
    if len(sys.argv) < 3 or sys.argv[1] != 'snapshot':
        raise SystemExit('usage: sqlite_probe.py snapshot DB [--pages]')
    print(json.dumps(snapshot(sys.argv[2], '--pages' in sys.argv[3:])))
