import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { SQLiteLog } from '../src/log.v2.js'
import { ProtocolStore, databasePaths, openApplyDatabase, readDatabase } from '../src/storage.v2.js'
import * as sql from '../src/utils.v2.js'
import { digestEntry } from '../src/utilm.v2.js'
import * as Err from '../src/error.v2.js'

const roles = ['procA', 'procB', 'procC']
const record = (term=1n, entry='entry') => ({ term, entry: Buffer.from(entry) })

function fixture() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-v2-storage-'))
  const base = path.join(dir, 'node.db')
  let stores = []
  let apply = null
  const close = () => {
    for (const store of stores) { store.close() }
    if (apply !== null) { apply.close(); apply = null }
  }
  return {
    base, paths: databasePaths(base),
    get stores() { return stores },
    get apply() { return apply },
    open() {
      stores = roles.map((role) => new ProtocolStore(role, base))
      stores.forEach((store) => store.open())
      apply = openApplyDatabase(base)
      return this
    },
    capture() { return stores.map((store) => store.capture()) },
    reconcile() {
      this.capture()
      stores.forEach((store) => store.adopt())
      return stores.map((store) => store.reconcile())
    },
    close,
    cleanup() { close(); fs.rmSync(dir, { recursive: true, force: true }) },
  }
}

function receipt(db2, seq, data, digest=Buffer.alloc(32)) {
  db2.prepare(`
    UPDATE monsterft_meta_apply SET seq = ?, entry_hash = ?, local_digest = ? WHERE id = 1
  `).run(seq, digestEntry(data.term, data.entry), digest)
}

test('v2 log retains payload records and joins executor transactions', (t) => {
  const f = fixture()
  const log = new SQLiteLog(f.base)
  try {
    t.throws(() => new SQLiteLog(':memory:'), /file path/, 'memory log rejected')
    log.open()
    const entries = [record(0n, ''), record(9_223_372_036_854_775_807n, '\0\xff'), record(2n, 'x')]
    log.db.exec('BEGIN IMMEDIATE')
    log.election(7n, 'a')
    log.appendBatch(entries)
    t.ok(log.db.isTransaction, 'batch leaves executor transaction open')
    t.equal(log.seq, 2n, 'cache includes transaction writes')
    t.equal(readDatabase(f.base, (db) => db.prepare('SELECT seq FROM raft_log').all().length),
      0, 'reader cannot see uncommitted input')
    log.db.exec('ROLLBACK')
    log.readHead()
    t.equal(log.seq, -1n, 'rollback refresh restores empty cache')
    t.equal(log.elec.term, 0n, 'election and append rolled back together')
    log.appendBatch(entries)
    const rows = log.db.prepare('SELECT term, entry FROM raft_log ORDER BY seq').all()
    t.deepEqual(rows.map((row) => ({ term: row.term, entry: Buffer.from(row.entry) })), entries,
      'stored term and exact empty/binary payloads are separate')
    t.deepEqual([...log.iter(0n, { iterStepSize: 1 })], entries, 'paged iteration preserves mixed terms')
    t.throws(() => log.append(record(-1n)), /term must be >=/, 'negative term rejected')
    t.throws(() => log.append(record(9_223_372_036_854_775_808n)), /term must be <=/,
      'oversized term rejected')
    t.throws(() => log.append({ term: 1, entry: Buffer.alloc(0) }), /term must be bigint/)
    t.throws(() => log.append({ term: 1n, entry: new Uint8Array(2) }), /entry must be buffer/)
    log.close()
    log.open()
    t.equal(log.seq, 2n, 'reopening restores head')
    log.trim(0n)
    t.equal(log.seq, 0n, 'trim preserves lower record')
  } finally { log.close(); f.cleanup() }
  t.end()
})

test('v2 standalone batch failure rolls back every inserted row', (t) => {
  const f = fixture()
  const log = new SQLiteLog(f.base)
  try {
    log.open()
    log.append(record())
    log.db.exec(`
      CREATE TRIGGER reject_second BEFORE INSERT ON raft_log WHEN NEW.seq = 2
      BEGIN SELECT RAISE(ABORT, 'injected insert'); END
    `)
    t.throws(() => log.appendBatch([record(), record()]), /injected insert/)
    t.equal(log.seq, 0n, 'cache remains at committed head')
    t.equal(log.db.prepare('SELECT count(*) AS n FROM raft_log').get().n, 1n)
    t.notOk(log.db.isTransaction, 'owned transaction ended')
  } finally { log.close(); f.cleanup() }
  t.end()
})

test('v2 all DB1 schema creation rolls back together', (t) => {
  const f = fixture()
  const log = new SQLiteLog(f.paths.procA)
  try {
    t.throws(() => log.open({ initialize(db) {
      sql.createMeta(db)
      throw new Error('injected creation')
    } }), /injected creation/)
    t.equal(readDatabase(f.paths.procA, (db) => db.prepare('SELECT name FROM sqlite_schema').all().length),
      0, 'no partial Raft or Monster tables escape')
    f.open()
    t.ok(f.reconcile().every((meta) => meta.applied_seq === -1n), 'interrupted empty schema initializes')
  } finally { log.close(); f.cleanup() }
  t.end()
})

test('v2 log rejects absent term columns and corrupt stored terms', (t) => {
  const f = fixture()
  const old = new DatabaseSync(f.base)
  old.exec(`
    CREATE TABLE raft_log (seq INTEGER PRIMARY KEY, entry BLOB NOT NULL) STRICT;
    CREATE TABLE raft_election (id INTEGER PRIMARY KEY, current_term INTEGER, voted_for TEXT) STRICT;
    INSERT INTO raft_election VALUES (1, 0, NULL)
  `)
  old.close()
  const log = new SQLiteLog(f.base)
  try {
    t.throws(() => log.open(), /term/, 'missing term column fails preparation')
    t.equal(readDatabase(f.base, (db) => db.prepare('PRAGMA table_info(raft_log)').all().length),
      2, 'schema remains unchanged')
    fs.rmSync(f.base)
    log.open()
    log.append(record())
    log.db.exec('UPDATE raft_log SET term = -1')
    log.close()
    t.throws(() => log.open(), (err) => err.code === Err.LOG_CORRUPT, 'corrupt stored term rejected')
  } finally { log.close(); f.cleanup() }
  t.end()
})

test('v2 fresh metadata can resume after each creation boundary', (t) => {
  for (let completed = 0; completed <= 4; completed++) {
    const f = fixture()
    try {
      for (let i = 0; i < Math.min(3, completed); i++) {
        const store = new ProtocolStore(roles[i], f.base)
        store.open()
        store.close()
      }
      if (completed === 4) { openApplyDatabase(f.base).close() }
      f.open()
      const metas = f.reconcile()
      t.ok(metas.every((meta) => meta.applied_seq === -1n), `initial checkpoint after ${completed} creations`)
      for (const store of f.stores) {
        const names = store.db.prepare("SELECT name FROM sqlite_schema WHERE type = 'table'")
          .all().map((row) => row.name).sort()
        t.deepEqual(names, ['monsterft_meta', 'raft_election', 'raft_log'], 'all DB1 tables present')
      }
      t.equal(sql.readApply(f.apply).seq, -1n, 'initial receipt')
    } finally { f.cleanup() }
  }
  const f = fixture()
  try {
    openApplyDatabase(f.base).close()
    f.open()
    t.equal(f.reconcile()[0].applied_seq, -1n, 'Apply may open before Procs')
  } finally { f.cleanup() }
  t.end()
})

test('v2 invalid established DB1 cannot be rebuilt from two valid peers', (t) => {
  for (const damage of ['missing', 'schema', 'meta', 'term', 'unreadable']) {
    const f = fixture().open()
    try {
      f.stores.forEach((store) => store.log.append(record()))
      f.close()
      if (damage === 'missing') {
        fs.rmSync(f.paths.procC)
      } else if (damage === 'unreadable') {
        fs.writeFileSync(f.paths.procC, 'not a sqlite database')
      } else {
        const db = new DatabaseSync(f.paths.procC)
        if (damage === 'schema') { db.exec('DROP TABLE monsterft_meta') }
        if (damage === 'meta') { db.exec('DELETE FROM monsterft_meta') }
        if (damage === 'term') { db.exec('UPDATE raft_election SET current_term = -1') }
        db.close()
      }
      t.throws(() => new ProtocolStore('procA', f.base).open(), undefined, `${damage} peer stops startup`)
      if (damage === 'missing') { t.notOk(fs.existsSync(f.paths.procC), 'missing peer remains absent') }
    } finally { f.cleanup() }
  }
  t.end()
})

test('v2 capture selects one combined majority and atomically copies retained rows', (t) => {
  const f = fixture().open()
  try {
    for (const store of f.stores.slice(0, 2)) {
      store.log.appendBatch([record(1n, 'zero'), record(2n, 'one'), record(3n, 'two')])
      store.log.election(4n, 'member-a')
      store.db.exec('DELETE FROM raft_log WHERE seq = 0')
      store.log.readHead()
    }
    f.stores[2].log.appendBatch([record(8n, 'other'), record(8n, 'suffix')])
    f.stores[2].log.election(9n, null)
    const reports = f.capture()
    t.ok(reports.every((report) => report.donor === 'procA'), 'first winning Proc is donor')
    t.ok(reports.every((report) => report.state.equals(reports[0].state)), 'independent selections match')
    f.stores.forEach((store) => store.adopt())
    t.equal(f.stores[2].log.begin, 1n, 'donor retained boundary copied')
    t.equal(f.stores[2].log.seq, 2n, 'multi-row head replaced')
    t.equal(f.stores[2].log.elec.term, 4n, 'election restored with log')
    t.deepEqual([...f.stores[2].log.iter(1n)], [...f.stores[0].log.iter(1n)], 'all retained payloads copied')
    t.throws(() => readDatabase(f.paths.procA, (db) => db.exec('DELETE FROM raft_log')),
      /readonly/, 'peer connection cannot write')
    f.stores.forEach((store) => store.reconcile())
  } finally { f.cleanup() }
  t.end()
})

test('v2 does not choose separate majorities for individual tables', (t) => {
  const f = fixture().open()
  try {
    f.stores[0].log.election(1n, null)
    f.stores[0].log.append(record(1n, 'x'))
    f.stores[1].log.election(1n, null)
    f.stores[1].log.append(record(2n, 'y'))
    f.stores[2].log.election(2n, null)
    f.stores[2].log.append(record(1n, 'x'))
    t.throws(() => f.capture(), /no majority/, 'combined states have no majority')
  } finally { f.cleanup() }
  t.end()
})

test('v2 adopted history failure closes and rolls back the entire copy', (t) => {
  const f = fixture().open()
  try {
    for (const store of f.stores.slice(0, 2)) {
      store.log.appendBatch([record(1n, 'zero'), record(1n, 'one'), record(1n, 'two')])
    }
    const outlier = f.stores[2]
    outlier.log.append(record(2n, 'original'))
    outlier.db.exec(`
      CREATE TRIGGER reject_copy BEFORE INSERT ON raft_log WHEN NEW.seq = 1
      BEGIN SELECT RAISE(ABORT, 'injected adoption'); END
    `)
    f.capture()
    t.throws(() => outlier.adopt(), /injected adoption/)
    t.notOk(outlier.log.isOpen, 'failed destination closed')
    const rows = readDatabase(f.paths.procC, (db) => db.prepare('SELECT seq, term, entry FROM raft_log').all())
    t.equal(rows.length, 1, 'replacement delete rolled back')
    t.equal(Buffer.from(rows[0].entry).toString(), 'original', 'original suffix remains')
    const db = new DatabaseSync(f.paths.procC)
    db.exec('DROP TRIGGER reject_copy')
    db.close()
    outlier.open()
    f.reconcile()
    t.equal(outlier.log.seq, 2n, 'reopened repair completes')
  } finally { f.cleanup() }
  t.end()
})

test('v2 majority survives interruptions between each receipt import', (t) => {
  for (let imported = 0; imported <= 3; imported++) {
    const f = fixture().open()
    try {
      const data = record(1n, '')
      f.stores.forEach((store) => store.log.append(data))
      receipt(f.apply, 0n, data)
      f.capture()
      f.stores.forEach((store) => store.adopt())
      for (const store of f.stores.slice(0, imported)) { store.reconcile() }
      f.close()
      f.open()
      const metas = f.reconcile()
      t.ok(metas.every((meta) => meta.applied_seq === 0n), `completed setup restored after ${imported} imports`)
      t.ok(metas.every((meta) => meta.pending_cmd_seq === null), 'setup never becomes pending CMD')
      t.ok(metas.every((meta) => Buffer.from(meta.applied_local_digest).equals(Buffer.alloc(32))),
        'zero setup digest retained')
      t.equal(sql.readApply(f.apply).seq, 0n, 'DB2 unchanged')
    } finally { f.cleanup() }
  }
  t.end()
})

test('v2 receipt is checked against adopted DB1 and the captured fixed receipt', (t) => {
  const f = fixture().open()
  try {
    const data = record(1n, '')
    f.stores[0].log.append(data)
    f.stores[1].log.append(data)
    f.stores[2].log.append(record(2n, 'outlier'))
    receipt(f.apply, 0n, data)
    f.capture()
    f.stores.forEach((store) => store.adopt())
    // A later receipt cannot replace the already captured input. Startup normally
    // keeps Apply idle; this also verifies the reusable reconciliation primitive.
    receipt(f.apply, 1n, record(2n, 'later'), Buffer.alloc(32, 3))
    const metas = f.stores.map((store) => store.reconcile())
    t.ok(metas.every((meta) => meta.applied_seq === 0n), 'all import fixed seq-zero receipt')
    t.equal(sql.readApply(f.apply).seq, 1n, 'DB2 is read-only during Proc reconciliation')
  } finally { f.cleanup() }
  t.end()
})

test('v2 failed receipt import preserves DB2 work for reopening', (t) => {
  const f = fixture().open()
  try {
    const data = record(1n, '')
    f.stores.forEach((store) => store.log.append(data))
    receipt(f.apply, 0n, data)
    f.stores[0].db.exec(`
      CREATE TRIGGER reject_import BEFORE UPDATE ON monsterft_meta
      BEGIN SELECT RAISE(ABORT, 'injected import'); END
    `)
    f.capture()
    f.stores.forEach((store) => store.adopt())
    t.throws(() => f.stores[0].reconcile(), /injected import/)
    t.equal(readDatabase(f.paths.procA, sql.readMeta).applied_seq, -1n, 'DB1 import rolled back')
    t.equal(sql.readApply(f.apply).seq, 0n, 'completed Apply receipt remains')
    const db = new DatabaseSync(f.paths.procA)
    db.exec('DROP TRIGGER reject_import')
    db.close()
    f.stores[0].open()
    t.ok(f.reconcile().every((meta) => meta.applied_seq === 0n), 'next startup completes import')
  } finally { f.cleanup() }
  t.end()
})

test('v2 receipt validation retains hash, pending and gap protections', (t) => {
  for (const kind of ['hash', 'gap', 'missing', 'pending', 'repair']) {
    const f = fixture().open()
    try {
      const data = record(1n, '')
      f.stores.forEach((store) => store.log.append(data))
      receipt(f.apply, kind === 'gap' ? 2n : kind === 'missing' ? 1n : 0n,
        kind === 'hash' ? record(1n, 'different') : data)
      if (kind === 'pending' || kind === 'repair') {
        receipt(f.apply, 0n, data)
        f.reconcile()
        const next = record(1n, 'cmd')
        f.stores.forEach((store) => {
          store.log.append(next)
          store.db.exec(kind === 'pending' ?
            'UPDATE monsterft_meta SET pending_cmd_seq = 0' :
            'UPDATE monsterft_meta SET repair_state = 1')
        })
        receipt(f.apply, 1n, next, Buffer.alloc(32, 4))
      }
      f.capture()
      f.stores.forEach((store) => store.adopt())
      t.throws(() => f.stores[0].reconcile(), (err) => err.code === Err.MONSTER_CORRUPT,
        `${kind} receipt rejected`)
    } finally { f.cleanup() }
  }
  t.end()
})

test('v2 retained digest permits reopening after the receipt entry was pruned', (t) => {
  const f = fixture().open()
  try {
    const zero = record(1n, '')
    const cmd = record(1n, 'cmd')
    const tail = record(2n, '')
    const localDigest = Buffer.alloc(32, 7)
    f.stores.forEach((store) => store.log.append(zero))
    receipt(f.apply, 0n, zero)
    f.reconcile()
    f.stores.forEach((store) => store.log.appendBatch([cmd, tail, tail, tail]))
    receipt(f.apply, 1n, cmd, localDigest)
    f.reconcile()
    for (const store of f.stores) {
      store.db.prepare(`
        UPDATE monsterft_meta SET applied_seq = 4, applied_entry_hash = ?, pending_cmd_seq = NULL
      `).run(digestEntry(tail.term, tail.entry))
      store.db.exec('DELETE FROM raft_log WHERE seq < 3')
      store.log.readHead()
    }
    f.close()
    f.open()
    const metas = f.reconcile()
    t.ok(metas.every((meta) => meta.applied_seq === 4n), 'protocol checkpoint remains ahead of receipt')
    t.ok(metas.every((meta) => Buffer.from(meta.applied_local_digest).equals(localDigest)),
      'resolved CMD digest survives SYNC/noops/retention')
    t.ok(f.stores.every((store) => store.log.begin === 3n), 'old receipt entry not recreated')
    f.apply.prepare('UPDATE monsterft_meta_apply SET local_digest = ?').run(Buffer.alloc(32, 8))
    f.capture()
    f.stores.forEach((store) => store.adopt())
    t.throws(() => f.stores[0].reconcile(), /apply local digest does not match/,
      'digest mismatch remains fatal even without receipt log entry')
  } finally { f.cleanup() }
  t.end()
})

test('v2 checkpoint authentication includes both term and payload', (t) => {
  for (const column of ['term', 'entry']) {
    const f = fixture().open()
    try {
      const data = record(1n, '')
      f.stores.forEach((store) => store.log.append(data))
      receipt(f.apply, 0n, data)
      f.reconcile()
      for (const store of f.stores) {
        if (column === 'term') { store.db.exec('UPDATE raft_log SET term = 2') }
        else { store.db.prepare('UPDATE raft_log SET entry = ?').run(Buffer.from('changed')) }
      }
      f.capture()
      f.stores.forEach((store) => store.adopt())
      t.throws(() => f.stores[0].reconcile(), /applied entry hash does not match/,
        `changing only ${column} rejects checkpoint`)
    } finally { f.cleanup() }
  }
  t.end()
})
