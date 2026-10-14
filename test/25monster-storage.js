import crypto from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { pack } from 'msgpackr'
import test from 'tape'
import * as Err from '../src/error.js'
import { SQLiteLog } from '../src/log.js'
import { MonsterFt } from '../src/monsterft.js'
import { digestEntry, initialize, reconcile } from '../src/monstermeta.js'

const fixture = (t, disk=false, initialized=true) => {
  const dir = disk ? fs.mkdtempSync(path.join(os.tmpdir(), 'monster-storage-')) : null
  const file = disk ? path.join(dir, 'db') : ':memory:'
  const log = new SQLiteLog(file)
  log.open()
  const db1 = log.db
  const db2 = new DatabaseSync(disk ? `${file}2` : ':memory:', { readBigInts: true })
  const close = () => {
    log.close()
    if (db2.isOpen) { db2.close() }
  }
  t.teardown(() => {
    close()
    if (dir !== null) { fs.rmSync(dir, { recursive: true, force: true }) }
  })
  if (initialized) { initialize(db1, db2) }
  return { db1, db2, log, file, close }
}

const meta = (db) => db.prepare('SELECT * FROM monsterft_meta WHERE id = 1').get()
const receipt = (db) => db.prepare('SELECT * FROM monsterft_meta_apply WHERE id = 1').get()
const saveReceipt = (db, seq, entryHash, digest) => {
  db.prepare(`
    UPDATE monsterft_meta_apply SET seq = ?, entry_hash = ?, local_digest = ?
    WHERE id = 1
  `).run(seq, entryHash, digest)
}
const append = (log, seq, digest=Buffer.alloc(32, 9)) => {
  const record = { term: 5n, entry: Buffer.from(`entry ${seq}`) }
  log.append(record, seq)
  return { hash: digestEntry(record.term, record.entry), digest }
}
const setup = (state) => {
  const { hash, digest } = append(state.log, 0n, Buffer.alloc(32))
  saveReceipt(state.db2, 0n, hash, digest)
  return reconcile(state.db1, state.db2)
}
const isCode = (code) => (err) => err instanceof Err.ErrorWithCode && err.code === code

test('split metadata initializes, imports setup once, and checks exact entry bytes', (t) => {
  const state = fixture(t)
  const { db1, db2, log } = state
  t.equal(meta(db1).applied_seq, -1n, 'DB1 starts before application')
  t.equal(receipt(db2).seq, -1n, 'DB2 starts before application')
  t.equal(db1.prepare("SELECT name FROM sqlite_schema WHERE name = 'monsterft_meta_apply'").get(),
    undefined, 'DB1 has no receipt table')
  t.equal(db2.prepare("SELECT name FROM sqlite_schema WHERE name = 'monsterft_meta'").get(),
    undefined, 'DB2 has no protocol metadata table')
  const initial = setup(state)
  t.equal(initial.applied_seq, 0n, 'setup receipt advances checkpoint')
  t.equal(initial.pending_cmd_seq, null, 'setup is not pending agreement')
  t.deepEqual(Buffer.from(initial.applied_local_digest), Buffer.alloc(32), 'setup retains zero digest')
  t.deepEqual(reconcile(db1, db2), initial, 'reconciliation is idempotent')

  const payload = log.head
  const prefix = Buffer.alloc(8)
  prefix.writeBigUInt64LE(log.term)
  const expected = crypto.createHash('sha256').update(Buffer.concat([prefix, payload])).digest()
  t.deepEqual(Buffer.from(initial.applied_entry_hash), expected, 'entry hash bytes remain unchanged')
  db1.prepare('UPDATE raft_log SET term = term + 1 WHERE seq = 0').run()
  t.throws(() => reconcile(db1, db2), isCode(Err.MONSTER_CORRUPT), 'changed term rejects reopening')
  db1.prepare('UPDATE raft_log SET term = ?, entry = ? WHERE seq = 0').run(log.term, Buffer.from('changed'))
  t.throws(() => reconcile(db1, db2), isCode(Err.MONSTER_CORRUPT), 'changed payload rejects reopening')
  t.end()
})

test('receipt reconciliation uses identity without decoding entry type', (t) => {
  const state = fixture(t)
  setup(state)
  const record = append(state.log, 1n)
  saveReceipt(state.db2, 1n, record.hash, record.digest)
  const imported = reconcile(state.db1, state.db2)
  t.equal(imported.pending_cmd_seq, 1n, 'arbitrary matching payload imports as later pending application')
  t.deepEqual(Buffer.from(imported.applied_local_digest), record.digest, 'apply digest is imported')
  t.end()
})

test('settled apply digest survives newer checkpoints and pruned input', (t) => {
  const state = fixture(t)
  setup(state)
  const applied = append(state.log, 1n)
  saveReceipt(state.db2, 1n, applied.hash, applied.digest)
  reconcile(state.db1, state.db2)
  append(state.log, 2n)
  const later = append(state.log, 3n)
  state.db1.prepare(`
    UPDATE monsterft_meta SET applied_seq = 3, applied_entry_hash = ?,
      pending_cmd_seq = NULL, repair_state = 2
  `).run(later.hash)
  state.db1.prepare('DELETE FROM raft_log WHERE seq < 2').run()
  const restored = reconcile(state.db1, state.db2)
  t.equal(restored.applied_seq, 3n, 'newer protocol checkpoint remains')
  t.equal(restored.pending_cmd_seq, null, 'settled application stays settled')
  t.equal(restored.repair_state, 2n, 'repair fence remains')
  t.deepEqual(Buffer.from(restored.applied_local_digest), applied.digest, 'pruned application remains linked by digest')
  saveReceipt(state.db2, 1n, applied.hash, Buffer.alloc(32, 7))
  t.throws(() => reconcile(state.db1, state.db2), isCode(Err.MONSTER_CORRUPT), 'different file receipt digest is rejected')
  t.end()
})

test('receipt validation rejects gaps, missing interior history, and pending mismatch', (t) => {
  const state = fixture(t)
  setup(state)
  const first = append(state.log, 1n)
  const second = append(state.log, 2n)
  saveReceipt(state.db2, 2n, second.hash, second.digest)
  t.throws(() => reconcile(state.db1, state.db2), isCode(Err.MONSTER_CORRUPT), 'more than one ahead cannot import')
  saveReceipt(state.db2, 1n, first.hash, first.digest)
  state.db1.exec('UPDATE monsterft_meta SET repair_state = 1')
  t.throws(() => reconcile(state.db1, state.db2), isCode(Err.MONSTER_CORRUPT), 'repair fence prevents import')
  state.db1.exec('UPDATE monsterft_meta SET repair_state = 0')
  reconcile(state.db1, state.db2)
  state.db1.prepare('UPDATE monsterft_meta SET applied_seq = 2, applied_entry_hash = ?, pending_cmd_seq = 2').run(second.hash)
  t.throws(() => reconcile(state.db1, state.db2), isCode(Err.MONSTER_CORRUPT), 'pending sequence must match receipt')
  state.db1.exec('UPDATE monsterft_meta SET pending_cmd_seq = NULL; DELETE FROM raft_log WHERE seq = 1')
  t.throws(() => reconcile(state.db1, state.db2), isCode(Err.MONSTER_CORRUPT), 'missing interior receipt entry is not mistaken for retention')
  t.end()
})

test('malformed log values retain log corruption classification', (t) => {
  const state = fixture(t)
  setup(state)
  state.db1.exec('UPDATE raft_log SET term = -1 WHERE seq = 0')
  t.throws(() => reconcile(state.db1, state.db2), isCode(Err.LOG_CORRUPT), 'negative term is log corruption')
  t.end()
})

test('an interrupted first initialization can finish only while pristine', (t) => {
  const state = fixture(t, false, false)
  const originalExec = state.db2.exec.bind(state.db2)
  const failure = new Error('injected receipt creation failure')
  state.db2.exec = (sql) => {
    if (sql.includes('INSERT INTO monsterft_meta_apply')) { throw failure }
    return originalExec(sql)
  }
  t.throws(() => initialize(state.db1, state.db2), (err) => err === failure, 'original initialization failure survives rollback')
  t.equal(meta(state.db1).applied_seq, -1n, 'DB1 creation committed first')
  t.notOk(state.db2.isTransaction, 'failed DB2 creation rolls back transaction')
  state.db2.exec = originalExec
  t.equal(initialize(state.db1, state.db2).applied_seq, -1n, 'retry finishes pristine receipt creation')
  state.db2.exec('DROP TABLE monsterft_meta_apply; CREATE TABLE user_data (id INTEGER)')
  t.throws(() => initialize(state.db1, state.db2), isCode(Err.MONSTER_CORRUPT), 'application schema forbids recreating missing metadata')
  t.end()
})

test('missing metadata cannot be silently recreated beside existing state', (t) => {
  const populated = fixture(t, false, false)
  populated.db2.exec('CREATE TABLE user_data (id INTEGER PRIMARY KEY)')
  t.throws(() => initialize(populated.db1, populated.db2), isCode(Err.MONSTER_CORRUPT),
    'application schema prevents fresh initialization')
  t.equal(populated.db1.prepare("SELECT name FROM sqlite_schema WHERE name = 'monsterft_meta'").get(),
    undefined, 'rejected initialization leaves DB1 untouched')
  const empty = fixture(t)
  empty.db2.exec('DELETE FROM monsterft_meta_apply')
  t.throws(() => initialize(empty.db1, empty.db2), isCode(Err.MONSTER_CORRUPT), 'missing receipt row is not initialized')
  t.end()
})

test('failed receipt import preserves both durable sides for retry', (t) => {
  const state = fixture(t)
  setup(state)
  const next = append(state.log, 1n)
  saveReceipt(state.db2, 1n, next.hash, next.digest)
  state.db1.exec(`
    CREATE TRIGGER fail_import BEFORE UPDATE ON monsterft_meta
    BEGIN SELECT RAISE(ABORT, 'injected import failure'); END
  `)
  t.throws(() => reconcile(state.db1, state.db2), isCode(Err.SQLITE_ERROR), 'failed import is SQLite failure')
  t.equal(meta(state.db1).applied_seq, 0n, 'checkpoint remains before application')
  t.equal(receipt(state.db2).seq, 1n, 'committed apply receipt remains')
  state.db1.exec('DROP TRIGGER fail_import')
  t.equal(reconcile(state.db1, state.db2).pending_cmd_seq, 1n, 'retry imports retained receipt')
  t.end()
})

test('certification rolls back discovered pending application unless overridden', (t) => {
  const state = fixture(t, true)
  setup(state)
  const next = append(state.log, 1n)
  saveReceipt(state.db2, 1n, next.hash, next.digest)
  const before = receipt(state.db2)
  state.close()
  t.throws(() => MonsterFt.certify(state.file), isCode(Err.ARGUMENT_ILLEGAL), 'default certification rejects recovered pending application')
  let check = new DatabaseSync(state.file, { readBigInts: true })
  t.equal(meta(check).applied_seq, 0n, 'rejected certification rolls back tentative import')
  check.close()
  t.equal(MonsterFt.certify(state.file, true), 1n, 'override commits recovered pending application')
  check = new DatabaseSync(state.file, { readBigInts: true })
  t.equal(meta(check).pending_cmd_seq, 1n, 'override keeps pending identity')
  check.close()
  check = new DatabaseSync(`${state.file}2`, { readBigInts: true })
  t.deepEqual(receipt(check), before, 'certification leaves DB2 receipt untouched')
  check.close()
  t.end()
})

test('certification override never bypasses receipt corruption', (t) => {
  const state = fixture(t, true)
  setup(state)
  state.db1.exec('UPDATE monsterft_meta SET repair_state = 2')
  state.db2.exec('UPDATE monsterft_meta_apply SET local_digest = randomblob(32)')
  state.close()
  t.throws(() => MonsterFt.certify(state.file, true), isCode(Err.MONSTER_CORRUPT), 'override still verifies apply digest')
  const check = new DatabaseSync(state.file, { readBigInts: true })
  t.equal(meta(check).repair_state, 2n, 'failed certification preserves repair fence')
  check.close()
  t.end()
})

test('a second CMD cannot replace an application still awaiting SYNC', async (t) => {
  const state = fixture(t, true, false)
  state.close()
  const nodes = []
  let calls = 0
  const build = () => {
    const node = new MonsterFt('1', ['1', '2', '3'], () => {}, state.file, {
      electionTimeout: 60_000,
      apply(db, buf, term, seq) {
        if (seq === 0n) {
          db.exec('CREATE TABLE app (id INTEGER PRIMARY KEY, count INTEGER)')
          db.exec('INSERT INTO app VALUES (1, 0)')
          return null
        }
        calls++
        db.exec('UPDATE app SET count = count + 1')
        return calls
      },
    })
    node.on('error', () => {})
    node.on('warn', () => {})
    nodes.push(node)
    return node
  }
  t.teardown(() => nodes.forEach((node) => node.close()))
  const node = build()
  node.open()
  node.log.append({ term: 0n, entry: Buffer.alloc(0) })
  for (let i = 0; i < 2; i++) {
    node.log.append({
      term: 0n,
      entry: Buffer.from(pack({
        type: 'cmd', items: [Buffer.from(`${i}`)], matchIndex: [-1n, -1n, -1n],
      })),
    })
  }
  node._commitSeq = 2n
  node._commitTerm = 0n
  const error = await node._apply(2n).then(() => null, (err) => err)
  t.match(error?.message ?? '', /CMD entry while another CMD is pending/,
    'corrupt consecutive commands fail before the next application')
  t.equal(calls, 1, 'the second apply callback never runs')
  t.notOk(node.isOpen, 'invalid command order closes the node')

  const reopened = build()
  reopened.open()
  t.equal(reopened._applySeq, 1n, 'reopening preserves the first command checkpoint')
  t.equal(reopened._monsterPendingCommand?.cmdSeq, 1n, 'the first command remains pending')
  t.equal(receipt(reopened.db).seq, 1n, 'DB2 retains the first apply receipt')
  t.equal(reopened.db.prepare('SELECT count FROM app').get().count, 1n,
    'DB2 contains only the first command effects')
  t.equal(calls, 1, 'reopening does not repeat the completed application')
  reopened.close()
})
