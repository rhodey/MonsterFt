import test from 'tape'
import { DatabaseSync } from 'node:sqlite'
import { pack } from 'msgpackr'
import { protocol, append, record, cmd, messages } from './fixtures/protocol.js'
import { readDatabase } from '../src/storage.js'
import { certify } from '../src/certify.js'
import * as sql from '../src/utils.js'
import * as Err from '../src/error.js'

const digest = Buffer.alloc(16, 7)
function command(f) {
  const r = f.rpc(append({ seqP: 0n, termP: 1n, data: [record(1n, cmd())], commitSeq: 1n }))
  f.complete(messages(r, 'execute', 'apply')[0], [[0, 'ok']], digest)
}
function sync(f) {
  f.rpc(append({ seqP: 1n, termP: 1n, commitSeq: 2n, data: [record(1n, pack({
    type: 'sync', cmdSeq: 1n, quorum: true, agree: ['a', 'b'], disagree: [], digest,
  }))] }))
}
function edit(file, run) {
  const db = new DatabaseSync(file, { readBigInts: true })
  try { run(db) } finally { db.close() }
}
const loose = (db, table) => db.exec(`
  ALTER TABLE ${table} RENAME TO saved;
  CREATE TABLE ${table} AS SELECT * FROM saved;
  DROP TABLE saved;
`)

test('Apply receipts require 16-byte entry hashes and local digests', (t) => {
  for (const makeDigest of [() => Buffer.alloc(16), () => new Uint8Array(16)]) {
    const receipt = { seq: 0n, entry_hash: makeDigest(), local_digest: makeDigest() }
    t.equal(sql.validateReceipt(receipt), receipt, '16-byte Buffer and SQLite byte arrays are accepted')
    for (const field of ['entry_hash', 'local_digest']) {
      for (const size of [8, 15, 17, 32]) {
        t.throws(() => sql.validateReceipt({ ...receipt, [field]: Buffer.alloc(size) }),
          (err) => err.code === Err.MONSTER_CORRUPT, `${field}: ${size}-byte hash is rejected`)
      }
    }
  }
  t.end()
})

test('startup and certification reject malformed protocol meta without executing Apply', (t) => {
  const cases = [
    ['repair_state = -1'], ['repair_state = 3'], ['pending_cmd_seq = -1'],
    ['pending_cmd_seq = applied_seq + 1'], ["pending_cmd_seq = 'bad'"],
    ['applied_entry_hash = NULL'], ["applied_entry_hash = 'bad'"],
    ['applied_local_digest = NULL'], ["applied_local_digest = 'bad'"],
    ['applied_seq = -2'], ["applied_seq = 'bad'"], ['applied_seq = -1'],
    ['id = 2'], ['repair_state = 1', true], ['repair_state = 2', true],
    ...[0, 8, 15, 17, 32].flatMap((size) => [
      [`applied_entry_hash = zeroblob(${size})`], [`applied_local_digest = zeroblob(${size})`],
    ]),
  ]
  for (const [mutation, initial] of cases) {
    const f = protocol()
    try {
      if (!initial) f.setup()
      const receipt = sql.readApply(f.db2)
      for (const proc of f.procs) {
        loose(proc.db, 'monsterft_meta')
        proc.db.exec(`UPDATE monsterft_meta SET ${mutation}`)
      }
      f.stop()
      t.throws(() => certify(f.base, true), (err) => err.code === Err.MONSTER_CORRUPT,
        `certify: ${mutation}`)
      t.throws(() => f.reopen(), (err) => err.code === Err.MONSTER_CORRUPT,
        `startup: ${mutation}`)
      t.deepEqual(readDatabase(f.paths.apply, sql.readApply), receipt, 'DB2 unchanged')
    } finally { f.close() }
  }
  t.end()
})

test('damaged Apply meta is never reseeded beside established user state', (t) => {
  for (const damage of ['table', 'row', 'duplicate', 'hash', 'digest', 'seq', 'empty log']) {
    const f = protocol().setup()
    try {
      f.db2.exec('CREATE TABLE user_data (value TEXT); INSERT INTO user_data VALUES (\'kept\')')
      if (damage === 'table') f.db2.exec('DROP TABLE monsterft_meta_apply')
      else if (damage === 'row') f.db2.exec('DELETE FROM monsterft_meta_apply')
      else if (damage === 'empty log') {
        for (const proc of f.procs) proc.db.exec('DELETE FROM raft_log')
      } else {
        loose(f.db2, 'monsterft_meta_apply')
        if (damage === 'duplicate') f.db2.exec('INSERT INTO monsterft_meta_apply SELECT * FROM monsterft_meta_apply')
        if (damage === 'hash') f.db2.exec("UPDATE monsterft_meta_apply SET entry_hash = 'bad'")
        if (damage === 'digest') f.db2.exec('UPDATE monsterft_meta_apply SET local_digest = zeroblob(15)')
        if (damage === 'seq') f.db2.exec('UPDATE monsterft_meta_apply SET seq = -1')
      }
      f.stop()
      t.throws(() => f.reopen(), (err) => err.code === Err.MONSTER_CORRUPT, damage)
      t.equal(readDatabase(f.paths.apply, (db) => db.prepare('SELECT value FROM user_data').get().value), 'kept')
      if (damage === 'row') t.equal(readDatabase(f.paths.apply, (db) =>
        db.prepare('SELECT count(*) AS n FROM monsterft_meta_apply').get().n), 0n)
    } finally { f.close() }
  }
  t.end()
})

test('older retained receipts must match their own entry and pending sequence', (t) => {
  for (const damage of ['hash', 'missing interior', 'pending', 'initial receipt']) {
    const f = protocol().setup()
    try {
      command(f)
      sync(f)
      if (damage === 'hash') f.db2.exec('UPDATE monsterft_meta_apply SET entry_hash = zeroblob(16)')
      if (damage === 'initial receipt') f.db2.exec(`
        UPDATE monsterft_meta_apply SET seq = -1, entry_hash = NULL, local_digest = NULL
      `)
      for (const proc of f.procs) {
        if (damage === 'missing interior') proc.db.exec('DELETE FROM raft_log WHERE seq = 1')
        if (damage === 'pending') proc.db.exec('UPDATE monsterft_meta SET pending_cmd_seq = 2')
      }
      const before = sql.readApply(f.db2)
      f.stop()
      t.throws(() => f.reopen(), (err) => err.code === Err.MONSTER_CORRUPT, damage)
      t.deepEqual(readDatabase(f.paths.apply, sql.readApply), before)
    } finally { f.close() }
  }
  t.end()
})

test('runtime receipt copy failures preserve completed setup and CMD for restart', (t) => {
  for (const setup of [true, false]) {
    for (const zeroRows of [false, true]) {
      const f = protocol()
      try {
        if (!setup) f.setup()
        const seq = setup ? 0n : 1n
        const r = f.rpc(append({
          seqP: seq - 1n, termP: setup ? -1n : 1n,
          data: [record(1n, setup ? '' : cmd())], commitSeq: seq,
        }))
        for (const proc of f.procs) proc.db.exec(`
          CREATE TRIGGER fail_import BEFORE UPDATE ON monsterft_meta
          BEGIN SELECT RAISE(${zeroRows ? 'IGNORE' : "ABORT, 'receipt copy failed'"}); END
        `)
        t.equal(f.complete(messages(r, 'execute', 'apply')[0],
          setup ? [] : [[0, 'ok']], setup ? Buffer.alloc(16) : digest), undefined)
        const err = f.diagnostics.find(({ type }) => type === 'error').error
        t.equal(err.code, Err.SQLITE_ERROR)
        t.match(err.message, /DB1 copy apply/)
        if (!zeroRows) t.ok(Number.isInteger(err.sqlCode), 'native SQLite error retained')
        t.equal(f.state().meta.applied_seq, seq - 1n)
        t.equal(sql.readApply(f.db2).seq, seq, 'completed receipt survives failed import')
        f.stop()
        for (const role of ['procA', 'procB', 'procC']) edit(f.paths[role], (db) => db.exec('DROP TRIGGER fail_import'))
        f.reopen()
        t.equal(f.proc.engine.appliedSeq, seq)
        t.equal(f.proc.engine.pendingApply, null, 'reconciliation does not request execution again')
        t.equal(f.state().meta.pending_cmd_seq, setup ? null : seq)
      } finally { f.close() }
    }
  }
  t.end()
})

test('failed checkpoint and repair updates cannot publish a decision or clear pending state', (t) => {
  for (const stage of ['noop', 'sync', 'fence']) {
    for (const zeroRows of [false, true]) {
      const f = protocol().setup()
      try {
        if (stage !== 'noop') command(f)
        const before = f.state().meta
        for (const proc of f.procs) proc.db.exec(`
          CREATE TRIGGER fail_meta BEFORE UPDATE ON monsterft_meta
          BEGIN SELECT RAISE(${zeroRows ? 'IGNORE' : "ABORT, 'meta failed'"}); END
        `)
        const data = stage === 'noop' ? record() : record(1n, pack({
          type: 'sync', cmdSeq: 1n, quorum: true, digest,
          agree: stage === 'fence' ? ['b', 'c'] : ['a', 'b'],
          disagree: stage === 'fence' ? ['a'] : [],
        }))
        const seq = stage === 'noop' ? 1n : 2n
        const result = f.rpc(append({ seqP: seq - 1n, termP: 1n, data: [data], commitSeq: seq }))
        t.equal(result, undefined, `${stage}: failed input has no output`)
        t.deepEqual(f.state().meta, before, 'checkpoint, pending state and fence are atomic')
        t.equal(f.diagnostics.find(({ type }) => type === 'error').error.code, Err.SQLITE_ERROR)
      } finally { f.close() }
    }
  }
  t.end()
})

test('certification preserves unapplied suffixes and rejects a failed fence update', (t) => {
  const f = protocol().setup()
  try {
    f.rpc(append({ seqP: 0n, termP: 1n, data: [record(1n, cmd())] }))
    const before = f.rows()
    f.stop()
    t.equal(certify(f.base), 0n)
    t.deepEqual(f.rows(), before, 'unapplied CMD remains byte-for-byte intact')
    for (const role of ['procA', 'procB', 'procC']) edit(f.paths[role], (db) => db.exec(`
      UPDATE monsterft_meta SET repair_state = 1;
      CREATE TRIGGER fail_cert BEFORE UPDATE OF repair_state ON monsterft_meta
      BEGIN SELECT RAISE(ABORT, 'certification update failed'); END
    `))
    t.throws(() => certify(f.base), (err) => err.code === Err.SQLITE_ERROR && Number.isInteger(err.sqlCode))
    for (const role of ['procA', 'procB', 'procC']) {
      t.equal(readDatabase(f.paths[role], sql.readMeta).repair_state, 1n)
      edit(f.paths[role], (db) => db.exec('DROP TRIGGER fail_cert'))
    }
    t.equal(certify(f.base), 0n)
    f.reopen()
    const r = f.rpc(append({ seqP: 1n, termP: 1n, commitSeq: 1n }))
    t.equal(messages(r, 'execute', 'apply').length, 1, 'ordinary recovery requests the unapplied command once')
    f.complete(messages(r, 'execute', 'apply')[0], [[0, 'ok']], digest)
    t.equal(messages(f.rpc(append({ seqP: 1n, termP: 1n, commitSeq: 1n })), 'execute', 'apply').length, 0)
  } finally { f.close() }
  t.end()
})
