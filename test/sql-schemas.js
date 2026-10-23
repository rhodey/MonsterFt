import test from 'tape'
import { DatabaseSync } from 'node:sqlite'
import * as util from '../src/util.js'
import * as sql from '../src/utils.js'
import * as Err from '../src/error.js'
import { digestEntry } from '../src/utilm.js'
import { readState, stateDigest, copyProtocolState } from '../src/storage.js'

const max = 9_223_372_036_854_775_807n
const seed = Buffer.alloc(16, 1)
const isCorrupt = (code) => (err) => err instanceof Err.ErrorWithCode && err.code === code

function database() {
  const db = new DatabaseSync(':memory:', { readBigInts: true })
  db.exec(`
    CREATE TABLE raft_log (seq INTEGER PRIMARY KEY, term, entry);
    CREATE TABLE raft_election (id INTEGER PRIMARY KEY, current_term, voted_for);
    INSERT INTO raft_election VALUES (1, 0, NULL);
  `)
  sql.createMeta(db, seed)
  return db
}

test('stored log schemas retain original objects, extra fields, and exact byte views', (t) => {
  const backing = Uint8Array.from([8, 9, 10, 11, 12])
  const entry = backing.subarray(1, 4)
  const row = { seq: max, term: max, entry, extra: { retained: true } }
  t.equal(util.validate(util.logSchemas.row, row, 'log', Err.LOG_CORRUPT), row)
  t.equal(row.entry, entry, 'validation preserves the exact SQLite byte view')
  t.equal(row.entry.buffer, backing.buffer, 'validation does not copy payloads')
  t.equal(row.entry.byteOffset, backing.byteOffset + 1, 'view offset is unchanged')
  t.deepEqual([...row.entry], [9, 10, 11], 'unrelated bytes are excluded')
  t.deepEqual(row.extra, { retained: true }, 'extra fields remain intact')
  for (const empty of [Buffer.alloc(0), new Uint8Array(0)]) {
    const record = { term: 0n, entry: empty }
    t.equal(util.validate(util.logSchemas.record, record, 'log', Err.LOG_CORRUPT), record,
      'empty payload remains valid')
  }
  for (const fields of [
    { seq: -1n }, { seq: max + 1n }, { seq: 0 },
    { term: -1n }, { term: max + 1n }, { term: 0 },
    { entry: null }, { entry: 'payload' },
  ]) {
    t.throws(() => util.validate(util.logSchemas.row, { ...row, ...fields }, 'log', Err.LOG_CORRUPT),
      isCorrupt(Err.LOG_CORRUPT), `${Object.keys(fields)[0]} is rejected as log corruption`)
  }
  const election = { term: 0n, votedFor: 'not-in-current-membership', id: 1n }
  t.equal(util.validate(util.logSchemas.election, election, 'election', Err.LOG_CORRUPT), election,
    'nonempty saved votes are not checked against membership')
  t.end()
})

test('Apply receipt variants preserve original hashes and reject invalid initial states', (t) => {
  const initial = { seq: -1n, entry_hash: null, local_digest: null, extra: 'kept' }
  t.equal(sql.validateReceipt(initial), initial, 'initial receipt is returned unchanged')
  const bytes = new Uint8Array(20).fill(7)
  const receipt = { seq: max, entry_hash: bytes.subarray(1, 17), local_digest: Buffer.alloc(16) }
  t.equal(sql.validateReceipt(receipt), receipt, 'completed receipt is returned unchanged')
  t.equal(receipt.entry_hash.buffer, bytes.buffer, 'hash view is not copied')
  t.equal(receipt.entry_hash.byteOffset, bytes.byteOffset + 1, 'hash offset is preserved')
  for (const invalid of [
    null,
    { ...initial, seq: -2n },
    { ...initial, seq: -1 },
    { ...initial, entry_hash: Buffer.alloc(16) },
    { ...initial, local_digest: undefined },
    { ...receipt, seq: max + 1n },
    { ...receipt, entry_hash: null },
    { ...receipt, local_digest: undefined },
  ]) {
    t.throws(() => sql.validateReceipt(invalid), isCorrupt(Err.MONSTER_CORRUPT),
      'invalid receipt remains Monster corruption')
  }
  t.end()
})

test('startup rows and checkpoint references retain their corruption classifications', (t) => {
  const db = database()
  try {
    const entry = Buffer.from('application payload')
    const hash = digestEntry(7n, entry)
    db.prepare('INSERT INTO raft_log VALUES (0, 7, ?)').run(entry)
    db.exec("UPDATE raft_election SET current_term = 0, voted_for = 'former-member'")
    t.deepEqual(readState(db).election, { term: 0n, votedFor: 'former-member' },
      'startup permits a lower election term and an unknown saved voter')
    for (const mutation of ["current_term = -1", "voted_for = ''"]) {
      db.exec(`UPDATE raft_election SET ${mutation}`)
      t.throws(() => readState(db), isCorrupt(Err.LOG_CORRUPT), mutation)
      db.exec('UPDATE raft_election SET current_term = 0, voted_for = NULL')
    }
    for (const mutation of ['term = -1', "entry = 'not a blob'"]) {
      db.exec(`UPDATE raft_log SET ${mutation}`)
      t.throws(() => readState(db), isCorrupt(Err.LOG_CORRUPT), 'startup rejects the damaged head')
      t.throws(() => sql.verifyEntry(db, 0n, hash), isCorrupt(Err.LOG_CORRUPT),
        'checkpoint verification classifies the damaged row as log corruption')
      db.prepare('UPDATE raft_log SET term = 7, entry = ?').run(entry)
    }
    db.exec('UPDATE raft_log SET term = 8')
    t.throws(() => sql.verifyEntry(db, 0n, hash), isCorrupt(Err.MONSTER_CORRUPT),
      'valid row with a different checkpoint hash remains Monster corruption')
  } finally { db.close() }
  t.end()
})

test('donor copying validates interior rows while preserving the destination on rollback', (t) => {
  for (const mutation of ['term = -1', "entry = 'not a blob'"]) {
    const donor = database()
    const target = database()
    try {
      const insert = donor.prepare('INSERT INTO raft_log VALUES (?, 1, ?)')
      for (let seq = 0n; seq < 3n; seq++) { insert.run(seq, Buffer.from(`entry ${seq}`)) }
      target.prepare('INSERT INTO raft_log VALUES (0, 2, ?)').run(Buffer.from('original'))
      const before = stateDigest(readState(target))
      const selected = stateDigest(readState(donor))
      donor.exec(`UPDATE raft_log SET ${mutation} WHERE seq = 1`)
      t.deepEqual(stateDigest(readState(donor)), selected,
        'head-only capture remains valid when an interior row is damaged')
      target.exec('BEGIN IMMEDIATE')
      t.throws(() => copyProtocolState(target, donor, selected), isCorrupt(Err.LOG_CORRUPT),
        'donor iteration rejects the damaged interior row')
      target.exec('ROLLBACK')
      t.deepEqual(stateDigest(readState(target)), before, 'rollback preserves original target state')
    } finally { donor.close(); target.close() }
  }
  t.end()
})
