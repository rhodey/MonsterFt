import { DatabaseSync } from 'node:sqlite'
import test from 'tape'
import { ErrorWithCode, MONSTER_CORRUPT } from '../src/error.js'
import { SQLiteLog } from '../src/log.js'
import { digestEntry, initialize, reconcile } from '../src/monstermeta.js'

const fixture = (t) => {
  const log = new SQLiteLog(':memory:')
  log.open()
  const db1 = log.db
  const db2 = new DatabaseSync(':memory:', { readBigInts: true })
  t.teardown(() => {
    db2.close()
    log.close()
  })
  initialize(db1, db2)
  const setup = { term: 1n, entry: Buffer.alloc(0) }
  log.append(setup)
  db2.prepare(`
    UPDATE monsterft_meta_apply
    SET seq = 0, entry_hash = ?, local_digest = zeroblob(32)
  `).run(digestEntry(setup.term, setup.entry))
  reconcile(db1, db2)
  const command = { term: 1n, entry: Buffer.from('command') }
  log.append(command)
  db2.prepare(`
    UPDATE monsterft_meta_apply
    SET seq = 1, entry_hash = ?, local_digest = ?
  `).run(digestEntry(command.term, command.entry), Buffer.alloc(32, 7))
  return { log, db1, db2 }
}

const meta = (db) => db.prepare('SELECT * FROM monsterft_meta').all()
const receipt = (db) => db.prepare('SELECT * FROM monsterft_meta_apply').all()
const corrupt = (message) => (err) =>
  err instanceof ErrorWithCode && err.code === MONSTER_CORRUPT &&
  message.test(err.message)

for (const imported of [false, true]) {
  test(`receipt hash is checked ${imported ? 'behind' : 'ahead of'} the protocol checkpoint`, (t) => {
    const { log, db1, db2 } = fixture(t)
    if (imported) {
      reconcile(db1, db2)
      const sync = { term: 1n, entry: Buffer.from('sync') }
      log.append(sync)
      db1.prepare(`
        UPDATE monsterft_meta
        SET applied_seq = 2, applied_entry_hash = ?, pending_cmd_seq = NULL
      `).run(digestEntry(sync.term, sync.entry))
    }
    db2.exec('UPDATE monsterft_meta_apply SET entry_hash = zeroblob(32)')
    const beforeMeta = meta(db1)
    const beforeReceipt = receipt(db2)
    t.throws(() => reconcile(db1, db2),
      corrupt(/apply entry hash does not match/),
      'a wrong receipt hash is rejected despite a valid DB1 checkpoint and digest')
    t.deepEqual(meta(db1), beforeMeta, 'rejection preserves DB1 metadata')
    t.deepEqual(receipt(db2), beforeReceipt, 'rejection preserves DB2 receipt')
    t.end()
  })
}

test('an ahead receipt requires its referenced log entry', (t) => {
  const { db1, db2 } = fixture(t)
  db1.exec('DELETE FROM raft_log WHERE seq = 1')
  const before = meta(db1)
  t.throws(() => reconcile(db1, db2),
    corrupt(/apply log entry not found/),
    'a one-entry-ahead receipt cannot import missing history')
  t.deepEqual(meta(db1), before, 'missing history does not advance DB1')
  t.equal(receipt(db2)[0].seq, 1n, 'the completed apply receipt remains intact')
  t.end()
})

for (const missing of ['row', 'table']) {
  test(`startup refuses to recreate a missing DB1 metadata ${missing}`, (t) => {
    const { db1, db2 } = fixture(t)
    reconcile(db1, db2)
    const before = receipt(db2)
    db1.exec(missing === 'row'
      ? 'DELETE FROM monsterft_meta'
      : 'DROP TABLE monsterft_meta')
    const expected = corrupt(new RegExp(`meta ${missing} is missing`))
    t.throws(() => initialize(db1, db2), missing === 'row'
      ? expected
      : corrupt(/meta init requires empty raft_log/),
      'startup rejects missing protocol metadata beside completed application state')
    t.throws(() => reconcile(db1, db2), expected,
      'certification reconciliation rejects the same missing protocol metadata')
    if (missing === 'row') {
      t.deepEqual(meta(db1), [], 'the lost protocol row stays absent')
    } else {
      t.equal(db1.prepare(`
        SELECT name FROM sqlite_schema WHERE name = 'monsterft_meta'
      `).get(), undefined, 'the lost protocol table stays absent')
    }
    t.deepEqual(receipt(db2), before, 'completed apply receipt stays unchanged')
    t.end()
  })
}
