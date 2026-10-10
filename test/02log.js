import fs from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import test from 'tape'
import {
  ErrorWithCode,
  FS_ERROR,
  ARGUMENT_ILLEGAL,
  LOG_CORRUPT,
  LOG_NOT_OPEN,
  LOG_OPEN,
  SQLITE_ERROR,
} from '../src/error.js'
import { SQLiteLog } from '../src/index.js'
import { logFixture, toBuf, toEntry } from './util.js'

const MAX_SEQ = 9_223_372_036_854_775_807n
const sqliteFiles = (file) => [
  file,
  `${file}-journal`,
  `${file}-wal`,
  `${file}-shm`,
]

test('sqlite log lifecycle', (t) => {
  const { create } = logFixture(t, '02-lifecycle')
  const log = create()

  t.notOk(log.isOpen, 'closed initially')
  t.equal(log.begin, null, 'begin is null while closed')
  t.equal(log.seq, null, 'seq is null while closed')
  t.equal(log.head, null, 'head is null while closed')
  try {
    log.append(Buffer.alloc(0))
    t.fail('append should throw while closed')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'closed append uses ErrorWithCode')
    t.equal(err.message, '(log append) log not open',
      'closed append includes operation context')
    t.equal(err.code, LOG_NOT_OPEN, 'closed append has LOG_NOT_OPEN code')
    t.equal(err.sqlCode, null, 'closed append has no SQLite code')
  }
  t.throws(() => log.appendBatch([toEntry('data')]),
    /\(log appendBatch\) log not open$/,
    'appendBatch adds operation context while closed')
  t.throws(() => log.election(0n, null),
    /\(log election\) log not open$/,
    'election adds operation context while closed')
  t.throws(() => log.trim(), /\(log trim\) log not open$/,
    'trim adds operation context while closed')
  try {
    log.iter()
    t.fail('iter should throw while closed')
  } catch (err) {
    t.equal(err.message, '(log iter) log not open',
      'iter adds operation context while closed')
    t.equal(err.code, LOG_NOT_OPEN, 'closed iter has LOG_NOT_OPEN code')
    t.equal(err.sqlCode, null, 'closed iter has no SQLite code')
  }

  log.del()
  log.open()
  t.ok(log.isOpen, 'open')
  t.equal(log.begin, -1n, 'empty begin')
  t.equal(log.seq, -1n, 'empty seq')
  t.equal(log.head, null, 'empty head')
  t.equal(
    String(log.db.prepare('PRAGMA journal_mode').get().journal_mode).toLowerCase(),
    'wal',
    'persistent logs use WAL journaling',
  )
  log.open()
  try {
    log.del()
    t.fail('delete should throw while open')
  } catch (err) {
    t.equal(err.message, '(log del) log is open',
      'open delete includes operation context')
    t.equal(err.code, LOG_OPEN, 'open delete has LOG_OPEN code')
    t.equal(err.sqlCode, null, 'open delete has no SQLite code')
  }

  log.close()
  t.notOk(log.isOpen, 'closed')
  t.equal(log.begin, null, 'begin resets on close')
  t.equal(log.seq, null, 'seq resets on close')
  log.close()
  t.end()
})

test('filesystem failures use FS_ERROR', (t) => {
  const { create, file } = logFixture(t, '02-delete-error')
  const log = create()
  log.del()
  log.open()
  log.append(toEntry('persisted'))
  log.close()

  const rmSync = fs.rmSync
  const deleteError = new Error('remove failed')
  const attempted = []
  fs.rmSync = (target) => {
    attempted.push(target)
    if (target === file) { throw deleteError }
  }

  try {
    log.del()
    t.fail('delete should throw')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'delete uses ErrorWithCode')
    t.equal(err.code, FS_ERROR, 'delete has FS_ERROR code')
    t.equal(err.sqlCode, null, 'delete has no SQLite code')
    t.equal(err.stack, deleteError.stack, 'delete preserves the original filesystem stack')
    t.match(err.message, /\(log del\) remove failed/,
      'delete adds operation context')
  } finally {
    fs.rmSync = rmSync
  }

  t.deepEqual(attempted, [file], 'delete stops at the first failed file')
  log.open()
  t.equal(log.seq, 0n, 'failed delete preserves persisted entries')
  log.close()
  log.del()
  log.open()
  t.equal(log.seq, -1n, 'delete can be retried after failure')
  log.close()

  const mkdirSync = fs.mkdirSync
  const mkdirError = new Error('mkdir failed')
  fs.mkdirSync = () => { throw mkdirError }
  try {
    log.open()
    t.fail('directory creation should throw')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'directory creation uses ErrorWithCode')
    t.equal(err.code, FS_ERROR, 'directory creation has FS_ERROR code')
    t.equal(err.sqlCode, null, 'directory creation has no SQLite code')
    t.equal(err.stack, mkdirError.stack, 'directory creation preserves the original filesystem stack')
    t.match(err.message, /\(log open\) mkdir failed/,
      'directory creation adds open context')
  } finally {
    fs.mkdirSync = mkdirSync
  }
  t.end()
})

test('election term and vote state persist', (t) => {
  const { create } = logFixture(t, '02-election-state')
  let log = create()
  log.del()
  log.open()

  t.equal(log.term, -1n, 'empty log has no head term')
  t.equal(log.elec.term, 0n, 'starts at term zero')
  t.equal(log.elec.votedFor, null, 'starts without a vote')
  t.deepEqual(log.election(0n, null), [0n, null], 'accepts term zero')
  t.deepEqual(log.election(3n, 'node-a'), [3n, 'node-a'],
    'writes term and candidate together')
  t.equal(log.elec.term, 3n, 'caches the updated term')
  t.equal(log.elec.votedFor, 'node-a', 'caches the updated candidate')
  log.append(toEntry(Buffer.alloc(0), 2n))
  t.equal(log.term, 2n, 'head term can differ from election term')

  log.close()
  log = create()
  log.open()
  t.equal(log.term, 2n, 'restores the head term in a new instance')
  t.equal(log.elec.term, 3n, 'restores election term in a new instance')
  t.equal(log.elec.votedFor, 'node-a', 'restores candidate in a new instance')
  t.deepEqual(log.election(4n, null), [4n, null], 'new term clears the vote')
  t.deepEqual(log.elec, { term: 4n, votedFor: null }, 'cache stores the cleared vote')
  log.close()
  log = create()
  log.open()
  t.deepEqual(log.elec, { term: 4n, votedFor: null },
    'restores the cleared vote in a new instance')
  const throwsInvalidArgument = (run, message, label) => {
    try {
      run()
      t.fail(`${label} should throw`)
    } catch (err) {
      t.match(err.message, message, `${label} explains the invalid argument`)
      t.equal(err.code, ARGUMENT_ILLEGAL, `${label} has ARGUMENT_ILLEGAL code`)
      t.equal(err.sqlCode, null, `${label} has no SQLite code`)
    }
  }
  throwsInvalidArgument(
    () => log.election(-1n, null), /term must be >= 0/, 'negative term')
  throwsInvalidArgument(
    () => log.election(1, null), /term must be bigint/,
    'non-bigint term')
  throwsInvalidArgument(
    () => log.election(MAX_SEQ + 1n, null),
    new RegExp(`term must be <= ${MAX_SEQ}`),
    'term above maximum')
  throwsInvalidArgument(
    () => log.election(4n, ''), /votedFor must be null or a non-empty string/,
    'empty candidate')
  throwsInvalidArgument(
    () => log.election(4n, 1), /votedFor must be null or a non-empty string/,
    'non-string candidate')
  t.end()
})

test('fresh election state is durable before any vote or append', (t) => {
  const { create, file } = logFixture(t, '02-initial-election')
  let log = create()
  log.del()
  log.open()
  log.close()

  const db = new DatabaseSync(file, { readBigInts: true })
  try {
    const row = db.prepare('SELECT current_term, voted_for FROM raft_election WHERE id = 1').get()
    t.equal(row?.current_term, 0n, 'term zero is persisted at initialization')
    t.equal(row?.voted_for, null, 'the initial null vote is persisted')
  } finally {
    db.close()
  }

  log = create()
  log.open()
  t.deepEqual(log.elec, { term: 0n, votedFor: null }, 'restores fresh election state')
  log.election(MAX_SEQ, 'node-a')
  log.close()
  log.open()
  t.deepEqual(log.elec, { term: MAX_SEQ, votedFor: 'node-a' },
    'restores the maximum term and a nonempty vote')
  t.end()
})

test('stored election fields are validated on open', (t) => {
  const cases = [
    ['negative-term', -2n, null, /term must be >= 0/],
    ['null-term', null, null, /term must be bigint/],
    ['text-term', '4', null, /term must be bigint/],
    ['real-term', 1.5, null, /term must be bigint/],
    ['oversized-term', Number(MAX_SEQ) * 2, null, /term must be bigint/],
    ['empty-vote', 4n, '', /votedFor must be null or a non-empty string/],
    ['integer-vote', 4n, 2n, /votedFor must be null or a non-empty string/],
    ['blob-vote', 4n, Buffer.from('2'), /votedFor must be null or a non-empty string/],
  ]
  for (const [name, term, votedFor, message] of cases) {
    const { create, file } = logFixture(t, `02-election-${name}`)
    const seed = create()
    seed.del()
    seed.open()
    // Remove affinities and constraints to exercise restoration checks.
    seed.db.exec(`
      DROP TABLE raft_election;
      CREATE TABLE raft_election (id INTEGER PRIMARY KEY, current_term, voted_for);
    `)
    seed.db.prepare('INSERT INTO raft_election VALUES (1, ?, ?)').run(term, votedFor)
    seed.close()

    const log = create()
    t.throws(() => log.open(),
      (err) => err.code === LOG_CORRUPT && message.test(err.message),
      `${name}: reports corrupt election state`)
    t.notOk(log.isOpen, `${name}: remains closed`)
    t.equal(log.db, null, `${name}: releases the database handle`)
    t.deepEqual(log.elec, { term: null, votedFor: null }, `${name}: clears cached election state`)

    const db = new DatabaseSync(file)
    try {
      db.prepare('UPDATE raft_election SET current_term = 4, voted_for = NULL').run()
    } finally {
      db.close()
    }
    log.open()
    t.deepEqual(log.elec, { term: 4n, votedFor: null }, `${name}: can reopen after repair`)
  }
  t.end()
})

test('existing databases must retain both log tables and their election row', (t) => {
  const cases = [
    ['election-row', 'DELETE FROM raft_election', 'election row is missing'],
    ['election-table', 'DROP TABLE raft_election', 'raft_election table is missing'],
    ['log-table', 'DROP TABLE raft_log', 'raft_log table is missing'],
    ['unrelated-schema', `
      DROP TABLE raft_election;
      DROP TABLE raft_log;
      CREATE VIEW other_data AS SELECT 1;
    `, 'raft_log table is missing'],
  ]
  for (const [name, sql, message] of cases) {
    const { create, file } = logFixture(t, `02-missing-${name}`)
    const seed = create()
    seed.del()
    seed.open()
    seed.append(toEntry('saved history', 4n))
    seed.election(4n, 'node-a')
    seed.db.exec(sql)
    const schemaBefore = seed.db.prepare('SELECT name, type FROM sqlite_schema ORDER BY name').all()
    seed.close()

    const log = create()
    t.throws(() => log.open(),
      (err) => err.code === LOG_CORRUPT && err.message === `(log open) ${message}`,
      `${name}: reports missing persisted state`)
    t.notOk(log.isOpen, `${name}: remains closed`)
    const db = new DatabaseSync(file, { readBigInts: true })
    try {
      t.deepEqual(db.prepare('SELECT name, type FROM sqlite_schema ORDER BY name').all(),
        schemaBefore, `${name}: does not recreate missing tables`)
      if (name === 'election-row') {
        t.equal(db.prepare('SELECT * FROM raft_election').get(), undefined,
          'does not replace a missing election row')
      }
      if (name === 'log-table') {
        const row = db.prepare('SELECT current_term, voted_for FROM raft_election').get()
        t.equal(row.current_term, 4n, 'preserves the remaining election term')
        t.equal(row.voted_for, 'node-a', 'preserves the remaining vote')
      }
    } finally {
      db.close()
    }
  }
  t.end()
})

test('failed log initialization rolls back tables and election state', (t) => {
  const { create, file } = logFixture(t, '02-init-rollback')
  const log = create()
  log.del()
  const exec = DatabaseSync.prototype.exec
  let injected = false
  DatabaseSync.prototype.exec = function(sql) {
    if (sql.includes('CREATE TABLE raft_election')) {
      injected = true
      sql = sql.replace('COMMIT;', 'SELECT * FROM missing_init_table; COMMIT;')
    }
    return exec.call(this, sql)
  }
  try {
    t.throws(() => log.open(), (err) => err.code === SQLITE_ERROR,
      'failure after inserting initial election state prevents open')
  } finally {
    DatabaseSync.prototype.exec = exec
  }
  t.ok(injected, 'failure was injected in fresh initialization')
  t.notOk(log.isOpen, 'failed initialization leaves the log closed')
  t.equal(log.db, null, 'failed initialization releases the database')
  const db = new DatabaseSync(file)
  try {
    t.deepEqual(db.prepare('SELECT name FROM sqlite_schema').all(), [],
      'closing the failed transaction rolls back all schema changes')
  } finally {
    db.close()
  }
  log.open()
  t.deepEqual(log.elec, { term: 0n, votedFor: null }, 'retry initializes the election row')
  t.equal(log.append(toEntry('after retry')), 0n, 'retry creates a usable log')
  t.end()
})

test('append records persist terms and exact payloads across reopen', (t) => {
  const { create } = logFixture(t, '02-append')
  let log = create()
  log.del()
  log.open()

  const entries = [Buffer.alloc(0), Buffer.from([0, 255]), toBuf({ value: 3 })]
  for (let i = 0; i < entries.length; i++) {
    const seq = log.append(toEntry(entries[i], BigInt(i)))
    t.equal(seq, BigInt(i), `append returns ${i}`)
    t.equal(log.begin, 0n, `begin remains at zero after append ${i}`)
    t.equal(log.seq, BigInt(i), `head seq ${i}`)
    t.equal(log.term, BigInt(i), `head term ${i}`)
    t.ok(log.head.equals(entries[i]), `head buffer ${i}`)
    const row = log.db.prepare('SELECT term, entry FROM raft_log WHERE seq = ?').get(seq)
    t.equal(row.term, BigInt(i), `SQL term ${i} is stored separately`)
    t.deepEqual(Buffer.from(row.entry), entries[i], `SQL payload ${i} has no prefix`)
  }

  t.equal(log.append(toEntry('stable', 3n)), 3n, 'append returns 3')

  log.close()
  log.open()
  t.equal(log.begin, 0n, 'same instance restores begin')
  t.equal(log.seq, 3n, 'same instance restores seq')
  t.equal(log.head.toString(), 'stable', 'same instance restores head')

  log.close()
  log = create()
  log.open()
  t.equal(log.begin, 0n, 'new instance restores begin')
  t.equal(log.seq, 3n, 'new instance restores seq')
  t.equal(log.head.toString(), 'stable', 'new instance restores head')
  t.equal(log.append(toEntry('next', 4n)), 4n, 'append continues sequence')
  t.end()
})

test('append batch updates first seq and head', (t) => {
  const { create } = logFixture(t, '02-batch')
  const log = create()
  log.del()
  log.open()

  log.append(toEntry('seed'))
  const entries = [Buffer.alloc(0), Buffer.from('a'), Buffer.from('longer')]
  const first = log.appendBatch(entries.map((entry) => toEntry(entry, 1n)))
  t.equal(first, 1n, 'returns first batch seq')
  t.equal(log.seq, 3n, 'seq is final batch seq')
  t.equal(log.term, 1n, 'term is final batch term')
  t.ok(log.head.equals(entries[2]), 'head is final batch buffer')

  log.close()
  log.open()
  t.equal(log.seq, 3n, 'batch seq persists')
  t.ok(log.head.equals(entries[2]), 'batch head persists')
  t.end()
})

test('empty payloads can be appended after reading and from zero-byte views', (t) => {
  const source = new SQLiteLog(':memory:')
  const target = new SQLiteLog(':memory:')
  t.teardown(() => { source.close(); target.close() })
  source.open()
  target.open()
  source.append(toEntry(Buffer.alloc(0), 2n))
  const readback = source.iter().next().value
  const view = toEntry(Buffer.from(new ArrayBuffer(0)), 3n)

  for (const record of [readback, view]) {
    target.append(record)
    t.equal(target.head, record.entry, 'append keeps ownership of the original empty buffer')
    target.appendBatch([record])
    t.equal(target.head, record.entry, 'appendBatch keeps ownership of the original empty buffer')
  }
  const rows = target.db.prepare('SELECT term, typeof(entry) AS type, length(entry) AS size FROM raft_log').all()
  t.deepEqual(rows.map((row) => row.term), [2n, 2n, 3n, 3n], 'both append paths preserve terms')
  t.ok(rows.every((row) => row.type === 'blob' && row.size === 0n),
    'all empty payloads bind as zero-length BLOBs rather than NULL')
  t.deepEqual([...target.iter()], [readback, readback, view, view],
    'readback records and external zero-byte views remain readable')
  t.end()
})

test('trim and append after trim', (t) => {
  const { create } = logFixture(t, '02-trim')
  const log = create()
  log.del()
  log.open()

  const entries = ['zero', 'one', 'two', 'three'].map(Buffer.from)
  log.appendBatch(entries.map((entry) => toEntry(entry, 1n)))

  log.trim(1n)
  t.equal(log.seq, 1n, 'trim updates seq')
  t.ok(log.head.equals(entries[1]), 'trim updates head')
  log.trim(9n)
  t.equal(log.seq, 1n, 'larger trim is a no-op')
  t.equal(log.append(toEntry('replacement', 2n)), 2n, 'append resumes after trim')

  log.trim(-1n)
  t.equal(log.begin, -1n, 'trim all resets begin')
  t.equal(log.seq, -1n, 'trim all resets seq')
  t.equal(log.term, -1n, 'trim all resets head term')
  t.equal(log.head, null, 'trim all resets head')
  log.trim(-1n)
  t.pass('trim empty log is a no-op')
  t.equal(log.append(toEntry(Buffer.alloc(0))), 0n, 'append restarts at zero')
  t.equal(log.begin, 0n, 'append restores begin')

  log.close()
  log.open()
  t.equal(log.begin, 0n, 'trimmed begin persists')
  t.equal(log.seq, 0n, 'trimmed state persists')
  t.equal(log.head.byteLength, 0, 'empty head persists')
  t.end()
})

test('append and trim validate arguments', (t) => {
  const { create } = logFixture(t, '02-validation')
  const log = create()
  log.del()
  log.open()

  const throwsInvalidArgument = (run, message, label) => {
    try {
      run()
      t.fail(`${label} should throw`)
    } catch (err) {
      t.match(err.message, message, `${label} explains the invalid argument`)
      t.equal(err.code, ARGUMENT_ILLEGAL, `${label} has ARGUMENT_ILLEGAL code`)
      t.equal(err.sqlCode, null, `${label} has no SQLite code`)
    }
  }

  t.throws(() => log.append('nope'), /data must be record/, 'append requires a record')
  t.throws(() => log.append({ term: 0n, entry: 'nope' }), /entry must be buffer/,
    'append requires a buffer payload')
  t.throws(() => log.append(Buffer.alloc(0), -1n), /seq must be >= 0/,
    'append rejects negative seq')
  throwsInvalidArgument(
    () => log.append(toEntry(Buffer.alloc(0)), 1n),
    /next 0 !== 1/, 'append sequence gap')
  t.throws(() => log.append(Buffer.alloc(0), MAX_SEQ + 1n), /seq must be <=/,
    'append rejects overflow')
  throwsInvalidArgument(
    () => log.appendBatch('nope'), /data must be array/, 'batch non-array data')
  throwsInvalidArgument(
    () => log.appendBatch([]), /length > 0/, 'batch empty data')
  t.throws(() => log.appendBatch([toEntry(Buffer.alloc(0)), 'nope']), /data must be record/,
    'batch requires records')
  t.throws(() => log.appendBatch([{ entry: Buffer.alloc(0) }]), /term must be bigint/,
    'batch requires explicit terms')
  throwsInvalidArgument(
    () => log.appendBatch([toEntry(Buffer.alloc(0))], 1n),
    /next 0 !== 1/, 'batch sequence gap')
  t.throws(() => log.trim(-2n), /seq must be >= -1/, 'trim rejects seq below sentinel')
  t.throws(() => log.trim(0), /seq must be bigint/, 'trim requires bigint')
  try {
    log.trim(-2n)
    t.fail('invalid trim sequence should throw')
  } catch (err) {
    t.equal(err.code, ARGUMENT_ILLEGAL, 'caller sequence has ARGUMENT_ILLEGAL code')
    t.equal(err.sqlCode, null, 'caller sequence has no SQLite code')
  }
  t.end()
})

test('entry terms stay within the supported range', (t) => {
  const { create } = logFixture(t, '02-entry-terms')
  const log = create()
  log.del()
  log.open()

  const entries = [toEntry('zero', 0n), toEntry('maximum', MAX_SEQ)]
  t.equal(log.append(entries[0]), 0n, 'accepts term zero')
  t.equal(log.appendBatch([entries[1]]), 1n, 'accepts the maximum term')

  for (const term of [MAX_SEQ + 1n, (1n << 64n) - 1n]) {
    const invalid = toEntry('invalid', term)
    const rejected = (err) => err.code === ARGUMENT_ILLEGAL &&
      err.message.includes(`term must be <= ${MAX_SEQ}`)
    t.throws(() => log.append(invalid), rejected, `append rejects term ${term}`)
    t.throws(() => log.appendBatch([toEntry('valid', MAX_SEQ), invalid]),
      rejected, `batch rejects term ${term}`)
  }
  t.equal(log.seq, 1n, 'rejected entries preserve the cached head sequence')
  t.deepEqual([...log.iter()], entries, 'rejected entries leave stored history unchanged')

  log.close()
  log.open()
  t.equal(log.term, MAX_SEQ, 'the maximum term survives reopening')
  t.deepEqual([...log.iter()], entries, 'boundary terms can be read from storage')
  t.end()
})

test('append prevalidates every record before writing', (t) => {
  const { create } = logFixture(t, '02-record-validation')
  const log = create()
  log.del()
  log.open()
  const seed = toEntry('seed', 2n)
  log.append(seed)

  const cases = [
    ['null record', null],
    ['array record', []],
    ['old buffer format', Buffer.alloc(8)],
    ['missing term', { entry: Buffer.alloc(0) }],
    ['number term', toEntry('invalid', 1)],
    ['negative term', toEntry('invalid', -1n)],
    ['overflow term', toEntry('invalid', MAX_SEQ + 1n)],
    ['missing payload', { term: 0n }],
    ['null payload', { term: 0n, entry: null }],
    ['typed array payload', { term: 0n, entry: new Uint8Array(1) }],
  ]
  const exec = log.db.exec.bind(log.db)
  const insert = log._statements.insert.run.bind(log._statements.insert)
  let writes = 0
  log.db.exec = (...args) => { writes++; return exec(...args) }
  log._statements.insert.run = (...args) => { writes++; return insert(...args) }
  try {
    for (const [name, record] of cases) {
      t.throws(() => log.append(record), (err) => err.code === ARGUMENT_ILLEGAL,
        `append rejects ${name}`)
      t.throws(() => log.appendBatch([toEntry('valid', 3n), record]),
        (err) => err.code === ARGUMENT_ILLEGAL, `batch rejects ${name}`)
    }
  } finally {
    delete log.db.exec
    delete log._statements.insert.run
  }
  t.equal(writes, 0, 'invalid records reach neither transaction setup nor inserts')
  t.equal(log.seq, 0n, 'invalid records leave the cached sequence unchanged')
  t.deepEqual([...log.iter()], [seed], 'invalid records leave stored history unchanged')
  t.end()
})

test('stored invalid entry terms and payloads report log corruption', (t) => {
  const cases = [
    ['negative-term', -1n, Buffer.alloc(0), /term must be >= 0/],
    ['null-term', null, Buffer.alloc(0), /term must be bigint/],
    ['text-term', '1', Buffer.alloc(0), /term must be bigint/],
    ['real-term', 1.5, Buffer.alloc(0), /term must be bigint/],
    ['oversized-term', Number(MAX_SEQ) * 2, Buffer.alloc(0), /term must be bigint/],
    ['null-payload', 1n, null, /entry must be blob/],
    ['text-payload', 1n, 'payload', /entry must be blob/],
    ['integer-payload', 1n, 2n, /entry must be blob/],
  ]
  for (const [name, term, entry, message] of cases) {
    const { create } = logFixture(t, `02-corrupt-entry-${name}`)
    const log = create()
    log.del()
    log.open()
    // Remove affinities and constraints to exercise restoration checks.
    log.db.exec(`
      DROP TABLE raft_log;
      CREATE TABLE raft_log (seq INTEGER PRIMARY KEY, term, entry);
    `)
    const insert = log.db.prepare('INSERT INTO raft_log (seq, term, entry) VALUES (?, ?, ?)')
    insert.run(0n, term, entry)
    insert.run(1n, 1n, Buffer.from('valid'))
    log.close()
    log.open()

    const corrupt = (err) => err.code === LOG_CORRUPT && message.test(err.message)
    t.throws(() => [...log.iter()], corrupt, `${name}: iteration rejects an invalid interior row`)

    log.db.prepare('UPDATE raft_log SET term = ?, entry = ? WHERE seq = 1').run(term, entry)
    log.close()
    t.throws(() => log.open(), corrupt, `${name}: open rejects an invalid head row`)
    t.notOk(log.isOpen, `${name}: failed open leaves the log closed`)
    t.equal(log.db, null, `${name}: failed open releases the database`)
  }
  t.end()
})

test('old log schemas fail without migration or data loss', (t) => {
  const { create, file } = logFixture(t, '02-old-schema')
  const log = create()
  log.del()
  log.open()
  log.election(4n, 'node-a')
  log.db.exec(`
    DROP TABLE raft_log;
    CREATE TABLE raft_log (seq INTEGER PRIMARY KEY, entry BLOB NOT NULL) STRICT;
  `)
  const prefix = Buffer.alloc(8)
  prefix.writeBigUInt64LE(4n)
  const entry = Buffer.concat([prefix, Buffer.from('saved history')])
  log.db.prepare('INSERT INTO raft_log (seq, entry) VALUES (?, ?)').run(0n, entry)
  log.close()

  t.throws(() => log.open(), (err) => err.code === SQLITE_ERROR && /term/.test(err.message),
    'statement preparation rejects the old schema')
  t.notOk(log.isOpen, 'failed open remains closed')
  t.equal(log.db, null, 'failed open releases the database')
  const db = new DatabaseSync(file, { readBigInts: true })
  try {
    t.deepEqual(db.prepare('PRAGMA table_info(raft_log)').all().map((row) => row.name),
      ['seq', 'entry'], 'no column or table migration occurs')
    t.deepEqual(Buffer.from(db.prepare('SELECT entry FROM raft_log WHERE seq = 0').get().entry),
      entry, 'old payload bytes remain untouched')
    const election = db.prepare('SELECT current_term, voted_for FROM raft_election').get()
    t.equal(election.current_term, 4n, 'old election term remains untouched')
    t.equal(election.voted_for, 'node-a', 'old vote remains untouched')
  } finally {
    db.close()
  }
  t.end()
})

test('stored invalid sequences report log corruption', (t) => {
  const { create } = logFixture(t, '02-corrupt-seq')
  const seed = create()
  seed.del()
  seed.open()
  seed.db.prepare(
    'INSERT INTO raft_log (seq, term, entry) VALUES (?, ?, ?)').run(-2n, 0n, Buffer.from('invalid'))
  seed.close()

  const log = create()
  try {
    log.open()
    t.fail('invalid stored sequence should prevent open')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'stored sequence uses ErrorWithCode')
    t.equal(err.code, LOG_CORRUPT, 'stored sequence has LOG_CORRUPT code')
    t.equal(err.sqlCode, null, 'stored sequence has no SQLite code')
    t.match(err.message, /seq must be >= 0/, 'stored sequence explains corruption')
  }
  t.end()
})

test('storage errors are normalized and gain operation context', (t) => {
  const { create } = logFixture(t, '02-storage-errors')
  const log = create()
  log.del()
  log.open()

  const appendError = new Error('insert failed')
  log._statements.insert.run = () => { throw appendError }
  try {
    log.append(toEntry('data'))
    t.fail('append should throw')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'append returns ErrorWithCode')
    t.notEqual(err, appendError, 'append normalizes the original error')
    t.equal(err.code, null, 'append classifies an unknown error')
    t.equal(err.sqlCode, null, 'append has no SQLite code')
    t.match(err.message, /\(log append\) insert failed/,
      'append adds operation context')
  }
  delete log._statements.insert.run

  const db = log.db
  const closeError = new Error('close failed')
  db.close = () => { throw closeError }
  try {
    log.close()
    t.fail('close should throw')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'close returns ErrorWithCode')
    t.notEqual(err, closeError, 'close normalizes the original error')
    t.equal(err.code, null, 'close classifies an unknown error')
    t.equal(err.sqlCode, null, 'close has no SQLite code')
    t.match(err.message, /\(log close\) close failed/,
      'close adds operation context')
  }
  t.equal(log.db, db, 'close failure retains the database handle')
  t.ok(log.isOpen, 'close failure keeps the log open')
  t.ok(log._statements, 'close failure retains prepared statements')

  delete db.close
  log.close()
  t.notOk(log.isOpen, 'close retry succeeds')
  t.equal(log.db, null, 'successful retry clears the database handle')
  t.end()
})

test('SQLite errors expose library and native error codes', (t) => {
  const { create } = logFixture(t, '02-sqlite-error-codes')
  const log = create()
  log.del()
  log.open()

  let sqliteError = null
  try {
    log.db.exec('SELECT * FROM missing_table')
  } catch (err) {
    sqliteError = err
  }
  t.ok(sqliteError, 'node:sqlite supplies an error')
  t.equal(sqliteError.errcode, 1, 'node:sqlite supplies the SQLite error code')

  log._statements.insert.run = () => { throw sqliteError }
  try {
    log.append(toEntry('data'))
    t.fail('SQLite append failure should throw')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'normalizes SQLite to ErrorWithCode')
    t.notEqual(err, sqliteError, 'does not return the native SQLite error')
    t.equal(err.code, SQLITE_ERROR, 'sets SQLITE_ERROR code')
    t.equal(err.sqlCode, sqliteError.errcode, 'copies errcode to sqlCode')
    t.equal(err.sqlCode, 1, 'preserves the numeric SQLite code')
    t.match(err.message, /\(log append\) no such table/,
      'adds operation context to the SQLite error')
  }
  delete log._statements.insert.run

  const nonSQLiteError = new Error('not SQLite')
  nonSQLiteError.code = 'E_OTHER'
  nonSQLiteError.errcode = 1
  log._statements.insert.run = () => { throw nonSQLiteError }
  try {
    log.append(toEntry('data'))
    t.fail('non-SQLite append failure should throw')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'normalizes external coded errors')
    t.equal(err.code, null,
      'normalizes a string source code to null')
    t.equal(err.sqlCode, null,
      'does not copy errcode from a non-SQLite error')
  }
  delete log._statements.insert.run
  t.end()
})

test('constructor validates path and iterator step size', (t) => {
  const throwsInvalidArgument = (create, message, label) => {
    try {
      create()
      t.fail(`${label} should throw`)
    } catch (err) {
      t.ok(err instanceof ErrorWithCode, `${label} uses ErrorWithCode`)
      t.equal(err.message, message, `${label} explains the invalid argument`)
      t.equal(err.code, ARGUMENT_ILLEGAL, `${label} has ARGUMENT_ILLEGAL code`)
      t.equal(err.sqlCode, null, `${label} has no SQLite code`)
    }
  }

  throwsInvalidArgument(() => new SQLiteLog(''),
    'SQLiteLog DB path must be non-empty string', 'path')
  throwsInvalidArgument(
    () => new SQLiteLog('/tmp/test.sqlite', { iterStepSize: 0 }),
    'iterStepSize must be int > 0', 'zero step size')
  throwsInvalidArgument(
    () => new SQLiteLog('/tmp/test.sqlite', { iterStepSize: 1.5 }),
    'iterStepSize must be int > 0', 'fractional step size')
  t.end()
})

test('in-memory log is ephemeral and deletion is a closed no-op', (t) => {
  const log = new SQLiteLog(':memory:')
  t.equal(log.path, ':memory:', 'retains the exact in-memory path')
  log.open()
  t.equal(
    String(log.db.prepare('PRAGMA journal_mode').get().journal_mode).toLowerCase(),
    'memory',
    'SQLite selects its in-memory journal mode',
  )
  log.append(toEntry('ephemeral'))
  t.equal(log.seq, 0n, 'in-memory log accepts entries')
  t.throws(() => log.del(), /log is open/, 'open in-memory deletion is rejected')
  log.close()

  const rmSync = fs.rmSync
  let removals = 0
  fs.rmSync = () => {
    removals++
    throw new Error('in-memory deletion touched the filesystem')
  }
  try {
    t.equal(log.del(), undefined, 'closed in-memory deletion succeeds')
  } finally {
    fs.rmSync = rmSync
  }
  t.equal(removals, 0, 'in-memory deletion performs no filesystem calls')

  log.open()
  t.equal(log.seq, -1n, 'reopening creates a fresh in-memory log')
  t.equal(log.head, null, 'the prior in-memory head is discarded')
  log.close()
  t.end()
})

test('delete removes persisted log', (t) => {
  const { create, file } = logFixture(t, '02-delete')
  let log = create()
  log.del()
  log.open()
  log.append(toEntry('persisted'))
  log.close()
  for (const sidecar of sqliteFiles(file).slice(1)) {
    fs.writeFileSync(sidecar, '')
  }
  log.del()
  t.ok(sqliteFiles(file).every((target) => !fs.existsSync(target)),
    'delete removes the database and every deterministic sidecar')

  log = create()
  log.open()
  t.equal(log.seq, -1n, 'deleted log reopens empty')
  t.equal(log.head, null, 'deleted head is empty')
  t.end()
})
