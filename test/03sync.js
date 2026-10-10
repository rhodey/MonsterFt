import test from 'tape'
import { ErrorWithCode } from '../src/error.js'
import { logFixture, toEntry } from './util.js'

const collect = (log, seq=0n, opts={}) => {
  const entries = []
  for (const entry of log.iter(seq, opts)) {
    entries.push(entry.entry.toString())
  }
  return entries
}

test('SQLiteLog return values and iteration', (t) => {
  const { create } = logFixture(t, '03-sync')
  const log = create()

  log.del()
  log.open()
  t.equal(log.append(toEntry('zero', 1n)), 0n, 'append returns its seq directly')
  t.deepEqual(log.election(2n, 'node-a'), [2n, 'node-a'],
    'election returns state directly')
  t.equal(log.appendBatch([
    toEntry('one', 2n),
    toEntry('two', 3n),
  ]), 1n, 'appendBatch returns its first seq directly')

  const iter = log.iter(0n)
  t.deepEqual(Array.from(iter, (entry) => entry.entry.toString()),
    ['zero', 'one', 'two'], 'iterator yields entries')
  t.deepEqual(Array.from(log.iter(), (entry) => entry.term),
    [1n, 2n, 3n], 'iterator preserves each term in a mixed-term batch')
  log.trim(0n)
  log.close()
  log.del()
  t.end()
})

test('appendBatch publishes cache only after commit', (t) => {
  const { create } = logFixture(t, '03-batch-publish')
  const log = create()
  log.del()
  log.open()
  log.append(toEntry('seed', 1n))

  const exec = log.db.exec.bind(log.db)
  let cacheAtCommit = null
  log.db.exec = (sql) => {
    if (sql === 'COMMIT') {
      cacheAtCommit = {
        seq: log.seq,
        term: log.term,
        head: log.head.toString(),
      }
    }
    return exec(sql)
  }

  const first = log.appendBatch([
    toEntry('one', 2n),
    toEntry('two', 3n),
  ])
  delete log.db.exec

  t.equal(first, 1n, 'batch returns its first seq')
  t.deepEqual(cacheAtCommit, {
    seq: 0n,
    term: 1n,
    head: 'seed',
  }, 'cache still describes the committed prefix when COMMIT starts')
  t.equal(log.seq, 2n, 'sequence publishes after commit')
  t.equal(log.term, 3n, 'head term publishes after commit')
  t.equal(log.head.toString(), 'two', 'head publishes after commit')
  t.deepEqual(collect(log), ['seed', 'one', 'two'], 'whole batch is durable')
  t.end()
})

test('appendBatch rollback preserves rows and cache', (t) => {
  const { create } = logFixture(t, '03-batch-rollback')
  const log = create()
  log.del()
  log.open()
  log.append(toEntry('seed', 1n))

  const before = {
    seq: log.seq,
    term: log.term,
    head: Buffer.from(log.head),
  }
  const failure = new Error('insert failed')
  const insert = log._statements.insert.run.bind(log._statements.insert)
  let inserts = 0
  log._statements.insert.run = (...args) => {
    if (++inserts === 2) { throw failure }
    return insert(...args)
  }

  try {
    log.appendBatch([
      toEntry('pending-one', 2n),
      toEntry('pending-two', 2n),
    ])
    t.fail('batch should throw')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'batch normalizes the original error')
    t.notEqual(err, failure, 'batch returns the normalized error')
    t.equal(err.code, null, 'batch classifies an unknown error')
    t.equal(err.sqlCode, null, 'batch has no SQLite code')
    t.match(err.message, /\(log appendBatch\) insert failed/,
      'batch adds operation context')
  } finally {
    delete log._statements.insert.run
  }

  t.equal(log.seq, before.seq, 'failed batch keeps cached seq')
  t.equal(log.term, before.term, 'failed batch keeps cached term')
  t.deepEqual(log.head, before.head, 'failed batch keeps cached head')
  t.deepEqual(collect(log), ['seed'], 'failed batch rolls back inserted rows')
  t.equal(log.append(toEntry('after', 3n)), 1n, 'transaction is closed after rollback')
  t.deepEqual(collect(log), ['seed', 'after'], 'later append succeeds')
  t.end()
})

test('appendBatch reloads cache when rollback fails', (t) => {
  const { create } = logFixture(t, '03-batch-rollback-failure')
  const log = create()
  log.del()
  log.open()
  log.append(toEntry('seed', 1n))

  const insertFailure = new Error('insert failed')
  const rollbackFailure = new Error('rollback failed')
  const insert = log._statements.insert.run.bind(log._statements.insert)
  const exec = log.db.exec.bind(log.db)
  let inserts = 0
  log._statements.insert.run = (...args) => {
    if (++inserts === 2) { throw insertFailure }
    return insert(...args)
  }
  log.db.exec = (sql) => {
    if (sql === 'ROLLBACK') { throw rollbackFailure }
    return exec(sql)
  }

  try {
    log.appendBatch([
      toEntry('transaction-visible', 2n),
      toEntry('never-inserted', 3n),
    ])
    t.fail('batch should throw')
  } catch (err) {
    t.ok(err instanceof ErrorWithCode, 'batch normalizes the original error')
    t.notEqual(err, insertFailure, 'batch returns the normalized error')
    t.equal(err.code, null, 'batch classifies an unknown error')
    t.equal(err.sqlCode, null, 'batch has no SQLite code')
    t.equal(log.seq, 1n, 'cache reloads the connection-visible sequence')
    t.equal(log.term, 2n, 'cache reloads the connection-visible term')
    t.equal(log.head.toString(), 'transaction-visible',
      'cache reloads the connection-visible head')
  } finally {
    delete log._statements.insert.run
    delete log.db.exec
  }

  exec('ROLLBACK')
  log.close()
  log.open()
  t.equal(log.seq, 0n, 'reopen reloads the committed sequence')
  t.equal(log.term, 1n, 'reopen reloads the committed term')
  t.equal(log.head.toString(), 'seed', 'reopen reloads the committed head')
  t.end()
})
