import test from 'tape'
import { ARGUMENT_ILLEGAL, LOG_CORRUPT } from '../src/error.js'
import { logFixture, toEntry } from './util.js'

const collect = (iter) => {
  const entries = []
  for (const entry of iter) { entries.push(entry) }
  return entries
}

const strings = (entries) => entries.map((entry) => entry.subarray(8).toString())
const toEntries = (entries) => entries.map((entry) => toEntry(entry))

test('iterate all entries and from an offset', (t) => {
  const { create } = logFixture(t, '04-range')
  const log = create()
  log.del()
  log.open()

  const entries = [toEntry(Buffer.alloc(0)), toEntry('x'), toEntry('longer')]
  log.appendBatch(entries)
  let found = collect(log.iter(0n))
  t.equal(found.length, 3, 'all entries returned')
  found.forEach((entry, idx) => t.ok(entry.equals(entries[idx]), `entry ${idx} matches`))

  found = collect(log.iter(1n))
  t.deepEqual(strings(found), ['x', 'longer'], 'offset entries returned')
  t.deepEqual(collect(log.iter(3n)), [], 'head plus one is empty')
  t.deepEqual(collect(log.iter(99n)), [], 'past head is empty')
  t.end()
})

test('iterator batches lazily and preserves order', (t) => {
  const { create } = logFixture(t, '04-batches')
  const log = create()
  log.del()
  log.open()
  const expected = ['zero', 'one', 'two', 'three', 'four']
  log.appendBatch(toEntries(expected))

  for (const iterStepSize of [1, 2, 3, 99]) {
    const found = strings(collect(log.iter(0n, { iterStepSize })))
    t.deepEqual(found, expected, `step size ${iterStepSize}`)
  }

  const range = log._statements.range.all.bind(log._statements.range)
  let reads = 0
  log._statements.range.all = (...args) => {
    reads++
    return range(...args)
  }
  const iter = log.iter(0n, { iterStepSize: 2 })
  t.equal(reads, 0, 'creating an iterator does not read rows')
  t.equal(iter.next().value.subarray(8).toString(), 'zero', 'first row is available')
  t.equal(reads, 1, 'first step reads one batch')
  t.deepEqual(strings(collect(iter)), ['one', 'two', 'three', 'four'],
    'remaining batches preserve order')
  t.equal(reads, 3, 'five rows use three batches')
  delete log._statements.range.all
  t.end()
})

test('iterator end is captured when it is created', (t) => {
  const { create } = logFixture(t, '04-snapshot')
  const log = create()
  log.del()
  log.open()
  log.appendBatch(toEntries(['zero', 'one', 'two']))

  const iter = log.iter(0n)
  log.append(toEntry('three'))
  t.deepEqual(strings(collect(iter)), ['zero', 'one', 'two'], 'later append is excluded')
  t.deepEqual(strings(collect(log.iter(0n))), ['zero', 'one', 'two', 'three'],
    'new iterator sees append')
  t.end()
})

test('empty log iterator returns no entries', (t) => {
  const { create } = logFixture(t, '04-empty')
  const log = create()
  log.del()
  log.open()
  t.deepEqual(collect(log.iter()), [], 'empty iterator')
  t.end()
})

test('iteration can stop early', (t) => {
  const { create } = logFixture(t, '04-break')
  const log = create()
  log.del()
  log.open()
  log.appendBatch(toEntries(['zero', 'one', 'two']))

  let count = 0
  for (const entry of log.iter(0n)) {
    t.equal(entry.subarray(8).toString(), 'zero', 'first entry')
    count++
    break
  }
  t.equal(count, 1, 'one entry consumed')
  t.end()
})

test('iterator validates arguments', (t) => {
  const { create } = logFixture(t, '04-validation')
  const log = create()
  log.del()
  log.open()

  const throwsInvalidArgument = (run, message, label) => {
    try {
      run()
      t.fail(`${label} should throw`)
    } catch (err) {
      t.match(err.message, message, `${label} includes iterator context`)
      t.equal(err.code, ARGUMENT_ILLEGAL, `${label} has ARGUMENT_ILLEGAL code`)
      t.equal(err.sqlCode, null, `${label} has no SQLite code`)
    }
  }

  throwsInvalidArgument(
    () => log.iter(-1n), /\(log iter\) seq must be >= 0/,
    'negative start')
  throwsInvalidArgument(
    () => log.iter(0), /\(log iter\) seq must be bigint/,
    'non-bigint start')
  throwsInvalidArgument(
    () => log.iter(0n, { iterStepSize: 0 }),
    /\(log iter\) iterStepSize must be int > 0/, 'zero step')
  throwsInvalidArgument(
    () => log.iter(0n, { iterStepSize: 1.5 }),
    /\(log iter\) iterStepSize must be int > 0/, 'fractional step')
  t.end()
})

test('iterator ends when the remaining range is empty', (t) => {
  const { create } = logFixture(t, '04-trailing-gap')
  const log = create()
  log.del()
  log.open()
  log.appendBatch(toEntries(['zero', 'one', 'two']))
  log.db.prepare('DELETE FROM raft_log WHERE seq = ?').run(2n)

  const found = strings(collect(log.iter(0n)))
  t.deepEqual(found, ['zero', 'one'], 'missing trailing row ends iteration')
  t.end()
})

test('iterator rejects an out-of-order row after a gap', (t) => {
  const { create } = logFixture(t, '04-middle-gap')
  const log = create()
  log.del()
  log.open()
  log.appendBatch(toEntries(['zero', 'one', 'two']))
  log.db.prepare('DELETE FROM raft_log WHERE seq = ?').run(1n)

  try {
    collect(log.iter(0n))
    t.fail('middle gap should throw')
  } catch (err) {
    t.ok(err.message.includes('(log iter) seq 2 !== 1'),
      'later row is out of order with iterator context')
    t.equal(err.code, LOG_CORRUPT, 'out-of-order stored row has LOG_CORRUPT code')
    t.equal(err.sqlCode, null, 'out-of-order stored row has no SQLite code')
  }
  t.end()
})
