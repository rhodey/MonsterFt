import test from 'tape'
import { unpack } from 'msgpackr'
import { protocol, append, messages, outputs } from './fixtures/protocol.js'
import * as Err from '../src/error.js'

const digest = Buffer.alloc(16)
const page = (reply) => messages(reply, 'append', 'b').find((msg) => msg.data)
const ack = (f, message) => f.rpc({
  type: 'ack', cid: message.cid, term: message.term,
  seq: message.seqP + BigInt(message.data.length),
})
const report = (f, from='b', value=digest, timems=f.proc.executor.timems) =>
  f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest: value }, from, timems)
const queries = (reply) => outputs(reply).filter(({ message }) =>
  message.type === 'monster_outcome_request').map(({ to }) => to)
const begin = (f, forwarded=false) => {
  const items = [Buffer.from('one')]
  const reply = forwarded
    ? f.rpc({ type: 'fwd_cmd', cid: 'caller', term: f.proc.engine.term, items }, 'c')
    : f.input('append', { clientId: 'caller', items })
  return messages(ack(f, page(reply)), 'execute', 'apply')[0]
}

for (const forwarded of [false, true]) {
  test(`${forwarded ? 'forwarded' : 'local'} CMD uses announcements before requesting missing reports`, (t) => {
    const f = protocol().leader()
    try {
      const completed = f.complete(begin(f, forwarded))
      t.deepEqual(queries(completed), [], 'receipt import sends no immediate query')
      const timer = f.proc.engine.reportTimer
      const due = f.proc.executor.timers.get(timer).due
      t.equal(due, f.proc.executor.timems + f.proc.engine.retryms)
      // Input processing precedes due timers, so this announcement cancels the query.
      const decided = report(f, 'b', digest, due)
      t.deepEqual(queries(decided), [], 'a ready announcement needs no retry, even at the deadline')
      t.equal(unpack(page(decided).data[0].entry).type, 'sync')
      t.ok(!f.proc.executor.timers.has(timer))
      const result = forwarded ? messages(decided, 'ack', 'c').find((m) => m.cid === 'caller')
        : messages(decided, 'ack', 'gate').find((m) => m.clientId === 'caller')
      t.deepEqual(result.results, [[0, 'ok']])
      t.ok(f.procs.every((proc) => proc.engine.leaderCommand.phase === 'sync'))
    } finally { f.close() }
    t.end()
  })
}

test('lost announcements retry only missing peers until agreement', (t) => {
  const f = protocol({}, ['a', 'b', 'c', 'd', 'e']).leader()
  try {
    // Five members require two remote reports; retain one before local Apply finishes.
    const first = f.input('append', { clientId: 'caller', items: [Buffer.from('one')] })
    ack(f, page(first))
    const other = messages(first, 'append', 'c').find((msg) => msg.data)
    const committed = f.rpc({ type: 'ack', cid: other.cid, term: other.term, seq: 1n }, 'c')
    report(f)
    f.complete(messages(committed, 'execute', 'apply')[0])
    const due = () => f.proc.executor.timers.get(f.proc.engine.reportTimer).due
    t.deepEqual(queries(f.tick(due() - 1)), [])
    t.deepEqual(queries(f.tick(due())), ['c', 'd', 'e'])
    t.deepEqual(queries(f.tick(due())), ['c', 'd', 'e'], 'a lost query retries normally')
    const decided = report(f, 'c')
    t.equal(unpack(page(decided).data[0].entry).quorum, true)
    t.equal(f.proc.engine.reportTimer, null)
    t.deepEqual(queries(f.tick(f.proc.executor.timems + f.proc.engine.retryms)), [])
  } finally { f.close() }
  t.end()
})

test('early reports keep their first digest and cannot decide before the local receipt', (t) => {
  const f = protocol().leader()
  try {
    const execute = begin(f)
    report(f)
    report(f, 'b', Buffer.alloc(16, 1))
    report(f, 'c', Buffer.alloc(16, 2))
    t.equal(f.proc.log.seq, 1n)
    t.ok(f.procs.every((proc) => proc.engine.reports.get('b').equals(digest)))
    const decided = f.complete(execute)
    t.deepEqual(queries(decided), [])
    const sync = unpack(page(decided).data[0].entry)
    t.deepEqual(sync.agree, ['a', 'b'])
    t.deepEqual(sync.disagree, ['c'])
  } finally { f.close() }
  t.end()
})

for (const dissent of [false, true]) {
  test(`early reports preserve ${dissent ? 'fatal leader dissent' : 'live no-quorum repair'}`, (t) => {
    const f = protocol().leader()
    try {
      const execute = begin(f)
      report(f, 'b', Buffer.alloc(16, 1))
      report(f, 'c', Buffer.alloc(16, dissent ? 1 : 2))
      t.ok(f.procs.every((proc) => proc.isOpen), 'remote reports alone cannot fence the leader')
      const decided = f.complete(execute)
      if (dissent) {
        t.equal(decided, undefined, 'fatal repair publishes no completion')
        t.equal(f.state().meta.repair_state, BigInt(Err.REPAIR_OUTSIDE))
        t.equal(f.diagnostics.filter(({ type }) => type === 'error').length, 3)
      } else {
        t.equal(unpack(page(decided).data[0].entry).quorum, false)
        ack(f, page(decided))
        t.equal(f.state().meta.repair_state, BigInt(Err.REPAIR_NO_QUORUM))
        t.ok(f.procs.every((proc) => proc.isOpen))
      }
    } finally { f.close() }
    t.end()
  })
}

for (const restart of [false, true]) {
  test(`${restart ? 'restart' : 'leadership loss'} queries an inherited CMD without waiting for a retry`, (t) => {
    const f = protocol().leader()
    try {
      f.complete(begin(f))
      const oldTimer = f.proc.engine.reportTimer
      if (restart) { f.reopen() }
      else {
        f.rpc(append({ term: 2n, seqP: 1n, termP: 1n, commitSeq: 1n }))
        t.equal(f.proc.engine.reportTimer, null)
        t.ok(!f.proc.executor.timers.has(oldTimer))
      }
      const elected = f.elect()
      const resumed = ack(f, page(elected))
      t.equal(f.proc.engine.leaderCommand.request, null)
      t.deepEqual(queries(resumed), ['b', 'c'], 'recovery does not depend on another announcement')
      t.equal(messages(resumed, 'execute', 'apply').length, 0, 'committed Apply work is not repeated')
      const decided = report(f)
      t.equal(unpack(page(decided).data[0].entry).cmdSeq, 1n)
      t.ok(f.procs.every((proc) => proc.engine.reportTimer === null))
    } finally { f.close() }
    t.end()
  })
}
