import test from 'tape'
import { pack, unpack } from 'msgpackr'
import { cluster, until, roles } from './helper.js'
import { protocol, append, cmd, record, messages, outputs } from './fixtures/protocol.js'
import { readApply, readMeta } from '../src/utils.js'
import * as Err from '../src/error.js'

const buf = (value) => Buffer.from(value)
const page = (reply) => messages(reply, 'append', 'b').find((msg) => msg.data)
const types = (message) => message.data.map(({ entry }) => unpack(entry).type)
const ack = (f, message) => f.rpc({
  type: 'ack', cid: message.cid, term: message.term, seq: message.seqP + 1n,
})
const submit = (f, clientId, values=[clientId]) =>
  f.input('append', { clientId, items: values.map(buf) })
const decision = (f, seq, digest=Buffer.alloc(16), from='b') =>
  f.rpc({ type: 'monster_outcome', cmdSeq: seq, digest }, from)
const pending = (f) => {
  const first = submit(f, 'first')
  const committed = ack(f, page(first))
  f.complete(messages(committed, 'execute', 'apply')[0], [[0, 'first']])
  return first
}
const clientReplies = (reply, clientId) => outputs(reply).filter(({ to, message }) =>
  to === 'gate' && message.clientId === clientId).map(({ message }) => message)
const errors = (f) => f.diagnostics.filter(({ type }) => type === 'error')

test('an empty queue keeps initial CMD and final SYNC separate without waiting for more work', (t) => {
  const f = protocol().leader()
  try {
    const first = pending(f)
    t.deepEqual(types(page(first)), ['cmd'])
    const decided = decision(f, 1n)
    t.deepEqual(types(page(decided)), ['sync'])
    t.deepEqual(clientReplies(decided, 'first').map(({ cmdSeq, results }) => [cmdSeq, results]),
      [[1n, [[0, 'first']]]], 'the result still precedes SYNC commitment')
    const settled = ack(f, page(decided))
    t.equal(messages(settled, 'execute', 'apply').length, 0)
    t.equal(f.proc.engine.appliedSeq, 2n)
    t.equal(f.proc.engine.leaderCommand, null)
    t.deepEqual(errors(f), [])
  } finally { f.close() }
  t.end()
})

test('backlogged local and forwarded commands pair in FIFO order with separate results and events', (t) => {
  const f = protocol().leader()
  try {
    pending(f)
    submit(f, 'second', ['second-0', 'second-1'])
    f.rpc({ type: 'fwd_cmd', cid: 'third', term: f.proc.engine.term, items: [buf('third')] }, 'c')
    const decided = decision(f, 1n)
    const pair = page(decided)
    t.deepEqual(types(pair), ['sync', 'cmd'], 'both records leave in the first replication page')
    const next = unpack(pair.data[1].entry)
    t.deepEqual(next.items.map((item) => Buffer.from(item).toString()), ['second-0', 'second-1'])
    t.deepEqual(next.matchIndex, [2n, 1n, -1n], 'self includes SYNC; remote indices retain their ACK positions')
    t.equal(f.proc.engine.leaderCommand.cmdSeq, 3n)
    t.equal(f.proc.engine.pendingCommand.cmdSeq, 1n, 'the prior command remains pending until SYNC applies')
    t.equal(clientReplies(decided, 'second').length, 0)
    const committed = ack(f, pair)
    t.deepEqual(outputs(committed).filter(({ message }) =>
      ['sync', 'execute'].includes(message.type)).map(({ message }) => message.type),
    ['sync', 'execute'], 'SYNC precedes the next execution')
    t.equal(readMeta(f.proc.db).applied_seq, 2n)
    t.equal(readMeta(f.proc.db).pending_cmd_seq, null)
    const execute = messages(committed, 'execute', 'apply')[0]
    t.equal(execute.seq, 3n)
    const completed = f.complete(execute, [[0, 'second-0'], [0, 'second-1']])
    t.equal(messages(completed, 'cmd', 'gate')[0].cmdSeq, 3n)
    const again = decision(f, 3n)
    t.deepEqual(types(page(again)), ['sync', 'cmd'], 'a continuous backlog pairs repeatedly')
    t.deepEqual(clientReplies(again, 'second')[0].results, [[0, 'second-0'], [0, 'second-1']])
    const third = messages(ack(f, page(again)), 'execute', 'apply')[0]
    t.equal(third.seq, 5n)
    f.complete(third, [[0, 'third']])
    const final = decision(f, 5n)
    t.deepEqual(types(page(final)), ['sync'])
    t.deepEqual(messages(final, 'ack', 'c').filter(({ cid }) => cid === 'third')
      .map(({ cmdSeq, results }) => [cmdSeq, results]), [[5n, [[0, 'third']]]])
    ack(f, page(final))
    t.deepEqual(f.rows().slice(1).map(({ entry }) => unpack(entry).type),
      ['cmd', 'sync', 'cmd', 'sync', 'cmd', 'sync'], 'log encoding and sequence positions stay ordinary')
    t.deepEqual(errors(f), [])
  } finally { f.close() }
  t.end()
})

for (const opts of [{ rpcMax: 1 }, { rpcMaxBytes: 1 }]) {
  test(`paired appends respect replication paging (${Object.keys(opts)[0]})`, (t) => {
    const f = protocol(opts).leader()
    try {
      pending(f)
      submit(f, 'second')
      const decided = decision(f, 1n)
      t.equal(f.proc.log.seq, 3n, 'both rows are already appended')
      t.deepEqual(types(page(decided)), ['sync'], 'the first page obeys its configured limit')
      const synced = ack(f, page(decided))
      t.equal(f.proc.engine.appliedSeq, 2n)
      t.equal(messages(synced, 'execute', 'apply').length, 0, 'CMD waits for its own commitment')
      t.deepEqual(types(page(synced)), ['cmd'])
      const committed = ack(f, page(synced))
      t.equal(messages(committed, 'execute', 'apply')[0].seq, 3n)
      t.equal(clientReplies(committed, 'second').length, 0, 'replication is not command agreement')
      t.deepEqual(errors(f), [])
    } finally { f.close() }
    t.end()
  })
}

test('no-quorum and dissenting-leader decisions never attach a queued command', (t) => {
  for (const dissent of [false, true]) {
    const f = protocol().leader()
    try {
      pending(f)
      submit(f, 'second')
      decision(f, 1n, Buffer.alloc(16, 1))
      const decided = decision(f, 1n, Buffer.alloc(16, dissent ? 1 : 2), 'c')
      if (dissent) {
        t.equal(decided, undefined, 'a dissenting leader publishes no completion')
        t.equal(f.rows().length, 2, 'no decision or queued CMD is appended')
        t.equal(f.state().meta.repair_state, BigInt(Err.REPAIR_OUTSIDE))
        t.equal(errors(f).length, 3, 'each Proc stops with its own repair error')
      } else {
        t.deepEqual(types(page(decided)), ['sync'])
        t.equal(unpack(page(decided).data[0].entry).quorum, false)
        const fenced = ack(f, page(decided))
        t.equal(messages(fenced, 'execute', 'apply').length, 0)
        t.equal(clientReplies(fenced, 'second')[0].code, Err.REPAIR_NO_QUORUM)
        t.ok(f.procs.every((proc) => proc.isOpen), 'no-quorum remains a live fence')
        t.equal(f.proc.engine.pendingCommand, null)
      }
    } finally { f.close() }
  }
  t.end()
})

test('a dissenting follower fences before executing the CMD paired with SYNC', (t) => {
  const f = protocol().setup()
  try {
    const first = f.rpc(append({
      seqP: 0n, termP: 1n, commitSeq: 1n, data: [record(1n, cmd())],
    }))
    f.complete(messages(first, 'execute', 'apply')[0])
    const sync = pack({
      type: 'sync', cmdSeq: 1n, quorum: true, digest: Buffer.alloc(16, 1),
      agree: ['b', 'c'], disagree: ['a'],
    })
    const fenced = f.rpc(append({
      seqP: 1n, termP: 1n, commitSeq: 3n,
      data: [record(1n, sync), record(1n, cmd([buf('must not execute')], [1n, 2n, 1n]))],
    }))
    t.equal(fenced, undefined, 'fatal repair publishes no completion or next execution')
    t.equal(f.state().meta.repair_state, BigInt(Err.REPAIR_OUTSIDE), 'the repair fence commits')
    t.equal(f.state().meta.applied_seq, 2n, 'only the SYNC is applied')
    t.equal(readApply(f.db2).seq, 1n, 'the prior Apply receipt remains unchanged')
    t.deepEqual(errors(f).map(({ error }) => error.code), roles.map(() => Err.REPAIR_OUTSIDE))
    t.ok(f.procs.every((proc) => !proc.isOpen))
  } finally { f.close() }
  t.end()
})

test('drain before deciding rejects queued work and appends only SYNC', (t) => {
  const f = protocol().leader()
  try {
    pending(f)
    submit(f, 'second')
    const draining = f.input('drain', { clientId: 'drain' })
    t.equal(clientReplies(draining, 'second')[0].code, Err.DRAINING)
    t.equal(clientReplies(draining, 'drain').length, 0)
    const decided = decision(f, 1n)
    t.deepEqual(types(page(decided)), ['sync'])
    const drained = ack(f, page(decided))
    t.equal(clientReplies(drained, 'drain')[0].type, 'ack')
    t.equal(messages(drained, 'execute', 'apply').length, 0)
    t.equal(f.proc.log.seq, 2n)
    t.deepEqual(errors(f), [])
  } finally { f.close() }
  t.end()
})

test('drain after paired replication applies SYNC and leaves the unstarted CMD for restart', (t) => {
  const f = protocol().leader()
  try {
    pending(f)
    submit(f, 'second')
    const pair = page(decision(f, 1n))
    f.input('drain', { clientId: 'drain' })
    const drained = ack(f, pair)
    t.equal(clientReplies(drained, 'drain')[0].type, 'ack')
    t.equal(messages(drained, 'execute', 'apply').length, 0)
    t.equal(readMeta(f.proc.db).applied_seq, 2n)
    t.equal(readMeta(f.proc.db).pending_cmd_seq, null)
    f.reopen()
    const resumed = f.rpc(append({ seqP: 3n, termP: 1n, commitSeq: 3n }))
    t.equal(messages(resumed, 'execute', 'apply')[0].seq, 3n)
    t.equal(messages(resumed, 'sync', 'gate').length, 0, 'the durable SYNC is not replayed')
    t.deepEqual(errors(f), [])
  } finally { f.close() }
  t.end()
})

for (const cause of ['term', 'timeout']) {
  test(`paired command ownership survives ${cause === 'term' ? 'leadership loss' : 'replication timeout'}`, (t) => {
    const f = protocol({ electionTimeout: 5000, appendTimeout: 100 }).leader()
    try {
      pending(f)
      submit(f, 'second')
      const decided = decision(f, 1n)
      t.equal(clientReplies(decided, 'first')[0].type, 'ack')
      const failed = cause === 'term'
        ? f.rpc(append({ term: 2n, seqP: 3n, termP: 1n, commitSeq: 1n }))
        : f.tick(f.proc.executor.timems + 101)
      t.equal(clientReplies(failed, 'first').length, 0, 'the prior caller is never replied to twice')
      t.equal(clientReplies(failed, 'second')[0].code,
        cause === 'term' ? Err.NOT_LEADER : Err.APPEND_TIMEOUT)
      t.equal(messages(failed, 'execute', 'apply').length, 0)
      const late = ack(f, page(decided))
      t.equal(clientReplies(late, 'second').length, 0, 'the stale ACK cannot settle the new caller again')
      t.equal(f.proc.engine.state, 'follower')
      t.deepEqual(errors(f), [])
    } finally { f.close() }
    t.end()
  })
}

for (const delay of [0, 2]) {
  test(`real cluster pairs queued singles and batches with ${delay ? 'delayed' : 'synchronous'} transport`, async (t) => {
    let holdReports = false
    const c = cluster({
      delay, localDelay: delay,
      opts: { electionTimeout: 1000, appendTimeout: 5000, localTimeout: 5000 },
      transport({ message }) {
        if (holdReports && message.type === 'monster_outcome') { return false }
      },
    })
    try {
      await c.open()
      const leader = c.leader()
      const engine = c.nodes.get(leader.id).procA.engine
      holdReports = true
      const works = [leader.append(buf('first'))]
      works[0].catch(() => {})
      await until(() => engine.pendingCommand !== null, 'first command awaits agreement')
      const follower = c.gates().find((gate) => gate.id !== leader.id)
      const requests = [
        { gate: leader, items: ['batch-0', 'batch-1'] },
        { gate: follower, items: ['forwarded'] },
        { gate: follower, items: ['forwarded-batch-0', 'forwarded-batch-1'] },
        { gate: leader, items: ['last'] },
      ]
      for (const [index, request] of requests.entries()) {
        const work = request.items.length === 1 ? request.gate.append(buf(request.items[0]))
          : request.gate.appendBatch(request.items.map(buf))
        work.catch(() => {})
        works.push(work)
        await until(() => engine.commands.length === index + 1, 'FIFO backlog reaches leader')
      }
      holdReports = false
      const results = await Promise.all(works)
      await c.settled()
      await until(() => c.ids.every((id) => c.events.filter((event) =>
        event.id === id && ['cmd', 'sync'].includes(event.type)).length === works.length * 2),
      'all committed event outputs reach Gate')
      const seqs = results.map(([seq]) => seq)
      t.deepEqual(seqs.slice(1), seqs.slice(0, -1).map((seq) => seq + 2n), 'command positions remain separated by SYNC')
      t.deepEqual(results.map(([, result]) => Array.isArray(result) ? result.map(({ value }) => value) : [result]),
        [['first'], ...requests.map(({ items }) => items)], 'each caller receives its own item results')
      const paired = c.history.filter(({ from, message }) => from === leader.id && message.type === 'append' &&
        message.data?.length === 2 && message.data.every(({ entry }) => entry.length) &&
        types(message).join(',') === 'sync,cmd')
      t.deepEqual([...new Set(paired.map(({ message }) => message.seqP + 2n))]
        .sort((a, b) => a < b ? -1 : a > b ? 1 : 0), seqs.slice(1),
      'every queued command is paired with the preceding SYNC')
      for (const [id, node] of c.nodes) {
        const progress = c.events.filter((event) => event.id === id && ['cmd', 'sync'].includes(event.type))
        t.deepEqual(progress.map(({ type, event }) => [type, event.cmdSeq]),
          seqs.flatMap((seq) => [['cmd', seq], ['sync', seq]]), `${id}: every decision precedes the next CMD event`)
        const values = node.apply.db.prepare('SELECT value FROM items ORDER BY id').all()
          .map(({ value }) => Buffer.from(value).toString())
        t.deepEqual(values, ['first', ...requests.flatMap(({ items }) => items)], `${id}: Apply preserves FIFO item order`)
        t.ok(roles.every((role) => node[role].engine.pendingCommand === null), `${id}: all three Procs agree on completion`)
      }
      t.deepEqual(c.errors, [])
    } finally { holdReports = false; await c.close() }
    t.end()
  })
}
