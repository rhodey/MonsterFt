import test from 'tape'
import { pack, unpack } from 'msgpackr'
import { protocol, append, record, cmd, messages, outputs } from './fixtures/protocol.js'
import { isFwdCmdAck } from '../src/utilm.js'
import * as Err from '../src/error.js'

const digest = Buffer.alloc(16, 7)
const sync = (fields={}) => ({
  type: 'sync', cmdSeq: 1n, quorum: true, digest, agree: ['a', 'b'], disagree: [], ...fields,
})
function pending(f) {
  const r = f.rpc(append({ seqP: 0n, termP: 1n, data: [record(1n, cmd())], commitSeq: 1n }))
  f.complete(messages(r, 'execute', 'apply')[0], [[0, 'ok']], digest)
}
const page = (r) => messages(r, 'append', 'b').find((msg) => msg.data)
const ack = (f, msg, from='b') => f.rpc({ type: 'ack', cid: msg.cid, term: msg.term, seq: msg.seqP + 1n }, from)
function active(f) {
  const request = f.input('append', { requestId: 'user', items: [Buffer.from('item')] })
  const r = ack(f, page(request))
  f.complete(messages(r, 'execute', 'apply')[0], [[0, 'ok']], digest)
}

test('committed malformed CMD and SYNC entries cannot advance Apply', (t) => {
  const cmds = [
    { type: 'unknown' }, { type: 'cmd', items: [], matchIndex: [0n, 0n, 0n] },
    { type: 'cmd', items: ['bad'], matchIndex: [0n, 0n, 0n] },
    ...[undefined, [], [0n, 0n], [0, 0n, 0n], [1n, 0n, 0n], [-2n, 0n, 0n]]
      .map((matchIndex) => ({ type: 'cmd', items: [Buffer.from('item')], matchIndex })),
  ]
  const syncs = [
    { cmdSeq: -1n }, { quorum: 'yes' }, { agree: ['a'] }, { agree: ['b', 'a'] },
    { agree: ['a', 'a'] }, { agree: ['a', 'foreign'] }, { disagree: ['c', 'c'] },
    { disagree: ['foreign'] }, { digest: null },
    ...[8, 15, 17, 32].map((size) => ({ digest: Buffer.alloc(size) })),
    { quorum: false, agree: ['a'] }, { cmdSeq: 99n },
  ].map(sync)
  for (const entry of [...cmds, ...syncs]) {
    const f = protocol().setup()
    try {
      if (entry.type === 'sync') pending(f)
      const before = f.state().meta
      const seq = before.applied_seq + 1n
      const r = f.rpc(append({ seqP: seq - 1n, termP: 1n, data: [record(1n, pack(entry))], commitSeq: seq }))
      t.equal(r, undefined, 'corrupt entry has no successful completion')
      t.equal(f.proc._fatalError.code, Err.MONSTER_CORRUPT)
      t.deepEqual(f.state().meta, before, 'checkpoint and receipt import state unchanged')
    } finally { f.close() }
  }
  for (const secondCmd of [false, true]) {
    const f = protocol().setup()
    try {
      if (secondCmd) pending(f)
      const seq = f.proc.engine.appliedSeq + 1n
      const entry = secondCmd ? cmd() : pack(sync())
      t.equal(f.rpc(append({ seqP: seq - 1n, termP: 1n, data: [record(1n, entry)], commitSeq: seq })), undefined)
      t.equal(f.proc._fatalError.code, secondCmd ? Err.MONSTER_ILLEGAL : Err.MONSTER_CORRUPT)
      t.equal(f.state().meta.pending_cmd_seq, secondCmd ? 1n : null)
    } finally { f.close() }
  }
  t.end()
})

test('SYNC membership takes precedence over the unlisted digest comparison', (t) => {
  for (const [fields, expected] of [
    [{ agree: ['a', 'b'], disagree: ['a'], digest: Buffer.alloc(16, 9) }, 0],
    [{ agree: ['b', 'c'], disagree: ['a'] }, 2],
    [{ agree: ['b', 'c'], disagree: [] }, 0],
    [{ agree: ['b', 'c'], disagree: [], digest: Buffer.alloc(16, 9) }, 2],
  ]) {
    const f = protocol().setup()
    try {
      pending(f)
      f.rpc(append({ seqP: 1n, termP: 1n, commitSeq: 2n, data: [record(1n, pack(sync(fields)))] }))
      t.equal(f.state().meta.repair_state, BigInt(expected))
      t.equal(f.proc.isOpen, expected === 0)
    } finally { f.close() }
  }
  t.end()
})

test('early and stale reports remain isolated from later command decisions', (t) => {
  const f = protocol().leader()
  try {
    const r = ack(f, page(f.input('append', { requestId: 'user', items: [Buffer.from('item')] })))
    f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest })
    t.equal(f.proc.log.seq, 1n, 'remote outcome cannot decide before local receipt')
    const local = f.complete(messages(r, 'execute', 'apply')[0], [[0, 'ok']], digest)
    t.ok(messages(local, 'monster_outcome_request').length, 'missing report is requested again')
    for (const msg of [
      { type: 'monster_outcome', cmdSeq: -1n, digest },
      ...[8, 15, 17, 32].map((size) =>
        ({ type: 'monster_outcome', cmdSeq: 1n, digest: Buffer.alloc(size) })),
      { type: 'monster_outcome_request', cmdSeq: -1n },
      { type: 'monster_outcome', cmdSeq: 99n, digest },
    ]) f.rpc(msg)
    t.equal(f.proc.log.seq, 1n)
    t.equal(f.diagnostics.filter(({ role, type }) => role === 'procA' && type === 'warn').length, 6)
    const decided = f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest })
    t.equal(f.proc.log.seq, 2n, 'decisive report appends SYNC in the same input')
    ack(f, page(decided))
    f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest })
    t.equal(f.proc.engine.reports.size, 0, 'late report cannot restore a completed decision')
    active(f)
    const old = f.proc.log.seq
    f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest })
    t.equal(f.proc.log.seq, old, 'previous command report cannot decide the next command')
  } finally { f.close() }
  t.end()
})

test('five-member quorum remains possible until all matching groups are exhausted', (t) => {
  const f = protocol({}, ['a', 'b', 'c', 'd', 'e']).leader()
  try {
    const submitted = f.input('append', { requestId: 'user', items: [Buffer.from('item')] })
    let applied
    for (const from of ['b', 'c']) applied = ack(f,
      messages(submitted, 'append', from).find((msg) => msg.data), from)
    f.complete(messages(applied, 'execute', 'apply')[0], [[0, 'ok']], digest)
    for (const [from, byte] of [['b', 8], ['c', 9]]) {
      f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest: Buffer.alloc(16, byte) }, from)
      t.equal(f.proc.log.seq, 1n, 'remaining two members can still join an existing digest')
    }
    f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest: Buffer.alloc(16, 8) }, 'b')
    t.equal(f.proc.log.seq, 1n, 'duplicate report does not consume an absent member')
    f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest: Buffer.alloc(16, 10) }, 'd')
    t.equal(unpack(f.proc.log.head).quorum, false, 'four distinct reports make quorum impossible')
  } finally { f.close() }
  t.end()
})

test('forwarded responses validate complete tuples and settle only their correlated request', (t) => {
  const f = protocol().setup()
  try {
    const sent = f.input('append', { requestId: 'client', items: [Buffer.from('one'), Buffer.from('two')] })
    const request = messages(sent, 'fwd_cmd')[0]
    const valid = { type: 'ack', cid: request.cid, term: 1n, cmdSeq: 1n,
      results: [[0, 42], [1, 'failure', 17, 19, 'shared trace']] }
    const malformed = [
      { results: undefined }, { cmdSeq: -1n }, { cmdSeq: 1 },
      { results: [[0, 42]] }, { results: [[0], [0, 1]] },
      ...[[2, 'bad', 1, null], [1, 5, 1, null], [1, 'bad', 'code', null],
        [1, 'bad', 1, {}], [1, 'bad', 1, null, 5], [1, 'bad', 1, null, 'trace', 'extra']]
        .map((item) => ({ results: [[0, 42], item] })),
    ]
    for (const fields of malformed) {
      t.notOk(isFwdCmdAck({ ...valid, ...fields }, 2))
      t.equal(outputs(f.rpc({ ...valid, ...fields })).filter(({ to }) => to === 'client').length, 0)
      t.ok(f.proc.engine.requests.has(request.cid))
    }
    const result = outputs(f.rpc(valid)).find(({ to }) => to === 'client').message
    t.deepEqual(result.results, valid.results)
    t.notOk(f.proc.engine.requests.has(request.cid))
    t.equal(outputs(f.rpc(valid)).filter(({ to }) => to === 'client').length, 0, 'duplicate ACK ignored')
    const noStack = { ...valid, results: [[0, 42], [1, 'failure', 17, 19]] }
    t.ok(isFwdCmdAck(noStack, 2), 'missing optional stack is valid')

    const later = messages(f.input('append', { requestId: 'later', items: [Buffer.from('x')] }), 'fwd_cmd')[0]
    const timed = f.tick(f.proc.executor.timems + 500)
    t.equal(outputs(timed).find(({ to }) => to === 'client').message.code, Err.APPEND_TIMEOUT)
    t.notOk(f.proc.engine.requests.has(later.cid))
    t.equal(outputs(f.rpc({ ...valid, cid: later.cid })).filter(({ to }) => to === 'client').length, 0)
  } finally { f.close() }
  t.end()
})

test('forwarding preserves state rejections and shared remote error stacks', (t) => {
  const f = protocol()
  try {
    const response = (r, cid) => outputs(r).find(({ message }) => message.cid === cid)?.message
    t.equal(response(f.input('append', { requestId: 'no-leader', items: [Buffer.from('x')] }), 'no-leader').code, Err.NO_LEADER)
    const request = { type: 'fwd_cmd', cid: 'forward', term: 0n, items: [Buffer.from('x')] }
    t.equal(response(f.rpc(request), request.cid).code, Err.NOT_LEADER)
    f.leader()
    t.equal(response(f.rpc(request), request.cid).code, Err.TERM_DIFF)
    f.input('drain', { requestId: 'drain' })
    t.equal(response(f.rpc({ ...request, term: 1n }), request.cid).code, Err.DRAINING)
  } finally { f.close() }
  const receiver = protocol().setup()
  try {
    const request = messages(receiver.input('append', { requestId: 'user', items: [Buffer.from('x')] }), 'fwd_cmd')[0]
    const r = receiver.rpc({ type: 'err', term: 2n, cid: request.cid, message: 'remote failure',
      code: Err.SQLITE_ERROR, sqlCode: 5, stack: 'shared remote trace' })
    const result = outputs(r).find(({ to }) => to === 'client').message
    t.equal(receiver.proc.engine.term, 2n, 'valid higher-term reply advances election')
    t.deepEqual([result.message, result.code, result.sqlCode, result.stack],
      ['remote failure', Err.SQLITE_ERROR, 5, 'shared remote trace'])
  } finally { receiver.close() }
  t.end()
})

test('leadership loss rejects queued work and recovery decides without replay', (t) => {
  for (const outcome of ['agree', 'outside', 'impossible']) {
    const f = protocol().leader()
    try {
      active(f)
      f.input('append', { requestId: 'queued', items: [Buffer.from('must not run')] })
      const lost = f.rpc(append({ term: 2n, seqP: 1n, termP: 1n, commitSeq: 1n }))
      t.deepEqual(outputs(lost).filter(({ to }) => to === 'client').map(({ message }) => message.code),
        [Err.NOT_LEADER, Err.NOT_LEADER])
      const election = f.elect()
      const committed = ack(f, page(election))
      t.equal(messages(committed, 'execute', 'apply').length, 0, 'inherited CMD is not reapplied')
      t.equal(f.proc.engine.active.request, null, 'recovery has no old caller')
      f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest: outcome === 'agree' ? digest : Buffer.alloc(16, 8) })
      if (outcome !== 'agree') f.rpc({ type: 'monster_outcome', cmdSeq: 1n,
        digest: Buffer.alloc(16, outcome === 'outside' ? 8 : 9) }, 'c')
      if (outcome === 'outside') {
        t.equal(f.state().meta.repair_state, 2n)
        t.notOk(f.proc.isOpen)
      } else {
        t.equal(unpack(f.proc.log.head).quorum, outcome === 'agree')
      }
      t.equal(f.state().meta.pending_cmd_seq, 1n, 'only the original CMD remains pending')
    } finally { f.close() }
  }
  t.end()
})

test('SYNC insertion failure discards buffered success; later timeout cannot replace released success', (t) => {
  for (const failure of ['insert', 'timeout', 'fence']) {
    const f = protocol({ electionTimeout: 1000, appendTimeout: 60 }).leader()
    try {
      active(f)
      if (failure !== 'timeout') for (const proc of f.procs) proc.db.exec(failure === 'insert' ? `
        CREATE TRIGGER fail_sync BEFORE INSERT ON raft_log WHEN NEW.seq = 2
        BEGIN SELECT RAISE(ABORT, 'SYNC insert failed'); END
      ` : `
        CREATE TRIGGER fail_fence BEFORE UPDATE OF repair_state ON monsterft_meta
        BEGIN SELECT RAISE(ABORT, 'fence update failed'); END
      `)
      let r = f.rpc({ type: 'monster_outcome', cmdSeq: 1n,
        digest: failure === 'fence' ? Buffer.alloc(16, 8) : digest })
      if (failure === 'fence') r = f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest: Buffer.alloc(16, 8) }, 'c')
      if (failure === 'timeout') {
        t.equal(outputs(r).filter(({ to }) => to === 'client').length, 1)
        const timed = f.tick(f.proc.executor.timems + 60)
        t.equal(outputs(timed).filter(({ to }) => to === 'client').length, 0, 'no second caller settlement')
        t.equal(f.proc.engine.state, 'follower')
        t.ok(f.diagnostics.some(({ type, error }) => type === 'warn' && error.code === Err.APPEND_TIMEOUT))
      } else {
        t.equal(r, undefined)
        t.equal(f.state().meta.repair_state, 0n, 'failed fence not durable')
        t.equal(f.state().meta.pending_cmd_seq, 1n)
        t.ok(f.diagnostics.some(({ error }) => /failed/.test(error.message)))
      }
    } finally { f.close() }
  }
  t.end()
})
