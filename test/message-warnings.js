import test from 'tape'
import { setImmediate } from 'node:timers/promises'
import { protocol, append, messages, outputs } from './fixtures/protocol.js'
import { checksum } from '../src/wire.js'
import * as Err from '../src/error.js'

const max = 9_223_372_036_854_775_807n
const page = (reply) => messages(reply, 'append', 'b').find((message) => message.data)
const warnings = (f) => f.diagnostics.filter(({ type }) => type === 'warn')

function warned(t, f, before, type, field, code = Err.RPC_ILLEGAL) {
  const found = f.diagnostics.slice(before)
  t.deepEqual(found.map(({ role, type }) => [role, type]),
    ['procA', 'procB', 'procC'].map((role) => [role, 'warn']), 'each Proc warns locally once')
  for (const { error } of found) {
    t.equal(error.code, code)
    t.ok(error.message.includes('from b') && error.message.includes(type) &&
      (!field || error.message.includes(field)), `warning identifies sender, ${type}, and ${field ?? 'type'}`)
  }
  t.ok(f.procs.every((proc) => proc.isOpen), 'warnings keep every Proc open')
}

function waiting(t, f, request, snapshot) {
  t.ok(f.procs.every((proc) => proc.engine.requests.has(request.cid)), 'request remains pending on every Proc')
  t.ok(f.procs.every((proc) => proc.executor.timers.has(proc.engine.requests.get(request.cid)?.timer)),
    'request deadlines remain active')
  t.deepEqual(f.state(), snapshot, 'malformed reply leaves durable state unchanged')
}

test('malformed remote ack_err headers warn without settling or advancing a request', (t) => {
  const f = protocol()
  try {
    const request = page(f.elect())
    const reply = { type: 'ack', cid: request.cid, term: request.term, seq: 0n }
    const snapshot = f.state()
    for (const [fields, field] of [
      [{ cid: undefined }, 'cid'], [{ cid: '' }, 'cid'], [{ cid: 7 }, 'cid'],
      [{ term: undefined }, 'term'], [{ term: 1 }, 'term'], [{ term: -1n }, 'term'],
      [{ type: 'err', term: max + 1n }, 'term'], [{ type: 'err', cid: '' }, 'cid'],
    ]) {
      const before = f.diagnostics.length
      const result = f.rpc({ ...reply, ...fields })
      warned(t, f, before, fields.type ?? 'ack', field)
      waiting(t, f, request, snapshot)
      t.equal(messages(result, 'err', 'b').length, 0, 'no ERR is sent in response to a malformed reply')
    }
    const before = warnings(f).length
    f.rpc({ ...reply, cid: 'unmatched', seq: 'bad' })
    f.rpc({ ...reply, seq: 'bad' }, 'c')
    t.equal(warnings(f).length, before, 'unmatched and wrong-peer replies remain quiet')
    waiting(t, f, request, snapshot)
    t.equal(messages(f.rpc(reply), 'execute', 'apply').length, 1, 'later valid reply commits and starts setup')
    t.ok(f.procs.every((proc) => !proc.engine.requests.has(request.cid)), 'valid reply settles every request')
    const after = warnings(f).length
    f.rpc(reply)
    t.equal(warnings(f).length, after, 'duplicate reply stays quiet')
  } finally { f.close() }
  t.end()
})

test('malformed page and heartbeat ACKs warn while valid stale ACKs remain quiet', (t) => {
  for (const heartbeat of [false, true]) {
    const f = protocol()
    try {
      let request
      if (heartbeat) {
        f.leader()
        const due = f.proc.executor.timers.get(f.proc.engine.heartbeatTimer).due
        request = messages(f.tick(due), 'append', 'b').find((message) => !message.data)
      } else { request = page(f.elect()) }
      const reply = { type: 'ack', cid: request.cid, term: request.term, seq: request.seqP + 1n }
      const snapshot = f.state()
      for (const seq of [undefined, -1n, max + 1n, 0, '0']) {
        const before = f.diagnostics.length
        f.rpc({ ...reply, term: request.term + 1n, seq })
        warned(t, f, before, 'ack', 'seq')
        waiting(t, f, request, snapshot)
        t.equal(f.proc.engine.term, request.term, 'invalid body cannot advance term')
      }
      const before = warnings(f).length
      f.rpc({ ...reply, term: request.term - 1n })
      t.equal(warnings(f).length, before, 'structurally valid lower-term ACK stays quiet')
      waiting(t, f, request, snapshot)
      f.rpc(reply)
      t.ok(f.procs.every((proc) => !proc.engine.requests.has(request.cid)), 'valid ACK still settles the request')
      t.equal(warnings(f).length, before)
    } finally { f.close() }
  }
  t.end()
})

test('malformed forwarded ACKs warn and preserve the caller until a valid delayed result', async (t) => {
  const f = protocol().setup()
  try {
    const sent = f.input('append', { clientId: 'client', items: [Buffer.from('one'), Buffer.from('two')] })
    const request = messages(sent, 'fwd_cmd', 'b')[0]
    const reply = { type: 'ack', cid: request.cid, term: request.term, cmdSeq: 1n,
      results: [[0, 'one'], [1, 'failure', 17, 19, 'original trace']] }
    const snapshot = f.state()
    for (const [fields, field, code] of [
      [{ cmdSeq: -1n }, 'cmdSeq'], [{ results: undefined }, 'results'],
      [{ results: [[0, 'one']] }, 'results', Err.MONSTER_ILLEGAL],
      [{ results: [[0], [0, 'two']] }, 'results'],
      [{ results: [[0, 'one'], [1, 'failure', 17, 19, 42]] }, 'results'],
    ]) {
      await setImmediate()
      const before = f.diagnostics.length
      const result = f.rpc({ ...reply, term: request.term + 1n, ...fields })
      warned(t, f, before, 'ack', field, code)
      waiting(t, f, request, snapshot)
      t.equal(outputs(result).filter(({ to, message }) =>
        to === 'gate' && ['ack', 'err'].includes(message.type)).length, 0, 'invalid result does not reject or settle caller')
    }
    await setImmediate()
    const before = warnings(f).length
    const result = outputs(f.rpc({ ...reply, term: request.term - 1n })).find(({ to, message }) =>
      to === 'gate' && ['ack', 'err'].includes(message.type))
    t.deepEqual(result.message.results, reply.results, 'valid lower-term forwarded ACK preserves values, codes, and stack')
    t.equal(result.message.clientId, 'client')
    t.equal(warnings(f).length, before)
    t.ok(f.procs.every((proc) => !proc.engine.requests.has(request.cid)))
    f.rpc(reply)
    t.equal(warnings(f).length, before, 'late duplicate result stays quiet')
  } finally { f.close() }
  t.end()
})

test('valid lower-term page and heartbeat ERR replies retain term-difference handling', (t) => {
  for (const heartbeat of [false, true]) {
    const f = protocol()
    try {
      let request
      if (heartbeat) {
        f.leader()
        const due = f.proc.executor.timers.get(f.proc.engine.heartbeatTimer).due
        request = messages(f.tick(due), 'append', 'b').find((message) => !message.data)
      } else { request = page(f.elect()) }
      const timer = f.proc.engine.requests.get(request.cid).timer
      const snapshot = f.state()
      const before = f.diagnostics.length
      f.rpc({ type: 'err', cid: request.cid, term: request.term - 1n,
        message: 'remote failure', code: Err.NOT_LEADER })
      t.deepEqual(f.diagnostics.slice(before).map(({ role, type, error }) => [role, type, error.code]),
        ['procA', 'procB', 'procC'].map((role) => [role, 'warn', Err.TERM_DIFF]),
        'valid error follows the existing term-difference warning path')
      t.ok(f.procs.every((proc) => !proc.engine.requests.has(request.cid)), 'ERR settles the matching request')
      t.notOk(f.proc.executor.timers.has(timer), 'settled request deadline is cancelled')
      t.deepEqual(f.state(), snapshot, 'lower term does not change durable state')
      t.ok(f.procs.every((proc) => proc.isOpen))
    } finally { f.close() }
  }
  t.end()
})

test('malformed vote requests warn and retain negative vote responses', (t) => {
  const f = protocol().setup()
  try {
    const snapshot = f.state()
    for (const [fields, field] of [
      [{ term: '2' }, 'term'], [{ termP: -2n }, 'termP'], [{ seqP: undefined }, 'seqP'],
    ]) {
      const before = f.diagnostics.length
      const completions = new Map()
      f.onSend = (proc, reply) => completions.set(proc.role, reply)
      const result = f.rpc({ type: 'vote_request', term: 2n, termP: 1n, seqP: 0n, ...fields })
      f.onSend = null
      warned(t, f, before, 'vote_request', field)
      t.deepEqual(messages(result, 'vote', 'b'), [{ type: 'vote', from: 'a', term: 1n, voteGranted: false }])
      for (const role of ['procB', 'procC']) {
        const expected = result.outputs.map((output) => {
          const hash = checksum(output)
          return role === 'procB' ? hash.subarray(0, 8) : hash.subarray(8)
        })
        t.deepEqual(completions.get(role).outputs, expected, `${role} agrees on negative vote outputs`)
      }
      t.deepEqual(f.state(), snapshot, 'invalid request cannot change persisted term or vote')
    }
    const before = warnings(f).length
    const denied = f.rpc({ type: 'vote_request', term: 0n, termP: -1n, seqP: -1n })
    t.equal(messages(denied, 'vote', 'b')[0].voteGranted, false)
    t.equal(warnings(f).length, before, 'ordinary stale request denial stays quiet')
    const granted = f.rpc({ type: 'vote_request', term: 2n, termP: 1n, seqP: 0n })
    t.equal(messages(granted, 'vote', 'b')[0].voteGranted, true, 'later valid request can obtain a vote')
    t.equal(warnings(f).length, before)
  } finally { f.close() }
  t.end()
})

test('malformed votes warn without changing election progress', (t) => {
  const f = protocol()
  try {
    f.tick(f.proc.executor.timers.get(f.proc.engine.electionTimer).due)
    const term = f.proc.engine.term
    const snapshot = f.state()
    for (const [fields, field] of [
      [{ term: max + 1n }, 'term'], [{ voteGranted: 'yes' }, 'voteGranted'],
    ]) {
      const before = f.diagnostics.length
      f.rpc({ type: 'vote', term: term + 1n, voteGranted: true, ...fields })
      warned(t, f, before, 'vote', field)
      t.equal(f.proc.engine.state, 'candidate')
      t.deepEqual([...f.proc.engine.votes], ['a'])
      t.deepEqual(f.state(), snapshot)
    }
    const before = warnings(f).length
    f.rpc({ type: 'vote', term: term - 1n, voteGranted: true })
    f.rpc({ type: 'vote', term, voteGranted: false })
    t.equal(warnings(f).length, before, 'valid stale and denied votes stay quiet')
    f.rpc({ type: 'vote', term, voteGranted: true })
    t.equal(f.proc.engine.state, 'leader', 'later valid vote elects the candidate')
    f.rpc({ type: 'vote', term, voteGranted: true })
    t.equal(warnings(f).length, before, 'duplicate vote after election stays quiet')
  } finally { f.close() }
  t.end()
})

test('requests without usable CIDs and unknown message types warn without replies or state changes', (t) => {
  const f = protocol().setup()
  try {
    const snapshot = f.state()
    for (const type of ['append', 'fwd_cmd']) {
      for (const cid of [undefined, '', 7]) {
        const before = f.diagnostics.length
        const result = f.rpc(type === 'append' ? append({ cid, term: 100n }) :
          { type, cid, term: 100n, items: [Buffer.from('one')] })
        warned(t, f, before, type, 'cid')
        t.equal(outputs(result).filter(({ to }) => to === 'b').length, 0, 'no uncorrelatable reply is sent')
        t.deepEqual(f.state(), snapshot)
      }
    }
    const before = f.diagnostics.length
    const result = f.rpc({ type: 'future_protocol', cid: 'unknown', term: 100n })
    warned(t, f, before, 'future_protocol')
    t.equal(outputs(result).filter(({ to }) => to === 'b').length, 0, 'unknown type receives no generic ERR')
    t.deepEqual(f.state(), snapshot)
    t.equal(messages(f.rpc(append({ seqP: 0n, termP: 1n })), 'ack', 'b').length, 1,
      'ordinary traffic still succeeds after warnings')
  } finally { f.close() }
  t.end()
})

test('Monster messages retain the inherited sender and stopped guards', (t) => {
  const f = protocol().setup()
  try {
    const engine = f.proc.engine
    const effects = []
    engine.output = (to, message) => effects.push({ to, message })
    engine.queueLocalEvent = (type, error) => effects.push({ type, error })
    const receive = (from) => f.proc.executor.run(f.proc.executor.timems, () => engine.receive(from, {
      type: 'fwd_cmd', cid: 'guarded-forward', term: engine.term, items: [Buffer.from('one')],
    }))
    for (const from of [engine.id, 'nonmember']) {
      receive(from)
      t.deepEqual(effects, [], `${from}: ignored without a reply or warning`)
    }
    receive('b')
    t.equal(effects[0]?.message.code, Err.NOT_LEADER, 'an eligible member reaches Monster forwarding')
    effects.length = 0
    engine.stop()
    receive('b')
    t.deepEqual(effects, [], 'stopped engine ignores Monster messages without a reply or warning')
  } finally { f.close() }
  t.end()
})
