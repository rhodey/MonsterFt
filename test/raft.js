import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { MonsterProc } from '../src/proc.js'
import { openApplyDatabase, readDatabase, readState, stateDigest, databasePaths } from '../src/storage.js'
import * as wire from '../src/wire.js'
import * as Err from '../src/error.js'

function fixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-raft-'))
  const base = path.join(directory, 'node')
  const replies = []
  const procs = ['procA', 'procB', 'procC'].map((role) => {
    const proc = new MonsterProc(role, 'a', ['a', 'b', 'c'], (to, frame) => {
      replies.push(wire.decodeFrame(frame))
    }, base, { electionTimeout: 150, localTimeout: 10_000 })
    proc.on('error', () => {})
    return proc
  })
  let cid = 0
  const request = (proc, message) => {
    message = { cid: `local-${++cid}`, ...message }
    proc.onReceive('gate', wire.encodeFrame(message))
    const reply = replies.find((value) => value.cid === message.cid)
    return reply
  }
  const seed = Buffer.from('10111213202122233031323340414243', 'hex')
  for (const proc of procs) { request(proc, { type: 'hello', seed }) }
  for (const proc of procs) { request(proc, { type: 'open' }) }
  const db2 = openApplyDatabase(base, seed)
  for (const type of ['capture', 'adopt', 'reconcile']) {
    for (const proc of procs) {
      const reply = request(proc, { type })
      if (reply.type !== 'ack') { throw new Error(`startup ${type}: ${reply.message}`) }
    }
  }
  const proc = procs[0]
  const input = (kind, fields = {}) => request(proc, {
    type: 'input', kind, tickCount: proc.tickCount + 1n,
    timems: proc.executor.timems, ...fields,
  })
  const rpc = (message, from = 'b', timems = proc.executor.timems) => input('rpc', {
    from, timems, frame: wire.encodeFrame({ to: 'a', message: { from, ...message } }),
  })
  const outputs = (reply) => (reply.outputs ?? []).map(wire.decodePayload)
  input('start')
  return {
    proc, input, rpc, outputs,
    state: () => readDatabase(databasePaths(base).procA, readState),
    rows: () => [...proc.log.iter()].map((record) => ({ term: record.term, entry: record.entry })),
    elect() {
      input('tick', { timems: 10_000 })
      return rpc({ type: 'vote', term: proc.engine.term, voteGranted: true })
    },
    leader() {
      const election = this.elect()
      const append = outputs(election).find(({ to, message }) => to === 'b' && message.type === 'append').message
      const commit = rpc({ type: 'ack', term: proc.engine.term, cid: append.cid, seq: 0n })
      const execute = outputs(commit).find(({ to }) => to === 'apply').message
      const receipt = { seq: 0n, entry_hash: execute.entryHash, local_digest: Buffer.alloc(16) }
      db2.prepare('UPDATE monsterft_meta_apply SET seq=?, entry_hash=?, local_digest=?')
        .run(receipt.seq, receipt.entry_hash, receipt.local_digest)
      input('apply', { reply: { type: 'ack', cid: execute.cid,
        tickCount: execute.tickCount, receipt, outcomes: [] } })
      return this
    },
    close() {
      for (const member of procs) { member.close() }
      db2.close()
      fs.rmSync(directory, { recursive: true, force: true })
    },
  }
}

const append = (overrides = {}) => ({
  type: 'append', cid: 'replication', term: 1n,
  seqP: -1n, termP: -1n, commitSeq: -1n, ...overrides,
})
const record = (term, payload) => ({ term, entry: Buffer.from(payload) })
const response = (f, reply, cid) => f.outputs(reply).find(({ message }) => message.cid === cid)?.message

test('Raft rejects malformed replication and forwarding before changing durable or runtime state', (t) => {
  for (const leader of [false, true]) {
    const f = fixture()
    try {
      if (leader) { f.leader() }
      const before = stateDigest(f.state())
      const term = f.proc.engine.term
      const role = f.proc.engine.state
      for (const incoming of [term, term + 9n]) {
        const cases = [
          { type: 'append', cid: 'missing-fields', term: incoming, data: [Buffer.from('not replication')] },
          append({ cid: 'buffer-data', term: incoming, data: Buffer.from('not records') }),
          append({ cid: 'bad-term', term: incoming, data: [record(-1n, 'bad')] }),
          append({ cid: 'bad-payload', term: incoming, data: [{ term: 0n, entry: 'bad' }] }),
          append({ cid: 'bad-commit', term: incoming, commitSeq: 0 }),
          { type: 'fwd_cmd', cid: 'bad-command', term: incoming, items: [1] },
        ]
        for (const message of cases) {
          const reply = f.rpc(message)
          t.equal(response(f, reply, message.cid).code, Err.RPC_ILLEGAL, message.cid)
          t.equal(f.proc.engine.term, term, 'term stays unchanged')
          t.equal(f.proc.engine.state, role, 'role stays unchanged')
          t.ok(stateDigest(f.state()).equals(before), 'durable state stays unchanged')
        }
      }
    } finally { f.close() }
  }
  t.end()
})

test('Raft same-term valid APPEND steps down a leader; lower term cannot', (t) => {
  const f = fixture().leader()
  try {
    const term = f.proc.engine.term
    const before = stateDigest(f.state())
    const old = f.rpc(append({ term: term - 1n, seqP: 0n, termP: term, commitSeq: 0n }))
    t.equal(response(f, old, 'replication').code, Err.TERM_DIFF)
    t.equal(f.proc.engine.state, 'leader')
    t.ok(stateDigest(f.state()).equals(before))
    const valid = f.rpc(append({ term, seqP: 0n, termP: term, commitSeq: 0n }))
    t.equal(response(f, valid, 'replication').type, 'ack')
    t.equal(f.proc.engine.state, 'follower')
    t.equal(f.proc.engine.leader, 'b')
    t.equal(f.proc.engine.term, term)
    t.equal(f.proc.log.seq, 0n)
  } finally { f.close() }
  t.end()
})

test('Raft progress events keep captured status and publish one final change per input', (t) => {
  const f = fixture().leader()
  try {
    const oldTerm = f.proc.engine.term
    const term = oldTerm + 1n
    const reply = f.rpc(append({
      term, seqP: 0n, termP: oldTerm, commitSeq: 1n,
      data: [record(oldTerm, ''), record(term, '')],
    }))
    const events = f.outputs(reply).filter(({ to }) => to === 'event').map(({ message }) => message)
    t.deepEqual(events.map(({ type }) => type), ['commit', 'change'],
      'the role transition and commitment produce one final change')
    t.equal(events[0].seq, 1n, 'the commit event still identifies the committed sequence')
    t.deepEqual(events[0].status, {
      state: 'follower', leader: 'b', term, seq: 2n,
      commitSeq: 1n, commitTerm: oldTerm, followers: [],
    }, 'commit reports its actual older term and includes the unapplied tail')
    t.deepEqual(events[1].status, events[0].status, 'change retains the final committed status')
    t.equal(f.outputs(reply).at(-1).message.type, 'change')
    t.ok(events.every(({ status }) => !Object.hasOwn(status, 'appliedSeq')),
      'protocol event snapshots do not carry applied progress')
    t.equal(f.proc.engine.appliedSeq, 1n, 'all snapshots are released after the input finishes')

    const later = f.rpc(append({ term, seqP: 2n, termP: term, commitSeq: 2n }))
    const laterEvents = f.outputs(later).filter(({ to }) => to === 'event').map(({ message }) => message)
    t.deepEqual(laterEvents.map(({ type }) => type), ['commit', 'change'])
    t.equal(laterEvents[0].status.commitTerm, term, 'the next commit uses its own entry term')
    t.equal(f.proc.engine.appliedSeq, 2n, 'the later no-op still advances the internal applied checkpoint')
    t.equal(events[0].status.commitTerm, oldTerm, 'earlier outputs retain their captured term')
    t.equal(events[0].status.commitSeq, 1n, 'earlier outputs retain their captured commit sequence')
  } finally { f.close() }
  t.end()
})

test('Raft reconciliation compares both terms and payload bytes', (t) => {
  for (const changed of [record(2n, 'same'), record(1n, 'different')]) {
    const f = fixture()
    try {
      const original = [record(1n, 'prefix'), record(1n, 'same'), record(1n, 'old tail')]
      t.equal(response(f, f.rpc(append({ data: original })), 'replication').type, 'ack')
      const replacement = [original[0], changed, record(3n, 'new tail')]
      const reply = f.rpc(append({ term: 3n, data: replacement }))
      t.equal(response(f, reply, 'replication').type, 'ack')
      t.deepEqual(f.rows(), replacement, 'only matching term-and-payload prefix survives')
      t.equal(f.proc.engine.term, 3n)
      t.equal(f.state().election.term, 3n, 'higher term and replacement committed together')
    } finally { f.close() }
  }
  t.end()
})

test('Raft cannot replace committed history even with identical payload bytes', (t) => {
  const f = fixture().leader()
  try {
    const before = f.state()
    const reply = f.rpc(append({ term: 2n, data: [{ term: 2n, entry: Buffer.alloc(0) }] }))
    t.equal(reply, undefined, 'fatal replication failure sends no local reply')
    t.equal(f.proc._fatalError.code, Err.RAFT_ILLEGAL)
    t.notOk(f.proc.isOpen)
    t.ok(stateDigest(f.state()).equals(stateDigest(before)),
      'fatal input rolls back its election change and leaves committed history intact')
  } finally { f.close() }
  t.end()
})

test('Raft malformed ACKs leave the pending request and election state unchanged', (t) => {
  for (const heartbeat of [false, true]) {
    const f = fixture()
    try {
      const election = f.elect()
      const page = f.outputs(election).find(({ to, message }) => to === 'b' && message.type === 'append').message
      const request = heartbeat
        ? f.outputs(f.input('tick', { timems: f.proc.executor.timems + 45 }))
          .find(({ to, message }) => to === 'b' && message.type === 'append' && !message.data).message
        : page
      const waiting = f.proc.engine.requests.get(request.cid)
      const before = stateDigest(f.state())
      for (const fields of [
        { seq: undefined }, { seq: -1n }, { seq: 0 }, { seq: 9_223_372_036_854_775_808n },
        { term: '50' }, { term: 9_223_372_036_854_775_808n }, { term: 0n },
        { cid: undefined }, { cid: '' }, { cid: 1 },
      ]) {
        f.rpc({ type: 'ack', cid: request.cid, term: 50n, seq: 0n, ...fields })
        t.equal(f.proc.engine.term, 1n)
        t.equal(f.proc.engine.commitSeq, -1n)
        t.equal(f.proc.engine.requests.get(request.cid), waiting, 'invalid ACK preserves the request')
        t.ok(f.proc.executor.timers.has(waiting.timer), 'invalid ACK preserves its deadline')
        t.ok(stateDigest(f.state()).equals(before), 'invalid ACK leaves durable state unchanged')
      }
      f.rpc({ type: 'ack', cid: request.cid, term: 50n, seq: 0n }, 'c')
      t.equal(f.proc.engine.term, 1n, 'wrong sender cannot satisfy the request')
      let reply = f.rpc({ type: 'ack', cid: request.cid, term: 1n, seq: 0n })
      t.notOk(f.proc.engine.requests.has(request.cid), 'valid ACK settles the request')
      if (heartbeat) {
        t.equal(f.proc.engine.commitSeq, -1n, 'heartbeat ACK does not establish replication')
        reply = f.rpc({ type: 'ack', cid: page.cid, term: 1n, seq: 0n })
      }
      t.equal(f.proc.engine.commitSeq, 0n, 'valid replication ACK needs no command results')
      t.ok(f.outputs(reply).some(({ to }) => to === 'apply'), 'valid ACK enables setup')
    } finally { f.close() }
  }
  t.end()
})

test('Raft head hints accelerate only matching-term backtracking', (t) => {
  for (const [hint, expected] of [
    [undefined, 7n], [null, 7n], ['2', 7n], [9_223_372_036_854_775_808n, 7n],
    [100n, 7n], [2n, 2n], [-1n, -1n],
  ]) {
    const f = fixture()
    try {
      f.rpc(append({ data: Array.from({ length: 9 }, () => record(1n, '')) }))
      const election = f.elect()
      const request = f.outputs(election).find(({ to, message }) => to === 'b' && message.type === 'append').message
      const error = { type: 'err', cid: request.cid, term: 2n,
        code: Err.REPL_BACKTRACK, message: 'missing predecessor', sqlCode: null }
      if (hint !== undefined) { error.logSeq = hint }
      const reply = f.rpc(error)
      const retry = f.outputs(reply).find(({ to, message }) => to === 'b' && message.type === 'append').message
      t.equal(retry.seqP, expected, `hint ${String(hint)}`)
      t.equal(f.proc.engine.replication.get('b').matchIndex, -1n, 'hint does not establish replicated history')
    } finally { f.close() }
  }
  const f = fixture()
  try {
    f.rpc(append({ data: Array.from({ length: 9 }, () => record(1n, '')) }))
    const election = f.elect()
    const request = f.outputs(election).find(({ to, message }) => to === 'b' && message.type === 'append').message
    const reply = f.rpc({ type: 'err', cid: request.cid, term: 1n,
      code: Err.REPL_BACKTRACK, message: 'old rejection', logSeq: -1n })
    t.notOk(f.outputs(reply).some(({ message }) => message.type === 'append'), 'stale error keeps normal delayed retry')
    t.equal(f.proc.engine.replication.get('b').nextIndex, 9n, 'stale hint cannot lower position')
  } finally { f.close() }
  t.end()
})

test('Raft denied higher-term vote preserves the existing election deadline', (t) => {
  const f = fixture()
  try {
    f.rpc(append({ term: 5n, data: [record(5n, 'retained')] }))
    const before = f.proc.executor.timers.get(f.proc.engine.electionTimer).due
    const denied = f.rpc({ type: 'vote_request', term: 6n, termP: 0n, seqP: 0n }, 'c', before - 1)
    const vote = f.outputs(denied).find(({ message }) => message.type === 'vote').message
    t.notOk(vote.voteGranted)
    t.equal(f.proc.engine.term, 6n, 'higher election term is persisted')
    t.equal(f.proc.executor.timers.get(f.proc.engine.electionTimer).due, before,
      'denial does not grant a new election delay')
    const expired = f.input('tick', { timems: before })
    t.equal(f.proc.engine.term, 7n)
    t.equal(f.proc.engine.state, 'candidate')
    t.ok(f.outputs(expired).some(({ message }) => message.type === 'vote_request'),
      'original deadline starts the next election')
  } finally { f.close() }
  t.end()
})
