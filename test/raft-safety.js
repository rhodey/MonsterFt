import test from 'tape'
import { protocol, append, record, cmd, messages, outputs } from './fixtures/protocol.js'
import { stateDigest } from '../src/storage.js'
import * as Err from '../src/error.js'

const max = 9_223_372_036_854_775_807n
const reply = (r, cid='page') => outputs(r).find(({ message }) => message.cid === cid)?.message
const page = (r, to='b') => messages(r, 'append', to).find((msg) => msg.data)
const ack = (f, msg, from='b') => f.rpc({ type: 'ack', term: msg.term, cid: msg.cid, seq: msg.seqP + 1n }, from)

test('votes are durable before publication and survive restart', (t) => {
  const f = protocol()
  try {
    let observed = 0
    f.onSend = (proc, r) => {
      if (proc.role !== 'procA') return
      for (const msg of messages(r, 'vote')) {
        if (!msg.voteGranted) continue
        const saved = f.state().election
        t.equal(saved.term, msg.term)
        t.equal(saved.votedFor, 'b', 'durable vote precedes grant')
        observed++
      }
    }
    const vote = { type: 'vote_request', term: 1n, termP: -1n, seqP: -1n }
    t.ok(messages(f.rpc(vote), 'vote')[0].voteGranted)
    t.notOk(messages(f.rpc(vote, 'c'), 'vote')[0].voteGranted, 'competing candidate denied')
    t.ok(messages(f.rpc(vote), 'vote')[0].voteGranted, 'duplicate grant remains the same vote')
    f.reopen()
    t.equal(f.proc.engine.votedFor, 'b')
    t.notOk(messages(f.rpc(vote, 'c'), 'vote')[0].voteGranted, 'restart cannot grant second vote')
    t.equal(observed, 2)
    f.onSend = (proc, r) => {
      if (proc.role === 'procA' && messages(r, 'vote_request').length) {
        t.equal(f.state().election.votedFor, 'a', 'self vote is durable before requests')
        t.equal(f.state().election.term, f.proc.engine.term)
      }
    }
    f.elect()
  } finally { f.close() }
  t.end()
})

test('log freshness, unique votes, and configured quorum govern elections', (t) => {
  for (const [termP, seqP, granted] of [[1n, 9n, false], [2n, 0n, false], [2n, 1n, true], [3n, 0n, true]]) {
    const f = protocol()
    try {
      f.rpc(append({ term: 2n, data: [record(2n), record(2n)] }))
      const r = f.rpc({ type: 'vote_request', term: 4n, termP, seqP }, 'c')
      t.equal(messages(r, 'vote')[0].voteGranted, granted, `${termP}/${seqP} log freshness`)
    } finally { f.close() }
  }
  const f = protocol({ quorum: 4 }, ['a', 'b', 'c', 'd', 'e'])
  try {
    f.tick(1000)
    for (const from of ['b', 'b', 'c']) f.rpc({ type: 'vote', term: 1n, voteGranted: true }, from)
    t.equal(f.proc.engine.state, 'candidate', 'self plus two unique peers cannot satisfy quorum four')
    f.rpc({ type: 'vote', term: 1n, voteGranted: true }, 'd')
    t.equal(f.proc.engine.state, 'leader')
  } finally { f.close() }
  t.end()
})

test('a returning stale candidate cannot win votes from a more recent log', (t) => {
  const a = protocol().setup()
  const b = protocol({}, ['a', 'b', 'c'], 'b').setup()
  try {
    b.rpc(append({ seqP: 0n, termP: 1n, data: [record(1n), record(1n)] }), 'c')
    a.reopen()
    const election = a.tick(a.proc.executor.timers.get(a.proc.engine.electionTimer).due)
    const request = messages(election, 'vote_request', 'b')[0]
    const denied = messages(b.rpc(request, 'a'), 'vote', 'a')[0]
    t.equal(denied.voteGranted, false)
    a.rpc(denied, 'b')
    t.equal(a.proc.engine.state, 'candidate', 'higher election term cannot compensate for stale history')
    const fresh = b.tick(b.proc.executor.timers.get(b.proc.engine.electionTimer).due)
    const grant = messages(a.rpc(messages(fresh, 'vote_request', 'a')[0], 'b'), 'vote', 'b')[0]
    t.equal(grant.voteGranted, true)
    b.rpc(grant, 'a')
    t.equal(b.proc.engine.state, 'leader', 'the up-to-date candidate can establish leadership')
    t.equal(a.proc.engine.state, 'follower')
  } finally { a.close(); b.close() }
  t.end()
})

test('election SQL failures release no vote or election output', (t) => {
  for (const action of ['self', 'grant', 'higher append']) {
    const f = protocol()
    try {
      const before = stateDigest(f.state())
      for (const proc of f.procs) proc.db.exec(`
        CREATE TRIGGER reject_vote BEFORE UPDATE ON raft_election
        BEGIN SELECT RAISE(ABORT, 'vote write failed'); END
      `)
      const r = action === 'self' ? f.tick(1000) : action === 'grant'
        ? f.rpc({ type: 'vote_request', term: 5n, termP: -1n, seqP: -1n })
        : f.rpc(append({ term: 5n, data: [record(5n)] }))
      t.equal(r, undefined, `${action} sends no completion`)
      t.ok(stateDigest(f.state()).equals(before), 'election/log state rolls back')
      t.match(f.diagnostics.find(({ type }) => type === 'error').error.message, /vote write failed/)
    } finally { f.close() }
  }
  t.end()
})

test('old-term pages cannot commit until a current-term entry reaches quorum', (t) => {
  const f = protocol({ rpcMax: 1 })
  try {
    f.rpc(append({ data: [record(), record(), record()] }))
    const election = f.elect()
    const first = page(election)
    let r = f.rpc({ type: 'err', cid: first.cid, term: first.term,
      code: Err.REPL_BACKTRACK, message: 'empty', logSeq: -1n })
    f.input('append', { requestId: 'queued', items: [Buffer.from('later')] })
    for (let i = 0; i < 3; i++) {
      r = ack(f, page(r))
      t.equal(f.proc.engine.commitSeq, -1n, 'old-term replica count does not commit')
      t.equal(f.proc.engine.active, null, 'queued command waits for election commitment')
    }
    r = ack(f, page(r))
    t.equal(f.proc.engine.commitSeq, 3n)
    const applied = f.complete(messages(r, 'execute', 'apply')[0])
    t.equal(f.proc.engine.active.cmdSeq, 4n, 'command follows setup and committed election no-op')
    t.ok(page(applied))
  } finally { f.close() }
  t.end()
})

test('election no-op retries stay with their term and stop after commitment', (t) => {
  for (const pending of [false, true]) {
    for (const finish of ['commit', 'stepdown', 'retry']) {
      const f = protocol({ electionTimeout: 1000, appendTimeout: 60 })
      try {
        if (pending) {
          f.setup()
          const r = f.rpc(append({ seqP: 0n, termP: 1n, data: [record(1n, cmd())], commitSeq: 1n }))
          f.complete(messages(r, 'execute', 'apply')[0], [[0, 'ok']], Buffer.alloc(16, 7))
        }
        const r = f.elect()
        const head = f.proc.log.seq
        const now = f.proc.executor.timems
        if (finish === 'commit') ack(f, page(r))
        f.tick(now + 60)
        if (finish === 'stepdown') f.rpc(append({ term: f.proc.engine.term + 1n }))
        f.tick(now + 161)
        t.equal(f.proc.log.seq, finish === 'retry' ? head + 1n : head,
          `${pending ? 'pending CMD: ' : ''}${finish}`)
        if (pending) t.equal(f.proc.engine.pendingCommand.cmdSeq, 1n, 'no-op cannot replace pending CMD')
      } finally { f.close() }
    }
  }
  t.end()
})

test('duplicate pages preserve suffixes and commit only verified history', (t) => {
  const f = protocol().setup()
  try {
    const original = [record(), record(2n), record(2n)]
    f.rpc(append({ term: 3n, seqP: 0n, termP: 1n, data: original }))
    const before = f.rows()
    t.equal(reply(f.rpc(append({ term: 3n, seqP: 0n, termP: 1n, data: original }))).type, 'ack')
    t.deepEqual(f.rows(), before, 'fully duplicated suffix remains intact')
    f.rpc(append({ term: 3n, seqP: 0n, termP: 1n, data: original.slice(0, 1), commitSeq: 3n }))
    t.deepEqual(f.rows(), before, 'short duplicate does not truncate remaining history')
    t.equal(f.proc.engine.commitSeq, 1n, 'duplicate commit limited to verified page')
    f.rpc(append({ term: 3n, seqP: 1n, termP: 1n, commitSeq: 3n }))
    t.equal(f.proc.engine.commitSeq, 1n, 'nonmatching-head heartbeat cannot commit suffix')
    f.rpc(append({ term: 3n, seqP: 1n, termP: 1n, data: [record(3n), record(3n)], commitSeq: 3n }))
    t.equal(f.proc.engine.appliedSeq, 3n)
    f.rpc(append({ term: 3n, seqP: 3n, termP: 3n, commitSeq: 100n }))
    t.equal(f.proc.engine.commitSeq, 3n, 'matching heartbeat capped at predecessor')
  } finally { f.close() }
  t.end()
})

test('replacement append failure rolls back trim and election together', (t) => {
  const f = protocol().setup()
  try {
    f.rpc(append({ seqP: 0n, termP: 1n, data: [record(), record()] }))
    const before = f.rows()
    for (const proc of f.procs) proc.db.exec(`
      CREATE TRIGGER reject_replace BEFORE INSERT ON raft_log WHEN NEW.term = 2
      BEGIN SELECT RAISE(ABORT, 'replacement failed'); END
    `)
    t.equal(f.rpc(append({ term: 2n, seqP: 0n, termP: 1n, data: [record(2n)] })), undefined)
    t.deepEqual(f.rows(), before, 'trim is rolled back with replacement')
    t.equal(f.state().election.term, 1n, 'higher election term also rolls back')
    f.reopen()
    t.deepEqual(f.rows(), before, 'reopening preserves the previous suffix')
  } finally { f.close() }
  t.end()
})

test('Raft read failures abort replication, commitment, and application inputs', (t) => {
  for (const stage of ['predecessor', 'commit', 'apply']) {
    const f = protocol().leader()
    try {
      const before = stateDigest(f.state())
      const request = f.input('append', { requestId: 'command', items: [Buffer.from('one')] })
      for (const proc of f.procs) {
        const all = proc.log._statements.range.all.bind(proc.log._statements.range)
        let reads = 0
        proc.log._statements.range.all = (...args) => {
          if (++reads === (stage === 'apply' ? 2 : 1)) throw new Error(`${stage} read failed`)
          return all(...args)
        }
      }
      const r = stage === 'predecessor'
        ? f.rpc({ type: 'err', term: f.proc.engine.term, cid: page(request).cid,
          code: Err.REPL_BACKTRACK, message: 'retry', logSeq: 0n })
        : ack(f, page(request))
      t.equal(r, undefined, stage)
      t.ok(f.diagnostics.some(({ error }) => error.message.includes(`${stage} read failed`)))
      t.equal(f.state().meta.applied_seq, 0n, 'no successful checkpoint escapes')
      f.reopen()
      t.equal(f.proc.engine.appliedSeq, 0n)
      t.notOk(stateDigest(f.state()).equals(before), 'durable uncommitted command remains available')
    } finally { f.close() }
  }
  t.end()
})

test('genesis retry is attempted once and explicit rejection cancels it', (t) => {
  const f = protocol()
  try {
    f.rpc(append({ data: [record(), record()] }))
    const first = page(f.elect())
    const r = f.rpc({ type: 'err', cid: first.cid, term: first.term,
      code: Err.REPL_BACKTRACK, message: 'missing', logSeq: -1n })
    const root = page(r)
    t.equal(root.seqP, -1n)
    t.deepEqual(f.diagnostics.map(({ role, type, error }) => [role, type, error.code]),
      f.procs.map(({ role }) => [role, 'warn', Err.REPL_BACKTRACK]),
      'each Proc warns about ordinary backtracking before retrying')
    const rejected = f.rpc({ type: 'err', cid: root.cid, term: root.term,
      code: Err.REPL_BACKTRACK, message: 'genesis rejected', logSeq: -1n })
    t.equal(page(rejected), undefined)
    t.ok(f.proc.engine.replication.get('b').cancelled)
    t.equal(f.proc.engine.replication.get('b').matchIndex, -1n)
    t.ok(f.diagnostics.some(({ error }) => error.code === Err.RAFT_ILLEGAL))
  } finally { f.close() }
  t.end()
})

test('replication validation covers overflow, later bad records, and unrelated traffic', (t) => {
  const f = protocol().setup()
  try {
    const before = stateDigest(f.state())
    for (const fields of [
      { seqP: max, data: [record()] }, { seqP: -2n }, { seqP: 0 }, { termP: max + 1n },
      { termP: -2n }, { commitSeq: max + 1n }, { data: [record(), record(-1n)] },
      { data: [record(), { term: 1n, entry: 'text' }] }, { data: [] },
    ]) {
      const r = f.rpc(append({ term: 10n, ...fields }))
      t.equal(reply(r).code, Err.RPC_ILLEGAL)
      t.ok(stateDigest(f.state()).equals(before))
    }
    for (const from of ['unknown', 'a']) {
      const other = protocol().setup()
      try {
        const saved = stateDigest(other.state())
        other.rpc(append({ term: 50n, data: [record(50n)] }), from)
        t.ok(stateDigest(other.state()).equals(saved), `${from} cannot change durable state`)
        t.equal(other.proc._fatalError.code, Err.RPC_ILLEGAL, 'invalid Gate input stops the Proc')
      } finally { other.close() }
    }
    for (const message of [
      { type: 'ack', cid: 'expired', term: 50n, seq: 1n },
      { type: 'err', cid: 'unrelated', term: 50n, message: 'error' },
      { type: 'vote', term: 50n, voteGranted: 'yes' },
      { type: 'vote_request', term: '50', termP: 0n, seqP: 0n },
    ]) {
      f.rpc(message)
      t.ok(stateDigest(f.state()).equals(before), 'invalid/unrelated response cannot advance term')
    }
    const r = f.rpc(append({ term: max, seqP: 0n, termP: 1n, data: [record(max)] }))
    t.equal(reply(r).type, 'ack', 'maximum term is valid')
    t.equal(f.proc.log.term, max)
  } finally { f.close() }
  t.end()
})
