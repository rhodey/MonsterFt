import test from 'tape'
import {
  APPEND_TIMEOUT,
  APPLY_ERROR,
  REPL_BACKTRACK,
  REPL_FORGOT,
  ErrorWithCode,
  NOT_COMMIT,
  NOT_LEADER,
  RAFT_ILLEGAL,
} from '../src/error.js'
import { RaftNode as ProductionRaftNode } from '../src/index.js'
import { sleep, comms, connect, reset, open, close, ready, databasePath } from './util.js'
import { leaders, followers, logFixture } from './util.js'

let injectedLogId = 0
class TestRaftNode extends ProductionRaftNode {
  constructor(id, nodes, send, log, opts) {
    const file = typeof log?.path === 'string' && log.path.length > 0
      ? log.path
      : databasePath(`16-injected-log-${process.pid}-${++injectedLogId}`)
    super(id, nodes, send, file, opts)
    this.log = log
  }
}
const RaftNode = TestRaftNode

const ids = ['1', '2', '3']

const rejects = async (t, promise, pattern, name) => {
  try {
    await promise
    t.fail(name)
  } catch (err) {
    t.ok(pattern.test(err.message), name)
  }
}

const rejection = async (promise) => {
  try {
    await promise
    return null
  } catch (err) {
    return err
  }
}

const entry = (term, data) => {
  const prefix = Buffer.alloc(8)
  prefix.writeBigUInt64LE(term)
  return Buffer.concat([prefix, Buffer.from(data)])
}

const applyNull = (node, bufs) => bufs.map(() => null)

const waitFor = async (fn, ms=1_000) => {
  const end = Date.now() + ms
  while (!fn()) {
    if (Date.now() >= end) { throw new Error('waitFor timeout') }
    await sleep(5)
  }
}

test('vote observes a directly persisted append before comparing logs', async (t) => {
  const fixture = logFixture(t, 'node-append-before-vote')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  log.append(entry(0n, 'zero'))
  node.leader = '2'

  await node.onReceive('2', {
    type: 'append',
    term: 0n,
    termP: 0n,
    seqP: 0n,
    commitSeq: -1n,
    cid: 'first-append',
    data: [entry(0n, 'one')],
  })
  await node.onReceive('3', {
    type: 'vote_request',
    term: 1n,
    termP: 0n,
    seqP: 0n,
  })

  await node.onReceive('2', {
    type: 'append',
    term: 0n,
    termP: 0n,
    seqP: 0n,
    commitSeq: -1n,
    cid: 'late-old-append',
    data: [entry(0n, 'late')],
  })

  await waitFor(() => sent.some(([, msg]) => msg.cid === 'first-append'))

  const appendResponse = sent.find(([, msg]) => msg.cid === 'first-append')
  const voteResponse = sent.find(([to, msg]) => to === '3' && msg.type === 'vote')
  t.equal(appendResponse[1].type, 'ack', 'earlier append commits successfully')
  t.equal(voteResponse[1].voteGranted, false, 'candidate behind the appended entry is rejected')
  t.equal(sent.find(([, msg]) => msg.cid === 'late-old-append')[1].type, 'err',
    'later old-term append is rejected')
  t.equal(log.seq, 1n, 'append is durable before the vote')
  t.deepEqual(log.elec, { term: 1n, votedFor: null },
    'rejected vote state is durable')
  node.close()
})

test('vote observes the unchanged head after an earlier append is rejected', async (t) => {
  const fixture = logFixture(t, 'node-abort-before-vote')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  log.append(entry(0n, 'zero'))
  node.leader = '2'

  await node.onReceive('2', {
    type: 'append',
    term: 0n,
    termP: 1n,
    seqP: 0n,
    commitSeq: -1n,
    cid: 'aborted-append',
    data: [entry(0n, 'one')],
  })
  await node.onReceive('3', {
    type: 'vote_request',
    term: 1n,
    termP: 0n,
    seqP: 0n,
  })

  const appendResponse = sent.find(([, msg]) => msg.cid === 'aborted-append')
  const voteResponse = sent.find(([to, msg]) => to === '3' && msg.type === 'vote')
  t.equal(appendResponse[1].type, 'err', 'invalid append is rejected')
  t.equal(voteResponse[1].voteGranted, true, 'candidate matching the unchanged head is granted')
  t.equal(log.seq, 0n, 'rejected append does not change the durable head')
  t.deepEqual(log.elec, { term: 1n, votedFor: '3' },
    'granted vote state is durable')
  node.close()
})

test('concurrent same-term vote requests grant at most one candidate', async (t) => {
  const fixture = logFixture(t, 'node-concurrent-votes')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  const changes = []
  node.on('change', (state) => changes.push(state))

  const msg = { type: 'vote_request', term: 1n, termP: -1n, seqP: -1n }
  await Promise.all([
    node.onReceive('2', { ...msg }),
    node.onReceive('2', { ...msg }),
    node.onReceive('3', { ...msg }),
  ])

  const votes = sent.filter(([, response]) => response.type === 'vote')
  t.equal(votes.length, 3, 'all vote requests receive a response')
  t.deepEqual([...new Set(votes.filter(([, response]) => response.voteGranted)
    .map(([to]) => to))], ['2'], 'only one candidate is granted')
  t.equal(votes.find(([, response]) => response.voteGranted)[0], '2',
    'the first candidate receives the vote')
  t.equal(changes.length, 1, 'duplicate grant does not emit another change')
  t.deepEqual(log.elec, { term: 1n, votedFor: '2' },
    'one granted vote is durable')
  node.close()
})

test('higher-term log rejection retains the AppendEntries leader', async (t) => {
  const fixture = logFixture(t, 'node-higher-term-log-rejection')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()

  await node.onReceive('2', {
    type: 'append',
    term: 1n,
    termP: 0n,
    seqP: 0n,
    commitSeq: -1n,
    cid: 'missing-prefix',
    data: [entry(1n, 'data')],
  })

  t.equal(node.term, 1n, 'advances to the AppendEntries term')
  t.equal(node.leader, '2', 'retains the leader after rejecting its log position')
  t.ok(node._pingms > 0, 'retains the leader contact time')
  t.equal(sent.length, 1, 'sends one response')
  t.equal(sent[0][1].type, 'err', 'rejects the missing previous entry')
  t.equal(sent[0][1].msg, 'seqP 0 not found', 'reports the log mismatch position')
  t.equal(sent[0][1].code, REPL_BACKTRACK,
    'marks the response for leader backtracking')
  t.equal(sent[0][1].sqlCode, null,
    'backtracking has no SQLite code')
  t.notOk(Object.hasOwn(sent[0][1], 'backtrack'),
    'does not emit the legacy backtrack field')
  node.close()
})

test('follower ACKs a fully duplicated suffix without appending an empty batch', async (t) => {
  const fixture = logFixture(t, 'node-idempotent-resend')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.leader = '2'

  const data = [entry(node.term, 'once')]
  const base = {
    type: 'append',
    term: node.term,
    termP: -1n,
    seqP: -1n,
    commitSeq: -1n,
    data,
  }
  await node.onReceive('2', { ...base, cid: 'first' })
  await waitFor(() => sent.some((msg) => msg.cid === 'first'))
  await node.onReceive('2', { ...base, cid: 'again' })
  await waitFor(() => sent.some((msg) => msg.cid === 'again'))

  t.equal(log.seq, 0n, 'duplicate keeps the same log head')
  t.equal(sent.find((msg) => msg.cid === 'again').type, 'ack', 'duplicate is ACKed')
  node.close()
})

test('a duplicated page preserves the follower suffix', async (t) => {
  const fixture = logFixture(t, 'node-duplicate-page')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.leader = '2'

  const data = ['zero', 'one', 'two', 'three'].map((value) => entry(node.term, value))
  log.appendBatch(data)
  await node.onReceive('2', {
    type: 'append',
    term: node.term,
    termP: -1n,
    seqP: -1n,
    commitSeq: -1n,
    cid: 'duplicate-page',
    data: data.slice(0, 2),
  })
  await waitFor(() => sent.some((msg) => msg.cid === 'duplicate-page'))

  t.equal(log.seq, 3n, 'duplicate page does not truncate later entries')
  t.equal(log.head.toString(), 'three', 'later suffix remains intact')
  t.equal(sent.find((msg) => msg.cid === 'duplicate-page').type, 'ack',
    'duplicate page is ACKed')
  node.close()
})

test('a duplicated page commits only through its verified entries', async (t) => {
  const fixture = logFixture(t, 'node-duplicate-page-commit')
  const log = fixture.create()
  const sent = []
  const applied = []
  const appliedTerms = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
    apply: (appliedNode, bufs, seqs, terms) => {
      applied.push(...bufs.map((buf) => buf && buf.toString()))
      appliedTerms.push(...terms)
      return bufs
    },
  })
  node.open()
  node.leader = '2'

  const prefix = ['zero', 'one'].map((value) => entry(1n, value))
  const divergent = ['bad-two', 'bad-three'].map((value) => entry(2n, value))
  const replacement = ['two', 'three'].map((value) => entry(3n, value))
  log.appendBatch([...prefix, ...divergent])

  await node.onReceive('2', {
    type: 'append',
    term: 3n,
    termP: -1n,
    seqP: -1n,
    commitSeq: 3n,
    cid: 'duplicate-prefix',
    data: prefix,
  })
  await waitFor(() => sent.some((msg) => msg.cid === 'duplicate-prefix'))
  await waitFor(() => node._applySeq === 1n)

  t.equal(node._commitSeq, 1n, 'leaderCommit is capped at the duplicated page end')
  t.equal(log.seq, 3n, 'unverified follower suffix remains stored')
  t.equal(log.head.toString(), 'bad-three', 'unverified suffix is not truncated early')
  t.deepEqual(applied, ['zero', 'one'], 'unverified suffix is not applied')

  await node.onReceive('2', {
    type: 'append',
    term: 3n,
    termP: 1n,
    seqP: 1n,
    commitSeq: 3n,
    cid: 'replace-suffix',
    data: replacement,
  })
  await waitFor(() => sent.some((msg) => msg.cid === 'replace-suffix'))
  await waitFor(() => node._applySeq === 3n)

  t.equal(node._commitSeq, 3n, 'replacement page advances commit through its verified end')
  t.equal(log.seq, 3n, 'replacement retains the leader log length')
  t.equal(log.head.toString(), 'three', 'replacement removes the divergent suffix')
  t.deepEqual(applied, ['zero', 'one', 'two', 'three'],
    'only verified leader entries are applied')
  t.deepEqual(appliedTerms, [1n, 1n, 3n, 3n],
    'no term from the divergent suffix is applied')
  node.close()
})

test('a matching heartbeat commits only through its verified seqP', async (t) => {
  const fixture = logFixture(t, 'node-heartbeat-commit-bound')
  const log = fixture.create()
  const sent = []
  const applied = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
    apply: (appliedNode, bufs) => {
      applied.push(...bufs.map((buf) => buf && buf.toString()))
      return bufs
    },
  })
  node.open()
  node.leader = '2'
  log.appendBatch(['zero', 'one'].map((value) => entry(node.term, value)))

  await node.onReceive('2', {
    type: 'append',
    term: node.term,
    termP: node.term,
    seqP: 1n,
    commitSeq: 1n,
    cid: 'matching-heartbeat',
  })
  await waitFor(() => sent.some((msg) => msg.cid === 'matching-heartbeat'))
  await waitFor(() => node._applySeq === 1n)

  t.equal(sent.find((msg) => msg.cid === 'matching-heartbeat').type, 'ack',
    'matching heartbeat is ACKed')
  t.equal(node._commitSeq, 1n, 'heartbeat commit is capped at seqP')
  t.deepEqual(applied, ['zero', 'one'], 'heartbeat applies the verified prefix')
  node.close()
})

test('follower closes rather than truncate its committed prefix', async (t) => {
  const fixture = logFixture(t, 'node-committed-prefix')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.leader = '2'

  const data = ['zero', 'one', 'two', 'three'].map((value) => entry(node.term, value))
  log.appendBatch(data)
  node._commitSeq = 2n
  const fatals = []
  const errors = []
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))

  await node.onReceive('2', {
    type: 'append',
    term: node.term,
    termP: node.term,
    seqP: 0n,
    commitSeq: 2n,
    cid: 'committed-conflict',
    data: [entry(node.term, 'different')],
  })
  await errored

  const probe = fixture.create()
  probe.open()
  t.equal(probe.seq, 3n, 'committed conflict does not truncate the log')
  t.equal(probe.head.toString(), 'three', 'committed suffix remains intact')
  probe.close()
  t.equal(fatals.length, 1, 'committed conflict is fatal')
  t.equal(errors.length, 1, 'committed conflict is re-emitted after close')
  t.match(errors[0].message, /cannot trim committed 2 to 0/,
    'error identifies the rejected trim')
  t.equal(errors[0].code, RAFT_ILLEGAL,
    'committed trim conflict is a Raft invariant failure')
  t.equal(errors[0].sqlCode, null,
    'Raft invariant failure has no SQLite code')
  t.deepEqual(sent, [], 'fatal conflict sends no response')
  t.notOk(node.isOpen, 'fatal conflict closes the node')
})

test('follower append failure after trim leaves a retryable shorter prefix', async (t) => {
  const fixture = logFixture(t, 'node-fail-forward-trim')
  const log = fixture.create()
  log.open()
  log.appendBatch([entry(0n, 'zero'), entry(0n, 'divergent')])
  const appendFailure = new Error('replacement append failed')
  log.appendBatch = () => { throw appendFailure }
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), log)
  node._open = true
  node.state = 'follower'
  node.leader = '2'
  node.term = 0n
  const fatals = []
  const errors = []
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))

  await node.onReceive('2', {
    type: 'append',
    term: 0n,
    termP: 0n,
    seqP: 0n,
    commitSeq: -1n,
    cid: 'failing-append',
    data: [entry(0n, 'replacement')],
  })
  await errored

  t.deepEqual(fatals, [appendFailure], 'replacement failure is fatal')
  t.deepEqual(errors, [appendFailure], 'replacement failure is emitted after close')
  t.deepEqual(sent, [], 'failed replacement sends no acknowledgement')
  t.notOk(node.isOpen, 'failed replacement closes the node')

  const probe = fixture.create()
  probe.open()
  t.equal(probe.seq, 0n, 'the committed trim leaves the valid prefix')
  t.equal(probe.head.toString(), 'zero', 'the retained prefix remains intact')
  probe.close()

  const retryLog = fixture.create()
  const retrySent = []
  const retry = new RaftNode('1', ids, (to, msg) => retrySent.push(msg), retryLog, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  retry.open()
  await retry.onReceive('2', {
    type: 'append',
    term: 0n,
    termP: 0n,
    seqP: 0n,
    commitSeq: -1n,
    cid: 'retry-append',
    data: [entry(0n, 'replacement')],
  })
  t.equal(retryLog.seq, 1n, 'leader retry appends onto the shorter prefix')
  t.equal(retryLog.head.toString(), 'replacement', 'retry installs the replacement suffix')
  t.equal(retrySent[0].type, 'ack', 'successful retry is acknowledged')
  retry.close()
})

test('replication pump retries remote errors and coalesces follower targets', async (t) => {
  const data = [entry(0n, 'zero'), entry(0n, 'one'), entry(0n, 'two')]
  const log = {
    seq: 2n,
    term: 0n,
    *iter(begin) {
      for (let seq = begin; seq <= this.seq; seq++) { yield data[Number(seq)] }
    },
  }
  let attempts = 0
  let node = null
  const send = (to, msg) => {
    if (msg.type !== 'append' || !msg.data) { return }
    attempts++
    const response = attempts === 1
      ? { type: 'err', term: 0n, cid: msg.cid, msg: 'temporary follower error' }
      : { type: 'ack', term: 0n, cid: msg.cid, seq: msg.seqP + 1n }
    setImmediate(() => node.onReceive(to, response))
  }
  node = new RaftNode('1', ids, send, log, {
    appendTimeout: 1_000,
    rpcMax: 1,
  })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']
  const warnings = []
  node.on('warn', (err) => warnings.push(err))

  const first = node._appendToFollower('2', 0n, 1n)
  const second = node._appendToFollower('2', 2n, 2n)
  await Promise.all([first, second])

  t.equal(node._replication.get('2').matchIndex, 2n, 'worker reaches the coalesced target')
  t.equal(node._replication.get('2').waiters.size, 0,
    'resolved replication waiters are removed before callers resume')
  t.ok(attempts >= 4, 'remote error and all rpc pages were retried')
  t.equal(warnings.length, 1, 'remote error is warned once')
  node._toFollower()
  node._stopTimers()
})

test('appendToFollowers preserves lagging progress and eagerly announces commit', async (t) => {
  const data = ['zero', 'one', 'two', 'three', 'four'].map((value) => entry(0n, value))
  const log = {
    seq: 4n,
    term: 0n,
    *iter(begin) {
      for (let seq = begin; seq <= this.seq; seq++) { yield data[Number(seq)] }
    },
  }
  const pages = []
  let node = null
  const send = (to, msg) => {
    if (msg.type !== 'append' || !msg.data) { return }
    pages.push(msg)
    setImmediate(() => node.onReceive(to, {
      type: 'ack',
      term: 0n,
      cid: msg.cid,
      seq: msg.seqP + 1n,
    }))
  }
  node = new RaftNode('1', ids, send, log, {
    appendTimeout: 1_000,
    rpcMax: 2,
  })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']
  node._replicationState('2', node.term, 1n)

  let settled = false
  const announced = []
  node._pingFollowers = () => {
    announced.push({ commitSeq: node._commitSeq, settled })
  }
  const applyEnds = []
  const apply = node._apply.bind(node)
  node._apply = (end) => {
    applyEnds.push(end)
    return apply(end)
  }
  const appending = node._appendToFollowers(4n, 4n)
    .finally(() => { settled = true })
  await appending

  t.deepEqual(pages.map((msg) => msg.seqP), [0n, 2n],
    'replication begins at stored nextIndex rather than command begin')
  t.deepEqual(pages.map((msg) => msg.data.length), [2, 2],
    'lagging entries are sent in rpcMax pages')
  t.equal(node._replication.get('2').matchIndex, 4n, 'follower catches up through command end')
  t.deepEqual(announced, [{ commitSeq: 4n, settled: false }],
    'announces the committed sequence once before append settlement')
  t.deepEqual(applyEnds, [2n, 4n],
    'each commit advancement schedules application exactly once')
  node._toFollower()
  node._stopTimers()
})

test('an older overlapping target does not rewind acknowledged replication', async (t) => {
  const data = ['zero', 'one', 'two', 'three'].map((value) => entry(0n, value))
  const log = {
    seq: 3n,
    term: 0n,
    *iter(begin) {
      for (let seq = begin; seq <= this.seq; seq++) { yield data[Number(seq)] }
    },
  }
  const pages = []
  let node = null
  const send = (to, msg) => {
    if (msg.type !== 'append' || !msg.data) { return }
    pages.push(msg)
    setImmediate(() => node.onReceive(to, {
      type: 'ack',
      term: 0n,
      cid: msg.cid,
      seq: msg.seqP + 1n,
    }))
  }
  node = new RaftNode('1', ids, send, log, {
    appendTimeout: 1_000,
    quorum: 3,
  })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']

  await node._appendToFollower('2', 0n, 2n)
  const state = node._replication.get('2')
  t.equal(state.matchIndex, 2n, 'initial page establishes acknowledged progress')
  t.equal(state.nextIndex, 3n, 'initial ACK advances nextIndex')
  pages.length = 0

  await node._appendToFollower('2', 0n, 1n)
  t.equal(state.nextIndex, 3n, 'older satisfied target does not rewind nextIndex')
  t.equal(pages.length, 0, 'older satisfied target sends no page')

  await node._appendToFollower('2', 3n, 3n)
  t.deepEqual(pages.map((msg) => msg.seqP), [2n],
    'new target resumes immediately after the acknowledged prefix')
  t.deepEqual(pages.map((msg) => msg.data.length), [1],
    'acknowledged entries are not resent')
  node._toFollower()
  node._stopTimers()
})

test('heartbeat ACK catches a discovered follower up to the current head', async (t) => {
  const data = entry(0n, 'no-op')
  const log = {
    seq: 0n,
    term: 0n,
    *iter(begin) {
      if (begin <= this.seq) { yield data }
    },
  }
  const pages = []
  let node = null
  const send = (to, msg) => {
    if (msg.type !== 'append') { return }
    if (msg.data) { pages.push([to, msg]) }
    setImmediate(() => node.onReceive(to, {
      type: 'ack',
      term: 0n,
      cid: msg.cid,
      seq: msg.seqP + 1n,
    }))
  }
  node = new RaftNode('1', ids, send, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']
  node._pongs.set('2', Date.now())
  node._pongs.set('3', Date.now())
  node._replicationState('2', node.term, 1n)
  node._replicationState('3', node.term, 1n)
  node._startPingTimer()

  await waitFor(() => node._replication.get('3').matchIndex === 0n)
  t.ok(node.followers.includes('3'), 'heartbeat discovers the follower')
  t.equal(pages.filter(([to]) => to === '3').length, 1,
    'heartbeat schedules the missing current entry')
  t.equal(node._replication.get('3').target, 0n,
    'background replication targets the current head')

  node._catchUpFollower('3')
  await sleep(10)
  t.equal(pages.filter(([to]) => to === '3').length, 1,
    'later heartbeats do not resend an acknowledged head')
  node._toFollower()
  node._stopTimers()
})

test('backtrack to nextIndex zero still attempts the genesis page', async (t) => {
  const data = [entry(0n, 'zero'), entry(0n, 'one')]
  const log = {
    begin: 0n,
    seq: 1n,
    term: 0n,
    *iter(begin) {
      for (let seq = begin; seq <= this.seq; seq++) { yield data[Number(seq)] }
    },
  }
  const pages = []
  let cancelledAtGenesis = null
  let node = null
  const send = (to, msg) => {
    if (msg.type !== 'append' || !msg.data) { return }
    pages.push(msg)
    if (msg.seqP === -1n) {
      cancelledAtGenesis = node._replication.get(to).cancelled
    }
    const response = msg.seqP === 0n
      ? {
          type: 'err',
          term: 0n,
          cid: msg.cid,
          msg: 'termP mismatch at seqP 0',
          code: REPL_BACKTRACK,
          sqlCode: null,
        }
      : { type: 'ack', term: 0n, cid: msg.cid, seq: msg.seqP + 1n }
    setImmediate(() => node.onReceive(to, response))
  }
  node = new RaftNode('1', ids, send, log, {
    appendTimeout: 1_000,
    pingTimeout: 1_000,
  })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']
  node.on('warn', () => {})

  await node._appendToFollower('2', 1n, 1n)
  const state = node._replication.get('2')

  t.deepEqual(pages.map((msg) => msg.seqP), [0n, -1n],
    'backtracking from seqP zero sends one page with the sentinel predecessor')
  t.equal(cancelledAtGenesis, false,
    'reaching nextIndex zero does not cancel before the genesis attempt')
  t.notOk(state.cancelled, 'successful genesis replication remains active')
  t.equal(state.matchIndex, 1n, 'the genesis page reaches the requested target')
  node._toFollower()
  node._stopTimers()
})

test('forgotten replication response cancels without backtracking', async (t) => {
  const data = [entry(0n, 'zero'), entry(0n, 'one')]
  const log = {
    begin: 0n,
    seq: 1n,
    term: 0n,
    *iter(begin) {
      for (let seq = begin; seq <= this.seq; seq++) { yield data[Number(seq)] }
    },
  }
  const pages = []
  let node = null
  const send = (to, msg) => {
    if (msg.type !== 'append' || !msg.data) { return }
    pages.push(msg.seqP)
    setImmediate(() => node.onReceive(to, {
      type: 'err',
      term: 0n,
      cid: msg.cid,
      msg: 'rx append wants forgotten 0 have 1',
      code: REPL_FORGOT,
      sqlCode: null,
    }))
  }
  node = new RaftNode('1', ids, send, log, {
    appendTimeout: 1_000,
    pingTimeout: 1_000,
  })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']
  const warnings = []
  node.on('warn', (err) => warnings.push(err))

  const err = await rejection(node._appendToFollower('2', 1n, 1n))
  const state = node._replication.get('2')
  await waitFor(() => !state.running)

  t.ok(err instanceof ErrorWithCode,
    'the pending append rejects with ErrorWithCode')
  t.equal(err.code, REPL_FORGOT,
    'the pending append retains the forgotten-prefix code')
  t.equal(err.sqlCode, null, 'the forgotten-prefix error has no SQLite code')
  t.deepEqual(pages, [0n], 'the forgotten response prevents another page')
  t.equal(state.nextIndex, 1n, 'the forgotten response does not backtrack')
  t.ok(state.cancelled, 'the forgotten response cancels replication')
  t.deepEqual(warnings, [err], 'the terminal error is warned once')

  node._toFollower()
  node._stopTimers()
})

test('an in-flight backtrack above the root does not cancel lowered shared state',
  async (t) => {
    const data = [entry(0n, 'zero'), entry(0n, 'one')]
    const log = {
      begin: 0n,
      seq: 1n,
      term: 0n,
      *iter(begin) {
        for (let seq = begin; seq <= this.seq; seq++) { yield data[Number(seq)] }
      },
    }
    const pages = []
    let node = null
    const send = (to, msg) => {
      if (msg.type === 'append' && msg.data) { pages.push([to, msg]) }
    }
    node = new RaftNode('1', ids, send, log, {
      appendTimeout: 1_000,
      pingTimeout: 20,
    })
    node._open = true
    node.state = 'leader'
    node.leader = '1'
    node.term = 0n
    node.followers = ['2']
    node.on('warn', () => {})

    const state = node._replicationState('2', node.term, 1n)
    state.target = 1n
    const work = node._replicationPage(state)
    await waitFor(() => pages.length === 1)
    state.nextIndex = 0n
    await node.onReceive('2', {
      type: 'err',
      term: 0n,
      cid: pages[0][1].cid,
      msg: 'termP mismatch at seqP 0',
      code: REPL_BACKTRACK,
      sqlCode: null,
    })

    t.equal(await work, true, 'the above-root page remains retryable')
    t.equal(pages[0][1].seqP, 0n, 'the rejected RPC was captured above the root')
    t.equal(state.nextIndex, 0n, 'the concurrently lowered nextIndex is preserved')
    t.notOk(state.cancelled,
      'mutable nextIndex equality does not cancel an above-root RPC')
    node._toFollower()
    node._stopTimers()
  })

test('backtrack rejecting the genesis page cancels replication', async (t) => {
  const data = entry(0n, 'zero')
  const log = {
    begin: 0n,
    seq: 0n,
    term: 0n,
    *iter() { yield data },
  }
  let attempts = 0
  const heartbeats = []
  let node = null
  const send = (to, msg) => {
    if (msg.type !== 'append') { return }
    if (!msg.data) {
      heartbeats.push([to, msg])
      setImmediate(() => node.onReceive(to, {
        type: 'ack',
        term: node.term,
        cid: msg.cid,
        seq: msg.seqP + 1n,
      }))
      return
    }
    attempts++
    setImmediate(() => node.onReceive(to, {
      type: 'err',
      term: 0n,
      cid: msg.cid,
      msg: 'termP mismatch at seqP -1',
      code: REPL_BACKTRACK,
      sqlCode: null,
    }))
  }
  node = new RaftNode('1', ids, send, log, {
    appendTimeout: 1_000,
    pingTimeout: 1_000,
  })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2', '3']
  const warnings = []
  node.on('warn', (err) => warnings.push(err))

  const work = node._appendToFollower('2', 0n, 0n)
  const err = await rejection(work)
  const state = node._replication.get('2')
  await waitFor(() => !state.running)

  t.equal(err.message,
    'genesis rejected append ERR termP mismatch at seqP -1',
    'the pending append identifies the illegal genesis response')
  t.ok(err instanceof ErrorWithCode,
    'the pending append rejects with ErrorWithCode')
  t.equal(err.code, RAFT_ILLEGAL,
    'the impossible genesis backtrack becomes a Raft invariant failure')
  t.equal(err.sqlCode, null,
    'the pending append has no SQLite code')
  t.equal(attempts, 1, 'the rejected genesis page is sent once')
  t.equal(warnings.length, 1, 'the terminal backtrack is warned once')
  t.equal(warnings[0].code, RAFT_ILLEGAL,
    'the warning uses the Raft invariant code')
  t.ok(state.cancelled, 'the terminal backtrack cancels the same-term state')
  t.equal(node._replication.get('2'), state,
    'the cancelled state remains in the same-term map')
  t.equal(state.waiters.size, 0, 'terminal cancellation drains pending waiters')

  node._catchUpFollower('2')
  await sleep(10)
  t.equal(attempts, 1, 'later catch-up does not restart cancelled replication')
  const cancelled = await rejection(node._appendToFollower('2', 0n, 0n))
  t.equal(cancelled.message, 'follower replication cancelled',
    'later same-term append rejects immediately')
  t.ok(cancelled instanceof ErrorWithCode,
    'cancelled replication rejects with ErrorWithCode')
  t.equal(cancelled.code, NOT_COMMIT,
    'cancelled replication uses NOT_COMMIT')
  t.equal(cancelled.sqlCode, null,
    'cancelled replication has no SQLite code')

  const now = Date.now()
  node._pongs.set('2', now)
  node._pongs.set('3', now)
  node._replicationState('3', node.term, 1n).matchIndex = 0n
  node._pingFollowers()
  await waitFor(() => node._acks.size === 0)
  await sleep(10)
  t.ok(heartbeats.some(([to]) => to === '2'),
    'cancelled replication does not suppress follower heartbeats')
  t.equal(attempts, 1, 'heartbeat catch-up still sends no cancelled data page')
  t.ok(state.cancelled, 'heartbeat handling preserves same-term cancellation')

  node.term = 1n
  const fresh = node._replicationState('2', node.term, 1n)
  t.notEqual(fresh, state, 'a new term replaces the cancelled state')
  t.notOk(fresh.cancelled, 'new-term replication begins enabled')
  node._toFollower()
  node._stopTimers()
})

test('local replication log errors close the node and reject its waiter', async (t) => {
  const failure = new Error('replication read failed')
  const log = {
    isOpen: true,
    seq: 0n,
    term: 0n,
    *iter() { throw failure },
    close() { this.isOpen = false },
  }
  const node = new RaftNode('1', ids, () => {}, log, { appendTimeout: 20 })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']
  const fatals = []
  const errors = []
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))

  const work = node._appendToFollower('2', 0n, 0n)
  work.catch(() => {})
  await waitFor(() => errors.length === 1)
  await rejects(t, work, /node not open/,
    'fatal close immediately rejects the target waiter')
  t.deepEqual(fatals, [failure], 'local log failure is fatal')
  t.deepEqual(errors, [failure], 'local log failure is emitted as error')
  t.notOk(node.isOpen, 'fatal log failure closes the node')
})

test('retained floor warns a leader asked for a forgotten predecessor',
  async (t) => {
    const entries = new Map([
      [5n, entry(0n, 'retained predecessor')],
      [6n, entry(0n, 'next entry')],
    ])
    const iterBegins = []
    const log = {
      isOpen: true,
      seq: 6n,
      term: 0n,
      begin: 5n,
      *iter(begin) {
        iterBegins.push(begin)
        for (let seq = begin; seq <= this.seq; seq++) {
          const data = entries.get(seq)
          if (data) { yield data }
        }
      },
      close() { this.isOpen = false },
    }
    const node = new RaftNode('1', ids, () => {}, log, {
      appendTimeout: 100,
    })
    node._open = true
    node.state = 'leader'
    node.leader = '1'
    node.term = 0n
    node.followers = ['2', '3']
    node._commitSeq = 5n
    node._commitTerm = 0n
    node._applySeq = 5n
    const warnings = []
    const fatals = []
    const errors = []
    node.on('warn', (err) => warnings.push(err))
    node.on('fatal', (err) => fatals.push(err))
    node.on('error', (err) => errors.push(err))

    const stale = node._replicationState('2', node.term, 1n)
    const work = node._appendToFollower('2', 1n, 6n)
    const err = await rejection(work)
    await waitFor(() => !stale.running)

    t.match(err.message, /replication wants forgotten 0 have 5/,
      'the pending append rejects with the forgotten-prefix error')
    t.ok(err instanceof ErrorWithCode,
      'the forgotten-prefix failure uses ErrorWithCode')
    t.equal(err.code, REPL_FORGOT,
      'the forgotten-prefix failure uses REPL_FORGOT')
    t.equal(err.sqlCode, null,
      'the forgotten-prefix failure has no SQLite code')
    t.ok(stale.cancelled, 'the forgotten-prefix warning cancels replication state')
    t.equal(node._replication.get('2'), stale,
      'same-term lookup retains the cancelled state')
    t.equal(stale.waiters.size, 0, 'forgotten-prefix cancellation drains waiters')
    const nextIndex = stale.nextIndex
    const target = stale.target

    node._catchUpFollower('2')
    await sleep(10)
    await rejects(t, node._appendToFollower('2', 6n, 6n),
      /follower replication cancelled/,
      'later same-term append rejects immediately')

    t.equal(stale.nextIndex, nextIndex,
      'cancelled append does not change the stored nextIndex')
    t.equal(stale.target, target,
      'cancelled append does not change the stored target')
    t.deepEqual(iterBegins, [],
      'the worker detects the forgotten predecessor before reading the log')
    t.equal(warnings.length, 1, 'later catch-up attempts emit no repeated warning')
    t.match(warnings[0].message,
      /replication wants forgotten 0 have 5/,
      'the warning identifies the unavailable predecessor')
    t.equal(warnings[0].code, REPL_FORGOT,
      'the warning retains the forgotten-prefix code')

    node._replicationState('3', node.term, 7n).matchIndex = 6n
    await node._appendToFollowers(6n, 6n)
    t.equal(node._commitSeq, 6n,
      'self and a healthy follower still satisfy quorum')
    t.deepEqual(fatals, [], 'the forgotten prefix is not fatal')
    t.deepEqual(errors, [], 'the forgotten prefix emits no public error')
    t.ok(node.isOpen, 'the leader remains open after the forgotten request')
    t.ok(log.isOpen, 'the leader keeps its retained log open')
    node.close()
  })

test('retained follower warns on a page below its log begin',
  async (t) => {
    const entries = new Map([
      [5n, entry(0n, 'retained predecessor')],
      [6n, entry(0n, 'retained entry')],
    ])
    const iterBegins = []
    const log = {
      isOpen: true,
      seq: 6n,
      term: 0n,
      begin: 5n,
      *iter(begin) {
        iterBegins.push(begin)
        for (let seq = begin; seq <= this.seq; seq++) {
          const data = entries.get(seq)
          if (data) { yield data }
        }
      },
      election() {},
      close() { this.isOpen = false },
    }
    const sent = []
    const node = new RaftNode(
      '1',
      ids,
      (to, msg) => sent.push([to, msg]),
      log,
      { appendTimeout: 1_000 },
    )
    node._open = true
    node.state = 'follower'
    node.leader = '2'
    node.term = 0n
    node._commitSeq = 6n
    node._commitTerm = 0n
    node._applySeq = 6n
    const warnings = []
    const fatals = []
    const errors = []
    node.on('warn', (err) => warnings.push(err))
    node.on('fatal', (err) => fatals.push(err))
    node.on('error', (err) => errors.push(err))

    await node.onReceive('2', {
      type: 'append',
      term: 0n,
      termP: 0n,
      seqP: 5n,
      commitSeq: 6n,
      cid: 'retained-predecessor',
      data: [entries.get(6n)],
    })

    t.deepEqual(iterBegins, [5n],
      'the exact retained begin remains a valid predecessor')
    t.equal(sent.length, 1, 'the retained-boundary page receives one response')
    t.equal(sent[0][1].type, 'ack',
      'a page at the exact retained predecessor boundary is accepted')
    t.deepEqual(fatals, [], 'the valid boundary remains nonfatal')
    t.ok(node.isOpen, 'the follower remains available after the valid boundary')

    sent.length = 0
    iterBegins.length = 0
    await node.onReceive('2', {
      type: 'append',
      term: 0n,
      termP: 0n,
      seqP: 4n,
      commitSeq: 6n,
      cid: 'forgotten-predecessor',
      data: [entry(0n, 'delayed entry')],
    })

    t.equal(sent.length, 1, 'the forgotten page sends one protocol response')
    t.equal(sent[0][0], '2', 'the forgotten response is routed to the leader')
    t.equal(sent[0][1].type, 'err', 'the forgotten page sends an ERR')
    t.equal(sent[0][1].cid, 'forgotten-predecessor',
      'the ERR retains the request correlation id')
    t.equal(sent[0][1].msg,
      'rx append wants forgotten 4 have 5',
      'the ERR uses the warning message')
    t.equal(sent[0][1].code, REPL_FORGOT,
      'the ERR reports the forgotten predecessor')
    t.equal(sent[0][1].sqlCode, null,
      'the backtracking ERR has no SQLite code')
    t.notOk(Object.hasOwn(sent[0][1], 'backtrack'),
      'the ERR omits the legacy backtrack field')
    t.deepEqual(iterBegins, [],
      'the unavailable predecessor is detected before log iteration')
    t.equal(warnings.length, 1, 'the forgotten page emits one warning')
    t.match(warnings[0].message, /rx append wants forgotten 4 have 5/,
      'the warning identifies the unavailable predecessor')
    t.equal(warnings[0].code, REPL_FORGOT,
      'the warning uses the forgotten-prefix code')
    t.deepEqual(fatals, [], 'the forgotten page is not fatal')
    t.deepEqual(errors, [], 'the forgotten page emits no public error')
    t.ok(node.isOpen, 'the retained follower remains open')
    t.ok(log.isOpen, 'the follower keeps its retained log open')

    sent.length = 0
    await node.onReceive('2', {
      type: 'append',
      term: 0n,
      termP: 0n,
      seqP: 5n,
      commitSeq: 6n,
      cid: 'retained-predecessor-retry',
      data: [entries.get(6n)],
    })
    t.equal(sent.length, 1, 'the follower continues handling later pages')
    t.equal(sent[0][1].type, 'ack',
      'a later page at the retained boundary is accepted')
    node.close()
  })

test('expired waiters are removed while the follower worker keeps replicating', async (t) => {
  const data = entry(0n, 'eventual')
  const log = {
    seq: 0n,
    term: 0n,
    *iter() { yield data },
  }
  let reply = false
  let node = null
  const send = (to, msg) => {
    if (!reply || msg.type !== 'append' || !msg.data) { return }
    setImmediate(() => node.onReceive(to, {
      type: 'ack', term: 0n, cid: msg.cid, seq: msg.seqP + 1n,
    }))
  }
  node = new RaftNode('1', ids, send, log, { appendTimeout: 20 })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']
  node.on('warn', () => {})

  const err = await rejection(node._appendToFollower('2', 0n, 0n))
  t.equal(err?.message, 'follower append timeout', 'client target waiter expires')
  t.equal(err?.code, APPEND_TIMEOUT,
    'catch-up expiry uses APPEND_TIMEOUT')
  t.equal(err?.sqlCode, null, 'catch-up expiry has no SQLite code')
  t.equal(node._replication.get('2').waiters.size, 0, 'expired waiter is removed')
  reply = true
  await waitFor(() => node._replication.get('2').matchIndex === 0n)
  t.equal(node._replication.get('2').matchIndex, 0n,
    'background worker eventually reaches its target')
  node._toFollower()
  node._stopTimers()
})

test('stepdown immediately rejects outstanding replication waiters', async (t) => {
  const data = entry(0n, 'pending')
  const log = {
    seq: 0n,
    term: 0n,
    *iter() { yield data },
  }
  const node = new RaftNode('1', ids, () => {}, log, { appendTimeout: 60_000 })
  node._open = true
  node.state = 'leader'
  node.leader = '1'
  node.term = 0n
  node.followers = ['2']

  const state = node._replicationState('2', node.term, 0n)
  const work = node._appendToFollower('2', 0n, 0n)
  node._toFollower()
  node._stopTimers()
  const err = await rejection(work)
  t.equal(err?.message, 'node not leader',
    'stepdown rejects target waiter immediately')
  t.equal(err?.code, NOT_LEADER,
    'stepdown uses the NOT_LEADER code')
  t.equal(err?.sqlCode, null, 'not-leader failure has no SQLite code')
  t.equal(state.waiters.size, 0,
    'rejected replication waiter is removed before callers resume')
  t.equal(node._replication.size, 0, 'stepdown clears replication workers')
})

test('close immediately rejects outstanding replication waiters', async (t) => {
  const fixture = logFixture(t, 'node-close-replication')
  const log = fixture.create()
  const node = new RaftNode('1', ids, () => {}, log, {
    appendTimeout: 60_000,
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  log.append(entry(node.term, 'pending'))
  node.state = 'leader'
  node.leader = '1'
  node.followers = ['2']

  const work = node._appendToFollower('2', 0n, 0n)
  node.close()
  await rejects(t, work, /node not open/,
    'close rejects target waiter immediately')
  t.equal(node._replication.size, 0, 'close clears replication workers')
})

test('close is durable before returning and rejects later local append', async (t) => {
  const fixture = logFixture(t, 'node-close-cancels-append')
  const log = fixture.create()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node._stopTimers()
  node.state = 'leader'
  node.leader = node.id

  node.close()
  await rejects(t, node.append(Buffer.from('issued-after-close')), /node not open/,
    'public append observes the terminal close')

  const probe = fixture.create()
  probe.open()
  t.equal(probe.seq, -1n, 'the rejected append never reaches durable storage')
  t.equal(probe.term, -1n, 'the empty log keeps its initial term')
  t.equal(probe.head, null, 'the empty log keeps its initial head')
  probe.close()
})

test('apply subscriptions collect results once and clean before callers resume', async (t) => {
  const node = new RaftNode('1', ids, () => {}, {}, {})
  const waiter = node._subscribeApply(2n, 4n)

  t.equal(node._applyWaiters.size, 1, 'subscription is registered')
  node._routeApply(3n, ['three'])
  node._routeApply(3n, ['duplicate'])
  node._routeApply(2n, ['two'])
  t.equal(waiter.remaining, 1, 'distinct in-range results are collected once')
  t.notOk(waiter.settled, 'subscription remains pending until its range is complete')

  node._routeApply(4n, ['four'])
  const results = await waiter.promise
  t.deepEqual(results, ['two', 'three', 'four'], 'results retain sequence order')
  t.ok(waiter.settled, 'completed subscription is settled')
  t.equal(node._applyWaiters.size, 0,
    'completed subscription is removed before its caller resumes')
  t.notOk(waiter.reject(new Error('late rejection')),
    'completed subscription cannot be settled again')
})

test('rejected apply subscriptions clean up and ignore later results', async (t) => {
  const node = new RaftNode('1', ids, () => {}, {}, {})
  const waiter = node._subscribeApply(5n, 6n)
  const err = new Error('apply subscription cancelled')

  t.ok(waiter.reject(err), 'first rejection settles the subscription')
  t.equal(await rejection(waiter.promise), err, 'rejection preserves the cancellation error')
  t.equal(node._applyWaiters.size, 0,
    'rejected subscription is removed before its caller resumes')
  node._routeApply(5n, ['late'])
  t.equal(waiter.remaining, 2, 'later application does not update the rejected subscription')
  t.notOk(0 in waiter.results, 'later application result is not retained')
  t.notOk(waiter.resolve(['replacement']), 'rejected subscription cannot resolve later')
})

test('large and concurrent commands route results across rpc and apply batches', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const apply = (node, bufs, seqs, terms) => bufs.map((buf, idx) => {
    const value = buf === null ? null : buf.toString()
    return `${seqs[idx]}:${value}`
  })
  const opts = () => ({ apply, quorum: 3, rpcMax: 2, applyMax: 1 })
  const nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  open(nodes)
  await ready(nodes)

  const leader = leaders(nodes)[0]
  followers(nodes).forEach((node) => { node.opts.apply = applyNull })
  await waitFor(() => leader._commitSeq >= 0n)
  await waitFor(() => leader._applySeq >= 0n)

  const values = ['a', 'b', 'c', 'd', 'e'].map((value) => Buffer.from(value))
  const large = leader.appendBatch(values)
  const later = leader.append(Buffer.from('later'))
  const [[seq, results], [seq2, result2]] = await Promise.all([large, later])

  t.equal(results.length, values.length, 'large command receives every result')
  t.deepEqual(results, values.map((buf, idx) => `${seq + BigInt(idx)}:${buf}`),
    'large results remain aligned across apply batches')
  t.equal(result2, `${seq2}:later`, 'concurrent command receives only its result')
  t.equal(leader._applyWaiters.size, 0, 'completed result subscriptions are removed')
})

test('timed out commands discard results while replication and application continue', async (t) => {
  t.teardown(() => close(nodes))
  let allowed = true
  const coms = comms(() => allowed)
  const apply = (node, bufs) => bufs.map((buf) => buf && buf.toString())
  const nodes = connect(coms, 3, null, () => ({
    apply,
    quorum: 3,
    appendTimeout: 20,
    pingTimeout: 1_000,
  }))
  await reset(nodes)
  open(nodes)
  await ready(nodes)

  const leader = leaders(nodes)[0]
  followers(nodes).forEach((node) => { node.opts.apply = applyNull })
  await waitFor(() => leader._commitSeq >= 0n)
  await waitFor(() => leader._applySeq >= 0n)
  leader.on('warn', () => {})

  allowed = false
  const failed = leader.append(Buffer.from('ambiguous'))
  await waitFor(() => leader._applyWaiters.size === 1)
  const target = leader.seq
  const err = await rejection(failed)
  t.equal(err?.message, 'quorum append timeout',
    'partitioned command returns an error')
  t.equal(err?.code, APPEND_TIMEOUT,
    'append quorum expiry uses APPEND_TIMEOUT')
  t.equal(err?.sqlCode, null, 'append quorum expiry has no SQLite code')
  t.equal(leader._applyWaiters.size, 0, 'timed out command subscription is removed')

  allowed = true
  await waitFor(() => leader._applySeq >= target, 2_000)
  t.equal(leader._applyWaiters.size, 0, 'eventual application does not cache its result')
  const [seq, result] = await leader.append(Buffer.from('retry'))
  t.equal(result, 'retry', 'retry receives only its newly appended result')
  t.ok(seq > target, 'retry is represented by a new log entry')
})

test('failed application closes the node without retrying', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const calls = []
  const apply = (node, bufs, seqs, terms) => {
    calls.push([seqs[0], bufs.map((buf) => buf && buf.toString())])
    if (bufs.some((buf) => buf && buf.toString() === 'failed')) {
      throw new Error('state machine failed')
    }
    return bufs.map((buf) => buf && buf.toString())
  }
  const nodes = connect(coms, 3, null, () => ({ apply, quorum: 3, applyMax: 1 }))
  await reset(nodes)
  open(nodes)
  await ready(nodes)

  const leader = leaders(nodes)[0]
  followers(nodes).forEach((node) => { node.opts.apply = applyNull })
  await waitFor(() => leader._commitSeq >= 0n)
  await waitFor(() => leader._applySeq >= 0n)
  const fatals = []
  const errors = []
  leader.on('fatal', (err) => fatals.push(err))
  leader.on('error', (err) => errors.push(err))
  const errored = new Promise((resolve) => leader.once('error', resolve))
  const before = leader._applySeq

  const failed = await rejection(leader.append(Buffer.from('failed')))
  t.ok(failed instanceof ErrorWithCode,
    'fatal application rejects the command with ErrorWithCode')
  t.equal(failed?.message, '(apply) state machine failed',
    'fatal application preserves its operation context')
  t.equal(failed?.code, APPLY_ERROR,
    'fatal application rejects the command with APPLY_ERROR')
  t.equal(failed?.sqlCode, null,
    'fatal application rejection has no SQLite code')
  await errored
  t.equal(leader._applySeq, before, 'throw does not advance apply sequence')
  t.equal(leader._applyWaiters.size, 0, 'failed command subscription is removed')
  const failedCalls = calls.filter(([callSeq]) => callSeq === before + 1n)
  t.equal(failedCalls.length, 1, 'failed application is attempted once')
  t.equal(fatals.length, 1, 'application failure is fatal')
  t.equal(failed, fatals[0],
    'the command receives the exact fatal application error')
  t.equal(errors[0], fatals[0], 're-emits the same application error after close')
  t.notOk(leader.isOpen, 'application failure closes the node')
})

test('async application and invalid results are fatal', async (t) => {
  const run = async (apply, pattern) => {
    const data = entry(0n, 'data')
    const log = {
      isOpen: true,
      seq: 0n,
      term: 0n,
      *iter() { yield data },
      close() { this.isOpen = false },
    }
    const node = new RaftNode('1', ids, () => {}, log, { apply })
    node._open = true
    node._commitSeq = 0n
    const fatals = []
    const errors = []
    node.on('fatal', (err) => fatals.push(err))
    node.on('error', (err) => errors.push(err))
    const errored = new Promise((resolve) => node.once('error', resolve))
    const waiter = node._subscribeApply(0n, 0n)
    const waiterFailure = rejection(waiter.promise)

    const failed = await rejection(node._apply(0n))
    const waited = await waiterFailure
    t.match(failed?.message ?? '', pattern, 'application rejects')
    t.ok(failed instanceof ErrorWithCode, 'application uses ErrorWithCode')
    t.equal(failed.code, APPLY_ERROR, 'application has APPLY_ERROR code')
    t.equal(failed.sqlCode, null, 'application has no SQLite code')
    t.equal(waited, failed, 'apply waiter receives the exact application error')
    t.equal(node._applyWaiters.size, 0, 'failed apply waiter is removed')
    await errored
    t.equal(fatals.length, 1, 'application failure is fatal')
    t.equal(fatals[0], failed, 'fatal receives the normalized error')
    t.equal(errors[0], fatals[0], 're-emits the same fatal application error')
    t.equal(node._applySeq, -1n, 'application failure does not advance apply sequence')
    t.notOk(node.isOpen, 'application failure closes the node')
  }

  const failure = new Error('async state machine failed')
  await run(async () => { throw failure }, /\(apply\) async state machine failed/)
  await run(() => [], /\(apply\) results must be array with len 1/)
})

test('application materializes each batch before invoking async apply', async (t) => {
  const fixture = logFixture(t, 'node-apply-materializes-batch')
  const log = fixture.create()
  const iter = log.iter.bind(log)
  let iterating = false
  log.iter = (...args) => {
    const source = iter(...args)
    return (function * () {
      iterating = true
      try {
        yield * source
      } finally {
        iterating = false
      }
    })()
  }
  const batches = []
  const observedBefore = []
  const node = new RaftNode('1', ids, () => {}, log, {
    applyMax: 2,
    electionTimeout: 60_000,
    pingTimeout: 60_000,
    apply: async (appliedNode, bufs, seqs) => {
      t.notOk(iterating, 'apply starts after synchronous iteration finishes')
      await Promise.resolve()
      batches.push(seqs)
      observedBefore.push(appliedNode._applySeq)
      return bufs.map((buf) => buf && buf.toString())
    },
  })
  node.open()
  node._stopTimers()
  log.appendBatch(['zero', 'one', 'two'].map((value) => entry(node.term, value)))
  node._commitSeq = 2n

  await node._apply(2n)

  t.deepEqual(batches, [[0n, 1n], [2n]], 'each applyMax batch is materialized separately')
  t.equal(node._applySeq, 2n, 'application still advances through every batch')
  t.deepEqual(observedBefore, [-1n, 1n],
    'publishes each completed batch before invoking the next one')
  node.close()
})

test('log iteration failure during apply closes the node', async (t) => {
  const failure = new Error('apply log read failed')
  const log = {
    isOpen: true,
    seq: 0n,
    term: 0n,
    *iter() { throw failure },
    close() { this.isOpen = false },
  }
  const node = new RaftNode('1', ids, () => {}, log, {
    apply: (node, bufs) => bufs,
  })
  node._open = true
  node._commitSeq = 0n
  const fatals = []
  const errors = []
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))
  const waiter = node._subscribeApply(0n, 0n)
  const waiterFailure = rejection(waiter.promise)

  const work = node._apply(0n)
  work.catch(() => {})
  const workFailure = await rejection(work)
  t.equal(workFailure, failure, 'apply rejects with the exact log failure')
  t.equal(await waiterFailure, failure,
    'apply waiter receives the exact log failure before shutdown')
  await errored

  t.deepEqual(fatals, [failure], 'emits the log read failure as fatal')
  t.deepEqual(errors, [failure], 're-emits the log read failure after close')
  t.equal(node._applyWaiters.size, 0, 'failed log-read waiter is removed')
  t.notOk(node.isOpen, 'fatal log read closes the node')
})

test('short application rejects its waiter with the Raft invariant error', async (t) => {
  const log = {
    isOpen: true,
    *iter() {},
    close() { this.isOpen = false },
  }
  const node = new RaftNode('1', ids, () => {}, log)
  node._open = true
  node._commitSeq = 0n
  const fatals = []
  const errors = []
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))
  const waiter = node._subscribeApply(0n, 0n)
  const waiterFailure = rejection(waiter.promise)

  const failed = await rejection(node._apply(0n))
  t.ok(failed instanceof ErrorWithCode,
    'short application uses ErrorWithCode')
  t.equal(failed?.message, 'apply ended at -1 wanted 0',
    'short application identifies its incomplete range')
  t.equal(failed?.code, RAFT_ILLEGAL,
    'short application uses RAFT_ILLEGAL')
  t.equal(failed?.sqlCode, null,
    'short application has no SQLite code')
  t.equal(await waiterFailure, failed,
    'apply waiter receives the exact invariant error before shutdown')
  await errored
  t.deepEqual(fatals, [failed], 'emits the invariant error as fatal')
  t.deepEqual(errors, [failed], 're-emits the invariant error after close')
  t.equal(node._applyWaiters.size, 0, 'failed invariant waiter is removed')
  t.notOk(node.isOpen, 'short application closes the node')
})

test('close does not wait for apply and suppresses its late settlement', async (t) => {
  for (const kind of ['resolve', 'reject']) {
    let settleApply = null
    let applyStarted = null
    const started = new Promise((resolve) => { applyStarted = resolve })
    const applying = new Promise((resolve, reject) => {
      settleApply = kind === 'resolve' ? resolve : reject
    })
    const log = {
      isOpen: true,
      seq: 0n,
      term: 0n,
      *iter() { yield entry(0n, 'pending') },
      close() { this.isOpen = false },
    }
    const node = new RaftNode('1', ids, () => {}, log, {
      apply: () => {
        applyStarted()
        return applying
      },
    })
    node._open = true
    node._commitSeq = 0n
    const fatals = []
    const errors = []
    node.on('fatal', (err) => fatals.push(err))
    node.on('error', (err) => errors.push(err))

    const outcome = rejection(node._apply(0n))
    await started
    node.close()
    t.notOk(log.isOpen, `${kind}: close releases the log before returning`)

    const failure = new Error(`late apply ${kind}`)
    settleApply(kind === 'resolve' ? [null] : failure)
    const observed = await outcome
    t.notEqual(observed, failure, `${kind}: late settlement is not surfaced as apply failure`)
    if (observed) {
      t.match(observed.message, /node not open/,
        `${kind}: pending caller is normalized to shutdown`)
    }
    await new Promise((resolve) => setImmediate(resolve))
    t.equal(node._applySeq, -1n, `${kind}: abandoned apply does not advance state`)
    t.deepEqual(fatals, [], `${kind}: abandoned apply is not fatal`)
    t.deepEqual(errors, [], `${kind}: abandoned apply emits no error`)
  }
})
