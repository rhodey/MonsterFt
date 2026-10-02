import test from 'tape'
import {
  ARGUMENT_ILLEGAL,
  ErrorWithCode,
  NO_LEADER,
} from '../src/error.js'
import { RaftNode as ProductionRaftNode } from '../src/index.js'
import { databasePath, logFixture, sleep } from './util.js'

let injectedLogId = 0
class TestRaftNode extends ProductionRaftNode {
  constructor(id, nodes, send, log, opts) {
    const file = typeof log?.path === 'string' && log.path.length > 0
      ? log.path
      : databasePath(`21-injected-log-${process.pid}-${++injectedLogId}`)
    super(id, nodes, send, file, opts)
    this.log = log
  }
}
const RaftNode = TestRaftNode

const ids = ['1', '2', '3']

const entry = (term, data=null) => {
  const prefix = Buffer.alloc(8)
  prefix.writeBigUInt64LE(term)
  return data === null ? prefix : Buffer.concat([prefix, Buffer.from(data)])
}

const rejection = async (promise) => {
  try {
    await promise
    return null
  } catch (err) {
    return err
  }
}

const waitFor = async (fn, ms=1_000) => {
  const end = Date.now() + ms
  while (!fn()) {
    if (Date.now() >= end) { throw new Error('waitFor timeout') }
    await sleep(5)
  }
}

const opts = {
  appendTimeout: 25,
  electionTimeout: 60_000,
  pingTimeout: 60_000,
}

test('a self vote persists the local candidate id without installing a leader', async (t) => {
  const fixture = logFixture(t, '21-self-vote')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log, opts)
  node.open()
  node._stopTimers()

  node._voteForSelf()

  t.equal(node.state, 'candidate', 'a self vote starts an election')
  t.equal(node.leader, null, 'a self vote does not establish a leader')
  t.equal(node._votedFor, '1', 'the node records itself as the candidate')
  t.deepEqual(log.elec, { term: 1n, votedFor: '1' },
    'the local candidate id is durable before RequestVote is sent')
  t.deepEqual(sent.map(([to]) => to), ['2', '3'],
    'RequestVote is sent to the other members')

  node.close()
})

test('a restarted node repeats only its persisted vote', async (t) => {
  const fixture = logFixture(t, '21-restarted-vote')
  const seed = fixture.create()
  seed.open()
  seed.election(4n, '2')
  seed.close()

  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log, opts)
  node.open()
  node._stopTimers()
  const request = {
    type: 'vote_request',
    term: 4n,
    termP: -1n,
    seqP: -1n,
  }

  await node.onReceive('2', request)
  await node.onReceive('3', request)

  const votes = sent.filter(([, msg]) => msg.type === 'vote')
  t.equal(votes.length, 2, 'both candidates receive a response')
  t.equal(votes[0][1].voteGranted, true,
    'the persisted candidate receives its idempotent grant')
  t.equal(votes[1][1].voteGranted, false,
    'a different candidate cannot receive the term vote')
  t.equal(node._votedFor, '2', 'the restored vote identity remains unchanged')
  t.equal(node.leader, null, 'restoring a vote does not restore a leader')

  node.close()
})

test('RequestVote candidate identity stays separate from established leader identity', async (t) => {
  const fixture = logFixture(t, '21-vote-leader-separation')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log, opts)
  node.open()
  node._stopTimers()

  let leaderReady = false
  const waiting = node.awaitLeader().then(() => { leaderReady = true })
  const voteRequest = {
    type: 'vote_request',
    term: 1n,
    termP: -1n,
    seqP: -1n,
  }

  await node.onReceive('2', voteRequest)
  await Promise.resolve()

  let votes = sent.filter(([, msg]) => msg.type === 'vote')
  t.equal(votes.at(-1)[1].voteGranted, true, 'grants the first candidate its vote')
  t.equal(node._votedFor, '2', 'records the granted candidate')
  t.deepEqual(log.elec, { term: 1n, votedFor: '2' },
    'persists the candidate id with the term')
  t.equal(node.leader, null, 'does not infer a leader from RequestVote')
  t.notOk(leaderReady, 'RequestVote does not satisfy leader readiness')

  const sentBeforeForward = sent.length
  const forwardError = await rejection(node.append(Buffer.from('not-yet-routable')))
  t.equal(forwardError?.message, 'forward no leader',
    'commands cannot be forwarded to a candidate')
  t.equal(forwardError?.code, NO_LEADER,
    'unroutable command uses NO_LEADER')
  t.equal(sent.length, sentBeforeForward, 'no operation is sent to the candidate')

  await node.onReceive('2', voteRequest)
  votes = sent.filter(([, msg]) => msg.type === 'vote')
  t.equal(votes.at(-1)[1].voteGranted, true,
    'retransmission from the persisted candidate is granted')

  await node.onReceive('3', {
    type: 'append',
    term: 1n,
    termP: -1n,
    seqP: -1n,
    commitSeq: -1n,
    cid: 'leader-heartbeat',
  })
  await waiting
  t.equal(node.leader, '3', 'valid AppendEntries establishes its sender as leader')

  await node.onReceive('3', voteRequest)
  votes = sent.filter(([, msg]) => msg.type === 'vote')
  t.equal(votes.at(-1)[1].voteGranted, false,
    'a delayed RequestVote cannot obtain a second vote in the term')
  t.equal(node._votedFor, '2', 'AppendEntries did not overwrite the persisted vote')
  t.deepEqual(log.elec, { term: 1n, votedFor: '2' },
    'the original vote remains durable')
  t.equal(node.leader, '3', 'rejecting the delayed vote retains the known leader')

  await node.onReceive('3', { ...voteRequest, term: 2n })
  votes = sent.filter(([, msg]) => msg.type === 'vote')
  t.equal(votes.at(-1)[1].voteGranted, true, 'a higher term can grant a new vote')
  t.equal(node._votedFor, '3', 'the higher-term candidate replaces the old vote')
  t.deepEqual(log.elec, { term: 2n, votedFor: '3' },
    'the higher-term candidate is durable')
  t.equal(node.leader, null, 'RequestVote in the new term clears the old leader')

  node.close()
})

test('follower readiness uses the committed entry term and accepts an internal no-op', async (t) => {
  const fixture = logFixture(t, '21-follower-commit-term')
  const seed = fixture.create()
  seed.open()
  seed.append(entry(1n, 'old-term-command'))
  seed.election(2n, null)
  seed.close()

  const log = fixture.create()
  const applied = []
  const appliedTerms = []
  const node = new RaftNode('1', ids, () => {}, log, {
    ...opts,
    apply: (appliedNode, bufs, seqs, terms) => {
      applied.push(...bufs.map((buf) => buf === null ? null : buf.toString()))
      appliedTerms.push(...terms)
      return bufs
    },
  })
  node.open()
  node._stopTimers()

  let currentTermReady = false
  const waiting = node.awaitLeader(true).then(() => { currentTermReady = true })

  await node.onReceive('2', {
    type: 'append',
    term: 2n,
    termP: 1n,
    seqP: 0n,
    commitSeq: 0n,
    cid: 'commit-old-term',
  })
  await Promise.resolve()

  t.equal(node.leader, '2', 'AppendEntries establishes the leader')
  t.equal(node._commitSeq, 0n, 'the old-term entry becomes committed')
  t.equal(node._commitTerm, 1n, 'commit term comes from the committed entry')
  t.notOk(currentTermReady,
    'an old-term commit does not satisfy current-term leader readiness')
  await node.awaitLeader()
  t.pass('leader readiness without the commit requirement is satisfied')

  await node.onReceive('2', {
    type: 'append',
    term: 2n,
    termP: 1n,
    seqP: 0n,
    commitSeq: 1n,
    cid: 'commit-current-no-op',
    data: [entry(2n)],
  })
  await waiting
  await waitFor(() => node._applySeq === 1n)

  t.equal(node._commitSeq, 1n, 'the current-term no-op becomes committed')
  t.equal(node._commitTerm, 2n, 'the no-op supplies the current commit term')
  t.equal(log.seq, 1n, 'the term-only entry is appended')
  t.equal(log.head.length, 0, 'the term-only entry has an empty application payload')
  t.deepEqual(applied, ['old-term-command', null],
    'the internal no-op is applied as null')
  t.deepEqual(appliedTerms, [1n, 2n], 'application observes each stored entry term')

  node.close()
})

test('empty public commands are rejected before forwarding or log mutation', async (t) => {
  const fixture = logFixture(t, '21-public-empty-command')
  const log = fixture.create()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log, opts)
  node.open()
  node._stopTimers()

  await node.onReceive('2', {
    type: 'append',
    term: 0n,
    termP: -1n,
    seqP: -1n,
    commitSeq: -1n,
    cid: 'establish-leader',
  })
  t.equal(node.leader, '2', 'test follower has an established forwarding target')
  sent.length = 0
  const before = log.seq

  const appendError = await rejection(node.append(Buffer.alloc(0)))
  const batchError = await rejection(node.appendBatch([
    Buffer.from('valid'),
    Buffer.alloc(0),
  ]))

  t.ok(appendError instanceof Error, 'append rejects an empty application command')
  t.ok(batchError instanceof Error, 'appendBatch rejects a batch containing an empty command')
  t.ok(appendError instanceof ErrorWithCode,
    'append input failure uses ErrorWithCode')
  t.equal(appendError.code, ARGUMENT_ILLEGAL,
    'append input failure uses ARGUMENT_ILLEGAL')
  t.equal(appendError.sqlCode, null,
    'append input failure has no SQLite code')
  t.ok(batchError instanceof ErrorWithCode,
    'appendBatch input failure uses ErrorWithCode')
  t.equal(batchError.code, ARGUMENT_ILLEGAL,
    'appendBatch input failure uses ARGUMENT_ILLEGAL')
  t.equal(batchError.sqlCode, null,
    'appendBatch input failure has no SQLite code')
  t.deepEqual(sent, [], 'invalid commands are not forwarded')
  t.equal(log.seq, before, 'invalid commands do not mutate the local log')

  node.close()
})
