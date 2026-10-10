import test from 'tape'
import { DatabaseSync } from 'node:sqlite'
import {
  ARGUMENT_ILLEGAL,
  ErrorWithCode,
  LOG_CORRUPT,
  NO_LEADER,
  RPC_ILLEGAL,
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

test('replication delays failed RPCs but backtracks immediately through divergent history', async (t) => {
  const requests = []
  const delays = []
  const byId = new Map()
  const nodes = ['1', '2'].map((id) => {
    const send = (to, msg) => {
      if (id === '1' && msg.data) {
        requests.push(msg.seqP)
        if (requests.length === 1) { return }
        if (requests.length === 2) {
          return byId.get(id).onReceive(to, {
            type: 'err', term: msg.term, cid: msg.cid,
            msg: 'rejected RPC', code: RPC_ILLEGAL,
          })
        }
      }
      return byId.get(to).onReceive(id, msg)
    }
    const node = new ProductionRaftNode(id, ids, send, ':memory:', {
      ...opts, pingTimeout: 1500, rpcMax: 2,
    })
    byId.set(id, node)
    return node
  })
  t.teardown(() => nodes.forEach((node) => node.close()))
  for (const node of nodes) {
    node.open()
    node._stopTimers()
  }
  const [leader, follower] = nodes
  leader.log.appendBatch(['a', 'b', 'c', 'd'].map((value) => entry(1n, value)))
  follower.log.appendBatch(['old-a', 'old-b'].map((value) => entry(0n, value)))
  leader.state = 'leader'
  leader.leader = leader.id
  leader.term = 1n
  leader._delay = async (ms) => { delays.push({ afterRequest: requests.length, ms }) }

  leader._catchUpFollower('2')
  const state = leader._replication.get('2')
  leader.log.append(entry(1n, 'e'))
  leader._catchUpFollower('2')
  t.equal(requests.length, 1, 'new work shares the pending replication worker')
  await state.work

  t.deepEqual(delays, [
    { afterRequest: 1, ms: 150 },
    { afterRequest: 2, ms: 150 },
  ], 'only the timeout and ordinary RPC error schedule a retry delay')
  t.deepEqual(requests, [2n, 2n, 2n, 1n, 0n, -1n, 1n, 3n],
    'retries failed pages, backtracks to the beginning, then advances by page')
  t.equal(state.matchIndex, leader.seq, 'catches up through the newly appended entry')
  t.deepEqual([...follower.log.iter(0n)], [...leader.log.iter(0n)],
    'replaces the divergent suffix and copies the complete leader history')
})

test('an earlier-term heartbeat ACK does not restore followers after reelection', async (t) => {
  const byId = new Map()
  const errors = []
  let phase = 'normal'
  let heldCid = null
  let heldAck = null
  const nodes = ids.map((id) => {
    const send = (to, msg) => {
      if (phase === 'isolate' && (id === '3' || to === '3')) { return }
      if (phase === 'capture' && id === '1' && to === '3' &&
          msg.type === 'append' && msg.data === undefined) {
        heldCid = msg.cid
      }
      if (phase === 'capture' && id === '3' && to === '1' && msg.cid === heldCid) {
        heldAck = msg
        return
      }
      return byId.get(to).onReceive(id, msg)
    }
    const node = new ProductionRaftNode(id, ids, send, ':memory:', {
      ...opts, appendTimeout: 1_000,
    })
    node.on('error', (err) => errors.push(err))
    byId.set(id, node)
    return node
  })
  t.teardown(() => nodes.forEach((node) => node.close()))
  nodes.forEach((node) => node.open())
  const leader = nodes[0]
  leader._voteForSelf()
  await waitFor(() => leader._commitTerm === leader.term)

  phase = 'capture'
  leader._pingFollowers()
  await sleep(0)
  t.equal(heldAck?.term, 1n, 'holds a heartbeat ACK from the first leadership')

  phase = 'isolate'
  await leader.onReceive('2', {
    type: 'vote_request', term: 2n, termP: leader.log.term, seqP: leader.log.seq,
  })
  leader._voteForSelf()
  await waitFor(() => leader._commitTerm === leader.term)
  t.equal(leader.term, 3n, 'the same node is reelected in term three')
  t.deepEqual(leader.followers, ['2'], 'only the responding peer is a current follower')
  t.ok(leader._acks.has(heldCid), 'the earlier heartbeat still has a pending response')

  const lastPong = leader._pongs.get('3')
  const committed = leader._commitSeq
  const changes = []
  leader.on('change', (change) => changes.push(change))
  await waitFor(() => Date.now() > lastPong)
  await leader.onReceive('3', heldAck)
  await sleep(0)

  t.notOk(leader._acks.has(heldCid), 'the old response still settles its request')
  t.deepEqual(leader.followers, ['2'], 'the old ACK does not add a current follower')
  t.equal(leader._pongs.get('3'), lastPong, 'the old ACK does not refresh liveness')
  t.deepEqual(changes, [], 'the old ACK does not publish a follower change')
  t.equal(leader._replication.get('3').matchIndex, -1n,
    'the isolated peer has no confirmed current-term replication')
  t.equal(leader._commitSeq, committed, 'the old ACK does not advance commitment')
  t.deepEqual(errors, [], 'both elections complete without fatal errors')
})

test('closing during a heartbeat warning does not restore follower state or timers', async (t) => {
  const byId = new Map()
  const errors = []
  const failure = new Error('heartbeat transport failed')
  let failSend = false
  const nodes = ids.map((id) => {
    const send = (to, msg) => {
      if (failSend && id === '1') { throw failure }
      return byId.get(to).onReceive(id, msg)
    }
    const node = new ProductionRaftNode(id, ids, send, ':memory:', {
      ...opts, appendTimeout: 1_000,
    })
    node.on('error', (err) => errors.push(err))
    byId.set(id, node)
    return node
  })
  t.teardown(() => nodes.forEach((node) => {
    node.close()
    node._stopTimers()
  }))
  nodes.forEach((node) => node.open())
  const leader = nodes[0]
  leader._voteForSelf()
  await waitFor(() => leader._applySeq >= 0n)

  const timer = leader._electionTimer
  const warnings = []
  leader.on('warn', (err) => {
    warnings.push(err)
    leader.close()
  })
  failSend = true
  leader._pingFollowers()
  await sleep(0)

  t.equal(warnings.length, 1, 'the first failed send warns and closes the node')
  t.equal(warnings[0]?.message, '(send) heartbeat transport failed',
    'the warning preserves the transport failure')
  t.notOk(leader.isOpen, 'the node remains closed after the heartbeat returns')
  t.notOk(leader.log.isOpen, 'storage remains closed')
  t.equal(leader.state, null, 'the heartbeat does not restore follower state')
  t.equal(leader._electionTimer, timer, 'does not replace the stopped election timer')
  t.equal(leader._acks.size, 0, 'no response waiters remain after shutdown')
  t.deepEqual(errors, [], 'shutdown produces no additional errors')
})

test('fatal election no-op failure publishes one terminal change', async (t) => {
  const byId = new Map()
  const errors = []
  const nodes = ids.map((id) => {
    const send = (to, msg) => byId.get(to).onReceive(id, msg)
    const node = new ProductionRaftNode(id, ids, send, ':memory:', opts)
    node.on('error', (err) => errors.push(err))
    byId.set(id, node)
    return node
  })
  t.teardown(() => nodes.forEach((node) => node.close()))
  nodes.forEach((node) => node.open())
  const leader = nodes[0]
  const changes = []
  leader.on('change', ({ state, open }) => changes.push([state, open]))
  leader.log.db.exec(`
    CREATE TRIGGER fail_noop BEFORE INSERT ON raft_log
    BEGIN SELECT RAISE(ABORT, 'election no-op failed'); END
  `)

  leader._voteForSelf()
  await sleep(0)

  t.deepEqual(changes, [['candidate', true], [null, false]],
    'election failure publishes the closed state exactly once')
  t.notOk(leader.isOpen, 'fatal initialization closes the node')
  t.notOk(leader.log.isOpen, 'fatal initialization closes storage')
  t.equal(errors.length, 1, 'reports one public error')
  t.match(errors[0]?.message ?? '', /election no-op failed/,
    'reports the storage error that prevented initialization')
})

test('an obsolete heartbeat round preserves a synchronously learned leader', async (t) => {
  const byId = new Map()
  const errors = []
  const oldRoundPeers = []
  let phase = 'normal'
  let heldHeartbeat = null
  let learned = null
  const nodes = ids.map((id) => {
    const send = (to, msg) => {
      if (phase === 'isolate' && (id === '1' || to === '1')) {
        if (id === '2' && to === '1' && msg.type === 'append' && msg.data === undefined) {
          heldHeartbeat = msg
        }
        return
      }
      if (id === '1' && msg.type === 'append' && msg.term === 1n && phase !== 'normal') {
        oldRoundPeers.push(to)
      }
      const result = byId.get(to).onReceive(id, msg)
      if (phase === 'deliver' && id === '1' && to === '2') {
        phase = 'delivered'
        const oldLeader = byId.get('1')
        oldLeader.onReceive('2', heldHeartbeat)
        learned = { leader: oldLeader.leader, pingms: oldLeader._pingms }
      }
      return result
    }
    const node = new ProductionRaftNode(id, ids, send, ':memory:', {
      ...opts, appendTimeout: 1_000,
      apply: (node, bufs) => bufs.map((buf) => buf?.toString() ?? null),
    })
    node.on('error', (err) => errors.push(err))
    byId.set(id, node)
    return node
  })
  t.teardown(() => nodes.forEach((node) => node.close()))
  nodes.forEach((node) => node.open())
  const [oldLeader, newLeader] = nodes
  oldLeader._voteForSelf()
  await waitFor(() => oldLeader._applySeq === 0n)

  phase = 'isolate'
  newLeader._voteForSelf()
  await waitFor(() => newLeader._applySeq === 1n)
  t.equal(heldHeartbeat?.term, 2n, 'holds a heartbeat from the new leader')

  phase = 'deliver'
  oldLeader._pingFollowers()

  t.equal(learned?.leader, '2', 'the transport delivers the new leader during the send')
  t.ok(learned?.pingms > 0, 'the received heartbeat records leader contact')
  t.equal(oldLeader.state, 'follower', 'the old leader steps down')
  t.equal(oldLeader.term, 2n, 'the old leader adopts the new term')
  t.equal(oldLeader.leader, '2', 'the old round preserves the newly learned leader')
  t.equal(oldLeader._pingms, learned?.pingms, 'the old round preserves the contact time')
  t.deepEqual(oldRoundPeers, ['2'], 'the obsolete round stops before sending to the next peer')

  const [, result] = await oldLeader.append(Buffer.from('forwarded'))
  t.equal(result, 'forwarded', 'an immediate command forwards successfully to the new leader')
  t.deepEqual(errors, [], 'both elections and forwarding complete without fatal errors')
})

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

test('a returning stale candidate cannot postpone elections indefinitely', async (t) => {
  const byId = new Map()
  const errors = []
  const denied = []
  const requests = []
  let isolated = false
  let healed = false
  let healAfterTerm = null
  const files = ids.map((id) => {
    const fixture = logFixture(t, `21-election-deadline-${id}`)
    fixture.create().del()
    return fixture.file
  })
  const readCommands = (idx) => {
    const db = new DatabaseSync(files[idx], { readOnly: true })
    try {
      return db.prepare('SELECT entry FROM raft_log ORDER BY seq').all()
        .map(({ entry }) => Buffer.from(entry).subarray(8).toString())
        .filter(Boolean)
    } finally { db.close() }
  }
  const nodes = ids.map((id, idx) => {
    const send = (to, msg) => {
      if (healAfterTerm !== null && id === '1' && msg.type === 'vote_request' &&
          msg.term > healAfterTerm) {
        isolated = false
        healed = true
        healAfterTerm = null
      }
      if (isolated && (id === '1' || to === '1')) { return }
      if (healed && to === '1' && msg.type === 'vote' && !msg.voteGranted) {
        denied.push(msg.term)
      }
      const receiver = byId.get(to)
      const state = receiver.state
      const timer = receiver._electionTimer
      const higherRequest = msg.type === 'vote_request' && msg.term > receiver.term
      const result = receiver.onReceive(id, msg)
      if (healed && higherRequest) {
        requests.push({ state, renewed: receiver._electionTimer !== timer,
          granted: receiver._votedFor === id })
      }
      return result
    }
    const node = new ProductionRaftNode(id, ids, send, files[idx], {
      electionTimeout: [40, 160, 200][idx], pingTimeout: 120,
      appendTimeout: 1_000, rpcMax: 1, applyMax: 1,
      apply: (node, bufs) => bufs.map((buf) => buf?.toString() ?? null),
    })
    node.on('error', (err) => errors.push(err))
    byId.set(id, node)
    return node
  })
  t.teardown(() => nodes.forEach((node) => node.close()))
  nodes.forEach((node) => node.open())
  await waitFor(() => nodes[0].state === 'leader')
  t.equal((await nodes[0].append(Buffer.from('base')))[1], 'base',
    'the shortest-timeout node initially leads and accepts a command')
  await waitFor(() => ids.every((id, idx) => readCommands(idx).includes('base')))

  isolated = true
  await waitFor(() => nodes.slice(1).some((node) => node.state === 'leader'))
  const successor = nodes.slice(1).find((node) => node.state === 'leader')
  t.equal((await successor.append(Buffer.from('newer')))[1], 'newer',
    'the other peers commit newer history during the partition')
  await waitFor(() => readCommands(1).includes('newer') && readCommands(2).includes('newer'))
  t.deepEqual(readCommands(0), ['base'], 'the isolated node has fallen behind')

  healAfterTerm = successor.term
  await waitFor(() => denied.length > 0)
  await waitFor(() => nodes.slice(1).some((node) => node.state === 'leader'), 2_000)
  const leader = nodes.slice(1).find((node) => node.state === 'leader')
  t.equal((await leader.append(Buffer.from('restored')))[1], 'restored',
    'a current peer wins despite the shorter election timeout on the stale node')
  await waitFor(() => nodes.every((node, idx) => readCommands(idx).includes('restored')))
  t.ok(nodes.every((node) => node.isOpen), 'progress resumes without closing any node')
  for (const [idx, id] of ids.entries()) {
    t.deepEqual(readCommands(idx), ['base', 'newer', 'restored'],
      `node ${id} durably retains every successful command in order`)
  }
  const rejectedFollowers = requests.filter(({ state, granted }) => state === 'follower' && !granted)
  t.ok(rejectedFollowers.length > 0 && rejectedFollowers.every(({ renewed }) => !renewed),
    'rejected requests preserve an existing follower election deadline')
  t.ok(requests.some(({ state, granted, renewed }) => state === 'leader' && !granted && renewed),
    'a leader rejecting a higher-term request starts its follower election timer')
  const grantedRequests = requests.filter(({ granted }) => granted)
  t.ok(grantedRequests.length > 0 && grantedRequests.every(({ renewed }) => renewed),
    'granting a vote still renews the election deadline')
  t.deepEqual(errors, [], 'the partition and recovery produce no fatal errors')
})

test('a vote reply precedes a change listener that delivers a newer vote request', async (t) => {
  const byId = new Map()
  const requests = []
  const replies = []
  const errors = []
  const nodes = ids.map((id) => {
    const send = (to, msg) => {
      if (id === '1' && msg.type === 'vote') {
        replies.push({ to, term: msg.term, granted: msg.voteGranted,
          election: { ...byId.get(id).log.elec } })
        return byId.get(to).onReceive(id, msg)
      }
      if (to === '1' && msg.type === 'vote_request') {
        requests.push({ from: id, msg })
      }
    }
    const node = new ProductionRaftNode(id, ids, send, ':memory:', {
      ...opts, electionTimeout: id === '1' ? 60_000 : 50,
    })
    node.on('error', (err) => errors.push(err))
    byId.set(id, node)
    return node
  })
  t.teardown(() => nodes.forEach((node) => node.close()))
  nodes.forEach((node) => node.open())
  await Promise.all(nodes.slice(1).map(async (node) => {
    await waitFor(() => node.term >= 2n)
    node._stopTimers()
  }))
  t.deepEqual(nodes.slice(1).map((node) => [node.term, node.state]),
    [[2n, 'candidate'], [2n, 'candidate']],
    'both real election timers have started a second election')

  const oldRequest = requests.find(({ from, msg }) => from === '2' && msg.term === 1n)
  const newRequest = requests.find(({ from, msg }) => from === '3' && msg.term === 2n)
  const follower = nodes[0]
  follower.on('change', ({ term }) => {
    if (term === 1n) { follower.onReceive(newRequest.from, newRequest.msg) }
  })
  await follower.onReceive(oldRequest.from, oldRequest.msg)

  t.deepEqual(replies, [
    { to: '2', term: 1n, granted: true, election: { term: 1n, votedFor: '2' } },
    { to: '3', term: 2n, granted: true, election: { term: 2n, votedFor: '3' } },
  ], 'each reply carries the term and vote already persisted when it is sent')
  t.deepEqual(nodes.slice(1).map((node) => [node.term, node.state]),
    [[2n, 'candidate'], [2n, 'leader']],
    'the delayed term-one grant cannot elect a second leader in term two')
  t.deepEqual(follower.log.elec, { term: 2n, votedFor: '3' },
    'the follower retains only the newer candidate vote for term two')
  t.deepEqual(errors, [], 'reentrant request delivery causes no fatal errors')
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

test('lost election state prevents startup and a second vote in the same term', async (t) => {
  for (const [name, sql] of [
    ['row', 'DELETE FROM raft_election'],
    ['table', 'DROP TABLE raft_election'],
  ]) {
    const fixture = logFixture(t, `21-lost-election-${name}`)
    fixture.create().del()
    const sent = []
    const send = (to, msg) => sent.push([to, { ...msg }])
    const nodes = []
    t.teardown(() => nodes.forEach((node) => node.close()))
    const create = () => {
      const node = new ProductionRaftNode('1', ids, send, fixture.file, opts)
      nodes.push(node)
      return node
    }
    const request = { type: 'vote_request', term: 4n, termP: -1n, seqP: -1n }
    const first = create()
    first.open()
    first._stopTimers()
    await first.onReceive('2', request)
    t.equal(sent.at(-1)[1].voteGranted, true, `${name}: grants the first vote`)
    t.equal(first.seq, -1n, `${name}: a durable vote does not require log entries`)
    first.close()

    const intact = create()
    intact.open()
    intact._stopTimers()
    await intact.onReceive('3', request)
    t.equal(sent.at(-1)[1].voteGranted, false, `${name}: intact restart preserves the vote`)
    intact.close()
    const db = new DatabaseSync(fixture.file)
    try { db.exec(sql) } finally { db.close() }

    const reopened = create()
    const changes = []
    reopened.on('change', (change) => changes.push(change))
    t.throws(() => reopened.open(), (err) => err.code === LOG_CORRUPT,
      `${name}: missing election state prevents startup`)
    t.notOk(reopened.isOpen, `${name}: Raft stays closed`)
    t.notOk(reopened.log.isOpen, `${name}: the log stays closed`)
    t.deepEqual(changes, [], `${name}: publishes no open state`)
    t.equal(reopened._electionTimer, undefined, `${name}: starts no election timer`)
    const count = sent.length
    await reopened.onReceive('3', request)
    t.equal(sent.length, count, `${name}: cannot grant a second vote after lost state`)
  }
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

test('invalid replicated entry terms are rejected before state changes or trimming', async (t) => {
  const fixture = logFixture(t, '21-invalid-replicated-terms')
  const log = fixture.create()
  const sent = []
  const errors = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), log, opts)
  t.teardown(() => node.close())
  node.on('error', (err) => errors.push(err))
  node.open()
  const original = [entry(1n, 'committed'), entry(1n, 'uncommitted')]
  await node.onReceive('2', {
    type: 'append', cid: 'initial-history', term: 1n,
    termP: -1n, seqP: -1n, commitSeq: 0n, data: original,
  })
  await node._applyPrev
  sent.length = 0
  const before = [node.state, node.leader, node.term, node._pingms]
  const election = { ...log.elec }
  const changes = []
  node.on('change', (change) => changes.push(change))
  const tooLarge = 9_223_372_036_854_775_808n
  const unsignedMax = 18_446_744_073_709_551_615n

  for (const [cid, term, seqP, data] of [
    ['append', 2n, 1n, [entry(tooLarge, 'invalid')]],
    ['replace', 1n, 0n, [entry(tooLarge, 'invalid')]],
    ['later-entry', 2n, 0n, [entry(2n, 'replacement'), entry(unsignedMax, 'invalid')]],
  ]) {
    await node.onReceive(term === 1n ? '2' : '3', {
      type: 'append', cid, term, termP: 1n, seqP, commitSeq: 1n, data,
    })
    t.deepEqual(sent.splice(0).map((msg) => [msg.cid, msg.type, msg.code, msg.term]),
      [[cid, 'err', RPC_ILLEGAL, 1n]], `${cid}: rejects with RPC_ILLEGAL in the existing term`)
    t.ok(node.isOpen, `${cid}: follower remains open`)
    t.deepEqual([...log.iter()], original, `${cid}: preserves committed and uncommitted history`)
    t.deepEqual([node.state, node.leader, node.term, node._pingms], before,
      `${cid}: preserves role, leader, term, and contact time`)
    t.deepEqual(log.elec, election, `${cid}: preserves the durable election state`)
    t.equal(node._commitSeq, 0n, `${cid}: does not advance commitment`)
  }
  t.deepEqual(changes, [], 'invalid entry terms publish no state changes')
  t.deepEqual(errors, [], 'invalid entry terms produce no fatal errors')

  const replacement = entry(1n, 'valid replacement')
  await node.onReceive('2', {
    type: 'append', cid: 'valid-replacement', term: 1n,
    termP: 1n, seqP: 0n, commitSeq: 1n, data: [replacement],
  })
  await node._applyPrev
  t.equal(sent[0]?.type, 'ack', 'a later valid replacement is acknowledged')
  t.deepEqual([...log.iter()], [original[0], replacement], 'valid reconciliation still replaces the suffix')
  t.equal(node._applySeq, 1n, 'the valid replacement commits and applies normally')
})

test('replicated entry terms accept both legal bounds', async (t) => {
  const sent = []
  const node = new ProductionRaftNode('1', ids, (to, msg) => sent.push(msg), ':memory:', opts)
  t.teardown(() => node.close())
  node.open()
  const maximum = 9_223_372_036_854_775_807n
  const data = [entry(0n), entry(maximum, 'maximum term')]

  await node.onReceive('2', {
    type: 'append', cid: 'legal-terms', term: maximum,
    termP: -1n, seqP: -1n, commitSeq: 1n, data,
  })
  await node._applyPrev

  t.equal(sent[0]?.type, 'ack', 'zero and the maximum term are acknowledged')
  t.deepEqual([...node.log.iter()], data, 'both entries are stored unchanged, including the no-op')
  t.equal(node._commitTerm, maximum, 'the maximum term can be committed')
  t.equal(node._applySeq, 1n, 'both entries are applied')
  t.ok(node.isOpen, 'the follower remains open')
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
