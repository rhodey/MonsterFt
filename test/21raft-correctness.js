import test from 'tape'
import { DatabaseSync } from 'node:sqlite'
import {
  ARGUMENT_ILLEGAL,
  ErrorWithCode,
  LOG_CORRUPT,
  NO_LEADER,
  RAFT_ILLEGAL,
  REPL_BACKTRACK,
  RPC_ILLEGAL,
  TERM_DIFF,
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
  return { term, entry: data === null ? Buffer.alloc(0) : Buffer.from(data) }
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

for (const delayMs of [0, 2]) {
  test(`public forwarded singles and batches use FWD_APPEND with ${delayMs ? 'delayed' : 'synchronous'} transport`, async (t) => {
    const byId = new Map()
    const sent = []
    const errors = []
    const applied = new Map(ids.map((id) => [id, []]))
    const timers = new Set()
    const nodes = ids.map((id) => {
      const send = (to, msg) => {
        sent.push({ from: id, to, msg })
        if (!delayMs) { return byId.get(to).onReceive(id, msg) }
        const timer = setTimeout(() => {
          timers.delete(timer)
          byId.get(to).onReceive(id, msg)
        }, delayMs)
        timers.add(timer)
      }
      const node = new ProductionRaftNode(id, ids, send, ':memory:', {
        ...opts, electionTimeout: id === '1' ? 50 : 60_000,
        pingTimeout: 100, appendTimeout: 1_000,
        apply: (node, bufs) => bufs.map((buf) => {
          if (buf === null) { return null }
          const result = buf.toString('hex')
          applied.get(id).push(result)
          return result
        }),
      })
      node.on('error', (err) => errors.push(err))
      byId.set(id, node)
      return node
    })
    t.teardown(() => {
      nodes.forEach((node) => node.close())
      timers.forEach((timer) => clearTimeout(timer))
    })
    nodes.forEach((node) => node.open())
    const [leader, follower] = nodes
    await waitFor(() => nodes.every((node) => node._applySeq === 0n))
    t.equal(leader.state, 'leader', 'a real election installs the forwarding target')
    const begin = leader.seq + 1n
    const single = Buffer.from([0, 255, 1])
    const batch = [Buffer.from('first'), Buffer.from([255, 0, 2])]
    const results = [single, ...batch].map((buf) => buf.toString('hex'))

    t.deepEqual(await follower.append(single), [begin, results[0]],
      'a forwarded single returns its sequence and application result')
    t.deepEqual(await follower.appendBatch(batch), [begin + 1n, results.slice(1)],
      'a forwarded batch returns its first sequence and ordered results')
    await waitFor(() => nodes.every((node) => node._applySeq === leader.seq))

    const forwarded = sent.filter(({ msg }) => msg.type === 'fwd_append')
    t.equal(forwarded.length, 2, 'both public calls use the dedicated forwarding type')
    t.deepEqual(forwarded.map(({ from, to, msg }) => [from, to, msg.term]),
      [['2', '1', leader.term], ['2', '1', leader.term]],
      'both requests address the known leader in the current term')
    t.equal(forwarded[0]?.msg.data, single, 'the single payload remains a Buffer')
    t.deepEqual(forwarded[1]?.msg.data, batch, 'the batch remains an array of payload Buffers')
    t.ok(forwarded.every(({ msg }) => typeof msg.cid === 'string' && !Object.hasOwn(msg, 'termP')),
      'forwarding keeps correlation IDs and does not add a replication predecessor')
    const replication = sent.filter(({ msg }) => msg.type === 'append' && msg.data !== undefined)
    t.ok(replication.length > 0 && replication.every(({ msg }) =>
      Array.isArray(msg.data) && msg.data.every((record) =>
        typeof record.term === 'bigint' && Buffer.isBuffer(record.entry))),
    'APPEND carries replication records rather than forwarded payloads')
    for (const node of nodes) {
      t.deepEqual(applied.get(node.id), results, `${node.id}: applies each command once in order`)
    }
    t.deepEqual(errors, [], 'forwarding and replication complete without fatal errors')
  })
}

for (const termDelta of [0n, 1n]) {
  test(`an elected leader accepts ${termDelta ? 'higher' : 'same'}-term APPEND as replication`, async (t) => {
    const byId = new Map()
    const sent = []
    const errors = []
    let isolated = false
    const nodes = ids.map((id) => {
      const send = (to, msg) => {
        if (isolated) { sent.push({ from: id, to, msg }); return }
        return byId.get(to).onReceive(id, msg)
      }
      const node = new ProductionRaftNode(id, ids, send, ':memory:', {
        ...opts, electionTimeout: id === '1' ? 50 : 60_000,
        pingTimeout: 100, appendTimeout: 1_000,
      })
      node.on('error', (err) => errors.push(err))
      byId.set(id, node)
      return node
    })
    t.teardown(() => nodes.forEach((node) => node.close()))
    nodes.forEach((node) => node.open())
    const leader = nodes[0]
    await waitFor(() => nodes.every((node) => node._applySeq === 0n))
    t.equal(leader.state, 'leader', 'the target begins as an elected leader')
    const originalTerm = leader.term
    const original = [...leader.log.iter()]
    const acceptedTerm = originalTerm + termDelta
    const payload = entry(acceptedTerm, 'replicated command')
    const request = {
      type: 'append', cid: 'replication-to-leader', term: acceptedTerm,
      termP: leader.log.term, seqP: leader.seq, commitSeq: leader.seq + 1n,
      data: [payload],
    }
    isolated = true

    await leader.onReceive('2', { ...request, cid: 'older-leader', term: originalTerm - 1n })
    t.deepEqual(sent.map(({ msg }) => [msg.type, msg.code, msg.term]),
      [['err', TERM_DIFF, originalTerm]], 'a lower-term APPEND is rejected')
    t.deepEqual([leader.state, leader.leader, leader.term], ['leader', '1', originalTerm],
      'rejecting the lower term preserves leadership')
    t.deepEqual([...leader.log.iter()], original, 'the lower-term request cannot change history')
    sent.length = 0

    await leader.onReceive('2', request)
    await leader._applyPrev
    t.deepEqual([leader.state, leader.leader, leader.term], ['follower', '2', acceptedTerm],
      'APPEND installs the sender as leader through the replication handler')
    t.deepEqual(sent.map(({ msg }) => [msg.type, msg.term, msg.seq]),
      [['ack', acceptedTerm, request.seqP + 1n]], 'the record is acknowledged as replication')
    t.notOk(Object.hasOwn(sent[0].msg, 'results'), 'the replication ACK has no forwarded result')
    t.deepEqual([...leader.log.iter()], [...original, payload],
      'replication preserves committed history and appends the new record')
    t.equal(leader._applySeq, request.commitSeq, 'the received commit advances application')
    const election = leader.log.db.prepare(`
      SELECT current_term AS term, voted_for AS votedFor FROM raft_election WHERE id = 1
    `)
    election.setReadBigInts(true)
    t.deepEqual({ ...election.get() }, {
      term: acceptedTerm, votedFor: termDelta ? null : '1',
    }, 'the accepted term and appropriate vote are durable')
    t.deepEqual(errors, [], 'stepping down and accepting replication causes no fatal errors')
  })
}

for (const delayMs of [0, 2]) {
  for (const prefixLength of [0, 8]) {
    test(`a newly elected leader skips a large missing ${prefixLength ? 'partial' : 'empty'} follower tail with ${delayMs ? 'delayed' : 'synchronous'} transport`, async (t) => {
      const byId = new Map()
      const timers = new Set()
      const errors = []
      const pages = []
      const backtracks = []
      const matches = []
      const applied = new Map(ids.map((id) => [id, []]))
      let isolated = prefixLength === 0
      const nodes = ids.map((id) => {
        const send = (to, msg) => {
          if (isolated && (id === '3' || to === '3')) { return }
          if (id === '2' && to === '3' && msg.type === 'append' && Array.isArray(msg.data)) {
            pages.push(msg)
          }
          if (id === '3' && to === '2' && msg.code === REPL_BACKTRACK) {
            backtracks.push(msg)
          }
          const deliver = () => {
            if (isolated && (id === '3' || to === '3')) { return }
            return byId.get(to).onReceive(id, msg)
          }
          if (!delayMs) { return deliver() }
          const timer = setTimeout(() => {
            timers.delete(timer)
            deliver()
          }, delayMs)
          timers.add(timer)
        }
        const node = new ProductionRaftNode(id, ids, send, ':memory:', {
          electionTimeout: { 1: 40, 2: 160, 3: 60_000 }[id],
          pingTimeout: 250, appendTimeout: 2_000, rpcMax: 16,
          apply: (node, bufs) => bufs.map((buf) => {
            if (buf === null) { return null }
            const value = buf.toString()
            applied.get(id).push(value)
            return value
          }),
        })
        node.on('error', (err) => errors.push(err))
        if (id === '2') {
          node.on('warn', (err) => {
            if (err.code === REPL_BACKTRACK) {
              matches.push(node._replication.get('3').matchIndex)
            }
          })
        }
        byId.set(id, node)
        return node
      })
      t.teardown(() => {
        nodes.forEach((node) => node.close())
        timers.forEach((timer) => clearTimeout(timer))
      })
      nodes.forEach((node) => node.open())
      const [original, successor, lagging] = nodes
      await waitFor(() => original.state === 'leader' && successor._applySeq === 0n)
      const firstTerm = original.term
      const prefix = Array.from({ length: prefixLength }, (_, i) => `prefix-${i}`)
      if (prefix.length) {
        await original.appendBatch(prefix.map((value) => Buffer.from(value)))
        await waitFor(() => lagging._applySeq === original.seq)
        isolated = true
      }
      const missing = Array.from({ length: 256 }, (_, i) => `missing-${i}`)
      await original.appendBatch(missing.map((value) => Buffer.from(value)))
      await waitFor(() => successor._applySeq === original.seq)
      const retainedSeq = lagging.seq
      const oldHead = original.seq
      t.equal(retainedSeq, prefix.length ? BigInt(prefix.length) : -1n,
        'the follower retains only the intended prefix while the majority advances')
      original.close()
      isolated = false

      await waitFor(() => successor.state === 'leader' && lagging._applySeq === oldHead + 1n, 2_000)
      t.ok(successor.term > firstTerm, 'a real election changes leadership and term')
      t.equal(backtracks.length, 1, 'one rejected page locates the missing history')
      t.equal(backtracks[0]?.logSeq, retainedSeq, 'the rejection advertises the cached follower head')
      t.equal(pages[0]?.seqP, oldHead, 'the new leader first probes its own previous head')
      t.equal(pages[1]?.seqP, retainedSeq, 'the next page immediately starts after the follower head')
      t.deepEqual(matches, [-1n], 'a hint alone does not establish any matched history')
      const expected = [entry(firstTerm), ...[...prefix, ...missing].map((value) => entry(firstTerm, value)),
        entry(successor.term)]
      for (const node of [successor, lagging]) {
        t.deepEqual([...node.log.iter()], expected, `${node.id}: retains exact terms and payloads after catch-up`)
        t.deepEqual(applied.get(node.id), [...prefix, ...missing],
          `${node.id}: applies every command once in order`)
      }
      t.deepEqual(await lagging.append(Buffer.from('continued')), [oldHead + 2n, 'continued'],
        'a subsequent forwarded append returns its ordinary sequence and result')
      await waitFor(() => lagging._applySeq === successor.seq)
      t.deepEqual([...lagging.log.iter()], [...expected, entry(successor.term, 'continued')],
        'the caught-up follower continues replicating and applying new commands')
      t.equal(backtracks.length, 1, 'normal replication continues without another backtrack')
      t.deepEqual(errors, [], 'leadership change and catch-up produce no fatal errors')
    })
  }
}

for (const followerLength of [2, 4, 6]) {
  test(`replication delays failed RPCs but backtracks one entry through ${followerLength}-entry divergent history`, async (t) => {
    const requests = []
    const delays = []
    const backtracks = []
    const byId = new Map()
    const nodes = ['1', '2'].map((id) => {
      const send = (to, msg) => {
        if (id === '2' && msg.code === REPL_BACKTRACK) { backtracks.push(msg.logSeq) }
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
    follower.log.appendBatch(Array.from({ length: followerLength }, (_, i) => entry(0n, `old-${i}`)))
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
    t.deepEqual(backtracks, Array(3).fill(BigInt(followerLength - 1)),
      'shorter, equal, or longer heads cannot skip a predecessor term mismatch')
    t.equal(state.matchIndex, leader.seq, 'catches up through the newly appended entry')
    t.deepEqual([...follower.log.iter(0n)], [...leader.log.iter(0n)],
      'replaces the divergent suffix and copies the complete leader history')
  })
}

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
        .map(({ entry }) => Buffer.from(entry).toString())
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
  t.equal(log.seq, 1n, 'the no-op entry is appended')
  t.equal(log.head.length, 0, 'the no-op entry has an empty application payload')
  t.deepEqual(applied, ['old-term-command', null],
    'the internal no-op is applied as null')
  t.deepEqual(appliedTerms, [1n, 2n], 'application observes each stored entry term')

  node.close()
})

test('invalid replicated records are rejected before state changes or trimming', async (t) => {
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
    ['negative-term', 2n, 0n, [entry(-1n, 'invalid')]],
    ['number-term', 2n, 0n, [entry(1, 'invalid')]],
    ['string-term', 2n, 0n, [entry('1', 'invalid')]],
    ['missing-term', 2n, 0n, [{ entry: Buffer.from('invalid') }]],
    ['missing-entry', 2n, 0n, [{ term: 1n }]],
    ['string-entry', 2n, 0n, [{ term: 1n, entry: 'invalid' }]],
    ['typed-array-entry', 2n, 0n, [{ term: 1n, entry: new Uint8Array([1]) }]],
    ['null-entry', 2n, 0n, [{ term: 1n, entry: null }]],
    ['null-record', 2n, 0n, [null]],
    ['array-record', 2n, 0n, [[]]],
    ['old-prefixed-buffer', 2n, 0n, [Buffer.from([1, 0, 0, 0, 0, 0, 0, 0, 97])]],
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
  t.deepEqual(changes, [], 'invalid records publish no state changes')
  t.deepEqual(errors, [], 'invalid records produce no fatal errors')

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

test('reconciliation compares both record term and payload without replacing committed history', async (t) => {
  for (const [label, replacement] of [
    ['term conflict', entry(2n, 'same payload')],
    ['payload conflict', entry(1n, 'different payload')],
  ]) {
    for (const committed of [false, true]) {
      const name = `${label}, ${committed ? 'committed' : 'uncommitted'}`
      const fixture = logFixture(t, `21-record-conflict-${label.replace(' ', '-')}-${committed}`)
      fixture.create().del()
      const log = fixture.create()
      const sent = []
      const errors = []
      const applied = []
      const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), log, {
        ...opts,
        apply: (node, bufs, seqs, terms) => {
          applied.push(...bufs.map((buf, index) => [terms[index], buf?.toString()]))
          return bufs
        },
      })
      t.teardown(() => node.close())
      node.on('error', (err) => errors.push(err))
      node.open()
      const original = [entry(1n, 'prefix'), entry(1n, 'same payload'), entry(1n, 'suffix')]
      await node.onReceive('2', {
        type: 'append', cid: 'seed-records', term: 2n,
        termP: -1n, seqP: -1n, commitSeq: committed ? 1n : 0n, data: original,
      })
      await node._applyPrev
      sent.length = 0
      const failed = committed ? new Promise((res) => node.once('error', res)) : null
      await node.onReceive('2', {
        type: 'append', cid: 'replace-record', term: 2n,
        termP: 1n, seqP: 0n, commitSeq: 1n, data: [replacement],
      })
      if (committed) {
        const err = await failed
        t.equal(err.code, RAFT_ILLEGAL, `${name}: rejects a committed conflict`)
        t.notOk(node.isOpen, `${name}: closes on the invariant failure`)
        t.deepEqual(sent, [], `${name}: does not acknowledge the conflict`)
        const probe = fixture.create()
        probe.open()
        t.deepEqual([...probe.iter()], original, `${name}: preserves every durable record`)
        probe.close()
      } else {
        await node._applyPrev
        t.equal(sent[0]?.type, 'ack', `${name}: acknowledges reconciliation`)
        t.deepEqual([...log.iter()], [original[0], replacement],
          `${name}: replaces the differing record and drops the suffix`)
        t.deepEqual(applied, [[1n, 'prefix'], [replacement.term, replacement.entry.toString()]],
          `${name}: applies only the retained and replacement records`)
        t.equal(node._commitTerm, replacement.term, `${name}: commits the replacement term`)
        t.deepEqual(errors, [], `${name}: produces no application failure`)
      }
      node.close()
    }
  }
})

test('replication pages retain mixed terms and raw payloads through a public election and append', async (t) => {
  const history = [entry(0n), entry(1n, 'a'), entry(1n, Buffer.from([0, 255])), entry(2n, 'last')]
  const byId = new Map()
  const pages = []
  const applied = new Map()
  const errors = []
  const nodes = ids.map((id) => {
    const fixture = logFixture(t, `21-mixed-record-pages-${id}`)
    const seed = fixture.create()
    seed.del()
    seed.open()
    if (id !== '3') { seed.appendBatch(history) }
    seed.election(2n, null)
    seed.close()
    applied.set(id, [])
    const send = (to, msg) => {
      if (id === '1' && to === '3' && Array.isArray(msg.data)) {
        pages.push({ seqP: msg.seqP, termP: msg.termP, data: msg.data })
      }
      return byId.get(to).onReceive(id, msg)
    }
    const node = new ProductionRaftNode(id, ids, send, fixture.file, {
      electionTimeout: id === '1' ? 50 : 60_000,
      pingTimeout: 100,
      appendTimeout: 2_000,
      rpcMax: 2,
      applyMax: 2,
      apply: (node, bufs, seqs, terms) => {
        applied.get(id).push(...bufs.map((buf, i) => ({ term: terms[i], entry: buf })))
        return bufs
      },
    })
    node.on('error', (err) => errors.push(err))
    byId.set(id, node)
    return node
  })
  t.teardown(() => nodes.forEach((node) => node.close()))
  nodes.forEach((node) => node.open())
  await Promise.all(nodes.map((node) => node.awaitLeader(true)))
  const [leader, , follower] = nodes
  const payload = Buffer.from([255, 0, 1])
  await follower.append(payload)
  await waitFor(() => nodes.every((node) => node._applySeq === leader.seq))

  t.equal(leader.state, 'leader', 'the seeded node becomes leader through an election')
  t.deepEqual(pages.find((page) => page.seqP === -1n)?.data, history.slice(0, 2),
    'the first page preserves an empty payload and two distinct terms')
  const second = pages.find((page) => page.seqP === 1n)
  t.equal(second?.termP, 1n, 'the next page reads the predecessor term from its record')
  t.deepEqual(second?.data, history.slice(2), 'the next page preserves binary bytes and mixed terms')
  const expected = [...history, entry(leader.term), entry(leader.term, payload)]
  for (const node of nodes) {
    t.deepEqual([...node.log.iter()], expected, `${node.id}: stores identical record fields`)
    t.deepEqual(applied.get(node.id), expected.map(({ term, entry }) => ({
      term, entry: entry.length ? entry : null,
    })), `${node.id}: application receives payloads and separate terms, with null no-ops`)
  }
  t.deepEqual(errors, [], 'election, replication, and forwarded append complete without errors')
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
