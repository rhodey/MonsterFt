import test from 'tape'
import { isDeepStrictEqual } from 'node:util'
import { pack } from 'msgpackr'
import { cluster, until, pause } from './helper.js'
import { protocol, append, record, cmd, messages, outputs } from './fixtures/protocol.js'
import { RaftEngine } from '../src/raft.js'
import { checksum } from '../src/wire.js'
import * as Err from '../src/error.js'

const keys = (value) => Object.keys(value).sort()
const cmdKeys = ['type', 'cmdSeq', 'cmdCount'].sort()
const syncKeys = ['type', 'cmdSeq', 'syncSeq', 'quorum', 'agree', 'disagree'].sort()
const changeKeys = [
  'state', 'leader', 'term', 'seq', 'commitSeq', 'commitTerm', 'followers',
].sort()

test('change comparison uses every scalar and ignores the baseline of other event types', (t) => {
  const log = { seq: -1n, elec: { term: 0n, votedFor: null } }
  const store = { log, load: () => ({ applied_seq: -1n }) }
  const events = []
  const engine = new RaftEngine({
    id: 'a', nodes: ['a', 'b', 'c'], store, executor: { timems: 0 }, random: null,
    output: (to, message) => events.push(message),
    queueLocalEvent: () => {},
  })
  engine.change()
  const initial = events.shift().status
  for (const [key, value] of [
    ['state', 'candidate'], ['leader', 'b'], ['term', 1n],
    ['seq', 0n], ['commitSeq', 0n], ['commitTerm', 0n],
  ]) {
    const target = key === 'seq' ? log : engine
    const previous = target[key]
    target[key] = value
    for (const type of ['commit', 'cmd', 'sync']) { engine.event(type) }
    engine.change()
    engine.change()
    target[key] = previous
    engine.change()
    t.deepEqual(events.map(({ type }) => type), ['commit', 'cmd', 'sync', 'change', 'change'],
      `${key}: other events preserve the comparison baseline and only the duplicate is suppressed`)
    t.deepEqual(events[3]?.status, { ...initial, [key]: value }, `${key}: the changed value is captured`)
    t.deepEqual(events[4]?.status, initial, `${key}: returning to the earlier snapshot is observable`)
    events.length = 0
  }
  t.end()
})

test('follower changes report uncommitted appends and truncation after the complete input', (t) => {
  const f = protocol().setup()
  try {
    const status = {
      state: 'follower', leader: 'b', term: 1n,
      seq: 2n, commitSeq: 0n, commitTerm: 1n, followers: [],
    }
    const page = append({
      seqP: 0n, termP: 1n, commitSeq: 0n, data: [record(1n, 'one'), record(1n, 'two')],
    })
    const grown = f.rpc(page)
    t.deepEqual(messages(grown, 'change', 'gate').map(({ status }) => status), [status],
      'uncommitted growth reports one final snapshot')
    t.equal(outputs(grown).at(-1).message.type, 'change', 'change follows the replication ACK')
    t.equal(messages(f.rpc(page), 'change', 'gate').length, 0,
      'repeating the same page leaves the snapshot unchanged')
    const shortened = f.rpc({ ...page, data: [record(1n, 'replacement')] })
    t.deepEqual(messages(shortened, 'change', 'gate').map(({ status }) => status),
      [{ ...status, seq: 1n }], 'trim and append report only the resulting head, with no commit change')
    t.equal(outputs(shortened).at(-1).message.type, 'change')
    const replaced = f.rpc({ ...page, data: [record(1n, 'another replacement')] })
    t.equal(messages(replaced, 'change', 'gate').length, 0,
      'replacing payload bytes alone does not change the public snapshot')
  } finally { f.close() }
  t.end()
})

test('candidate votes become sorted followers only when the final state is leader', (t) => {
  const f = protocol({}, ['e', 'd', 'a', 'c', 'b'])
  try {
    const election = f.tick(f.proc.executor.timers.get(f.proc.engine.electionTimer).due)
    t.deepEqual(messages(election, 'change', 'gate').map(({ status }) => [status.state, status.followers]),
      [['candidate', []]], 'starting an election reports no followers')
    const term = f.proc.engine.term
    const partial = f.rpc({ type: 'vote', term, voteGranted: true }, 'd')
    t.equal(messages(partial, 'change', 'gate').length, 0, 'a vote below quorum changes no public field')
    t.deepEqual(f.proc.engine.followers, [], 'a candidate does not expose voters as followers')
    const completions = new Map()
    f.onSend = (proc, reply) => completions.set(proc.role, reply)
    const elected = f.rpc({ type: 'vote', term, voteGranted: true }, 'b')
    const expected = {
      state: 'leader', leader: 'a', term, seq: 0n,
      commitSeq: -1n, commitTerm: -1n, followers: ['b', 'd'],
    }
    t.deepEqual(messages(elected, 'change', 'gate').map(({ status }) => status), [expected],
      'leadership, sorted voters, and the election no-op appear in one final snapshot')
    t.equal(outputs(elected).at(-1).message.type, 'change')
    const hashes = completions.get('procA').outputs.map(checksum)
    for (const role of ['procB', 'procC']) {
      const expected = hashes.map((hash) => role === 'procB' ? hash.subarray(0, 8) : hash.subarray(8))
      t.deepEqual(completions.get(role).outputs, expected, `${role} agrees on its half of every checksum`)
    }
    const steppedDown = f.rpc({ type: 'vote_request', term: term + 1n, termP: -1n, seqP: -1n }, 'e')
    t.deepEqual(messages(steppedDown, 'change', 'gate').map(({ status }) =>
      [status.state, status.followers]), [['follower', []]], 'stepping down clears public followers')
  } finally { f.close() }
  t.end()
})

test('backtracking replies report newly live followers without a successful replication ACK', (t) => {
  const f = protocol().setup()
  try {
    const elected = f.elect()
    const page = messages(elected, 'append', 'c').find((message) => message.data)
    const reply = f.rpc({
      type: 'err', term: page.term, cid: page.cid,
      code: Err.REPL_BACKTRACK, message: 'missing predecessor', logSeq: -1n,
    }, 'c')
    t.deepEqual(messages(reply, 'change', 'gate').map(({ status }) => status.followers),
      [['b', 'c']], 'the backtracking responder becomes visible immediately')
    t.equal(messages(reply, 'commit', 'gate').length, 0, 'liveness does not imply commitment')
    t.equal(outputs(reply).at(-1).message.type, 'change', 'the retry is buffered before final change')
    t.ok(messages(reply, 'append', 'c').some((message) => message.seqP === -1n),
      'ordinary backtracking continues')
  } finally { f.close() }
  t.end()
})

test('setup and CMD receipt imports do not publish apply-only change events', (t) => {
  const f = protocol()
  try {
    const setup = f.rpc(append({ data: [record()], commitSeq: 0n }))
    const setupResult = f.complete(messages(setup, 'execute', 'apply')[0])
    t.equal(outputs(setupResult).filter(({ to, message }) =>
      to === 'gate' && ['change', 'commit', 'cmd', 'sync'].includes(message.type)).length, 0,
      'setup completion publishes no extra event')
    t.equal(f.proc.engine.appliedSeq, 0n, 'setup still advances internal applied state')
    const command = f.rpc(append({
      seqP: 0n, termP: 1n, data: [record(1n, cmd())], commitSeq: 1n,
    }))
    const completed = f.complete(messages(command, 'execute', 'apply')[0])
    const events = outputs(completed).filter(({ to, message }) =>
      to === 'gate' && ['change', 'commit', 'cmd', 'sync'].includes(message.type)).map(({ message }) => message)
    t.deepEqual(events.map(({ type }) => type), ['cmd'], 'CMD completion reports only command progress')
    t.deepEqual(keys(events[0].status), changeKeys, 'CMD internal status also omits appliedSeq')
    t.equal(events[0].cmdSeq, 1n)
    t.equal(f.state().meta.applied_seq, 1n, 'receipt checkpoint remains durable')
    t.equal(f.proc.engine.pendingCommand.cmdSeq, 1n)
  } finally { f.close() }
  t.end()
})

test('successful heartbeat responses suppress identical change snapshots', (t) => {
  const f = protocol().leader()
  try {
    const now = f.proc.executor.timems
    for (const offset of [45, 90]) {
      const tick = f.tick(now + offset)
      const heartbeat = messages(tick, 'append', 'b').find((msg) => msg.data === undefined)
      const response = f.rpc({ type: 'ack', cid: heartbeat.cid, term: heartbeat.term, seq: 1n }, 'b')
      t.equal(messages(response, 'change', 'gate').length, 0,
        'successful heartbeat publishes no duplicate change output')
    }
  } finally { f.close() }
  t.end()
})

test('follower membership is sorted regardless of membership and response order', (t) => {
  for (const peers of [['c', 'b'], ['b', 'c']]) {
    const f = protocol({}, [peers[0], 'a', peers[1]]).leader()
    try {
      const now = f.proc.executor.timems
      const respond = (tick, peer) => {
        const heartbeat = messages(tick, 'append', peer).find((msg) => msg.data === undefined)
        return f.rpc({ type: 'ack', cid: heartbeat.cid, term: heartbeat.term, seq: 1n }, peer)
      }
      const tick = f.tick(now + 45)
      const joined = respond(tick, peers[1])
      t.deepEqual(messages(joined, 'change', 'gate').map(({ status }) => status.followers),
        [['b', 'c']], 'a new member produces one sorted snapshot')
      t.equal(messages(respond(tick, peers[0]), 'change', 'gate').length, 0,
        'refreshing an existing member does not change the snapshot')
      const next = f.tick(now + 90)
      for (const peer of peers) {
        t.equal(messages(respond(next, peer), 'change', 'gate').length, 0,
          'reversing response order leaves membership unchanged')
      }
      for (const offset of [135, 180, 225]) {
        t.equal(messages(respond(f.tick(now + offset), 'b'), 'change', 'gate').length, 0,
          'membership remains unchanged while the other follower is still live')
      }
      const expired = f.tick(now + 270)
      t.deepEqual(messages(expired, 'change', 'gate').map(({ status }) => status.followers),
        [['b']], 'the clock input itself reports expired membership')
      t.equal(messages(respond(expired, 'b'), 'change', 'gate').length, 0,
        'a later response does not repeat the expiration')
      t.deepEqual(messages(respond(expired, 'c'), 'change', 'gate').map(({ status }) => status.followers),
        [['b', 'c']], 'returning to previous membership still produces a change')
      t.equal(f.diagnostics.filter(({ type }) => type === 'error').length, 0)
    } finally { f.close() }
  }
  t.end()
})

test('an input refreshes an expiring follower before publishing final membership', (t) => {
  const f = protocol().leader()
  try {
    const now = f.proc.executor.timems
    const heartbeat = (reply, peer) => messages(reply, 'append', peer).find((message) => !message.data)
    const ack = (request, peer, timems = f.proc.executor.timems) => f.rpc({
      type: 'ack', cid: request.cid, term: request.term, seq: 1n,
    }, peer, timems)
    const first = f.tick(now + 45)
    ack(heartbeat(first, 'b'), 'b')
    ack(heartbeat(first, 'c'), 'c')
    ack(heartbeat(f.tick(now + 90), 'b'), 'b')
    const later = f.tick(now + 135)
    ack(heartbeat(later, 'b'), 'b')
    const refreshed = ack(heartbeat(later, 'c'), 'c', now + 195)
    t.equal(messages(refreshed, 'change', 'gate').length, 0,
      'refresh at the liveness deadline produces no transient expired/rejoined notifications')
    t.deepEqual(f.proc.engine.followers, ['b', 'c'])
    t.equal(f.proc.engine.state, 'leader', 'the due heartbeat observes the refreshed membership')
  } finally { f.close() }
  t.end()
})

test('a fresh engine publishes its initial change snapshot on every restart', (t) => {
  const f = protocol().setup()
  try {
    const changes = []
    f.onSend = (proc, reply) => {
      if (proc.role === 'procA') { changes.push(...messages(reply, 'change', 'gate')) }
    }
    f.reopen()
    f.reopen()
    t.equal(changes.length, 2, 'each restart publishes an initial snapshot')
    t.deepEqual(changes[0].status, {
      state: 'follower', leader: null, term: 1n, seq: 0n,
      commitSeq: 0n, commitTerm: 1n, followers: [],
    }, 'the initial snapshot includes restored durable progress')
    t.deepEqual(changes[1], changes[0], 'identical snapshots from separate lifetimes are retained')
  } finally { f.close() }
  t.end()
})

test('applying SYNC still pumps queued commands and reports the resulting append', (t) => {
  const f = protocol().leader()
  const page = (reply) => messages(reply, 'append', 'b').find((msg) => msg.data)
  const ack = (message) => f.rpc({
    type: 'ack', cid: message.cid, term: message.term, seq: message.seqP + 1n,
  })
  try {
    const first = f.input('append', { clientId: 'first', items: [Buffer.from('first')] })
    t.equal(messages(first, 'change', 'gate').at(-1).status.seq, 1n, 'CMD append reports its log head')
    const committed = ack(page(first))
    t.deepEqual(outputs(committed).filter(({ to, message }) =>
      to === 'gate' && ['change', 'commit', 'cmd', 'sync'].includes(message.type)).map(({ message }) => message.type),
      ['commit', 'change'], 'commit remains observable without a duplicate replication change')
    f.complete(messages(committed, 'execute', 'apply')[0])
    f.input('append', { clientId: 'second', items: [Buffer.from('second')] })
    const decision = f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest: Buffer.alloc(16) }, 'b')
    const finished = ack(page(decision))
    t.equal(f.proc.engine.appliedSeq, 2n, 'SYNC advances internal applied progress')
    t.equal(f.proc.engine.leaderCommand.cmdSeq, 3n, 'next command is started')
    t.deepEqual(messages(finished, 'change', 'gate').map(({ status }) => status.seq), [3n],
      'commit, SYNC, and the next append produce one final change snapshot')
    t.equal(messages(finished, 'sync', 'gate')[0].syncSeq, 2n)
    t.deepEqual(outputs(finished).filter(({ to, message }) =>
      to === 'gate' && ['change', 'commit', 'cmd', 'sync'].includes(message.type)).map(({ message }) =>
      [message.type, message.status.seq]), [['commit', 2n], ['sync', 2n], ['change', 3n]],
    'progress events retain capture-time status before the final change')
    t.equal(outputs(finished).at(-1).message.type, 'change')
  } finally { f.close() }
  t.end()
})

for (const delay of [0, 2]) {
  test(`progress events expose their current public state (${delay}ms transport)`, async (t) => {
    const c = cluster({ delay, localDelay: delay })
    try {
      const samples = []
      for (const { gate, procA } of c.nodes.values()) {
        t.notOk('appliedSeq' in gate, 'Gate has no public applied checkpoint before open')
        gate.on('commit', (seq) => {
          const record = procA.db.prepare('SELECT term FROM raft_log WHERE seq = ?').get(seq)
          samples.push({ id: gate.id, type: 'commit', seq, commitSeq: gate.commitSeq,
            commitTerm: gate.commitTerm, storedTerm: record.term })
        })
        for (const type of ['cmd', 'sync']) {
          gate.on(type, (event) => samples.push({ id: gate.id, type, event,
            commitSeq: gate.commitSeq }))
        }
        gate.on('change', (event) => samples.push({
          id: gate.id, type: 'change', event,
          state: Object.fromEntries(changeKeys.map((key) => [key, gate[key]])),
        }))
      }
      await c.open()
      await c.settled()
      await Promise.all(c.gates().flatMap((gate) => [gate.awaitLeader(), gate.awaitLeader(true)]))

      const waits = c.gates().flatMap((gate) => [
        gate.awaitEvent('commit', (seq) => gate.commitSeq === seq),
        gate.awaitEvent('cmd', (event) => event.cmdSeq > 0n),
        gate.awaitEvent('sync', (event) => event.syncSeq > event.cmdSeq),
      ])
      const waiting = Promise.all(waits).then(() => true, () => false)
      const leader = c.leader()
      const [localSeq, localResult] = await leader.append(Buffer.from('local'))
      t.equal(localResult, 'local', 'a local command retains its result')
      await c.settled()
      const follower = c.gates().find((gate) => gate.id !== leader.id)
      const [forwardedSeq, results] = await follower.appendBatch([
        Buffer.from('forwarded one'), Buffer.from('forwarded two'),
      ])
      t.deepEqual(results, [
        { status: 'fulfilled', value: 'forwarded one' },
        { status: 'fulfilled', value: 'forwarded two' },
      ], 'a forwarded batch retains its results')
      await c.settled()
      await until(() => c.ids.every((id) => samples.some((sample) =>
        sample.id === id && sample.type === 'sync' && sample.event.cmdSeq === forwardedSeq)),
      'forwarded SYNC events on every node')
      t.ok(await Promise.race([waiting, pause(100).then(() => false)]),
        'commit and command-progress waits resolve on every node')

      const commits = samples.filter((sample) => sample.type === 'commit')
      t.ok(commits.length >= c.ids.length, 'commit events reach every node')
      t.ok(commits.every((sample) => typeof sample.seq === 'bigint'),
        'commit retains its bigint argument')
      t.deepEqual(commits.map(({ id, seq, commitSeq, commitTerm }) =>
        ({ id, seq, commitSeq, commitTerm })),
      commits.map(({ id, seq, storedTerm }) =>
        ({ id, seq, commitSeq: seq, commitTerm: storedTerm })),
      'commit listeners see the committed sequence and its stored term')

      for (const id of c.ids) {
        const progress = samples.filter((sample) => sample.id === id &&
          (sample.type === 'cmd' || sample.type === 'sync'))
        t.deepEqual(progress.map(({ type, event }) => [type, event.cmdSeq]), [
          ['cmd', localSeq], ['sync', localSeq], ['cmd', forwardedSeq], ['sync', forwardedSeq],
        ], `${id} preserves CMD/SYNC event order for local and forwarded commands`)
        t.ok(progress.every(({ type, event, commitSeq }) =>
          commitSeq >= (type === 'cmd' ? event.cmdSeq : event.syncSeq)),
        `${id} exposes committed state before CMD/SYNC listeners run`)
        t.deepEqual(progress.filter(({ type }) => type === 'cmd').map(({ event }) => event.cmdCount),
          [1, 2], `${id} retains command counts`)
      }
      t.ok(samples.filter(({ type }) => type === 'cmd').every(({ event }) =>
        JSON.stringify(keys(event)) === JSON.stringify(cmdKeys)), 'CMD payload shape is unchanged')
      t.ok(samples.filter(({ type }) => type === 'sync').every(({ event }) =>
        JSON.stringify(keys(event)) === JSON.stringify(syncKeys)), 'SYNC payload shape is unchanged')
      const changes = samples.filter(({ type }) => type === 'change')
      t.ok(changes.length > 0 && changes.every(({ event }) =>
        JSON.stringify(keys(event)) === JSON.stringify(changeKeys)), 'change contains exactly seven fields')
      t.deepEqual(changes.map(({ state }) => state), changes.map(({ event }) => event),
        'Gate properties match each captured snapshot before listeners run')
      for (const id of c.ids) {
        const nodeChanges = changes.filter((sample) => sample.id === id).map(({ event }) => event)
        const initial = nodeChanges[0]
        t.deepEqual(initial, {
          state: 'follower', leader: null, term: 0n, seq: -1n,
          commitSeq: -1n, commitTerm: -1n, followers: [],
        }, 'startup still publishes its initial status')
        t.ok(nodeChanges.every((event, index) => index === 0 ||
          !isDeepStrictEqual(event, nodeChanges[index - 1])),
        `${id} delivers no consecutive duplicate changes`)
        t.notOk('appliedSeq' in c.nodes.get(id).gate, 'Gate does not acquire an applied checkpoint')
      }
      t.equal(c.errors.length, 0, 'event publication introduces no component errors')
    } finally { await c.close() }
    t.end()
  })
}

test('dissenting follower stops without publishing its fatal repair input', async (t) => {
  let dissenter = null
  const c = cluster({
    apply(id, db, buffer, term, seq) {
      if (seq === 0n) { db.exec('CREATE TABLE items (value BLOB) STRICT'); return null }
      if (id === dissenter) { throw new Error('dissenting result') }
      db.prepare('INSERT INTO items VALUES (?)').run(buffer)
      return 'accepted'
    },
  })
  try {
    await c.open()
    await c.settled()
    dissenter = c.ids.find((id) => id !== c.leader().id)
    const { gate } = c.nodes.get(dissenter)
    const syncs = []
    gate.on('sync', (event) => syncs.push(event))
    const waiting = gate.awaitEvent('sync', (event) => event.syncSeq > event.cmdSeq)
      .then(() => null, (err) => err)
    t.equal((await c.leader().append(Buffer.from('item')))[1], 'accepted')
    await until(() => !gate.isOpen, 'Gate detects its stopped Procs')
    t.deepEqual(syncs, [], 'the repair SYNC is not published without a completion')
    t.notOk('appliedSeq' in gate)
    t.equal((await waiting).code, Err.LOCAL_TIMEOUT,
      'the progress wait rejects with Gate\'s missing-completion timeout')
    t.ok(c.errors.some(({ id, role, error }) => id === dissenter && role === 'procA' &&
      error.code === Err.REPAIR_OUTSIDE), 'the owner still reports its repair decision')
  } finally { await c.close() }
  t.end()
})

test('a committed repair decision retains its fence without publishing any outputs', (t) => {
  const f = protocol().setup()
  try {
    const command = f.rpc(append({
      seqP: 0n, termP: 1n, data: [record(1n, cmd())], commitSeq: 1n,
    }))
    f.complete(messages(command, 'execute', 'apply')[0],
      [[1, 'dissenting result', Err.APPLY_ERROR, null]], Buffer.alloc(16, 1))
    const decision = pack({
      type: 'sync', cmdSeq: 1n, quorum: true, digest: Buffer.alloc(16),
      agree: ['b', 'c'], disagree: ['a'],
    })
    const completions = []
    f.onSend = (proc, reply) => completions.push(reply)
    const reply = f.rpc(append({
      seqP: 1n, termP: 1n, data: [record(1n, decision)], commitSeq: 2n,
    }))
    t.equal(reply, undefined, 'fatal repair has no final completion')
    t.deepEqual(completions, [], 'no Proc publishes replication ACKs or protocol events')
    t.equal(f.state().meta.repair_state, BigInt(Err.REPAIR_OUTSIDE), 'the repair fence is durable')
    t.equal(f.state().meta.applied_seq, 2n, 'the repair decision remains applied')
    t.equal(f.state().meta.pending_cmd_seq, null, 'the decision clears the pending command')
    t.ok(f.procs.every((proc) => !proc.isOpen), 'all three owners stop after committing the decision')
    t.equal(f.diagnostics.filter(({ type, error }) =>
      type === 'error' && error.code === Err.REPAIR_OUTSIDE).length, 3)
  } finally { f.close() }
  t.end()
})
