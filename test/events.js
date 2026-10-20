import test from 'tape'
import { cluster, until, pause } from './helper.js'
import { protocol, append, record, cmd, messages, outputs } from './fixtures/protocol.js'
import * as Err from '../src/error.js'

const keys = (value) => Object.keys(value).sort()
const cmdKeys = ['type', 'cmdSeq', 'cmdCount'].sort()
const syncKeys = ['type', 'cmdSeq', 'syncSeq', 'quorum', 'agree', 'disagree'].sort()
const changeKeys = [
  'state', 'leader', 'term', 'seq', 'commitSeq', 'commitTerm', 'followers',
].sort()

test('setup and CMD receipt imports do not publish apply-only change events', (t) => {
  const f = protocol()
  try {
    const setup = f.rpc(append({ data: [record()], commitSeq: 0n }))
    const setupResult = f.complete(messages(setup, 'execute', 'apply')[0])
    t.equal(outputs(setupResult).filter(({ to }) => to === 'event').length, 0,
      'setup completion publishes no extra event')
    t.equal(f.proc.engine.appliedSeq, 0n, 'setup still advances internal applied state')
    const command = f.rpc(append({
      seqP: 0n, termP: 1n, data: [record(1n, cmd())], commitSeq: 1n,
    }))
    const completed = f.complete(messages(command, 'execute', 'apply')[0])
    const events = outputs(completed).filter(({ to }) => to === 'event').map(({ message }) => message)
    t.deepEqual(events.map(({ type }) => type), ['cmd'], 'CMD completion reports only command progress')
    t.deepEqual(keys(events[0].status), changeKeys, 'CMD internal status also omits appliedSeq')
    t.equal(events[0].cmdSeq, 1n)
    t.equal(f.state().meta.applied_seq, 1n, 'receipt checkpoint remains durable')
    t.equal(f.proc.engine.pendingCommand.cmdSeq, 1n)
  } finally { f.close() }
  t.end()
})

test('successful heartbeat responses still publish repeated identical change snapshots', (t) => {
  const f = protocol().leader()
  try {
    const changes = []
    const now = f.proc.executor.timems
    for (const offset of [45, 90]) {
      const tick = f.tick(now + offset)
      const heartbeat = messages(tick, 'append', 'b').find((msg) => msg.data === undefined)
      const response = f.rpc({ type: 'ack', cid: heartbeat.cid, term: heartbeat.term, seq: 1n }, 'b')
      const events = messages(response, 'change', 'event')
      t.equal(events.length, 1, 'successful heartbeat retains its change notification')
      changes.push(events[0].status)
    }
    t.deepEqual(changes[0], changes[1], 'identical snapshots are not suppressed')
    t.deepEqual(keys(changes[0]), changeKeys)
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
    const first = f.input('append', { requestId: 'first', items: [Buffer.from('first')] })
    t.equal(messages(first, 'change', 'event').at(-1).status.seq, 1n, 'CMD append reports its log head')
    const committed = ack(page(first))
    f.complete(messages(committed, 'execute', 'apply')[0])
    f.input('append', { requestId: 'second', items: [Buffer.from('second')] })
    const decision = f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest: Buffer.alloc(32) }, 'b')
    const finished = ack(page(decision))
    t.equal(f.proc.engine.appliedSeq, 2n, 'SYNC advances internal applied progress')
    t.equal(f.proc.engine.active.cmdSeq, 3n, 'next command is started')
    t.deepEqual(messages(finished, 'change', 'event').map(({ status }) => status.seq), [2n, 2n, 3n],
      'commit, replication and next append notify without an extra apply-only change')
    t.equal(messages(finished, 'sync', 'event')[0].syncSeq, 2n)
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
        const initial = changes.find((sample) => sample.id === id).event
        t.deepEqual(initial, {
          state: 'follower', leader: null, term: 0n, seq: -1n,
          commitSeq: -1n, commitTerm: -1n, followers: [],
        }, 'startup still publishes its initial status')
        t.notOk('appliedSeq' in c.nodes.get(id).gate, 'Gate does not acquire an applied checkpoint')
      }
      t.equal(c.errors.length, 0, 'event publication introduces no component errors')
    } finally { await c.close() }
    t.end()
  })
}

test('dissenting follower reports its final SYNC sequence without a following change', async (t) => {
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
    let finalSync = null
    let laterChanges = 0
    gate.on('sync', (event) => { finalSync = { event, commitSeq: gate.commitSeq } })
    gate.on('change', () => { if (finalSync) { laterChanges++ } })
    const waiting = gate.awaitEvent('sync', (event) => event.syncSeq > event.cmdSeq)
      .then(() => true, () => false)
    t.equal((await c.leader().append(Buffer.from('item')))[1], 'accepted')
    await until(() => finalSync, 'dissenting follower SYNC')
    t.ok(finalSync.commitSeq >= finalSync.event.syncSeq,
      'repair SYNC listeners observe the final decision and its committed state')
    t.notOk('appliedSeq' in gate)
    t.ok(await Promise.race([waiting, pause(100).then(() => false)]),
      'a progress wait observes the final SYNC before repair shutdown')
    t.equal(laterChanges, 0, 'the update does not depend on another change event')
    t.ok(c.errors.some(({ id, role, error }) => id === dissenter && role === 'procA' &&
      error.code === Err.REPAIR_OUTSIDE_AGREEMENT), 'the owner still reports its repair decision')
  } finally { await c.close() }
  t.end()
})
