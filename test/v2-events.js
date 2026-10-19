import test from 'tape'
import { cluster, until, pause } from './v2-helper.js'
import * as Err from '../src/error.v2.js'

const keys = (value) => Object.keys(value).sort()
const cmdKeys = ['type', 'cmdSeq', 'cmdCount'].sort()
const syncKeys = ['type', 'cmdSeq', 'syncSeq', 'quorum', 'agree', 'disagree'].sort()
const changeKeys = [
  'type', 'state', 'leaderId', 'term', 'seq', 'commitSeq', 'commitTerm',
  'appliedSeq', 'followers', 'id', 'nodes', 'leader', 'open',
].sort()

for (const delay of [0, 2]) {
  test(`v2 progress events expose their current public state (${delay}ms transport)`, async (t) => {
    const c = cluster({ delay, localDelay: delay })
    try {
      await c.open()
      await c.settled()
      const samples = []
      for (const { gate, procA } of c.nodes.values()) {
        gate.on('commit', (seq) => {
          const record = procA.db.prepare('SELECT term FROM raft_log WHERE seq = ?').get(seq)
          samples.push({ id: gate.id, type: 'commit', seq, commitSeq: gate.commitSeq,
            commitTerm: gate.commitTerm, storedTerm: record.term })
        })
        for (const type of ['cmd', 'sync']) {
          gate.on(type, (event) => samples.push({ id: gate.id, type, event,
            appliedSeq: gate.appliedSeq }))
        }
        gate.on('change', (event) => samples.push({ id: gate.id, type: 'change', event }))
      }

      const waits = c.gates().flatMap((gate) => [
        gate.awaitEvent('commit', (seq) => gate.commitSeq === seq),
        gate.awaitEvent('cmd', (event) => gate.appliedSeq === event.cmdSeq),
        gate.awaitEvent('sync', (event) => gate.appliedSeq === event.syncSeq),
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
        'state-based event waits resolve on every node')

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
        t.deepEqual(progress.map(({ appliedSeq }) => appliedSeq),
          progress.map(({ type, event }) => type === 'cmd' ? event.cmdSeq : event.syncSeq),
          `${id} exposes the applied sequence before CMD/SYNC listeners run`)
        t.deepEqual(progress.filter(({ type }) => type === 'cmd').map(({ event }) => event.cmdCount),
          [1, 2], `${id} retains command counts`)
      }
      t.ok(samples.filter(({ type }) => type === 'cmd').every(({ event }) =>
        JSON.stringify(keys(event)) === JSON.stringify(cmdKeys)), 'CMD payload shape is unchanged')
      t.ok(samples.filter(({ type }) => type === 'sync').every(({ event }) =>
        JSON.stringify(keys(event)) === JSON.stringify(syncKeys)), 'SYNC payload shape is unchanged')
      const changes = samples.filter(({ type }) => type === 'change')
      t.ok(changes.length > 0 && changes.every(({ event }) =>
        JSON.stringify(keys(event)) === JSON.stringify(changeKeys)), 'change payload shape is unchanged')
      t.equal(c.errors.length, 0, 'event publication introduces no component errors')
    } finally { await c.close() }
    t.end()
  })
}

test('v2 dissenting follower exposes its final applied SYNC without a following change', async (t) => {
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
    gate.on('sync', (event) => { finalSync = { event, appliedSeq: gate.appliedSeq } })
    gate.on('change', () => { if (finalSync) { laterChanges++ } })
    const waiting = gate.awaitEvent('sync', (event) => gate.appliedSeq === event.syncSeq)
      .then(() => true, () => false)
    t.equal((await c.leader().append(Buffer.from('item')))[1], 'accepted')
    await until(() => finalSync, 'dissenting follower SYNC')
    t.equal(finalSync.appliedSeq, finalSync.event.syncSeq,
      'repair SYNC listeners observe the final durable applied sequence')
    t.ok(await Promise.race([waiting, pause(100).then(() => false)]),
      'a state-based wait observes the final SYNC before repair shutdown')
    t.equal(laterChanges, 0, 'the update does not depend on another change event')
    t.ok(c.errors.some(({ id, role, error }) => id === dissenter && role === 'procA' &&
      error.code === Err.REPAIR_OUTSIDE_AGREEMENT), 'the owner still reports its repair decision')
  } finally { await c.close() }
  t.end()
})
