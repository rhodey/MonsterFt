import test from 'tape'
import { DatabaseSync } from 'node:sqlite'
import { cluster, until, roles } from './helper.js'
import * as Err from '../src/error.js'
import { readDatabase, readState, stateDigest } from '../src/storage.js'
import { decodeFrame } from '../src/wire.js'

function checkCopies(t, members) {
  const states = roles.map((role) => stateDigest(readState(members[role].db)))
  t.ok(states.every((value) => value.equals(states[0])), `${members.gate.id} protocol copies match`)
}

for (const delayed of [false, true]) {
  test(`cluster local and forwarded commands over ${delayed ? 'delayed' : 'synchronous'} transport`, async (t) => {
    const c = cluster({ delay: delayed ? 2 : 0, localDelay: delayed ? 1 : 0 })
    try {
      await c.open()
      const leader = c.leader()
      const follower = c.gates().find((gate) => gate.id !== leader.id)
      const first = await leader.append(Buffer.from('local'))
      const second = await follower.append(Buffer.from('forwarded'))
      const third = await follower.appendBatch([Buffer.from('batch1'), Buffer.from('batch2')])
      t.equal(first[1], 'local')
      t.equal(second[1], 'forwarded')
      t.deepEqual(third[1], [
        { status: 'fulfilled', value: 'batch1' }, { status: 'fulfilled', value: 'batch2' },
      ])
      t.ok(first[0] < second[0] && second[0] < third[0])
      await c.settled()
      await until(() => [...c.nodes.values()].every((node) =>
        node.apply.db.prepare('SELECT count(*) AS n FROM items').get().n === 4n), 'all application rows')
      for (const members of c.nodes.values()) {
        checkCopies(t, members)
        t.equal(c.calls.filter((call) => call.id === members.gate.id && call.seq === 0n).length, 1,
          'setup runs once per cluster node')
        t.equal(c.calls.filter((call) => call.id === members.gate.id && call.seq > 0n).length, 4,
          'commands run once per node, not once per Proc')
      }
      t.ok(c.history.some((item) => item.message.type === 'fwd_cmd'), 'forwarding uses Monster command protocol')
      t.equal(c.errors.length, 0)
    } finally { await c.close() }
    t.end()
  })
}

test('cluster agreed apply failures warn locally and preserve command stacks', async (t) => {
  const c = cluster({
    apply(id, db, buffer, term, seq) {
      if (seq === 0n) { db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB) STRICT'); return null }
      const value = buffer.toString()
      if (value === 'reject') {
        const error = new Error('same rejection')
        error.stack = `trace from ${id}`
        throw error
      }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(buffer)
      return value
    },
  })
  try {
    await c.open()
    const leader = c.leader()
    const follower = c.gates().find((gate) => gate.id !== leader.id)
    try { await follower.append(Buffer.from('reject')); t.fail('expected rejection') } catch (err) {
      t.equal(err.message, 'same rejection')
      t.equal(err.stack, `trace from ${leader.id}`, 'forwarded result retains leader trace')
    }
    const [, outcomes] = await leader.appendBatch([Buffer.from('good'), Buffer.from('reject')])
    t.equal(outcomes[0].status, 'fulfilled')
    t.equal(outcomes[1].status, 'rejected')
    await c.settled()
    await until(() => c.warnings.filter((item) => item.error.code === Err.APPLY_ERROR).length === 6,
      'one warning per failed item on each node')
    for (const id of c.ids) {
      const warnings = c.warnings.filter((item) => item.id === id && item.error.code === Err.APPLY_ERROR)
      t.equal(warnings.length, 2)
      t.ok(warnings.every((item) => item.role === 'apply'), 'only Apply emits its item warnings')
      t.ok(warnings.every((item) => item.error.stack === `trace from ${id}`))
      t.equal(c.nodes.get(id).apply.db.prepare('SELECT count(*) AS n FROM items').get().n, 1n)
    }
    t.equal(c.errors.length, 0, 'agreed failures do not require repair')
  } finally { await c.close() }
  t.end()
})

test('cluster leadership replacement catches up a stopped follower and resumes commands', async (t) => {
  const c = cluster({ opts: { rpcMax: 2 } })
  try {
    await c.open()
    const original = c.leader()
    await original.append(Buffer.from('before'))
    await c.settled()
    const old = c.nodes.get(original.id)
    await original.close()
    const replacement = await until(() => c.leader([original.id]), 'replacement leader')
    for (let i = 0; i < 5; i++) { await replacement.append(Buffer.from(`after${i}`)) }
    await c.settled()
    t.ok(replacement.term > original.term, 'replacement leader advances term')
    const reopened = c.makeNode(original.id)
    await reopened.gate.open()
    await until(() => reopened.apply.db.prepare('SELECT count(*) AS n FROM items').get().n === 6n,
      'reopened follower catches up')
    t.equal(c.calls.filter((call) => call.id === original.id && call.seq === 0n).length, 1,
      'restart does not rerun setup')
    await reopened.gate.awaitLeader(true)
    const last = await reopened.gate.append(Buffer.from('continued'))
    t.equal(last[1], 'continued')
    await c.settled()
    await until(() => [...c.nodes.values()].every((node) =>
      node.apply.db.prepare('SELECT count(*) AS n FROM items').get().n === 7n), 'continued writes on all nodes')
    checkCopies(t, reopened)
    t.ok(old.procA._closing, 'old Proc remains stopped')
    t.equal(c.errors.length, 0)
  } finally { await c.close() }
  t.end()
})

test('cluster retention and reopening preserve completed application work', async (t) => {
  const c = cluster({ opts: { keepTarget: 2, keepTrigger: 5 } })
  try {
    await c.open()
    for (let i = 0; i < 8; i++) { await c.leader().append(Buffer.from(`item${i}`)) }
    await c.settled()
    await until(() => [...c.nodes.values()].every((node) =>
      node.apply.db.prepare('SELECT count(*) AS n FROM items').get().n === 8n), 'retained workload')
    for (const node of c.nodes.values()) {
      t.ok(node.procA.log.begin > 0n, 'retention removed old log entries')
      checkCopies(t, node)
    }
    const count = c.calls.length
    await Promise.all(c.gates().map((gate) => gate.close()))
    c.ids.forEach(c.makeNode)
    await c.open()
    t.equal(c.calls.length, count, 'reopening does not execute completed setup or commands')
    const result = await c.leader().append(Buffer.from('after reopen'))
    t.equal(result[1], 'after reopen')
    await c.settled()
    t.equal(c.errors.length, 0)
  } finally { await c.close() }
  t.end()
})

test('cluster skips a missing log tail after another leadership change', async (t) => {
  const c = cluster({ opts: { rpcMax: 3 } })
  try {
    await c.open()
    const lagger = c.leader()
    await lagger.append(Buffer.from('original'))
    await c.settled()
    await lagger.close()
    const second = await until(() => c.leader([lagger.id]), 'second leader')
    for (let i = 0; i < 12; i++) { await second.append(Buffer.from(`gap${i}`)) }
    await c.settled()
    await second.close()
    const boundary = c.history.length
    const returned = c.makeNode(lagger.id)
    await returned.gate.open()
    const third = await until(() => c.leader([lagger.id, second.id]), 'third leader')
    await until(() => returned.apply.db.prepare('SELECT count(*) AS n FROM items').get().n === 13n,
      'missing tail repaired')
    const rejects = c.history.slice(boundary).filter((item) => item.from === lagger.id &&
      item.to === third.id && item.message.type === 'err' && item.message.code === Err.REPL_BACKTRACK)
    t.equal(rejects.length, 1, 'one rejection skips the entire missing range')
    t.equal(typeof rejects[0].message.logSeq, 'bigint', 'rejection carries the cached log head')
    await returned.gate.awaitLeader(true)
    t.equal((await returned.gate.append(Buffer.from('continued')))[1], 'continued')
    await c.settled()
    checkCopies(t, returned)
    t.equal(c.calls.filter((call) => call.id === lagger.id && call.seq > 0n).length, 14,
      'all retained commands apply exactly once')
    t.equal(c.errors.length, 0)
  } finally { await c.close() }
  t.end()
})

test('cluster dissenting follower warns before its repair fence closes it', async (t) => {
  let dissenter = null
  const c = cluster({
    apply(id, db, buffer, term, seq) {
      if (seq === 0n) { db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB) STRICT'); return null }
      if (id === dissenter) {
        const error = new Error('local application failed')
        error.stack = 'dissenting follower trace'
        throw error
      }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(buffer)
      return 'accepted'
    },
  })
  try {
    await c.open()
    dissenter = c.ids.find((id) => id !== c.leader().id)
    const events = []
    c.nodes.get(dissenter).apply.on('warn', (err) => events.push(['warn', err]))
    c.nodes.get(dissenter).procA.on('error', (err) => events.push(['error', err]))
    t.equal((await c.leader().append(Buffer.from('item')))[1], 'accepted')
    await until(() => !c.nodes.get(dissenter).gate.isOpen, 'dissenting follower closes')
    const relevant = events.filter((item) => item[1].code === Err.APPLY_ERROR ||
      item[1].code === Err.REPAIR_OUTSIDE_AGREEMENT)
    t.equal(relevant[0][0], 'warn')
    t.equal(relevant[0][1].stack, 'dissenting follower trace')
    t.equal(relevant[1][0], 'error')
    t.equal(relevant[1][1].code, Err.REPAIR_OUTSIDE_AGREEMENT)
    const db = new DatabaseSync(`${c.directory}/${dissenter}.a`, { readOnly: true, readBigInts: true })
    try {
      t.equal(db.prepare('SELECT repair_state FROM monsterft_meta').get().repair_state, 2n,
        'repair fence is durable before shutdown')
    } finally { db.close() }
  } finally { await c.close() }
  t.end()
})

for (const committed of [1, 2]) {
  test(`cluster reconciles interruption after ${committed} Proc commits`, async (t) => {
    let target = null
    let interrupt = false
    const c = cluster({
      local({ id, from, to, frame }) {
        if (!interrupt || id !== target || from !== 'gate' || !roles.includes(to)) { return }
        const input = decodeFrame(frame)
        if (input.type === 'input' && input.kind === 'append' && roles.indexOf(to) >= committed) {
          return false
        }
      },
    })
    try {
      await c.open()
      const original = c.leader()
      target = original.id
      const members = c.nodes.get(target)
      await original.append(Buffer.from('baseline'))
      await c.settled()
      const before = members.procA.log.seq
      interrupt = true
      const pending = original.append(Buffer.from('interrupted')).catch((err) => err)
      await until(() => members.procA.log.seq > before, 'first interrupted commit')
      await original.close()
      t.equal((await pending).code, Err.NODE_NOT_OPEN)
      const states = roles.map((role, index) => readDatabase(`${c.directory}/${target}.${'abc'[index]}`, readState))
      t.equal(states[0].head.seq, before + 1n)
      t.equal(states[committed].head.seq, before, 'unprocessed Proc retains entire old state')
      t.ok(stateDigest(states[committed === 1 ? 1 : 0]).equals(
        stateDigest(states[committed === 1 ? 2 : 1])), 'interrupted input leaves a majority')
      interrupt = false
      await until(() => c.leader([target]), 'leader after interruption')
      const reopened = c.makeNode(target)
      await reopened.gate.open()
      const expected = committed === 1 ? 1n : 2n
      await until(() => [...c.nodes.values()].every((node) =>
        node.apply.db.prepare('SELECT count(*) AS n FROM items').get().n === expected),
      'recovered application state')
      await c.settled()
      checkCopies(t, reopened)
      t.equal(c.calls.filter((call) => call.seq > 0n && call.buffer.toString() === 'interrupted').length,
        committed === 1 ? 0 : 3, 'only the committed majority can authorize Apply')
      t.equal(c.calls.filter((call) => call.seq > 0n && call.buffer.toString() === 'baseline').length,
        3, 'previous completed work is not reapplied')
      await reopened.gate.awaitLeader(true)
      t.equal((await reopened.gate.append(Buffer.from('continued')))[1], 'continued')
      t.equal(c.errors.length, 0)
    } finally { await c.close() }
    t.end()
  })
}
