import test from 'tape'
import { DatabaseSync } from 'node:sqlite'
import { cluster, until, roles } from './v2-helper.js'
import { decodeFrame } from '../src/wire.v2.js'
import { readApply, readMeta } from '../src/utils.v2.js'
import * as Err from '../src/error.v2.js'

const buffer = (value) => Buffer.from(value)
const schema = 'CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB NOT NULL) STRICT'
const deferred = () => {
  let resolve
  const promise = new Promise((done) => { resolve = done })
  return { promise, resolve }
}
const writes = (db) => db.prepare('SELECT value FROM items ORDER BY id').all()
  .map((row) => Buffer.from(row.value).toString())

test('v2 impossible command agreement persists a nonfatal fence on every node', async (t) => {
  const c = cluster({
    apply(id, db, data, term, seq) {
      if (seq === 0n) { db.exec(schema); return null }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
      return id
    },
  })
  try {
    await c.open()
    const rejection = await c.leader().append(buffer('different outcomes')).then(
      () => null, (err) => err,
    )
    t.equal(rejection.code, Err.REPAIR_QUORUM_IMPOSSIBLE)
    await until(() => [...c.nodes.values()].every((node) => roles.every((role) =>
      node[role].engine.repairState === 1 && node[role].engine.pendingCommand === null)),
    'nonfatal repair fence on all protocol copies')
    for (const node of c.nodes.values()) {
      t.ok(node.gate.isOpen, `${node.gate.id} remains open to disseminate the repair decision`)
      for (const role of roles) {
        t.equal(readMeta(node[role].db).repair_state, 1n, `${role} fence is durable`)
      }
      const next = await node.gate.append(buffer('must not apply')).then(() => null, (err) => err)
      t.equal(next.code, Err.REPAIR_QUORUM_IMPOSSIBLE)
      t.deepEqual(writes(node.apply.db), ['different outcomes'])
    }
    t.ok(c.errors.every(({ error }) => error.code === Err.REPAIR_QUORUM_IMPOSSIBLE),
      'repair errors surface without local divergence or fatal processing errors')
    t.equal(c.warnings.filter(({ error }) => error.code === Err.APPLY_ERROR).length, 0,
      'successful callbacks do not warn merely because their results disagree')
  } finally { await c.close() }
  t.end()
})

test('v2 dissenting leader reports its original apply warning then fences before replacement leadership', async (t) => {
  let dissenter = null
  const c = cluster({
    apply(id, db, data, term, seq) {
      if (seq === 0n) { db.exec(schema); return null }
      if (id === dissenter) {
        const err = new Error('leader apply failed')
        err.stack = 'original dissenting leader trace'
        throw err
      }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
      return data.toString()
    },
  })
  try {
    await c.open()
    dissenter = c.leader().id
    const original = c.nodes.get(dissenter)
    const events = []
    original.gate.on('warn', (err) => events.push(['warn', err]))
    original.gate.on('error', (err) => events.push(['error', err]))
    const caller = c.gates().find((gate) => gate.id !== dissenter)
    const rejection = await caller.append(buffer('first')).then(() => null, (err) => err)
    t.equal(rejection.code, Err.REPAIR_OUTSIDE_AGREEMENT,
      'forwarded caller receives the repair error before the leader shuts down')
    await until(() => !original.gate.isOpen, 'dissenting leader closes')
    await original.gate.close()
    const relevant = events.filter(([, err]) =>
      err.code === Err.APPLY_ERROR || err.code === Err.REPAIR_OUTSIDE_AGREEMENT)
    t.equal(relevant[0][0], 'warn')
    t.equal(relevant[0][1].stack, 'original dissenting leader trace')
    t.equal(relevant[1][0], 'error')
    t.equal(relevant[1][1].code, Err.REPAIR_OUTSIDE_AGREEMENT)
    for (const suffix of ['a', 'b', 'c']) {
      const db = new DatabaseSync(`${c.directory}/${dissenter}.${suffix}`, { readOnly: true, readBigInts: true })
      try {
        const meta = readMeta(db)
        t.equal(meta.repair_state, 2n, 'intentional repair fence survives shutdown')
        t.notEqual(meta.pending_cmd_seq, null, 'leader preserves the unresolved command for repair')
      } finally { db.close() }
    }
    const replacement = await until(() => c.leader([dissenter]), 'healthy replacement leader')
    await c.settled()
    t.equal((await replacement.append(buffer('second')))[1], 'second')
    await c.settled()
    for (const node of [...c.nodes.values()].filter((node) => node.gate.isOpen)) {
      t.deepEqual(writes(node.apply.db), ['first', 'second'])
      t.equal(c.calls.filter((call) => call.id === node.gate.id && call.seq > 0n).length, 2,
        'replacement resolves the first command without reapplying it')
    }
  } finally { await c.close() }
  t.end()
})

test('v2 slow Apply preserves protocol order across leadership change and pending-command recovery', async (t) => {
  const held = deferred()
  let slow = null
  let holdReports = false
  const c = cluster({
    async apply(id, db, data, term, seq) {
      if (seq === 0n) { db.exec(schema); return null }
      if (id === slow && data.toString() === 'one') { await held.promise }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
      return data.toString()
    },
    transport({ message }) {
      if (holdReports && message.type === 'monster_outcome') { return false }
    },
  })
  try {
    await c.open()
    const original = c.leader()
    slow = c.ids.find((id) => id !== original.id)
    const fast = c.ids.find((id) => id !== original.id && id !== slow)
    const [firstSeq] = await original.append(buffer('one'))
    await until(() => c.nodes.get(fast).procA.engine.pendingCommand === null &&
      c.nodes.get(fast).procA.engine.appliedSeq > firstSeq, 'first command SYNC on healthy follower')
    holdReports = true
    const secondWork = original.append(buffer('two')).then((value) => ({ value }), (error) => ({ error }))
    const second = await until(() => c.calls.find((call) => call.id === original.id &&
      call.buffer?.toString() === 'two'), 'second command executes at original leader')
    const secondSeq = second.seq
    await until(() => [original.id, fast].every((id) =>
      c.nodes.get(id).procA.engine.pendingCommand?.cmdSeq === secondSeq), 'second command awaits agreement')
    await until(() => c.nodes.get(slow).procA.log.seq >= secondSeq, 'slow node stores later committed history')
    const slowNode = c.nodes.get(slow)
    t.equal(slowNode.procA.engine.pendingApply.seq, firstSeq)
    t.equal(slowNode.procA.engine.appliedSeq, firstSeq - 1n,
      'checkpoint stays before the outstanding Apply despite later log entries')
    await original.close()
    holdReports = false
    t.ok((await secondWork).error, 'closing the original leader settles its pending caller')
    const replacement = await until(() => c.leader([original.id]), 'leader while Apply is still pending')
    t.ok(replacement.term > original.term)
    t.ok(slowNode.procA.log.seq > secondSeq, 'election no-op can follow the unresolved command')
    held.resolve()
    await c.settled()
    for (const id of [slow, fast]) {
      t.deepEqual(writes(c.nodes.get(id).apply.db), ['one', 'two'])
      t.equal(c.calls.filter((call) => call.id === id && call.buffer?.toString() === 'one').length, 1)
      t.equal(c.calls.filter((call) => call.id === id && call.buffer?.toString() === 'two').length, 1)
    }
    t.equal((await replacement.append(buffer('three')))[1], 'three')
    await c.settled()
    const reopened = c.makeNode(original.id)
    await reopened.gate.open()
    await until(() => writes(reopened.apply.db).length === 3, 'old leader recovers through pending receipt')
    t.deepEqual(writes(reopened.apply.db), ['one', 'two', 'three'])
    t.equal(c.calls.filter((call) => call.id === original.id && call.buffer?.toString() === 'two').length, 1,
      'reopening does not rerun already committed Apply work')
    t.equal(c.errors.length, 0)
  } finally {
    held.resolve()
    await c.close()
  }
  t.end()
})

test('v2 delayed Proc uses its fixed Apply reply when DB2 has already committed the next command', async (t) => {
  const held = deferred()
  let slow = null
  let firstSeq = null
  let resumeProcC = null
  const c = cluster({
    async apply(id, db, data, term, seq) {
      if (seq === 0n) { db.exec(schema); return null }
      if (id === slow && data.toString() === 'one') { await held.promise }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
      return data.toString()
    },
    local({ id, from, to, frame, deliver }) {
      if (id !== slow || from !== 'gate' || to !== 'procC' || firstSeq === null) { return }
      const message = decodeFrame(frame)
      if (message.kind === 'apply' && message.reply?.receipt?.seq === firstSeq) {
        resumeProcC = deliver
        return false
      }
    },
  })
  try {
    await c.open()
    const leader = c.leader()
    slow = c.ids.find((id) => id !== leader.id)
    ;[firstSeq] = await leader.append(buffer('one'))
    const [secondSeq] = await leader.append(buffer('two'))
    const node = c.nodes.get(slow)
    await until(() => node.procA.engine.commitSeq >= secondSeq, 'slow follower retains both commands')
    held.resolve()
    await until(() => resumeProcC !== null && readApply(node.apply.db).seq === secondSeq,
      'second Apply commits before ProcC imports first reply')
    t.equal(node.procC.engine.pendingApply.seq, firstSeq)
    t.equal(node.procC.engine.appliedSeq, firstSeq - 1n)
    t.equal(node.procA.engine.pendingApply.seq, secondSeq)
    t.equal(node.procB.engine.pendingApply.seq, secondSeq)
    t.deepEqual(writes(node.apply.db), ['one', 'two'], 'Apply has executed each command once')
    resumeProcC()
    resumeProcC = null
    await c.settled()
    await until(() => roles.every((role) => node[role].engine.appliedSeq >= secondSeq),
      'third Proc imports the fixed receipt and catches up')
    for (const role of roles) {
      t.equal(readMeta(node[role].db).pending_cmd_seq, null)
      t.deepEqual(Buffer.from(readMeta(node[role].db).applied_local_digest),
        Buffer.from(readApply(node.apply.db).local_digest))
    }
    t.equal(c.calls.filter((call) => call.id === slow && call.seq > 0n).length, 2)
    t.equal(c.errors.length, 0)
  } finally {
    held.resolve()
    if (resumeProcC !== null) { resumeProcC() }
    await c.close()
  }
  t.end()
})

test('v2 drain waits for pending Apply and SYNC but does not start the next command', async (t) => {
  const held = deferred()
  let slow = null
  const c = cluster({
    async apply(id, db, data, term, seq) {
      if (seq === 0n) { db.exec(schema); return null }
      if (id === slow && data.toString() === 'one') { await held.promise }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
      return data.toString()
    },
  })
  try {
    await c.open()
    const leader = c.leader()
    slow = c.ids.find((id) => id !== leader.id)
    const [firstSeq] = await leader.append(buffer('one'))
    const node = c.nodes.get(slow)
    let drained = false
    const draining = node.gate.drainCmd().then(() => { drained = true })
    const rejection = await node.gate.append(buffer('rejected')).then(() => null, (err) => err)
    t.equal(rejection.code, Err.DRAINING)
    const [secondSeq] = await leader.append(buffer('two'))
    await until(() => node.procA.log.seq >= secondSeq, 'draining node receives later command')
    t.equal(drained, false, 'pending Apply keeps drain open')
    t.ok(node.gate.isOpen)
    held.resolve()
    await draining
    t.notOk(node.gate.isOpen)
    const db = new DatabaseSync(`${c.directory}/${slow}2`, { readOnly: true, readBigInts: true })
    try {
      t.equal(readApply(db).seq, firstSeq)
      t.deepEqual(writes(db), ['one'], 'only the already-started command executes')
    } finally { db.close() }
    for (const suffix of ['a', 'b', 'c']) {
      const db1 = new DatabaseSync(`${c.directory}/${slow}.${suffix}`, { readOnly: true, readBigInts: true })
      try { t.equal(readMeta(db1).pending_cmd_seq, null, 'drain waits for the command decision') }
      finally { db1.close() }
    }
    t.equal(c.errors.length, 0)
  } finally {
    held.resolve()
    await c.close()
  }
  t.end()
})

test('v2 leader drain continues pending agreement and rejects queued commands', async (t) => {
  let holdReports = false
  const c = cluster({
    transport({ message }) {
      if (holdReports && message.type === 'monster_outcome') { return false }
    },
  })
  try {
    await c.open()
    const leader = c.leader()
    holdReports = true
    const first = leader.append(buffer('one'))
    await until(() => c.nodes.get(leader.id).procA.engine.pendingCommand !== null,
      'leader waits for command agreement')
    const queued = leader.append(buffer('queued')).then(() => null, (err) => err)
    let drained = false
    const draining = leader.drainCmd().then(() => { drained = true })
    t.equal((await queued).code, Err.DRAINING)
    t.notOk(drained)
    t.ok(leader.isOpen)
    holdReports = false
    t.equal((await first)[1], 'one', 'in-flight command keeps its agreed result')
    await draining
    t.notOk(leader.isOpen)
    const db = new DatabaseSync(`${c.directory}/${leader.id}.a`, { readOnly: true, readBigInts: true })
    try { t.equal(readMeta(db).pending_cmd_seq, null) } finally { db.close() }
    t.equal(c.calls.some((call) => call.buffer?.toString() === 'queued'), false)
    t.equal(c.errors.length, 0)
  } finally {
    holdReports = false
    await c.close()
  }
  t.end()
})
