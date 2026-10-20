import test from 'tape'
import fs from 'node:fs'
import { pack, unpack } from 'msgpackr'
import { DatabaseSync } from 'node:sqlite'
import { cluster, until, roles } from './helper.js'
import { protocol, append, record, cmd, messages } from './fixtures/protocol.js'
import { certify } from '../src/certify.js'
import { readApply, readMeta } from '../src/utils.js'
import { decodeFrame, encodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const buf = (value) => Buffer.from(value)
const schema = 'CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB NOT NULL) STRICT'
const rows = (db) => db.prepare('SELECT value FROM items ORDER BY id').all().map((row) => Buffer.from(row.value).toString())
const rejection = (work) => work.then(() => null, (err) => err)
const deferred = () => {
  let resolve
  const promise = new Promise((done) => { resolve = done })
  return { promise, resolve }
}
const decisions = (node) => [...node.procA.log.iter()].flatMap(({ entry }) => {
  if (!entry.length) { return [] }
  const value = unpack(entry)
  return value.type === 'sync' ? [value] : []
})

test('agreement covers the complete batch vector and DB2 transition', async (t) => {
  for (const mode of ['item majorities', 'different patchsets', 'all rejected']) {
    let divergent = true
    const c = cluster({
      apply(id, db, data, term, seq, index) {
        if (seq === 0n) { db.exec(schema); return null }
        if (divergent && (mode === 'all rejected' ||
            mode === 'item majorities' && index === ['a', 'b', 'c'].indexOf(id))) {
          throw new Error('agreed rejection')
        }
        if (mode !== 'item majorities') {
          db.prepare('INSERT INTO items (value) VALUES (?)').run(
            buf(divergent && mode === 'different patchsets' ? id : data.toString()))
        }
        return data.toString()
      },
    })
    try {
      await c.open()
      const work = c.leader().appendBatch([buf('one'), buf('two'), buf('three')])
      if (mode === 'all rejected') {
        const [, result] = await work
        t.ok(result.every((item) => item.status === 'rejected' && item.reason.message === 'agreed rejection'))
      } else {
        t.equal((await rejection(work)).code, Err.REPAIR_QUORUM_IMPOSSIBLE, mode)
      }
      await until(() => [...c.nodes.values()].every((node) => decisions(node).length === 1 &&
        node.procA.engine.pendingCommand === null), 'decision applied on all members')
      for (const node of c.nodes.values()) {
        const sync = decisions(node).at(-1)
        t.equal(sync.quorum, mode === 'all rejected')
        if (mode === 'all rejected') {
          t.deepEqual(rows(node.apply.db), [], 'rejected batch has an empty net patchset')
          t.equal(readMeta(node.procA.db).repair_state, 0n)
        } else {
          t.equal(readMeta(node.procA.db).repair_state, 1n)
        }
      }
      if (mode === 'different patchsets') {
        const digests = [...c.nodes.values()].map((node) => Buffer.from(readApply(node.apply.db).local_digest).toString('hex'))
        t.equal(new Set(digests).size, 3, 'identical return values cannot hide different DB2 writes')
        await Promise.all(c.gates().map((gate) => gate.close()))
        c.ids.forEach(c.makeNode)
        await c.open()
        for (const node of c.nodes.values()) {
          t.equal((await rejection(node.gate.append(buf('blocked')))).code, Err.REPAIR_QUORUM_IMPOSSIBLE)
          t.ok(node.gate.isOpen, 'state 1 remains online after restart')
        }
        await Promise.all(c.gates().map((gate) => gate.close()))
        for (const id of c.ids) { certify(`${c.directory}/${id}`) }
        divergent = false
        c.ids.forEach(c.makeNode)
        await c.open()
        t.equal((await c.leader().append(buf('continued')))[1], 'continued')
        await c.settled()
        for (const id of c.ids) {
          t.equal(c.calls.filter((call) => call.id === id && call.buffer?.toString() === 'one').length, 1,
            'certification and restart do not repeat completed work')
        }
      }
    } finally { await c.close() }
  }
  t.end()
})

test('a late unlisted patchset dissenter fences without producing another decision', async (t) => {
  let slow = null
  const held = deferred()
  const c = cluster({
    opts: { localTimeout: 300 },
    async apply(id, db, data, term, seq) {
      if (seq === 0n) { db.exec(schema); return null }
      if (id === slow) { await held.promise }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(id === slow ? buf('different write') : data)
      return 'identical result'
    },
  })
  try {
    await c.open()
    slow = c.ids.find((id) => id !== c.leader().id)
    const [seq, value] = await c.leader().append(buf('healthy write'))
    t.equal(value, 'identical result')
    const healthy = c.nodes.get(c.leader().id)
    await until(() => decisions(healthy).length === 1, 'healthy quorum decision')
    const sync = decisions(healthy)[0]
    t.notOk(sync.agree.includes(slow))
    t.notOk(sync.disagree.includes(slow), 'slow follower has not supplied an outcome')
    held.resolve()
    await until(() => c.errors.some(({ id, role, error }) =>
      id === slow && role === 'procA' && error.code === Err.REPAIR_OUTSIDE_AGREEMENT), 'late dissent fence')
    t.equal(decisions(healthy).length, 1)
    const target = c.nodes.get(slow)
    await target.gate.close()
    const db = new DatabaseSync(`${c.directory}/${slow}.a`, { readOnly: true, readBigInts: true })
    try { t.equal(readMeta(db).repair_state, 2n) } finally { db.close() }
    const reopened = c.makeNode(slow)
    t.equal((await rejection(reopened.gate.open())).code, Err.LOCAL_TIMEOUT, 'state 2 cannot start protocol')
    t.ok(c.errors.some(({ id, role, error }) =>
      id === slow && role === 'procA' && error.code === Err.REPAIR_OUTSIDE_AGREEMENT))
    await reopened.gate.close()
    certify(`${c.directory}/${slow}`)
    const repaired = c.makeNode(slow)
    await repaired.gate.open()
    await until(() => repaired.procA.engine.appliedSeq >= seq + 1n, 'certified state starts')
    t.equal(c.calls.filter((call) => call.id === slow && call.seq === seq).length, 1)
  } finally { held.resolve(); await c.close() }
  t.end()
})

test('concurrent local and forwarded commands preserve item association and matchIndex snapshots', async (t) => {
  const expectedSnapshots = new Map()
  const c = cluster({
    ids: ['c', 'a', 'b'],
    delay: 2,
    opts: { rpcMax: 1, rpcMaxBytes: 100, keepTarget: 6, keepTrigger: 12, electionTimeout: 400 },
    local({ id, from, to, frame, members }) {
      const message = decodeFrame(frame)
      if (from !== 'gate' || to !== 'procA' || message.type !== 'input') { return }
      const engine = members.procA.engine
      if (!engine || members.procA.log.append.observed) { return }
      // The engine records the exact snapshot when a CMD is appended, which
      // can happen in a continuation after this input's ACK advances a peer.
      const append = members.procA.log.append.bind(members.procA.log)
      members.procA.log.append = (record, ...args) => {
        if (record.entry.length) {
          const value = unpack(record.entry)
          if (value.type === 'cmd') {
            const current = engine.nodes.map((peer) => peer === id
              ? engine.seq : engine.replication.get(peer)?.matchIndex ?? -1n)
            expectedSnapshots.set(`${id}:${engine.seq + 1n}`, current)
          }
        }
        return append(record, ...args)
      }
      // Install once per fresh owner rather than wrapping once per input.
      members.procA.log.append.observed = true
    },
  })
  try {
    await c.open()
    const leader = c.leader()
    const order = new Map(c.ids.map((id) => [id, []]))
    for (const [id, node] of c.nodes) {
      node.gate.on('cmd', ({ cmdSeq }) => order.get(id).push(['cmd', cmdSeq]))
      node.gate.on('sync', ({ cmdSeq }) => order.get(id).push(['sync', cmdSeq]))
    }
    const submissions = Array.from({ length: 12 }, (_, i) => {
      const gate = i % 3 === 0 ? leader : c.gates()[i % c.ids.length]
      const items = i === 0 ? [Buffer.alloc(0)] : i % 2 ? [buf(`single-${i}`)] : [buf(`batch-${i}-0`), buf(`batch-${i}-1`)]
      const batch = items.length > 1
      return { gate, items, batch, work: batch ? gate.appendBatch(items) : gate.append(items[0]) }
    })
    const results = await Promise.all(submissions.map(({ work }) => work))
    await until(() => [...order.values()].every((events) => events.length === 24), 'every command decision on every node')
    await c.settled()
    t.equal(new Set(results.map(([seq]) => seq)).size, submissions.length)
    results.forEach(([seq, result], i) => {
      const { items, batch } = submissions[i]
      t.deepEqual(batch ? result.map(({ value }) => value) : [result], items.map((item) => item.toString()))
      for (const id of c.ids) {
        const calls = c.calls.filter((call) => call.id === id && call.seq === seq)
        t.deepEqual(calls.map((call) => call.index), items.map((_, index) => index))
        t.deepEqual(calls.map((call) => call.buffer), items, 'empty Buffer remains an application CMD')
        const snapshot = expectedSnapshots.get(`${leader.id}:${seq}`)
        t.deepEqual(calls[0].matchIndex, snapshot, 'callback sees the sorted append-time replication snapshot')
      }
    })
    for (const [id, events] of order) {
      t.deepEqual(events.map(([type]) => type), Array.from({ length: 12 }, () => ['cmd', 'sync']).flat(),
        `${id}: one CMD decision before the next CMD`)
      t.ok(c.nodes.get(id).procA.log.begin > 0n, 'retention also operates for forwarded singles/batches')
    }
    t.deepEqual(c.errors, [])
  } finally { await c.close() }
  t.end()
})

test('oversized commands reject before log mutation or leadership change', (t) => {
  const f = protocol().leader()
  try {
    // A size-only Buffer fixture avoids encoding or allocating a gigabyte.
    // The production guard reads byteLength before packing the request.
    const large = Buffer.alloc(1)
    Object.defineProperty(large, 'byteLength', { value: 999_999_000 })
    const engine = f.proc.engine
    const before = f.rows()
    const term = engine.term
    const outputs = []
    engine.output = (to, message) => outputs.push({ to, message })
    const warnings = []
    engine.diagnostic = (type, error) => warnings.push({ type, error })
    for (const items of [[large], [large, Buffer.alloc(0)]]) {
      f.proc.executor.run(f.proc.executor.timems, () => engine.submit('oversized', items))
      t.equal(outputs.at(-1).message.code, Err.ARGUMENT_ILLEGAL)
    }
    f.proc.executor.run(f.proc.executor.timems, () => engine.receiveCustom('b', {
      type: 'fwd_cmd', cid: 'oversized-forward', term, items: [large],
    }))
    t.equal(outputs.at(-1).message.code, Err.RPC_ILLEGAL)
    t.equal(warnings.at(-1).error.code, Err.RPC_ILLEGAL)
    t.deepEqual(f.rows(), before)
    t.equal(engine.term, term)
    t.equal(engine.state, 'leader')
  } finally { f.close() }
  t.end()
})

test('five-node donor repair preserves completed work while the remaining quorum progresses', async (t) => {
  let bad = null
  const c = cluster({
    ids: ['a', 'b', 'c', 'd', 'e'],
    opts: { localTimeout: 500 },
    apply(id, db, data, term, seq) {
      if (seq === 0n) { db.exec(schema); return null }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(id === bad ? buf('bad') : data)
      return data.toString()
    },
  })
  try {
    await c.open()
    const oldLeader = c.leader()
    bad = c.ids.find((id) => id !== oldLeader.id)
    const donorId = c.ids.find((id) => id !== bad && id !== oldLeader.id)
    await oldLeader.append(buf('first'))
    await until(() => c.errors.some(({ id, error }) => id === bad && error.code === Err.REPAIR_OUTSIDE_AGREEMENT),
      'target fenced')
    await c.nodes.get(bad).gate.close()
    const donor = c.nodes.get(donorId)
    await donor.gate.drainCmd()
    certify(`${c.directory}/${donorId}`)
    t.equal((await oldLeader.append(buf('while donor stopped')))[1], 'while donor stopped',
      'three healthy nodes still form a quorum')
    for (const suffix of ['.a', '.b', '.c', '2']) {
      for (const sidecar of ['', '-wal', '-shm', '-journal']) {
        const from = `${c.directory}/${donorId}${suffix}${sidecar}`
        const to = `${c.directory}/${bad}${suffix}${sidecar}`
        fs.rmSync(to, { force: true })
        if (fs.existsSync(from)) { fs.copyFileSync(from, to) }
      }
    }
    const repairedId = bad
    bad = null
    await c.makeNode(donorId).gate.open()
    await c.makeNode(repairedId).gate.open()
    // Reset the old leader's replication positions as documented for donor
    // copies. Four live nodes remain while it is stopped.
    await oldLeader.close()
    const replacement = await until(() => c.leader([oldLeader.id]), 'replacement after donor copy')
    await c.makeNode(oldLeader.id).gate.open()
    await until(() => [...c.nodes.values()].every((node) => rows(node.apply.db).length === 2), 'repaired catch-up')
    t.equal((await replacement.append(buf('after repair')))[1], 'after repair')
    await until(() => [...c.nodes.values()].every((node) => rows(node.apply.db).length === 3), 'final repaired replication')
    await c.settled()
    for (const node of c.nodes.values()) {
      t.deepEqual(rows(node.apply.db), ['first', 'while donor stopped', 'after repair'])
    }
    t.equal(c.calls.filter((call) => call.id === repairedId && call.buffer?.toString() === 'first').length, 1,
      'copied completed command is not executed again on target')
    t.equal(c.calls.filter((call) => call.id === donorId && call.buffer?.toString() === 'first').length, 1)
  } finally { await c.close() }
  t.end()
})

test('idle drains, unapplied CMD suffixes, and commands received behind a repair fence', (t) => {
  for (const state of ['follower', 'candidate', 'unapplied suffix', 'repair fence']) {
    const f = protocol().setup()
    try {
      if (state === 'candidate') {
        f.tick(f.proc.executor.timers.get(f.proc.engine.electionTimer).due)
        t.equal(f.proc.engine.state, 'candidate')
      }
      if (state === 'repair fence') {
        const r = f.rpc(append({ seqP: 0n, termP: 1n, data: [record(1n, cmd())], commitSeq: 1n }))
        f.complete(messages(r, 'execute')[0])
        // Use the real protocol to establish the nonfatal fence.
        const sync = pack({ type: 'sync', cmdSeq: 1n, quorum: false, agree: [], disagree: ['a', 'b', 'c'] })
        f.rpc(append({ seqP: 1n, termP: 1n, data: [record(1n, sync)], commitSeq: 2n }))
        const r2 = f.rpc(append({ seqP: 2n, termP: 1n, data: [record(1n, cmd())], commitSeq: 3n }))
        t.equal(messages(r2, 'execute').length, 0, 'fenced node never executes a new CMD')
        t.notOk(f.proc.isOpen)
        t.equal(f.state().meta.applied_seq, 2n)
        continue
      }
      if (state === 'unapplied suffix') {
        f.rpc(append({ seqP: 0n, termP: 1n, data: [record(1n, cmd())] }))
      }
      const before = f.rows()
      const r = f.input('drain', { requestId: 'drain' })
      t.ok(messages(r, 'ack', 'client').some((msg) => msg.cid === 'drain'), state)
      t.deepEqual(f.rows(), before)
      if (state === 'unapplied suffix') {
        const committed = f.rpc(append({ seqP: 1n, termP: 1n, commitSeq: 1n }))
        t.equal(messages(committed, 'execute').length, 0, 'drain leaves wholly unapplied CMD for restart')
        t.equal(readApply(f.db2).seq, 0n)
      }
    } finally { f.close() }
  }
  t.end()
})

test('drain before open rejects and warning-initiated drain finishes the active batch', async (t) => {
  let draining
  let target
  const c = cluster({
    apply(id, db, data, term, seq, index) {
      if (seq === 0n) { db.exec(schema); return null }
      if (index === 0) { throw new Error('first item rejected') }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
      return data.toString()
    },
  })
  try {
    t.equal((await rejection(c.gates()[0].drainCmd())).code, Err.NODE_NOT_OPEN)
    await c.open()
    target = c.nodes.get(c.ids.find((id) => id !== c.leader().id))
    target.apply.on('warn', () => { draining = target.gate.drainCmd(); draining.catch(() => {}) })
    const [seq, result] = await c.leader().appendBatch([buf('reject'), buf('keep')])
    await until(() => draining, 'drain initiated from warning')
    await draining
    t.deepEqual(result.map((item) => item.status), ['rejected', 'fulfilled'])
    t.notOk(target.gate.isOpen)
    for (const role of roles) { t.equal(target[role].db, null) }
    const db = new DatabaseSync(`${c.directory}/${target.gate.id}2`, { readOnly: true, readBigInts: true })
    try {
      t.deepEqual(rows(db), ['keep'])
      t.equal(readApply(db).seq, seq)
    } finally { db.close() }
    const reopened = c.makeNode(target.gate.id)
    await reopened.gate.open()
    await until(() => reopened.procA.engine.appliedSeq > seq, 'reopened drained checkpoint')
    t.equal(c.calls.filter((call) => call.id === target.gate.id && call.seq === seq).length, 2,
      'both items execute once despite warning and restart')
  } finally { await c.close() }
  t.end()
})

test('drain rejects on transaction loss, pending SYNC failure, and terminal close failure', async (t) => {
  for (const fault of ['transaction', 'sync', 'close']) {
    const held = deferred()
    const entered = deferred()
    let target = null
    let armed = false
    let heldPage
    let dbClose
    let ownerDb
    const c = cluster({
      opts: { localTimeout: 300, applyTimeout: 300 },
      async apply(id, db, data, term, seq) {
        if (seq === 0n) { db.exec(schema); return null }
        if (armed && id === target && fault === 'transaction') {
          entered.resolve()
          await held.promise
          db.exec("INSERT OR ROLLBACK INTO items VALUES (1, x'00')")
        }
        db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
        return data.toString()
      },
      transport({ to, message, deliver }) {
        if (armed && fault === 'sync' && to === target && message.type === 'append' &&
            message.data?.some(({ entry }) => entry.length && unpack(entry).type === 'sync')) {
          heldPage = deliver
          return false
        }
      },
    })
    try {
      await c.open()
      await c.leader().append(buf('baseline'))
      await c.settled()
      target = c.ids.find((id) => id !== c.leader().id)
      const node = c.nodes.get(target)
      let closed = 0
      node.gate.on('close', () => { closed++ })
      armed = true
      if (fault === 'close') {
        ownerDb = node.apply.db
        dbClose = ownerDb.close.bind(ownerDb)
        ownerDb.close = () => { throw new Err.ErrorWithCode('terminal close failed', Err.SQLITE_ERROR, 10) }
      } else {
        const work = c.leader().append(buf('during drain'))
        work.catch(() => {})
        if (fault === 'transaction') { await entered.promise }
        else {
          await until(() => heldPage && node.procA.engine.pendingCommand, 'pending SYNC')
          for (const role of roles) node[role].db.exec(`
            CREATE TRIGGER fail_sync BEFORE UPDATE ON monsterft_meta
            WHEN NEW.pending_cmd_seq IS NULL
            BEGIN SELECT RAISE(ABORT, 'drain SYNC failed'); END
          `)
        }
      }
      const draining = rejection(node.gate.drainCmd())
      held.resolve()
      if (fault === 'sync') {
        armed = false
        heldPage()
      }
      const err = await draining
      t.equal(err.code, fault === 'close' ? Err.SQLITE_ERROR :
        fault === 'transaction' ? Err.APPLY_TIMEOUT : Err.LOCAL_TIMEOUT, fault)
      t.equal(closed, 0, 'failed drain never claims coordinated shutdown succeeded')
      t.ok(c.errors.some(({ id, role, error }) => id === target && role !== 'gate' &&
        (fault === 'transaction' ? error.sqlCode === 1555 : /drain SYNC|terminal close/.test(error.message))),
      'database owner reports the actual storage failure')
      armed = false
      if (dbClose) { ownerDb.close = dbClose }
      await node.gate.close()
      t.equal(closed, 1, 'explicit cleanup confirms closure after the fault is removed')
      if (fault === 'sync') {
        for (const suffix of ['a', 'b', 'c']) {
          const db = new DatabaseSync(`${c.directory}/${target}.${suffix}`)
          try { db.exec('DROP TRIGGER fail_sync') } finally { db.close() }
        }
      }
      const reopened = c.makeNode(target)
      await reopened.gate.open()
      await until(() => rows(reopened.apply.db).length === (fault === 'close' ? 1 : 2), 'recovery after failed drain')
      await until(() => reopened.procA.engine.pendingCommand === null, 'recovered command decision')
      t.equal(c.calls.filter((call) => call.id === target && call.buffer?.toString() === 'baseline').length, 1,
        'completed baseline never reexecutes')
      if (fault === 'sync') {
        t.equal(c.calls.filter((call) => call.id === target && call.buffer?.toString() === 'during drain').length, 1,
          'committed receipt survives failed SYNC and drain')
      }
    } finally {
      armed = false
      held.resolve()
      if (dbClose) { ownerDb.close = dbClose }
      await c.close()
    }
  }
  t.end()
})

test('closing a forwarded caller abandons its reply without affecting later callers', async (t) => {
  let caller
  let heldReply
  let hold = true
  const c = cluster({
    transport({ to, message, deliver }) {
      if (hold && to === caller && message.type === 'ack' && message.results) {
        heldReply = deliver
        return false
      }
    },
  })
  try {
    await c.open()
    caller = c.ids.find((id) => id !== c.leader().id)
    const gate = c.nodes.get(caller).gate
    let settlements = 0
    const work = gate.append(buf('old caller')).then(
      (value) => { settlements++; return { value } },
      (error) => { settlements++; return { error } },
    )
    await until(() => heldReply, 'held forwarded result')
    await gate.close()
    t.equal((await work).error.code, Err.NODE_NOT_OPEN)
    heldReply()
    t.equal(settlements, 1)
    hold = false
    const reopened = c.makeNode(caller)
    await reopened.gate.open()
    await reopened.gate.awaitLeader()
    t.equal((await reopened.gate.append(buf('new caller')))[1], 'new caller')
    t.equal(settlements, 1)
    const before = reopened.gate.term
    reopened.gate.onReceive('unknown', encodeFrame({ arbitrary: 'ignored' }))
    reopened.gate.onReceive(caller, encodeFrame({ arbitrary: 'ignored' }))
    t.equal(reopened.gate.term, before, 'unknown and self senders are ignored at Gate')
  } finally { await c.close() }
  t.end()
})
