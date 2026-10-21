import test from 'tape'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import * as api from '../src/index.js'
import { decodeFrame } from '../src/wire.js'
import { cluster, until } from './helper.js'

test('public components are explicitly wired and preserve command APIs', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-public-'))
  const nodes = new Map()
  const ids = ['a', 'b', 'c']
  const errors = []
  const calls = []
  try {
    for (const id of ids) {
      const components = new Map()
      const localSend = (from) => (to, frame) => components.get(to).onReceive(from, frame)
      const base = path.join(directory, id)
      const opts = { electionTimeout: 150, appendTimeout: 1_000, tickInterval: 2 }
      const gate = new api.Gate(id, ids,
        (to, frame) => nodes.get(to)?.gate.onReceive(id, frame), localSend('gate'), () => opts)
      components.set('gate', gate)
      for (const role of ['procA', 'procB', 'procC']) {
        components.set(role, new api.MonsterProc(role, id, ids, localSend(role), base, opts))
      }
      components.set('apply', new api.MonsterApply(id, localSend('apply'), base, {
        apply(db, buffer, term, seq, index, matchIndex) {
          calls.push({ id, seq, term, index, matchIndex })
          if (seq === 0n) {
            db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT) STRICT')
            return null
          }
          db.prepare('INSERT INTO items (value) VALUES (?)').run(buffer.toString())
          return { item: buffer.toString(), seq }
        },
      }))
      for (const component of components.values()) { component.on('error', (err) => errors.push(err)) }
      nodes.set(id, { gate, components })
    }
    t.equal(api.MonsterFt, undefined, 'automatic component wrapper is not exported')
    t.equal(api.RaftNode, undefined, 'standalone RaftNode is not exported')
    await Promise.all([...nodes.values()].map(({ gate }) => gate.open()))
    await Promise.all([...nodes.values()].map(({ gate }) => gate.awaitLeader(true)))
    const node = nodes.get('a').gate
    const commit = node.awaitEvent('commit', (seq) => seq > 0n)
    const [seq, result] = await node.append(Buffer.from('one'))
    t.deepEqual(result, { item: 'one', seq })
    t.equal(typeof await commit, 'bigint', 'commit event preserves its bigint argument')
    const [batchSeq, outcomes] = await nodes.get('b').gate.appendBatch([Buffer.from('two'), Buffer.from('three')])
    t.deepEqual(outcomes, [
      { status: 'fulfilled', value: { item: 'two', seq: batchSeq } },
      { status: 'fulfilled', value: { item: 'three', seq: batchSeq } },
    ])
    await until(() => [...nodes.values()].every((member) =>
      member.components.get('apply').db.prepare('SELECT count(*) AS n FROM items').get().n === 3n),
    'explicit components apply each item')
    t.equal(calls.filter((call) => call.seq === 0n).length, 3)
    t.equal(calls.filter((call) => call.seq > 0n).length, 9)
    t.ok(calls.filter((call) => call.seq > 0n).every((call) =>
      call.matchIndex.length === 3 && typeof call.term === 'bigint'), 'apply callback retains protocol arguments')
    await Promise.all([...nodes.values()].map(({ gate }) => gate.close()))
    await assert.rejects(node.open(), { code: api.ErrorCodes.NODE_NOT_OPEN })
    t.notOk(node.isOpen, 'a closed instance cannot pretend that its old open promise reopened it')
    t.ok([...nodes.values()].every(({ components }) => [...components]
      .filter(([role]) => role !== 'gate').every(([, component]) => component.db === null)),
    'Gate close waits for all explicitly wired database owners')
    t.equal(errors.length, 0)
  } finally {
    await Promise.all([...nodes.values()].map(({ gate }) => gate.close().catch(() => {})))
    for (const { components } of nodes.values()) {
      for (const [role, component] of components) {
        if (role !== 'gate') { await component.close() }
      }
    }
    fs.rmSync(directory, { recursive: true, force: true })
  }
  t.end()
})

test('public constructor rejects malformed component configuration', (t) => {
  const make = (opts = {}, localSend = () => {}) =>
    new api.Gate('a', ['a', 'b', 'c'], () => {}, localSend, opts)
  for (const opts of [
    null, 1, () => null,
    { tickInterval: 0 }, { localTimeout: 0 }, { applyTimeout: 0 },
  ]) {
    t.throws(() => make(opts), { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  }
  for (const localSend of [null, 1]) {
    t.throws(() => make({}, localSend), { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  }
  t.throws(() => new api.Gate('a', ['a', 'b', 'c'], () => {}),
    { code: api.ErrorCodes.ARGUMENT_ILLEGAL }, 'local transport is required')
  t.throws(() => new api.Gate('a', ['a', 'a', 'b'], () => {}, () => {}),
    { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  for (const id of ['procA', 'procB', 'procC', 'apply', 'gate', 'event', 'client']) {
    for (const localId of [id, 'a']) {
      t.throws(() => new api.Gate(localId, ['a', 'b', id], () => {}, () => {}),
        { code: api.ErrorCodes.ARGUMENT_ILLEGAL, message: 'node ids must not name internal destinations' },
        `${id} is rejected as a ${localId === id ? 'local' : 'peer'} id`)
    }
  }
  t.throws(() => new api.MonsterApply('a', () => {}, '/tmp/not-opened-v2', { apply: null }),
    { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  t.throws(() => new api.MonsterApply('a', () => {}, ':memory:', { apply: () => null }),
    { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  t.throws(() => new api.MonsterProc('procA', 'a', ['a', 'b', 'c'], () => {}, ':memory:'),
    { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  t.end()
})

test('public close waits for explicitly wired Apply to acknowledge shutdown', async (t) => {
  let hold = false
  let release
  const c = cluster({
    local({ id, from, to, frame, deliver }) {
      if (!hold || id !== 'a' || from !== 'gate' || to !== 'apply') { return }
      if (decodeFrame(frame).type === 'close') { release = deliver; return false }
    },
  })
  try {
    await c.open()
    hold = true
    const node = c.nodes.get('a').gate
    let closed = false
    const closing = node.close().then(() => { closed = true })
    await until(() => release, 'held Apply close')
    t.notOk(closed, 'close remains pending until every owner has stopped')
    t.ok(c.nodes.get('a').apply.isOpen, 'Apply still owns its database while close delivery is held')
    hold = false
    release()
    await closing
    t.ok(closed)
    t.equal(c.nodes.get('a').apply.db, null, 'Apply closed before Gate close resolves')
  } finally { hold = false; release?.(); await c.close() }
  t.end()
})

test('public close shares its attempt with synchronous Proc close listeners', async (t) => {
  const requests = []
  const c = cluster({
    local({ id, from, to, frame }) {
      if (id === 'a' && from === 'gate' && decodeFrame(frame).type === 'close') {
        requests.push(to)
      }
    },
  })
  try {
    await c.open()
    await c.settled()
    const { gate, procA } = c.nodes.get('a')
    let reentrantClose
    let closeEvents = 0
    procA.once('close', () => { reentrantClose = gate.close() })
    gate.on('close', () => { closeEvents++ })

    const closing = gate.close()
    await closing
    t.equal(reentrantClose, closing, 'the Proc listener receives the existing close promise')
    t.deepEqual(requests.sort(), ['apply', 'procA', 'procB', 'procC'],
      'each owner receives exactly one close request')
    t.equal(closeEvents, 1, 'Gate emits close only once')
    t.equal(gate.close(), closing, 'successful close remains shared')
    t.equal(requests.length, 4, 'repeated close sends no more requests')
  } finally { await c.close() }
  t.end()
})

test('public close shares a failed attempt with synchronous Proc error listeners', async (t) => {
  const requests = []
  const failedOwners = []
  const c = cluster({
    local({ id, from, to, frame }) {
      if (id === 'a' && from === 'gate' && decodeFrame(frame).type === 'close') {
        requests.push(to)
      }
    },
  })
  let db
  let closeDatabase
  try {
    await c.open()
    await c.settled()
    const { gate, procA } = c.nodes.get('a')
    failedOwners.push(procA)
    db = procA.db
    closeDatabase = db.close
    const failure = Object.assign(new Error('injected DB1 close failure'), {
      code: 'ERR_SQLITE_ERROR', errcode: 5,
    })
    const stack = failure.stack
    let closeCalls = 0
    db.close = () => {
      if (++closeCalls === 1) { throw failure }
      return closeDatabase.call(db)
    }
    let reentrantClose
    let closeEvents = 0
    procA.once('error', () => {
      reentrantClose = gate.close()
      reentrantClose.catch(() => {})
    })
    gate.on('close', () => { closeEvents++ })

    const failedClose = gate.close()
    await assert.rejects(failedClose, {
      message: '(log close) injected DB1 close failure',
      code: api.ErrorCodes.SQLITE_ERROR, sqlCode: 5, stack,
    })
    t.equal(reentrantClose, failedClose, 'the Proc error listener shares the failing attempt')
    t.equal(closeCalls, 1, 'the error listener does not retry the database close')
    t.equal(procA.db, db, 'the failed owner retains its database until process exit')
    t.deepEqual(requests.sort(), ['apply', 'procA', 'procB', 'procC'],
      'the failing attempt sends one request to each owner')
    t.equal(closeEvents, 0, 'Gate reports no successful close during the failed attempt')

    const repeated = gate.close()
    t.equal(repeated, failedClose, 'a later close retains the original failed attempt')
    await assert.rejects(repeated, {
      message: '(log close) injected DB1 close failure',
      code: api.ErrorCodes.SQLITE_ERROR, sqlCode: 5, stack,
    })
    t.equal(closeCalls, 1, 'later calls do not retry the database close')
    t.equal(procA.db, db, 'failed cleanup remains visible')
    t.equal(requests.length, 4, 'later calls send no new requests')
    t.equal(closeEvents, 0, 'failed cleanup never emits close')
    t.equal(c.errors.filter(({ id, role }) => id === 'a' && role === 'procA').length, 1,
      'the owning Proc reports its cleanup error once')
  } finally {
    if (db) { db.close = closeDatabase }
    await c.close(failedOwners)
  }
  t.end()
})

for (const localDelay of [0, 2]) {
  test(`public close retains Apply database failure (${localDelay}ms local transport)`, async (t) => {
    const failedOwners = []
    const requests = []
    const c = cluster({
      localDelay,
      local({ id, from, to, frame }) {
        if (id === 'a' && from === 'gate' && decodeFrame(frame).type === 'close') {
          requests.push(to)
        }
      },
    })
    let db
    let closeDatabase
    try {
      await c.open()
      await c.settled()
      const { gate, apply } = c.nodes.get('a')
      failedOwners.push(apply)
      db = apply.db
      closeDatabase = db.close
      const failure = Object.assign(new Error('injected database close failure'), {
        code: 'ERR_SQLITE_ERROR', errcode: 5,
      })
      const stack = failure.stack
      db.close = () => { throw failure }
      let closeEvents = 0
      gate.on('close', () => { closeEvents++ })

      const failedClose = gate.close()
      t.equal(gate.close(), failedClose, 'concurrent close calls share the attempt')
      await assert.rejects(failedClose, {
        message: 'DB2 close injected database close failure',
        code: api.ErrorCodes.SQLITE_ERROR, sqlCode: 5, stack,
      })
      t.pass('Gate receives the correlated SQLite error, including its original stack')
      t.equal(closeEvents, 0, 'Gate does not emit a successful close')
      t.equal(apply.db, db, 'Apply retains the database after close fails')
      t.equal(db.prepare('SELECT 1 AS n').get().n, 1n, 'the failed close leaves the database open')

      db.close = closeDatabase
      const repeatedClose = gate.close()
      t.equal(repeatedClose, failedClose, 'later calls retain the original failed attempt')
      await assert.rejects(repeatedClose, {
        message: 'DB2 close injected database close failure',
        code: api.ErrorCodes.SQLITE_ERROR, sqlCode: 5, stack,
      })
      t.equal(apply.db, db, 'removing the fault does not retry owner cleanup')
      t.ok(['procA', 'procB', 'procC'].every((role) => c.nodes.get('a')[role].db === null),
        'healthy Proc owners still close')
      t.deepEqual(requests.sort(), ['apply', 'procA', 'procB', 'procC'],
        'each owner receives one close request')
      t.equal(closeEvents, 0, 'Gate never confirms failed coordinated cleanup')
      t.equal(c.errors.filter(({ id, role }) => id === 'a' && role === 'apply').length, 1,
        'Apply reports its cleanup error once')
    } finally {
      if (db) { db.close = closeDatabase }
      await c.close(failedOwners)
    }
    t.end()
  })
}

test('late Apply cleanup cannot change a failed coordinated close', async (t) => {
  let holdId
  let release
  const requests = []
  const c = cluster({
    opts: { localTimeout: 250, applyTimeout: 2_000 },
    local({ id, from, to, frame }) {
      if (id === holdId && from === 'gate' && decodeFrame(frame).type === 'close') {
        requests.push(to)
      }
    },
    apply(id, db, buffer, term, seq) {
      if (seq === 0n) {
        db.exec('CREATE TABLE items (value TEXT) STRICT')
        return null
      }
      db.prepare('INSERT INTO items VALUES (?)').run(buffer.toString())
      if (id === holdId) {
        return new Promise((resolve) => { release = () => resolve(buffer.toString()) })
      }
      return buffer.toString()
    },
  })
  try {
    await c.open()
    await c.settled()
    const gate = c.leader()
    const node = c.nodes.get(gate.id)
    holdId = gate.id
    const appendFailure = assert.rejects(gate.append(Buffer.from('held')), {
      code: api.ErrorCodes.NODE_NOT_OPEN,
    })
    await until(() => release, 'Apply callback to start')
    let closeEvents = 0
    gate.on('close', () => { closeEvents++ })
    const closing = gate.close()
    await assert.rejects(closing, { code: api.ErrorCodes.LOCAL_TIMEOUT })
    await appendFailure
    t.equal(closeEvents, 0, 'timeout does not report successful owner shutdown')
    t.notEqual(node.apply.db, null, 'Apply still owns its database while the callback is pending')

    release()
    await until(() => node.apply.db === null, 'Apply to finish closing')
    t.equal(gate.close(), closing, 'late owner closure cannot replace the failed attempt')
    await assert.rejects(gate.close(), { code: api.ErrorCodes.LOCAL_TIMEOUT })
    t.ok(['procA', 'procB', 'procC', 'apply'].every((role) => node[role].db === null),
      'owners may finish cleanup after Gate has already timed out')
    t.deepEqual(requests.sort(), ['apply', 'procA', 'procB', 'procC'],
      'no second set of close requests is sent')
    t.equal(closeEvents, 0, 'late replies do not create a Gate close notification')
  } finally {
    release?.()
    await c.close()
  }
  t.end()
})
