import test from 'tape'
import crypto from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { pack } from 'msgpackr'
import { MonsterApply } from '../src/apply.js'
import { ProtocolStore } from '../src/storage.js'
import { readApply } from '../src/utils.js'
import { digestCmd, digestEntry, outcomesPublic } from '../src/utilm.js'
import { encodeFrame, decodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const schema = 'CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)'
const buf = (value) => Buffer.from(value)
const deferred = () => {
  let resolve
  const promise = new Promise((done) => { resolve = done })
  return { promise, resolve }
}

function fixture(apply) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'monster-apply-'))
  const base = path.join(dir, 'node')
  for (const role of ['procA', 'procB', 'procC']) {
    const store = new ProtocolStore(role, base)
    store.open()
    store.close()
  }
  const replies = []
  const diagnostics = []
  const component = new MonsterApply('node', (to, frame) => {
    if (to !== 'gate') { throw new Error('unexpected destination') }
    replies.push(decodeFrame(frame))
  }, base, { apply })
  for (const type of ['warn', 'error', 'fatal']) {
    component.on(type, (err) => diagnostics.push({ type, err }))
  }
  let cid = 0
  let tick = 0n
  const request = async (msg) => {
    msg = { cid: `request-${++cid}`, ...msg }
    await component.onReceive('gate', encodeFrame(msg))
    return replies.find((reply) => reply.cid === msg.cid)
  }
  return {
    component, replies, diagnostics, dir, base, request,
    async start() {
      await request({ type: 'hello' })
      await request({ type: 'open' })
      await request({ type: 'ready' })
    },
    execute(seq, items, extra={}) {
      return request({
        type: 'execute', term: 3n, seq, entryHash: digestEntry(3n, buf(`${seq}`)),
        tickCount: ++tick, items,
        matchIndex: seq === 0n ? null : [-1n, 0n, seq - 1n], ...extra,
      })
    },
    async close() {
      await component.close()
      fs.rmSync(dir, { recursive: true, force: true })
    },
  }
}

test('Apply commits setup and CMD receipts without hashing receipt writes', async (t) => {
  const calls = []
  const f = fixture((db, data, term, seq, index, matchIndex) => {
    calls.push({ data, term, seq, index, matchIndex })
    if (seq === 0n) { db.exec(schema); return }
    const value = Number(data.toString())
    db.prepare('INSERT INTO items VALUES (?, ?)').run(index, value)
    return value * 2
  })
  try {
    await f.start()
    const setup = await f.execute(0n, null)
    t.equal(setup.type, 'ack')
    t.deepEqual(setup.receipt.local_digest, Buffer.alloc(32), 'setup uses zero digest')
    t.deepEqual(setup.receipt.entry_hash, digestEntry(3n, buf('0')), 'setup keeps requested entry hash')
    t.deepEqual(calls[0], { data: null, term: 3n, seq: 0n, index: 0, matchIndex: null },
      'setup callback arguments remain unchanged')
    const result = await f.execute(5n, [buf('7'), buf('9')])
    t.deepEqual(result.outcomes, [[0, 14], [0, 18]])
    t.equal(f.component.tickCount, 2n)
    t.deepEqual(calls[2].matchIndex, [-1n, 0n, 4n])
    t.ok(Buffer.isBuffer(calls[2].data), 'callback receives payload Buffer')
    t.equal(readApply(f.component.db).seq, 5n, 'sequence gaps for intervening protocol entries are allowed')
    const control = new DatabaseSync(path.join(f.dir, 'control'))
    let patchset
    try {
      control.exec(schema)
      const session = control.createSession()
      control.exec('INSERT INTO items VALUES (0, 7), (1, 9)')
      patchset = Buffer.from(session.patchset())
      session.close()
    } finally { control.close() }
    const expected = crypto.createHash('sha256')
      .update(pack([3n, 5n, patchset, [[0, 14], [0, 18]]])).digest()
    t.deepEqual(result.receipt.local_digest, expected,
      'digest bytes match the original formula and exclude receipt metadata')
    t.deepEqual(f.diagnostics, [], 'success emits no diagnostics')
  } finally { await f.close() }
  t.end()
})

test('Apply retains failed-item diagnostics, rolls back items, and preserves outcomes', async (t) => {
  const original = new Err.ErrorWithCode('rejected item', 123, 456)
  original.stack = 'custom application stack'
  const f = fixture(async function applyItems(db, data, term, seq, index) {
    if (seq === 0n) { db.exec(schema); return }
    db.prepare('INSERT INTO items VALUES (?, ?)').run(index, index)
    const kind = data.toString()
    if (kind === 'reject') { await Promise.resolve(); throw original }
    if (kind === 'sqlite') { db.exec('INSERT INTO items VALUES (0, 9)') }
    if (kind === 'encoding') { return Symbol('unencodable') }
    return index
  })
  try {
    await f.start()
    await f.execute(0n, null)
    const listenerError = new Error('listener throw')
    const warnings = []
    f.component.prependListener('error', () => { throw new Error('error listener throw') })
    f.component.on('warn', (warning) => {
      warnings.push({ message: warning.message, code: warning.code, stack: warning.stack })
      warning.message = 'listener mutation'
      warning.code = 999
      warning.stack = 'changed stack'
      throw listenerError
    })
    let laterWarnings = 0
    f.component.on('warn', () => { laterWarnings++ })
    const result = await f.execute(1n, [buf('ok'), buf('reject'), buf('sqlite'), buf('encoding')])
    t.equal(result.type, 'ack', 'ordinary failures are agreed outcomes')
    t.notOk(Object.hasOwn(result, 'warnings'), 'execution ACK carries no diagnostics')
    t.equal(warnings.length, 3)
    t.equal(laterWarnings, 3, 'throwing listeners cannot hide warnings from later listeners')
    t.equal(f.diagnostics.filter(({ type, err }) =>
      type === 'error' && err === listenerError).length, 3,
    'later error listeners receive the original exception even if an earlier one throws')
    t.ok(warnings.every((warning) => warning.code === Err.APPLY_ERROR))
    t.deepEqual(result.outcomes[1], [1, 'rejected item', 123, 456, original.stack])
    t.equal(warnings[0].stack, original.stack)
    t.equal(warnings[0].message, 'rejected item')
    t.equal(result.outcomes[2][2], Err.SQLITE_ERROR)
    t.equal(result.outcomes[2][3], 1555)
    t.equal(result.outcomes[3][2], Err.APPLY_ERROR)
    t.deepEqual(f.component.db.prepare('SELECT id FROM items').all().map((row) => row.id), [0n],
      'failed item writes roll back while successful writes survive')
    t.equal(f.diagnostics.filter((event) => event.type === 'fatal').length, 0)
    const publicOutcomes = outcomesPublic(result.outcomes)
    t.equal(publicOutcomes[1].reason.stack, original.stack)
  } finally { await f.close() }
  t.end()
})

test('command digests exclude stacks while rejected result tuples preserve them', (t) => {
  const outcome = (stack) => [{ status: 'rejected', error: {
    message: 'failure', code: 55, sqlCode: null, stack,
  } }]
  const patchset = buf('independent patch bytes')
  const expected = crypto.createHash('sha256')
    .update(pack([9n, 13n, patchset, [[1, 'failure', 55, null]]])).digest()
  t.deepEqual(digestCmd(9n, 13n, patchset, outcome('A trace')), expected)
  t.deepEqual(digestCmd(9n, 13n, patchset, outcome('B trace')), expected)
  t.end()
})

test('Apply fatal transaction loss and rollback failures preserve the original error', async (t) => {
  for (const mode of ['transaction loss', 'rollback failure']) {
    const original = new Error('original apply failure')
    const f = fixture((db, data, term, seq, index) => {
      if (seq === 0n) { db.exec(`${schema}; INSERT INTO items VALUES (0, 1)`); return }
      if (index === 0) { db.exec('INSERT INTO items VALUES (1, 2)'); return }
      if (mode === 'transaction loss') {
        db.exec('INSERT OR ROLLBACK INTO items VALUES (0, 3)')
      }
      throw original
    })
    try {
      await f.start()
      await f.execute(0n, null)
      if (mode === 'rollback failure') {
        const exec = f.component.db.exec.bind(f.component.db)
        f.component.db.exec = (sql) => {
          if (sql === 'ROLLBACK TO monsterft_item') { throw new Error('rollback failed') }
          return exec(sql)
        }
      }
      const reply = await f.execute(1n, [buf('first'), buf('second')])
      await f.component.close()
      t.equal(reply, undefined, `${mode} sends no execution reply`)
      const fatal = f.diagnostics.filter(({ type }) => type === 'error').at(-1).err
      t.equal(fatal.code, Err.APPLY_ERROR)
      t.equal(f.diagnostics.filter(({ type }) => type === 'warn').length, 1)
      const events = f.diagnostics.map((event) => event.type)
      t.equal(events[0], 'warn', 'item warning precedes fatal reporting')
      t.notOk(events.includes('fatal'), 'error is the only public fatal diagnostic')
      if (mode === 'rollback failure') {
        t.ok(f.diagnostics.some(({ err }) => err.stack === original.stack),
          'rollback failure does not hide the original trace')
        t.match(fatal.message, /rollback failed/)
      } else {
        t.equal(fatal.sqlCode, 1555)
      }
      const db = new DatabaseSync(`${f.base}2`, { readOnly: true, readBigInts: true })
      try {
        t.equal(readApply(db).seq, 0n, 'failed execution does not advance receipt')
        t.deepEqual(db.prepare('SELECT id FROM items').all().map((row) => row.id), [0n],
          'fatal shutdown rolls back the complete batch')
      } finally { db.close() }
    } finally { await f.close() }
  }
  t.end()
})

test('Apply close waits for callbacks and suppresses late completion', async (t) => {
  const entered = deferred()
  const resume = deferred()
  const f = fixture(async (db, data, term, seq) => {
    if (seq === 0n) { db.exec(schema); return }
    db.exec('INSERT INTO items VALUES (1, 2)')
    entered.resolve()
    await resume.promise
    db.exec('INSERT INTO items VALUES (2, 3)')
    throw new Error('late error')
  })
  try {
    await f.start()
    await f.execute(0n, null)
    const execution = f.execute(1n, [buf('work')])
    await entered.promise
    let stopped = false
    const closing = f.component.close().then(() => { stopped = true })
    await Promise.resolve()
    t.notOk(stopped, 'close cannot claim the callback has stopped early')
    resume.resolve()
    t.equal(await execution, undefined, 'no reply escapes shutdown')
    await closing
    t.deepEqual(f.diagnostics, [], 'late application failure remains quiet')
    const db = new DatabaseSync(`${f.base}2`, { readOnly: true, readBigInts: true })
    try {
      t.equal(db.prepare('SELECT count(*) AS count FROM items').get().count, 0n)
      t.equal(readApply(db).seq, 0n)
    } finally { db.close() }
  } finally { await f.close() }
  t.end()
})

test('Apply replies with the close error and acknowledges only successful teardown', async (t) => {
  const f = fixture(() => null)
  let db
  let close
  try {
    await f.start()
    db = f.component.db
    close = db.close.bind(db)
    const original = new Err.ErrorWithCode('injected close failure', Err.SQLITE_ERROR, 10)
    db.close = () => { throw original }
    const reply = await f.request({ type: 'close', cid: 'failed-close' })
    t.equal(reply?.type, 'err', 'failed teardown returns ERR instead of timing out')
    t.equal(reply?.cid, 'failed-close', 'error refers to the close request')
    t.equal(reply?.message, 'DB2 close injected close failure')
    t.equal(reply?.code, Err.SQLITE_ERROR)
    t.equal(reply?.sqlCode, 10)
    t.equal(reply?.stack, original.stack, 'error keeps the original trace')
    t.equal(f.component.db, db, 'failed close retains the connection for cleanup')
    t.equal(f.diagnostics.filter(({ type }) => type === 'error').length, 1,
      'close failure is also reported locally once')
    db.close = close
    t.equal((await f.request({ type: 'close' })).type, 'ack', 'retry confirms successful teardown')
    t.equal(f.component.db, null)
  } finally {
    if (db && close) { db.close = close }
    await f.close()
  }
  t.end()
})

test('Apply warning listeners may close without subsequent item work', async (t) => {
  let calls = 0
  const f = fixture((db, data, term, seq) => {
    if (seq === 0n) { db.exec(schema); return }
    calls++
    db.exec('INSERT INTO items VALUES (1, 2)')
    throw new Error('stop after warning')
  })
  try {
    await f.start()
    await f.execute(0n, null)
    f.component.on('warn', () => { f.component.close() })
    const reply = await f.execute(1n, [buf('first'), buf('second')])
    await f.component.close()
    t.equal(reply, undefined)
    t.equal(calls, 1)
    t.equal(f.diagnostics.length, 1, 'shutdown prevents additional errors and warnings')
    const db = new DatabaseSync(`${f.base}2`, { readOnly: true, readBigInts: true })
    try {
      t.equal(db.prepare('SELECT count(*) AS count FROM items').get().count, 0n)
      t.equal(readApply(db).seq, 0n)
    } finally { db.close() }
  } finally { await f.close() }
  t.end()
})

test('Apply setup errors stay fatal without per-item warnings and cannot commit ended transactions', async (t) => {
  for (const mode of ['throw', 'ended']) {
    const f = fixture((db) => {
      db.exec(schema)
      if (mode === 'throw') { throw new Error('setup error') }
      db.exec('ROLLBACK')
    })
    try {
      await f.start()
      const reply = await f.execute(0n, null)
      await f.component.close()
      t.equal(reply, undefined, 'fatal setup failure sends no reply')
      const diagnostic = f.diagnostics.find(({ type }) => type === 'error').err
      t.equal(diagnostic.code, Err.APPLY_ERROR)
      t.match(diagnostic.message, mode === 'throw' ? /setup error/ : /DB2 seq0 transaction ended/)
      t.equal(f.diagnostics.filter((event) => event.type === 'warn').length, 0)
      t.equal(f.diagnostics.filter((event) => event.type === 'fatal').length, 0)
      const db = new DatabaseSync(`${f.base}2`, { readOnly: true, readBigInts: true })
      try { t.equal(readApply(db).seq, -1n) } finally { db.close() }
    } finally { await f.close() }
  }
  t.end()
})

test('Apply reports setup failures locally without attempting error transmission', async (t) => {
  for (const asynchronous of [false, true]) {
    const original = new Error('original setup failure')
    const f = fixture(() => {
      if (asynchronous) { return Promise.reject(original) }
      throw original
    })
    try {
      await f.start()
      let sends = 0
      f.component._localSend = () => { sends++; throw new Error('transport is broken') }
      t.equal(await f.execute(0n, null), undefined)
      await f.component.close()
      t.equal(sends, 0, 'fatal reporting does not depend on local transport')
      t.equal(f.diagnostics.length, 1, 'the original error is reported once')
      const { type, err } = f.diagnostics[0]
      t.equal(type, 'error')
      t.equal(err.message, '(apply) original setup failure')
      t.equal(err.code, Err.APPLY_ERROR)
      t.equal(err.stack, original.stack, 'the original trace survives fatal reporting')
      t.equal(f.component.db, null, 'Apply closes its own database')
    } finally { await f.close() }
  }
  t.end()
})

test('Apply rejects duplicate execution and hello after startup', async (t) => {
  let calls = 0
  const f = fixture(() => { calls++ })
  try {
    const hello1 = await f.request({ type: 'hello' })
    const hello2 = await f.request({ type: 'hello' })
    t.equal(hello1.type, 'ack')
    t.equal(hello2.type, 'ack')
    await f.request({ type: 'open' })
    await f.request({ type: 'ready' })
    await f.execute(0n, null)
    const reply = await f.execute(0n, null)
    t.equal(reply, undefined, 'invalid execution sends no ERR')
    t.equal(f.diagnostics.find(({ type }) => type === 'error').err.code, Err.MONSTER_ILLEGAL)
    t.equal(calls, 1, 'completed setup is never reapplied')
  } finally { await f.close() }
  const second = fixture(() => {})
  try {
    await second.start()
    t.equal(await second.request({ type: 'hello' }), undefined, 'post-startup hello has no ACK')
    t.notOk(second.component.isOpen)
    t.equal(second.diagnostics.filter((event) => event.type === 'error').length, 1)
    t.equal(second.diagnostics.filter((event) => event.type === 'fatal').length, 0)
  } finally { await second.close() }
  t.end()
})

test('Apply preserves the correct receipt on either side of COMMIT failure', async (t) => {
  for (const committed of [false, true]) {
    let executions = 0
    const f = fixture((db, data, term, seq) => {
      if (seq === 0n) { db.exec(schema); return }
      executions++
      db.exec('INSERT INTO items VALUES (1, 2)')
    })
    let reopened = null
    try {
      await f.start()
      await f.execute(0n, null)
      const exec = f.component.db.exec.bind(f.component.db)
      f.component.db.exec = (sql) => {
        if (sql === 'COMMIT') {
          if (committed) { exec(sql) }
          throw new Error('injected commit failure')
        }
        return exec(sql)
      }
      const reply = await f.execute(1n, [buf('work')])
      await f.component.close()
      t.equal(reply, undefined, 'failed commit sends no execution reply')
      t.match(f.diagnostics.find(({ type }) => type === 'error').err.message, /injected commit failure/)
      const closeReply = await f.request({ type: 'close' })
      t.equal(closeReply.type, 'ack', 'a fatally closed Apply still confirms completed teardown')
      const replies = []
      const errors = []
      reopened = new MonsterApply('node', (to, frame) => replies.push(decodeFrame(frame)),
        f.base, { apply: () => { executions++ } })
      reopened.on('error', (err) => errors.push(err))
      const receive = async (msg) => {
        await reopened.onReceive('gate', encodeFrame(msg))
        return replies.find((reply) => reply.cid === msg.cid)
      }
      await receive({ type: 'hello', cid: 'h' })
      await receive({ type: 'open', cid: 'o' })
      await receive({ type: 'ready', cid: 'r' })
      t.equal(readApply(reopened.db).seq, committed ? 1n : 0n)
      t.equal(reopened.db.prepare('SELECT count(*) AS count FROM items').get().count,
        committed ? 1n : 0n, 'data and receipt share the same commit boundary')
      t.equal(executions, 1, 'opening never executes old apply work')
      if (committed) {
        const duplicate = await receive({
          type: 'execute', cid: 'duplicate', tickCount: 1n, term: 3n, seq: 1n,
          entryHash: digestEntry(3n, buf('1')), items: [buf('work')], matchIndex: [0n],
        })
        t.equal(duplicate, undefined, 'duplicate execution cannot produce a reply')
        t.equal(errors[0].code, Err.MONSTER_ILLEGAL)
        t.equal(executions, 1, 'committed execution cannot be repeated after a lost ACK')
      }
    } finally {
      if (reopened !== null) { await reopened.close() }
      await f.close()
    }
  }
  t.end()
})

test('Apply validates execution and frame checksums before invoking user code', async (t) => {
  for (const kind of ['invalid items', 'corrupt frame']) {
    let calls = 0
    const f = fixture(() => { calls++ })
    try {
      await f.start()
      if (kind === 'invalid items') {
        const reply = await f.execute(0n, [buf('not setup')])
        t.equal(reply, undefined)
        t.equal(f.diagnostics.find(({ type }) => type === 'error').err.code, Err.RPC_ILLEGAL)
      } else {
        const frame = encodeFrame({ type: 'execute', cid: 'corrupt' })
        frame[0] ^= 1
        await f.component.onReceive('gate', frame)
        t.equal(f.diagnostics.find(({ type }) => type === 'error').err.code, Err.FRAME_CORRUPT)
      }
      await f.component.close()
      t.equal(calls, 0)
      const db = new DatabaseSync(`${f.base}2`, { readOnly: true, readBigInts: true })
      try { t.equal(readApply(db).seq, -1n) } finally { db.close() }
    } finally { await f.close() }
  }
  t.end()
})
