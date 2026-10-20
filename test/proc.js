import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { MonsterProc } from '../src/proc.js'
import { Random } from '../src/random.js'
import { openApplyDatabase, readDatabase, databasePaths } from '../src/storage.js'
import * as wire from '../src/wire.js'
import * as Err from '../src/error.js'

const roles = ['procA', 'procB', 'procC']
const seed = Buffer.from('10111213202122233031323340414243', 'hex')
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function fixture(opts={}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-proc-'))
  const base = path.join(dir, 'node')
  const replies = roles.map(() => [])
  const events = roles.map(() => [])
  const procs = roles.map((role, i) => {
    const proc = new MonsterProc(role, 'a', ['a', 'b', 'c'], (to, frame) => {
      if (to !== 'gate') { throw new Error('unexpected destination') }
      replies[i].push(wire.decodeFrame(frame))
    }, base, opts)
    for (const type of ['error', 'fatal', 'warn', 'close']) {
      proc.on(type, (value) => events[i].push({ type, value }))
    }
    return proc
  })
  let cid = 0
  return {
    base, procs, replies, events, paths: databasePaths(base),
    request(i, message) {
      message = { cid: `request-${++cid}`, ...message }
      procs[i].onReceive('gate', wire.encodeFrame(message))
      return replies[i].find((reply) => reply.cid === message.cid)
    },
    startup() {
      for (let i = 0; i < 3; i++) { this.request(i, { type: 'hello', seed }) }
      for (let i = 0; i < 3; i++) { this.request(i, { type: 'open' }) }
      openApplyDatabase(base).close()
      for (const type of ['capture', 'adopt', 'reconcile']) {
        for (let i = 0; i < 3; i++) {
          const reply = this.request(i, { type })
          if (reply?.type !== 'ack') { throw new Error(`startup ${type}: ${JSON.stringify(reply)}`) }
        }
      }
      return this
    },
    input(i, kind, fields={}) {
      return this.request(i, {
        type: 'input', kind, tickCount: procs[i].tickCount + 1n,
        timems: procs[i].executor.timems, ...fields,
      })
    },
    cleanup() {
      for (const proc of procs) { proc.close() }
      fs.rmSync(dir, { recursive: true, force: true })
    },
  }
}

test('Proc hello retains one seed and initializes randomness only on leaving hello', (t) => {
  const f = fixture()
  try {
    t.equal(f.request(0, { type: 'hello', seed }).type, 'ack')
    t.equal(f.request(0, { type: 'hello', seed }).type, 'ack', 'repeated hello acknowledged')
    t.equal(f.procs[0].random, null, 'hello does not initialize or advance RNG')
    t.equal(f.request(0, { type: 'open' }).type, 'ack')
    t.equal(f.procs[0].random.cid(), new Random(seed).cid(), 'first draw uses complete shared state')
    t.equal(f.request(0, { type: 'hello', seed }), undefined, 'post-hello startup cannot ACK another run')
    t.equal(f.events[0].filter((event) => event.type === 'error').length, 1)
    t.notOk(f.events[0].some((event) => event.type === 'fatal'), 'failure uses only the local error event')
    t.equal(f.request(0, { type: 'close' }).type, 'ack', 'already closed Proc acknowledges teardown')
  } finally { f.cleanup() }
  t.end()
})

test('Proc rejects startup work before hello and malformed seeds', (t) => {
  for (const message of [{ type: 'open' }, { type: 'hello', seed: Buffer.alloc(16) }]) {
    const f = fixture()
    try {
      const reply = f.request(0, message)
      t.equal(reply, undefined, 'invalid startup request sends no reply')
      t.equal(f.events[0].find((event) => event.type === 'error').value.code, Err.RPC_ILLEGAL)
      t.notOk(fs.existsSync(f.paths.procA), 'invalid request creates no database')
    } finally { f.cleanup() }
  }
  t.end()
})

test('Proc startup reports agree and input completion carries matched output lists', (t) => {
  const f = fixture().startup()
  try {
    const selections = f.replies.map((replies) => replies.find((reply) => reply.selection).selection)
    t.deepEqual(selections[1], selections[0], 'B independently selects the same state')
    t.deepEqual(selections[2], selections[0], 'C independently selects the same state')
    const start = f.procs.map((proc, i) => f.input(i, 'start'))
    t.ok(start.every((reply) => reply.type === 'ack' && reply.tickCount === 1n))
    t.deepEqual(start[0].outputs.map(wire.checksum), start[1].outputs, 'B checksums match A messages')
    t.deepEqual(start[1].outputs, start[2].outputs, 'C checksums match B')
    t.ok(f.procs.every((proc) => proc.isOpen), 'normal runtime follows coordinated phases')
    const elections = f.procs.map((proc, i) => f.input(i, 'tick', { timems: 10_000 }))
    t.deepEqual(elections[0].outputs.map(wire.checksum), elections[1].outputs,
      'election choices, CIDs and outputs are deterministic')
    t.deepEqual(elections[1].outputs, elections[2].outputs)
    t.ok(f.procs.every((proc) => proc.log.elec.term === 1n), 'tick drives election')
    const quiet = f.input(0, 'tick', { timems: 10_000 })
    t.deepEqual(quiet.outputs, [], 'empty output sequence is acknowledged')
    t.equal(f.procs[0].tickCount, 3n, 'equal timems still advances input count')
  } finally { f.cleanup() }
  t.end()
})

test('Proc commits runnable continuation writes before releasing any output', (t) => {
  const f = fixture().startup()
  try {
    const proc = f.procs[0]
    proc.engine.start = () => {
      proc.log.election(4n, 'b')
      proc.log.appendBatch([{ term: 4n, entry: Buffer.from('first') }])
      proc.engine.output('event', { type: 'test' })
      t.equal(f.replies[0].filter((reply) => reply.outputs).length, 0, 'output remains buffered')
      t.equal(readDatabase(f.paths.procA, (db) => db.prepare('SELECT current_term FROM raft_election').get().current_term),
        0n, 'other connections cannot see intermediate input')
      proc.executor.enqueue(() => proc.log.append({ term: 4n, entry: Buffer.from('continuation') }))
    }
    const response = f.input(0, 'start')
    t.equal(response.type, 'ack')
    const outputs = response.outputs.map(wire.decodePayload)
    t.deepEqual(outputs.map(({ message }) => message.type), ['test', 'change'])
    t.equal(outputs[1].message.status.seq, 1n, 'final change includes the continuation write')
    t.notOk(proc.db.isTransaction, 'completion follows COMMIT')
    const state = readDatabase(f.paths.procA, (db) => ({
      election: db.prepare('SELECT current_term FROM raft_election').get().current_term,
      entries: db.prepare('SELECT entry FROM raft_log ORDER BY seq').all().map((row) => Buffer.from(row.entry).toString()),
    }))
    t.equal(state.election, 4n)
    t.deepEqual(state.entries, ['first', 'continuation'], 'ready work shares one durable input')
  } finally { f.cleanup() }
  t.end()
})

test('Proc failures discard success outputs and rollback uncommitted work on close', (t) => {
  for (const boundary of ['handler', 'continuation', 'timer', 'completion', 'commit before', 'commit after']) {
    const f = fixture().startup()
    try {
      const proc = f.procs[0]
      const original = new Error(`injected ${boundary}`)
      original.stack = `original ${boundary} stack`
      const fail = () => { throw original }
      const change = proc.engine.change.bind(proc.engine)
      let checks = 0
      proc.engine.change = () => {
        checks++
        t.ok(proc.executor.running, 'final comparison runs inside executor')
        t.ok(proc.db.isTransaction, 'final comparison precedes commit')
        change()
        if (boundary === 'completion') { fail() }
      }
      proc.engine.start = () => {
        proc.log.election(5n, 'a')
        proc.log.append({ term: 5n, entry: Buffer.from('pending') })
        proc.engine.output('event', { type: 'test' })
        if (boundary === 'handler') { fail() }
        if (boundary === 'continuation') { proc.executor.enqueue(fail) }
        if (boundary === 'timer') { proc.executor.after(0, fail) }
      }
      if (boundary.startsWith('commit')) {
        const exec = proc.db.exec.bind(proc.db)
        proc.db.exec = (sql) => {
          if (sql === 'COMMIT') {
            if (boundary === 'commit after') { exec(sql) }
            throw original
          }
          return exec(sql)
        }
      }
      const reply = f.input(0, 'start')
      t.equal(reply, undefined, `${boundary}: no completion or fatal ERR is sent`)
      t.equal(checks, ['handler', 'continuation', 'timer'].includes(boundary) ? 0 : 1,
        'only successfully processed inputs reach the final comparison')
      t.equal(f.events[0].find((event) => event.type === 'error').value.stack, original.stack,
        'original trace remains available locally')
      const state = readDatabase(f.paths.procA, (db) => ({
        election: db.prepare('SELECT current_term FROM raft_election').get().current_term,
        entries: db.prepare('SELECT seq FROM raft_log').all().length,
      }))
      t.equal(state.election, boundary === 'commit after' ? 5n : 0n)
      t.equal(state.entries, boundary === 'commit after' ? 1 : 0,
        'only complete old/new state survives')
    } finally { f.cleanup() }
  }
  t.end()
})

test('Proc treats CMD insert failures as fatal for local and forwarded commands', (t) => {
  for (const forwarded of [false, true]) {
    const f = fixture().startup()
    try {
      const proc = f.procs[0]
      const receive = (message) => f.input(0, 'rpc', {
        from: 'b', frame: wire.encodeFrame({ to: 'a', message: { ...message, from: 'b' } }),
      })
      f.input(0, 'start')
      f.input(0, 'tick', { timems: 10_000 })
      const election = receive({ type: 'vote', term: 1n, voteGranted: true })
      const append = election.outputs.map(wire.decodePayload)
        .find(({ to, message }) => to === 'b' && message.type === 'append').message
      const committed = receive({ type: 'ack', term: 1n, cid: append.cid, seq: 0n })
      const execute = committed.outputs.map(wire.decodePayload)
        .find(({ to }) => to === 'apply').message
      f.input(0, 'apply', { reply: {
        type: 'ack', cid: execute.cid,
        receipt: { seq: 0n, entry_hash: execute.entryHash, local_digest: Buffer.alloc(32) },
        outcomes: [],
      } })
      t.equal(proc.engine.appliedSeq, 0n, 'real election no-op and setup completed')
      proc.db.exec(`
        CREATE TRIGGER reject_cmd BEFORE INSERT ON raft_log WHEN NEW.seq = 1
        BEGIN SELECT RAISE(ABORT, 'injected CMD insert'); END
      `)
      const response = forwarded ? receive({
        type: 'fwd_cmd', cid: 'forwarded', term: 1n, items: [Buffer.from('cmd')],
      }) : f.input(0, 'append', { requestId: 'local', items: [Buffer.from('cmd')] })
      t.equal(response, undefined, `${forwarded ? 'forwarded' : 'local'} SQL failure aborts input without a reply`)
      t.notOk(proc.isOpen, 'node cannot keep a half-started command in runtime')
      t.match(f.events[0].find((event) => event.type === 'error').value.message, /injected CMD insert/,
        'failed input reports its diagnostic locally')
      t.equal(readDatabase(f.paths.procA, (db) => db.prepare('SELECT max(seq) AS seq FROM raft_log').get().seq),
        0n, 'durable history remains at completed setup')
    } finally { f.cleanup() }
  }
  t.end()
})

test('Proc commits and reports intentional repair before final completion', (t) => {
  const f = fixture().startup()
  try {
    const proc = f.procs[0]
    const err = new Err.ErrorWithCode('repair required', Err.REPAIR_OUTSIDE_AGREEMENT)
    proc.engine.start = () => {
      proc.db.prepare('UPDATE monsterft_meta SET repair_state = 2').run()
      proc.engine.output('client', { type: 'err', cid: 'caller', ...wire.errorToWire(err, false) })
      proc.engine.fatal(err)
    }
    const send = proc._localSend
    proc._localSend = (to, frame) => {
      t.notOk(proc.log.isOpen, 'owner is closed before final completion is sent')
      t.equal(f.events[0].find((event) => event.type === 'error').value, err,
        'original repair diagnostic precedes send')
      return send(to, frame)
    }
    const reply = f.input(0, 'start')
    t.equal(reply.type, 'ack', 'durable intended transition completes')
    const output = wire.decodePayload(reply.outputs[0])
    t.equal(output.to, 'client', 'buffered command result remains available')
    t.equal(output.message.code, Err.REPAIR_OUTSIDE_AGREEMENT)
    t.equal(wire.decodePayload(reply.outputs.at(-1)).message.type, 'change',
      'intentional repair retains the final change output')
    t.equal(readDatabase(f.paths.procA, (db) => db.prepare('SELECT repair_state FROM monsterft_meta').get().repair_state),
      2n, 'repair fence retained through close')
    t.notOk(proc.isOpen)
    t.notOk(f.events[0].some((event) => event.type === 'fatal'), 'repair does not emit a redundant fatal event')
  } finally { f.cleanup() }
  t.end()
})

test('Proc keeps local diagnostics outside agreed outputs', (t) => {
  const f = fixture().startup()
  try {
    const proc = f.procs[0]
    const err = new Error('local diagnostic')
    err.stack = 'unique Proc stack'
    proc.engine.start = () => proc.engine.warn(err)
    const reply = f.input(0, 'start')
    t.deepEqual(reply.outputs.map((output) => wire.decodePayload(output).message.type), ['change'],
      'only the initial change is published; warnings are never protocol outputs')
    t.equal(f.events[0].find((event) => event.type === 'warn').value.stack, err.stack,
      'local diagnostic retains original trace')
  } finally { f.cleanup() }
  t.end()
})

test('Proc preserves committed repair diagnostics when final completion send fails', async (t) => {
  for (const asynchronous of [false, true]) {
    const f = fixture().startup()
    try {
      const proc = f.procs[0]
      const repair = new Err.ErrorWithCode('repair required', Err.REPAIR_OUTSIDE_AGREEMENT)
      repair.stack = 'original local repair trace'
      proc.engine.start = () => {
        proc.db.prepare('UPDATE monsterft_meta SET repair_state = 2').run()
        proc.engine.fatal(repair)
      }
      let attempted = 0
      proc._localSend = () => {
        attempted++
        t.equal(f.events[0].find((event) => event.type === 'error').value, repair,
          'repair is reported before the failed send')
        t.notOk(proc.log.isOpen, 'Proc stops before trying its final completion')
        const err = new Error('completion send failed')
        if (asynchronous) { return Promise.reject(err) }
        throw err
      }
      t.equal(f.input(0, 'start'), undefined, 'transport failure delivers no reply')
      await delay(0)
      t.equal(attempted, 1, 'only the final completion was attempted')
      const errors = f.events[0].filter((event) => event.type === 'error').map((event) => event.value)
      t.deepEqual(errors, [repair], 'send failure cannot replace the original diagnostic')
      t.equal(errors[0].stack, repair.stack)
      t.equal(readDatabase(f.paths.procA, (db) => db.prepare('SELECT repair_state FROM monsterft_meta').get().repair_state),
        2n, 'failed send leaves the repair fence durable')
    } finally { f.cleanup() }
  }
  t.end()
})

test('Proc reports quorum-impossible repair locally without stopping', (t) => {
  const f = fixture().startup()
  try {
    const proc = f.procs[0]
    const digest = Buffer.alloc(32, 1)
    proc.engine.start = () => {
      proc.db.prepare(`UPDATE monsterft_meta SET applied_seq = 1,
        applied_entry_hash = ?, pending_cmd_seq = 1, applied_local_digest = ?`).run(digest, digest)
      proc.engine.pendingCommand = { cmdSeq: 1n, localDigest: digest }
      proc.engine.applySync({ cmdSeq: 1n, quorum: false, agree: [], disagree: ['a', 'b', 'c'] }, 2n, digest)
    }
    const reply = f.input(0, 'start')
    t.deepEqual(reply.outputs.map((output) => wire.decodePayload(output).message.type), ['sync', 'change'],
      'SYNC and final change remain protocol events; repair error does not')
    t.equal(f.events[0].find((event) => event.type === 'error').value.code, Err.REPAIR_QUORUM_IMPOSSIBLE)
    t.equal(proc.db.prepare('SELECT repair_state FROM monsterft_meta').get().repair_state, 1n)
    t.ok(proc.isOpen, 'quorum-impossible repair remains nonfatal')
  } finally { f.cleanup() }
  t.end()
})

test('Proc independently verifies nested cluster frames before applying them', (t) => {
  const f = fixture().startup()
  try {
    f.input(0, 'start')
    const frame = wire.encodeFrame({ to: 'a', message: {
      type: 'append', from: 'b', cid: 'replication', term: 9n,
      seqP: -1n, termP: -1n, commitSeq: -1n,
      data: [{ term: 9n, entry: Buffer.from('must not write') }],
    } })
    frame[0] ^= 1
    const reply = f.input(0, 'rpc', { from: 'b', frame })
    t.equal(reply, undefined, 'corrupt frame sends no fatal ERR reply')
    t.equal(f.events[0].find((event) => event.type === 'error').value.code, Err.FRAME_CORRUPT)
    t.equal(readDatabase(f.paths.procA, (db) => db.prepare('SELECT current_term FROM raft_election').get().current_term),
      0n, 'corrupt cluster frame does not change election')
    t.equal(readDatabase(f.paths.procA, (db) => db.prepare('SELECT seq FROM raft_log').all().length),
      0, 'corrupt cluster frame does not change log')
  } finally { f.cleanup() }
  t.end()
})

test('Proc diagnostic listeners cannot hide later listeners or prevent teardown', (t) => {
  const f = fixture().startup()
  try {
    const proc = f.procs[0]
    let warned = 0
    let errored = 0
    proc.on('warn', () => { throw new Error('warning listener failed') })
    proc.on('warn', () => { warned++ })
    proc.on('error', () => { throw new Error('error listener failed') })
    proc.on('error', () => { errored++ })
    proc.engine.start = () => proc.engine.warn(new Error('original warning'))
    t.equal(f.input(0, 'start').type, 'ack')
    t.equal(warned, 1, 'later warning listener still runs')
    t.equal(errored, 1, 'later error listener still runs')
    proc.engine.receive = () => { throw new Error('fatal work') }
    const frame = wire.encodeFrame({ to: 'a', message: { from: 'b', type: 'test' } })
    t.equal(f.input(0, 'rpc', { from: 'b', frame }), undefined)
    t.equal(errored, 2, 'fatal error reaches remaining listeners')
    t.notOk(proc.log.isOpen, 'throwing error listener cannot prevent closing DB')
  } finally { f.cleanup() }
  t.end()
})

test('Proc close failure reports an error and never confirms completed teardown', (t) => {
  const f = fixture().startup()
  const proc = f.procs[0]
  const close = proc.store.close.bind(proc.store)
  try {
    proc.store.close = () => { throw new Error('database close failed') }
    const failed = f.request(0, { type: 'close' })
    t.equal(failed.type, 'err', 'cannot ACK an unfinished close')
    t.match(failed.message, /database close failed/)
    t.ok(proc.log.isOpen, 'failed close remains visible')
    proc.store.close = close
    t.equal(f.request(0, { type: 'close' }).type, 'ack', 'later close can finish teardown')
    t.notOk(proc.log.isOpen)
  } finally { proc.store.close = close; f.cleanup() }
  t.end()
})

test('Proc rejects input reordering and clock regressions', (t) => {
  for (const bad of [{ tickCount: 1n, timems: 5 }, { tickCount: 2n, timems: 4 }]) {
    const f = fixture().startup()
    try {
      f.input(0, 'start', { timems: 5 })
      const reply = f.input(0, 'tick', bad)
      t.equal(reply, undefined, 'bad input sends no fatal ERR reply')
      t.equal(f.events[0].find((event) => event.type === 'error').value.code, Err.RPC_ILLEGAL)
      t.notOk(f.procs[0].isOpen)
    } finally { f.cleanup() }
  }
  t.end()
})

test('Proc watchdog is inactive during startup and only closes on Gate silence', async (t) => {
  const f = fixture({ localTimeout: 80, electionTimeout: 10 }).startup()
  try {
    await delay(100)
    t.ok(f.procs.every((proc) => proc.log.isOpen), 'startup wait does not arm watchdog')
    f.input(0, 'start')
    await delay(40)
    f.input(0, 'tick', { timems: 0 })
    await delay(50)
    t.ok(f.procs[0].isOpen, 'valid Gate input rearms native watchdog')
    t.equal(f.procs[0].engine.term, 0n, 'native elapsed time does not drive protocol election')
    await delay(60)
    t.notOk(f.procs[0].isOpen)
    t.equal(f.events[0].find((event) => event.type === 'error').value.code, Err.LOCAL_TIMEOUT)
    t.equal(f.procs[0].executor.timems, 0, 'watchdog does not advance supplied time')
  } finally { f.cleanup() }
  t.end()
})
