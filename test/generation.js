import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { MonsterProc } from '../src/proc.js'
import { MonsterApply } from '../src/apply.js'
import { databasePaths, readDatabase, readState, stateDigest } from '../src/storage.js'
import { readApply, readMeta } from '../src/utils.js'
import { encodeFrame, decodeFrame, decodePayload } from '../src/wire.js'
import * as Err from '../src/error.js'

const roles = ['procA', 'procB', 'procC']
const allRoles = [...roles, 'apply']
const originalSeed = Buffer.alloc(16, 1)
const replacementSeed = Buffer.alloc(16, 2)
const schema = 'CREATE TABLE items (value TEXT NOT NULL)'

function fixture(callback = null) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-generation-'))
  const base = path.join(directory, 'node')
  const paths = databasePaths(base)
  const groups = []
  const calls = []
  return {
    base, paths, calls,
    group(seed) {
      const components = {}
      const errors = Object.fromEntries(allRoles.map((role) => [role, []]))
      const replies = Object.fromEntries(allRoles.map((role) => [role, []]))
      const send = (role) => (to, frame) => {
        if (to !== 'gate') { throw new Error('unexpected local destination') }
        replies[role].push(decodeFrame(frame))
      }
      for (const role of roles) {
        components[role] = new MonsterProc(role, 'a', ['a', 'b', 'c'], send(role), base, {
          electionTimeout: 100, localTimeout: 60_000,
        })
      }
      components.apply = new MonsterApply('a', send('apply'), base, {
        apply(db, data, term, seq, index, matchIndex) {
          calls.push({ seq, data, seed })
          if (callback) { return callback(db, data, term, seq, index, matchIndex) }
          if (seq === 0n) { db.exec(schema); return null }
          db.prepare('INSERT INTO items VALUES (?)').run(data.toString())
          return data.toString()
        },
      })
      for (const role of allRoles) {
        components[role].on('error', (err) => errors[role].push(err))
      }
      let cid = 0
      const group = {
        components, errors, replies, seed,
        async request(role, message) {
          message = { cid: `request-${++cid}`, ...message }
          await components[role].onReceive('gate', encodeFrame(message))
          return replies[role].find((reply) => reply.cid === message.cid)
        },
        async ack(role, message) {
          const reply = await this.request(role, message)
          if (reply?.type !== 'ack') {
            throw new Error(`${role} ${message.type}: ${errors[role].at(-1)?.message ?? 'missing ACK'}`)
          }
          return reply
        },
        async hello() {
          for (const role of allRoles) { await this.ack(role, { type: 'hello', seed }) }
        },
        async open() {
          await this.hello()
          for (const role of allRoles) { await this.ack(role, { type: 'open' }) }
        },
        async phase(type) {
          const results = []
          for (const role of roles) { results.push(await this.ack(role, { type })) }
          return results
        },
        input(role, kind, fields = {}) {
          const proc = components[role]
          return this.request(role, {
            type: 'input', kind, tickCount: proc.tickCount + 1n,
            timems: proc.executor.timems, ...fields,
          })
        },
        async inputAll(kind, fields = {}) {
          const results = []
          for (const role of roles) {
            const reply = await this.input(role, kind, fields)
            if (reply?.type !== 'ack') {
              throw new Error(`${role} ${kind}: ${errors[role].at(-1)?.message ?? 'missing ACK'}`)
            }
            results.push(reply)
          }
          return results
        },
        rpcAll(message) {
          return this.inputAll('rpc', {
            from: 'b', frame: encodeFrame({ to: 'a', message: { ...message, from: 'b' } }),
          })
        },
        async finish() {
          for (const phase of ['capture', 'adopt', 'reconcile']) { await this.phase(phase) }
          await this.ack('apply', { type: 'ready' })
          await this.inputAll('start')
        },
        async elect() {
          await this.inputAll('tick', { timems: 10_000 })
          const election = await this.rpcAll({ type: 'vote', term: 1n, voteGranted: true })
          const append = output(election[0], 'b', 'append')
          const committed = await this.rpcAll({ type: 'ack', term: 1n, cid: append.cid, seq: 0n })
          const execute = output(committed[0], 'apply', 'execute')
          const reply = await this.ack('apply', execute)
          await this.inputAll('apply', { reply })
        },
        async commitCommand(value) {
          const appended = await this.inputAll('append', {
            requestId: 'public-command', items: [Buffer.from(value)],
          })
          const append = output(appended[0], 'b', 'append')
          const committed = await this.rpcAll({ type: 'ack', term: 1n, cid: append.cid, seq: 1n })
          return output(committed[0], 'apply', 'execute')
        },
      }
      groups.push(group)
      return group
    },
    snapshot() {
      return roles.map((role) => readDatabase(paths[role], (db) => stateDigest(readState(db))))
    },
    async close() {
      for (const group of groups) {
        await Promise.all(allRoles.map(async (role) => group.components[role].close()))
      }
      fs.rmSync(directory, { recursive: true, force: true })
    },
  }
}

function output(reply, to, type) {
  const result = reply.outputs.map(decodePayload)
    .find((item) => item.to === to && item.message.type === type)
  if (!result) { throw new Error(`missing ${to} ${type} output`) }
  return result.message
}

function fenced(t, group, role, reply, label) {
  t.equal(reply, undefined, `${label}: stale owner sends no completion`)
  t.equal(group.errors[role].at(-1)?.code, Err.GENERATION_DIFF,
    `${label}: different generation is reported locally`)
  t.notOk(group.components[role].isOpen, `${label}: stale owner stops`)
}

test('new generation fences old voting, timer, command, and Apply execution paths', async (t) => {
  const f = fixture()
  try {
    const old = f.group(originalSeed)
    await old.open()
    await old.finish()
    await old.elect()
    t.ok(roles.every((role) => old.components[role].engine.state === 'leader'),
      'ordinary vote responses elect the three matching engines')
    const before = f.snapshot()
    const next = f.group(replacementSeed)
    await next.open()
    t.deepEqual(f.snapshot(), before, 'claiming a generation does not alter protocol state digests')
    fenced(t, old, 'procA', await old.input('procA', 'rpc', {
      from: 'b', frame: encodeFrame({ to: 'a', message: {
        type: 'vote_request', from: 'b', term: 20n, termP: 1n, seqP: 0n,
      } }),
    }), 'higher-term vote')
    fenced(t, old, 'procB', await old.input('procB', 'tick', { timems: 20_000 }), 'timer input')
    fenced(t, old, 'procC', await old.input('procC', 'append', {
      requestId: 'stale-command', items: [Buffer.from('must not append')],
    }), 'local command')
    const callCount = f.calls.length
    fenced(t, old, 'apply', await old.request('apply', {
      type: 'execute', tickCount: 100n, term: 1n, seq: 1n,
      entryHash: Buffer.alloc(32, 8), items: [Buffer.from('must not execute')],
      matchIndex: [0n, 0n, 0n],
    }), 'Apply command')
    t.equal(f.calls.length, callCount, 'stale Apply cannot enter the user callback')
    t.deepEqual(f.snapshot(), before, 'stale inputs cannot change term, log, or checkpoint')
    t.equal(readDatabase(f.paths.apply, readApply).seq, 0n, 'stale Apply cannot change its receipt')
    await next.finish()
    t.ok(roles.every((role) => next.components[role].isOpen), 'replacement completes reconciliation')
    t.ok(allRoles.every((role) => next.errors[role].length === 0), 'replacement remains healthy')
  } finally { await f.close() }
  t.end()
})

for (const phase of ['capture', 'adopt', 'reconcile']) {
  test(`new generation fences a stale startup at ${phase}`, async (t) => {
    const f = fixture()
    try {
      const old = f.group(originalSeed)
      await old.open()
      if (phase !== 'capture') { await old.phase('capture') }
      if (phase === 'reconcile') { await old.phase('adopt') }
      const next = f.group(replacementSeed)
      await next.open()
      const before = f.snapshot()
      for (const role of roles) {
        fenced(t, old, role, await old.request(role, { type: phase }), phase)
      }
      t.deepEqual(f.snapshot(), before, 'stale startup changes no protocol state')
      await next.finish()
      t.ok(roles.every((role) => next.components[role].isOpen), 'replacement startup succeeds')
    } finally { await f.close() }
    t.end()
  })
}

for (const claimed of ['procA', 'apply']) {
  test(`capture rejects a family with only ${claimed} claimed by another generation`, async (t) => {
    const f = fixture()
    try {
      const old = f.group(originalSeed)
      await old.open()
      const next = f.group(replacementSeed)
      await next.hello()
      await next.ack(claimed, { type: 'open' })
      const before = f.snapshot()
      fenced(t, old, 'procB', await old.request('procB', { type: 'capture' }),
        'peer generation different')
      if (claimed === 'procA') {
        fenced(t, next, 'procA', await next.request('procA', { type: 'capture' }),
          'claimant has not claimed the complete family')
      }
      t.deepEqual(f.snapshot(), before, 'identical logical heads cannot hide mixed generations')
    } finally { await f.close() }
    t.end()
  })
}

test('outlier adoption rejects a donor claimed after capture', async (t) => {
  const f = fixture()
  try {
    const old = f.group(originalSeed)
    await old.open()
    await old.finish()
    await old.elect()
    t.equal((await old.input('procC', 'append', {
      requestId: 'partial-input', items: [Buffer.from('uncommitted outlier')],
    }))?.type, 'ack', 'one Proc commits the interrupted input')
    const next = f.group(replacementSeed)
    await next.open()
    const captured = await next.phase('capture')
    t.equal(captured[2].selection.donor, 'procA', 'outlier selects the matching majority donor')
    const before = f.snapshot()
    t.notOk(before[0].equals(before[2]), 'outlier still needs to copy protocol state')
    const newer = f.group(Buffer.alloc(16, 3))
    await newer.hello()
    await newer.ack('procA', { type: 'open' })
    fenced(t, next, 'procC', await next.request('procC', { type: 'adopt' }),
      'changed donor generation')
    t.deepEqual(f.snapshot(), before, 'failed adoption copies no donor contents')
  } finally { await f.close() }
  t.end()
})

test('an active Apply transaction blocks generation takeover until it completes', async (t) => {
  const entered = Promise.withResolvers()
  const release = Promise.withResolvers()
  const f = fixture(async (db, data, term, seq) => {
    if (seq === 0n) { db.exec(schema); return null }
    db.prepare('INSERT INTO items VALUES (?)').run(data.toString())
    entered.resolve()
    await release.promise
    return data.toString()
  })
  let work
  try {
    const old = f.group(originalSeed)
    await old.open()
    await old.finish()
    await old.elect()
    const execute = await old.commitCommand('commit before takeover')
    work = old.request('apply', execute)
    await entered.promise
    const blocked = f.group(replacementSeed)
    await blocked.hello()
    // Match Gate's startup order: DB1 claims can finish before DB2 is busy.
    for (const role of roles) { await blocked.ack(role, { type: 'open' }) }
    t.equal(await blocked.request('apply', { type: 'open' }), undefined, 'busy takeover sends no ACK')
    t.equal(blocked.errors.apply.at(-1)?.code, Err.SQLITE_ERROR)
    t.equal(blocked.errors.apply.at(-1)?.sqlCode, 5, 'SQLite writer lock prevents mid-callback takeover')
    t.ok(old.components.apply.isOpen, 'original Apply remains available')
    t.equal(readDatabase(f.paths.apply, readApply).seq, 0n, 'uncommitted work remains invisible')
    release.resolve()
    const reply = await work
    t.equal(reply?.type, 'ack', 'original Apply can finish its transaction')
    t.equal(reply?.receipt.seq, 1n)
    const next = f.group(Buffer.alloc(16, 3))
    await next.open()
    await next.finish()
    t.equal(f.calls.filter((call) => call.seq === 1n).length, 1,
      'replacement imports the committed receipt without repeating work')
    t.ok(roles.every((role) => readMeta(next.components[role].db).applied_seq === 1n))
    t.deepEqual(readDatabase(f.paths.apply, (db) =>
      db.prepare('SELECT value FROM items').all().map((row) => row.value)), ['commit before takeover'])
  } finally {
    release.resolve()
    await work
    await f.close()
  }
  t.end()
})

for (const imports of [0, 1, 2, 3]) {
  test(`generation takeover recovers an Apply receipt after ${imports} Proc imports`, async (t) => {
    const f = fixture()
    try {
      const old = f.group(originalSeed)
      await old.open()
      await old.finish()
      await old.elect()
      const execute = await old.commitCommand('committed once')
      const reply = await old.ack('apply', execute)
      for (const role of roles.slice(0, imports)) {
        t.equal((await old.input(role, 'apply', { reply }))?.type, 'ack')
      }
      t.equal(roles.filter((role) => readMeta(old.components[role].db).applied_seq === 1n).length,
        imports, 'only the selected imports reached DB1')
      const next = f.group(replacementSeed)
      await next.open()
      await next.finish()
      const before = f.snapshot()
      fenced(t, old, 'procC', await old.input('procC', 'apply', { reply }), 'late receipt import')
      t.deepEqual(f.snapshot(), before, 'late old-generation import cannot disturb replacement state')
      t.ok(roles.every((role) => readMeta(next.components[role].db).applied_seq === 1n),
        'all replacement Procs retain the completed receipt')
      t.ok(roles.every((role) => readMeta(next.components[role].db).pending_cmd_seq === 1n),
        'receipt recovery preserves pending agreement')
      t.equal(f.calls.filter((call) => call.seq === 0n).length, 1, 'setup is never repeated')
      t.equal(f.calls.filter((call) => call.seq === 1n).length, 1, 'committed command is never repeated')
      t.deepEqual(readDatabase(f.paths.apply, (db) =>
        db.prepare('SELECT value FROM items').all().map((row) => row.value)), ['committed once'])
      t.ok(allRoles.every((role) => next.errors[role].length === 0), 'replacement has no diagnostic failures')
    } finally { await f.close() }
    t.end()
  })
}
