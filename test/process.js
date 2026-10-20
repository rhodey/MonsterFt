import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { fork } from 'node:child_process'
import { DatabaseSync } from 'node:sqlite'
import { Gate } from '../src/gate.js'
import { MonsterApply } from '../src/apply.js'
import { decodeFrame } from '../src/wire.js'
import { readMeta, readApply } from '../src/utils.js'
import { readState, stateDigest } from '../src/storage.js'
import * as Err from '../src/error.js'
import { until, roles } from './helper.js'

const read = (file, callback) => {
  const db = new DatabaseSync(file, { readOnly: true, readBigInts: true })
  try { return callback(db) } finally { db.close() }
}

function bounded(work, name, milliseconds = 15_000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name} timeout`)), milliseconds)
    work.then((value) => { clearTimeout(timer); resolve(value) },
      (err) => { clearTimeout(timer); reject(err) })
  })
}

test('runs nine independent protocol processes against shared files with delayed hello readiness', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monster-process-'))
  const ids = ['a', 'b', 'c']
  const members = new Map()
  const children = []
  const errors = []
  const warnings = []
  const calls = []
  const helloCounts = new Map()
  const opts = {
    electionTimeout: 600, appendTimeout: 3_000,
    tickInterval: 10, localTimeout: 1_000, applyTimeout: 5_000,
  }
  let stopped = false
  try {
    for (const id of ids) {
      const base = path.join(directory, id)
      const node = { base, children: new Map(), gate: null, apply: null }
      const localSend = (to, frame) => {
        if (to === 'apply') { return node.apply.onReceive('gate', frame) }
        const msg = decodeFrame(frame)
        if (msg.type === 'hello') {
          const key = `${id}/${to}`
          helloCounts.set(key, (helloCounts.get(key) ?? 0) + 1)
        }
        node.children.get(to).send({ type: 'frame', frame })
      }
      node.gate = new Gate(id, ids, (to, frame) => {
        if (!stopped) { members.get(to).gate.onReceive(id, frame) }
      }, localSend, opts)
      node.apply = new MonsterApply(id, (to, frame) => node.gate.onReceive('apply', frame), base, {
        async apply(db, data, term, seq) {
          calls.push({ id, seq, data })
          if (seq === 0n) {
            db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB NOT NULL) STRICT')
            return null
          }
          await Promise.resolve()
          if (data.toString() === 'reject') {
            const err = new Error('agreed rejection')
            err.stack = `Apply trace from cluster node ${id}`
            throw err
          }
          db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
          return data.toString()
        },
      })
      node.gate.on('error', (error) => errors.push({ id, role: 'gate', error }))
      node.gate.on('warn', (error) => warnings.push({ id, error }))
      node.apply.on('error', (error) => errors.push({ id, role: 'apply', error }))
      node.apply.on('warn', (error) => warnings.push({ id, role: 'apply', error }))
      for (const role of roles) {
        const child = fork(new URL('./fixtures/proc-child.js', import.meta.url), [], {
          serialization: 'advanced', stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
        })
        const record = { child, stderr: '', exited: null }
        record.exited = new Promise((resolve) => child.once('exit', (code, signal) => resolve({ code, signal })))
        child.stderr.on('data', (data) => { record.stderr += data.toString() })
        child.on('error', (error) => errors.push({ id, role, error }))
        child.on('message', (msg) => {
          if (msg.type === 'frame') {
            if (!Buffer.isBuffer(msg.frame)) { throw new Error('IPC did not preserve Buffer') }
            node.gate.onReceive(role, msg.frame)
          } else if (msg.type === 'error') {
            errors.push({ id, role, error: msg.error })
          }
        })
        node.children.set(role, child)
        children.push(record)
        child.send({
          type: 'boot', id, role, nodes: ids, databasePath: base, opts,
          delay: role === 'procC' ? 1_600 : role === 'procB' ? 100 : 0,
        })
      }
      members.set(id, node)
    }
    await bounded(Promise.all([...members.values()].map((node) => node.gate.open())), 'startup')
    t.equal(children.length, 9)
    t.ok([...helloCounts].filter(([key]) => key.endsWith('/procC')).every(([, count]) => count >= 2),
      'late processes join through fresh hello retries')
    const noLeader = await members.get('a').gate.append(Buffer.from('too early'))
      .then(() => null, (error) => error)
    t.equal(noLeader.code, Err.NO_LEADER,
      'independent Proc error stacks do not disrupt output agreement')
    const leader = await until(() => [...members.values()].find(({ gate }) =>
      gate.state === 'leader' && gate.commitTerm === gate.term)?.gate, 'process cluster leader', 10_000)
    await until(() => calls.filter((call) => call.seq === 0n).length === 3, 'setup on each node')
    const follower = [...members.values()].find((node) => node.gate.id !== leader.id).gate
    t.equal((await bounded(leader.append(Buffer.from('first')), 'local append'))[1], 'first')
    t.equal((await bounded(follower.append(Buffer.from('second')), 'forwarded append'))[1], 'second')
    const rejection = await bounded(follower.append(Buffer.from('reject')).then(() => null, (error) => error),
      'agreed rejection')
    t.equal(rejection.message, 'agreed rejection')
    t.equal(rejection.stack, `Apply trace from cluster node ${leader.id}`)
    await until(() => [...members.values()].every(({ base }) => {
      const receipt = read(`${base}2`, readApply)
      return receipt.seq > 0n && roles.every((role) => {
        const meta = read(`${base}1${role.at(-1).toLowerCase()}`, readMeta)
        return meta.pending_cmd_seq === null && meta.applied_seq > receipt.seq
      })
    }), 'completed SYNC across separate processes')
    await until(() => warnings.filter(({ error }) => error.code === Err.APPLY_ERROR).length === 3,
      'one local warning from each Apply')
    t.ok(warnings.filter(({ error }) => error.code === Err.APPLY_ERROR)
      .every(({ role }) => role === 'apply'), 'Gate does not mirror Apply warnings')
    await bounded(Promise.all([...members.values()].map((node) => node.gate.close())), 'component shutdown')
    for (const { child } of children) { child.send({ type: 'exit' }) }
    const exits = await bounded(Promise.all(children.map((record) => record.exited)), 'OS process exit')
    t.ok(exits.every(({ code, signal }) => code === 0 && signal === null), 'all nine OS processes exit normally')
    for (const { base, gate } of members.values()) {
      const states = roles.map((role) => read(`${base}1${role.at(-1).toLowerCase()}`, (db) => stateDigest(readState(db))))
      t.ok(states.every((state) => state.equals(states[0])), `${gate.id} protocol files agree after shutdown`)
      t.deepEqual(read(`${base}2`, (db) => db.prepare('SELECT value FROM items ORDER BY id').all()
        .map((row) => Buffer.from(row.value).toString())), ['first', 'second'])
      t.equal(calls.filter((call) => call.id === gate.id).length, 4,
        'one setup and three command executions per node, not per process')
    }
    t.deepEqual(errors, [])
    t.ok(children.every(({ stderr }) => stderr === ''), 'children report no unhandled failures')
  } finally {
    stopped = true
    await Promise.all([...members.values()].map((node) => node.gate.close().catch(() => {})))
    await Promise.all([...members.values()].map((node) => node.apply.close().catch(() => {})))
    for (const { child } of children) {
      if (child.exitCode === null && child.signalCode === null) {
        if (child.connected) { child.send({ type: 'exit' }) }
        else { child.kill('SIGTERM') }
      }
    }
    await Promise.all(children.map(async ({ child, exited }) => {
      try { await bounded(exited, 'child cleanup', 3_000) }
      catch { child.kill('SIGKILL'); await exited }
    }))
    fs.rmSync(directory, { recursive: true, force: true })
  }
  t.end()
})
