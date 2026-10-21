import test from 'tape'
import fs from 'node:fs'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { Supervisor } from '../src/supervisor.js'
import { Gate } from '../src/gate.js'
import { udsServer } from '../src/uds.js'
import { decodeFrame, decodePayload } from '../src/wire.js'
import { readApply, readMeta } from '../src/utils.js'
import * as Err from '../src/error.js'
import { roles, until, pause } from './helper.js'

const owner = new URL('./fixtures/supervisor-owner.js', import.meta.url)
const allRoles = [...roles, 'apply']
const read = (file, callback) => {
  const db = new DatabaseSync(file, { readOnly: true, readBigInts: true })
  try { return callback(db) } finally { db.close() }
}
const closeServer = (server) => new Promise((resolve, reject) =>
  server.close((err) => err ? reject(err) : resolve()))

async function processCluster(options = {}) {
  const directory = fs.mkdtempSync('/tmp/mf-supervisor-')
  const ids = ['a', 'b', 'c']
  const supervisors = new Map()
  const runs = new Map(ids.map((id) => [id, []]))
  const errors = []
  let stopped = false
  for (const id of ids) {
    const base = path.join(directory, id)
    const supervisor = new Supervisor(async () => {
      const previous = runs.get(id).at(-1)
      if (previous && ![...previous.children.values()].every((child) =>
        child.exitCode !== null || child.signalCode !== null)) {
        throw new Error('replacement started before owners exited')
      }
      const attempt = fs.mkdtempSync(path.join(directory, `${id}-`))
      const sockets = new Map()
      const servers = []
      const ready = []
      const run = { base, sockets, attempt, children: null, gate: null, cleaned: false }
      let gate
      const cleanup = async () => {
        for (const connection of sockets.values()) { connection.destroy() }
        await Promise.all(servers.map(closeServer))
        fs.rmSync(attempt, { recursive: true, force: true })
        run.cleaned = true
      }
      try {
        for (const role of allRoles) {
          ready.push(new Promise((resolve) => {
            run[`ready${role}`] = resolve
          }))
          const server = await udsServer(path.join(attempt, role), (connection) => {
            if (sockets.has(role)) { throw new Error('duplicate owner connection') }
            sockets.set(role, connection)
            connection.on('data', (frame) => {
              if (options.receive?.({ id, role, frame, run }) === false) { return }
              gate.onReceive(role, frame)
            })
            run[`ready${role}`]()
          }, (error) => errors.push({ id, role: 'transport', error }))
          servers.push(server)
        }
        gate = new Gate(id, ids, (to, frame) => {
          if (!stopped) { supervisors.get(to)?.gate?.onReceive(id, frame) }
        }, (to, frame) => sockets.get(to).write(frame), {
          tickInterval: 5, localTimeout: 1_500, applyTimeout: 400,
        })
        gate.on('error', (error) => errors.push({ id, role: 'gate', error }))
        run.gate = gate
        runs.get(id).push(run)
        return {
          gate,
          children: Object.fromEntries(allRoles.map((role) => [role, {
            file: owner, args: [role, id, base, path.join(attempt, role)],
          }])),
          ready: async (children) => {
            run.children = children
            await Promise.all(ready)
          },
          cleanup,
        }
      } catch (err) {
        await cleanup()
        throw err
      }
    }, { restartDelay: 10, startupTimeout: 5_000 })
    supervisor.on('error', (error) => errors.push({ id, role: 'supervisor', error }))
    supervisors.set(id, supervisor)
  }
  const gates = () => [...supervisors.values()].map((supervisor) => supervisor.gate).filter(Boolean)
  const leader = () => gates().find((gate) =>
    gate.isOpen && gate.state === 'leader' && gate.commitTerm === gate.term)
  const close = async () => {
    stopped = true
    await Promise.all([...supervisors.values()].map((supervisor) => supervisor.close()))
    fs.rmSync(directory, { recursive: true, force: true })
  }
  const open = async () => {
    await Promise.all([...supervisors.values()].map((supervisor) => supervisor.open()))
    await until(leader, 'supervised cluster leader', 10_000)
    await until(() => ids.every((id) => read(`${path.join(directory, id)}2`, readApply).seq >= 0n),
      'setup on each supervised Apply')
  }
  const settled = async () => {
    await until(() => ids.every((id) => {
      const gate = supervisors.get(id).gate
      if (!gate?.isOpen) { return false }
      const base = path.join(directory, id)
      const receipt = read(`${base}2`, readApply)
      return roles.every((role) => {
        const meta = read(`${base}1${role.at(-1).toLowerCase()}`, readMeta)
        return meta.pending_cmd_seq === null && meta.applied_seq >= receipt.seq
      })
    }), 'supervised protocol settlement', 10_000)
  }
  return { directory, supervisors, runs, errors, ids, gates, leader, open, close, settled }
}

test('Supervisor restarts four owners and imports a committed Apply receipt without replay', async (t) => {
  let target = null
  let held = null
  const c = await processCluster({
    receive({ id, role, frame, run }) {
      if (id !== target || role !== 'apply') { return }
      const message = decodeFrame(frame)
      if (message.type === 'ack' && message.receipt?.seq > 0n) {
        held = { run, receipt: message.receipt }
        return false
      }
    },
  })
  try {
    await c.open()
    const processes = [...c.runs.values()].flatMap((runs) => [...runs[0].children.values()])
    t.equal(new Set(processes.map((child) => child.pid)).size, 12, 'each owner has its own process')
    t.ok(processes.every((child) => child.pid !== process.pid))
    target = c.ids.find((id) => id !== c.leader().id)
    const supervisor = c.supervisors.get(target)
    const base = path.join(c.directory, target)
    t.equal((await c.leader().append(Buffer.from('committed once')))[1], 'committed once')
    await until(() => held, 'Apply receipt held before DB1 import')
    const receipt = read(`${base}2`, readApply)
    t.equal(receipt.seq, held.receipt.seq)
    t.ok(roles.every((role) =>
      read(`${base}1${role.at(-1).toLowerCase()}`, readMeta).applied_seq < receipt.seq),
    'all DB1 copies still precede the completed Apply receipt')
    const previous = held.run
    target = null
    previous.children.get('procB').kill('SIGKILL')
    await until(() => supervisor.gate?.isOpen && supervisor.gate !== previous.gate,
      'replacement group', 10_000)
    await c.settled()
    t.ok([...previous.children.values()].every((child) =>
      child.exitCode !== null || child.signalCode !== null), 'old owners are all gone')
    t.ok(previous.cleaned, 'old sockets are discarded before replacement')
    const calls = fs.readFileSync(`${base}.calls`, 'utf8').trim().split('\n')
    t.equal(calls.filter((seq) => seq === receipt.seq.toString()).length, 1,
      'restart does not repeat committed Apply work')
    t.equal(calls.filter((seq) => seq === '0').length, 1, 'setup is not rerun')
    t.deepEqual(read(`${base}2`, (db) => db.prepare('SELECT value FROM items ORDER BY id').all()
      .map((row) => Buffer.from(row.value).toString())), ['committed once'])
    await until(() => c.leader(), 'leader after replacement')
    const continued = await supervisor.gate.append(Buffer.from('continued'))
    t.equal(continued[1], 'continued',
      'forwarded command succeeds after recovery')
    await until(() => read(`${base}2`, readApply).seq >= continued[0],
      'continued command receipt')
    await c.settled()
    const second = c.runs.get(supervisor.gate.id).at(-1)
    second.sockets.get('procB').destroy()
    await until(() => supervisor.gate?.isOpen && supervisor.gate !== second.gate,
      'replacement after local transport disconnection', 10_000)
    await c.settled()
    t.ok([...second.children.values()].every((child) =>
      child.exitCode !== null || child.signalCode !== null),
    'local socket loss replaces and reaps the entire previous group')
    t.equal(c.runs.get(supervisor.gate.id).length, 3,
      'the same Supervisor handles repeated recovery')
    const afterDisconnect = fs.readFileSync(`${base}.calls`, 'utf8').trim().split('\n')
    t.equal(afterDisconnect.filter((seq) => seq === continued[0].toString()).length, 1,
      'transport disconnection does not replay completed work')
    t.equal(afterDisconnect.filter((seq) => seq === receipt.seq.toString()).length, 1)
    t.notOk(c.errors.some(({ error }) =>
      [Err.MONSTER_CORRUPT, Err.LOCAL_DIVERGENCE].includes(error.code)))
  } finally { await c.close() }
  t.end()
})

test('Supervisor kills hung Apply, rolls back its transaction, and resumes committed protocol work', async (t) => {
  const c = await processCluster()
  try {
    await c.open()
    const target = c.ids.find((id) => id !== c.leader().id)
    const supervisor = c.supervisors.get(target)
    const before = supervisor.gate
    const original = c.runs.get(target)[0]
    const base = path.join(c.directory, target)
    fs.writeFileSync(`${base}.hang`, '')
    const result = await c.leader().append(Buffer.from('hang'))
    t.equal(result[1], 'hang',
      'other cluster nodes can agree while the follower Apply is hung')
    await until(() => fs.existsSync(`${base}.blocked`), 'hung Apply callback')
    t.deepEqual(read(`${base}2`, (db) => db.prepare('SELECT value FROM items').all()), [],
      'unfinished Apply transaction is not externally visible')
    await until(() => supervisor.gate?.isOpen && supervisor.gate !== before,
      'Apply-timeout replacement', 10_000)
    await until(() => read(`${base}2`, readApply).seq >= result[0],
      'retried Apply receipt', 10_000)
    await c.settled()
    t.equal(original.children.get('apply').signalCode, 'SIGKILL',
      'hung callback cannot prevent owner termination')
    t.ok(c.errors.some(({ id, role, error }) =>
      id === target && role === 'gate' && error.code === Err.APPLY_TIMEOUT))
    t.deepEqual(read(`${base}2`, (db) => db.prepare('SELECT value FROM items ORDER BY id').all()
      .map((row) => Buffer.from(row.value).toString())), ['hang'],
    'uncommitted first execution rolls back; recovery commits the command once')
    const receipt = read(`${base}2`, readApply)
    const calls = fs.readFileSync(`${base}.calls`, 'utf8').trim().split('\n')
    t.equal(calls.filter((seq) => seq === receipt.seq.toString()).length, 2,
      'only unfinished work is executed again')
    t.equal(calls.filter((seq) => seq === '0').length, 1)
    await pause(20)
    t.equal(c.runs.get(target).length, 2, 'replacement remains stable')
    t.equal((await supervisor.gate.append(Buffer.from('after hang')))[1], 'after hang')
  } finally { await c.close() }
  t.end()
})

test('supervised owners deliver their final repair completion before process shutdown', async (t) => {
  let target = null
  const completions = []
  const c = await processCluster({
    receive({ id, role, frame }) {
      if (id !== target || !roles.includes(role)) { return }
      const message = decodeFrame(frame)
      if (message.type === 'ack' && Array.isArray(message.outputs)) {
        completions.push({ role, message })
      }
    },
  })
  try {
    await c.open()
    target = c.ids.find((id) => id !== c.leader().id)
    const supervisor = c.supervisors.get(target)
    const run = c.runs.get(target)[0]
    const base = path.join(c.directory, target)
    const syncs = []
    run.gate.on('sync', (message) => syncs.push(message))
    fs.writeFileSync(`${base}.reject`, '')
    const result = await c.leader().append(Buffer.from('repair'))
    t.equal(result[1], 'repair', 'the healthy quorum still returns its accepted result')
    const diagnostics = () => fs.existsSync(`${base}.diagnostics`)
      ? fs.readFileSync(`${base}.diagnostics`, 'utf8').trim().split('\n').map(JSON.parse)
      : []
    await until(() => syncs.some((sync) => sync.cmdSeq === result[0]) &&
      diagnostics().filter(({ role, code }) =>
        roles.includes(role) && code === Err.REPAIR_OUTSIDE_AGREEMENT).length === 3,
    'final repair SYNC and all local diagnostics')
    const final = completions.find(({ role, message }) => role === 'procA' &&
      message.outputs.some((output) => {
        const envelope = decodePayload(output)
        return envelope.to === 'event' && envelope.message.type === 'sync' &&
          envelope.message.cmdSeq === result[0]
      }))
    t.ok(final, 'ProcA sends the SYNC output despite closing its database')
    await until(() => roles.every((role) => completions.some((completion) =>
      completion.role === role && completion.message.cid === final.message.cid)),
    'all final repair completions')
    const local = diagnostics()
    t.deepEqual(local.filter(({ kind }) => kind === 'warn').map(({ role, code }) => [role, code]),
      [['apply', Err.APPLY_ERROR]], 'Apply keeps the dissent diagnostic local')
    t.deepEqual(local.filter(({ code }) => code === Err.REPAIR_OUTSIDE_AGREEMENT)
      .map(({ role }) => role).sort(), roles,
    'each Proc reports its own repair fence')
    t.ok(roles.every((role) =>
      read(`${base}1${role.at(-1).toLowerCase()}`, readMeta).repair_state === 2n),
    'all repair fences are durable before cleanup')
    t.ok([...run.children.values()].every((child) =>
      child.exitCode === null && child.signalCode === null),
    'component diagnostics and close notifications do not prematurely exit child scripts')
    t.equal(supervisor.gate, run.gate, 'the final completion reaches the original Gate')
    t.notOk(c.errors.some(({ id, role, error }) => id === target && role === 'gate' &&
      [Err.APPLY_ERROR, Err.REPAIR_OUTSIDE_AGREEMENT].includes(error.code)),
    'Gate does not mirror owner diagnostics')
    await supervisor.close()
    t.ok([...run.children.values()].every((child) => child.signalCode === 'SIGKILL'),
      'explicit shutdown still kills and reaps all four owners')
  } finally { await c.close() }
  t.end()
})
