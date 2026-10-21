import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { Gate } from '../src/gate.js'
import { MonsterProc } from '../src/proc.js'
import { MonsterApply } from '../src/apply.js'
import { decodeFrame } from '../src/wire.js'

const roles = ['procA', 'procB', 'procC']
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function until(check, message = 'condition', timeout = 5_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    const value = check()
    if (value) { return value }
    await pause(5)
  }
  throw new Error(`timed out waiting for ${message}`)
}

function cluster(options = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-'))
  const ids = options.ids ?? ['a', 'b', 'c']
  const nodes = new Map()
  const history = []
  const errors = []
  const warnings = []
  const events = []
  const calls = []
  const blocked = new Set()
  const timers = new Set()
  let stopped = false
  const schedule = (fn, ms) => {
    if (!ms) { return fn() }
    const timer = setTimeout(() => { timers.delete(timer); fn() }, ms)
    timers.add(timer)
  }
  const defaultApply = (id, db, buffer, term, seq, index, matchIndex) => {
    if (seq === 0n) {
      db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB NOT NULL) STRICT')
      return null
    }
    db.prepare('INSERT INTO items (value) VALUES (?)').run(buffer)
    return buffer.toString()
  }
  const settings = {
    electionTimeout: 150, appendTimeout: 1_000,
    tickInterval: 2, localTimeout: 2_000, applyTimeout: 3_000,
    ...options.opts,
  }

  function makeNode(id) {
    const members = {}
    const base = path.join(directory, id)
    const localSend = (from) => (to, frame) => {
      if (stopped) { return }
      const receiver = members[to]
      const deliver = () => receiver?.onReceive(from, frame)
      if (options.local?.({ id, from, to, frame, deliver, members }) === false) { return }
      return schedule(deliver, options.localDelay ?? 0)
    }
    const send = (to, frame) => {
      if (stopped || blocked.has(id) || blocked.has(to)) { return }
      const decoded = decodeFrame(frame)
      history.push({ from: id, to, message: decoded.message, frame })
      const receiver = nodes.get(to)?.gate
      const deliver = () => receiver?.onReceive(id, frame)
      if (options.transport?.({ from: id, to, frame, message: decoded.message, deliver }) === false) { return }
      return schedule(deliver, options.delay ?? 0)
    }
    members.gate = new Gate(id, ids, send, localSend('gate'), settings)
    for (const role of roles) {
      members[role] = new MonsterProc(role, id, ids, localSend(role), base, settings)
    }
    members.apply = new MonsterApply(id, localSend('apply'), base, {
      ...settings,
      apply(db, buffer, term, seq, index, matchIndex) {
        calls.push({ id, buffer, term, seq, index, matchIndex })
        return (options.apply ?? defaultApply)(id, db, buffer, term, seq, index, matchIndex)
      },
    })
    for (const [role, member] of Object.entries(members)) {
      member.on('error', (error) => errors.push({ id, role, error }))
      member.on('warn', (error) => warnings.push({ id, role, error }))
      if (role === 'gate') {
        for (const type of ['cmd', 'sync', 'change']) {
          member.on(type, (event) => events.push({ id, type, event }))
        }
      }
    }
    nodes.set(id, members)
    return members
  }
  ids.forEach(makeNode)
  const gates = () => [...nodes.values()].map((node) => node.gate)
  const leader = (excluding = []) => gates().find((gate) =>
    !excluding.includes(gate.id) && gate.isOpen && gate.state === 'leader' &&
    gate.commitTerm === gate.term)
  const open = async () => {
    await Promise.all(gates().map((gate) => gate.open()))
    await until(() => leader(), 'committed leader')
    await until(() => [...nodes.values()].every((node) =>
      node.procA.engine.appliedSeq >= 0n), 'setup on all nodes')
  }
  const settled = async () => {
    await until(() => [...nodes.values()].filter((node) => node.gate.isOpen).every((node) =>
      roles.every((role) => node[role].engine.pendingApply === null &&
        node[role].engine.pendingCommand === null &&
        node[role].engine.appliedSeq === node[role].engine.commitSeq)), 'settled command protocol')
  }
  const close = async (failedOwners = []) => {
    await Promise.all(gates().map((gate) => gate.close().catch(() => {})))
    for (const node of nodes.values()) {
      // Release injected failed owners directly to simulate process exit.
      for (const role of roles) {
        if (failedOwners.includes(node[role])) { node[role].store.close() }
        else { node[role].close() }
      }
      if (failedOwners.includes(node.apply)) {
        node.apply.db?.close()
        node.apply.db = null
      } else { await node.apply.close() }
    }
    stopped = true
    for (const timer of timers) { clearTimeout(timer) }
    timers.clear()
    fs.rmSync(directory, { recursive: true, force: true })
  }
  return {
    directory, ids, nodes, settings, history, errors, warnings, events, calls,
    blocked, gates, leader, makeNode, open, settled, close,
  }
}

export { cluster, until, pause, roles }
