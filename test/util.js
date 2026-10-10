import path from 'node:path'
import { SQLiteLog, RaftNode } from '../src/index.js'

const noop = () => {}
const sleep = (ms) => new Promise((res) => setTimeout(res, ms))
const sendAll = () => true
const delayNone = () => 0
const TEST_DIR = process.env.TEST_DIR ?? '/tmp/'

const toBuf = (obj) => {
  if (obj === null) { return null }
  return Buffer.from(JSON.stringify(obj), 'utf8')
}

const toEntry = (data, term=0n) => {
  return { term, entry: Buffer.isBuffer(data) ? data : Buffer.from(data) }
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

const databasePath = (name) => {
  return path.join(TEST_DIR, `${name}.sqlite`)
}

function logFixture(t, name, opts={}) {
  const file = databasePath(name)
  const logs = []
  const create = () => {
    const log = new SQLiteLog(file, opts)
    logs.push(log)
    return log
  }
  t.teardown(() => {
    for (const log of logs) { log.close() }
    new SQLiteLog(file).del()
  })
  return { file, create }
}

function comms(allowSend=sendAll, delaySend=delayNone) {
  const nodes = new Map()
  const register = (node) => nodes.set(node.id, node)

  async function send(to, from, msg) {
    if (!allowSend(to, from, msg)) { return }
    const delay = delaySend(to, from, msg)
    if (delay) { await sleep(delay) }
    const node = nodes.get(to)
    if (!node) { throw new Error(`node ${from} send to ${to} not found`) }
    node.onReceive(from, msg)
  }

  return { register, send }
}

function connect(comms, a=3, b=null, opts={}, logFn=null) {
  b = b ?? 1
  logFn = logFn ?? ((id) => databasePath(`node-${id}`))
  const nodes = []
  for (let i = b; i <= a; i++) { nodes.push(String(i)) }
  return nodes.map((id) => {
    const file = logFn(id)
    const send = (to, msg) => comms.send(to, id, msg)
    const node = new RaftNode(id, nodes, send, file, opts)
    comms.register(node)
    return node
  })
}

const reset = (nodes) => nodes.forEach((node) => node.log.del())
const open = (nodes) => nodes.forEach((node) => node.open())
const close = (nodes) => {
  const failures = []
  for (const node of nodes) {
    try {
      node.close()
    } catch (err) {
      failures.push(err)
    }
  }
  if (failures.length === 1) { throw failures[0] }
  if (failures.length > 1) {
    throw new AggregateError(failures, failures[0].message)
  }
}

const awaitResolve = (promises, minimum) => {
  return new Promise((res) => {
    let count = 0
    promises.forEach((promise) => {
      promise.then(() => {
        if (++count >= minimum) { res() }
      }).catch(noop)
    })
  })
}

const ready = (nodes, count=null, commit=false) => {
  count = count ?? nodes.length
  return awaitResolve(nodes.map((node) => node.awaitLeader(commit)), count)
}

const leaders = (nodes) => nodes.filter((node) => node.state === 'leader')
const followers = (nodes) => nodes.filter((node) => node.state === 'follower')

export {
  TEST_DIR,
  toBuf, toObj, toEntry,
  databasePath, logFixture,
  sleep, comms, connect, reset,
  open, close, ready,
  leaders, followers,
}
