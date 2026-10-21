import { mkdirSync, mkdtempSync, rmSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { randomUUID } from 'node:crypto'
import { Gate, Supervisor, udsServer } from '../src/index.js'
import { clusterTransport, nodeIds } from '../example-shared/cluster.js'

const id = process.argv[2]
if (!nodeIds.includes(id)) { throw new Error('usage: node example3/entry.js 1|2|3') }
const exitWithError = (err) => {
  console.error(`${id}/entry`, err.message, err)
  process.exit(1)
}
const serverUrl = process.env.MONSTERFT_FACT_URL
if (!serverUrl) { throw new Error('MONSTERFT_FACT_URL is required; use example3/entry.sh') }
const directory = process.env.MONSTERFT_DATA ?? fileURLToPath(new URL('data/', import.meta.url))
mkdirSync(directory, { recursive: true })
let supervisor
const cluster = await clusterTransport(id, Number(process.env.MONSTERFT_PORT ?? 42000),
  (from, frame) => supervisor?.gate?.onReceive(from, frame))

supervisor = new Supervisor(async () => {
  // Short paths also fit macOS's Unix socket path limit.
  const socketDirectory = mkdtempSync('/tmp/mft-')
  const local = new Map()
  const servers = []
  const children = {}
  const connections = []
  let gate
  const cleanup = async () => {
    for (const socket of local.values()) { socket.destroy() }
    await Promise.all(servers.map((server) => new Promise((resolve, reject) => {
      if (!server.listening) { resolve(); return }
      server.close((err) => err ? reject(err) : resolve())
    })))
    rmSync(socketDirectory, { recursive: true, force: true })
  }
  try {
    const databasePath = `${directory}/node-${id}.db`
    for (const role of ['procA', 'procB', 'procC', 'apply']) {
      const socketPath = `${socketDirectory}/${role}`
      const connected = Promise.withResolvers()
      connected.promise.catch(() => {})
      connections.push(connected.promise)
      servers.push(await udsServer(socketPath, (socket) => {
        if (local.has(role)) { socket.destroy(); return }
        local.set(role, socket)
        socket.on('data', (frame) => gate.onReceive(role, frame))
        socket.on('error', (err) => console.error(`${id}/${role}/uds`, err))
        connected.resolve()
      }, (err) => {
        console.error(`${id}/${role}/uds`, err)
        connected.reject(err)
        for (const socket of local.values()) { socket.destroy() }
      }))
      children[role] = {
        file: new URL(`${role}.js`, import.meta.url),
        args: role === 'apply' ? [id, socketPath, databasePath, serverUrl]
          : [id, socketPath, databasePath],
      }
    }

    const localSend = (to, frame) => {
      const socket = local.get(to)
      if (!socket || socket.destroyed) { throw new Error(`${to} is disconnected`) }
      socket.write(frame)
    }

    gate = new Gate(id, nodeIds, cluster.send, localSend)

    gate.on('error', (err) => console.error(`${id}/gate`, err))
    return { gate, children, ready: () => Promise.all(connections), cleanup }
  } catch (err) {
    await cleanup()
    throw err
  }
})
supervisor.on('error', (err) => {
  if (supervisor.isStopped) { exitWithError(err) }
  console.error(`${id}/supervisor`, err)
})

let stopping = false
let closeWork
const stopped = Promise.withResolvers()
const close = () => {
  if (closeWork) { return closeWork }
  stopping = true
  closeWork = (async () => {
    try { await supervisor.close() } finally { await cluster.close() }
  })().finally(() => stopped.resolve())
  return closeWork
}
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => close().catch(exitWithError))
}

try {
  await supervisor.open()
  if (id === '1') {
    const gate = supervisor.gate
    await gate.awaitLeader(true)

    const requestId = randomUUID()
    const fact = { account: 'alice', balance: 42 }
    const postCommand = Buffer.from(JSON.stringify({
      method: 'POST', requestId, fact,
    }))
    const [postSeq, postResult] = await gate.append(postCommand)
    console.log('POST', postSeq, postResult)

    const getCommand = Buffer.from(JSON.stringify({
      method: 'GET', requestId, observationId: randomUUID(),
    }))
    const [getSeq, getResult] = await gate.append(getCommand)
    console.log('GET', getSeq, getResult)
    if (getResult.status !== 'confirmed') {
      throw new Error('the remote service did not confirm the fact')
    }
  } else {
    await stopped.promise
  }
} catch (err) {
  if (!stopping) { console.error(err); process.exitCode = 1 }
} finally {
  await close().catch(exitWithError)
}
