import { mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { Gate, Supervisor, TcpCluster, UdsRouter } from '../src/index.js'

const nodeIds = ['1', '2', '3']
const roles = ['procA', 'procB', 'procC', 'apply']
const id = process.argv[2]
if (!nodeIds.includes(id)) { throw new Error('usage: node example2/entry.js 1|2|3') }
const exitWithError = (err) => {
  console.error(`${id}/entry`, err.message, err)
  process.exit(1)
}

const directory = process.env.MONSTERFT_DATA ?? fileURLToPath(new URL('data/', import.meta.url))
mkdirSync(directory, { recursive: true })

const basePort = Number(process.env.MONSTERFT_PORT ?? 41000)
const peers = Object.fromEntries(nodeIds.map((nodeId, index) => [nodeId, {
  host: '127.0.0.1', basePort: basePort + index * nodeIds.length,
}]))

const supervisor = new Supervisor(async () => {
  const cluster = new TcpCluster(id, peers)
  const local = new UdsRouter(roles)
  const send = (to, frame) => cluster.send(to, frame)
  const localSend = (to, frame) => local.send(to, frame)
  const gate = new Gate(id, nodeIds, send, localSend)

  cluster.on('message', (from, frame) => gate.onReceive(from, frame))
  local.on('message', (from, frame) => gate.onReceive(from, frame))
  cluster.on('error', (err) => console.error(`${id}/tcp`, err))
  local.on('error', (err) => console.error(`${id}/uds`, err))
  gate.on('error', (err) => console.error(`${id}/gate`, err))

  const stop = () => { cluster.stop(); local.stop() }
  const cleanup = async () => {
    const results = await Promise.allSettled([cluster.close(), local.close()])
    const failure = results.find((result) => result.status === 'rejected')
    if (failure) { throw failure.reason }
  }

  try {
    await Promise.all([cluster.open(), local.open()])
    const databasePath = `${directory}/node-${id}.db`
    const children = Object.fromEntries(roles.map((role) => [role, {
      file: new URL(`${role}.js`, import.meta.url),
      args: [id, local.paths[role], databasePath],
    }]))

    return {
      gate, children,
      ready: () => local.ready(),
      failed: Promise.race([cluster.failed, local.failed]),
      stop, cleanup,
    }
  } catch (err) {
    gate.stop()
    stop()
    await cleanup().catch(exitWithError)
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
  closeWork = supervisor.close().finally(() => stopped.resolve())
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

    const command = Buffer.from(JSON.stringify({
      type: 'increment',
      name: 'count',
    }))
    const [cmdSeq, value] = await gate.append(command)
    console.log('count =', value)
  } else {
    await stopped.promise
  }
} catch (err) {
  if (!stopping) { console.error(err); process.exitCode = 1 }
} finally {
  await close().catch(exitWithError)
}
