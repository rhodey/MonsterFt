import { mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { randomUUID } from 'node:crypto'
import { Gate, Supervisor, TcpCluster, UdsRouter } from '../src/index.js'

const nodeIds = ['1', '2', '3']
const roles = ['procA', 'procB', 'procC', 'apply']
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
const basePort = Number(process.env.MONSTERFT_PORT ?? 42000)
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
      args: role === 'apply' ? [id, local.paths[role], databasePath, serverUrl]
        : [id, local.paths[role], databasePath],
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
