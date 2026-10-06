import http from 'node:http'
import { createHash, randomUUID } from 'node:crypto'
import { MonsterFt } from './src/index.js'

const toBuf = (value) => Buffer.from(JSON.stringify(value), 'utf8')
const toObj = (buf) => JSON.parse(buf.toString('utf8'))
const hashFact = (fact) => createHash('sha256')
  .update(JSON.stringify(fact))
  .digest('hex')

const quorum = 2
const nodeIds = ['1', '2', '3']
const nodesById = new Map()
const nodeIdHeader = 'monsterft-node-id'

function httpError(statusCode, message) {
  const err = new Error(message)
  err.statusCode = statusCode
  return err
}

async function readJson(req) {
  const chunks = []
  for await (const chunk of req) { chunks.push(chunk) }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'))
}

function sendJson(res, statusCode, value) {
  const body = Buffer.from(JSON.stringify(value), 'utf8')
  res.writeHead(statusCode, {
    'content-type': 'application/json',
    'content-length': body.length,
  })
  res.end(body)
}

function createFactServer(quorum) {
  // The service confirms a fact only after a quorum of MonsterFt nodes reports
  // the same value. Observations are frozen so replicated GETs also agree.
  const reports = new Map() // requestId -> (nodeId -> { hash, fact })
  const observations = new Map() // (requestId, observationId) -> result

  const postFact = async (req) => {
    const nodeIds = req.headersDistinct[nodeIdHeader] ?? []
    const { requestId, fact } = await readJson(req)
    if (nodeIds.length !== 1 || !requestId || fact === undefined) {
      throw httpError(400, 'POST requires a node ID, request ID, and fact')
    }

    const nodeId = nodeIds[0]
    const hash = hashFact(fact)
    let requestReports = reports.get(requestId)
    if (!requestReports) {
      requestReports = new Map()
      reports.set(requestId, requestReports)
    }

    const previous = requestReports.get(nodeId)
    if (previous !== undefined && previous.hash !== hash) {
      throw httpError(409, 'a node cannot change its fact report')
    }
    const report = previous ?? { hash, fact }
    if (previous === undefined) {
      requestReports.set(nodeId, report)
    }

    // Every member receives the same response for the same command.
    return {
      status: 'reported',
      requestId,
      fact: report.fact,
    }
  }

  const getFact = (url) => {
    const requestId = url.searchParams.get('requestId')
    const observationId = url.searchParams.get('observationId')
    if (!requestId || !observationId) {
      throw httpError(400, 'GET requires a request ID and observation ID')
    }

    // Freeze each observation so every member sees the same response even
    // if another report arrives while MonsterFt is applying this command.
    const observationKey = JSON.stringify([requestId, observationId])
    const previous = observations.get(observationKey)
    if (previous) { return previous }

    const counts = new Map()
    const requestReports = reports.get(requestId) ?? new Map()
    for (const report of requestReports.values()) {
      counts.set(report.hash, (counts.get(report.hash) ?? 0) + 1)
    }
    const agreedHash = [...counts]
      .find(([, count]) => count >= quorum)?.[0]
    const agreedReport = [...requestReports.values()]
      .find((report) => report.hash === agreedHash)
    const observation = agreedReport === undefined
      ? { status: 'pending', requestId, observationId }
      : {
          status: 'confirmed',
          requestId,
          observationId,
          fact: agreedReport.fact,
        }
    observations.set(observationKey, observation)
    return observation
  }

  return http.createServer((req, res) => {
    const handle = async () => {
      const url = new URL(req.url, 'http://localhost')
      if (url.pathname !== '/facts') {
        throw httpError(404, 'not found')
      }
      if (req.method === 'POST') {
        return sendJson(res, 200, await postFact(req))
      }
      if (req.method === 'GET') {
        return sendJson(res, 200, getFact(url))
      }
      throw httpError(405, 'method not allowed')
    }

    handle().catch((err) => {
      sendJson(res, err.statusCode ?? 500, { error: err.message })
    })
  })
}

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.removeListener('error', reject)
      resolve()
    })
  })
  return `http://127.0.0.1:${server.address().port}/facts`
}

async function closeServer(server) {
  if (!server.listening) { return }
  const closed = new Promise((resolve, reject) => {
    server.close((err) => err ? reject(err) : resolve())
  })
  server.closeAllConnections()
  await closed
}

async function requestJson(url, options) {
  const response = await fetch(url, options)
  const value = await response.json()
  if (!response.ok) { throw new Error(value.error) }
  return value
}

let serverUrl = null

function node(id, ids) {
  const send = (to, msg) => {
    return nodesById.get(to).onReceive(id, msg)
  }
  const apply = async (db, buf, term, seq) => {
    if (seq === 0n) { return }
    const command = toObj(buf)
    if (command.method === 'POST') {
      return requestJson(serverUrl, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'MonsterFt-Node-Id': id,
        },
        body: JSON.stringify({
          requestId: command.requestId,
          fact: command.fact,
        }),
      })
    }
    if (command.method === 'GET') {
      const url = new URL(serverUrl)
      url.searchParams.set('requestId', command.requestId)
      url.searchParams.set('observationId', command.observationId)
      return requestJson(url)
    }
    throw new Error(`unknown method: ${command.method}`)
  }
  const databasePath = `/tmp/node-${id}.db`
  const node = new MonsterFt(id, ids, send, databasePath, { apply, quorum })
  nodesById.set(id, node)
  return node
}

async function main() {
  const server = createFactServer(quorum)
  let nodes = []
  try {
    nodes = nodeIds.map((id) => node(id, nodeIds))
    for (const node of nodes) { node.del() }
    serverUrl = await listen(server)
    for (const node of nodes) { node.open() }
    await Promise.all(nodes.map((node) => node.awaitLeader(true)))

    // Queue the fact to be recorded
    const requestId = randomUUID().substr(0, 6)
    const fact = { account: 'alice', balance: 42 }
    const [postSeq, postResult] = await nodes[0].append(toBuf({
      method: 'POST', requestId, fact,
    }))
    console.log('POST', postSeq, postResult)

    // After POST success check total success using GET and observationId
    const observationId = randomUUID().substr(0, 6)
    const [getSeq, getResult] = await nodes[1].append(toBuf({
      method: 'GET', requestId, observationId,
    }))
    console.log('GET', getSeq, getResult)

    if (getResult.status !== 'confirmed') {
      throw new Error('the remote service did not confirm the fact')
    }
  } finally {
    for (const node of nodes) { node.close() }
    await closeServer(server)
  }
}

main().catch((err) => {
  console.error(err)
  process.exitCode = 1
})
