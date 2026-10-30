import http from 'node:http'
import { createHash } from 'node:crypto'
import { mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { DatabaseSync } from 'node:sqlite'

const directory = process.env.MONSTERFT_DATA ?? fileURLToPath(new URL('data/', import.meta.url))
mkdirSync(directory, { recursive: true })
// Keep external observations across demo restarts, just like the Monster DBs.
const db = new DatabaseSync(`${directory}/facts.db`)
db.exec(`
  CREATE TABLE IF NOT EXISTS reports (
    request_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    hash TEXT NOT NULL,
    fact TEXT NOT NULL,
    PRIMARY KEY (request_id, node_id)
  ) STRICT;
  CREATE TABLE IF NOT EXISTS observations (
    request_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    result TEXT NOT NULL,
    PRIMARY KEY (request_id, observation_id)
  ) STRICT;
`)
const quorum = 2
const httpError = (statusCode, message) => Object.assign(new Error(message), { statusCode })
const hashFact = (fact) => createHash('sha256').update(JSON.stringify(fact)).digest('hex')

async function postFact(req) {
  const nodeIds = req.headersDistinct['monsterft-node-id'] ?? []
  const chunks = []
  for await (const chunk of req) { chunks.push(chunk) }
  const { requestId, fact } = JSON.parse(Buffer.concat(chunks).toString())
  if (nodeIds.length !== 1 || !['1', '2', '3'].includes(nodeIds[0]) ||
      typeof requestId !== 'string' || !requestId || fact === undefined) {
    throw httpError(400, 'POST requires a node ID, request ID, and fact')
  }
  const nodeId = nodeIds[0]
  const hash = hashFact(fact)
  const previous = db.prepare('SELECT hash FROM reports WHERE request_id = ? AND node_id = ?')
    .get(requestId, nodeId)
  if (previous && previous.hash !== hash) {
    throw httpError(409, 'a node cannot change its fact report')
  }
  db.prepare('INSERT OR IGNORE INTO reports VALUES (?, ?, ?, ?)')
    .run(requestId, nodeId, hash, JSON.stringify(fact))
  // Every member receives the same response for the same command.
  return { status: 'reported', requestId, fact }
}

function getFact(url) {
  const requestId = url.searchParams.get('requestId')
  const observationId = url.searchParams.get('observationId')
  if (!requestId || !observationId) {
    throw httpError(400, 'GET requires a request ID and observation ID')
  }
  const previous = db.prepare(`
    SELECT result FROM observations WHERE request_id = ? AND observation_id = ?
  `).get(requestId, observationId)
  if (previous) { return JSON.parse(previous.result) }

  const agreed = db.prepare(`
    SELECT fact FROM reports WHERE request_id = ?
    GROUP BY hash HAVING count(*) >= ?
  `).get(requestId, quorum)
  const observation = agreed
    ? { status: 'confirmed', requestId, observationId, fact: JSON.parse(agreed.fact) }
    : { status: 'pending', requestId, observationId }
  // Freeze the observation even if another report arrives during application.
  db.prepare('INSERT INTO observations VALUES (?, ?, ?)')
    .run(requestId, observationId, JSON.stringify(observation))
  return observation
}

function sendJson(res, status, value) {
  const body = Buffer.from(JSON.stringify(value))
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': body.length })
  res.end(body)
}

const server = http.createServer((req, res) => {
  const handle = async () => {
    const url = new URL(req.url, 'http://localhost')
    if (url.pathname !== '/facts') { throw httpError(404, 'not found') }
    if (req.method === 'POST') { return sendJson(res, 200, await postFact(req)) }
    if (req.method === 'GET') { return sendJson(res, 200, getFact(url)) }
    throw httpError(405, 'method not allowed')
  }
  handle().catch((err) => sendJson(res, err.statusCode ?? 500, { error: err.message }))
})
await new Promise((resolve, reject) => {
  server.once('error', reject)
  server.listen(Number(process.env.MONSTERFT_HTTP_PORT ?? 43000), '127.0.0.1', () => {
    server.removeListener('error', reject)
    resolve()
  })
})
const close = () => {
  server.close(() => db.close())
  server.closeAllConnections()
}
process.once('SIGINT', close)
process.once('SIGTERM', close)
