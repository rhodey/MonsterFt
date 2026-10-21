import { mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { Gate, MonsterProc, MonsterApply } from '../src/index.js'

const directory = process.env.MONSTERFT_DATA ?? fileURLToPath(new URL('data/', import.meta.url))
mkdirSync(directory, { recursive: true })

const nodeIds = ['1', '2', '3']
const nodesById = new Map()

function createNode(id) {
  const send = (to, frame) => {
    return nodesById.get(to).onReceive(id, frame)
  }

  const apply = (db, data, term, seq, index) => {
    if (seq === 0n) {
      db.exec(`
        CREATE TABLE IF NOT EXISTS counters (
          name TEXT PRIMARY KEY NOT NULL,
          value INTEGER NOT NULL
        ) STRICT
      `)
      return
    }

    const command = JSON.parse(data.toString())
    if (command.type !== 'increment') {
      throw new Error(`unknown command: ${command.type}`)
    }

    db.prepare(`
      INSERT INTO counters (name, value) VALUES (?, 1)
        ON CONFLICT (name) DO UPDATE SET value = value + 1
    `).run(command.name)

    return db.prepare(
      'SELECT value FROM counters WHERE name = ?',
    ).get(command.name).value
  }

  const local = new Map()
  const localSend = (from) => (to, frame) => {
    return local.get(to).onReceive(from, frame)
  }

  const gate = new Gate(id, nodeIds, send, localSend('gate'))
  local.set('gate', gate)

  const opts = { electionTimeout: 100 }
  const databasePath = `${directory}/node-${id}.db`
  for (const role of ['procA', 'procB', 'procC']) {
    local.set(role, new MonsterProc(
      role, id, nodeIds, localSend(role), databasePath, opts
    ))
  }
  local.set('apply', new MonsterApply(
    id, localSend('apply'), databasePath, { apply },
  ))

  for (const [role, proc] of local) {
    proc.on('error', (err) => console.error(`${id}/${role}`, err))
  }

  nodesById.set(id, gate)
  return gate
}

const nodes = nodeIds.map(createNode)
try {
  await Promise.all(nodes.map((node) => node.open()))
  await Promise.all(nodes.map((node) => node.awaitLeader(true)))

  const command = Buffer.from(JSON.stringify({
    type: 'increment',
    name: 'count',
  }))
  const [cmdSeq, value] = await nodes[0].append(command)
  console.log('count =', value)
} finally {
  await Promise.all(nodes.map((node) => node.close()))
}
