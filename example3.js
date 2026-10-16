import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { Gate, MonsterProc, MonsterApply } from './src/index.v2.js'

const directory = mkdtempSync(`${tmpdir()}/monsterft-example3-`)

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

  const databasePath = `${directory}/node-${id}.db`
  const components = new Map()
  const sendFrom = (from) => (to, frame) => {
    return components.get(to).onReceive(from, frame)
  }

  const gate = new Gate(id, nodeIds, send, sendFrom('gate'))
  components.set('gate', gate)
  for (const role of ['procA', 'procB', 'procC']) {
    components.set(role, new MonsterProc(
      role, id, nodeIds, sendFrom(role), databasePath,
    ))
  }
  components.set('apply', new MonsterApply(
    id, sendFrom('apply'), databasePath, { apply },
  ))

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
  rmSync(directory, { recursive: true, force: true })
}
