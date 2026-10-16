import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { MonsterFt } from './src/index.v2.js'

const directory = mkdtempSync(`${tmpdir()}/monsterft-example3-`)

const nodeIds = ['1', '2', '3']
const nodesById = new Map()

function createNode(id) {
  const send = (to, message) => {
    return nodesById.get(to).onReceive(id, message)
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

  const node = new MonsterFt(
    id,
    nodeIds,
    send,
    `${directory}/node-${id}.db`,
    { apply },
  )
  nodesById.set(id, node)
  return node
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
