import { MonsterFt } from './src/index.js'

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
    `/tmp/node${id}.db`,
    { apply },
  )
  nodesById.set(id, node)
  return node
}

const nodes = nodeIds.map(createNode)
nodes.forEach((node) => node.del())
nodes.forEach((node) => node.open())
await Promise.all(nodes.map((node) => node.awaitLeader(true)))

const command = Buffer.from(JSON.stringify({
  type: 'increment',
  name: 'count',
}))
const [cmdSeq, value] = await nodes[0].append(command)
console.log('count =', value)

nodes.forEach((node) => node.close())
