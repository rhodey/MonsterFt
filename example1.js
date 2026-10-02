import { RaftNode } from './src/index.js'

const toBuf = (obj) => Buffer.from(JSON.stringify(obj), 'utf8')
const toObj = (buf) => JSON.parse(buf.toString('utf8'))

const nodeIds = ['1', '2', '3']
const nodesById = new Map()

function createNode(id) {
  const send = (to, message) => {
    return nodesById.get(to).onReceive(id, message)
  }

  const opts = () => {
    let count = 0n
    const apply = (node, bufs, seqs, terms) => bufs.map((buf) => buf ? ++count : null)
    return { apply, quorum: 3 } // full repl for demo
  }

  const databasePath = `/tmp/node${id}.db`
  const node = new RaftNode(id, nodeIds, send, databasePath, opts)
  nodesById.set(id, node)
  return node
}

async function main() {
  const nodes = nodeIds.map(createNode)
  for (const node of nodes) { node.del() }
  for (const node of nodes) { node.open() }
  console.log('open')

  await Promise.all(nodes.map((node) => node.awaitLeader(true)))
  console.log('have leader')

  // append to any node = fwd to leader
  const data = [{ a: 1 }, { b: 2 }, { c: 3 }]
  for (const [idx, value] of data.entries()) {
    const [seq, result] = await nodes[idx].append(toBuf(value))
    console.log('append', seq, result)
  }

  nodes.forEach((node) => console.log('head', node.id, toObj(node.head)))

  for (const node of nodes) { node.close() }
}

main().catch(console.log)
