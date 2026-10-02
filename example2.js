import sodium from 'libsodium-wrappers'
import {
  RaftNode,
  tcpServer, tcpClient,
} from './src/index.js'

function errCb(err) {
  console.error('error', err)
  process.exit(1)
}

function opts() {
  let myCount = 0n
  const apply = (node, bufs, seqs, terms) => {
    const results = []
    bufs.forEach((buf) => results.push(buf ? ++myCount : null))
    return results
  }
  return { apply, quorum: 2 }
}

function node(key, id, ids) {
  const clients = {}
  const send = (to, msg) => {
    let client = clients[to]
    if (!client) {
      const [host, port] = to.split(`:`)
      client = clients[to] = tcpClient(key, parseInt(port), host).then((sock) => sock)
    }
    return client.then((sock) => sock.write(msg))
  }
  const number = ids.indexOf(id) + 1
  const databasePath = `/tmp/node${number}.db`
  const node = new RaftNode(id, ids, send, databasePath, opts)
  const port = parseInt(id.split(`:`)[1])
  const msgCb = (sock, msg) => node.onReceive(msg.from, msg)
  return tcpServer(key, port, msgCb, errCb).then((srv) => {
    node.clients = clients
    node.srv = srv
    return node
  })
}

async function main() {
  await sodium.ready
  const key = sodium.crypto_generichash(32, sodium.from_string('secret'))
  const ids = new Array(3).fill(0).map((z, idx) => `127.0.0.1:${9000 + idx + 1}`)
  let nodes = ids.map((id) => node(key, id, ids))
  nodes = await Promise.all(nodes)
  for (const node of nodes) { node.del() }
  for (const node of nodes) { node.open() }
  console.log('open')
  await Promise.all(nodes.map((node) => node.awaitLeader(true)))
  console.log('have leader')

  const buf = Buffer.from(new Array(1024).fill('a').join(''), 'utf8')
  let bufs = []

  const producer = setInterval(() => {
    bufs.push(buf)
    bufs.push(buf)
    bufs.push(buf)
  }, 500)

  const leader = nodes.find((node) => node.state === 'leader')

  const consumer = setInterval(() => {
    const copy = [...bufs]
    bufs = []
    if (copy.length <= 0) { return }
    leader.appendBatch(copy).then((ok) => {
      const [seq, count] = ok
      console.log(seq, count)
    }).catch(errCb)
  }, 500)

  const end = () => {
    clearInterval(producer)
    clearInterval(consumer)
    for (const node of nodes) { node.close() }
    nodes.map((node) => Object.values(node.clients)).flat()
      .forEach((client) => client.then((conn) => conn.destroy()))
    nodes.forEach((node) => node.srv.close())
  }

  setTimeout(end, 10_000)
}

main().catch(errCb)
