import sodium from 'libsodium-wrappers'
import { tcpClient, tcpServer } from '../src/index.js'

export const nodeIds = ['1', '2', '3']

// A known demo key only. Set MONSTERFT_KEY to a shared 32-byte hex key outside
// the examples. Each lower-numbered node dials its higher-numbered peers.
export async function clusterTransport(id, basePort, onReceive) {
  await sodium.ready
  const key = Buffer.from(process.env.MONSTERFT_KEY ?? '42'.repeat(32), 'hex')
  if (key.length !== 32) { throw new Error('MONSTERFT_KEY must encode 32 bytes') }
  const peers = new Map()
  const sockets = new Set()
  const senders = new Set()
  const attempts = new Set()
  const timers = new Set()
  let closing = false

  const server = await tcpServer(key, basePort + Number(id), (stream, frame) => {
    if (!stream.peerId) {
      const peer = frame.toString()
      if (!nodeIds.includes(peer) || peer >= id || peers.has(peer) && !peers.get(peer).destroyed) {
        stream.destroy()
        return
      }
      stream.peerId = peer
      senders.delete(peers.get(peer))
      senders.add(stream)
      peers.set(peer, stream)
      stream.write(Buffer.from(id))
      return
    }
    onReceive(stream.peerId, frame)
  }, (err) => {
    if (!closing) { console.warn(`${id}/tcp`, err.message) }
  })
  server.on('connection', (socket) => {
    sockets.add(socket)
    socket.once('close', () => sockets.delete(socket))
  })

  const retry = (peer) => {
    if (closing) { return }
    const timer = setTimeout(() => {
      timers.delete(timer)
      connect(peer)
    }, 250)
    timers.add(timer)
  }
  const connect = (peer) => {
    const work = (async () => {
      let stream
      let identified = false
      try {
        stream = await tcpClient(key, basePort + Number(peer), '127.0.0.1', (frame) => {
          if (!identified) {
            if (frame.toString() !== peer) { stream.destroy(); return }
            identified = true
            peers.set(peer, stream)
            return
          }
          onReceive(peer, frame)
        })
        if (closing) { stream.destroy(); return }
        senders.add(stream)
        stream.on('error', (err) => {
          if (!closing) { console.warn(`${id}/tcp/${peer}`, err.message) }
        })
        stream.once('close', () => {
          senders.delete(stream)
          if (peers.get(peer) === stream) { peers.delete(peer) }
          retry(peer)
        })
        stream.write(Buffer.from(id))
      } catch {
        // Peers can start later; a failed connection attempt is retried.
        retry(peer)
      }
    })()
    attempts.add(work)
    work.finally(() => attempts.delete(work))
  }
  for (const peer of nodeIds.filter((peer) => peer > id)) { connect(peer) }

  return {
    send(to, frame) {
      const stream = peers.get(to)
      // Cluster transport is lossy; Raft retries after disconnected periods.
      if (stream && !stream.destroyed) { stream.write(frame) }
    },
    async close() {
      closing = true
      for (const timer of timers) { clearTimeout(timer) }
      timers.clear()
      await Promise.all(attempts)
      for (const stream of senders) { stream.destroy() }
      for (const socket of sockets) { socket.destroy() }
      await new Promise((resolve, reject) => {
        server.close((err) => err ? reject(err) : resolve())
      })
    },
  }
}
