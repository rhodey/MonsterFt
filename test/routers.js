import test from 'tape'
import net from 'node:net'
import { mkdtemp, rm, lstat, readdir, writeFile, readFile } from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { TcpCluster } from '../src/net/tcpcluster.js'
import { UdxRouter } from '../src/net/udxrouter.js'
import { udxClient } from '../src/net/udx.js'
import * as Err from '../src/error.js'
import { until, pause } from './helper.js'

const noop = () => {}
const event = (target, name) => new Promise((resolve) => target.once(name, resolve))
const closeServer = (server) => !server?.listening ? Promise.resolve() :
  new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve()))
const timeout = (promise, ms = 3_000) => new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error('router test timeout')), ms)
  promise.then((value) => { clearTimeout(timer); resolve(value) },
    (err) => { clearTimeout(timer); reject(err) })
})
const frame = (payload) => {
  const header = Buffer.alloc(4)
  header.writeUInt32BE(payload.length)
  return Buffer.concat([header, payload])
}
const watch = (router) => {
  const warnings = []
  const errors = []
  const messages = []
  router.on('warn', (err) => warnings.push(err))
  router.on('error', (err) => errors.push(err))
  router.on('message', (from, bytes) => messages.push({ from, bytes }))
  return { warnings, errors, messages }
}

async function listen(port = 0) {
  const server = net.createServer()
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(port, '127.0.0.1', resolve)
  })
  return server
}

async function freePorts(count = 12) {
  // Reserve the entire derived range before releasing it for the routers.
  for (;;) {
    const servers = [await listen()]
    const base = servers[0].address().port
    try {
      if (base + count > 65535) { continue }
      for (let index = 1; index < count; index++) { servers.push(await listen(base + index)) }
      return base
    } catch (err) {
      if (err.code !== 'EADDRINUSE') { throw err }
    } finally { await Promise.all(servers.map(closeServer)) }
  }
}

function members(base) {
  return {
    zulu: { host: '127.0.0.1', basePort: base + 8 },
    alpha: { host: '127.0.0.1', basePort: base },
    middle: { host: '127.0.0.1', basePort: base + 4 },
  }
}

async function rawConnect(port) {
  const socket = net.connect(port, '127.0.0.1')
  socket.on('error', noop)
  await timeout(event(socket, 'connect'))
  return socket
}

test('router constructors validate endpoints, derived ports, and limits', (t) => {
  const peers = members(20_000)
  const illegal = (fn) => t.throws(fn, (err) => err.code === Err.ARGUMENT_ILLEGAL)
  illegal(() => new TcpCluster('missing', peers))
  illegal(() => new TcpCluster('alpha', []))
  illegal(() => new TcpCluster('alpha', { alpha: { host: '', basePort: 1000 } }))
  illegal(() => new TcpCluster('alpha', { alpha: { host: 'localhost', basePort: 0 } }))
  illegal(() => new TcpCluster('alpha', { ...peers, zulu: { host: 'localhost', basePort: 65535 } }))
  illegal(() => new TcpCluster('alpha', { ...peers, alpha: { host: 'localhost', basePort: 65534 } }))
  t.doesNotThrow(() => new TcpCluster('alpha', {
    ...peers, zulu: { host: 'localhost', basePort: 65534 },
  }), 'an unused own-index port does not restrict the actual listening ports')
  illegal(() => new TcpCluster('alpha', peers, { connectTimeout: 0 }))
  illegal(() => new TcpCluster('alpha', peers, { reconnectDelay: -1 }))
  illegal(() => new TcpCluster('alpha', peers, { maxFrameBytes: -1 }))
  illegal(() => new UdxRouter([]))
  illegal(() => new UdxRouter(['same', 'same']))
  illegal(() => new UdxRouter(['a'], { maxQueuedBytes: 3 }))
  t.end()
})

test('TcpCluster sorts membership and routes opaque frames through separate incoming and outgoing connections', async (t) => {
  const peers = members(await freePorts())
  const routers = Object.keys(peers).map((id) => new TcpCluster(id, peers, {
    bindHost: '127.0.0.1', reconnectDelay: 10,
  }))
  const observed = routers.map(watch)
  try {
    await Promise.all(routers.map((router) => router.open()))
    await until(() => routers.every((router) => router._incoming.size === 2 && router._outgoing.size === 2),
      'TCP connections in both directions')
    for (const router of routers) {
      t.deepEqual(router.nodes, ['alpha', 'middle', 'zulu'])
      const index = router.nodes.indexOf(router.id)
      t.deepEqual([...router._servers].map((server) => server.address().port).sort((a, b) => a - b),
        router.nodes.map((_, peer) => peers[router.id].basePort + peer)
          .filter((port) => port !== peers[router.id].basePort + index),
        `${router.id}: every other peer has its own listening port`)
      t.notEqual(router._incoming.get(router.nodes[(index + 1) % 3]),
        router._outgoing.get(router.nodes[(index + 1) % 3]), 'directions use distinct connections')
      for (const other of routers.filter((other) => other !== router)) {
        t.equal(router.send(other.id, Buffer.from([0xc1, 0xff, index])), true)
        t.equal(router.send(other.id, Buffer.alloc(0)), true)
      }
    }
    await until(() => observed.every(({ messages }) => messages.length === 4), 'messages from every peer')
    for (let index = 0; index < routers.length; index++) {
      const router = routers[index]
      for (const from of router.nodes.filter((id) => id !== router.id)) {
        t.deepEqual(observed[index].messages.filter((item) => item.from === from).map((item) => item.bytes),
          [Buffer.from([0xc1, 0xff, router.nodes.indexOf(from)]), Buffer.alloc(0)],
          'frames need not contain valid protocol or MessagePack data')
      }
      t.deepEqual(observed[index].errors, [])
    }
  } finally { await Promise.all(routers.map((router) => router.close())) }
  t.end()
})

test('TcpCluster sends no greeting and replaces accepted connections without stale disconnects', async (t) => {
  const base = await freePorts()
  const peers = { a: { host: '127.0.0.1', basePort: base }, b: { host: '127.0.0.1', basePort: base + 4 } }
  const server = await listen(peers.a.basePort + 1)
  const connection = event(server, 'connection')
  const router = new TcpCluster('b', peers, { bindHost: '127.0.0.1' })
  const seen = watch(router)
  const sockets = []
  try {
    await router.open()
    const outgoing = await timeout(connection)
    sockets.push(outgoing)
    outgoing.on('error', noop)
    const outgoingBytes = []
    outgoing.on('data', (data) => outgoingBytes.push(data))
    const first = await rawConnect(peers.b.basePort)
    sockets.push(first)
    const bytes = []
    first.on('data', (data) => bytes.push(data))
    await pause(20)
    t.deepEqual(bytes, [], 'accepting a connection sends no identification or encryption header')
    t.deepEqual(outgoingBytes, [], 'initiating a connection sends no greeting either')
    const payload = Buffer.from([0xc1, 0, 255])
    t.equal(router.send('a', payload), true)
    await until(() => Buffer.concat(outgoingBytes).length === payload.length + 4, 'plain outgoing frame')
    t.deepEqual(Buffer.concat(outgoingBytes), frame(payload), 'wire starts directly with a length and payload')
    t.deepEqual(bytes, [], 'accepted connections never carry outgoing messages')
    const originalOutgoing = router._outgoing.get('a')
    const firstClosed = event(first, 'close')
    const second = await rawConnect(peers.b.basePort)
    sockets.push(second)
    second.resume()
    await timeout(firstClosed)
    second.write(frame(Buffer.from('replacement')))
    await until(() => seen.messages.length === 1, 'replacement accepted socket')
    t.deepEqual(seen.messages[0], { from: 'a', bytes: Buffer.from('replacement') },
      'the listening port determines the peer id')
    t.equal(router._outgoing.get('a'), originalOutgoing, 'replacing the incoming direction preserves the outgoing connection')
    second.write(frame(Buffer.from('still connected')))
    await until(() => seen.messages.length === 2, 'old close callback preserves its incoming replacement')
    t.equal(router.send('a', Buffer.from('still connected')), true)
    t.deepEqual(seen.warnings, [], 'replacing an old socket is not a peer outage')
    t.deepEqual(seen.errors, [])
    t.ok(router.isOpen)
  } finally {
    await router.close()
    for (const socket of sockets) { socket.destroy() }
    await closeServer(server)
  }
  t.end()
})

test('TcpCluster opens independently, drops disconnected sends, and reconnects ordinary peer outages', async (t) => {
  const base = await freePorts()
  const peers = { a: { host: '127.0.0.1', basePort: base }, b: { host: '127.0.0.1', basePort: base + 4 } }
  const a = new TcpCluster('a', peers, { bindHost: '127.0.0.1', reconnectDelay: 10 })
  const seen = watch(a)
  let b = null
  let failed = false
  a.failed.catch(() => { failed = true })
  try {
    await timeout(a.open())
    t.ok(a.isOpen, 'opening does not wait for any remote node')
    t.equal(a.send('b', Buffer.from('dropped')), false)
    await until(() => seen.warnings.length > 0, 'connection-refused warning')
    t.ok(a.isOpen, 'connection failure leaves local transport running')
    for (let attempt = 0; attempt < 2; attempt++) {
      b = new TcpCluster('b', peers, { bindHost: '127.0.0.1', reconnectDelay: 10 })
      const peer = watch(b)
      await b.open()
      await until(() => [a, b].every((router) => router._incoming.size === 1 && router._outgoing.size === 1),
        'reconnected pair in both directions')
      a.send('b', Buffer.from(`attempt ${attempt}`))
      await until(() => peer.messages.length === 1, 'delivered after reconnect')
      t.deepEqual(peer.messages, [{ from: 'a', bytes: Buffer.from(`attempt ${attempt}`) }],
        'disconnected frames are not replayed after connection returns')
      await b.close()
      await until(() => a._incoming.size === 0 && a._outgoing.size === 0, 'both peer directions disconnected')
      t.ok(a.isOpen)
      t.equal(a.send('b', Buffer.from('also dropped')), false)
    }
    t.equal(failed, false, 'peer outages never reject the attempt failure promise')
    t.deepEqual(seen.errors, [])
  } finally { await Promise.all([a.close(), b?.close()]) }
  t.end()
})

test('TcpCluster reconnects one outgoing direction while the other direction keeps receiving', async (t) => {
  const base = await freePorts()
  const peers = { a: { host: '127.0.0.1', basePort: base }, b: { host: '127.0.0.1', basePort: base + 4 } }
  const routers = ['a', 'b'].map((id) => new TcpCluster(id, peers, {
    bindHost: '127.0.0.1', reconnectDelay: 100,
  }))
  const [a, b] = routers
  const [seenA, seenB] = routers.map(watch)
  try {
    await Promise.all(routers.map((router) => router.open()))
    await until(() => routers.every((router) => router._incoming.size === 1 && router._outgoing.size === 1),
      'both directions connected')
    const incomingA = a._incoming.get('b')
    const outgoingB = b._outgoing.get('a')
    const outgoingA = a._outgoing.get('b')
    outgoingA.destroy()
    await until(() => !a._outgoing.has('b') && !b._incoming.has('a'), 'one disconnected direction')
    t.equal(a.send('b', Buffer.from('dropped')), false, 'only the disconnected direction drops sends')
    t.equal(a._incoming.get('b'), incomingA, 'incoming connection remains installed')
    t.equal(b._outgoing.get('a'), outgoingB, 'higher-index node keeps its own outgoing connection')
    t.equal(b.send('a', Buffer.from('reverse still works')), true)
    await until(() => seenA.messages.length === 1, 'opposite direction still receives')
    t.deepEqual(seenA.messages, [{ from: 'b', bytes: Buffer.from('reverse still works') }])
    await until(() => a._outgoing.has('b') && b._incoming.has('a'), 'disconnected direction reconnected')
    t.notEqual(a._outgoing.get('b'), outgoingA)
    t.equal(b._outgoing.get('a'), outgoingB, 'reconnect does not replace the independent reverse connection')
    t.equal(a.send('b', Buffer.from('after reconnect')), true)
    await until(() => seenB.messages.length === 1, 'new outgoing connection sends')
    t.deepEqual(seenB.messages, [{ from: 'a', bytes: Buffer.from('after reconnect') }],
      'messages dropped during the outage are not replayed')
    t.deepEqual(seenA.errors, [])
    t.deepEqual(seenB.errors, [])
  } finally { await Promise.all(routers.map((router) => router.close())) }
  t.end()
})

test('TcpCluster rejects incoming messages on outgoing connections and retries only that direction', async (t) => {
  const base = await freePorts()
  const peers = { a: { host: '127.0.0.1', basePort: base }, b: { host: '127.0.0.1', basePort: base + 4 } }
  const server = await listen(peers.a.basePort + 1)
  const sockets = []
  server.on('connection', (socket) => { sockets.push(socket); socket.on('error', noop); socket.resume() })
  const router = new TcpCluster('b', peers, { bindHost: '127.0.0.1', reconnectDelay: 10 })
  const seen = watch(router)
  let incoming
  try {
    await router.open()
    await until(() => sockets.length === 1 && router._outgoing.has('a'), 'higher-index outgoing connection')
    incoming = await rawConnect(peers.b.basePort)
    incoming.resume()
    await until(() => router._incoming.has('a'), 'independent incoming connection')
    const originalIncoming = router._incoming.get('a')
    const closed = event(sockets[0], 'close')
    sockets[0].write(frame(Buffer.from('wrong direction')))
    await timeout(closed)
    await until(() => seen.warnings.length === 1, 'unexpected direction warning')
    t.deepEqual(seen.messages, [], 'outgoing sockets never forward received protocol frames')
    t.equal(router._incoming.get('a'), originalIncoming)
    incoming.write(frame(Buffer.from('right direction')))
    await until(() => seen.messages.length === 1, 'proper incoming connection remains usable')
    t.deepEqual(seen.messages, [{ from: 'a', bytes: Buffer.from('right direction') }])
    await until(() => sockets.length === 2 && router._outgoing.has('a'), 'retry after wrong-direction message')
    t.ok(router.isOpen)
    t.deepEqual(seen.errors, [])
  } finally {
    incoming?.destroy()
    await router.close()
    for (const socket of sockets) { socket.destroy() }
    await closeServer(server)
  }
  t.end()
})

test('TcpCluster drops queue overflow and rejects bad framing without stopping its listener', async (t) => {
  const base = await freePorts()
  const peers = { a: { host: '127.0.0.1', basePort: base }, b: { host: '127.0.0.1', basePort: base + 4 } }
  const server = await listen(peers.a.basePort + 1)
  const connection = event(server, 'connection')
  const router = new TcpCluster('b', peers, {
    bindHost: '127.0.0.1', maxFrameBytes: 8, maxQueuedBytes: 12,
  })
  const seen = watch(router)
  let socket
  let outgoing
  try {
    await router.open()
    outgoing = await timeout(connection)
    outgoing.on('error', noop)
    outgoing.resume()
    socket = await rawConnect(peers.b.basePort)
    socket.resume()
    await until(() => router._incoming.has('a') && router._outgoing.has('a'), 'both queue-test directions')
    const originalOutgoing = router._outgoing.get('a')
    t.equal(router.send('a', Buffer.alloc(8)), true)
    t.equal(router.send('a', Buffer.alloc(8)), false, 'pending bytes enforce the queue limit')
    t.ok(router.isOpen, 'a dropped outgoing frame does not stop TCP')
    const closed = event(socket, 'close')
    socket.write(Buffer.from([0, 0, 0, 9]))
    await timeout(closed)
    await until(() => seen.warnings.some((err) => err.code === Err.FRAME_CORRUPT), 'framing warning')
    t.ok(router.isOpen, 'malformed peer closes only its own connection')
    t.equal(router._outgoing.get('a'), originalOutgoing, 'malformed incoming data preserves the opposite direction')
    socket = await rawConnect(peers.b.basePort)
    socket.resume()
    socket.write(frame(Buffer.from('good')))
    await until(() => seen.messages.length === 1, 'replacement after malformed frame')
    t.deepEqual(seen.messages[0], { from: 'a', bytes: Buffer.from('good') })
    t.deepEqual(seen.errors, [])
  } finally {
    socket?.destroy()
    outgoing?.destroy()
    await router.close()
    await closeServer(server)
  }
  t.end()
})

test('TcpCluster closes earlier listeners when a later derived port is unavailable', async (t) => {
  const peers = members(await freePorts())
  const occupied = await listen(peers.zulu.basePort + 1)
  const router = new TcpCluster('zulu', peers, { bindHost: '127.0.0.1' })
  const seen = watch(router)
  try {
    const failure = await router.open().catch((err) => err)
    t.match(failure.message, /EADDRINUSE/)
    t.ok(router.isStopped)
    await timeout(router.close())
    const released = await listen(peers.zulu.basePort)
    await closeServer(released)
    t.pass('the earlier listener was released after partial startup')
    t.deepEqual(seen.errors, [], 'open rejection owns startup failure reporting')
    t.equal((await router.open().catch((err) => err)).code, Err.NODE_NOT_OPEN)
  } finally { await closeServer(occupied); await router.close() }
  t.end()
})

test('TcpCluster connection deadlines retry once per peer and shutdown cancels pending dialing', async (t) => {
  const base = await freePorts()
  const peers = { a: { host: '127.0.0.1', basePort: base },
    b: { host: '127.0.0.1', basePort: base + 4 } }
  const router = new TcpCluster('a', peers, { connectTimeout: 10, reconnectDelay: 20 })
  const seen = watch(router)
  const sockets = []
  const connect = net.Socket.prototype.connect
  // Hold the OS connect completion so the deadline is independent of network routing.
  net.Socket.prototype.connect = function () { sockets.push(this); return this }
  try {
    await router.open()
    await until(() => seen.warnings.length === 1, 'connection deadline warning')
    t.match(seen.warnings[0].message, /connect timeout/)
    t.equal(seen.warnings[0].code, Err.SEND_ERROR)
    t.ok(sockets[0].destroyed, 'expired dialing socket is destroyed')
    t.ok(router.isOpen, 'timeout is a peer failure rather than an attempt failure')
    await until(() => sockets.length === 2, 'one scheduled reconnect')
    t.equal(sockets.filter((socket) => !socket.destroyed).length, 1,
      'there is only one live connection attempt for the peer')
    await router.close()
    t.ok(sockets.every((socket) => socket.destroyed), 'shutdown aborts the outstanding dial')
    const attempts = sockets.length
    const warnings = seen.warnings.length
    await pause(40)
    t.equal(sockets.length, attempts, 'shutdown cancels future reconnect work')
    t.equal(seen.warnings.length, warnings, 'intentional abort adds no peer warning')
    t.deepEqual(seen.errors, [])
  } finally {
    net.Socket.prototype.connect = connect
    await router.close()
  }
  t.end()
})

test('UdxRouter manages private paths and waits for every named endpoint', async (t) => {
  const parent = await mkdtemp('/tmp/mft-router-parent-')
  const router = new UdxRouter(['owner/A', 'user apply'], { directory: parent })
  const seen = watch(router)
  const clients = []
  let ready = false
  router.ready().then(() => { ready = true })
  try {
    await router.open()
    const directory = dirname(router.paths['owner/A'])
    t.equal(dirname(directory), parent, 'managed directory lives inside the custom parent')
    t.equal((await lstat(directory)).mode & 0o777, 0o700, 'managed directory is private')
    clients.push(await udxClient(router.paths['owner/A']))
    clients[0].resume()
    await pause(10)
    t.equal(ready, false, 'the first endpoint cannot satisfy all-owner readiness')
    clients.push(await udxClient(router.paths['user apply']))
    const received = []
    clients[1].on('data', (bytes) => received.push(bytes))
    await timeout(router.ready())
    t.equal(ready, true)
    clients[0].write(Buffer.from([0xc1, 255]))
    router.send('user apply', Buffer.alloc(0))
    await until(() => seen.messages.length === 1 && received.length === 1, 'named UDX routing')
    t.deepEqual(seen.messages, [{ from: 'owner/A', bytes: Buffer.from([0xc1, 255]) }])
    t.deepEqual(received, [Buffer.alloc(0)])
    t.deepEqual(seen.errors, [])
    const closing = router.close()
    t.equal(router.close(), closing, 'close is single flight')
    t.equal(router.isStopped, true, 'close disables sends synchronously')
    await closing
    t.deepEqual(await readdir(parent), [], 'managed attempt directory is removed')
    t.ok((await lstat(parent)).isDirectory(), 'the supplied parent remains')
  } finally {
    for (const client of clients) { client.destroy() }
    await router.close()
    await rm(parent, { recursive: true, force: true })
  }
  t.end()
})

test('UdxRouter cleans partial startup while preserving its parent directory', async (t) => {
  const directory = await mkdtemp('/tmp/mft-router-partial-')
  const callerFile = join(directory, 'keep.txt')
  await writeFile(callerFile, 'owned by caller')
  const router = new UdxRouter(['first', 'second'], { directory })
  const seen = watch(router)
  const original = new Error('second listener failed')
  const listen = router._listen.bind(router)
  let firstServer
  router._listen = async (...args) => {
    if (firstServer) {
      t.ok(firstServer.listening, 'the first listener is bound before the second fails')
      throw original
    }
    firstServer = await listen(...args)
    return firstServer
  }
  try {
    const failure = await router.open().catch((err) => err)
    t.equal(failure, original)
    await router.close()
    t.notOk(firstServer.listening, 'cleanup closes the earlier listener')
    t.equal(await readFile(callerFile, 'utf8'), 'owned by caller')
    t.deepEqual(await readdir(directory), ['keep.txt'], 'cleanup removes only the owned attempt directory')
    t.deepEqual(seen.errors, [], 'startup failure is returned without an extra diagnostic')
  } finally { await router.close(); await rm(directory, { recursive: true, force: true }) }
  t.end()
})

test('UdxRouter rejects duplicate owners and makes established disconnect terminal', async (t) => {
  const router = new UdxRouter(['worker'])
  const seen = watch(router)
  let first
  let duplicate
  try {
    await router.open()
    first = await udxClient(router.paths.worker)
    first.resume()
    await router.ready()
    duplicate = await udxClient(router.paths.worker)
    duplicate.resume()
    await timeout(event(duplicate, 'close'))
    t.ok(router.isOpen, 'a duplicate cannot displace the accepted owner')
    first.write(Buffer.from('original'))
    await until(() => seen.messages.length === 1, 'original owner still routes')
    t.equal(seen.messages[0].bytes.toString(), 'original')
    first.destroy()
    const failure = await timeout(router.failed.catch((err) => err))
    t.equal(failure.code, Err.SEND_ERROR)
    t.ok(router.isStopped)
    t.notOk(router.isOpen)
    t.equal(seen.errors.length, 1)
    t.equal(seen.errors[0], failure)
    t.equal(await router.ready().catch((err) => err), failure)
    t.throws(() => router.send('worker', Buffer.from('late')), (err) => err === failure)
  } finally { first?.destroy(); duplicate?.destroy(); await router.close() }
  t.end()
})

test('UdxRouter overload fails its attempt while invalid arguments remain caller errors', async (t) => {
  const router = new UdxRouter(['worker'], { maxFrameBytes: 8, maxQueuedBytes: 12 })
  const seen = watch(router)
  let client
  try {
    await router.open()
    client = await udxClient(router.paths.worker)
    client.resume()
    await router.ready()
    for (const send of [() => router.send('absent', Buffer.alloc(0)),
      () => router.send('worker', 'not a buffer'), () => router.send('worker', Buffer.alloc(9))]) {
      t.throws(send, (err) => err.code === Err.ARGUMENT_ILLEGAL)
    }
    t.ok(router.isOpen)
    t.equal(router.send('worker', Buffer.alloc(8)), true)
    t.throws(() => router.send('worker', Buffer.alloc(8)), (err) => err.code === Err.SEND_ERROR)
    const failure = await router.failed.catch((err) => err)
    t.equal(failure.code, Err.SEND_ERROR)
    t.ok(router.isStopped)
    t.equal(seen.errors.length, 1)
  } finally { client?.destroy(); await router.close() }
  t.end()
})

for (const type of ['TCP', 'UDX']) {
  test(`${type} message listener failure stops before error and contains diagnostic listener throws`, async (t) => {
    const peers = type === 'TCP' ? members(await freePorts()) : null
    const router = type === 'TCP'
      ? new TcpCluster('zulu', peers, { bindHost: '127.0.0.1' }) : new UdxRouter(['worker'])
    const original = new Error(`${type} receiver failed`)
    const reported = []
    let laterMessage = false
    let socket
    router.on('message', () => { throw original })
    router.on('message', () => { laterMessage = true })
    router.on('error', () => { throw new Error('diagnostic listener failed too') })
    router.on('error', (err) => { reported.push(err); t.ok(router.isStopped, 'stopped before diagnosis') })
    try {
      await router.open()
      if (type === 'TCP') {
        socket = await rawConnect(peers.zulu.basePort)
        socket.write(frame(Buffer.from('trigger')))
      } else {
        socket = await udxClient(router.paths.worker)
        await router.ready()
        socket.write(Buffer.from('trigger'))
      }
      socket.resume()
      const failure = await timeout(router.failed.catch((err) => err))
      t.equal(failure.message, original.message)
      t.equal(failure.stack, original.stack)
      t.deepEqual(reported, [failure], 'later diagnostic listener still receives the original error')
      t.equal(laterMessage, false, 'message dispatch ends when its listener stops the helper')
    } finally { socket?.destroy(); await router.close() }
    t.end()
  })
}

test('TcpCluster warning listener failure stops retries and retains its original stack', async (t) => {
  const peers = members(await freePorts())
  const router = new TcpCluster('alpha', peers, { reconnectDelay: 10 })
  const seen = watch(router)
  const original = new Error('warning handler failed')
  router.on('warn', () => { throw original })
  try {
    await router.open()
    const failure = await timeout(router.failed.catch((err) => err))
    t.equal(failure.message, original.message)
    t.equal(failure.stack, original.stack)
    t.ok(router.isStopped)
    const warnings = seen.warnings.length
    await pause(30)
    t.equal(seen.warnings.length, warnings, 'all peer retry work was cancelled')
    t.deepEqual(seen.errors, [failure])
  } finally { await router.close() }
  t.end()
})

test('router close cancels startup and leaves normal failure promises unsettled', async (t) => {
  const parent = await mkdtemp('/tmp/mft-router-cancel-')
  const peers = members(await freePorts())
  const routers = [new UdxRouter(['one', 'two'], { directory: parent }),
    new TcpCluster('alpha', peers, { reconnectDelay: 10 })]
  try {
    for (const router of routers) {
      let failed = false
      router.failed.catch(() => { failed = true })
      const opening = router.open().catch((err) => err)
      const closing = router.close()
      t.equal(router.close(), closing)
      t.ok(router.isStopped)
      await timeout(closing)
      t.equal((await opening).code, Err.NODE_NOT_OPEN)
      t.equal(failed, false, 'intentional cancellation is not a runtime failure')
      t.equal((await router.open().catch((err) => err)).code, Err.NODE_NOT_OPEN)
    }
    t.deepEqual(await readdir(parent), [], 'cancelled UDX startup leaves no attempt directory')
  } finally {
    await Promise.all(routers.map((router) => router.close()))
    await rm(parent, { recursive: true, force: true })
  }
  t.end()
})
