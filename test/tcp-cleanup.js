import net from 'node:net'
import dns from 'node:dns'
import test from 'tape'
import { tcpServer, tcpClient } from '../src/net/tcp.js'
import { encodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const noop = () => {}
const event = (target, name) => new Promise((resolve) => target.once(name, resolve))
const closed = (stream) => stream.closed ? Promise.resolve() : event(stream, 'close')
const closeServer = (server) => !server.listening ? Promise.resolve() :
  new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve()))

function timeout(work, ms = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('TCP cleanup timeout')), ms)
    work.then((value) => { clearTimeout(timer); resolve(value) },
      (err) => { clearTimeout(timer); reject(err) })
  })
}

async function rawServer() {
  let accept
  const accepted = new Promise((resolve) => { accept = resolve })
  const server = net.createServer((socket) => { socket.on('error', noop); socket.resume(); accept(socket) })
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  return { server, accepted }
}

test('TCP connection shutdown preserves external close and error listeners', async (t) => {
  const { server, accepted } = await rawServer()
  const client = await tcpClient(server.address().port, '127.0.0.1')
  const peer = await accepted
  const errors = []
  const errorListener = (err) => errors.push(err)
  let closes = 0
  client.on('error', errorListener)
  client.on('close', () => { closes++ })
  try {
    const closing = closed(client)
    peer.end()
    await timeout(closing)
    t.equal(closes, 1)
    t.deepEqual(errors, [])
    t.ok(client.listeners('error').includes(errorListener), 'does not remove caller listeners')
    t.ok(client.socket.destroyed)
  } finally { client.destroy(); peer.destroy(); await closeServer(server) }
  t.end()
})

for (const source of ['socket', 'frames']) {
  test(`TCP ${source} errors report once and close both sides`, async (t) => {
    let accept
    const accepted = new Promise((resolve) => { accept = resolve })
    const failures = []
    const original = new Error(`${source} failed`)
    const server = await tcpServer(0, noop, (err, connection) => failures.push({ err, connection }), {
      onConnection: accept,
    })
    const client = await tcpClient(server.address().port, '127.0.0.1')
    const connection = await accepted
    try {
      let socketClosed = false
      connection.socket.on('close', () => { socketClosed = true })
      const closing = closed(client)
      const sourceStream = source === 'socket' ? connection.socket : connection
      sourceStream.emit('error', original)
      await timeout(closing)
      t.equal(failures.length, 1)
      t.equal(failures[0].err.stack, original.stack)
      t.equal(failures[0].connection, connection, 'error identifies its connection')
      t.ok(connection.destroyed)
      t.ok(socketClosed, 'external socket close watcher runs')
    } finally { client.destroy(); connection.destroy(); await closeServer(server) }
    t.end()
  })
}

test('TCP reply stream destruction closes its socket and the peer', async (t) => {
  let receive
  const received = new Promise((resolve) => { receive = resolve })
  const server = await tcpServer(0, receive, noop)
  const client = await tcpClient(server.address().port, '127.0.0.1')
  try {
    client.write(encodeFrame({ message: 'hello' }))
    const connection = await timeout(received)
    const closing = closed(client)
    connection.destroy()
    await timeout(closing)
    t.ok(connection.socket.destroyed)
    t.ok(client.destroyed)
  } finally { client.destroy(); await closeServer(server) }
  t.end()
})

test('TCP callback and malformed-input failures retain useful stacks', async (t) => {
  for (const source of ['accept', 'receive', 'length']) {
    const original = new Error(`${source} failed`)
    let report
    const reported = new Promise((resolve) => { report = resolve })
    const server = await tcpServer(0, () => { throw original }, report, {
      maxFrameBytes: 10,
      onConnection: source === 'accept' ? () => { throw original } : noop,
    })
    const socket = net.connect(server.address().port, '127.0.0.1')
    socket.on('error', noop)
    socket.resume()
    try {
      await event(socket, 'connect')
      const closing = closed(socket)
      if (source === 'length') socket.write(Buffer.from([255, 255, 255, 255]))
      else if (source === 'receive') socket.write(Buffer.from([0, 0, 0, 1, 9]))
      const err = await timeout(reported)
      await timeout(closing)
      if (source === 'length') {
        t.equal(err.code, Err.FRAME_CORRUPT)
        t.match(err.message, /maxFrameBytes/)
      } else {
        t.equal(err.message, original.message)
        t.equal(err.stack, original.stack)
      }
      t.ok(socket.destroyed)
    } finally { socket.destroy(); await closeServer(server) }
  }
  t.end()
})

test('TCP dial cancellation rejects before and during a pending connection', async (t) => {
  const originalConnect = net.Socket.prototype.connect
  const sockets = []
  net.Socket.prototype.connect = function () { sockets.push(this); return this }
  try {
    for (const before of [true, false]) {
      const controller = new AbortController()
      const reason = new Error('cancel dial')
      if (before) controller.abort(reason)
      const waiting = tcpClient(12345, '127.0.0.1', noop, { signal: controller.signal })
      if (!before) controller.abort(reason)
      const err = await waiting.then(() => null, (err) => err)
      t.equal(err.stack, reason.stack)
      t.match(err.message, /cancel dial/)
    }
    t.ok(sockets.every((socket) => socket.closed), 'every launched dial is closed before rejection')
  } finally { net.Socket.prototype.connect = originalConnect }
  t.end()
})

test('TCP connect timeout cancels a dial and never waits for a greeting', async (t) => {
  const originalConnect = net.Socket.prototype.connect
  let socket
  net.Socket.prototype.connect = function () { socket = this; return this }
  try {
    const err = await tcpClient(12345, '127.0.0.1', noop, { connectTimeout: 5 })
      .then(() => null, (err) => err)
    t.equal(err.code, Err.SEND_ERROR)
    t.match(err.message, /connect timeout/)
    t.ok(socket.closed, 'timeout waits for actual socket close')
  } finally { net.Socket.prototype.connect = originalConnect }
  const { server, accepted } = await rawServer()
  const client = await tcpClient(server.address().port, '127.0.0.1', noop, { connectTimeout: 5 })
  const peer = await accepted
  try {
    await new Promise((resolve) => setTimeout(resolve, 20))
    t.notOk(client.destroyed, 'connected silent peer has no greeting deadline')
  } finally { client.destroy(); peer.destroy(); await closeServer(server) }
  t.end()
})

test('TCP pre-aborted listener never opens a port', async (t) => {
  const controller = new AbortController()
  controller.abort(new Error('stop listener'))
  const err = await tcpServer(0, noop, noop, { signal: controller.signal }).then(() => null, (err) => err)
  t.match(err.message, /stop listener/)
  t.end()
})

test('TCP outbound wire contains no greeting, encryption header, or additional codec', async (t) => {
  const { server, accepted } = await rawServer()
  const client = await tcpClient(server.address().port, '127.0.0.1')
  const peer = await accepted
  const chunks = []
  peer.on('data', (chunk) => chunks.push(chunk))
  try {
    const empty = Buffer.alloc(0)
    const frame = Buffer.from([0, 255, 8])
    const ended = event(peer, 'end')
    client.write(empty)
    client.end(frame)
    await timeout(ended)
    t.deepEqual(Buffer.concat(chunks), Buffer.from([0, 0, 0, 0, 0, 0, 0, 3, 0, 255, 8]))
  } finally { client.destroy(); peer.destroy(); await closeServer(server) }
  t.end()
})


test('TCP cancelled listen cannot bind after delayed address resolution', async (t) => {
  const lookup = dns.lookup
  const listen = net.Server.prototype.listen
  let resolveAddress
  let listener
  dns.lookup = (host, opts, callback) => { resolveAddress = callback }
  net.Server.prototype.listen = function (...args) { listener = this; return listen.apply(this, args) }
  try {
    const controller = new AbortController()
    const opening = tcpServer(0, noop, noop, { host: 'delayed.local', signal: controller.signal })
    let closed = false
    listener.on('close', () => { closed = true })
    controller.abort(new Error('cancel resolving listener'))
    const failure = await opening.then(() => null, (err) => err)
    t.match(failure.message, /cancel resolving listener/)
    t.ok(closed, 'startup rejection follows listener cleanup')
    resolveAddress(null, [{ address: '127.0.0.1', family: 4 }])
    await new Promise((resolve) => setImmediate(resolve))
    t.notOk(listener.listening, 'late DNS result cannot reopen the listener')
  } finally {
    dns.lookup = lookup
    net.Server.prototype.listen = listen
  }
  t.end()
})
