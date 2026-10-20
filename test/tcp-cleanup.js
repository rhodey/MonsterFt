import net from 'node:net'
import test from 'tape'
import sodium from 'libsodium-wrappers'
import { tcpServer, tcpClient } from '../src/index.js'
import { encodeFrame } from '../src/wire.js'

const noop = () => {}

function listen(server, host='127.0.0.1') {
  return new Promise((res, rej) => {
    const onError = (err) => rej(err)
    server.once('error', onError)
    server.listen(0, host, () => {
      server.removeListener('error', onError)
      res(server)
    })
  })
}

function closeServer(server) {
  if (!server.listening) { return Promise.resolve() }
  return new Promise((res, rej) => {
    server.close((err) => err ? rej(err) : res())
  })
}

function event(emitter, name) {
  return new Promise((res) => emitter.once(name, res))
}

function closed(stream) {
  return stream.destroyed ? Promise.resolve() : event(stream, 'close')
}

function timeout(promise, name) {
  return new Promise((res, rej) => {
    const timer = setTimeout(() => rej(new Error(`${name} timeout`)), 1_000)
    promise.then((value) => {
      clearTimeout(timer)
      res(value)
    }, (err) => {
      clearTimeout(timer)
      rej(err)
    })
  })
}

test('tcp client clears the connection after the socket closes', async (t) => {
  t.plan(3)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let accept
  const accepted = new Promise((res) => { accept = res })
  const server = await listen(net.createServer((sock) => {
    sock.on('error', noop)
    accept(sock)
  }))
  const [pack, peer] = await Promise.all([
    tcpClient(key, server.address().port, '127.0.0.1'),
    accepted,
  ])
  const clientEvents = []
  pack.on('error', (err) => clientEvents.push(err.message))
  const packClosed = new Promise((res) => {
    pack.once('close', () => {
      clientEvents.push('close')
      res()
    })
  })
  const { header } = sodium.crypto_secretstream_xchacha20poly1305_init_push(key)
  peer.end(Buffer.from(header))

  await timeout(packClosed, 'client close')
  t.deepEqual(clientEvents, ['close'], 'notifies the user when the socket closes')
  t.equal(pack.eventNames().length, 0, 'removes all public stream listeners')
  t.equal(pack._readableState.pipes.length, 0, 'disconnects the public stream pipeline')
  await closeServer(server)
})

test('tcp client rejects and closes when connection setup throws', async (t) => {
  t.plan(2)
  await sodium.ready
  let accept
  const accepted = new Promise((res) => { accept = res })
  const server = await listen(net.createServer((sock) => {
    sock.on('error', noop)
    sock.resume()
    accept(sock)
  }))
  const candidate = tcpClient(Buffer.alloc(1), server.address().port, '127.0.0.1')
    .then(() => null, (err) => err)
  const [failure, peer] = await Promise.all([candidate, accepted])
  await timeout(closed(peer), 'peer close')
  t.equal(failure?.message, 'invalid key length', 'rejects the client promise')
  t.ok(peer.destroyed, 'closes the connected socket')
  await closeServer(server)
})

test('tcp server closes a connection when setup throws', async (t) => {
  t.plan(2)
  await sodium.ready
  let report
  const reported = new Promise((res) => { report = res })
  const server = await tcpServer(Buffer.alloc(1), 0, noop, report)
  const client = net.connect(server.address().port, '127.0.0.1')
  client.on('error', noop)
  client.resume()
  const failure = await timeout(reported, 'server setup error')
  await timeout(closed(client), 'client close')
  t.equal(failure.message, 'invalid key length', 'reports the setup error')
  t.ok(client.destroyed, 'closes the accepted socket')
  await closeServer(server)
})

test('tcp server reports connection errors and cleans up even if the error callback throws', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  for (const source of ['socket', 'decrypt', 'unpack', 'pack', 'encrypt', 'throwing callback']) {
    const original = new Error(`${source} failed`)
    const reporterFailure = new Error('error callback failed')
    const reported = []
    let receive
    const received = new Promise((res) => { receive = res })
    const server = await tcpServer(key, 0, (pack) => receive(pack), (err) => {
      reported.push(err)
      if (source === 'throwing callback') { throw reporterFailure }
    })
    const accepted = event(server, 'connection')
    const client = await tcpClient(key, server.address().port, '127.0.0.1')
    client.on('error', noop)
    try {
      const socket = await timeout(accepted, 'server connection')
      client.write(encodeFrame({ message: 'hello' }))
      const pack = await timeout(received, 'server message')
      const decrypt = socket._readableState.pipes[0]
      const unpack = decrypt._readableState.pipes[0]
      const encrypt = pack._readableState.pipes[0]
      const streams = { socket, decrypt, unpack, pack, encrypt }
      const clientClosed = closed(client)
      const stream = streams[source] || socket
      if (source === 'throwing callback') {
        // Callback exceptions may escape; teardown must succeed either way.
        try { stream.emit('error', original) } catch (err) {
          if (err !== reporterFailure) { throw err }
        }
      } else {
        stream.emit('error', original)
      }
      await timeout(clientClosed, `${source} connection close`)
      t.deepEqual(reported, [original], `${source}: reports the original error once`)
      t.ok(Object.values(streams).every((item) => item.destroyed),
        `${source}: destroys all connection streams`)
      t.ok(Object.values(streams).every((item) => item.eventNames().length === 0),
        `${source}: removes all connection listeners`)
      t.ok(Object.values(streams).every((item) => item._readableState.pipes.length === 0),
        `${source}: disconnects all connection pipes`)
    } finally {
      client.destroy()
      await closeServer(server)
    }
  }
})

test('tcp server clears the connection when a response stream closes', async (t) => {
  t.plan(3)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let receive
  const received = new Promise((res) => { receive = res })
  const server = await tcpServer(key, 0, (pack) => receive(pack), noop)
  const client = await tcpClient(key, server.address().port, '127.0.0.1')
  client.on('error', noop)
  client.write(encodeFrame({ message: 'hello' }))

  const pack = await timeout(received, 'server message')
  const clientClosed = closed(client)
  pack.destroy()
  await timeout(clientClosed, 'connection close')

  t.ok(pack.destroyed, 'destroys the response stream')
  t.equal(pack.eventNames().length, 0, 'removes all response stream listeners')
  t.equal(pack._readableState.pipes.length, 0, 'disconnects the response stream pipeline')
  await closeServer(server)
})

