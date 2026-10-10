import net from 'node:net'
import test from 'tape'
import sodium from 'libsodium-wrappers'
import { ErrorWithCode, ErrorCodes, RaftNode, tcpServer, tcpClient } from '../src/index.js'

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

test('tcp server rejects asynchronous listen errors', async (t) => {
  t.plan(5)
  const holder = await listen(net.createServer(), '0.0.0.0')
  let reported = null
  let failure = null
  try {
    await tcpServer(Buffer.alloc(32), holder.address().port, noop, (err) => {
      reported = err
    })
  } catch (err) {
    failure = err
  }
  t.ok(failure, 'listen promise rejects')
  t.ok(failure instanceof ErrorWithCode, 'normalizes the listen error')
  t.match(failure.message, /net error listen EADDRINUSE/, 'retains the listen context')
  t.match(failure.stack, /^Error: listen EADDRINUSE/,
    'the stack retains the original listen error header')
  t.equal(reported, null, 'does not report a startup error as a runtime error')
  await closeServer(holder)
})

test('tcp server preserves runtime error stacks', async (t) => {
  let reported = null
  const server = await tcpServer(Buffer.alloc(32), 0, noop, (err) => { reported = err })
  const original = new Error('server failure')
  try {
    server.emit('error', original)
    t.ok(reported instanceof ErrorWithCode, 'normalizes the runtime error')
    t.equal(reported.message, '0 net error server failure', 'retains the server context')
    t.equal(reported.stack, original.stack, 'preserves the complete original stack')
  } finally {
    await closeServer(server)
  }
})

test('tcp connection error stacks survive Raft send warnings', async (t) => {
  const original = new Error('connection failed')
  original.code = 'ECONNREFUSED'
  const connect = net.Socket.prototype.connect
  let socket = null
  net.Socket.prototype.connect = function () {
    socket = this
    queueMicrotask(() => this.emit('error', original))
    return this
  }
  const node = new RaftNode('1', ['1', '2', '3'],
    () => tcpClient(Buffer.alloc(32), 1, '127.0.0.1'), ':memory:', {
      electionTimeout: 60_000,
    })
  try {
    node.open()
    const warned = event(node, 'warn')
    node.send('2', { type: 'probe' })
    const warning = await timeout(warned, 'send warning')
    t.ok(warning instanceof ErrorWithCode, 'reports a normalized transport error')
    t.equal(warning.code, ErrorCodes.SEND_ERROR, 'retains the send error code')
    t.equal(warning.message, '(send) net error connection failed', 'retains both prefixes')
    t.equal(warning.stack, original.stack, 'preserves the original stack through both wrappers')
    t.ok(socket.destroyed, 'closes the failed socket')
  } finally {
    net.Socket.prototype.connect = connect
    node.close()
  }
})

test('tcp client destroys the connection after a decrypt error', async (t) => {
  t.plan(9)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let accept
  const accepted = new Promise((res) => { accept = res })
  const server = await listen(net.createServer((sock) => {
    sock.on('error', noop)
    sock.resume()
    accept(sock)
  }))
  const [pack, peer] = await Promise.all([
    tcpClient(key, server.address().port, '127.0.0.1'),
    accepted,
  ])
  const clientEvents = []
  let destroyedOnError = null
  const errored = new Promise((res) => {
    pack.once('error', (err) => {
      clientEvents.push('error')
      destroyedOnError = pack.destroyed
      res(err)
    })
  })
  const packClosed = new Promise((res) => {
    pack.once('close', () => {
      clientEvents.push('close')
      res()
    })
  })
  const peerClosed = event(peer, 'close')
  const header = Buffer.alloc(sodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES)
  const length = Buffer.alloc(4)
  length.writeUInt32BE(sodium.crypto_secretstream_xchacha20poly1305_ABYTES)
  peer.write(Buffer.concat([
    header,
    length,
    Buffer.alloc(sodium.crypto_secretstream_xchacha20poly1305_ABYTES),
  ]))
  const err = await timeout(errored, 'client error')
  await timeout(Promise.all([packClosed, peerClosed]), 'connection close')
  t.equal(err.message, 'decrypt error stream decrypt error', 'forwards the decrypt error')
  t.match(err.stack, /^Error: stream decrypt error\n/, 'preserves the original error header')
  t.match(err.stack, /at DecryptingStream\._drain/, 'preserves the decrypt failure location')
  t.notOk(destroyedOnError, 'emits the error before destroying the public stream')
  t.deepEqual(clientEvents, ['error', 'close'], 'closes after reporting the error')
  t.ok(pack.destroyed, 'destroys the public stream')
  t.ok(peer.destroyed, 'destroys the socket')
  t.equal(pack.eventNames().length, 0, 'removes all public stream listeners')
  t.equal(pack._readableState.pipes.length, 0, 'disconnects the public stream pipeline')
  await closeServer(server)
})

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

test('tcp client clears the connection when its public stream is destroyed', async (t) => {
  t.plan(4)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let accept
  const accepted = new Promise((res) => { accept = res })
  const server = await listen(net.createServer((sock) => {
    sock.on('error', noop)
    sock.resume()
    accept(sock)
  }))
  const [pack, peer] = await Promise.all([
    tcpClient(key, server.address().port, '127.0.0.1'),
    accepted,
  ])
  const clientEvents = []
  pack.on('error', noop)
  const packClosed = new Promise((res) => {
    pack.once('close', () => {
      clientEvents.push('close')
      res()
    })
  })
  const peerClosed = closed(peer)
  pack.destroy()

  await timeout(Promise.all([packClosed, peerClosed]), 'connection close')
  t.deepEqual(clientEvents, ['close'], 'notifies the user when the public stream closes')
  t.equal(pack.eventNames().length, 0, 'removes all public stream listeners')
  t.equal(pack._readableState.pipes.length, 0, 'disconnects the public stream pipeline')
  t.ok(peer.destroyed, 'destroys the socket')
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

test('tcp server reports an encrypted request callback failure and closes its connection', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const original = new Error('request callback failed')
  const stack = original.stack
  let response
  let report
  let destroyedOnError = null
  const reported = new Promise((res) => { report = res })
  const server = await tcpServer(key, 0, (pack) => {
    response = pack
    throw original
  }, (err) => {
    destroyedOnError = response.destroyed
    report(err)
  })
  const client = await tcpClient(key, server.address().port, '127.0.0.1')
  client.on('error', noop)
  try {
    const clientClosed = closed(client)
    client.write({ message: 'hello' })
    const failure = await timeout(reported, 'request callback error')
    await timeout(clientClosed, 'failed request connection close')
    t.equal(failure, original, 'reports the original callback error')
    t.equal(failure.stack, stack, 'preserves its original stack')
    t.equal(destroyedOnError, false, 'reports the error before closing the response stream')
    t.ok(client.destroyed, 'closes the client connection')
  } finally {
    client.destroy()
    await closeServer(server)
  }
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
      client.write({ message: 'hello' })
      const pack = await timeout(received, 'server message')
      const decrypt = socket._readableState.pipes[0]
      const unpack = decrypt._readableState.pipes[0]
      const encrypt = pack._readableState.pipes[0]
      const streams = { socket, decrypt, unpack, pack, encrypt }
      const clientClosed = closed(client)
      const stream = streams[source] || socket
      if (source === 'throwing callback') {
        t.throws(() => stream.emit('error', original), (err) => err === reporterFailure,
          'propagates an error callback failure after cleanup')
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
  client.write({ message: 'hello' })

  const pack = await timeout(received, 'server message')
  const clientClosed = closed(client)
  pack.destroy()
  await timeout(clientClosed, 'connection close')

  t.ok(pack.destroyed, 'destroys the response stream')
  t.equal(pack.eventNames().length, 0, 'removes all response stream listeners')
  t.equal(pack._readableState.pipes.length, 0, 'disconnects the response stream pipeline')
  await closeServer(server)
})

test('tcp transports structured outcome arrays in the enclosing message', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let receive
  const received = new Promise((res) => { receive = res })
  const server = await tcpServer(key, 0, (pack, msg) => {
    receive(msg)
    pack.write(msg)
  }, noop)
  let echo
  const echoed = new Promise((res) => { echo = res })
  const client = await tcpClient(
    key,
    server.address().port,
    '127.0.0.1',
    echo,
  )
  client.on('error', noop)
  const message = {
    type: 'ack',
    cid: 'structured-outcomes',
    term: 3n,
    cmdSeq: 9n,
    results: [
      [0, { answer: 42, tags: ['raw', 'value'] }],
      [1, 'bad input', 17, null, 'TypeError: bad input\n    at remoteApply (/app.js:10:3)'],
    ],
  }
  client.write(message)

  const [serverMessage, clientMessage] = await timeout(Promise.all([
    received,
    echoed,
  ]), 'structured outcome round trip')
  t.ok(Array.isArray(serverMessage.results),
    'the server receives results as an array')
  t.deepEqual(serverMessage.results[0], message.results[0],
    'the fulfilled tuple retains its raw structured value')
  t.equal(clientMessage.results[1][4], message.results[1][4],
    'the original stack string survives TCP serialization')
  t.deepEqual(clientMessage, message,
    'the complete outcome array survives a TCP round trip')

  const clientClosed = closed(client)
  client.destroy()
  await timeout(clientClosed, 'structured outcome client close')
  await closeServer(server)
})

test('tcp transports Raft records with separate bigint terms and Buffer payloads', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let receive
  const received = new Promise((res) => { receive = res })
  const server = await tcpServer(key, 0, (pack, msg) => {
    receive(msg)
    pack.write(msg)
  }, noop)
  let echo
  const echoed = new Promise((res) => { echo = res })
  const client = await tcpClient(key, server.address().port, '127.0.0.1', echo)
  client.on('error', noop)
  try {
    const message = {
      type: 'append',
      cid: 'separate-terms',
      term: 9_223_372_036_854_775_807n,
      termP: -1n,
      seqP: -1n,
      commitSeq: 2n,
      data: [
        { term: 0n, entry: Buffer.alloc(0) },
        { term: 1n, entry: Buffer.from([255]) },
        { term: 9_223_372_036_854_775_807n, entry: Buffer.from([0, 255, 0, 127]) },
      ],
    }
    client.write(message)
    const replies = await timeout(Promise.all([received, echoed]), 'Raft record round trip')
    for (const [index, reply] of replies.entries()) {
      const side = index === 0 ? 'server' : 'client'
      t.deepEqual(reply, message, `${side}: preserves every envelope and record field`)
      t.ok(reply.data.every((record) => typeof record.term === 'bigint'),
        `${side}: preserves bigint terms including zero and the signed maximum`)
      t.ok(reply.data.every((record) => Buffer.isBuffer(record.entry)),
        `${side}: preserves empty, short, and binary payloads as Buffers`)
    }
  } finally {
    const clientClosed = closed(client)
    client.destroy()
    await timeout(clientClosed, 'Raft record client close')
    await closeServer(server)
  }
})
