import test from 'tape'
import net from 'node:net'
import sodium from 'libsodium-wrappers'
import { PackrStream } from 'msgpackr'
import { tcpServer, tcpClient } from '../src/tcp.js'
import { EncryptingStream } from '../src/stream.js'
import { encodeFrame, decodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const noop = () => {}
const event = (target, name) => new Promise((resolve) => target.once(name, resolve))
const closed = (stream) => stream.destroyed ? Promise.resolve() : event(stream, 'close')
const closeServer = (server) => !server.listening ? Promise.resolve() :
  new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve()))

function timeout(work, ms = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('TCP test timeout')), ms)
    work.then((value) => { clearTimeout(timer); resolve(value) },
      (err) => { clearTimeout(timer); reject(err) })
  })
}

test('TCP transports exact checksummed Buffers including nested Raft and Apply records', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const failures = []
  const incoming = []
  const server = await tcpServer(key, 0, (reply, frame) => {
    incoming.push(frame)
    reply.write(frame)
  }, (err) => failures.push(err))
  let receive
  const client = await tcpClient(key, server.address().port, '127.0.0.1', (frame) => receive(frame))
  client.on('error', (err) => failures.push(err))
  try {
    const messages = [
      { type: 'append', term: 9_223_372_036_854_775_807n, seqP: -1n, data: [
        { term: 0n, entry: Buffer.alloc(0) },
        { term: 3n, entry: Buffer.from([0, 255, 128]) },
      ] },
      { type: 'ack', cid: 'apply', tickCount: 83n, receipt: {
        id: 1n, seq: 15n, entry_hash: Buffer.alloc(32, 9), local_digest: Buffer.alloc(32, 4),
      }, outcomes: [[0, { answer: 42 }], [1, 'failed', Err.APPLY_ERROR, 1555,
        'Error: failed\n    at userApply (/app.js:15:2)']] },
      { type: 'err', term: 3n, cid: 'hint', code: Err.REPL_BACKTRACK, logSeq: -1n },
      { type: 'large', data: Buffer.alloc(256 * 1024, 0xff) },
    ]
    for (const message of messages) {
      const frame = encodeFrame(message)
      const echoed = new Promise((resolve) => { receive = resolve })
      client.write(frame)
      const result = await timeout(echoed)
      t.ok(Buffer.isBuffer(incoming.at(-1)), 'server receives a Buffer')
      t.ok(Buffer.isBuffer(result), 'client receives a Buffer')
      t.deepEqual(incoming.at(-1), frame, 'request preserves the complete frame byte for byte')
      t.deepEqual(result, frame, 'reply preserves the complete frame byte for byte')
      t.deepEqual(decodeFrame(result), message, 'checksums and nested bigint/Buffer values remain valid')
    }
    t.deepEqual(failures, [])
  } finally {
    const closing = closed(client)
    client.destroy()
    await timeout(closing)
    await closeServer(server)
  }
  t.end()
})

for (const sender of ['client', 'server']) {
  test(`TCP ${sender} end flushes queued frames before closing`, async (t) => {
    await sodium.ready
    const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
    const payload = Buffer.alloc(16 * 1024 * 1024, 0xc5)
    payload.write('final payload bytes', payload.length - 19)
    const frames = [encodeFrame({ order: 1 }), encodeFrame({ order: 2, payload })]
    const received = []
    const failures = []
    let peer
    let peerClosing
    const server = await tcpServer(key, 0, (reply, frame) => {
      if (sender === 'client') received.push(frame)
      else {
        reply.write(frames[0])
        reply.end(frames[1])
      }
    }, (err) => failures.push(err))
    server.on('connection', (socket) => {
      peer = socket
      peerClosing = event(socket, 'close')
    })
    const client = await tcpClient(key, server.address().port, '127.0.0.1',
      (frame) => received.push(frame))
    client.on('error', (err) => failures.push(err))
    try {
      const closing = closed(client)
      if (sender === 'client') {
        client.write(frames[0])
        client.end(frames[1])
      } else client.write(encodeFrame({ type: 'request' }))
      await timeout(closing, 10000)
      await timeout(peerClosing)
      t.equal(received.length, frames.length, 'all queued frames arrive before closure')
      for (let index = 0; index < received.length; index++) {
        t.ok(received[index].equals(frames[index]), 'frame bytes and order are preserved')
        const message = decodeFrame(received[index])
        t.equal(message.order, index + 1, 'frame checksum and MessagePack payload are valid')
        if (message.payload) t.ok(message.payload.equals(payload), 'the complete final payload arrives')
      }
      t.ok(client.destroyed, 'client closes after the send finishes')
      t.ok(peer.destroyed, 'server socket closes after the send finishes')
      t.deepEqual(failures, [], 'orderly shutdown emits no errors')
    } finally {
      client.destroy()
      peer?.destroy()
      await closeServer(server)
    }
    t.end()
  })
}

test('TCP explicit destroy aborts pending output', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let accept
  const accepted = new Promise((resolve) => { accept = resolve })
  const server = net.createServer((socket) => {
    socket.on('error', noop)
    socket.pause()
    accept(socket)
  })
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const client = await tcpClient(key, server.address().port, '127.0.0.1')
  client.on('error', noop)
  const peer = await accepted
  try {
    let received = 0
    peer.on('data', (chunk) => { received += chunk.length })
    const peerClosing = closed(peer)
    const frame = encodeFrame({ payload: Buffer.alloc(16 * 1024 * 1024) })
    client.write(frame)
    client.write(frame)
    const closing = closed(client)
    client.destroy()
    await timeout(closing)
    t.ok(client.destroyed, 'explicit destroy closes without waiting for the peer')
    t.notOk(client.writableFinished, 'destroy does not finish the pending writes')
    peer.resume()
    await timeout(peerClosing)
    t.ok(received < frame.length * 2, 'pending output is discarded by explicit destroy')
  } finally {
    client.destroy()
    peer.destroy()
    await closeServer(server)
  }
  t.end()
})

test('TCP rejects non-Buffer writes and closes the connection', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const server = await tcpServer(key, 0, () => t.fail('invalid request reached server'), noop)
  const client = await tcpClient(key, server.address().port, '127.0.0.1')
  try {
    const failed = event(client, 'error')
    const closing = closed(client)
    client.write({ type: 'not a Buffer' })
    const err = await timeout(failed)
    await timeout(closing)
    t.equal(err.code, Err.ARGUMENT_ILLEGAL)
    t.equal(err.message, 'TCP message must be a Buffer')
    t.ok(client.destroyed)
  } finally {
    client.destroy()
    await closeServer(server)
  }
  t.end()
})

test('TCP closes on error without a caller error listener', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const server = await tcpServer(key, 0, noop, noop)
  const client = await tcpClient(key, server.address().port, '127.0.0.1')
  try {
    const closing = closed(client)
    client.write({ type: 'not a Buffer' })
    await timeout(closing)
    t.ok(client.destroyed, 'client closes without a caller error listener')
    t.equal(client.eventNames().length, 0, 'cleanup removes client listeners')
  } finally {
    client.destroy()
    await closeServer(server)
  }
  t.end()
})

test('TCP reports callback errors before closing and preserves their stack', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const original = new Error('receiver failed')
  let output
  let report
  let closedWhenReported
  const reported = new Promise((resolve) => { report = resolve })
  const server = await tcpServer(key, 0, (reply) => {
    output = reply
    throw original
  }, (err) => {
    closedWhenReported = output.destroyed
    report(err)
  })
  const client = await tcpClient(key, server.address().port, '127.0.0.1')
  client.on('error', noop)
  try {
    const closing = closed(client)
    client.write(encodeFrame({ type: 'test' }))
    const err = await timeout(reported)
    await timeout(closing)
    t.equal(err, original)
    t.equal(err.stack, original.stack)
    t.equal(closedWhenReported, false)
    t.ok(output.destroyed)
    t.ok(client.destroyed)
  } finally {
    client.destroy()
    await closeServer(server)
  }
  t.end()
})

test('TCP server rejects listen failures and preserves runtime error stacks', async (t) => {
  const holder = net.createServer()
  await new Promise((resolve, reject) => {
    holder.once('error', reject)
    holder.listen(0, '0.0.0.0', resolve)
  })
  try {
    const err = await tcpServer(Buffer.alloc(32), holder.address().port, noop, noop)
      .then(() => null, (error) => error)
    t.ok(err instanceof Err.ErrorWithCode)
    t.match(err.message, /listen EADDRINUSE/)
    t.match(err.stack, /^Error: listen EADDRINUSE/)
  } finally { await closeServer(holder) }
  let reported
  const server = await tcpServer(Buffer.alloc(32), 0, noop, (err) => { reported = err })
  try {
    const original = new Error('server runtime failure')
    server.emit('error', original)
    t.equal(reported.stack, original.stack)
    t.match(reported.message, /net error server runtime failure/)
  } finally { await closeServer(server) }
  t.end()
})

test('TCP client closes after decryption failure', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let accept
  const accepted = new Promise((resolve) => { accept = resolve })
  const server = net.createServer((socket) => {
    socket.on('error', noop)
    socket.resume()
    accept(socket)
  })
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const client = await tcpClient(key, server.address().port, '127.0.0.1')
  const peer = await accepted
  try {
    const failed = event(client, 'error')
    const closing = closed(client)
    const length = Buffer.alloc(4)
    length.writeUInt32BE(sodium.crypto_secretstream_xchacha20poly1305_ABYTES)
    peer.write(Buffer.concat([
      Buffer.alloc(sodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES), length,
      Buffer.alloc(sodium.crypto_secretstream_xchacha20poly1305_ABYTES),
    ]))
    const err = await timeout(failed)
    await timeout(closing)
    t.match(err.message, /decrypt error/)
    t.match(err.stack, /DecryptingStream/)
    t.equal(client.eventNames().length, 0)
    t.equal(client._readableState.pipes.length, 0)
  } finally {
    client.destroy()
    peer.destroy()
    await closeServer(server)
  }
  t.end()
})

test('TCP server rejects a decoded non-Buffer payload through its error callback', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let report
  const reported = new Promise((resolve) => { report = resolve })
  const server = await tcpServer(key, 0, () => t.fail('invalid frame reached message callback'), report)
  const socket = net.connect(server.address().port, '127.0.0.1')
  socket.on('error', noop)
  socket.resume()
  const pack = new PackrStream({ useRecords: false })
  const encrypt = new EncryptingStream(key)
  pack.on('error', noop)
  encrypt.on('error', noop)
  try {
    const closing = closed(socket)
    pack.pipe(encrypt).pipe(socket)
    pack.write({ invalid: 'outer MessagePack value' })
    const err = await timeout(reported)
    await timeout(closing)
    t.equal(err.code, Err.RPC_ILLEGAL)
    t.equal(err.message, 'TCP message must be a Buffer')
    t.ok(socket.destroyed)
  } finally {
    pack.destroy()
    encrypt.destroy()
    socket.destroy()
    await closeServer(server)
  }
  t.end()
})

test('TCP client rejects a decoded non-Buffer payload through its error event', async (t) => {
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  let send
  let peer
  let pack
  let encrypt
  let accept
  const accepted = new Promise((resolve) => { accept = resolve })
  const server = net.createServer((socket) => {
    peer = socket
    socket.on('error', noop)
    socket.resume()
    pack = new PackrStream({ useRecords: false })
    encrypt = new EncryptingStream(key)
    pack.on('error', noop)
    encrypt.on('error', noop)
    pack.pipe(encrypt).pipe(socket)
    send = () => pack.write({ invalid: 'outer MessagePack value' })
    accept()
  })
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const client = await tcpClient(key, server.address().port, '127.0.0.1',
    () => t.fail('invalid frame reached message callback'))
  try {
    await timeout(accepted)
    const failed = event(client, 'error')
    const closing = closed(client)
    send()
    const err = await timeout(failed)
    await timeout(closing)
    t.equal(err.code, Err.RPC_ILLEGAL)
    t.equal(err.message, 'TCP message must be a Buffer')
    t.ok(client.destroyed)
  } finally {
    client.destroy()
    pack?.destroy()
    encrypt?.destroy()
    peer?.destroy()
    await closeServer(server)
  }
  t.end()
})
