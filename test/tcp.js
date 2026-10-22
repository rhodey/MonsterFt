import test from 'tape'
import net from 'node:net'
import { tcpServer, tcpClient } from '../src/net/tcp.js'
import { encodeFrame, decodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const noop = () => {}
const event = (target, name) => new Promise((resolve) => target.once(name, resolve))
const closed = (stream) => stream.destroyed ? Promise.resolve() : event(stream, 'close')
const closeServer = (server) => !server.listening ? Promise.resolve() :
  new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve()))
const immediate = () => new Promise((resolve) => setImmediate(resolve))

function framed(frames) {
  return Buffer.concat(frames.flatMap((frame) => {
    const header = Buffer.alloc(4)
    header.writeUInt32BE(frame.length)
    return [header, frame]
  }))
}

function timeout(work, ms = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('TCP test timeout')), ms)
    work.then((value) => { clearTimeout(timer); resolve(value) },
      (err) => { clearTimeout(timer); reject(err) })
  })
}

test('TCP transports exact checksummed Buffers including nested Raft and Apply records', async (t) => {
  const failures = []
  const incoming = []
  const server = await tcpServer(0, (reply, frame) => {
    incoming.push(frame)
    reply.write(frame)
  }, (err) => failures.push(err))
  let receive
  const client = await tcpClient(server.address().port, '127.0.0.1', (frame) => receive(frame))
  client.on('error', (err) => failures.push(err))
  try {
    const messages = [
      { type: 'append', term: 9_223_372_036_854_775_807n, seqP: -1n, data: [
        { term: 0n, entry: Buffer.alloc(0) },
        { term: 3n, entry: Buffer.from([0, 255, 128]) },
      ] },
      { type: 'ack', cid: 'apply', tickCount: 83n, receipt: {
        id: 1n, seq: 15n, entry_hash: Buffer.alloc(16, 9), local_digest: Buffer.alloc(16, 4),
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
    const payload = Buffer.alloc(16 * 1024 * 1024, 0xc5)
    payload.write('final payload bytes', payload.length - 19)
    const frames = [encodeFrame({ order: 1 }), encodeFrame({ order: 2, payload })]
    const received = []
    const failures = []
    let peer
    let peerClosing
    const server = await tcpServer(0, (reply, frame) => {
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
    const client = await tcpClient(server.address().port, '127.0.0.1',
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
  const client = await tcpClient(server.address().port, '127.0.0.1')
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
  const server = await tcpServer(0, () => t.fail('invalid request reached server'), noop)
  const client = await tcpClient(server.address().port, '127.0.0.1')
  try {
    const failed = event(client, 'error')
    const closing = closed(client)
    client.write({ type: 'not a Buffer' })
    const err = await timeout(failed)
    await timeout(closing)
    t.equal(err.code, Err.ARGUMENT_ILLEGAL)
    t.equal(err.message, 'message must be a Buffer')
    t.ok(client.destroyed)
  } finally {
    client.destroy()
    await closeServer(server)
  }
  t.end()
})

test('TCP closes on error without a caller error listener', async (t) => {
  const server = await tcpServer(0, noop, noop)
  const client = await tcpClient(server.address().port, '127.0.0.1')
  try {
    const closing = closed(client)
    client.write({ type: 'not a Buffer' })
    await timeout(closing)
    t.ok(client.destroyed, 'client closes without a caller error listener')
    t.equal(client.socket.destroyed, true, 'cleanup closes the underlying socket')
  } finally {
    client.destroy()
    await closeServer(server)
  }
  t.end()
})

test('TCP reports callback errors before closing and preserves their stack', async (t) => {
  const original = new Error('receiver failed')
  let output
  let report
  let closedWhenReported
  const reported = new Promise((resolve) => { report = resolve })
  const server = await tcpServer(0, (reply) => {
    output = reply
    throw original
  }, (err) => {
    closedWhenReported = output.destroyed
    report(err)
  })
  const client = await tcpClient(server.address().port, '127.0.0.1')
  client.on('error', noop)
  try {
    const closing = closed(client)
    client.write(encodeFrame({ type: 'test' }))
    const err = await timeout(reported)
    await timeout(closing)
    t.equal(err.message, original.message)
    t.equal(err.stack, original.stack)
    t.equal(closedWhenReported, true, 'connection stops before reporting failure')
    t.ok(output.destroyed)
    t.ok(client.destroyed)
  } finally {
    client.destroy()
    await closeServer(server)
  }
  t.end()
})

for (const side of ['client', 'server']) {
  test(`TCP ${side} closes after a buffered receive callback throws`, async (t) => {
    const original = new Error('buffered receive failed')
    original.code = Err.SEND_ERROR
    const received = []
    const failures = []
    let receiver
    let accept
    let pause
    const accepted = new Promise((resolve) => { accept = resolve })
    const paused = new Promise((resolve) => { pause = resolve })
    const receive = (frame) => {
      received.push(frame)
      if (received.length === 1) { receiver.pause(); pause(); return }
      throw original
    }
    const server = await tcpServer(0, (reply, frame) => {
      if (side === 'server') { receive(frame) }
    }, (err) => failures.push(err), { onConnection: accept })
    const client = await tcpClient(server.address().port, '127.0.0.1', (frame) => {
      if (side === 'client') { receive(frame) }
    })
    client.on('error', (err) => failures.push(err))
    const peer = await timeout(accepted)
    receiver = side === 'client' ? client : peer
    const sender = side === 'client' ? peer : client
    try {
      const closing = Promise.all([event(receiver, 'close'), event(sender, 'close'),
        event(receiver.socket, 'close'), event(sender.socket, 'close')])
      const failed = event(receiver, 'error')
      sender.write(Buffer.from('first'))
      await timeout(paused)
      sender.write(Buffer.from('second'))
      sender.write(Buffer.from('third'))
      await timeout((async () => {
        while (!receiver.destroyed && receiver.readableLength < 2) { await immediate() }
      })())
      t.equal(receiver.readableLength, 2, 'later frames wait in the paused readable buffer')
      receiver.resume()
      const err = await timeout(failed)
      await timeout(closing)
      t.equal(err.message, original.message)
      t.equal(err.code, original.code)
      t.equal(err.stack, original.stack)
      t.deepEqual(failures, [err], 'reports the failure once through the public diagnostic')
      t.deepEqual(received.map((frame) => frame.toString()), ['first', 'second'],
        'does not deliver remaining buffered frames after failure')
      t.ok(receiver.socket.closed && sender.socket.closed, 'both underlying sockets close')
    } finally {
      client.destroy()
      peer.destroy()
      await closeServer(server)
    }
    t.end()
  })
}

test('TCP server rejects listen failures and preserves runtime error stacks', async (t) => {
  const holder = net.createServer()
  await new Promise((resolve, reject) => {
    holder.once('error', reject)
    holder.listen(0, '0.0.0.0', resolve)
  })
  try {
    const err = await tcpServer(holder.address().port, noop, noop)
      .then(() => null, (error) => error)
    t.ok(err instanceof Err.ErrorWithCode)
    t.match(err.message, /listen EADDRINUSE/)
    t.match(err.stack, /^Error: listen EADDRINUSE/)
  } finally { await closeServer(holder) }
  let reported
  const server = await tcpServer(0, noop, (err) => { reported = err })
  try {
    const original = new Error('server runtime failure')
    server.emit('error', original)
    t.equal(reported.stack, original.stack)
    t.match(reported.message, /listen error server runtime failure/)
  } finally { await closeServer(server) }
  t.end()
})


for (const receiver of ['server', 'client']) {
  for (const split of [false, true]) {
    test(`TCP ${receiver} receives plain length-prefixed records with ${split ? 'split' : 'coalesced'} bytes`, async (t) => {
      const frames = [Buffer.alloc(0), Buffer.from([0xc1]),
        encodeFrame({ message: 'first' }), encodeFrame({ message: 'second', data: Buffer.alloc(96 * 1024, 7) })]
      const bytes = framed(frames)
      const incoming = []
      const failures = []
      let finish
      const received = new Promise((resolve) => { finish = resolve })
      const onFrame = (frame) => {
        incoming.push(frame)
        if (incoming.length === frames.length) { finish() }
      }
      let server
      let client
      let peer
      try {
        if (receiver === 'server') {
          server = await tcpServer(0, (reply, frame) => onFrame(frame), (err) => failures.push(err))
          peer = net.connect(server.address().port, '127.0.0.1')
          peer.on('error', (err) => failures.push(err))
          peer.resume()
          await timeout(event(peer, 'connect'))
        } else {
          let accept
          const accepted = new Promise((resolve) => { accept = resolve })
          server = net.createServer((socket) => {
            socket.on('error', (err) => failures.push(err))
            socket.resume()
            accept(socket)
          })
          await new Promise((resolve, reject) => {
            server.once('error', reject)
            server.listen(0, '127.0.0.1', resolve)
          })
          client = await tcpClient(server.address().port, '127.0.0.1', onFrame)
          client.on('error', (err) => failures.push(err))
          peer = await timeout(accepted)
        }
        if (split) {
          // Split frame lengths and payloads across reads.
          const offsets = [0, 1, 3, 7, 10, bytes.length]
          for (let index = 1; index < offsets.length; index++) {
            peer.write(bytes.subarray(offsets[index - 1], offsets[index]))
            await immediate()
          }
        } else peer.write(bytes)
        await timeout(received)
        t.equal(incoming.length, frames.length, 'each frame is one callback')
        t.ok(incoming.every((frame) => Buffer.isBuffer(frame)), 'all records are Buffers')
        t.deepEqual(incoming, frames, 'exact bytes arrive, including empty and non-MessagePack records')
        t.deepEqual(failures, [], 'no transport errors')
      } finally {
        client?.destroy()
        peer?.destroy()
        if (server) { await closeServer(server) }
      }
      t.end()
    })
  }
}

test('TCP corked writes preserve individual Buffer frames through backpressure', async (t) => {
  const frames = Array.from({ length: 48 }, (_, index) => Buffer.alloc(index * 19, index))
  const incoming = []
  const failures = []
  let finish
  const received = new Promise((resolve) => { finish = resolve })
  const server = await tcpServer(0, (reply, frame) => reply.write(frame), (err) => failures.push(err))
  const client = await tcpClient(server.address().port, '127.0.0.1', (frame) => {
    incoming.push(frame)
    if (incoming.length === frames.length) { finish() }
  })
  client.on('error', (err) => failures.push(err))
  try {
    client.cork()
    const writable = frames.map((frame) => client.write(frame))
    t.ok(writable.includes(false), 'queued writes apply backpressure')
    client.uncork()
    await timeout(received)
    t.deepEqual(incoming, frames, 'corking never joins messages and preserves empty frames')
    t.deepEqual(failures, [], 'no transport errors')
  } finally {
    client.destroy()
    await closeServer(server)
  }
  t.end()
})
