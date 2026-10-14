import test from 'tape'
import net from 'node:net'
import { mkdtemp, rm, writeFile, readFile } from 'node:fs/promises'
import { udxServer, udxClient } from '../src/net/udx.js'
import * as Err from '../src/error.js'

const noop = () => {}
const event = (target, name) => new Promise((resolve) => target.once(name, resolve))
const immediate = () => new Promise((resolve) => setImmediate(resolve))
const closeServer = (server) => !server?.listening ? Promise.resolve() :
  new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve()))

function timeout(work, ms = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('UDX test timeout')), ms)
    work.then((value) => { clearTimeout(timer); resolve(value) },
      (err) => { clearTimeout(timer); reject(err) })
  })
}

function framed(frames) {
  return Buffer.concat(frames.flatMap((frame) => {
    const header = Buffer.alloc(4)
    header.writeUInt32BE(frame.length)
    return [header, frame]
  }))
}

async function fixture(onConnection = noop) {
  const directory = await mkdtemp('/tmp/monsterft-udx-')
  const path = `${directory}/socket`
  const errors = []
  const connections = []
  const server = await udxServer(path, (connection) => {
    connections.push(connection)
    onConnection(connection)
  }, (err) => errors.push(err))
  return {
    path, server, errors, connections,
    async close() {
      for (const connection of connections) { connection.destroy() }
      await closeServer(server)
      await rm(directory, { recursive: true, force: true })
    },
  }
}

test('UDX preserves Buffer frames, including empty and large messages', async (t) => {
  const f = await fixture((connection) => connection.on('data', (frame) => connection.write(frame)))
  const client = await udxClient(f.path)
  try {
    const frames = [Buffer.alloc(0), Buffer.from([0, 255, 128, 1]),
      Buffer.alloc(4 * 1024 * 1024, 0x7f)]
    const received = []
    let finish
    const done = new Promise((resolve) => { finish = resolve })
    client.on('data', (frame) => {
      received.push(frame)
      if (received.length === frames.length) { finish() }
    })
    client.cork()
    for (const frame of frames) { client.write(frame) }
    client.uncork()
    await timeout(done)
    t.ok(received.every(Buffer.isBuffer), 'every received message is a Buffer')
    t.deepEqual(received, frames, 'exact bytes and frame order survive both directions')
    t.deepEqual(f.errors, [], 'transport has no errors')
  } finally {
    client.destroy()
    await f.close()
  }
  t.end()
})

for (const split of [false, true]) {
  test(`UDX receives ${split ? 'fragmented' : 'combined'} length-prefixed messages`, async (t) => {
    const frames = [Buffer.alloc(0), Buffer.from('one'), Buffer.from('two'), Buffer.alloc(100_000, 77)]
    const received = []
    let finish
    const done = new Promise((resolve) => { finish = resolve })
    const f = await fixture((connection) => connection.on('data', (frame) => {
      received.push(frame)
      if (received.length === frames.length) { finish() }
    }))
    const raw = net.connect(f.path)
    raw.on('error', noop)
    try {
      await event(raw, 'connect')
      const bytes = framed(frames)
      if (split) {
        const offsets = [0, 1, 3, 5, 9, 12, 14, 20, bytes.length]
        for (let index = 1; index < offsets.length; index++) {
          raw.write(bytes.subarray(offsets[index - 1], offsets[index]))
          await immediate()
        }
      } else raw.write(bytes)
      await timeout(done)
      t.deepEqual(received, frames, 'frames are independent of socket read boundaries')
      t.deepEqual(f.errors, [], 'transport has no errors')
    } finally {
      raw.destroy()
      await f.close()
    }
    t.end()
  })
}

test('UDX writes only a uint32 length and original frame bytes', async (t) => {
  const directory = await mkdtemp('/tmp/monsterft-udx-')
  const path = `${directory}/socket`
  const chunks = []
  let finish
  const done = new Promise((resolve) => { finish = resolve })
  const server = net.createServer((socket) => {
    socket.on('data', (chunk) => chunks.push(chunk))
    socket.on('end', finish)
  })
  await new Promise((resolve) => server.listen(path, resolve))
  const client = await udxClient(path)
  client.resume()
  try {
    const frames = [Buffer.alloc(0), Buffer.from('raw bytes'), Buffer.from([0xc1, 0, 255])]
    client.write(frames[0])
    client.write(frames[1])
    client.end(frames[2])
    await timeout(done)
    t.deepEqual(Buffer.concat(chunks), framed(frames), 'wire carries no nested codec or checksum')
    await timeout(event(client, 'close'))
    t.ok(client.destroyed, 'orderly connection closes')
  } finally {
    client.destroy()
    await closeServer(server)
    await rm(directory, { recursive: true, force: true })
  }
  t.end()
})

test('UDX readable backpressure preserves buffered frame boundaries', async (t) => {
  let accept
  const accepted = new Promise((resolve) => { accept = resolve })
  const f = await fixture(accept)
  const client = await udxClient(f.path)
  client.resume()
  try {
    const receiver = await accepted
    const frames = Array.from({ length: 100 }, (_, index) => Buffer.alloc(8192, index))
    client.cork()
    const writes = frames.map((frame) => client.write(frame))
    client.uncork()
    client.end()
    await new Promise((resolve) => setTimeout(resolve, 20))
    t.ok(writes.includes(false), 'buffered writes provide backpressure')
    t.ok(receiver.readableLength <= receiver.readableHighWaterMark,
      'a paused receiver does not buffer unlimited messages')
    const received = []
    for await (const frame of receiver) { received.push(frame) }
    t.deepEqual(received, frames, 'resuming reads returns every original message')
    t.deepEqual(f.errors, [], 'flow control causes no framing errors')
  } finally {
    client.destroy()
    await f.close()
  }
  t.end()
})

for (const sender of ['client', 'server']) {
  test(`UDX ${sender} end flushes its final large frame`, async (t) => {
    const frames = [Buffer.from('first'), Buffer.alloc(16 * 1024 * 1024, 99)]
    const received = []
    const f = await fixture((connection) => {
      connection.on('data', (frame) => {
        if (sender === 'client') { received.push(frame) }
        else { connection.write(frames[0]); connection.end(frames[1]) }
      })
    })
    const client = await udxClient(f.path)
    client.on('data', (frame) => received.push(frame))
    try {
      const closing = event(client, 'close')
      if (sender === 'client') { client.write(frames[0]); client.end(frames[1]) }
      else client.write(Buffer.from('request'))
      await timeout(closing)
      t.deepEqual(received, frames, 'all frames arrive before closure')
      t.deepEqual(f.errors, [], 'graceful end reports no errors')
    } finally {
      client.destroy()
      await f.close()
    }
    t.end()
  })
}

for (const bytes of [Buffer.from([0, 0]), Buffer.from([0, 0, 0, 2, 1])]) {
  test('UDX rejects incomplete frames at end of input', async (t) => {
    const f = await fixture((connection) => connection.resume())
    const raw = net.connect(f.path)
    raw.on('error', noop)
    raw.resume()
    try {
      await event(raw, 'connect')
      const closing = event(raw, 'close')
      raw.end(bytes)
      await timeout(closing)
      t.equal(f.errors.length, 1, 'incomplete frame reports one error')
      t.equal(f.errors[0].code, Err.FRAME_CORRUPT)
      t.equal(f.errors[0].message, 'frame is incomplete')
      t.ok(f.connections[0].destroyed, 'receiver closes after invalid framing')
    } finally {
      raw.destroy()
      await f.close()
    }
    t.end()
  })
}

test('UDX rejects non-Buffer writes and cleans up without caller error listeners', async (t) => {
  const f = await fixture((connection) => connection.resume())
  const client = await udxClient(f.path)
  try {
    const failed = event(client, 'error')
    const closing = event(client, 'close')
    client.write({ bad: true })
    const err = await timeout(failed)
    await timeout(closing)
    t.equal(err.code, Err.ARGUMENT_ILLEGAL)
    t.ok(client.destroyed, 'invalid write closes connection')
    const next = await udxClient(f.path)
    const nextClosing = event(next, 'close')
    next.write('invalid')
    await timeout(nextClosing)
    t.ok(next.destroyed, 'internal listener prevents unhandled stream errors')
  } finally {
    client.destroy()
    await f.close()
  }
  t.end()
})

test('UDX reports receiver and connection callback errors with their original stacks', async (t) => {
  for (const source of ['connection', 'data']) {
    const original = new Error(`${source} failed`)
    const f = await fixture((connection) => {
      if (source === 'connection') { throw original }
      connection.on('data', () => { throw original })
    })
    const client = await udxClient(f.path)
    client.resume()
    try {
      const closing = event(client, 'close')
      if (source === 'data') { client.write(Buffer.from('request')) }
      await timeout(closing)
      t.equal(f.errors.length, 1, `${source}: reports once`)
      t.equal(f.errors[0].message, original.message)
      t.equal(f.errors[0].stack, original.stack)
      t.ok(f.connections[0].destroyed, `${source}: closes the failed connection`)
    } finally {
      client.destroy()
      await f.close()
    }
  }
  t.end()
})

test('UDX closes after a buffered data listener throws', async (t) => {
  const original = new Err.ErrorWithCode('buffered receive failed', Err.SEND_ERROR)
  const received = []
  let receiver
  let pause
  const paused = new Promise((resolve) => { pause = resolve })
  const f = await fixture((connection) => {
    receiver = connection
    connection.on('data', (frame) => {
      received.push(frame)
      if (received.length === 1) { connection.pause(); pause(); return }
      throw original
    })
  })
  const client = await udxClient(f.path)
  client.resume()
  try {
    client.write(Buffer.from('first'))
    await timeout(paused)
    const closing = Promise.all([event(receiver, 'close'), event(client, 'close'),
      event(receiver.socket, 'close'), event(client.socket, 'close')])
    client.write(Buffer.from('second'))
    client.write(Buffer.from('third'))
    await timeout((async () => {
      while (!receiver.destroyed && receiver.readableLength < 2) { await immediate() }
    })())
    t.equal(receiver.readableLength, 2, 'later frames wait in the paused readable buffer')
    receiver.resume()
    await timeout(closing)
    t.equal(f.errors.length, 1, 'reports the buffered listener failure once')
    t.equal(f.errors[0].message, original.message)
    t.equal(f.errors[0].code, original.code)
    t.equal(f.errors[0].stack, original.stack)
    t.deepEqual(received.map((frame) => frame.toString()), ['first', 'second'],
      'does not deliver remaining buffered frames after failure')
    t.ok(receiver.socket.closed && client.socket.closed, 'both underlying sockets close')
  } finally {
    client.destroy()
    await f.close()
  }
  t.end()
})

test('UDX explicit destroy aborts queued output', async (t) => {
  const directory = await mkdtemp('/tmp/monsterft-udx-')
  const path = `${directory}/socket`
  let accept
  const accepted = new Promise((resolve) => { accept = resolve })
  const server = net.createServer((socket) => { socket.pause(); accept(socket) })
  await new Promise((resolve) => server.listen(path, resolve))
  const client = await udxClient(path)
  const peer = await accepted
  peer.on('error', noop)
  try {
    const payload = Buffer.alloc(16 * 1024 * 1024)
    client.write(payload)
    client.write(payload)
    const closing = event(client, 'close')
    client.destroy()
    await timeout(closing)
    t.notOk(client.writableFinished, 'destroy does not flush pending frames')
    let received = 0
    peer.on('data', (chunk) => { received += chunk.length })
    const peerClosing = event(peer, 'close')
    peer.resume()
    await timeout(peerClosing)
    t.ok(received < payload.length * 2, 'remaining bytes are discarded')
  } finally {
    client.destroy()
    peer.destroy()
    await closeServer(server)
    await rm(directory, { recursive: true, force: true })
  }
  t.end()
})

test('UDX failures preserve existing paths and reject unavailable connections', async (t) => {
  const directory = await mkdtemp('/tmp/monsterft-udx-')
  const path = `${directory}/socket`
  try {
    await writeFile(path, 'do not replace')
    const failure = await udxServer(path, noop, noop).then(() => null, (err) => err)
    t.match(failure.message, /EADDRINUSE/, 'listen failure rejects')
    t.equal(await readFile(path, 'utf8'), 'do not replace', 'existing path is untouched')
    const absent = await udxClient(`${directory}/missing`).then(() => null, (err) => err)
    t.match(absent.message, /ENOENT/, 'connection failure rejects')
    t.match(absent.stack, /ENOENT/, 'original connect stack remains available')
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
  t.end()
})

test('UDX applies configurable frame limits before collecting payloads', async (t) => {
  const directory = await mkdtemp('/tmp/monsterft-udx-')
  const path = `${directory}/socket`
  let report
  const reported = new Promise((resolve) => { report = resolve })
  const server = await udxServer(path, (connection) => connection.resume(),
    (err, connection) => report({ err, connection }), { maxFrameBytes: 2 })
  const socket = net.connect(path)
  socket.on('error', noop)
  socket.resume()
  try {
    await event(socket, 'connect')
    const closing = event(socket, 'close')
    socket.write(Buffer.from([0, 0, 0, 3]))
    const { err, connection } = await timeout(reported)
    await timeout(closing)
    t.equal(err.code, Err.FRAME_CORRUPT)
    t.ok(connection.destroyed, 'reports the offending connection')
  } finally {
    socket.destroy()
    await closeServer(server)
    await rm(directory, { recursive: true, force: true })
  }
  t.end()
})

test('UDX cancelled setup preserves the cancellation stack', async (t) => {
  const controller = new AbortController()
  const original = new Error('cancel local transport')
  controller.abort(original)
  const options = { signal: controller.signal }
  const clientError = await udxClient('/tmp/monsterft-absent', options).then(() => null, (err) => err)
  const serverError = await udxServer('/tmp/monsterft-absent', noop, noop, options).then(() => null, (err) => err)
  t.equal(clientError.stack, original.stack)
  t.equal(serverError.stack, original.stack)
  t.end()
})
