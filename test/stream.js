import test from 'tape'
import { Duplex } from 'node:stream'
import { FrameSocket, frameOptions } from '../src/net/stream.js'
import * as Err from '../src/error.js'

const event = (target, name) => new Promise((resolve) => target.once(name, resolve))
const immediate = () => new Promise((resolve) => setImmediate(resolve))

class Socket extends Duplex {
  constructor(delayed = false) {
    super({ allowHalfOpen: true })
    this.output = []
    this.callbacks = []
    this.delayed = delayed
  }
  _read() {}
  _write(bytes, encoding, done) {
    this.output.push(bytes)
    if (this.delayed) this.callbacks.push(done)
    else done()
  }
  release() { this.callbacks.shift()?.() }
}

function framed(frames) {
  return Buffer.concat(frames.flatMap((frame) => {
    const header = Buffer.alloc(4)
    header.writeUInt32BE(frame.length)
    return [header, frame]
  }))
}

test('framing options validate limits and preserve their defaults', (t) => {
  t.deepEqual(frameOptions(), { maxFrameBytes: 128 * 1024 * 1024, maxQueuedBytes: 256 * 1024 * 1024 })
  for (const value of [-1, 0x1_0000_0000, 1.5, NaN, Infinity, '1']) {
    t.throws(() => frameOptions({ maxFrameBytes: value }), /illegal maxFrameBytes/)
  }
  for (const value of [0, 3, -1, 1.5, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    t.throws(() => frameOptions({ maxQueuedBytes: value }), /illegal maxQueuedBytes/)
  }
  t.deepEqual(frameOptions({ maxFrameBytes: 0, maxQueuedBytes: 4 }), { maxFrameBytes: 0, maxQueuedBytes: 4 })
  t.end()
})

for (const split of [false, true]) {
  test(`framing reconstructs ${split ? 'bytewise' : 'combined'} frames`, async (t) => {
    const socket = new Socket()
    const connection = new FrameSocket(socket)
    const frames = [Buffer.alloc(0), Buffer.from([0, 255, 128]), Buffer.alloc(200, 31)]
    const received = []
    connection.on('data', (frame) => received.push(frame))
    const ended = event(connection, 'end')
    const bytes = framed(frames)
    if (split) {
      for (const byte of bytes) { socket.push(Buffer.from([byte])); await immediate() }
    } else socket.push(bytes)
    socket.push(null)
    await ended
    t.deepEqual(received, frames, 'exact independent Buffers arrive, including empty payloads')
    connection.destroy()
    t.end()
  })
}

test('framing caps readable buffering and resumes without losing input', async (t) => {
  const socket = new Socket()
  const connection = new FrameSocket(socket)
  const frames = Array.from({ length: 50 }, (_, i) => Buffer.alloc(i, i))
  socket.push(framed(frames))
  socket.push(null)
  await immediate()
  t.equal(connection.readableLength, connection.readableHighWaterMark, 'paused input stops at its high water mark')
  const received = []
  for await (const frame of connection) { received.push(frame) }
  t.deepEqual(received, frames, 'draining preserves every boundary')
  t.end()
})

for (const action of ['throw', 'destroy']) {
  test(`framing stops buffered delivery when a listener ${action === 'throw' ? 'throws' : 'destroys the stream'}`, async (t) => {
    const socket = new Socket()
    const connection = new FrameSocket(socket)
    const original = new Err.ErrorWithCode('receiver failed', Err.APPLY_ERROR)
    const received = []
    const errors = []
    const closing = Promise.all([event(connection, 'close'), event(socket, 'close')])
    connection.on('error', (err) => errors.push(err))
    connection.pause()
    connection.on('data', (frame) => {
      received.push(frame.toString())
      if (action === 'throw') { throw original }
      connection.destroy()
    })
    socket.push(framed(['first', 'second', 'third'].map((value) => Buffer.from(value))))
    await immediate()
    t.equal(connection.readableLength, 3, 'frames are buffered before delivery')
    connection.resume()
    await closing
    t.deepEqual(received, ['first'], 'no later buffered frame reaches the listener')
    t.deepEqual(errors, action === 'throw' ? [original] : [], 'retains the original diagnostic')
    t.ok(connection.destroyed)
    t.ok(socket.destroyed, 'the underlying socket closes too')
    t.end()
  })
}

test('framing preserves non-data event listener behavior', async (t) => {
  const socket = new Socket()
  const connection = new FrameSocket(socket)
  const original = new Error('event listener failed')
  for (const type of ['error', 'custom']) {
    connection.once(type, () => { throw original })
    t.throws(() => connection.emit(type, original), (err) => err === original,
      `${type} listener exceptions still propagate`)
  }
  t.notOk(connection.destroyed, 'unrelated events do not trigger data-listener cleanup')
  const closing = event(connection, 'close')
  connection.destroy()
  await closing
  t.end()
})

test('framing rejects oversized lengths before collecting their payload', async (t) => {
  const socket = new Socket()
  const connection = new FrameSocket(socket, { maxFrameBytes: 10 })
  const failed = event(connection, 'error')
  const header = Buffer.alloc(4)
  header.writeUInt32BE(11)
  socket.push(header)
  const err = await failed
  t.equal(err.code, Err.FRAME_CORRUPT)
  t.equal(connection.parts.length, 0, 'does not accumulate the oversized payload')
  t.ok(connection.destroyed)
  t.ok(socket.destroyed)
  t.end()
})

for (const bytes of [Buffer.from([0]), Buffer.from([0, 0, 0]), Buffer.from([0, 0, 0, 2, 9])]) {
  test('framing rejects incomplete input on EOF', async (t) => {
    const socket = new Socket()
    const connection = new FrameSocket(socket)
    const failed = event(connection, 'error')
    socket.push(bytes)
    socket.push(null)
    const err = await failed
    t.equal(err.message, 'frame is incomplete')
    t.equal(err.code, Err.FRAME_CORRUPT)
    t.end()
  })
}

test('framing queue accounting includes in-flight writes and empty-frame headers', async (t) => {
  const socket = new Socket(true)
  const connection = new FrameSocket(socket, { maxFrameBytes: 8, maxQueuedBytes: 12 })
  t.ok(connection.canSend(Buffer.alloc(8)), 'the exact queue boundary is admitted')
  connection.write(Buffer.alloc(8))
  t.equal(connection.queuedBytes, 12, 'counts payload and header before callback')
  t.notOk(connection.canSend(Buffer.alloc(0)), 'even empty frames require queue capacity')
  socket.release()
  t.equal(connection.queuedBytes, 12, 'header completion does not release the payload reservation')
  socket.release()
  await immediate()
  t.equal(connection.queuedBytes, 0, 'complete frame releases reservation')
  connection.cork()
  for (let i = 0; i < 3; i++) { connection.write(Buffer.alloc(0)) }
  t.equal(connection.queuedBytes, 12, 'corked empty messages cannot evade limits')
  t.notOk(connection.canSend(Buffer.alloc(0)))
  connection.destroy()
  await immediate()
  t.equal(connection.queuedBytes, 0, 'destroy releases all queued reservations')
  t.end()
})

test('framing end includes its final payload in the queue limit', async (t) => {
  const socket = new Socket(true)
  const connection = new FrameSocket(socket, { maxQueuedBytes: 8 })
  const failed = event(connection, 'error')
  connection.write(Buffer.alloc(4))
  connection.end(Buffer.alloc(0))
  const err = await failed
  t.equal(err.code, Err.SEND_ERROR)
  t.equal(err.message, 'connection exceeds maxQueuedBytes')
  t.ok(socket.destroyed)
  t.end()
})

test('framing validates outbound records without changing retained Buffer ownership', async (t) => {
  const socket = new Socket()
  const connection = new FrameSocket(socket, { maxFrameBytes: 3 })
  t.throws(() => connection.canSend('bad'), /message must be a Buffer/)
  t.throws(() => connection.canSend(Buffer.alloc(4)), /message exceeds maxFrameBytes/)
  const frame = Buffer.from([0, 255, 33])
  await new Promise((resolve, reject) => connection.write(frame, (err) => err ? reject(err) : resolve()))
  t.deepEqual(Buffer.concat(socket.output), framed([frame]), 'only length and exact payload are written')
  t.equal(socket.output[1], frame, 'no payload copy is introduced')
  connection.destroy()
  t.end()
})

test('framing retains external lifecycle listeners during teardown', async (t) => {
  const socket = new Socket()
  const external = () => {}
  socket.on('error', external)
  socket.on('close', external)
  const connection = new FrameSocket(socket)
  connection.on('error', external)
  connection.on('close', external)
  const closing = event(connection, 'close')
  connection.destroy()
  await closing
  await immediate()
  t.ok(socket.listeners('error').includes(external))
  t.ok(socket.listeners('close').includes(external))
  t.ok(connection.listeners('error').includes(external))
  t.ok(connection.listeners('close').includes(external))
  t.equal(socket.listenerCount('readable'), 0, 'detaches its own socket input listener')
  t.end()
})
