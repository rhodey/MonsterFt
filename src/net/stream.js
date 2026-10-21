import net from 'node:net'
import { Duplex } from 'node:stream'
import * as Err from '../error.js'

const noop = () => {}
const MAX_FRAME_BYTES = 128 * 1024 * 1024
const MAX_QUEUED_BYTES = 256 * 1024 * 1024

function frameOptions(opts = {}) {
  const maxFrameBytes = opts.maxFrameBytes ?? MAX_FRAME_BYTES
  const maxQueuedBytes = opts.maxQueuedBytes ?? MAX_QUEUED_BYTES
  if (!Number.isSafeInteger(maxFrameBytes) || maxFrameBytes < 0 || maxFrameBytes > 0xffff_ffff) {
    throw new Err.ErrorWithCode('illegal maxFrameBytes', Err.ARGUMENT_ILLEGAL)
  }
  if (!Number.isSafeInteger(maxQueuedBytes) || maxQueuedBytes < 4) {
    throw new Err.ErrorWithCode('illegal maxQueuedBytes', Err.ARGUMENT_ILLEGAL)
  }
  return { maxFrameBytes, maxQueuedBytes }
}

class FrameSocket extends Duplex {
  constructor(socket, opts = {}) {
    super({ objectMode: true, allowHalfOpen: false })
    const limits = frameOptions(opts)
    this.socket = socket
    this.maxFrameBytes = limits.maxFrameBytes
    this.maxQueuedBytes = limits.maxQueuedBytes
    this.queuedBytes = 0
    this.header = Buffer.allocUnsafe(4)
    this.headerSize = 0
    this.remaining = null
    this.parts = []
    this.chunk = null
    this.offset = 0
    this.blocked = false
    this.receiving = false
    this.ended = false
    this.on('error', noop)
    const receive = () => this._receive()
    const end = () => { this.ended = true; this._receive() }
    const error = (err) => this.destroy(Err.wrapError(err))
    socket.on('readable', receive)
    socket.on('end', end)
    socket.on('error', error)
    socket.once('close', () => {
      socket.removeListener('readable', receive)
      socket.removeListener('end', end)
      socket.removeListener('error', error)
      if (!this.ended) { this.destroy() }
    })
  }

  canSend(frame) {
    if (!Buffer.isBuffer(frame)) {
      throw new Err.ErrorWithCode('message must be a Buffer', Err.ARGUMENT_ILLEGAL)
    }
    if (frame.length > this.maxFrameBytes) {
      throw new Err.ErrorWithCode('message exceeds maxFrameBytes', Err.ARGUMENT_ILLEGAL)
    }
    return this.writable && !this.destroyed &&
      frame.length + 4 <= this.maxQueuedBytes - this.queuedBytes
  }

  write(frame, encoding, callback) {
    if (typeof encoding === 'function') { callback = encoding; encoding = undefined }
    try {
      if (!this.canSend(frame)) {
        throw new Err.ErrorWithCode(this.destroyed || !this.writable ?
          'connection is closed' : 'connection exceeds maxQueuedBytes', Err.SEND_ERROR)
      }
    } catch (err) {
      this.destroy(err)
      if (callback) { process.nextTick(callback, err) }
      return false
    }
    const size = frame.length + 4
    this.queuedBytes += size
    return super.write(frame, encoding, (err) => {
      this.queuedBytes -= size
      if (callback) { callback(err) }
    })
  }

  end(frame, encoding, callback) {
    if (typeof frame === 'function') { callback = frame; frame = undefined }
    else if (typeof encoding === 'function') { callback = encoding }
    if (frame !== undefined && frame !== null) { this.write(frame) }
    return super.end(callback)
  }

  _read() {
    this.blocked = false
    this._receive()
  }

  _receive() {
    if (this.receiving || this.blocked || this.destroyed) { return }
    this.receiving = true
    try {
      while (!this.blocked && !this.destroyed) {
        if (this.chunk === null) {
          this.chunk = this.socket.read()
          this.offset = 0
          if (this.chunk === null) { break }
        }
        const available = this.chunk.length - this.offset
        if (this.remaining === null) {
          const count = Math.min(4 - this.headerSize, available)
          this.chunk.copy(this.header, this.headerSize, this.offset, this.offset + count)
          this.headerSize += count
          this.offset += count
          if (this.headerSize === 4) {
            this.remaining = this.header.readUInt32BE()
            this.headerSize = 0
            if (this.remaining > this.maxFrameBytes) {
              throw new Err.ErrorWithCode('frame exceeds maxFrameBytes', Err.FRAME_CORRUPT)
            }
          }
        } else {
          const count = Math.min(this.remaining, available)
          if (count > 0) { this.parts.push(this.chunk.subarray(this.offset, this.offset + count)) }
          this.remaining -= count
          this.offset += count
        }
        if (this.offset === this.chunk.length) { this.chunk = null }
        if (this.remaining === 0) {
          const frame = this.parts.length === 1 ? this.parts[0] : Buffer.concat(this.parts)
          this.parts = []
          this.remaining = null
          this.blocked = !this.push(frame)
        }
      }
      if (this.ended && !this.blocked && !this.destroyed) {
        if (this.remaining !== null || this.headerSize > 0) {
          throw new Err.ErrorWithCode('frame is incomplete', Err.FRAME_CORRUPT)
        }
        this.push(null)
      }
    } catch (err) {
      this.destroy(Err.wrapError(err))
    } finally {
      this.receiving = false
    }
  }

  _write(frame, encoding, callback) {
    const header = Buffer.allocUnsafe(4)
    header.writeUInt32BE(frame.length)
    this.socket.cork()
    this.socket.write(header)
    this.socket.write(frame, callback)
    this.socket.uncork()
  }

  _final(callback) { this.socket.end(callback) }

  _destroy(err, callback) {
    this.chunk = null
    this.parts = []
    this.socket.destroy()
    callback(err)
  }
}

function connectFrames(address, opts = {}) {
  frameOptions(opts)
  const connectTimeout = opts.connectTimeout
  if (connectTimeout !== undefined && (!Number.isSafeInteger(connectTimeout) || connectTimeout < 1)) {
    throw new Err.ErrorWithCode('illegal connectTimeout', Err.ARGUMENT_ILLEGAL)
  }
  return new Promise((resolve, reject) => {
    const socket = new net.Socket({ allowHalfOpen: true })
    let timer = null
    let settled = false
    const cleanup = () => {
      clearTimeout(timer)
      opts.signal?.removeEventListener('abort', abort)
      socket.removeListener('error', fail)
      socket.removeListener('close', closed)
      socket.removeListener('connect', connected)
    }
    const fail = (err) => {
      if (settled) { return }
      settled = true
      cleanup()
      const failure = Err.wrapError(err, null, 'connect error ')
      if (socket.closed) { reject(failure); return }
      socket.on('error', noop)
      socket.once('close', () => {
        socket.removeListener('error', noop)
        reject(failure)
      })
      socket.destroy()
    }
    const abort = () => fail(opts.signal.reason ?? new Err.ErrorWithCode('connect aborted', Err.SEND_ERROR))
    const closed = () => fail(new Err.ErrorWithCode('connection closed', Err.SEND_ERROR))
    const connected = () => {
      if (settled) { return }
      try {
        socket.setNoDelay(true)
        const connection = new FrameSocket(socket, opts)
        settled = true
        cleanup()
        resolve(connection)
      } catch (err) { fail(err) }
    }
    socket.once('error', fail)
    socket.once('close', closed)
    socket.once('connect', connected)
    if (opts.signal?.aborted) { abort(); return }
    opts.signal?.addEventListener('abort', abort, { once: true })
    if (connectTimeout !== undefined) {
      timer = setTimeout(() => fail(new Err.ErrorWithCode('connect timeout', Err.SEND_ERROR)), connectTimeout)
    }
    try { socket.connect(address) } catch (err) { fail(err) }
  })
}

function listenFrames(address, onConnection, errCb, opts = {}) {
  frameOptions(opts)
  const server = net.createServer({ allowHalfOpen: true }, (socket) => {
    socket.setNoDelay(true)
    const connection = new FrameSocket(socket, opts)
    connection.on('error', (err) => {
      try { errCb(err, connection) } finally { connection.destroy() }
    })
    try { onConnection(connection) } catch (err) { connection.destroy(Err.wrapError(err)) }
  })
  return new Promise((resolve, reject) => {
    let settled = false
    const cleanup = () => {
      server.removeListener('error', fail)
      opts.signal?.removeEventListener('abort', abort)
    }
    const fail = (err) => {
      if (settled) { return }
      settled = true
      cleanup()
      const failure = Err.wrapError(err, null, 'listen error ')
      server.on('error', noop)
      // close() invalidates pending address resolution as well as bound listeners.
      server.close(() => {
        server.removeListener('error', noop)
        reject(failure)
      })
    }
    const abort = () => fail(opts.signal.reason ?? new Err.ErrorWithCode('listen aborted', Err.SEND_ERROR))
    server.once('error', fail)
    if (opts.signal?.aborted) { abort(); return }
    opts.signal?.addEventListener('abort', abort, { once: true })
    try {
      server.listen(address, () => {
        if (settled) { server.close(); return }
        settled = true
        cleanup()
        server.on('error', (err) => errCb(Err.wrapError(err, null, 'listen error '), null))
        resolve(server)
      })
    } catch (err) { fail(err) }
  })
}

export { FrameSocket, frameOptions, connectFrames, listenFrames }
