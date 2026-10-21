import net from 'node:net'
import { Duplex } from 'node:stream'
import * as Err from './error.js'

const noop = () => {}

class FrameSocket extends Duplex {
  constructor(socket) {
    super({ objectMode: true, allowHalfOpen: false })
    this.socket = socket
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
    const error = (err) => this.destroy(err)
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
          throw new Err.ErrorWithCode('UDS frame is incomplete', Err.FRAME_CORRUPT)
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
    if (!Buffer.isBuffer(frame)) {
      callback(new Err.ErrorWithCode('UDS message must be a Buffer', Err.ARGUMENT_ILLEGAL))
      return
    }
    if (frame.length > 0xffff_ffff) {
      callback(new Err.ErrorWithCode('UDS message exceeds uint32 length', Err.ARGUMENT_ILLEGAL))
      return
    }
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

function udsServer(path, onConnection, errCb) {
  const server = net.createServer({ allowHalfOpen: true }, (socket) => {
    const connection = new FrameSocket(socket)
    connection.on('error', errCb)
    try { onConnection(connection) } catch (err) { connection.destroy(Err.wrapError(err)) }
  })
  return new Promise((resolve, reject) => {
    const onError = (err) => reject(Err.wrapError(err, null, 'UDS listen error '))
    server.once('error', onError)
    try {
      server.listen(path, () => {
        server.removeListener('error', onError)
        server.on('error', errCb)
        resolve(server)
      })
    } catch (err) {
      server.removeListener('error', onError)
      reject(err)
    }
  })
}

function udsClient(path) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket({ allowHalfOpen: true })
    const onError = (err) => {
      socket.destroy()
      reject(Err.wrapError(err, null, 'UDS connect error '))
    }
    socket.once('error', onError)
    try {
      socket.connect(path, () => {
        socket.removeListener('error', onError)
        resolve(new FrameSocket(socket))
      })
    } catch (err) { onError(err) }
  })
}

export { udsServer, udsClient }
