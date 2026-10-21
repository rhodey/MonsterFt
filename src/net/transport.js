import { EventEmitter } from 'node:events'
import { frameOptions, listenFrames } from './stream.js'
import * as Err from '../error.js'

const noop = () => {}
const stopped = () => new Err.ErrorWithCode('transport is stopped', Err.NODE_NOT_OPEN)
const illegal = (message) => new Err.ErrorWithCode(message, Err.ARGUMENT_ILLEGAL)

// Shared lifecycle for the optional socket routers, independent of Gate.
class Transport extends EventEmitter {
  constructor(opts = {}) {
    super()
    if (!opts || typeof opts !== 'object') { throw illegal('transport options are illegal') }
    this._limits = frameOptions(opts)
    this._open = false
    this._stopped = false
    this._error = null
    this._openWork = null
    this._closeWork = null
    this._abort = new AbortController()
    this._servers = new Set()
    this._serverWaits = []
    this._sockets = new Set()
    this._socketWaits = new Set()
    this._failure = Promise.withResolvers()
    this.failed = this._failure.promise
    this.failed.catch(noop)
  }

  get isOpen() { return this._open }
  get isStopped() { return this._stopped }

  _emitSafe(type, ...args) {
    for (const listener of this.rawListeners(type)) {
      try { listener.call(this, ...args) }
      catch (err) {
        if (type !== 'error') { this._fatal(err); return }
      }
      if (type !== 'error' && this._stopped) { return }
    }
  }

  _fatal(error) {
    if (this._stopped) { return }
    this._error = Err.wrapError(error)
    this.stop()
    this._failure.reject(this._error)
    this._emitSafe('error', this._error)
  }

  _throwIfStopped() {
    if (this._stopped) { throw this._error ?? stopped() }
  }

  _validateSend(frame) {
    this._throwIfStopped()
    if (!this._open) { throw new Err.ErrorWithCode('transport is not open', Err.NODE_NOT_OPEN) }
    if (!Buffer.isBuffer(frame)) { throw illegal('message must be a Buffer') }
    if (frame.length > this._limits.maxFrameBytes) { throw illegal('message exceeds maxFrameBytes') }
  }

  _track(connection) {
    const socket = connection.socket
    if (socket.closed || this._sockets.has(connection)) { return }
    this._sockets.add(connection)
    const wait = new Promise((resolve) => socket.once('close', resolve))
    this._socketWaits.add(wait)
    wait.then(() => {
      this._sockets.delete(connection)
      this._socketWaits.delete(wait)
    })
  }

  async _listen(address, onConnection) {
    const server = await listenFrames(address, (connection) => {
      this._track(connection)
      if (this._stopped) { connection.destroy(); return }
      try { onConnection(connection) } catch (err) { this._fatal(err) }
    }, (err, connection) => {
      if (this._stopped) { return }
      if (connection) { this._connectionError(connection, err) }
      else { this._fatal(err) }
    }, { ...this._limits, signal: this._abort.signal })
    this._servers.add(server)
    if (this._stopped) { this._closeServer(server); this._throwIfStopped() }
    return server
  }

  _closeServer(server) {
    if (!this._servers.delete(server)) { return }
    const wait = new Promise((resolve, reject) => {
      server.close((err) => err ? reject(err) : resolve())
    })
    wait.catch(noop)
    this._serverWaits.push(wait)
  }

  open() {
    if (this._stopped) { return Promise.reject(this._error ?? stopped()) }
    if (this._openWork !== null) { return this._openWork }
    this._openWork = Promise.resolve().then(async () => {
      try {
        this._throwIfStopped()
        await this._start()
        this._throwIfStopped()
        this._open = true
      } catch (err) {
        this.stop()
        // close() remains responsible for waiting for all resource cleanup.
        throw this._error ?? err
      }
    })
    return this._openWork
  }

  stop() {
    if (this._stopped) { return }
    this._stopped = true
    this._open = false
    this._abort.abort(this._error ?? stopped())
    this._cancel()
    for (const connection of this._sockets) { connection.destroy() }
    for (const server of this._servers) { this._closeServer(server) }
  }

  close() {
    if (this._closeWork !== null) { return this._closeWork }
    this._closeWork = Promise.resolve().then(async () => {
      await this._openWork?.catch(noop)
      await this._settle()
      const results = await Promise.allSettled([...this._serverWaits, ...this._socketWaits])
      const failure = results.find((result) => result.status === 'rejected')
      if (failure) { throw failure.reason }
      await this._cleanup()
    })
    this.stop()
    return this._closeWork
  }

  _cancel() {}
  _settle() {}
  _cleanup() {}
}

export { Transport, illegal, stopped }
