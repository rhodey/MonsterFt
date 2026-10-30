import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { Transport, illegal, stopped } from './transport.js'
import { oneShot } from '../util.js'
import * as Err from '../error.js'

class UdxRouter extends Transport {
  constructor(names, opts = {}) {
    super(opts)
    if (!Array.isArray(names) || names.length === 0 ||
        names.some((name) => typeof name !== 'string' || name.length === 0) ||
        new Set(names).size !== names.length) {
      throw illegal('UDX endpoint names are illegal')
    }
    this.names = [...names]
    this.paths = {}
    this._parent = opts.directory ?? '/tmp'
    this._directory = null
    if (typeof this._parent !== 'string' || this._parent.length === 0) { throw illegal('UDX directory is illegal') }
    this._connections = new Map()
    this._owners = new WeakMap()
    this._readyWait = oneShot()
    this._readyWait.promise.catch(() => {})
  }

  ready() {
    if (this._stopped) { return Promise.reject(this._error ?? stopped()) }
    return this._readyWait.promise
  }

  _cancel() {
    this._readyWait.reject(this._error ?? stopped())
    this._connections.clear()
  }

  async _cleanup() {
    if (this._directory !== null) { await rm(this._directory, { recursive: true, force: true }) }
  }

  _connectionError(connection, error) {
    if (!this._owners.has(connection)) { return }
    this._fatal(Err.wrapError(error, null, `UDX endpoint ${this._owners.get(connection)} `))
  }

  send(to, frame) {
    this._validateSend(frame)
    if (!this.names.includes(to)) { throw illegal('UDX destination is illegal') }
    const connection = this._connections.get(to)
    if (!connection || !connection.canSend(frame)) {
      const err = new Err.ErrorWithCode(`UDX endpoint ${to} is disconnected or its queue is full`, Err.SEND_ERROR)
      this._fatal(err)
      throw err
    }
    connection.write(frame)
    return true
  }

  _install(name, connection) {
    if (this._connections.has(name)) { connection.destroy(); return }
    this._connections.set(name, connection)
    this._owners.set(connection, name)
    connection.on('data', (frame) => {
      if (!this._stopped) { this._emitSafe('message', name, frame) }
    })
    connection.once('close', () => {
      if (!this._stopped) {
        this._fatal(new Err.ErrorWithCode(`UDX endpoint ${name} disconnected`, Err.SEND_ERROR))
      }
    })
    if (this._connections.size === this.names.length) { this._readyWait.resolve() }
  }

  async _start() {
    this._directory = await mkdtemp(join(this._parent, 'mft-'))
    this.paths = Object.fromEntries(this.names.map((name, index) => [name, join(this._directory, String(index))]))
    for (const name of this.names) {
      this._throwIfStopped()
      await this._listen({ path: this.paths[name] }, (connection) => this._install(name, connection))
    }
  }
}

export { UdxRouter }
