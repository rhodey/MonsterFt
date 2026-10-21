import { mkdtemp, rm, lstat } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { Transport, illegal, stopped } from './transport.js'
import * as Err from '../error.js'

class UdsRouter extends Transport {
  constructor(names, opts = {}) {
    super(opts)
    if (!Array.isArray(names) || names.length === 0 ||
        names.some((name) => typeof name !== 'string' || name.length === 0) ||
        new Set(names).size !== names.length) {
      throw illegal('UDS endpoint names are illegal')
    }
    if (opts.paths !== undefined && opts.directory !== undefined) {
      throw illegal('UDS paths and directory cannot both be supplied')
    }
    this.names = [...names]
    this.paths = {}
    this._parent = opts.directory ?? '/tmp'
    this._directory = null
    if (typeof this._parent !== 'string' || this._parent.length === 0) { throw illegal('UDS directory is illegal') }
    if (opts.paths !== undefined) {
      if (!opts.paths || typeof opts.paths !== 'object') { throw illegal('UDS paths are illegal') }
      this.paths = Object.fromEntries(names.map((name) => {
        const path = opts.paths[name]
        if (typeof path !== 'string' || path.length === 0) { throw illegal(`UDS path for ${name} is illegal`) }
        return [name, resolve(path)]
      }))
      if (new Set(Object.values(this.paths)).size !== names.length) { throw illegal('UDS paths must differ') }
    }
    this._customPaths = opts.paths !== undefined
    this._connections = new Map()
    this._owners = new WeakMap()
    this._readyWait = Promise.withResolvers()
    this._readyWait.promise.catch(() => {})
  }

  async _start() {
    if (!this._customPaths) {
      this._directory = await mkdtemp(join(this._parent, 'mft-'))
      this.paths = Object.fromEntries(this.names.map((name, index) => [name, join(this._directory, String(index))]))
    }
    for (const name of this.names) {
      this._throwIfStopped()
      const path = this.paths[name]
      try {
        await lstat(path)
        throw illegal(`UDS path already exists: ${path}`)
      } catch (err) {
        if (err.code !== 'ENOENT') { throw err }
      }
      this._throwIfStopped()
      await this._listen({ path }, (connection) => this._install(name, connection))
    }
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
        this._fatal(new Err.ErrorWithCode(`UDS endpoint ${name} disconnected`, Err.SEND_ERROR))
      }
    })
    if (this._connections.size === this.names.length) { this._readyWait.resolve() }
  }

  _connectionError(connection, error) {
    if (!this._owners.has(connection)) { return }
    this._fatal(Err.wrapError(error, null, `UDS endpoint ${this._owners.get(connection)} `))
  }

  ready() {
    if (this._stopped) { return Promise.reject(this._error ?? stopped()) }
    return this._readyWait.promise
  }

  send(to, frame) {
    this._validateSend(frame)
    if (!this.names.includes(to)) { throw illegal('UDS destination is illegal') }
    const connection = this._connections.get(to)
    if (!connection || !connection.canSend(frame)) {
      const err = new Err.ErrorWithCode(`UDS endpoint ${to} is disconnected or its queue is full`, Err.SEND_ERROR)
      this._fatal(err)
      throw err
    }
    connection.write(frame)
    return true
  }

  _cancel() {
    this._readyWait.reject(this._error ?? stopped())
    this._connections.clear()
  }

  async _cleanup() {
    if (this._directory !== null) { await rm(this._directory, { recursive: true, force: true }) }
  }
}

export { UdsRouter }
