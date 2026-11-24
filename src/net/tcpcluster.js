import { connectFrames } from './stream.js'
import { Transport, illegal } from './transport.js'
import * as Err from '../error.js'

class TcpCluster extends Transport {
  constructor(id, peers, opts = {}) {
    super(opts)
    if (!peers || typeof peers !== 'object' || Array.isArray(peers)) {
      throw illegal('TCP peers are illegal')
    }
    this.id = id
    this.nodes = Object.keys(peers).sort()
    if (!this.nodes.includes(id) || this.nodes.some((node) => node.length === 0)) {
      throw illegal('TCP node id is illegal')
    }
    this.peers = Object.fromEntries(this.nodes.map((node) => {
      const peer = peers[node]
      if (!peer || typeof peer.host !== 'string' || peer.host.length === 0 ||
          !Number.isSafeInteger(peer.port) || peer.port < 1 || peer.port > 65535) {
        throw illegal(`TCP peer ${node} endpoint is illegal`)
      }
      return [node, { host: peer.host, port: peer.port }]
    }))
    this.bindHost = opts.bindHost ?? '0.0.0.0'
    this.reconnectDelay = opts.reconnectDelay ?? 250
    this.connectTimeout = opts.connectTimeout ?? 5_000
    if (typeof this.bindHost !== 'string' || this.bindHost.length === 0 ||
        !Number.isSafeInteger(this.reconnectDelay) || this.reconnectDelay < 0 ||
        !Number.isSafeInteger(this.connectTimeout) || this.connectTimeout < 1) {
      throw illegal('TCP transport options are illegal')
    }
    this._outgoing = new Map()
    this._attempts = new Map()
    this._pending = new Set()
    this._timers = new Map()
  }

  _cancel() {
    for (const timer of this._timers.values()) { clearTimeout(timer) }
    this._timers.clear()
    for (const controller of this._attempts.values()) { controller.abort() }
    this._outgoing.clear()
  }

  async _settle() { await Promise.all(this._pending) }

  _connectionError(connection, error) {
    this._emitSafe('warn', Err.wrapError(error, null, 'TCP incoming connection '))
  }

  send(to, frame) {
    this._validateSend(frame)
    if (to === this.id || !this.nodes.includes(to)) { throw illegal('TCP destination is illegal') }
    const connection = this._outgoing.get(to)
    if (!connection || connection.destroyed) { return false }
    if (!connection.canSend(frame)) {
      this._emitSafe('warn', new Err.ErrorWithCode(`TCP peer ${to} queue is full`, Err.SEND_ERROR))
      return false
    }
    connection.write(frame)
    return true
  }

  _retry(peer) {
    if (this._stopped || this._outgoing.has(peer) ||
        this._attempts.has(peer) || this._timers.has(peer)) { return }
    this._timers.set(peer, setTimeout(() => {
      this._timers.delete(peer)
      this._connect(peer)
    }, this.reconnectDelay))
  }

  _installOutgoing(peer, connection) {
    this._track(connection)
    if (this._stopped) { connection.destroy(); return }
    this._outgoing.set(peer, connection)
    connection.once('data', () => {
      connection.destroy(new Err.ErrorWithCode(
        `TCP peer ${peer} sent on an outgoing connection`, Err.FRAME_CORRUPT,
      ))
    })
    connection.once('close', () => {
      if (this._stopped) { return }
      this._outgoing.delete(peer)
      const error = connection.errored
        ? Err.wrapError(connection.errored, null, `TCP peer ${peer} `)
        : new Err.ErrorWithCode(`TCP peer ${peer} disconnected`, Err.SEND_ERROR)
      this._emitSafe('warn', error)
      this._retry(peer)
    })
  }

  _connect(peer) {
    if (this._stopped || this._attempts.has(peer) || this._outgoing.has(peer)) { return }
    const controller = new AbortController()
    this._attempts.set(peer, controller)
    const work = connectFrames(this.peers[peer], {
      ...this._limits, connectTimeout: this.connectTimeout, signal: controller.signal,
    }).then((connection) => this._installOutgoing(peer, connection), (err) => {
      if (!this._stopped) { this._emitSafe('warn', Err.wrapError(err, null, `TCP peer ${peer} `)) }
    }).finally(() => {
      this._attempts.delete(peer)
      this._pending.delete(work)
      if (!this._outgoing.has(peer)) { this._retry(peer) }
    }).catch((err) => this._fatal(err))
    this._pending.add(work)
  }

  async _start() {
    await this._listen({ host: this.bindHost, port: this.peers[this.id].port }, (connection) => {
      connection.on('data', (frame) => {
        if (!this._stopped) { this._emitSafe('message', frame) }
      })
      connection.once('close', () => {
        if (!this._stopped && !connection.errored) {
          this._emitSafe('warn', new Err.ErrorWithCode('TCP incoming connection closed', Err.SEND_ERROR))
        }
      })
    })
    this._throwIfStopped()
    for (const peer of this.nodes) {
      if (peer !== this.id) { this._connect(peer) }
    }
  }
}

export { TcpCluster }
