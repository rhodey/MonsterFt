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
          !Number.isSafeInteger(peer.basePort) || peer.basePort < 1 ||
          peer.basePort > 65535 || this.nodes.some((sender, index) =>
            sender !== node && peer.basePort + index > 65535)) {
        throw illegal(`TCP peer ${node} endpoint is illegal`)
      }
      return [node, { host: peer.host, basePort: peer.basePort }]
    }))
    this.bindHost = opts.bindHost ?? '0.0.0.0'
    this.reconnectDelay = opts.reconnectDelay ?? 250
    this.connectTimeout = opts.connectTimeout ?? 5_000
    if (typeof this.bindHost !== 'string' || this.bindHost.length === 0 ||
        !Number.isSafeInteger(this.reconnectDelay) || this.reconnectDelay < 0 ||
        !Number.isSafeInteger(this.connectTimeout) || this.connectTimeout < 1) {
      throw illegal('TCP transport options are illegal')
    }
    this._index = this.nodes.indexOf(id)
    this._incoming = new Map()
    this._outgoing = new Map()
    this._owners = new WeakMap()
    this._warned = new WeakSet()
    this._attempts = new Map()
    this._pending = new Set()
    this._timers = new Map()
  }

  _cancel() {
    for (const timer of this._timers.values()) { clearTimeout(timer) }
    this._timers.clear()
    for (const controller of this._attempts.values()) { controller.abort() }
    this._incoming.clear()
    this._outgoing.clear()
  }

  async _settle() { await Promise.all(this._pending) }

  _connectionError(connection, error) {
    const owner = this._owners.get(connection)
    if (this._stopped || !owner || owner.connections.get(owner.peer) !== connection ||
        this._warned.has(connection)) { return }
    this._warned.add(connection)
    this._emitSafe('warn', Err.wrapError(error, null, `TCP peer ${owner.peer} `))
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

  _install(peer, connection, direction) {
    this._track(connection)
    if (this._stopped) { connection.destroy(); return }
    const connections = direction === 'incoming' ? this._incoming : this._outgoing
    const previous = connections.get(peer)
    connections.set(peer, connection)
    this._owners.set(connection, { peer, connections })
    previous?.destroy()
    connection.on('data', (frame) => {
      if (this._stopped || connections.get(peer) !== connection) { return }
      if (direction === 'incoming') {
        this._emitSafe('message', peer, frame)
      } else {
        connection.destroy(new Err.ErrorWithCode(
          `TCP peer ${peer} sent on an outgoing connection`, Err.FRAME_CORRUPT,
        ))
      }
    })
    connection.on('error', (err) => this._connectionError(connection, err))
    connection.once('close', () => {
      if (this._stopped || connections.get(peer) !== connection) { return }
      connections.delete(peer)
      if (!this._warned.has(connection)) {
        this._emitSafe('warn', new Err.ErrorWithCode(`TCP peer ${peer} disconnected`, Err.SEND_ERROR))
      }
      if (direction === 'outgoing') { this._retry(peer) }
    })
  }

  _connect(peer) {
    if (this._stopped || this._attempts.has(peer) || this._outgoing.has(peer)) { return }
    const controller = new AbortController()
    this._attempts.set(peer, controller)
    const endpoint = this.peers[peer]
    const work = connectFrames({ host: endpoint.host, port: endpoint.basePort + this._index }, {
      ...this._limits, connectTimeout: this.connectTimeout, signal: controller.signal,
    }).then((connection) => this._install(peer, connection, 'outgoing'), (err) => {
      if (!this._stopped) { this._emitSafe('warn', Err.wrapError(err, null, `TCP peer ${peer} `)) }
    }).finally(() => {
      this._attempts.delete(peer)
      this._pending.delete(work)
      if (!this._outgoing.has(peer)) { this._retry(peer) }
    }).catch((err) => this._fatal(err))
    this._pending.add(work)
  }

  async _start() {
    for (let index = 0; index < this.nodes.length; index++) {
      if (index === this._index) { continue }
      this._throwIfStopped()
      const peer = this.nodes[index]
      await this._listen({ host: this.bindHost, port: this.peers[this.id].basePort + index },
        (connection) => this._install(peer, connection, 'incoming'))
    }
    this._throwIfStopped()
    for (const peer of this.nodes) {
      if (peer !== this.id) { this._connect(peer) }
    }
  }
}

export { TcpCluster }
