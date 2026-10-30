import { EventEmitter } from 'node:events'
import * as Err from './error.js'
import { isCid } from './util.js'
import * as utilm from './utilm.js'
import { ProtocolStore } from './storage.js'
import { Executor } from './executor.js'
import { ProcStartup } from './startup.js'
import * as wire from './wire.js'

class MonsterProc extends EventEmitter {
  constructor(role, id, nodes, localSend, databasePath, opts={}) {
    super()
    opts = typeof opts === 'function' ? opts() : opts
    if (typeof id !== 'string' || id.length === 0) {
      throw new Err.ErrorWithCode('id must be non-empty string', Err.ARGUMENT_ILLEGAL)
    }
    if (!Array.isArray(nodes) || !nodes.every((node) =>
      typeof node === 'string' && node.length > 0)) {
      throw new Err.ErrorWithCode('nodes must be non-empty string array', Err.ARGUMENT_ILLEGAL)
    }
    if (!nodes.includes(id) || new Set(nodes).size < 3) {
      throw new Err.ErrorWithCode('nodes must contain at least 3 ids including its id', Err.ARGUMENT_ILLEGAL)
    }
    if (typeof localSend !== 'function') {
      throw new Err.ErrorWithCode('localSend must be a function', Err.ARGUMENT_ILLEGAL)
    }
    this.localTimeout = opts.localTimeout ?? 1_000
    if (!Number.isSafeInteger(this.localTimeout) || this.localTimeout < 1) {
      throw new Err.ErrorWithCode('localTimeout must be a positive safe integer', Err.ARGUMENT_ILLEGAL)
    }
    this.role = role
    this.id = id
    this.nodes = [...new Set(nodes)].sort()
    this.tickCount = 0n
    this.store = new ProtocolStore(role, databasePath, opts)
    this.executor = new Executor()
    this.engine = null
    this.random = null
    this._localSend = localSend
    this._startup = new ProcStartup(this, opts)
    this._outputs = null
    this._localEvents = null
    this._watchdog = null
    this._stopped = false
    this._closeError = null
    this._fatalError = null
  }

  get isOpen() { return this._startup === null && !this._stopped }
  get log() { return this.store.log }
  get db() { return this.store.db }

  _emitSafe(type, value) {
    for (const listener of this.rawListeners(type)) {
      try {
        listener.call(this, value)
      } catch (err) {
        if (type === 'error') { continue }
        if (this._stopped) { this._emitSafe('error', Err.wrapError(err)) }
        else { this._fatal(err) }
      }
    }
  }

  _fatal(err) {
    if (this._stopped) { return }
    err = Err.wrapError(err)
    this._fatalError = err
    try { this._close(err) } catch { }
  }

  _send(message) {
    try {
      const work = this._localSend('gate', wire.encodeFrame(message))
      Promise.resolve(work).catch((err) => {
        this._fatal(Err.wrapError(err, Err.SEND_ERROR, 'Proc send '))
      })
    } catch (err) {
      this._fatal(Err.wrapError(err, Err.SEND_ERROR, 'Proc send '))
    }
  }

  _ack(message, fields={}) {
    this._send({ type: 'ack', cid: message.cid, ...fields })
  }

  _armWatchdog() {
    clearTimeout(this._watchdog)
    this._watchdog = setTimeout(() => {
      this._watchdog = null
      this._fatal(new Err.ErrorWithCode('Gate communication timeout', Err.LOCAL_TIMEOUT))
    }, this.localTimeout)
  }

  queueLocalEvent(type, error) {
    if (this._localEvents === null) {
      throw new Err.ErrorWithCode('Proc local event is outside its input', Err.MONSTER_ILLEGAL)
    }
    this._localEvents.push({ type, error })
  }

  output(to, message) {
    if (this._outputs === null || !this.executor.running || !this.db?.isTransaction) {
      throw new Err.ErrorWithCode('Proc output is outside its input', Err.MONSTER_ILLEGAL)
    }
    const payload = wire.encodePayload({ to, message })
    if (this.role === 'procA') {
      this._outputs.push(payload)
    } else {
      const checksum = wire.checksum(payload)
      const middle = checksum.length / 2
      this._outputs.push(this.role === 'procB'
        ? checksum.subarray(0, middle) : checksum.subarray(middle))
    }
  }

  close() { this._close() }

  _close(error = null) {
    if (this._stopped) {
      if (this._closeError !== null) { throw this._closeError }
      return
    }
    this._stopped = true
    clearTimeout(this._watchdog)
    this._watchdog = null
    this._outputs = null
    this._localEvents = null
    const failures = []
    try {
      this.engine?.stop()
    } catch (err) {
      failures.push(Err.wrapError(err))
    }
    this.executor.clear()
    try {
      this.store.close()
    } catch (err) {
      failures.push(Err.wrapError(err))
    }
    this._closeError = failures[0] ?? null
    if (error !== null) { this._emitSafe('error', error) }
    for (const failure of failures) { this._emitSafe('error', failure) }
    if (this._closeError !== null) { throw this._closeError }
    this._emitSafe('close')
  }

  _input(message, clusterMessage, warning) {
    if (message.tickCount !== this.tickCount + 1n || message.timems < this.executor.timems) {
      throw new Err.ErrorWithCode('Proc input order is illegal', Err.MONSTER_ILLEGAL)
    }
    this.tickCount = message.tickCount
    this.engine.tickCount = message.tickCount
    this._outputs = []
    this._localEvents = []
    this.db.exec('BEGIN IMMEDIATE')
    this.store.checkGeneration()
    this.executor.run(message.timems, () => {
      switch (message.type) {
        case 'start': this.engine.start(); break
        case 'cluster_message':
          if (warning) { this.engine.warn(warning) }
          else { this.engine.receive(message.from, clusterMessage) }
          break
        case 'append': this.engine.submit(message.clientId, message.items); break
        case 'drain': this.engine.drain(message.clientId); break
        case 'apply_result': this.engine.receiveApply(message.reply); break
        case 'tick': break
      }
    }, () => this.engine.change())
    this.db.exec('COMMIT')
    const outputs = this._outputs
    const localEvents = this._localEvents
    const tickCount = this.tickCount
    const fatalError = this.engine.fatalError
    this._outputs = null
    this._localEvents = null
    // A repair fence is durable, but a terminal input publishes no outputs.
    if (fatalError) {
      this._fatal(fatalError)
      return
    }
    // Complete startup after commit, before synchronous ACK delivery can reenter.
    if (message.type === 'start') { this._startup = null }
    this._armWatchdog()
    // After commit, attempt the completion ACK before running local listeners,
    // which may throw or close Proc.
    this._ack(message, { tickCount, outputs })
    for (const { type, error } of localEvents) { this._emitSafe(type, error) }
  }

  onReceive(from, frame) {
    if (from !== 'gate') {
      if (this._stopped) { return }
      this._emitSafe('warn', new Err.ErrorWithCode(
        `Proc sender ${from} is illegal, expected gate`, Err.RPC_ILLEGAL,
      ))
      return
    }
    let message = null
    try {
      message = wire.decodeFrame(frame)
      if (message?.type === 'close' && isCid(message.cid)) {
        try {
          this.close()
          this._ack(message)
        } catch (err) {
          this._send({ type: 'err', cid: message.cid, ...wire.errorToWire(err) })
        }
        return
      }
      if (this._stopped) { return }
      const parsed = utilm.parseProcRequest(message, this.id, this.nodes)
      if (this._startup !== null) {
        this._startup.receive(parsed)
      } else {
        if (!utilm.isInputType(message.type) || message.type === 'start') {
          throw new Err.ErrorWithCode('Proc startup is complete', Err.MONSTER_ILLEGAL)
        }
        this._input(message, parsed.clusterMessage, parsed.warning)
      }
    } catch (err) {
      if (this._stopped) { return }
      err = Err.wrapError(err)
      this._outputs = null
      this._localEvents = null
      this._fatal(err)
    }
  }
}

export { MonsterProc }
