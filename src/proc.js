import { EventEmitter } from 'node:events'
import * as Err from './error.js'
import { isCid, isSeed } from './util.js'
import { ProtocolStore } from './storage.js'
import { Executor } from './executor.js'
import { Random } from './random.js'
import { MonsterEngine } from './monster.js'
import * as wire from './wire.js'

const illegal = (message) => new Err.ErrorWithCode(message, Err.RPC_ILLEGAL)

class MonsterProc extends EventEmitter {
  constructor(role, id, nodes, localSend, databasePath, opts={}) {
    super()
    opts = typeof opts === 'function' ? opts() : opts
    if (typeof id !== 'string' || id.length === 0 || !Array.isArray(nodes) ||
        !nodes.every((node) => typeof node === 'string' && node.length > 0) ||
        !nodes.includes(id) || new Set(nodes).size < 3 || typeof localSend !== 'function') {
      throw new Err.ErrorWithCode('Proc arguments are illegal', Err.ARGUMENT_ILLEGAL)
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
    this._opts = opts
    this._localSend = localSend
    this._seed = null
    this._pastHello = false
    this._phase = 'hello'
    this._outputs = null
    this._diagnostics = null
    this._watchdog = null
    this._stopped = false
    this._closeError = null
    this._fatalError = null
  }

  get isOpen() { return this._phase === 'runtime' && !this._stopped }
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

  _output(to, message) {
    if (this._outputs === null || !this.executor.running || !this.db?.isTransaction) {
      throw new Err.ErrorWithCode('Proc output is outside its input', Err.MONSTER_ILLEGAL)
    }
    const payload = wire.encodePayload({ to, message })
    this._outputs.push(this.role === 'procA' ? payload : wire.checksum(payload))
  }

  _validateInput(message) {
    if (typeof message.tickCount !== 'bigint' || message.tickCount !== this.tickCount + 1n ||
        !Number.isSafeInteger(message.timems) || message.timems < this.executor.timems) {
      throw illegal('Proc input order is illegal')
    }
    if (message.kind === 'start') {
      if (this._phase !== 'reconciled') { throw illegal('Proc cannot start before reconciliation') }
      return null
    }
    if (!this.isOpen) { throw illegal('Proc runtime has not started') }
    if (message.kind === 'rpc') {
      if (!this.nodes.includes(message.from) || message.from === this.id) {
        throw illegal('Proc cluster sender is illegal')
      }
      // The remote Gate forwards the original agreed envelope unchanged.
      const envelope = wire.decodeFrame(message.frame)
      if (envelope?.to !== this.id || envelope.message?.from !== message.from ||
          typeof envelope.message?.type !== 'string') {
        throw illegal('Proc cluster envelope is illegal')
      }
      return envelope.message
    }
    if (message.kind === 'append') {
      if (!isCid(message.requestId) || !Array.isArray(message.items) || message.items.length === 0 ||
          !message.items.every(Buffer.isBuffer)) {
        throw illegal('Proc append input is illegal')
      }
    } else if (message.kind === 'drain') {
      if (!isCid(message.requestId)) { throw illegal('Proc drain input is illegal') }
    } else if (message.kind === 'apply') {
      if (message.reply?.type !== 'ack' || !isCid(message.reply.cid)) {
        throw illegal('Proc Apply reply is illegal')
      }
    } else if (message.kind !== 'tick') {
      throw illegal('Proc input kind is illegal')
    }
    return null
  }

  _input(message) {
    const clusterMessage = this._validateInput(message)
    this.tickCount = message.tickCount
    this.engine.tickCount = message.tickCount
    this._outputs = []
    this._diagnostics = []
    this.db.exec('BEGIN IMMEDIATE')
    this.store.checkGeneration()
    this.executor.run(message.timems, () => {
      switch (message.kind) {
        case 'start': this.engine.start(); break
        case 'rpc': this.engine.receive(message.from, clusterMessage); break
        case 'append': this.engine.submit(message.requestId, message.items); break
        case 'drain': this.engine.drain(message.requestId); break
        case 'apply': this.engine.receiveApply(message.reply); break
        case 'tick': break
      }
    }, () => this.engine.change())
    this.db.exec('COMMIT')
    const outputs = this._outputs
    const diagnostics = this._diagnostics
    const tickCount = this.tickCount
    const fatalError = this.engine.fatalError
    this._outputs = null
    this._diagnostics = null
    this._phase = 'runtime'
    // A repair fence is durable, but a terminal input publishes no outputs.
    if (fatalError) {
      this._fatal(fatalError)
      return
    }
    this._armWatchdog()
    this._ack(message, { tickCount, outputs })
    for (const { type, error } of diagnostics) { this._emitSafe(type, error) }
  }

  onReceive(from, frame) {
    if (from !== 'gate') { return }
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
      if (!isCid(message?.cid)) { throw illegal('Proc request cid is illegal') }
      if (message.type === 'hello') {
        if (this._pastHello) { throw illegal('Proc received hello after startup') }
        // Validate without advancing the shared generator. A new Gate may start
        // hello again while this Proc is still waiting in the hello phase.
        if (!isSeed(message.seed)) {
          throw illegal('seed must be a nonzero 16-byte Buffer')
        }
        this._seed = message.seed
        this._ack(message)
        return
      }
      if (this._seed === null) { throw illegal('Proc has not received hello') }
      if (!this._pastHello) {
        this._pastHello = true
        this.random = new Random(this._seed)
      }
      if (message.type === 'open' && this._phase === 'hello') {
        this.store.open(this._seed)
        this._phase = 'opened'
        this._ack(message)
      } else if (message.type === 'capture' && this._phase === 'opened') {
        const selection = this.store.capture()
        this._phase = 'captured'
        this._ack(message, { selection })
      } else if (message.type === 'adopt' && this._phase === 'captured') {
        this.store.adopt()
        this._phase = 'adopted'
        this._ack(message)
      } else if (message.type === 'reconcile' && this._phase === 'adopted') {
        this.store.reconcile()
        this.engine = new MonsterEngine(this.id, this.nodes, this.store,
          this.executor, this.random, (to, value) => this._output(to, value), this._opts)
        this.engine.diagnostic = (type, error) => {
          if (this._diagnostics === null) {
            throw new Err.ErrorWithCode('Proc diagnostic is outside its input', Err.MONSTER_ILLEGAL)
          }
          this._diagnostics.push({ type, error })
        }
        this._phase = 'reconciled'
        this._ack(message)
      } else if (message.type === 'input' && this.engine !== null) {
        this._input(message)
      } else {
        throw illegal('Proc startup phase is illegal')
      }
    } catch (err) {
      if (this._stopped) { return }
      err = Err.wrapError(err)
      this._outputs = null
      this._diagnostics = null
      this._fatal(err)
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
    this._diagnostics = null
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
}

export { MonsterProc }
