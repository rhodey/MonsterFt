import crypto from 'node:crypto'
import { EventEmitter } from 'node:events'
import * as Err from './error.v2.js'
import * as wire from './wire.v2.js'

const PROCS = ['procA', 'procB', 'procC']
const COMPONENTS = [...PROCS, 'apply']
const noop = () => {}
const nodeClosed = () => new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
const illegal = (message) => new Err.ErrorWithCode(message, Err.ARGUMENT_ILLEGAL)
const rpcIllegal = (message) => new Err.ErrorWithCode(message, Err.RPC_ILLEGAL)

class Gate extends EventEmitter {
  constructor(id, nodes, send, localSend, opts = {}) {
    super()
    opts = typeof opts === 'function' ? opts() : opts
    if (!opts || typeof opts !== 'object') {
      throw illegal('options must be an object')
    }
    if (typeof id !== 'string' || id.length === 0) {
      throw illegal('id must be non-empty string')
    }
    if (!Array.isArray(nodes) || !nodes.every((node) =>
      typeof node === 'string' && node.length > 0)) {
      throw illegal('nodes must be non-empty string array')
    }
    this.nodes = [...new Set(nodes)].sort()
    if (this.nodes.length < 3 || !this.nodes.includes(id)) {
      throw illegal('nodes must contain at least 3 ids including its id')
    }
    if (this.nodes.some((node) => [...COMPONENTS, 'gate', 'event', 'client'].includes(node))) {
      throw illegal('node ids must not name internal destinations')
    }
    if (typeof send !== 'function' || typeof localSend !== 'function') {
      throw illegal('send and localSend must be functions')
    }
    this.id = id
    this.tickInterval = opts.tickInterval ?? 10
    this.localTimeout = opts.localTimeout ?? 1_000
    this.applyTimeout = opts.applyTimeout ?? 5_000
    for (const name of ['tickInterval', 'localTimeout', 'applyTimeout']) {
      if (!Number.isSafeInteger(this[name]) || this[name] < 1) {
        throw illegal(`${name} must be a positive safe integer`)
      }
    }
    this.tickCount = 0n
    this.timems = 0
    this.state = 'follower'
    this.leader = null
    this.term = 0n
    this.seq = -1n
    this.commitSeq = -1n
    this.commitTerm = -1n
    this.appliedSeq = -1n
    this.followers = []
    this._send = send
    this._localSend = localSend
    this._open = false
    this._opening = false
    this._closing = false
    this._openWork = null
    this._closeWork = null
    this._drainWork = null
    this._shutdownError = null
    this._requests = new Map()
    this._clients = new Map()
    this._shutdownWaiters = new Set()
    this._helloDone = new Set()
    this._queue = []
    this._current = null
    this._pumpQueued = false
    this._tickQueued = false
    this._tickTimer = null
    this._seed = null
  }

  get isOpen() { return this._open && !this._closing }

  _emitSafe(type, value) {
    for (const listener of this.rawListeners(type)) {
      try {
        listener.call(this, value)
      } catch (err) {
        if (type !== 'error') { this._emitSafe('error', err) }
      }
    }
  }

  _request(to, message, timeout = this.localTimeout, allowClosing = false, frame = null) {
    if (this._closing && !allowClosing) {
      return Promise.reject(this._shutdownError ?? nodeClosed())
    }
    const cid = message.cid ?? crypto.randomUUID()
    message = { ...message, cid }
    const key = `${to}:${cid}`
    return new Promise((resolve, reject) => {
      if (this._requests.has(key)) {
        reject(rpcIllegal('duplicate local request cid'))
        return
      }
      const finish = (err, reply) => {
        const request = this._requests.get(key)
        if (request?.finish !== finish) { return }
        this._requests.delete(key)
        clearTimeout(request.timer)
        err ? reject(err) : resolve(reply)
      }
      const code = message.type === 'execute' ? Err.APPLY_TIMEOUT : Err.LOCAL_TIMEOUT
      const timer = setTimeout(() => finish(new Err.ErrorWithCode(
        `${to} ${message.type} timeout`, code,
      )), timeout)
      this._requests.set(key, { finish, timer, allowClosing, type: message.type })
      try {
        const sent = this._localSend(to, frame ?? wire.encodeFrame(message))
        Promise.resolve(sent).catch((err) => finish(Err.wrapError(
          err, Err.SEND_ERROR, '(localSend) ',
        )))
      } catch (err) {
        finish(Err.wrapError(err, Err.SEND_ERROR, '(localSend) '))
      }
    })
  }

  onReceive(from, frame) {
    if (this._closing && this._requests.size === 0) { return }
    if (!this._opening && !this._open && !this._closing) { return }
    try {
      const message = wire.decodeFrame(frame)
      if (COMPONENTS.includes(from)) {
        if (message?.type !== 'ack' && message?.type !== 'err') {
          throw rpcIllegal('local reply must be ACK or ERR')
        }
        if (typeof message.cid !== 'string' || message.cid.length === 0) {
          throw rpcIllegal('local reply has invalid cid')
        }
        const request = this._requests.get(`${from}:${message.cid}`)
        if (!request) { return }
        if (request.type === 'hello' && message.type === 'ack') { this._helloDone.add(from) }
        request.finish(message.type === 'err'
          ? wire.errorFromWire(message.error ?? message) : null, message)
      } else {
        if (this._closing) { return }
        if (!this.nodes.includes(from) || from === this.id) {
          throw rpcIllegal('cluster frame has invalid sender')
        }
        this._enqueue({ kind: 'rpc', from, frame })
      }
    } catch (err) {
      this._fatal(err)
    }
  }

  async _hello(to) {
    while (!this._closing) {
      try {
        const message = { type: 'hello' }
        if (to !== 'apply') { message.seed = this._seed }
        await this._request(to, message)
        this._helloDone.add(to)
        return
      } catch (err) {
        if (err.code !== Err.LOCAL_TIMEOUT || this._closing) { throw err }
      }
    }
    throw this._shutdownError ?? nodeClosed()
  }

  _phase(type, components = PROCS) {
    return Promise.all(components.map((to) => this._request(to, { type })))
  }

  open() {
    if (this._closing) { return Promise.reject(this._shutdownError ?? nodeClosed()) }
    if (this._openWork !== null) { return this._openWork }
    this._opening = true
    do { this._seed = crypto.randomBytes(16) } while (this._seed.every((byte) => byte === 0))
    this._openWork = (async () => {
      await Promise.all(COMPONENTS.map((to) => this._hello(to)))
      // Opening inspects peer files; do not overlap those reads with fresh
      // schema creation or another owner's switch into WAL mode.
      for (const to of COMPONENTS) { await this._request(to, { type: 'open' }) }
      const captured = await this._phase('capture')
      const selections = captured.map((reply) => {
        const selection = reply.selection
        if (!Buffer.isBuffer(selection?.state) || !Buffer.isBuffer(selection.receipt) ||
            !PROCS.includes(selection.donor)) {
          throw rpcIllegal('capture reply has invalid selection')
        }
        return wire.encodePayload(selection)
      })
      if (!selections.every((selection) => selection.equals(selections[0]))) {
        throw new Err.ErrorWithCode('startup selections disagree', Err.LOCAL_DIVERGENCE)
      }
      await this._phase('adopt')
      await this._phase('reconcile')
      await this._phase('ready', ['apply'])
      if (this._closing) { throw this._shutdownError ?? nodeClosed() }
      this._open = true
      await new Promise((resolve, reject) => {
        this._queue.unshift({ input: { kind: 'start' }, resolve, reject })
        this._schedulePump()
      })
      if (this._closing) { throw this._shutdownError ?? nodeClosed() }
      this._opening = false
      this._scheduleTick()
    })().catch((err) => {
      this._fatal(err)
      throw err
    })
    return this._openWork
  }

  _scheduleTick() {
    if (!this.isOpen) { return }
    this._tickTimer = setTimeout(() => {
      this._tickTimer = null
      if (!this.isOpen) { return }
      if (!this._tickQueued) {
        this._tickQueued = true
        this._enqueue({ kind: 'tick' })
      }
      this._scheduleTick()
    }, this.tickInterval)
  }

  _enqueue(input) {
    if (this._closing) { return }
    this._queue.push({ input, resolve: noop, reject: noop })
    this._schedulePump()
  }

  _schedulePump() {
    if (!this.isOpen || this._current !== null || this._pumpQueued) { return }
    this._pumpQueued = true
    queueMicrotask(() => {
      this._pumpQueued = false
      if (!this.isOpen || this._current !== null) { return }
      const work = this._queue.shift()
      if (!work) { return }
      work.afterComplete = []
      this._current = work
      if (work.input.kind === 'tick') { this._tickQueued = false }
      this.tickCount += 1n
      this.timems = Math.max(this.timems, Date.now())
      const input = {
        type: 'input', cid: crypto.randomUUID(), tickCount: this.tickCount,
        timems: this.timems, ...work.input,
      }
      this._runInput(input).then(() => {
        work.resolve()
        this._current = null
        for (const finish of work.afterComplete) { finish() }
        this._schedulePump()
      }, (err) => {
        work.reject(err)
        this._current = null
        this._fatal(err)
      })
    })
  }

  async _runInput(input) {
    const completed = new Map()
    let released = false
    await Promise.all(PROCS.map(async (to) => {
      try {
        const reply = await this._request(to, input)
        if (this._closing) { throw this._shutdownError ?? nodeClosed() }
        if (!Array.isArray(reply.outputs) || !reply.outputs.every((value) =>
          Buffer.isBuffer(value) && (to === 'procA' || value.length === 32))) {
          throw rpcIllegal('completion has invalid outputs')
        }
        const hashes = to === 'procA' ? reply.outputs.map(wire.checksum) : reply.outputs
        for (const other of completed.values()) {
          if (hashes.length !== other.hashes.length ||
              !hashes.every((hash, i) => hash.equals(other.hashes[i]))) {
            throw new Err.ErrorWithCode('Proc outputs disagree', Err.LOCAL_DIVERGENCE)
          }
        }
        completed.set(to, { hashes, outputs: reply.outputs })
        const a = completed.get('procA')
        if (!released && completed.size >= 2 && a) {
          released = true
          const agreed = completed.get('procB') ?? completed.get('procC')
          for (let i = 0; i < a.outputs.length; i += 1) {
            if (this._closing) { break }
            this._release(a.outputs[i], agreed.hashes[i])
          }
        }
      } catch (err) {
        // Stop before another already-queued completion can release outputs.
        this._fatal(err)
        throw err
      }
    }))
  }

  _release(payload, hash) {
    const envelope = wire.decodePayload(payload)
    const { to, message } = envelope ?? {}
    if (typeof message?.type !== 'string') {
      throw rpcIllegal('output has invalid message')
    }
    if (to === 'apply') {
      if (message.type !== 'execute' || typeof message.cid !== 'string') {
        throw rpcIllegal('invalid Apply request')
      }
      // The pending request exists before sending, including synchronous transports.
      this._request('apply', message, this.applyTimeout, false,
        wire.framePayload(payload, hash)).then((reply) => {
        if (this._closing) { return }
        this._enqueue({ kind: 'apply', reply })
      }, (err) => this._fatal(err))
    } else if (to === 'client') {
      const pending = this._clients.get(message.cid)
      if (!pending) { return }
      if (message.type !== 'err' && message.type !== 'ack') {
        throw rpcIllegal('client output must be ACK or ERR')
      }
      const finish = () => {
        if (!this._clients.delete(message.cid)) { return }
        if (message.type === 'err') { pending.reject(wire.errorFromWire(message.error ?? message)) }
        else { pending.resolve(message) }
      }
      // A successful drain must let the last Proc finish before closing owners.
      if (pending.kind === 'drain' && message.type === 'ack') {
        this._current.afterComplete.push(finish)
      } else { finish() }
    } else if (to === 'event') {
      this._event(message)
    } else if (this.nodes.includes(to) && to !== this.id) {
      try {
        const sent = this._send(to, wire.framePayload(payload, hash))
        Promise.resolve(sent).catch((err) => this._sendFailure(err))
      } catch (err) {
        this._sendFailure(err)
      }
    } else {
      throw rpcIllegal('output has invalid destination')
    }
  }

  _sendFailure(err) {
    if (this.isOpen) {
      this._emitSafe('warn', Err.wrapError(err, Err.SEND_ERROR, '(send) '))
    }
  }

  _event({ status, ...message }) {
    if (!['change', 'commit', 'cmd', 'sync'].includes(message.type)) {
      throw rpcIllegal('output has invalid event type')
    }
    if (status === null || typeof status !== 'object') {
      throw rpcIllegal('output has invalid event status')
    }
    for (const key of ['state', 'term', 'seq', 'commitSeq', 'commitTerm', 'appliedSeq', 'followers']) {
      if (Object.hasOwn(status, key)) { this[key] = status[key] }
    }
    if (Object.hasOwn(status, 'leaderId')) { this.leader = status.leaderId }
    if (message.type === 'change') {
      this._emitSafe('change', {
        ...message, ...status, id: this.id, nodes: [...this.nodes], leader: this.leader,
        open: this.isOpen,
      })
    } else if (message.type === 'commit') {
      this._emitSafe('commit', message.seq)
    } else {
      this._emitSafe(message.type, message)
    }
  }

  _command(kind, fields = {}) {
    if (!this.isOpen) { return Promise.reject(this._shutdownError ?? nodeClosed()) }
    const requestId = crypto.randomUUID()
    return new Promise((resolve, reject) => {
      this._clients.set(requestId, { resolve, reject, kind })
      this._enqueue({ kind, requestId, ...fields })
    })
  }

  _outcomes(message, count) {
    if (typeof message.cmdSeq !== 'bigint' || message.cmdSeq < 0n ||
        !Array.isArray(message.results) || message.results.length !== count) {
      throw rpcIllegal('command result has invalid sequence or results')
    }
    return message.results.map((item) => {
      if (!Array.isArray(item)) { throw rpcIllegal('invalid command result') }
      if (item[0] === 0 && item.length === 2) {
        return { status: 'fulfilled', value: item[1] }
      }
      if (item[0] !== 1 || (item.length !== 4 && item.length !== 5) ||
          typeof item[1] !== 'string' ||
          (item[2] !== null && !Number.isSafeInteger(item[2])) ||
          (item[3] !== null && !Number.isSafeInteger(item[3])) ||
          (item.length === 5 && typeof item[4] !== 'string')) {
        throw rpcIllegal('invalid rejected command result')
      }
      const reason = wire.errorFromWire({ msg: item[1], code: item[2], sqlCode: item[3], stack: item[4] })
      return { status: 'rejected', reason }
    })
  }

  async append(data) {
    if (!this.isOpen) { throw this._shutdownError ?? nodeClosed() }
    if (this._drainWork !== null) { throw new Err.ErrorWithCode('node draining', Err.DRAINING) }
    if (!Buffer.isBuffer(data)) { throw illegal('data must be buffer') }
    const result = await this._command('append', { items: [data] })
    const [outcome] = this._outcomes(result, 1)
    if (outcome.status === 'rejected') { throw outcome.reason }
    return [result.cmdSeq, outcome.value]
  }

  async appendBatch(items) {
    if (!this.isOpen) { throw this._shutdownError ?? nodeClosed() }
    if (this._drainWork !== null) { throw new Err.ErrorWithCode('node draining', Err.DRAINING) }
    if (!Array.isArray(items) || items.length === 0 ||
        !items.every((item) => Buffer.isBuffer(item))) {
      throw illegal('data must be non-empty array of buffers')
    }
    const result = await this._command('append', { items })
    return [result.cmdSeq, this._outcomes(result, items.length)]
  }

  drainCmd() {
    if (this._drainWork !== null) { return this._drainWork }
    if (!this.isOpen) { return Promise.reject(this._shutdownError ?? nodeClosed()) }
    this._drainWork = this._command('drain').then(() => this.close())
    this._drainWork.catch(noop)
    return this._drainWork
  }

  awaitEvent(type, matches = () => true) {
    if (!this.isOpen) { return Promise.reject(this._shutdownError ?? nodeClosed()) }
    return new Promise((resolve, reject) => {
      let settled = false
      const finish = (settle, value) => {
        if (settled) { return }
        settled = true
        this.removeListener(type, listener)
        this._shutdownWaiters.delete(fail)
        settle(value)
      }
      const fail = (err) => finish(reject, err)
      const listener = (value) => {
        try {
          if (matches(value)) { finish(resolve, value) }
        } catch (err) { fail(err) }
      }
      this._shutdownWaiters.add(fail)
      this.on(type, listener)
    })
  }

  async awaitLeader(commit = false) {
    if (!this.isOpen) { throw this._shutdownError ?? nodeClosed() }
    const ready = () => (this.state === 'leader' ||
      (this.state === 'follower' && this.leader !== null)) &&
      (!commit || this.commitTerm === this.term)
    if (ready()) { return }
    await this.awaitEvent('change', ready)
  }

  _stop(err) {
    this._closing = true
    this._open = false
    this._opening = false
    this._shutdownError = err
    clearTimeout(this._tickTimer)
    this._tickTimer = null
    for (const request of [...this._requests.values()]) {
      if (!request.allowClosing) { request.finish(err) }
    }
    for (const pending of this._clients.values()) { pending.reject(err) }
    this._clients.clear()
    for (const fail of [...this._shutdownWaiters]) { fail(err) }
    for (const work of this._queue.splice(0)) { work.reject(err) }
  }

  _fatal(error) {
    if (this._closing) { return }
    const err = Err.wrapError(error)
    this._stop(err)
    this._emitSafe('error', err)
  }

  _closeComponents() {
    if (this._closeWork !== null) { return this._closeWork }
    // Publish the promise before synchronous owner listeners can call close again.
    this._closeWork = Promise.resolve().then(async () => {
      const results = await Promise.allSettled([...this._helloDone].map((to) =>
        this._request(to, { type: 'close' }, this.localTimeout, true)))
      const rejected = results.find((result) => result.status === 'rejected')
      if (rejected) { throw rejected.reason }
      this._emitSafe('close')
    }).catch((err) => {
      this._closeWork = null
      throw err
    })
    return this._closeWork
  }

  close() {
    if (!this._closing) { this._stop(nodeClosed()) }
    return this._closeComponents()
  }
}

export { Gate }
