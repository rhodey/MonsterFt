// https://raft.github.io/raft.pdf
import crypto from 'node:crypto'
import { EventEmitter } from 'node:events'
import * as Err from './error.js'
import { SQLiteLog } from './log.js'
import * as util from './util.js'

// states
const FOLLOWER = 'follower'
const CANDIDATE = 'candidate'
const LEADER = 'leader'

// rpc types
const VOTE_REQUEST = 'vote_request'
const VOTE = 'vote'
const APPEND = 'append'
const ACK = 'ack'
const ERR = 'err'

const noop = () => {}
const ready = Promise.resolve()
const applyNull = (node, bufs) => bufs.map(() => null)
const max = (a, b) => a > b ? a : b
const min = (a, b) => a < b ? a : b
const rand = (min, max) => Math.floor(Math.random() * (max - min)) + min
const isEntries = (data) => Array.isArray(data) && data.length > 0 &&
  data.every((entry) => Buffer.isBuffer(entry) && entry.length >= 8)
const isForwardData = (data) =>
  (Buffer.isBuffer(data) && data.length > 0) ||
  (Array.isArray(data) && data.length > 0 &&
    data.every((buf) => Buffer.isBuffer(buf) && buf.length > 0))

const defaults = {
  // how long before nodes try to elect themselves
  electionTimeout: 1_500,
  // how long before leader expires a follower
  pingTimeout: 1_500,
  // how long before throw error for a append
  appendTimeout: 1_500,
  // send <= rpcMax bufs per rpc
  rpcMax: 1024,
  // state machine
  apply: applyNull,
  // apply <= applyMax bufs per call
  applyMax: 1024,
  // number of nodes required for quorum
  quorum: undefined,
}

const ACK_OPERATION = Object.freeze({
  PING: Object.freeze({
    timeoutKey: 'pingTimeout',
    code: Err.PING_TIMEOUT,
    operation: 'ping',
  }),
  APPEND: Object.freeze({
    timeoutKey: 'appendTimeout',
    code: Err.APPEND_TIMEOUT,
    operation: 'append',
  }),
})

class RaftNode extends EventEmitter {
  constructor(id, nodes, send, databasePath, opts={}) {
    opts = typeof opts === 'function' ? opts() : opts
    opts = { ...defaults, ...opts }
    opts.apply = opts.apply ?? applyNull
    super()
    if (typeof id !== 'string' || id.length === 0) {
      throw new Err.ErrorWithCode(
        'id must be non-empty string', Err.ARGUMENT_ILLEGAL)
    }
    let nodeIds = null
    try {
      nodeIds = Array.from(nodes)
    } catch {
      throw new Err.ErrorWithCode(
        'nodes must be non-empty string array', Err.ARGUMENT_ILLEGAL)
    }
    if (!nodeIds.every((nodeId) => typeof nodeId === 'string' && nodeId.length > 0)) {
      throw new Err.ErrorWithCode(
        'nodes must be non-empty string array', Err.ARGUMENT_ILLEGAL)
    }
    this.id = id
    this.nodes = [...new Set(nodeIds)].sort()
    if (this.nodes.length < 3) {
      throw new Err.ErrorWithCode(
        'nodes must contain at least 3 ids', Err.ARGUMENT_ILLEGAL)
    }
    if (!this.nodes.includes(id)) {
      throw new Err.ErrorWithCode(
        'nodes must contain its id', Err.ARGUMENT_ILLEGAL)
    }
    const sendError = (err) => {
      if (!this.isOpen) { return }
      err = Err.wrapError(err, Err.SEND_ERROR, '(send) ')
      this._emitSafe('warn', err)
    }
    this.send = (to, msg) => {
      if (!this.isOpen) { return }
      try {
        msg.from = this.id
        const ok = send(to, msg)
        if (!(ok instanceof Promise)) { return }
        ok.catch(sendError)
      } catch (err) {
        sendError(err)
      }
    }
    this.log = new SQLiteLog(databasePath, opts)
    const quorum = Math.floor(this.nodes.length / 2) + 1
    this.quorum = opts.quorum ?? quorum
    if (!Number.isSafeInteger(this.quorum) || this.quorum < quorum) {
      throw new Err.ErrorWithCode(
        `quorum must be int >= ${quorum}`, Err.ARGUMENT_ILLEGAL)
    }
    if (this.quorum > this.nodes.length) {
      throw new Err.ErrorWithCode(
        'quorum must be <= nodes length', Err.ARGUMENT_ILLEGAL)
    }
    if (!Number.isSafeInteger(opts.rpcMax) || opts.rpcMax <= 0) {
      throw new Err.ErrorWithCode(
        'rpcMax must be int > 0', Err.ARGUMENT_ILLEGAL)
    }
    if (!Number.isSafeInteger(opts.applyMax) || opts.applyMax <= 0) {
      throw new Err.ErrorWithCode(
        'applyMax must be int > 0', Err.ARGUMENT_ILLEGAL)
    }
    opts.quorum = this.quorum
    this.opts = opts
    this.opts.rpcMax = BigInt(opts.rpcMax)

    this.state = null
    this.leader = null
    this.followers = []
    this.term = null
    this._votedFor = null
    this._votes = []
    this._pingms = 0
    this._pongs = new Map()

    this._acks = new Map()
    this._leaderReady = null
    this._replication = new Map()

    this._commitSeq = -1n
    this._commitTerm = -1n
    this._applySeq = -1n
    this._applyPrev = ready
    this._applyWaiters = new Set()

    this._open = false
    this._closing = false
    this._shutdownError = null
    this._shutdownWaiters = new Set()
    this._fatalErrors = []
    this._fatalCloseErrors = []
    this._fatalScheduled = false
    this._listenerCleanupDeferrals = 0
    this.on('fatal', (err) => this._fatalClose(err))
  }

  get isOpen() {
    return this._open && !this._closing
  }

  get seq() {
    return this.log.seq
  }

  get head() {
    return this.log.head
  }

  _autoRaft() {
    return true
  }

  _throwIfClosing() {
    if (!this._closing) { return }
    throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
  }

  _emitSafe(type, data) {
    try {
      this.emit(type, data)
    } catch (err) {
      try { this.emit('error', err) } catch {}
    }
  }

  _change() {
    const { id, nodes, state, leader, followers, term } = this
    const change = { id, nodes, state, leader, followers, term }
    change.open = this.isOpen
    this._emitSafe('change', change)
  }

  _stopTimers() {
    clearTimeout(this._electionTimer)
    clearInterval(this._pingTimer)
  }

  _isQuorum(ids=null) {
    ids = ids ?? this.followers
    const found = new Set(ids.filter((id) => id !== this.id))
    if (this.state === CANDIDATE || this.state === LEADER) { found.add(this.id) }
    return found.size >= this.quorum
  }

  _isQuorumErr(ids=null) {
    ids = ids ?? this.followers
    const failed = new Set(ids.filter((id) => id !== this.id)).size
    return (this.nodes.length - failed) < this.quorum
  }

  _persistElection() {
    const term = this.term
    const votedFor = this._votedFor
    if (term === this.log.elec.term &&
        votedFor === this.log.elec.votedFor) {
      return true
    }
    try {
      this.log.election(term, votedFor)
    } catch (err) {
      this._emitSafe('fatal', err)
      return false
    }
    return true
  }

  _replyVote(from, voteGranted) {
    if (this._closing) { return false }
    const msg = { type: VOTE, term: this.term, voteGranted }
    if (voteGranted && !this._persistElection()) { return false }
    this.send(from, msg)
    return true
  }

  _raceShutdown(work) {
    // Give an operation which caused shutdown one turn to surface its more
    // specific result before the shared cancellation reaches callers.
    let active = true
    let scheduledShutdown = null
    let rejectOnShutdown = null
    const shutdown = new Promise((_, reject) => {
      rejectOnShutdown = reject
    })
    const notifyShutdown = (err) => {
      if (!active || scheduledShutdown !== null) { return }
      scheduledShutdown = setImmediate(() => {
        scheduledShutdown = null
        if (active) { rejectOnShutdown(err) }
      })
    }
    this._shutdownWaiters.add(notifyShutdown)
    if (this._closing) {
      notifyShutdown(this._shutdownError ??
        new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN))
    }
    return Promise.race([work, shutdown]).finally(() => {
      active = false
      this._shutdownWaiters.delete(notifyShutdown)
      if (scheduledShutdown !== null) { clearImmediate(scheduledShutdown) }
    })
  }

  _delay(ms) {
    let timer = null
    const wait = new Promise((resolve) => {
      timer = setTimeout(resolve, ms)
    })
    return this._raceShutdown(wait).finally(() => clearTimeout(timer))
  }

  _awaitResponse(from, cid, deadline, validate=null) {
    return new Promise((res, rej) => {
      let settled = false
      const finish = (err, msg=null) => {
        if (settled) { return }
        settled = true
        this._acks.delete(cid)
        err ? rej(err) : res(msg)
      }
      const cb = (fromm, msg, err=null) => {
        if (err) { return finish(err) }
        if (ACK !== msg.type && ERR !== msg.type) { return }
        if (from !== fromm) { return }
        if (validate && !validate(msg)) { return }
        finish(null, msg)
      }
      this._acks.set(cid, cb)
      deadline.catch((err) => finish(err))
    })
  }

  _sendAndAwaitResponse(to, msg, timeoutMs, timeoutErr, validate=null) {
    const [timer, timedout] = util.timeout(timeoutMs)
    const deadline = timedout.catch(() => {
      throw timeoutErr
    })
    const response = this._awaitResponse(to, msg.cid, deadline, validate)
    this.send(to, msg)
    return response.finally(() => clearTimeout(timer))
  }

  _sendAndAwaitAck(to, msg, operation) {
    const { timeoutKey, code, operation: name } = operation
    const timeoutErr = new Err.ErrorWithCode(`${name} timeout`, code)
    const validate = (response) => {
      if (!util.isTerm(response.term)) { return false }
      if (response.type === ERR) { return true }
      if (response.term < msg.term) { return false }
      if (!util.isSeq(response.seq)) { return false }
      // todo: cleaner
      if (msg.type !== APPEND || response.seq < 0n) { return false }
      if (Object.hasOwn(msg, 'termP')) { return true }
      return Object.hasOwn(response, 'results')
    }
    return this._sendAndAwaitResponse(
      to, msg, this.opts[timeoutKey], timeoutErr, validate
    ).then((response) => {
      this._throwIfClosing()
      if (response.type === ERR) {
        throw Err.wrapError(response, null, `${name} ERR `)
      }
      if (response.term !== msg.term) { return response }
      this._pongs.set(to, Date.now())
      if (this.state === LEADER && !this.followers.includes(to)) {
        this.followers.push(to)
        this._change()
        this._throwIfClosing()
      }
      return response
    })
  }

  _subscribeApply(begin, end) {
    const count = Number(1n + end - begin)
    const waiter = util.oneShot()
    waiter.begin = begin
    waiter.end = end
    waiter.results = new Array(count)
    waiter.remaining = count
    waiter.promise
      .finally(() => this._applyWaiters.delete(waiter))
      .catch(noop)
    this._applyWaiters.add(waiter)
    return waiter
  }

  _routeApply(begin, results) {
    results.forEach((result, idx) => {
      const seq = begin + BigInt(idx)
      for (const waiter of [...this._applyWaiters]) {
        if (waiter.settled || seq < waiter.begin || seq > waiter.end) { continue }
        const ridx = Number(seq - waiter.begin)
        if (ridx in waiter.results) { continue }
        waiter.results[ridx] = result
        waiter.remaining--
        if (waiter.remaining === 0) { waiter.resolve(waiter.results) }
      }
    })
  }

  _rejectApplyWaiters(err) {
    for (const waiter of [...this._applyWaiters]) { waiter.reject(err) }
  }

  _apply(end=null) {
    end = end ?? this._commitSeq
    end = min(end, this._commitSeq)
    const apply = async () => {
      if (this._closing) { return }
      if (this._applySeq >= end) { return }

      while (this._applySeq < end) {
        if (this._closing) { return }
        const begin = this._applySeq + 1n
        let next = begin
        const arr = []
        try {
          for (const buf of this.log.iter(begin)) {
            if (this._closing) { return }
            arr.push(buf)
            next++
            if (arr.length >= this.opts.applyMax || next > end) { break }
          }
        } catch (err) {
          if (this._closing) { return }
          this._rejectApplyWaiters(err)
          this._emitSafe('fatal', err)
          throw err
        }
        if (arr.length <= 0) { break }

        try {
          const data = arr.map((buf) => buf.length > 8 ? buf.subarray(8) : null)
          const seqs = data.map((buf, idx) => begin + BigInt(idx))
          const terms = arr.map((buf) => buf.readBigUInt64LE())
          let results = this.opts.apply(this, data, seqs, terms)
          if (results instanceof Promise) { results = await results }
          if (this._closing) { return }
          if (!Array.isArray(results) || results.length !== data.length) {
            throw new Error(`results must be array with len ${data.length}`)
          }
          const applied = begin + BigInt(data.length - 1)
          this._applySeq = applied
          this._routeApply(begin, results)
          this._emitSafe('apply', applied)
        } catch (err) {
          if (this._closing) { return }
          err = Err.wrapError(err, Err.APPLY_ERROR, '(apply) ')
          this._rejectApplyWaiters(err)
          this._emitSafe('fatal', err)
          throw err
        }
      }
      if (this._closing) { return }
      if (this._applySeq < end) {
        const err = new Err.ErrorWithCode(
          `apply ended at ${this._applySeq} wanted ${end}`, Err.RAFT_ILLEGAL,
        )
        this._rejectApplyWaiters(err)
        this._emitSafe('fatal', err)
        throw err
      }
    }
    const operation = this._applyPrev.then(apply)
    this._applyPrev = operation
    operation.catch(noop)
    return operation
  }

  _beginLeaderReady(term) {
    const ready = util.oneShot()
    ready.term = term
    this._leaderReady = ready
    return ready
  }

  _cancelLeaderReady(err) {
    if (!this._leaderReady) { return }
    this._leaderReady.reject(err)
    this._leaderReady = null
  }

  _replicationActive(state) {
    return !state.cancelled && !this._closing &&
      this.state === LEADER && this.term === state.term
  }

  _replicationState(to, term, nextIndex) {
    let state = this._replication.get(to)
    if (state && state.term === term) { return state }
    state = {
      to,
      term,
      nextIndex,
      matchIndex: -1n,
      target: -1n,
      running: false,
      restart: false,
      work: null,
      cid: null,
      cancelled: false,
      waiters: new Set(),
    }
    this._replication.set(to, state)
    return state
  }

  _resolveReplicationWaiters(state) {
    for (const waiter of [...state.waiters]) {
      if (state.matchIndex >= waiter.end) { waiter.resolve() }
    }
  }

  _cancelReplicationState(state, err) {
    state.cancelled = true
    for (const waiter of [...state.waiters]) { waiter.reject(err) }
    if (state.cid) {
      const ack = this._acks.get(state.cid)
      ack && ack(null, null, err)
    }
  }

  _cancelReplication(err) {
    this._cancelLeaderReady(err)
    this._replication.forEach((state) => this._cancelReplicationState(state, err))
    this._replication.clear()
  }

  _reset() {
    this._cancelReplication(
      new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN),
    )
    this.state = null
    this.leader = null
    this.followers = []
    this.term = null
    this._votedFor = null
    this._pingms = 0
    this._pongs.clear()
    this._votes = []
    this._acks.clear()
    this._commitSeq = -1n
    this._commitTerm = -1n
    this._stopTimers()
  }

  _toFollower(leader=null, change=true) {
    this._cancelReplication(
      new Err.ErrorWithCode('node not leader', Err.NOT_LEADER),
    )
    this.state = FOLLOWER
    this.leader = leader
    this.followers = []
    this._pingms = 0
    this._pongs.clear()
    this._votes = []
    clearInterval(this._pingTimer)
    this._startElectionTimer()
    change && this._change()
  }

  _advanceTerm(term, leader=null) {
    if (!util.isTerm(term) || term <= this.term) { return false }
    this.term = term
    this._votedFor = null
    this._toFollower(leader, false)
    if (leader !== null) { this._pingms = Date.now() }
    if (!this._persistElection()) { return false }
    this._change()
    return !this._closing
  }

  _pruneFollowers() {
    const rm = this.nodes.filter((id) => {
      let delay = this._pongs.get(id) ?? 0
      delay = Date.now() - delay
      return delay >= this.opts.pingTimeout
    })
    this.followers = this.followers.filter((id) => !rm.includes(id))
    return this._isQuorum()
  }

  _startElectionTimer() {
    clearTimeout(this._electionTimer)
    const delay = () => {
      const r = Math.floor(this.opts.electionTimeout * 0.20)
      return this.opts.electionTimeout + rand(0, r)
    }
    let timeout = delay()
    const cb = () => {
      if (!this.isOpen) { return }
      if (this.state === LEADER) { return }
      if ((Date.now() - this._pingms) >= timeout) {
        this._voteForSelf()
      }
      if (this._closing) { return }
      if (this.state === LEADER) { return }
      timeout = delay()
      clearTimeout(this._electionTimer)
      this._electionTimer = setTimeout(cb, timeout)
    }
    clearTimeout(this._electionTimer)
    this._electionTimer = setTimeout(cb, timeout)
  }

  _startPingTimer() {
    clearInterval(this._pingTimer)
    const interval = this.opts.pingTimeout * 0.3
    this._pingTimer = setInterval(() => this._pingFollowers(), interval)
    this._pingFollowers()
  }

  _voteForSelf() {
    this.state = CANDIDATE
    this.leader = null
    this.followers = []
    const term = ++this.term
    this._votedFor = this.id
    this._pingms = 0
    this._pongs.clear()
    this._votes = []
    this._cancelReplication(
      new Err.ErrorWithCode('node not leader', Err.NOT_LEADER),
    )
    if (!this._persistElection()) { return }
    this._change()
    if (this._closing) { return }
    const termP = this.log.term
    const seqP = this.log.seq
    const msg = { type: VOTE_REQUEST, term, termP, seqP }
    this.nodes.filter((id) => id !== this.id).forEach((to) => this.send(to, msg))
  }

  _rxVoteRequest(msg, from) {
    const { term, termP, seqP } = msg
    if (!util.isTerm(term) || !util.isSeq(termP) || !util.isSeq(seqP)) {
      this._replyVote(from, false)
      return
    }

    if (term < this.term) {
      this._replyVote(from, false)
      return
    }
    const termChange = term > this.term
    if (termChange) {
      this.term = term
      this._votedFor = null
      this._toFollower(null, false)
    }

    const termPF = this.log.term
    const seqPF = this.log.seq
    const voteGranted = term === this.term &&
      (this._votedFor === null || this._votedFor === from) &&
      term >= termP &&
      (termP > termPF || (termP === termPF && seqP >= seqPF))
    let stateChange = false
    if (voteGranted) {
      stateChange = this.state !== FOLLOWER
      if (stateChange) { this._toFollower(null, false) }
      this._votedFor = from
      this._pingms = Date.now()
      this._startElectionTimer()
    }

    if (!this._persistElection()) { return }
    if (termChange || stateChange) { this._change() }
    this._replyVote(from, voteGranted)
  }

  _rxVote(msg, from) {
    const { term, voteGranted } = msg
    if (!util.isTerm(term) || typeof voteGranted !== 'boolean') { return }
    if (term > this.term) {
      this._advanceTerm(term)
      return
    }
    if (this.state !== CANDIDATE && this.state !== LEADER) { return }
    if (this.term !== term) { return }
    if (!voteGranted) { return }
    this._pongs.set(from, Date.now())
    if (!this._votes.includes(from)) { this._votes.push(from) }
    if (!this._isQuorum(this._votes)) { return }
    const change = this.state !== LEADER
    const update = change || !this.followers.includes(from)
    this.state = LEADER
    this.leader = this.id
    clearTimeout(this._electionTimer)
    if (change) {
      this.followers = [...this._votes]
      this._cancelReplication(
        new Err.ErrorWithCode('node term different', Err.TERM_DIFF),
      )
      this._beginLeaderReady(this.term)
      this.nodes.filter((id) => id !== this.id).forEach((id) => {
        this._pongs.set(id, Date.now())
        this._replicationState(id, this.term, this.seq + 1n)
      })
      this._startPingTimer()
      this._leaderAppendNoOp()
    } else if (update) {
      this.followers.push(from)
    }
    update && this._change()
  }

  _pingFollowers() {
    if (this.state !== LEADER) { return }
    const termP = this.log.term
    const seqP = this.log.seq
    const commitSeq = this._commitSeq
    const cid = crypto.randomUUID()
    const msg = { type: APPEND, term: this.term, termP, seqP, commitSeq }
    this.nodes.filter((id) => id !== this.id).forEach((to, idx) => {
      const msgg = { ...msg, cid: cid + idx }
      this._sendAndAwaitAck(to, msgg, ACK_OPERATION.PING)
        .then(() => this._catchUpFollower(to))
        .catch((err) => {
          if (!this._closing) { this._emitSafe('warn', err) }
        })
    })
    if (this._pruneFollowers()) { return }
    this._toFollower()
  }

  _checkCommit() {
    try {
      if (this.state !== LEADER) { return }
      const matches = this.followers.map((id) => {
        return this._replication.get(id)?.matchIndex ?? -1n
      })
      matches.push(this.seq)
      if (matches.length < this.quorum) { return }
      matches.sort((a, b) => a > b ? -1 : a < b ? 1 : 0)
      const match = matches[this.quorum - 1]
      if (match <= this._commitSeq) { return }

      const term = this.term
      let entryTerm = null
      for (const entry of this.log.iter(match)) {
        entryTerm = entry.readBigUInt64LE()
        break
      }
      if (entryTerm === null) {
        throw new Err.ErrorWithCode(
          `check commit ${match} not found`, Err.RAFT_ILLEGAL,
        )
      }
      if (entryTerm !== term) { return }

      this._commitSeq = match
      this._commitTerm = term
      const leaderReady = this._leaderReady
      if (leaderReady && leaderReady.term === term) {
        leaderReady.resolve(match)
      }
      this._emitSafe('commit', match)
      this._apply(match).catch(noop)
    } catch (err) {
      if (!this._closing) { this._emitSafe('fatal', err) }
    }
  }

  async _replicationPage(state) {
    const { to } = state
    const begin = state.nextIndex
    const end = state.target
    const seqP = begin - 1n
    let termP = seqP < 0n ? -1n : null
    const b = seqP < 0n ? begin : seqP
    const data = []
    let count = 1n + end - begin
    count = min(this.opts.rpcMax, count)

    try {
      if (!this._replicationActive(state)) { return false }
      const retainedBegin = this.log.begin
      if (retainedBegin > 0n && b < retainedBegin) {
        const err = new Err.ErrorWithCode(
          `replication wants forgotten ${b} have ${retainedBegin}`,
          Err.REPL_FORGOT,
        )
        this._cancelReplicationState(state, err)
        this._emitSafe('warn', err)
        return false
      }
      for (const buf of this.log.iter(b)) {
        if (!this._replicationActive(state)) { return false }
        if (termP !== null) { data.push(buf) }
        termP = termP ?? buf.readBigUInt64LE()
        if (BigInt(data.length) >= count) { break }
      }
    } catch (err) {
      if (!this._replicationActive(state)) { return false }
      this._emitSafe('fatal', err)
      return false
    }

    if (!this._replicationActive(state)) { return false }
    if (BigInt(data.length) !== count) {
      const err = new Err.ErrorWithCode(
        `replication read ${data.length} wanted ${count}`,
        Err.RAFT_ILLEGAL,
      )
      this._emitSafe('fatal', err)
      return false
    }

    const cid = crypto.randomUUID()
    state.cid = cid
    const msg = {
      cid,
      type: APPEND,
      term: state.term,
      termP,
      seqP,
      commitSeq: this._commitSeq,
      data,
    }
    try {
      await this._sendAndAwaitAck(to, msg, ACK_OPERATION.APPEND)
    } catch (err) {
      if (!this._replicationActive(state)) { return false }
      if (err.code === Err.REPL_FORGOT) {
        this._cancelReplicationState(state, err)
        this._emitSafe('warn', err)
        return false
      }
      const backtrack = err.code === Err.REPL_BACKTRACK
      let retryDelay = !backtrack
      if (backtrack) {
        if (seqP === -1n) {
          err = Err.wrapError(err, Err.RAFT_ILLEGAL, 'genesis rejected ')
          this._cancelReplicationState(state, err)
          this._emitSafe('warn', err)
          return false
        }
        const nextIndex = min(state.nextIndex, max(0n, begin - 1n))
        retryDelay = nextIndex === state.nextIndex
        state.nextIndex = nextIndex
      }
      this._emitSafe('warn', err)
      if (retryDelay) {
        const retryms = Math.max(2, this.opts.pingTimeout * 0.10)
        try {
          await this._delay(retryms)
        } catch {
          return false
        }
      }
      return true
    } finally {
      if (state.cid === cid) { state.cid = null }
    }

    if (!this._replicationActive(state)) { return false }
    const endd = seqP + count
    state.matchIndex = max(state.matchIndex, endd)
    state.nextIndex = endd + 1n
    this._checkCommit()
    queueMicrotask(() => this._resolveReplicationWaiters(state))
    return true
  }

  async _runReplication(state) {
    while (this._replicationActive(state) && state.matchIndex < state.target) {
      const ok = await this._replicationPage(state)
      if (!ok) { break }
    }
  }

  _startReplication(state) {
    if (state.cancelled) { return }
    if (state.running) {
      state.restart = true
      return
    }
    state.running = true
    state.restart = false
    const work = this._runReplication(state)
    state.work = work
    work.catch(noop).finally(() => {
      if (this._replication.get(state.to) !== state) { return }
      state.running = false
      state.work = null
      if (state.restart && this._replicationActive(state) && state.matchIndex < state.target) {
        this._startReplication(state)
      }
    })
  }

  _catchUpFollower(to) {
    if (this.state !== LEADER || this.seq < 0n) { return }
    const state = this._replicationState(to, this.term, this.seq)
    if (state.matchIndex >= this.seq) { return }
    state.nextIndex = min(state.nextIndex, this.seq)
    state.target = max(state.target, this.seq)
    this._startReplication(state)
  }

  _appendToFollower(to, begin, end) {
    if (this.state !== LEADER) {
      return Promise.reject(
        new Err.ErrorWithCode('node not leader', Err.NOT_LEADER),
      )
    }

    const state = this._replicationState(to, this.term, begin)
    if (state.cancelled) {
      return Promise.reject(new Err.ErrorWithCode(
        'follower replication cancelled', Err.NOT_COMMIT,
      ))
    }
    state.nextIndex = max(state.matchIndex + 1n, min(state.nextIndex, begin))
    state.target = max(state.target, end)

    if (state.matchIndex >= end) {
      this._startReplication(state)
      return ready
    }

    const [timer, timedout] = util.timeout(this.opts.appendTimeout)
    const waiter = util.oneShot()
    waiter.end = end
    const work = waiter.promise.finally(() => {
      clearTimeout(timer)
      state.waiters.delete(waiter)
    })
    work.catch(noop)
    state.waiters.add(waiter)
    timedout.catch(() => waiter.reject(
      new Err.ErrorWithCode('follower append timeout', Err.APPEND_TIMEOUT),
    ))
    this._startReplication(state)
    return work
  }

  _appendToFollowers(begin, end) {
    const [timer, timedout] = util.timeout(this.opts.appendTimeout)
    const work = new Promise((res, rej) => {
      if (this.state !== LEADER) {
        return rej(new Err.ErrorWithCode('node not leader', Err.NOT_LEADER))
      }
      timedout.catch(() => rej(
        new Err.ErrorWithCode('quorum append timeout', Err.APPEND_TIMEOUT),
      ))
      const acks = this.followers.map((to) => {
        const append = this._appendToFollower(to, begin, end)
        return append.then(() => to).catch(() => Promise.reject(to))
      })
      const q = this._isQuorum.bind(this)
      const qErr = this._isQuorumErr.bind(this)
      util.awaitResolve(acks, q, qErr).then(() => {
        this._checkCommit()
        if (this._commitSeq < end) {
          throw new Err.ErrorWithCode('append not commit', Err.NOT_COMMIT)
        }
        this._pingFollowers()
        res()
      }).catch((err) => {
        if (err?.code === Err.NOT_COMMIT) { return rej(err) }
        rej(new Err.ErrorWithCode('append not commit', Err.NOT_COMMIT))
      })
    })
    work.catch(noop).finally(() => clearTimeout(timer))
    return work
  }

  _appendToSelfAndFollowers(data) {
    const batch = Array.isArray(data)
    const raftTerm = this.term
    const term = Buffer.allocUnsafe(8)
    term.writeBigUInt64LE(raftTerm)
    data = batch ? data : [data]
    data = data.map((buf) => Buffer.concat([term, buf]))

    let seq = null
    try {
      seq = batch
        ? this.log.appendBatch(data)
        : this.log.append(data[0])
    } catch (err) {
      this._emitSafe('fatal', err)
      return Promise.reject(err)
    }
    const end = seq + BigInt(data.length - 1)
    const waiter = this._subscribeApply(seq, end)
    const append = Promise.resolve({ seq, end, waiter })

    return append.then(({ seq, end, waiter }) => {
      if (this._closing) {
        const err = new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
        waiter.reject(err)
        throw err
      }
      const replicated = this._appendToFollowers(seq, end).catch((err) => {
        waiter.reject(err)
        throw err
      })
      return Promise.all([replicated, waiter.promise])
        .then(([, results]) => [seq, batch ? results : results[0]])
    })
  }

  // allows discover commitSeq
  // raft paper explains this
  _leaderAppendNoOp() {
    if (this.state !== LEADER) { return }
    const leaderReady = this._leaderReady
    if (!leaderReady || leaderReady.term !== this.term ||
        this._commitTerm === this.term) {
      return
    }
    const buf = Buffer.alloc(0)
    this._appendToSelfAndFollowers(buf).catch((err) => {
      if (this._closing) { return }
      this._emitSafe('warn', err)
      if (this._leaderReady !== leaderReady ||
          this._commitTerm === leaderReady.term) {
        return
      }
      const retryms = Math.max(2, this.opts.pingTimeout * 0.10)
      this._delay(retryms).then(() => this._leaderAppendNoOp()).catch(noop)
    })
  }

  _fwdToLeader(data) {
    const leader = this.leader
    if (leader === null) {
      return Promise.reject(new Err.ErrorWithCode(
        'forward no leader', Err.NO_LEADER,
      ))
    }
    const cid = crypto.randomUUID()
    const msg = { type: APPEND, term: this.term, cid, data }
    return this._sendAndAwaitAck(
      leader, msg, ACK_OPERATION.APPEND
    ).then((response) => {
      // todo: cleaner
      if (Array.isArray(data) &&
          (!Array.isArray(response.results) ||
            response.results.length !== data.length)) {
        throw new Err.ErrorWithCode(
          `forward results must be array with len ${data.length}`,
          Err.RAFT_ILLEGAL,
        )
      }
      return [response.seq, response.results]
    })
  }

  _rxAppendToLeader(msg, from) {
    const { term, cid, data } = msg
    const error = (err, code=null) => {
      this.send(from, util.errorRpc(this.term, cid, err, code))
    }

    if (!util.isCid(cid)) {
      return
    } else if (!util.isTerm(term)) {
      return error('term is illegal', Err.RPC_ILLEGAL)
    } else if (term > this.term) {
      return error('higher term append routed to leader', Err.TERM_DIFF)
    } else if (this.term !== term) {
      return error('term is lesser', Err.TERM_DIFF)
    } else if (!isForwardData(data)) {
      return error('data is illegal', Err.RPC_ILLEGAL)
    }

    this._appendToSelfAndFollowers(data).then(([seq, results]) => {
      msg = { type: ACK, term: this.term, cid, seq, results }
      this.send(from, msg)
    }).catch((err) => error(err))
  }

  _rxAppendToFollower(msg, from) {
    const { cid, term, termP, seqP, commitSeq, data } = msg

    const error = (err, code=null) => {
      this.send(from, util.errorRpc(this.term, cid, err, code))
    }

    if (!util.isCid(cid)) {
      return
    } else if (!util.isTerm(term) || !util.isSeq(termP) || !util.isSeq(seqP) || !util.isSeq(commitSeq)) {
      return error('term, termP, seqP, commitSeq are illegal', Err.RPC_ILLEGAL)
    } else if (data !== undefined && !isEntries(data)) {
      return error('data is illegal', Err.RPC_ILLEGAL)
    } else if (data !== undefined && !util.isSeq(seqP + BigInt(data.length))) {
      return error('append end seq is illegal', Err.RPC_ILLEGAL)
    }

    try {
      if (term > this.term) {
        this.term = term
        this._votedFor = null
        this._toFollower(from, false)
        this._pingms = Date.now()
        if (!this._persistElection()) { return }
        this._change()
        if (this._closing) { return }
      }
      if (term < this.term) {
        error('term is lesser', Err.TERM_DIFF)
        return
      }
      if (this.state !== FOLLOWER || from !== this.leader) {
        this._toFollower(from)
        if (this._closing) { return }
      }

      this._pingms = Date.now()
      const seq = seqP + 1n
      const matchedEnd = data === undefined
        ? seqP
        : seqP + BigInt(data.length)

      const ack = () => {
        msg = { type: ACK, term: this.term, cid, seq }
        this.send(from, msg)
      }

      const apply = (logMatched) => {
        if (this._closing) { return }
        if (!logMatched) { return ack() }
        if (commitSeq <= this._commitSeq) { return ack() }
        const next = min(commitSeq, matchedEnd)
        if (next <= this._commitSeq) { return ack() }
        let commitTerm = null
        for (const entry of this.log.iter(next)) {
          commitTerm = entry.readBigUInt64LE()
          break
        }
        if (commitTerm === null) {
          throw new Err.ErrorWithCode(
            `commit ${next} not found`, Err.RAFT_ILLEGAL,
          )
        }
        this._commitSeq = next
        this._commitTerm = commitTerm
        this._emitSafe('commit', next)
        this._apply(next).catch(noop)
        ack()
      }

      let termPF = this.log.term
      const seqPF = this.log.seq
      const logMatched = termPF === termP && seqPF === seqP
      if (data === undefined) {
        apply(logMatched)
        return
      }

      if (logMatched) {
        this.log.appendBatch(data, seq)
        apply(true)
        return
      }

      let have = []
      const begin = max(seqP, 0n)
      const retainedBegin = this.log.begin
      if (retainedBegin > 0n && begin < retainedBegin) {
        const err = new Err.ErrorWithCode(
          `rx append wants forgotten ${seqP} have ${retainedBegin}`,
          Err.REPL_FORGOT,
        )
        this._emitSafe('warn', err)
        error(err)
        return
      }
      const haveMax = data.length + (seqP >= 0n ? 1 : 0)
      for (const next of this.log.iter(begin)) {
        have.push(next)
        if (have.length >= haveMax) { break }
      }

      if (seqP >= 0n && have.length <= 0) {
        error(`seqP ${seqP} not found`, Err.REPL_BACKTRACK)
        return
      }
      termPF = seqP >= 0n ? have[0].readBigUInt64LE() : -1n
      if (termPF !== termP) {
        error(`termP mismatch at seqP ${seqP}`, Err.REPL_BACKTRACK)
        return
      }
      if (seqP >= 0n) { have = have.slice(1) }
      let same = 0
      while (same < data.length && have[same] && have[same].equals(data[same])) {
        same++
      }

      if (same === data.length) {
        apply(true)
        return
      }

      const trim = seqP + BigInt(same)
      if (trim < this._commitSeq) {
        throw new Err.ErrorWithCode(
          `append cannot trim committed ${this._commitSeq} to ${trim}`,
          Err.RAFT_ILLEGAL,
        )
      }
      this.log.trim(trim)
      this.log.appendBatch(data.slice(same), trim + 1n)
      apply(true)
    } catch (err) {
      if (!this._closing) { this._emitSafe('fatal', err) }
    }
  }

  async onReceive(from, msg) {
    if (!this.nodes.includes(from)) { return }
    if (!this.isOpen) { return }
    if (!util.isRpc(msg)) { return }
    const { term } = msg
    switch (msg.type) {
      case ACK:
      case ERR:
        this._advanceTerm(term)
        try {
          this._acks.get(msg.cid)?.(from, msg)
        } catch (err) {
          if (!this._closing) { this._emitSafe('fatal', err) }
        }
        return

      case VOTE_REQUEST:
        this._rxVoteRequest(msg, from)
        break

      case VOTE:
        this._rxVote(msg, from)
        break

      case APPEND:
        if (this.state === LEADER && (!util.isTerm(term) || term <= this.term)) {
          this._rxAppendToLeader(msg, from)
        } else {
          this._rxAppendToFollower(msg, from)
        }
        break
    }
  }

  async awaitEvent(event, fn) {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    }
    return new Promise((res, rej) => {
      let settled = false
      const finish = (err=null, val=null) => {
        if (settled) { return }
        settled = true
        this.removeListener(event, cb)
        this._shutdownWaiters.delete(finish)
        err ? rej(err) : res(val)
      }
      const cb = (val) => {
        let matched = false
        try {
          matched = fn(val)
        } catch (err) {
          finish(err)
          return
        }
        if (matched) { finish(null, val) }
      }
      this._shutdownWaiters.add(finish)
      this.on(event, cb)
    })
  }

  async awaitLeader(commit=false) {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    }
    return new Promise((res, rej) => {
      const isFollower = (state) => state.state === FOLLOWER && state.leader !== null
      const fn = (state) => state.state === LEADER || isFollower(state)
      const fn2 = (state) => fn(state) && state.term >= 0n && state.term === this._commitTerm
      let settled = false
      const finish = (err=null) => {
        if (settled) { return }
        settled = true
        this.removeListener('change', changed)
        this.removeListener('commit', committed)
        this._shutdownWaiters.delete(finish)
        err ? rej(err) : res()
      }
      const changed = () => {
        if (fn(this)) { finish() }
      }
      const committed = () => {
        if (fn2(this)) { finish() }
      }
      this._shutdownWaiters.add(finish)
      if (fn(this) && !commit) { return finish() }
      if (fn2(this)) { return finish() }
      this.on(commit ? 'commit' : 'change', commit ? committed : changed)
    })
  }

  async append(data) {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    }
    if (!Buffer.isBuffer(data) || data.length <= 0) {
      throw new Err.ErrorWithCode(
        'data must be non-empty buffer', Err.ARGUMENT_ILLEGAL)
    }
    const isLeader = this.state === LEADER
    const work = isLeader ? this._appendToSelfAndFollowers(data) : this._fwdToLeader(data)
    return this._raceShutdown(work)
  }

  async appendBatch(data) {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    }
    if (!Array.isArray(data) || data.length <= 0) {
      throw new Err.ErrorWithCode(
        'data must be array with length > 0', Err.ARGUMENT_ILLEGAL)
    }
    if (!data.every((buf) => Buffer.isBuffer(buf) && buf.length > 0)) {
      throw new Err.ErrorWithCode(
        'data must be array of non-empty buffers', Err.ARGUMENT_ILLEGAL)
    }
    const isLeader = this.state === LEADER
    const work = isLeader ? this._appendToSelfAndFollowers(data) : this._fwdToLeader(data)
    return this._raceShutdown(work)
  }

  _startRaft() {
    this._throwIfClosing()
    if (this.isOpen) { return }
    if (!this.log.isOpen) {
      throw new Err.ErrorWithCode('log not open', Err.LOG_NOT_OPEN)
    }

    this._reset()
    this.state = FOLLOWER
    this.term = this.log.elec.term
    this._votedFor = this.log.elec.votedFor
    this._open = true
    this._startElectionTimer()
    this._change()
    this._throwIfClosing()
  }

  _stopRaft() {
    if (this._closing) { return }
    const changed = this._open
    this._closing = true
    const err = new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    this._shutdownError = err
    for (const cancel of [...this._shutdownWaiters]) { cancel(err) }
    for (const waiter of [...this._applyWaiters]) { waiter.reject(err) }
    this._cancelReplication(err)
    const waiters = [...this._acks.values()]
    waiters.forEach((cb) => cb(null, null, err))
    this._reset()
    this._open = false
    changed && this._change()
  }

  _closeAfterOpenFailure() {
    this._listenerCleanupDeferrals++
    try {
      this.close()
    } catch (closeErr) {
      this._emitSafe('fatal', closeErr)
    }
    this._listenerCleanupDeferrals--
    this._removeListenersAfterClose()
  }

  _removeListenersAfterClose() {
    if (this._listenerCleanupDeferrals > 0 || this._fatalScheduled) { return }
    this.removeAllListeners()
  }

  _fatalClose(err) {
    this._fatalErrors.push(err)
    if (this._fatalScheduled) { return }
    this._fatalScheduled = true

    if (!this._closing) {
      try {
        this.close()
      } catch (closeErr) {
        this._fatalCloseErrors.push(closeErr)
      }
    }
    queueMicrotask(() => {
      while (this._fatalErrors.length > 0) {
        this.emit('error', this._fatalErrors.shift())
      }
      while (this._fatalCloseErrors.length > 0) {
        this.emit('error', this._fatalCloseErrors.shift())
      }
      this._fatalScheduled = false
      this._removeListenersAfterClose()
    })
  }

  open() {
    this._throwIfClosing()
    if (this.isOpen) { return }

    try {
      this.log.open()
      if (this._autoRaft()) { this._startRaft() }
    } catch (err) {
      if (!this._closing) { this._closeAfterOpenFailure() }
      throw err
    }
  }

  close() {
    this._stopRaft()
    let closeErr = null
    if (this.log.isOpen) {
      try {
        this.log.close()
      } catch (err) {
        closeErr = err
      }
    }
    this._removeListenersAfterClose()
    if (closeErr) { throw closeErr }
  }

  del() {
    this.log.del()
  }
}

export { RaftNode, ACK_OPERATION }
