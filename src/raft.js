import * as Err from './error.js'
import * as util from './util.js'
import { errorToWire, errorFromWire } from './wire.js'

const FOLLOWER = 'follower'
const CANDIDATE = 'candidate'
const LEADER = 'leader'

// Maximum log entries per replication page.
const DEFAULT_RPC_MAX = 1024
// Payload bytes per page (64 MiB); one oversized entry is allowed.
const DEFAULT_RPC_MAX_BYTES = 67_108_864

const min = (a, b) => a < b ? a : b
const max = (a, b) => a > b ? a : b
const failure = (message, code) => new Err.ErrorWithCode(message, code)
const illegal = (message) => failure(message, Err.ARGUMENT_ILLEGAL)

// Internal Raft engine. All methods run synchronously in a Proc input's
// executor and DB1 transaction; remote waits are registered continuations.
class RaftEngine {
  constructor(proc, opts = {}) {
    const { id, nodes } = proc
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
    this.id = id
    this.store = proc.store
    this.log = this.store.log
    this.executor = proc.executor
    this.random = proc.random
    this.output = proc.output.bind(proc)
    this.queueLocalEvent = proc.queueLocalEvent.bind(proc)
    this.opts = {
      electionTimeout: 1500, appendTimeout: 1500,
      rpcMax: DEFAULT_RPC_MAX, rpcMaxBytes: DEFAULT_RPC_MAX_BYTES, ...opts,
    }
    const minQuorum = Math.floor(this.nodes.length / 2) + 1
    this.quorum = opts.quorum ?? minQuorum
    if (!Number.isSafeInteger(this.quorum) ||
        this.quorum < minQuorum || this.quorum > this.nodes.length) {
      throw illegal(`quorum must be an integer from ${minQuorum} to ${this.nodes.length}`)
    }
    for (const name of ['electionTimeout', 'appendTimeout', 'rpcMax', 'rpcMaxBytes']) {
      if (!Number.isSafeInteger(this.opts[name]) || this.opts[name] < (name === 'electionTimeout' ? 10 : 1)) {
        throw illegal(`${name} is illegal`)
      }
    }
    this.retryms = Math.max(2, this.opts.electionTimeout * 0.1)
    this.state = FOLLOWER
    this.leader = null
    this.term = this.log.elec.term
    this.votedFor = this.log.elec.votedFor
    this.commitSeq = this.store.load().applied_seq
    this.commitTerm = this.commitSeq < 0n ? -1n : this.entry(this.commitSeq).term
    this.appliedSeq = this.commitSeq
    this.requests = new Map()
    this.replication = new Map()
    this.appendWaiters = new Set()
    this.pongs = new Map()
    this.votes = new Set()
    this.electionTimer = null
    this.heartbeatTimer = null
    this.lastChange = null
    this.stopped = false
  }

  get now() { return this.executor.timems }
  get seq() { return this.log.seq }
  get followers() {
    if (this.state !== LEADER) { return [] }
    return this.nodes.filter((id) => id !== this.id &&
      this.now - (this.pongs.get(id) ?? -Infinity) < this.opts.electionTimeout)
  }
  applyReady() {}
  leaderChanged() {}

  entry(seq) {
    if (seq < 0n || seq > this.log.seq || seq < this.log.begin) { return null }
    return this.log.iter(seq, { iterStepSize: 1 }).next().value ?? null
  }
  warn(err) { this.queueLocalEvent('warn', err) }

  send(to, message) { this.output(to, { ...message, from: this.id }) }
  rpcError(to, cid, err, extra = {}) {
    this.send(to, { type: 'err', cid, term: this.term, ...errorToWire(err, false), ...extra })
  }

  event(type, fields = {}) {
    // Capture status before later work in this input advances it.
    const status = {
      state: this.state, leader: this.leader, term: this.term,
      seq: this.seq, commitSeq: this.commitSeq, commitTerm: this.commitTerm,
      followers: this.followers,
    }
    if (type === 'change') {
      const last = this.lastChange
      if (last && status.state === last.state && status.leader === last.leader &&
          status.term === last.term && status.seq === last.seq &&
          status.commitSeq === last.commitSeq && status.commitTerm === last.commitTerm &&
          status.followers.length === last.followers.length &&
          status.followers.every((id, index) => id === last.followers[index])) {
        return
      }
    }
    this.output('gate', { type, ...fields, status })
    if (type === 'change') { this.lastChange = status }
  }
  change() { this.event('change') }

  finishAppend(waiter, err = null) {
    if (!this.appendWaiters.delete(waiter)) { return }
    this.executor.cancel(waiter.timer)
    this.executor.enqueue(() => waiter.callback(err, waiter.seq))
  }

  request(to, message, timeout, callback, validate = () => true, timeoutError = null) {
    const cid = message.cid ?? this.random.cid()
    const timer = this.executor.after(timeout, () => {
      if (!this.requests.delete(cid)) { return }
      callback(timeoutError ?? failure(`${message.type} timeout`, Err.APPEND_TIMEOUT))
    })
    this.requests.set(cid, { to, timer, callback, validate })
    this.send(to, { ...message, cid })
    return cid
  }

  stop() {
    this.stopped = true
    this.requests.clear()
    this.appendWaiters.clear()
    this.replication.clear()
  }

  resetElection() {
    this.executor.cancel(this.electionTimer)
    if (this.state === LEADER) { return }
    const delay = this.opts.electionTimeout + this.random.int(0, Math.max(0, Math.floor(this.opts.electionTimeout * 0.2) - 1))
    this.electionTimer = this.executor.after(delay, () => {
      this.voteSelf()
      if (this.state !== LEADER) { this.resetElection() }
    })
  }

  start() {
    this.resetElection()
  }

  cancelLeadership(err) {
    this.executor.cancel(this.heartbeatTimer)
    for (const state of this.replication.values()) {
      state.cancelled = true
      this.executor.cancel(state.retryTimer)
      for (const cid of [state.cid, ...state.heartbeatCids]) {
        const waiting = this.requests.get(cid)
        if (waiting) { this.executor.cancel(waiting.timer) }
        this.requests.delete(cid)
      }
      state.heartbeatCids.clear()
    }
    this.replication.clear()
    for (const waiter of [...this.appendWaiters]) { this.finishAppend(waiter, err) }
  }

  follower(leader = null, reset = true) {
    this.state = FOLLOWER
    this.leader = leader
    this.cancelLeadership(failure('node not leader', Err.NOT_LEADER))
    this.pongs.clear()
    this.votes.clear()
    if (reset) { this.resetElection() }
    this.leaderChanged()
  }

  advanceTerm(term, leader = null, reset = true) {
    if (!util.isTerm(term) || term <= this.term) { return false }
    this.term = term
    this.votedFor = null
    this.log.election(this.term, null)
    this.follower(leader, reset)
    return true
  }

  voteSelf() {
    if (!util.isTerm(this.term + 1n)) { throw failure('election term exhausted', Err.RAFT_ILLEGAL) }
    this.cancelLeadership(failure('node not leader', Err.NOT_LEADER))
    this.term++
    this.state = CANDIDATE
    this.leader = null
    this.votedFor = this.id
    this.votes = new Set([this.id])
    this.pongs.clear()
    this.log.election(this.term, this.id)
    this.leaderChanged()
    for (const to of this.nodes) {
      if (to !== this.id) {
        this.send(to, { type: 'vote_request', term: this.term, termP: this.log.term, seqP: this.seq })
      }
    }
  }

  voteRequest(from, msg) {
    const { term, termP, seqP } = msg
    let valid = false
    try {
      util.validate(util.raftSchemas.voteRequest, msg, `vote_request from ${from}`)
      valid = true
    } catch (err) { this.warn(err) }
    let granted = false
    if (valid && term >= this.term) {
      const wasLeader = this.state === LEADER
      this.advanceTerm(term, null, wasLeader)
      granted = (this.votedFor === null || this.votedFor === from) && term >= termP &&
        (termP > this.log.term || (termP === this.log.term && seqP >= this.seq))
      if (granted) {
        if (this.state !== FOLLOWER) { this.follower(null) }
        this.votedFor = from
        this.log.election(this.term, from)
        this.resetElection()
      }
    }
    this.send(from, { type: 'vote', term: this.term, voteGranted: granted })
  }

  voteReply(from, msg) {
    try { util.validate(util.raftSchemas.vote, msg, `vote from ${from}`) }
    catch (err) { this.warn(err); return }
    if (this.advanceTerm(msg.term)) { return }
    if (this.state !== CANDIDATE || msg.term !== this.term || !msg.voteGranted) { return }
    this.votes.add(from)
    this.pongs.set(from, this.now)
    if (this.votes.size < this.quorum) { return }
    this.state = LEADER
    this.leader = this.id
    this.executor.cancel(this.electionTimer)
    for (const to of this.nodes) {
      if (to !== this.id) {
        this.replication.set(to, {
          to, term: this.term, nextIndex: this.seq + 1n, matchIndex: -1n,
          cid: null, retryTimer: null, cancelled: false, heartbeatCids: new Set(),
        })
      }
    }
    this.heartbeatTimer = this.executor.after(
      this.opts.electionTimeout * 0.3, () => this.heartbeat(), this.opts.electionTimeout * 0.3,
    )
    this.appendNoop()
    this.leaderChanged()
  }

  appendEntry(entry, callback) {
    if (this.state !== LEADER) { callback(failure('node not leader', Err.NOT_LEADER)); return }
    const seq = this.log.append({ term: this.term, entry })
    const waiter = { seq, term: this.term, callback, timer: null }
    waiter.timer = this.executor.after(this.opts.appendTimeout, () =>
      this.finishAppend(waiter, failure('quorum append timeout', Err.APPEND_TIMEOUT)))
    this.appendWaiters.add(waiter)
    for (const state of this.replication.values()) { this.replicate(state) }
    return seq
  }

  appendNoop() {
    if (this.state !== LEADER || this.commitTerm === this.term) { return }
    const term = this.term
    this.appendEntry(Buffer.alloc(0), (err) => {
      if (!err || this.state !== LEADER || this.term !== term || this.commitTerm === term) { return }
      this.warn(err)
      this.executor.after(this.retryms, () => {
        if (this.state === LEADER && this.term === term) { this.appendNoop() }
      })
    })
  }

  checkCommit() {
    if (this.state !== LEADER) { return }
    const matches = [this.seq, ...this.followers.map((id) => this.replication.get(id)?.matchIndex ?? -1n)]
      .sort((a, b) => a > b ? -1 : a < b ? 1 : 0)
    const seq = matches[this.quorum - 1] ?? -1n
    if (seq <= this.commitSeq) { return }
    const record = this.entry(seq)
    if (!record) { throw failure(`check commit ${seq} not found`, Err.RAFT_ILLEGAL) }
    if (record.term !== this.term) { return }
    this.commitSeq = seq
    this.commitTerm = record.term
    this.event('commit', { seq })
    this.executor.enqueue(() => this.applyReady())
    for (const waiter of [...this.appendWaiters]) {
      if (waiter.seq <= seq) { this.finishAppend(waiter) }
    }
    for (const state of this.replication.values()) { this.heartbeatFollower(state) }
  }

  heartbeatFollower(state) {
    if (this.state !== LEADER || state.term !== this.term) { return }
    const term = this.term
    const msg = {
      type: 'append', term, seqP: this.seq, termP: this.log.term, commitSeq: this.commitSeq,
    }
    // Heartbeats continue while data replication waits, retries, or is cancelled.
    const onReply = (err, reply) => {
      state.heartbeatCids.delete(cid)
      if (this.state !== LEADER || this.term !== term) { return }
      if (reply && reply.term !== term) { err = failure('node term different', Err.TERM_DIFF) }
      if (err) {
        this.warn(err)
        return
      }
      this.pongs.set(state.to, this.now)
      if (state.matchIndex < this.seq) { state.nextIndex = min(state.nextIndex, this.seq) }
      this.checkCommit()
      this.executor.enqueue(() => this.replicate(state))
    }
    const validateReply = (reply) => {
      if (reply.type === 'err') { return true }
      if (reply.term < term) { return false }
      util.validate(util.schemas.nonnegativeSeq, reply.seq, 'heartbeat ACK seq')
      return true
    }
    const cid = this.request(state.to, msg, this.opts.electionTimeout, onReply, validateReply,
      failure('heartbeat timeout', Err.HEARTBEAT_TIMEOUT))
    state.heartbeatCids.add(cid)
  }

  heartbeat() {
    if (this.state !== LEADER) { return }
    if (this.followers.length + 1 < this.quorum) {
      this.follower()
      return
    }
    for (const state of this.replication.values()) { this.heartbeatFollower(state) }
  }

  replicate(state) {
    if (this.state !== LEADER || state.term !== this.term || state.cancelled || state.cid) { return }
    if (state.retryTimer !== null) { return }
    const begin = state.nextIndex
    if (begin > this.seq) { return }
    const seqP = begin - 1n
    const requiredSeq = max(seqP, 0n)
    if (requiredSeq < this.log.begin) {
      state.cancelled = true
      this.warn(failure(`replication wants forgotten ${requiredSeq} have ${this.log.begin}`, Err.REPL_FORGOT))
      return
    }
    const previous = seqP < 0n ? null : this.entry(seqP)
    if (seqP >= 0n && !previous) { throw failure(`replication predecessor ${seqP} not found`, Err.RAFT_ILLEGAL) }
    const data = []
    let bytes = 0
    let rows = 0
    // Choose the page before reading payloads, allowing one oversized entry.
    for (const size of this.log.sizes(begin, this.opts.rpcMax)) {
      if (rows > 0 && bytes + size > this.opts.rpcMaxBytes) { break }
      bytes += size
      rows++
    }
    for (const record of this.log.iter(begin, { iterStepSize: rows })) {
      data.push(record)
      if (data.length >= rows) { break }
    }
    const term = this.term
    const msg = { type: 'append', term, seqP, termP: previous?.term ?? -1n, commitSeq: this.commitSeq, data }
    const matched = seqP + BigInt(data.length)
    const onReply = (err, reply) => {
      state.cid = null
      if (this.state !== LEADER || this.term !== term || state.cancelled) { return }
      if (reply && reply.term !== term) { err = failure('node term different', Err.TERM_DIFF) }
      if (err) {
        this.warn(err)
        if (err.code === Err.REPL_FORGOT) { state.cancelled = true; return }
        if (err.code === Err.REPL_BACKTRACK) {
          if (seqP < 0n) {
            state.cancelled = true
            this.warn(failure('genesis rejected', Err.RAFT_ILLEGAL))
            return
          }
          this.pongs.set(state.to, this.now)
          let next = begin - 1n
          if (reply?.term === term && util.isSeq(reply.logSeq)) {
            next = min(next, reply.logSeq + 1n)
          }
          state.nextIndex = min(state.nextIndex, next)
          this.executor.enqueue(() => this.replicate(state))
        } else {
          state.retryTimer = this.executor.after(this.retryms, () => {
            state.retryTimer = null
            this.replicate(state)
          })
        }
        return
      }
      this.pongs.set(state.to, this.now)
      state.matchIndex = max(state.matchIndex, matched)
      state.nextIndex = max(state.nextIndex, matched + 1n)
      this.checkCommit()
      this.executor.enqueue(() => this.replicate(state))
    }
    const validateReply = (reply) => {
      if (reply.type === 'err') { return true }
      if (reply.term < term) { return false }
      util.validate(util.schemas.nonnegativeSeq, reply.seq, 'append ACK seq')
      return true
    }
    state.cid = this.request(state.to, msg, this.opts.appendTimeout, onReply, validateReply)
  }

  appendRequest(from, msg) {
    const { cid, term, termP, seqP, commitSeq, data } = msg
    try {
      util.validate(util.raftSchemas.append, msg, 'append')
      if (data !== undefined && !util.isSeq(seqP + BigInt(data.length))) {
        throw failure('append sequence is illegal', Err.RPC_ILLEGAL)
      }
    } catch (err) {
      if (util.isCid(cid)) { this.rpcError(from, cid, err) }
      else { this.warn(Err.wrapError(err, null, `from ${from}: `)) }
      return
    }
    if (term < this.term) {
      this.rpcError(from, cid, failure('term is lesser', Err.TERM_DIFF))
      return
    }
    this.advanceTerm(term, from)
    if (this.state !== FOLLOWER || this.leader !== from) { this.follower(from) }
    this.resetElection()
    let matched = this.seq === seqP && this.log.term === termP
    if (data !== undefined) {
      if (!matched) {
        if (this.log.begin > 0n && max(seqP, 0n) < this.log.begin) {
          const err = failure(`rx append wants forgotten ${seqP} have ${this.log.begin}`, Err.REPL_FORGOT)
          this.warn(err)
          this.rpcError(from, cid, err)
          return
        }
        const prev = this.entry(seqP)
        if (seqP >= 0n && !prev || (seqP < 0n ? -1n : prev.term) !== termP) {
          this.rpcError(from, cid, failure(`predecessor mismatch at ${seqP}`, Err.REPL_BACKTRACK), { logSeq: this.seq })
          return
        }
        let same = 0
        for (; same < data.length; same++) {
          const have = this.entry(seqP + 1n + BigInt(same))
          if (!have || have.term !== data[same].term || !have.entry.equals(data[same].entry)) { break }
        }
        if (same < data.length) {
          const trim = seqP + BigInt(same)
          if (trim < this.commitSeq) {
            throw failure(`rx append cannot trim committed ${this.commitSeq} to ${trim}`, Err.RAFT_ILLEGAL)
          }
          this.log.trim(trim)
          this.log.appendBatch(data.slice(same), trim + 1n)
        }
      } else {
        this.log.appendBatch(data, seqP + 1n)
      }
      matched = true
    }
    if (matched) {
      const next = min(commitSeq, seqP + BigInt(data?.length ?? 0))
      if (next > this.commitSeq) {
        const record = this.entry(next)
        if (!record) { throw failure(`rx append commit ${next} not found`, Err.RAFT_ILLEGAL) }
        this.commitSeq = next
        this.commitTerm = record.term
        this.event('commit', { seq: next })
        this.executor.enqueue(() => this.applyReady())
      }
    }
    this.send(from, { type: 'ack', cid, term: this.term, seq: seqP + 1n })
  }

  receive(from, message) {
    if (this.stopped || !this.nodes.includes(from) || from === this.id) { return true }
    if (message.type === 'ack' || message.type === 'err') {
      let waiting
      try {
        util.validate(util.raftSchemas.ack_err, message, 'ack_err')
        waiting = this.requests.get(message.cid)
        if (!waiting || waiting.to !== from || !waiting.validate(message)) { return true }
      } catch (err) {
        this.warn(Err.wrapError(err, null, `${message.type} from ${from}: `))
        return true
      }
      if (message.term > this.term) { this.advanceTerm(message.term) }
      if (!this.requests.delete(message.cid)) { return true }
      this.executor.cancel(waiting.timer)
      this.executor.enqueue(() => waiting.callback(
        message.type === 'err' ? errorFromWire(message) : null, message,
      ))
    } else if (message.type === 'vote_request') {
      this.voteRequest(from, message)
    } else if (message.type === 'vote') {
      this.voteReply(from, message)
    } else if (message.type === 'append') {
      this.appendRequest(from, message)
    } else {
      return false
    }
    return true
  }
}

export { RaftEngine, FOLLOWER, CANDIDATE, LEADER }
