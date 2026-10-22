import { pack, unpack } from 'msgpackr'
import { RaftEngine } from './raft.js'
import * as Err from './error.js'
import * as util from './util.js'
import * as utilm from './utilm.js'
import * as sql from './utils.js'
import { errorToWire } from './wire.js'

const failure = (message, code) => new Err.ErrorWithCode(message, code)
const MAX_SQLITE_BYTES = 999_999_000
const commandBytes = (items, nodes) =>
  items.reduce((total, item) => total + item.byteLength + 5, 64 + 9 * nodes)

// Monster's pending operations are resumed only by later executor inputs.
class MonsterEngine extends RaftEngine {
  constructor(...args) {
    super(...args)
    const meta = this.store.load()
    this.repairState = Number(meta.repair_state)
    this.pendingCommand = meta.pending_cmd_seq === null ? null : {
      cmdSeq: meta.pending_cmd_seq, localDigest: utilm.asBuffer(meta.applied_local_digest),
    }
    this.keep = utilm.normalizeKeepOptions(this.opts)
    this.pendingApply = null
    this.reports = new Map()
    this.commands = []
    this.active = null
    this.reportTimer = null
    this.draining = false
    this.drainRequests = []
    this.fatalError = null
    this.tickCount = 0n
  }

  repairError(state = this.repairState) {
    if (state !== 1 && state !== 2) { throw failure('repair state is illegal', Err.MONSTER_ILLEGAL) }
    return failure('repair required', state === 1 ? Err.REPAIR_QUORUM_IMPOSSIBLE : Err.REPAIR_OUTSIDE_AGREEMENT)
  }

  available() {
    if (this.repairState !== 0) { throw this.repairError() }
    if (this.draining) { throw failure('commands are draining', Err.DRAINING) }
  }

  start() {
    if (this.repairState === 2) { throw this.repairError() }
    this.prune()
    super.start()
  }

  write(query, args, context) {
    try {
      if (this.store.db.prepare(query).run(...args).changes !== 1n) {
        throw failure('update count not one', Err.SQLITE_ERROR)
      }
    } catch (err) { throw Err.wrapError(err, Err.SQLITE_ERROR, `DB1 ${context} `) }
  }

  advanceApplied(seq, hash) {
    this.write(`UPDATE monsterft_meta SET applied_seq = ?, applied_entry_hash = ?
      WHERE id = 1`, [seq, hash], 'advance applied')
    this.appliedSeq = seq
  }

  prune() {
    if (this.keep === null || this.log.begin < 0n || this.pendingCommand !== null) { return }
    if (this.appliedSeq - this.log.begin + 1n < this.keep.trigger) { return }
    try {
      // The checkpoint and this deletion commit in the same DB1 transaction.
      this.store.db.prepare('DELETE FROM raft_log WHERE seq < ?')
        .run(this.appliedSeq - this.keep.target + 1n)
      this.log.readHead()
    } catch (err) { throw Err.wrapError(err, Err.SQLITE_ERROR, 'DB1 retention ') }
  }

  applyReady() {
    if (this.stopped || this.fatalError || this.pendingApply !== null) { return }
    while (this.appliedSeq < this.commitSeq) {
      const seq = this.appliedSeq + 1n
      const record = this.entry(seq)
      if (!record) { throw failure(`apply entry ${seq} not found`, Err.MONSTER_CORRUPT) }
      const hash = utilm.digestEntry(record.term, record.entry)
      if (record.entry.length === 0) {
        if (seq === 0n) {
          this.execute(record, seq, hash, null)
          return
        }
        this.advanceApplied(seq, hash)
      } else {
        let entry
        try { entry = unpack(record.entry) } catch (err) {
          throw Err.wrapError(err, Err.MONSTER_CORRUPT, 'entry decode ')
        }
        if (entry?.type === 'cmd') {
          if (this.draining) { this.finishDrain(); return }
          if (this.repairState !== 0) { this.fatal(this.repairError()); return }
          utilm.validateCmdEntry(this, entry, seq)
          if (this.pendingCommand !== null) {
            throw failure('CMD entry while another CMD is pending', Err.MONSTER_ILLEGAL)
          }
          this.execute(record, seq, hash, entry)
          return
        }
        if (entry?.type !== 'sync') { throw failure('entry type is corrupt', Err.MONSTER_CORRUPT) }
        this.applySync(entry, seq, hash)
        if (this.fatalError) { return }
      }
      this.prune()
      if (this.finishDrain()) { return }
    }
    this.pumpLeader()
    this.finishDrain()
  }

  execute(record, seq, hash, entry) {
    const cid = this.random.cid()
    this.pendingApply = { cid, seq, hash, count: entry?.items.length ?? 0 }
    this.output('apply', {
      type: 'execute', cid, tickCount: this.tickCount, term: record.term, seq,
      entryHash: hash, items: entry?.items ?? null, matchIndex: entry?.matchIndex ?? null,
    })
  }

  receiveApply(reply) {
    const pending = this.pendingApply
    if (!pending || reply?.type !== 'ack' || reply.cid !== pending.cid) {
      throw failure('Apply reply has no matching request', Err.RPC_ILLEGAL)
    }
    const receipt = sql.validateReceipt(reply.receipt)
    if (receipt.seq !== pending.seq || !utilm.asBuffer(receipt.entry_hash).equals(pending.hash) ||
        !Array.isArray(reply.outcomes) || reply.outcomes.length !== pending.count ||
        !reply.outcomes.every(utilm.isOutcomeTuple) ||
        (receipt.seq === 0n && !utilm.asBuffer(receipt.local_digest).equals(Buffer.alloc(util.HASH_BYTES)))) {
      throw failure('Apply reply does not match execution', Err.MONSTER_CORRUPT)
    }
    sql.copyApply(this.store.db, receipt)
    this.appliedSeq = receipt.seq
    this.pendingApply = null
    if (receipt.seq > 0n) {
      this.pendingCommand = { cmdSeq: receipt.seq, localDigest: utilm.asBuffer(receipt.local_digest) }
      this.reports.clear()
      if (this.active?.cmdSeq === receipt.seq) { this.active.outcomes = reply.outcomes }
      this.event('cmd', { cmdSeq: receipt.seq, cmdCount: pending.count })
      this.announceOutcome()
    }
    this.prune()
    this.applyReady()
  }

  syncAgrees(entry, digest) {
    if (!entry.quorum) { return false }
    if (entry.agree.includes(this.id)) { return true }
    if (entry.disagree.includes(this.id)) { return false }
    return utilm.asBuffer(digest).equals(utilm.asBuffer(entry.digest))
  }

  applySync(entry, seq, hash) {
    const { agree, disagree } = utilm.validateSyncEntry(this, entry)
    const command = this.pendingCommand
    if (command === null || command.cmdSeq !== entry.cmdSeq) {
      throw failure('SYNC entry is missing CMD', Err.MONSTER_CORRUPT)
    }
    const repair = !entry.quorum ? 1 : this.syncAgrees(entry, command.localDigest) ? 0 : 2
    this.write(`UPDATE monsterft_meta SET pending_cmd_seq = NULL,
      applied_seq = ?, applied_entry_hash = ?, repair_state = ?
      WHERE id = 1 AND pending_cmd_seq = ?`, [seq, hash, repair, entry.cmdSeq], 'write CMD decision')
    this.pendingCommand = null
    this.appliedSeq = seq
    this.repairState = repair
    this.reports.clear()
    this.event('sync', { cmdSeq: entry.cmdSeq, syncSeq: seq, quorum: entry.quorum, agree, disagree })
    if (repair === 1) { this.diagnostic?.('error', this.repairError()) }
    if (repair === 2) { this.fatal(this.repairError()) }
  }

  fatal(err) {
    if (this.fatalError) { return }
    this.fatalError = err
    this.executor.clear()
  }

  fenceLeader() {
    this.write('UPDATE monsterft_meta SET repair_state = 2 WHERE id = 1', [], 'write repair')
    this.repairState = 2
    this.follower()
    this.fatal(this.repairError())
  }

  respond(request, err, cmdSeq, outcomes) {
    if (!request || request.responded) { return }
    request.responded = true
    const message = err ? {
      type: 'err', cid: request.cid, term: this.term, ...errorToWire(err, false),
    } : { type: 'ack', cid: request.cid, term: this.term, cmdSeq, results: outcomes }
    if (request.to === 'client') { this.output('client', message) }
    else { this.send(request.to, message) }
  }

  submit(requestId, items) {
    const request = { to: 'client', cid: requestId, items, responded: false }
    try {
      this.available()
      if (!Array.isArray(items) || items.length === 0 || !items.every(Buffer.isBuffer) ||
          commandBytes(items, this.nodes.length) > MAX_SQLITE_BYTES) {
        throw failure('command items are illegal', Err.ARGUMENT_ILLEGAL)
      }
      if (this.state !== 'leader' && this.leader === null) {
        throw failure('forward no leader', Err.NO_LEADER)
      }
    } catch (err) { this.respond(request, err); return }
    if (this.state === 'leader') {
      this.commands.push(request)
      this.pumpLeader()
    } else {
      this.request(this.leader, { type: 'fwd_cmd', term: this.term, items }, this.opts.appendTimeout,
        (err, reply) => {
          if (err) {
            // This stack came from another node and is identical on the three Procs.
            if (!request.responded) {
              request.responded = true
              this.output('client', { type: 'err', cid: request.cid, ...errorToWire(err, typeof reply?.stack === 'string') })
            }
          } else { this.respond(request, null, reply.cmdSeq, reply.results) }
        }, (reply) => reply.type === 'err' || utilm.isFwdCmdAck(reply, items.length))
    }
  }

  leaderChanged() {
    if (!this.commands) { return }
    if (this.state !== 'leader' || this.active && this.active.term !== this.term) {
      const err = failure('node not leader', Err.NOT_LEADER)
      this.executor.cancel(this.reportTimer)
      this.reportTimer = null
      this.respond(this.active?.request, err)
      this.active = null
      for (const request of this.commands.splice(0)) { this.respond(request, err) }
      return
    }
    this.executor.enqueue(() => this.pumpLeader())
  }

  pumpLeader() {
    if (this.stopped || this.fatalError || this.state !== 'leader') { return }
    if (this.active) {
      if (this.active.term !== this.term) { return }
      if (this.active.phase === 'cmd' && this.pendingCommand?.cmdSeq === this.active.cmdSeq) {
        this.active.phase = 'decision'
        this.awaitDecision()
      } else if (this.active.phase === 'sync' && this.active.syncSeq !== null &&
          this.appliedSeq >= this.active.syncSeq) {
        this.active = null
        this.pumpLeader()
      }
      return
    }
    // The election no-op must be committed and applied before resuming protocol work.
    if (this.commitTerm !== this.term || this.appliedSeq < this.commitSeq || this.pendingApply) { return }
    if (this.repairState !== 0) {
      for (const request of this.commands.splice(0)) { this.respond(request, this.repairError()) }
      return
    }
    if (this.pendingCommand !== null) {
      this.active = { term: this.term, cmdSeq: this.pendingCommand.cmdSeq, request: null, phase: 'decision' }
      this.awaitDecision()
      return
    }
    if (this.finishDrain()) { return }
    const request = this.commands.shift()
    if (!request) { return }
    const active = { term: this.term, request, cmdSeq: null, outcomes: null, phase: 'cmd' }
    this.active = active
    const matchIndex = this.nodes.map((id) => id === this.id ? this.seq : this.replication.get(id)?.matchIndex ?? -1n)
    active.cmdSeq = this.appendEntry(pack({ type: 'cmd', items: request.items, matchIndex }), (err) => {
      if (err) { this.protocolFailure(active, err) }
      else { this.pumpLeader() }
    })
  }

  protocolFailure(active, err) {
    if (this.active !== active) { return }
    if (active.request?.responded || !active.request) { this.warn(err) }
    this.respond(active.request, err)
    if (this.state === 'leader' && this.term === active.term) { this.follower() }
  }

  announceOutcome() {
    const command = this.pendingCommand
    if (!command) { return }
    if (this.state === 'leader') { this.rememberReport(this.id, command.localDigest) }
    else if (this.leader !== null) {
      this.send(this.leader, { type: 'monster_outcome', cmdSeq: command.cmdSeq, digest: command.localDigest })
    }
  }

  rememberReport(from, digest) {
    if (!this.reports.has(from)) { this.reports.set(from, utilm.asBuffer(digest)) }
    if (this.active?.phase === 'decision') { this.executor.enqueue(() => this.decide()) }
  }

  awaitDecision() {
    this.rememberReport(this.id, this.pendingCommand.localDigest)
    this.decide()
    if (this.active?.phase !== 'decision') { return }
    const ask = () => {
      if (this.state !== 'leader' || this.active?.phase !== 'decision') { return }
      for (const to of this.nodes) {
        if (to !== this.id) { this.send(to, { type: 'monster_outcome_request', cmdSeq: this.active.cmdSeq }) }
      }
    }
    ask()
    this.executor.cancel(this.reportTimer)
    this.reportTimer = this.executor.after(this.retryms, ask, this.retryms)
  }

  decide() {
    const active = this.active
    if (!active || active.phase !== 'decision' || this.state !== 'leader' || this.term !== active.term) { return }
    const groups = new Map()
    let largest = 0
    let agreement = null
    for (const [id, digest] of this.reports) {
      const key = digest.toString('hex')
      let group = groups.get(key)
      if (!group) { group = { digest, nodes: [] }; groups.set(key, group) }
      group.nodes.push(id)
      largest = Math.max(largest, group.nodes.length)
      if (group.nodes.length >= this.quorum) { agreement = group }
    }
    const impossible = !agreement && largest + this.nodes.length - this.reports.size < this.quorum
    if (!agreement && !impossible) { return }
    this.executor.cancel(this.reportTimer)
    this.reportTimer = null
    const sync = agreement ? {
      type: 'sync', cmdSeq: active.cmdSeq, quorum: true, digest: agreement.digest,
      agree: [...agreement.nodes].sort(),
      disagree: [...this.reports].filter(([, digest]) => !digest.equals(agreement.digest)).map(([id]) => id).sort(),
    } : {
      type: 'sync', cmdSeq: active.cmdSeq, quorum: false, agree: [], disagree: [...this.reports.keys()].sort(),
    }
    const agrees = this.syncAgrees(sync, this.pendingCommand.localDigest)
    this.respond(active.request, agrees ? null : this.repairError(sync.quorum ? 2 : 1), active.cmdSeq, active.outcomes)
    if (sync.quorum && !agrees) { this.fenceLeader(); return }
    active.phase = 'sync'
    active.syncSeq = null
    active.syncSeq = this.appendEntry(pack(sync), (err) => {
      if (err) { this.protocolFailure(active, err) }
      else { this.pumpLeader() }
    })
  }

  receiveCustom(from, msg) {
    if (msg.type === 'fwd_cmd') {
      if (!util.isCid(msg.cid)) { return }
      const request = { to: from, cid: msg.cid, items: msg.items, responded: false }
      try {
        if (!utilm.isFwdCmdMsg(msg) || commandBytes(msg.items, this.nodes.length) > MAX_SQLITE_BYTES) {
          throw failure('forward CMD illegal', Err.RPC_ILLEGAL)
        }
        if (this.state !== 'leader') { throw failure('node not leader', Err.NOT_LEADER) }
        if (msg.term !== this.term) { throw failure('node term different', Err.TERM_DIFF) }
        this.available()
      } catch (err) { this.respond(request, err); this.warn(err); return }
      this.commands.push(request)
      this.pumpLeader()
    } else if (msg.type === 'monster_outcome_request') {
      try { utilm.validateOutcomeRequestMsg(msg) } catch (err) { this.warn(err); return }
      if (this.pendingCommand?.cmdSeq === msg.cmdSeq) {
        this.send(from, { type: 'monster_outcome', cmdSeq: msg.cmdSeq, digest: this.pendingCommand.localDigest })
      }
    } else if (msg.type === 'monster_outcome') {
      try { utilm.validateOutcomeMsg(msg) } catch (err) { this.warn(err); return }
      if (this.state === 'leader' && this.pendingCommand?.cmdSeq === msg.cmdSeq) {
        this.rememberReport(from, msg.digest)
      }
    }
  }

  drain(requestId) {
    this.draining = true
    this.drainRequests.push(requestId)
    for (const request of this.commands.splice(0)) { this.respond(request, failure('commands are draining', Err.DRAINING)) }
    this.finishDrain()
  }

  finishDrain() {
    if (!this.draining || this.pendingApply !== null || this.pendingCommand !== null) { return false }
    for (const cid of this.drainRequests.splice(0)) { this.output('client', { type: 'ack', cid }) }
    return true
  }
}

export { MonsterEngine }
