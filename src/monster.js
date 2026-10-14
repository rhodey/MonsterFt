import { pack } from 'msgpackr'
import { RaftEngine, LEADER } from './raft.js'
import * as Err from './error.js'
import * as util from './util.js'
import * as utilm from './utilm.js'
import * as sql from './utils.js'
import { errorToWire } from './wire.js'

const MONSTER_OUTCOME = 'monster_outcome'
const MONSTER_OUTCOME_REQUEST = 'monster_outcome_request'

const PHASE_CMD = 'cmd'
const PHASE_DECISION = 'decision'
const PHASE_SYNC = 'sync'

const failure = (message, code) => new Err.ErrorWithCode(message, code)
const MAX_SQLITE_BYTES = 999_999_000
const commandBytes = (items, nodes) =>
  items.reduce((total, item) => total + item.byteLength + 5, 64 + 9 * nodes)

// Monster's pending operations are resumed only by later executor inputs.
class MonsterEngine extends RaftEngine {
  constructor(proc, opts = {}) {
    super(proc, opts)
    const meta = this.store.load()
    this.repairState = Number(meta.repair_state)
    this.pendingCommand = meta.pending_cmd_seq === null ? null : {
      cmdSeq: meta.pending_cmd_seq, localDigest: utilm.asBuffer(meta.applied_local_digest),
    }
    this.keep = utilm.normalizeKeepOptions(this.opts)
    this.pendingApply = null
    this.reports = new Map()
    this.commands = []
    this.leaderCommand = null
    this.reportTimer = null
    this.draining = false
    this.drainRequests = []
    this.fatalError = null
    this.tickCount = 0n
  }

  repairError(state = this.repairState) {
    if (state !== Err.REPAIR_NO_QUORUM && state !== Err.REPAIR_OUTSIDE) {
      throw failure('repair state is illegal', Err.MONSTER_ILLEGAL)
    }
    return failure('repair required', state)
  }

  available() {
    if (this.repairState !== Err.REPAIR_NONE) { throw this.repairError() }
    if (this.draining) { throw failure('commands are draining', Err.DRAINING) }
  }

  fatal(err) {
    if (this.fatalError) { return }
    this.fatalError = err
    this.executor.clear()
  }

  writeOne(query, args, context) {
    try {
      if (this.store.db.prepare(query).run(...args).changes !== 1n) {
        throw failure('update count not one', Err.SQLITE_ERROR)
      }
    } catch (err) { throw Err.wrapError(err, Err.SQLITE_ERROR, `DB1 ${context} `) }
  }

  advanceApplied(seq, hash) {
    this.writeOne(`UPDATE monsterft_meta SET applied_seq = ?, applied_entry_hash = ?
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
    } catch (err) {
      const code = err instanceof Err.ErrorWithCode ? err.code : Err.SQLITE_ERROR
      throw Err.wrapError(err, code, 'DB1 retention ')
    }
  }

  start() {
    if (this.repairState === Err.REPAIR_OUTSIDE) { throw this.repairError() }
    this.prune()
    super.start()
  }

  reply(request, err, cmdSeq, outcomes) {
    if (!request || request.replied) { return }
    request.replied = true
    const message = err ? {
      type: 'err', term: this.term, ...errorToWire(err, false),
    } : { type: 'ack', term: this.term, cmdSeq, results: outcomes }
    if (request.to === 'gate') { this.output('gate', { ...message, clientId: request.clientId }) }
    else { this.send(request.to, { ...message, cid: request.cid }) }
  }

  protocolFailure(leaderCommand, err) {
    if (this.leaderCommand !== leaderCommand) { return }
    if (leaderCommand.request?.replied || !leaderCommand.request) { this.warn(err) }
    this.reply(leaderCommand.request, err)
    if (this.state === LEADER && this.term === leaderCommand.term) { this.follower() }
  }

  fenceLeader() {
    this.writeOne(`UPDATE monsterft_meta SET repair_state = ${Err.REPAIR_OUTSIDE} WHERE id = 1`, [], 'write repair')
    this.repairState = Err.REPAIR_OUTSIDE
    this.follower()
    this.fatal(this.repairError())
  }

  finishDrain() {
    if (!this.draining || this.pendingApply !== null || this.pendingCommand !== null) { return false }
    for (const clientId of this.drainRequests.splice(0)) { this.output('gate', { type: 'ack', clientId }) }
    return true
  }

  drain(clientId) {
    this.draining = true
    this.drainRequests.push(clientId)
    for (const request of this.commands.splice(0)) { this.reply(request, failure('commands are draining', Err.DRAINING)) }
    this.finishDrain()
  }

  execute(record, seq, hash, entry) {
    const cid = this.random.cid()
    this.pendingApply = { cid, seq, hash, tickCount: this.tickCount, count: entry?.items.length ?? 0 }
    this.output('apply', {
      type: 'execute', cid, tickCount: this.tickCount, term: record.term, seq,
      entryHash: hash, items: entry?.items ?? null, matchIndex: entry?.matchIndex ?? null,
    })
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
    const repair = !entry.quorum ? Err.REPAIR_NO_QUORUM :
      this.syncAgrees(entry, command.localDigest) ? Err.REPAIR_NONE : Err.REPAIR_OUTSIDE
    this.writeOne(`UPDATE monsterft_meta SET pending_cmd_seq = NULL,
      applied_seq = ?, applied_entry_hash = ?, repair_state = ?
      WHERE id = 1 AND pending_cmd_seq = ?`, [seq, hash, repair, entry.cmdSeq], 'write CMD decision')
    this.pendingCommand = null
    this.appliedSeq = seq
    this.repairState = repair
    this.reports.clear()
    this.event('sync', { cmdSeq: entry.cmdSeq, syncSeq: seq, quorum: entry.quorum, agree, disagree })
    if (repair === Err.REPAIR_NO_QUORUM) { this.queueLocalEvent('error', this.repairError()) }
    if (repair === Err.REPAIR_OUTSIDE) { this.fatal(this.repairError()) }
  }

  rememberReport(from, digest) {
    if (!this.reports.has(from)) { this.reports.set(from, utilm.asBuffer(digest)) }
    if (this.leaderCommand?.phase === PHASE_DECISION) { this.executor.enqueue(() => this.decide()) }
  }

  announceOutcome() {
    const command = this.pendingCommand
    if (!command) { return }
    if (this.state === LEADER) { this.rememberReport(this.id, command.localDigest) }
    else if (this.leader !== null) {
      this.send(this.leader, { type: MONSTER_OUTCOME, cmdSeq: command.cmdSeq, digest: command.localDigest })
    }
  }

  decide() {
    const leaderCommand = this.leaderCommand
    if (!leaderCommand || leaderCommand.phase !== PHASE_DECISION || this.state !== LEADER || this.term !== leaderCommand.term) { return }
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
      type: 'sync', cmdSeq: leaderCommand.cmdSeq, quorum: true, digest: agreement.digest,
      agree: [...agreement.nodes].sort(),
      disagree: [...this.reports].filter(([, digest]) => !digest.equals(agreement.digest)).map(([id]) => id).sort(),
    } : {
      type: 'sync', cmdSeq: leaderCommand.cmdSeq, quorum: false, agree: [], disagree: [...this.reports.keys()].sort(),
    }
    const agrees = this.syncAgrees(sync, this.pendingCommand.localDigest)
    this.reply(leaderCommand.request,
      agrees ? null : this.repairError(sync.quorum ? Err.REPAIR_OUTSIDE : Err.REPAIR_NO_QUORUM),
      leaderCommand.cmdSeq, leaderCommand.outcomes)
    if (sync.quorum && !agrees) { this.fenceLeader(); return }
    leaderCommand.phase = PHASE_SYNC
    leaderCommand.syncSeq = null
    leaderCommand.syncSeq = this.appendEntry(pack(sync), (err) => {
      if (err) { this.protocolFailure(leaderCommand, err) }
      else { this.pumpLeader() }
    })
  }

  awaitDecision() {
    this.rememberReport(this.id, this.pendingCommand.localDigest)
    this.decide()
    if (this.leaderCommand?.phase !== PHASE_DECISION) { return }
    const ask = () => {
      if (this.state !== LEADER || this.leaderCommand?.phase !== PHASE_DECISION) { return }
      for (const to of this.nodes) {
        if (to !== this.id) { this.send(to, { type: MONSTER_OUTCOME_REQUEST, cmdSeq: this.leaderCommand.cmdSeq }) }
      }
    }
    ask()
    this.executor.cancel(this.reportTimer)
    this.reportTimer = this.executor.after(this.retryms, ask, this.retryms)
  }

  leaderChanged() {
    if (!this.commands) { return }
    if (this.state !== LEADER || this.leaderCommand && this.leaderCommand.term !== this.term) {
      const err = failure('node not leader', Err.NOT_LEADER)
      this.executor.cancel(this.reportTimer)
      this.reportTimer = null
      this.reply(this.leaderCommand?.request, err)
      this.leaderCommand = null
      for (const request of this.commands.splice(0)) { this.reply(request, err) }
      return
    }
    this.executor.enqueue(() => this.pumpLeader())
  }

  pumpLeader() {
    if (this.stopped || this.fatalError || this.state !== LEADER) { return }
    if (this.leaderCommand) {
      if (this.leaderCommand.term !== this.term) { return }
      if (this.leaderCommand.phase === PHASE_CMD && this.pendingCommand?.cmdSeq === this.leaderCommand.cmdSeq) {
        this.leaderCommand.phase = PHASE_DECISION
        this.awaitDecision()
      } else if (this.leaderCommand.phase === PHASE_SYNC && this.leaderCommand.syncSeq !== null &&
          this.appliedSeq >= this.leaderCommand.syncSeq) {
        this.leaderCommand = null
        this.pumpLeader()
      }
      return
    }
    // The election no-op must be committed and applied before resuming protocol work.
    if (this.commitTerm !== this.term || this.appliedSeq < this.commitSeq || this.pendingApply) { return }
    if (this.repairState !== Err.REPAIR_NONE) {
      for (const request of this.commands.splice(0)) { this.reply(request, this.repairError()) }
      return
    }
    if (this.pendingCommand !== null) {
      this.leaderCommand = { term: this.term, cmdSeq: this.pendingCommand.cmdSeq, request: null, phase: PHASE_DECISION }
      this.awaitDecision()
      return
    }
    if (this.finishDrain()) { return }
    const request = this.commands.shift()
    if (!request) { return }
    const leaderCommand = { term: this.term, request, cmdSeq: null, outcomes: null, phase: PHASE_CMD }
    this.leaderCommand = leaderCommand
    const matchIndex = this.nodes.map((id) => id === this.id ? this.seq : this.replication.get(id)?.matchIndex ?? -1n)
    leaderCommand.cmdSeq = this.appendEntry(pack({ type: 'cmd', items: request.items, matchIndex }), (err) => {
      if (err) { this.protocolFailure(leaderCommand, err) }
      else { this.pumpLeader() }
    })
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
        const entry = utilm.decodeEntry(record.entry)
        if (entry.type === 'cmd') {
          if (this.draining) { this.finishDrain(); return }
          if (this.repairState !== Err.REPAIR_NONE) { this.fatal(this.repairError()); return }
          utilm.validateCmdEntry(this, entry, seq)
          if (this.pendingCommand !== null) {
            throw failure('CMD entry while another CMD is pending', Err.MONSTER_ILLEGAL)
          }
          this.execute(record, seq, hash, entry)
          return
        }
        this.applySync(entry, seq, hash)
        if (this.fatalError) { return }
      }
      this.prune()
      if (this.finishDrain()) { return }
    }
    this.pumpLeader()
    this.finishDrain()
  }

  receiveApply(reply) {
    const pending = this.pendingApply
    if (!pending || reply.cid !== pending.cid || reply.tickCount !== pending.tickCount) {
      throw failure('Apply reply has no matching request', Err.MONSTER_ILLEGAL)
    }
    const receipt = sql.validateReceipt(reply.receipt)
    if (receipt.seq !== pending.seq || !utilm.asBuffer(receipt.entry_hash).equals(pending.hash) ||
        !utilm.isOutcomes(reply.outcomes, pending.count) ||
        (receipt.seq === 0n && !utilm.asBuffer(receipt.local_digest).equals(Buffer.alloc(util.HASH_BYTES)))) {
      throw failure('Apply reply does not match execution', Err.MONSTER_CORRUPT)
    }
    sql.copyApplyInto(this.store.db, receipt)
    this.appliedSeq = receipt.seq
    this.pendingApply = null
    if (receipt.seq > 0n) {
      this.pendingCommand = { cmdSeq: receipt.seq, localDigest: utilm.asBuffer(receipt.local_digest) }
      this.reports.clear()
      if (this.leaderCommand?.cmdSeq === receipt.seq) { this.leaderCommand.outcomes = reply.outcomes }
      this.event('cmd', { cmdSeq: receipt.seq, cmdCount: pending.count })
      this.announceOutcome()
    }
    this.prune()
    this.applyReady()
  }

  submit(clientId, items) {
    const request = { to: 'gate', clientId, items, replied: false }
    try {
      this.available()
      if (commandBytes(items, this.nodes.length) > MAX_SQLITE_BYTES) {
        throw failure('command items are illegal', Err.ARGUMENT_ILLEGAL)
      }
      if (this.state !== LEADER && this.leader === null) {
        throw failure('forward no leader', Err.NO_LEADER)
      }
    } catch (err) { this.reply(request, err); return }
    if (this.state === LEADER) {
      this.commands.push(request)
      this.pumpLeader()
    } else {
      const onReply = (err, reply) => {
        if (err) {
          // This stack came from another node.
          if (!request.replied) {
            request.replied = true
            this.output('gate', { type: 'err', clientId: request.clientId, ...errorToWire(err, typeof reply?.stack === 'string') })
          }
        } else { this.reply(request, null, reply.cmdSeq, reply.results) }
      }
      const validateReply = (reply) => {
        if (reply.type === 'err') { return true }
        utilm.validateCommandResult(reply, items.length)
        return true
      }
      this.request(this.leader, { type: 'fwd_cmd', term: this.term, items },
        this.opts.appendTimeout, onReply, validateReply)
    }
  }

  receive(from, msg) {
    if (super.receive(from, msg)) { return }
    if (msg.type === 'fwd_cmd') {
      if (!util.isCid(msg.cid)) {
        this.warn(failure(`fwd_cmd from ${from} cid is illegal`, Err.RPC_ILLEGAL))
        return
      }
      const request = { to: from, cid: msg.cid, items: msg.items, replied: false }
      try {
        util.validate(utilm.monsterSchemas.forwarded, msg, 'forward CMD')
        if (commandBytes(msg.items, this.nodes.length) > MAX_SQLITE_BYTES) {
          throw failure('forward CMD illegal', Err.RPC_ILLEGAL)
        }
        if (this.state !== LEADER) { throw failure('node not leader', Err.NOT_LEADER) }
        if (msg.term !== this.term) { throw failure('node term different', Err.TERM_DIFF) }
        this.available()
      } catch (err) { this.reply(request, err); this.warn(err); return }
      this.commands.push(request)
      this.pumpLeader()
    } else if (msg.type === MONSTER_OUTCOME_REQUEST) {
      try { utilm.validateOutcomeRequestMsg(msg) } catch (err) { this.warn(err); return }
      if (this.pendingCommand?.cmdSeq === msg.cmdSeq) {
        this.send(from, { type: MONSTER_OUTCOME, cmdSeq: msg.cmdSeq, digest: this.pendingCommand.localDigest })
      }
    } else if (msg.type === MONSTER_OUTCOME) {
      try { utilm.validateOutcomeMsg(msg) } catch (err) { this.warn(err); return }
      if (this.state === LEADER && this.pendingCommand?.cmdSeq === msg.cmdSeq) {
        this.rememberReport(from, msg.digest)
      }
    } else {
      this.warn(failure(`unknown message type '${msg.type}' from ${from}`, Err.RPC_ILLEGAL))
    }
  }
}

export { MonsterEngine }
