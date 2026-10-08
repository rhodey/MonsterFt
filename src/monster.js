import crypto from 'node:crypto'
import fs from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import { pack, unpack } from 'msgpackr'
import * as Err from './error.js'
import { RaftNode } from './node.js'
import * as util from './util.js'
import * as utilm from './utilm.js'

const CMD = 'cmd'
const SYNC = 'sync'

const FWD_CMD = 'fwd_cmd'
const OUTCOME = 'monster_outcome'
const OUTCOME_REQUEST = 'monster_outcome_request'
const ACK = 'ack'
const ERR = 'err'

const FULFILLED = 'fulfilled'
const REJECTED = 'rejected'
const FOLLOWER = 'follower'
const LEADER = 'leader'

const REPAIR_NONE = 0
const REPAIR_QUORUM_IMPOSSIBLE = 1
const REPAIR_OUTSIDE_AGREEMENT = 2

const noop = () => {}

const asBuffer = (value) => {
  if (value === null || value === undefined) { return value }
  return Buffer.isBuffer(value)
    ? value
    : Buffer.from(value.buffer, value.byteOffset, value.byteLength)
}

const digestEntry = (term, buffer) => {
  const prefix = Buffer.allocUnsafe(8)
  prefix.writeBigUInt64LE(term)
  const hash = crypto.createHash('sha256').update(prefix)
  if (buffer !== null) { hash.update(buffer) }
  return hash.digest()
}

const outcomesWire = (outcomes, includeStack=true) => outcomes.map((outcome) => {
  if (outcome.status === FULFILLED) { return [0, outcome.result] }
  const { message, code, sqlCode } = outcome.error
  const item = [1, message, code, sqlCode]
  if (includeStack && typeof outcome.error.stack === 'string') {
    item.push(outcome.error.stack)
  }
  return item
})

const digestCmd = (term, cmdSeq, patchset, outcomes) => {
  outcomes = outcomesWire(outcomes, false)
  return crypto.createHash('sha256')
    .update(pack([term, cmdSeq, patchset, outcomes]))
    .digest()
}

class MonsterNode extends RaftNode {
  constructor(id, nodes, send, databasePath, opts={}) {
    opts = typeof opts === 'function' ? opts() : opts
    if (!opts || typeof opts.apply !== 'function') {
      throw new Err.ErrorWithCode(
        'apply must be a function',
        Err.ARGUMENT_ILLEGAL,
      )
    }

    const userApply = opts.apply
    let monster = null
    const apply = (...args) => monster._monsterApply(...args)
    super(
      id,
      nodes,
      send,
      databasePath,
      { ...opts, apply },
    )
    monster = this

    const memory = databasePath === ':memory:'
    const monsterDatabasePath = memory ? databasePath : `${databasePath}2`
    if (!memory) {
      const raftExists = fs.existsSync(databasePath)
      const monsterExists = fs.existsSync(monsterDatabasePath)
      if (raftExists !== monsterExists) {
        throw new Err.ErrorWithCode(
          'database pair must have both files present or both files absent',
          Err.ARGUMENT_ILLEGAL,
        )
      }
    }

    const keep = utilm.normalizeKeepOptions(opts)
    this.db = null
    this._monsterDatabasePath = monsterDatabasePath
    this._monsterUserApply = userApply
    this._monsterKeep = keep

    this._monsterDbActive = false
    this._monsterDbWork = null
    this._monsterDbQueue = []

    this._monsterRepairState = REPAIR_NONE
    this._monsterPendingCommand = null
    this._monsterPendingReports = new Map()

    this._monsterLeaderSync = Promise.resolve()
    this._monsterLeaderSyncTerm = null
    this._monsterProtocol = Promise.resolve()

    this._monsterDraining = false
    this._monsterDrainWork = null
    this.on('change', (state) => this._monsterOnChange(state))
  }

  _autoRaft() {
    return false
  }

  _monsterSortNodes(nodes) {
    return [...new Set(nodes)].sort()
  }

  _monsterSyncAgrees(entry, localDigest) {
    if (!entry.quorum) { return false }
    if (entry.agree.includes(this.id)) { return true }
    if (entry.disagree.includes(this.id)) { return false }
    return asBuffer(localDigest).equals(asBuffer(entry.digest))
  }

  _monsterPrune() {
    const keep = this._monsterKeep
    if (keep === null || this.log.begin < 0n ||
        this._monsterPendingCommand !== null) {
      return
    }
    const retained = this._applySeq - this.log.begin + 1n
    if (retained < keep.trigger) { return }

    // DB2 already durably includes this applied prefix. Keep its checkpoint
    // entry and every unapplied entry so the pair remains safe to reopen.
    const begin = this._applySeq - keep.target + 1n
    try {
      this.log.db.prepare('DELETE FROM raft_log WHERE seq < ?').run(begin)
      this.log.readHead()
    } catch (err) {
      throw Err.wrapError(err, Err.SQLITE_ERROR, 'DB1 retention ')
    }
  }

  _monsterFatalError(err) {
    if (!this._closing) { this._emitSafe('fatal', err) }
    return err
  }

  _monsterRepairError(state=this._monsterRepairState) {
    if (state !== REPAIR_QUORUM_IMPOSSIBLE &&
        state !== REPAIR_OUTSIDE_AGREEMENT) {
      throw this._monsterFatalError(new Err.ErrorWithCode(
        'repair state is illegal', Err.MONSTER_ILLEGAL
      ))
    }
    const code = state === REPAIR_QUORUM_IMPOSSIBLE
      ? Err.REPAIR_QUORUM_IMPOSSIBLE
      : Err.REPAIR_OUTSIDE_AGREEMENT
    return new Err.ErrorWithCode('repair required', code)
  }

  _monsterAssertAvailable() {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    }
    if (this._monsterRepairState !== REPAIR_NONE) {
      throw this._monsterRepairError()
    }
    if (this._monsterDraining) {
      throw new Err.ErrorWithCode('commands are draining', Err.DRAINING)
    }
  }

  _monsterAssertLeader(term=null) {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    }
    if (this.state !== LEADER) {
      throw new Err.ErrorWithCode('node not leader', Err.NOT_LEADER)
    }
    if (term !== null && this.term !== term) {
      throw new Err.ErrorWithCode('node term different', Err.TERM_DIFF)
    }
  }

  _monsterStepDownForTerm(term) {
    if (this._closing || this.state !== LEADER || this.term !== term) {
      return false
    }
    this._toFollower()
    return true
  }

  async _monsterWriteTx(db, fn) {
    let active = false
    try {
      this._throwIfClosing()
      db.exec('BEGIN IMMEDIATE')
      active = true
      const result = await fn(db)
      this._throwIfClosing()
      db.exec('COMMIT')
      active = false
      return result
    } catch (err) {
      this._throwIfClosing()
      if (active) {
        try {
          db.exec('ROLLBACK')
        } catch (rollbackErr) {
          this._monsterFatalError(Err.wrapError(
            rollbackErr,
            Err.SQLITE_ERROR,
            'DB2 rollback ',
          ))
        }
      }
      throw err
    }
  }

  _monsterAdvanceApplied(db, seq, entryHash) {
    try {
      const result = db.prepare(`
        UPDATE monsterft_meta
        SET applied_seq = ?, applied_entry_hash = ?
        WHERE id = 1
      `).run(seq, entryHash)
      if (result.changes !== 1n) {
        throw new Err.ErrorWithCode(
          'update count not one',
          Err.SQLITE_ERROR,
        )
      }
    } catch (err) {
      throw Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 advance applied ')
    }
  }

  _monsterInstallPendingCmd(db, term, seq, patchset, outcomes, entryHash) {
    const commandDigest = digestCmd(term, seq, patchset, outcomes)
    try {
      const result = db.prepare(`
        UPDATE monsterft_meta
        SET pending_cmd_seq = ?, pending_local_digest = ?,
            applied_seq = ?, applied_entry_hash = ?
        WHERE id = 1 AND pending_cmd_seq IS NULL
          AND pending_local_digest IS NULL
      `).run(seq, commandDigest, seq, entryHash)
      if (result.changes !== 1n) {
        throw new Err.ErrorWithCode(
          'update count not one',
          Err.SQLITE_ERROR,
        )
      }
      return commandDigest
    } catch (err) {
      throw Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 install pending CMD ')
    }
  }

  _monsterWriteCmdDecision(db, seq, entryHash, repairState, cmdSeq) {
    try {
      const result = db.prepare(`
        UPDATE monsterft_meta
        SET pending_cmd_seq = NULL,
            pending_local_digest = NULL,
            applied_seq = ?,
            applied_entry_hash = ?,
            repair_state = ?
        WHERE id = 1 AND pending_cmd_seq = ?
      `).run(seq, entryHash, repairState, cmdSeq)
      if (result.changes !== 1n) {
        throw new Err.ErrorWithCode(
          'update count not one',
          Err.SQLITE_ERROR,
        )
      }
    } catch (err) {
      throw Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 write CMD decision ')
    }
  }

  _monsterWriteRepair(db, repairState) {
    try {
      const result = db.prepare(`
        UPDATE monsterft_meta
        SET repair_state = ?
        WHERE id = 1
      `).run(repairState)
      if (result.changes !== 1n) {
        throw new Err.ErrorWithCode(
          'update count not one',
          Err.SQLITE_ERROR,
        )
      }
    } catch (err) {
      throw Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 write repair ')
    }
  }

  _monsterReleaseDb(work, value, failed) {
    this._monsterDbWork = null
    this._monsterDbActive = false
    failed ? work.reject(value) : work.resolve(value)
    if (!this._closing) { this._monsterDrainDb() }
  }

  _monsterDrainDb() {
    if (this._monsterDbActive) { return }
    const work = this._monsterDbQueue.shift()
    if (!work) { return }

    this._monsterDbActive = true
    this._monsterDbWork = work
    let result = null
    try {
      this._throwIfClosing()
      result = work.fn(this.db)
    } catch (err) {
      this._monsterReleaseDb(work, err, true)
      return
    }
    Promise.resolve(result).then(
      (value) => this._monsterReleaseDb(work, value, false),
      (err) => this._monsterReleaseDb(work, err, true),
    )
  }

  _monsterRunDb(fn) {
    return new Promise((resolve, reject) => {
      if (this._closing) {
        reject(this._shutdownError ??
          new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN))
        return
      }
      this._monsterDbQueue.push({
        fn,
        resolve,
        reject,
      })
      this._monsterDrainDb()
    })
  }

  _monsterRunDbGetCmd(cmdSeq=null) {
    return this._monsterRunDb(() => {
      const command = this._monsterPendingCommand
      if (command === null ||
          (cmdSeq !== null && command.cmdSeq !== cmdSeq)) {
        return null
      }
      return command
    })
  }

  _monsterInit(db) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS monsterft_meta (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        applied_seq INTEGER NOT NULL,
        applied_entry_hash BLOB,
        repair_state INTEGER NOT NULL CHECK (repair_state IN (0, 1, 2)),
        pending_cmd_seq INTEGER,
        pending_local_digest BLOB,
        CHECK (
          (applied_seq = -1 AND applied_entry_hash IS NULL) OR
          (applied_seq >= 0 AND applied_entry_hash IS NOT NULL AND
           length(applied_entry_hash) = 32)
        ),
        CHECK (
          (pending_cmd_seq IS NULL AND pending_local_digest IS NULL) OR
          (pending_cmd_seq IS NOT NULL AND pending_cmd_seq >= 0 AND
           pending_cmd_seq <= applied_seq AND
           pending_local_digest IS NOT NULL AND
           length(pending_local_digest) = 32)
        )
      ) STRICT;

      INSERT OR IGNORE INTO monsterft_meta
        (id, applied_seq, applied_entry_hash, repair_state,
         pending_cmd_seq, pending_local_digest)
      VALUES (1, -1, NULL, 0, NULL, NULL);
    `)

    const meta = db.prepare(`
      SELECT applied_seq, applied_entry_hash, repair_state,
             pending_cmd_seq, pending_local_digest
      FROM monsterft_meta
      WHERE id = 1
    `).get()
    this._applySeq = meta.applied_seq
    this._monsterRepairState = Number(meta.repair_state)
    this._monsterPendingCommand = meta.pending_cmd_seq === null
      ? null
      : {
          cmdSeq: meta.pending_cmd_seq,
          localDigest: asBuffer(meta.pending_local_digest),
        }
    this._monsterPendingReports.clear()
    this._monsterVerifyAppliedEntry(asBuffer(meta.applied_entry_hash))
  }

  // Require that the log (DB1) contains the last entry that monster (DB2) applied
  _monsterVerifyAppliedEntry(appliedEntryHash) {
    if (!util.isSeq(this._applySeq) || this._applySeq > this.log.seq) {
      throw new Err.ErrorWithCode(
        'applied sequence is illegal',
        Err.LOG_CORRUPT,
      )
    }
    if (this._applySeq === -1n) {
      if (appliedEntryHash !== null) {
        throw new Err.ErrorWithCode(
          'applied entry hash must be null for initial state',
          Err.LOG_CORRUPT,
        )
      }
      return
    }
    const found = this.log.iter(this._applySeq).next()
    if (found.done) {
      throw new Err.ErrorWithCode(
        'applied log entry not found',
        Err.LOG_CORRUPT,
      )
    }
    const expected = crypto.createHash('sha256').update(found.value).digest()
    if (!asBuffer(appliedEntryHash).equals(expected)) {
      throw new Err.ErrorWithCode(
        'applied entry hash does not match',
        Err.LOG_CORRUPT,
      )
    }
  }

  _monsterRememberReport(from, digest) {
    if (!this._monsterPendingReports.has(from)) {
      this._monsterPendingReports.set(from, asBuffer(digest))
    }
  }

  _monsterDecision() {
    const reports = this._monsterPendingReports
    const groups = new Map()
    let reported = 0
    let largest = 0
    let agreement = null
    for (const [id, digest] of reports) {
      reported++
      const key = digest.toString('hex')
      let group = groups.get(key)
      if (!group) {
        group = { digest, nodes: [] }
        groups.set(key, group)
      }
      group.nodes.push(id)
      largest = Math.max(largest, group.nodes.length)
      if (group.nodes.length >= this.quorum) { agreement = group }
    }
    const unknown = this.nodes.length - reported
    return {
      agreement,
      impossible: !agreement && largest + unknown < this.quorum,
      reports,
    }
  }

  async _monsterRxOutcomeRequest(from, msg) {
    try {
      utilm.validateOutcomeRequestMsg(msg)
    } catch (err) {
      this._emitSafe('warn', err)
      return
    }
    const command = await this._monsterRunDbGetCmd(msg.cmdSeq)
    if (command === null || this._closing) { return }
    this.send(from, {
      type: OUTCOME,
      cmdSeq: command.cmdSeq,
      digest: command.localDigest,
    })
  }

  async _monsterRxOutcome(from, msg) {
    try {
      utilm.validateOutcomeMsg(msg)
    } catch (err) {
      this._emitSafe('warn', err)
      return
    }
    if (this.state !== LEADER) { return }
    const command = await this._monsterRunDbGetCmd(msg.cmdSeq)
    if (this.state !== LEADER || command === null) { return }
    this._monsterRememberReport(from, msg.digest)
  }

  _monsterAnnounceOutcome(cmdSeq, digest) {
    if (this.state === LEADER) {
      this._monsterRememberReport(this.id, digest)
    } else if (this.leader !== null) {
      this.send(this.leader, { type: OUTCOME, cmdSeq, digest })
    }
  }

  async _monsterApplyNoop(term, seq, entryHash) {
    await this._monsterRunDb(async (db) => {
      this._throwIfClosing()
      if (seq === 0n) {
        await this._monsterWriteTx(db, async (db) => {
          // user callback gets called with seq 0 (always a no-op) to allow SQL schema setup
          await this._monsterUserApply(db, null, term, seq, 0, null)
          this._throwIfClosing()
          this._monsterAdvanceApplied(db, seq, entryHash)
        })
        this._throwIfClosing()
      } else {
        this._monsterAdvanceApplied(db, seq, entryHash)
      }
      this._applySeq = seq
    })
  }

  async _monsterApplyCmd(entry, term, seq, entryHash) {
    utilm.validateCmdEntry(this, entry, seq)

    let outcomes = null
    let commandDigest = null
    await this._monsterRunDb(async (db) => {
      await this._monsterWriteTx(db, async (db) => {
        const session = db.createSession({ db: 'main' })
        let patchset = null
        try {
          outcomes = []

          for (let index = 0; index < entry.items.length; index++) {
            this._throwIfClosing()
            // savepoints are faster than transactions
            db.exec('SAVEPOINT monsterft_item')
            let value = null
            try {
              value = await this._monsterUserApply(
                db, asBuffer(entry.items[index]), term, seq, index,
                entry.matchIndex,
              )
            } catch (err) {
              this._throwIfClosing()
              db.exec('ROLLBACK TO monsterft_item')
              db.exec('RELEASE monsterft_item')
              outcomes.push({
                status: REJECTED,
                error: Err.wrapError(err),
              })
              continue
            }

            this._throwIfClosing()
            db.exec('RELEASE monsterft_item')
            outcomes.push({ status: FULFILLED, result: value })
          }
          patchset = asBuffer(session.patchset())
        } finally {
          session.close()
        }
        commandDigest = this._monsterInstallPendingCmd(
          db, term, seq, patchset, outcomes, entryHash,
        )
      })
      this._throwIfClosing()
      this._monsterPendingReports.clear()
      this._monsterPendingCommand = {
        cmdSeq: seq,
        localDigest: commandDigest,
      }
      this._applySeq = seq
      this._emitSafe(CMD, {
        cmdSeq: seq,
        cmdCount: entry.items.length,
      })
      this._throwIfClosing()
    })
    this._throwIfClosing()
    this._monsterAnnounceOutcome(seq, commandDigest)
    return {
      localDigest: commandDigest,
      outcomes,
    }
  }

  async _monsterApplySync(entry, seq, entryHash) {
    const { agree, disagree } = utilm.validateSyncEntry(this, entry)
    const command = await this._monsterRunDbGetCmd(entry.cmdSeq)
    if (!command) {
      throw new Err.ErrorWithCode(
        'SYNC entry is missing CMD',
        Err.LOG_CORRUPT,
      )
    }
    const healthy = this._monsterSyncAgrees(entry, command.localDigest)
    let repairState = REPAIR_NONE
    if (!entry.quorum) {
      repairState = REPAIR_QUORUM_IMPOSSIBLE
    } else if (!healthy) {
      repairState = REPAIR_OUTSIDE_AGREEMENT
    }
    if (repairState === REPAIR_OUTSIDE_AGREEMENT && this.state === LEADER) {
      this._toFollower()
    }

    await this._monsterRunDb(async (db) => {
      await this._monsterWriteTx(db, (db) => {
        this._monsterWriteCmdDecision(
          db, seq, entryHash, repairState, entry.cmdSeq,
        )
      })
      this._throwIfClosing()
      this._monsterPendingCommand = null
      this._applySeq = seq
      this._monsterPendingReports.clear()
      this._monsterRepairState = repairState
      this._emitSafe(SYNC, {
        cmdSeq: entry.cmdSeq,
        syncSeq: seq,
        quorum: entry.quorum,
        agree: [...agree],
        disagree: [...disagree],
      })
      this._throwIfClosing()
    })

    if (repairState === REPAIR_OUTSIDE_AGREEMENT) {
      const err = this._monsterRepairError(repairState)
      this._monsterFatalError(err)
      return
    }
    this._throwIfClosing()
  }

  async _monsterApplyEntry(entry, term, seq, isNoop, entryHash) {
    if (isNoop) {
      await this._monsterApplyNoop(term, seq, entryHash)
      return null
    } else if (entry?.type === CMD) {
      if (this._monsterDraining) {
        await this._monsterDrainWork
        // already closing at this time so null never really goes anywhere
        return null
      }
      if (this._monsterRepairState !== REPAIR_NONE) {
        this._monsterFatalError(this._monsterRepairError())
        // fatal closes so this also goes nowhere
        return null
      }
      return this._monsterApplyCmd(entry, term, seq, entryHash)
    } else if (entry?.type === SYNC) {
      await this._monsterApplySync(entry, seq, entryHash)
      return null
    }
    throw new Err.ErrorWithCode('entry type is illegal', Err.LOG_CORRUPT)
  }

  // RaftNode apply entry point
  async _monsterApply(node, bufs, seqs, terms) {
    const results = []
    for (let idx = 0; idx < bufs.length; idx++) {
      this._throwIfClosing()
      const buf = bufs[idx]
      const seq = seqs[idx]
      const term = terms[idx]
      let entry = null
      if (buf !== null) {
        entry = unpack(buf)
      }
      const entryHash = digestEntry(term, buf)
      results.push(await this._monsterApplyEntry(
        entry,
        term,
        seq,
        buf === null,
        entryHash,
      ))
      this._throwIfClosing()
      this._monsterPrune()
    }
    return results
  }

  async _monsterAppendEntry(entry) {
    const term = this.term
    try {
      if (entry.type === CMD) {
        // matchIndex may be interesting to the user so add it here
        const matchIndex = this.nodes.map((id) => {
          if (id === this.id) { return this.seq }
          return this._replication.get(id)?.matchIndex ?? -1n
        })
        entry = { ...entry, matchIndex }
      }
      return await super.append(pack(entry))
    } catch (err) {
      // Let another leader resolve an append whose outcome is uncertain.
      this._monsterStepDownForTerm(term)
      throw err
    }
  }

  async _monsterAwaitDecision(command, term=null) {
    term = term ?? this.term
    this._monsterAssertLeader(term)
    this._monsterRememberReport(this.id, command.localDigest)

    let decision = null
    const retryms = Math.max(2, this.opts.pingTimeout * 0.15)
    while (true) {
      this._monsterAssertLeader(term)
      decision = this._monsterDecision()
      if (decision.agreement || decision.impossible) { break }
      this.nodes.filter((id) => id !== this.id).forEach((to) => {
        this.send(to, { type: OUTCOME_REQUEST, cmdSeq: command.cmdSeq })
      })
      await this._delay(retryms)
    }

    let sync = null
    if (decision.impossible) {
      sync = {
        type: SYNC,
        cmdSeq: command.cmdSeq,
        quorum: false,
        agree: [],
        disagree: this._monsterSortNodes(decision.reports.keys()),
      }
    } else {
      const { agreement } = decision
      const disagree = [...decision.reports]
        .filter(([, digest]) => !asBuffer(digest).equals(agreement.digest))
        .map(([id]) => id)
      sync = {
        type: SYNC,
        cmdSeq: command.cmdSeq,
        quorum: true,
        digest: agreement.digest,
        agree: this._monsterSortNodes(agreement.nodes),
        disagree: this._monsterSortNodes(disagree),
      }
    }
    const leaderAgrees = this._monsterSyncAgrees(
      sync,
      command.localDigest,
    )
    return { leaderAgrees, sync }
  }

  async _monsterAppendSync(entry, term) {
    this._monsterAssertLeader(term)
    const appended = await this._monsterAppendEntry(entry)
    this._throwIfClosing()
    const [syncSeq] = appended
    return syncSeq
  }

  async _monsterFenceLeader() {
    this._toFollower(null, this.state !== FOLLOWER)
    try {
      await this._monsterRunDb((db) => {
        this._throwIfClosing()
        this._monsterWriteRepair(db, REPAIR_OUTSIDE_AGREEMENT)
        this._monsterRepairState = REPAIR_OUTSIDE_AGREEMENT
      })
    } catch (err) {
      if (!(err instanceof Err.ErrorWithCode)) {
        err = Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 fence ')
      }
      throw this._monsterFatalError(err)
    }

    const err = this._monsterRepairError(REPAIR_OUTSIDE_AGREEMENT)
    throw this._monsterFatalError(err)
  }

  _monsterLeaderQueue(cmdItems) {
    const response = util.oneShot()
    const run = async () => {
      await this._monsterLeaderSync
      this._monsterAssertAvailable()
      this._monsterAssertLeader()
      const term = this.term
      const appended = await this._monsterAppendEntry({ type: CMD, items: cmdItems })
      this._throwIfClosing()
      const [cmdSeq, applied] = appended
      const command = { cmdSeq, localDigest: applied.localDigest }
      try {
        const decision = await this._monsterAwaitDecision(command, term)
        const result = { cmdSeq, outcomes: applied.outcomes }
        if (decision.leaderAgrees) {
          response.resolve(result)
        } else {
          const repairState = decision.sync.quorum
            ? REPAIR_OUTSIDE_AGREEMENT
            : REPAIR_QUORUM_IMPOSSIBLE
          response.reject(this._monsterRepairError(repairState))
        }
        if (decision.sync.quorum && !decision.leaderAgrees) {
          await this._monsterFenceLeader()
        }
        const syncSeq = await this._monsterAppendSync(
          decision.sync,
          term,
        )
        return { cmdSeq, syncSeq }
      } catch (err) {
        if (this._closing) { throw err }
        const pending = await this._monsterRunDbGetCmd(command.cmdSeq)
        if (pending !== null) {
          // let another leader take over CMD to SYNC
          this._monsterStepDownForTerm(term)
        }
        throw err
      }
    }
    const operation = this._monsterProtocol.then(run)
    this._monsterProtocol = operation.then(noop, noop)
    const completion = this._raceShutdown(operation)
    completion.catch((err) => {
      if (response.reject(err)) { return }
      if (!this._closing) { this._emitSafe('warn', err) }
    })
    return this._raceShutdown(response.promise)
  }

  // new leaders always resume CMD which need SYNC
  _monsterOnChange(state) {
    if (state.state !== LEADER) { return }
    const term = state.term
    // only start once per term
    if (this._monsterLeaderSyncTerm === term) { return }
    this._monsterLeaderSyncTerm = term
    const leaderSync = async () => {
      const ready = this._leaderReady
      if (ready && ready.term === term) { await ready.promise }
      this._monsterAssertLeader(term)
      await this._apply(this._commitSeq)
      this._monsterAssertLeader(term)
      let command = null
      try {
        command = await this._monsterRunDbGetCmd()
      } catch (err) {
        throw this._monsterFatalError(err)
      }
      // CMD needs SYNC
      if (command !== null) {
        this._monsterAssertLeader(term)
        const decision = await this._monsterAwaitDecision(command, term)
        if (decision.sync.quorum && !decision.leaderAgrees) {
          await this._monsterFenceLeader()
        } else {
          await this._monsterAppendSync(decision.sync, term)
        }
      }
    }
    this._monsterLeaderSync = leaderSync().catch((err) => {
      if (this._monsterStepDownForTerm(term)) { this._emitSafe('warn', err) }
      throw err
    })
    this._monsterLeaderSync.catch(noop)
  }

  // follower forward CMD to leader
  async _monsterRxFwdCmd(from, msg) {
    if (!util.isCid(msg.cid)) { return }
    const error = (err) => {
      this.send(from, util.errorRpc(this.term, msg.cid, err))
    }
    if (!utilm.isFwdCmdMsg(msg)) {
      error(new Err.ErrorWithCode('forward CMD illegal', Err.RPC_ILLEGAL))
      return
    }
    if (this.state !== LEADER) {
      error(new Err.ErrorWithCode('node not leader', Err.NOT_LEADER))
      return
    }
    if (msg.term !== this.term) {
      error(new Err.ErrorWithCode('node term different', Err.TERM_DIFF))
      return
    }
    try {
      const result = await this._monsterLeaderQueue(msg.items)
      this.send(from, {
        type: ACK,
        term: this.term,
        cid: msg.cid,
        cmdSeq: result.cmdSeq,
        results: outcomesWire(result.outcomes),
      })
    } catch (err) {
      error(err)
    }
  }

  async onReceive(from, msg) {
    if (!util.isRpc(msg)) { return }
    const custom = [FWD_CMD, OUTCOME, OUTCOME_REQUEST]
    if (!custom.includes(msg.type)) { return super.onReceive(from, msg) }
    if (!this.nodes.includes(from) || !this.isOpen) { return }

    switch (msg.type) {
      case FWD_CMD:
        await this._monsterRxFwdCmd(from, msg)
        return
      case OUTCOME:
        this._monsterRxOutcome(from, msg)
          .catch((err) => this._monsterFatalError(err))
        return
      case OUTCOME_REQUEST:
        this._monsterRxOutcomeRequest(from, msg)
          .catch((err) => this._monsterFatalError(err))
        return
    }
  }

  _monsterSendAndAwaitCmdAck(to, msg, validate) {
    const work = this._sendAndAwaitResponse(
      to, msg, this.opts.appendTimeout,
      new Err.ErrorWithCode('forward CMD timeout', Err.APPEND_TIMEOUT),
      validate,
    ).then((response) => {
      if (response.type === ACK) { return response }
      throw Err.wrapError(response)
    })
    return this._raceShutdown(work)
  }

  async _monsterFwdCmdToLeader(items) {
    const leader = this.leader
    const term = this.term
    if (leader === null) {
      throw new Err.ErrorWithCode('forward no leader', Err.NO_LEADER)
    }
    const cid = crypto.randomUUID()
    const msg = { type: FWD_CMD, cid, term, items }
    const validate = (response) => {
      if (!util.isTerm(response.term)) { return false }
      if (response.type === ERR) { return true }
      return utilm.isFwdCmdAck(response, items.length)
    }
    const response = await this._monsterSendAndAwaitCmdAck(
      leader, msg, validate,
    )
    return [response.cmdSeq, response.results]
  }

  async _monsterAppendOrFwdCmd(items) {
    this._monsterAssertAvailable()
    let cmdSeq = null
    let outcomes = null
    if (this.state === LEADER) {
      const result = await this._monsterLeaderQueue(items)
      cmdSeq = result.cmdSeq
      outcomes = outcomesWire(result.outcomes)
    } else {
      [cmdSeq, outcomes] = await this._monsterFwdCmdToLeader(items)
    }
    outcomes = outcomes.map((item) => {
      if (item[0] === 0) {
        return { status: FULFILLED, value: item[1] }
      }
      return {
        status: REJECTED,
        reason: Err.wrapError({
          message: item[1], code: item[2], sqlCode: item[3], stack: item[4],
        }),
      }
    })
    return [cmdSeq, outcomes]
  }

  async append(data) {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    }
    if (!Buffer.isBuffer(data)) {
      throw new Err.ErrorWithCode('data must be buffer', Err.ARGUMENT_ILLEGAL)
    }
    const [cmdSeq, outcomes] = await this._raceShutdown(
      this._monsterAppendOrFwdCmd([data]),
    )
    const outcome = outcomes[0]
    if (outcome.status === REJECTED) { throw outcome.reason }
    return [cmdSeq, outcome.value]
  }

  async appendBatch(data) {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    }
    if (!Array.isArray(data) || data.length <= 0) {
      throw new Err.ErrorWithCode(
        'data must be array with length > 0', Err.ARGUMENT_ILLEGAL)
    }
    if (!data.every((buf) => Buffer.isBuffer(buf))) {
      throw new Err.ErrorWithCode(
        'data must be array of buffers', Err.ARGUMENT_ILLEGAL)
    }
    return this._raceShutdown(this._monsterAppendOrFwdCmd(data))
  }

  // drain is an important part of the supported repair procedure
  // it would be wrong to copy a db to a node that needs repair if a CMD exists without a SYNC
  // it would be wrong because CMD outcome from 1 node would be counted as coming from 2 nodes
  drainCmd() {
    if (this._monsterDrainWork) { return this._monsterDrainWork }
    this._monsterDraining = true
    let work = null
    if (!this.isOpen) {
      work = Promise.reject(
        new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN),
      )
    } else {
      const retryms = Math.max(2, this.opts.pingTimeout * 0.1)
      work = this._monsterRunDbGetCmd().then(async (pending) => {
        this._throwIfClosing()
        if (pending !== null) {
          const { cmdSeq } = pending
          while (await this._monsterRunDbGetCmd(cmdSeq) !== null) {
            this._throwIfClosing()
            await this._delay(retryms)
          }
          this._throwIfClosing()
        }
        this.close()
      })
    }
    this._monsterDrainWork = this._raceShutdown(work)
    this._monsterDrainWork.catch(noop)
    return this._monsterDrainWork
  }

  open() {
    if (this.isOpen) { return }

    try {
      super.open()

      try {
        const db = new DatabaseSync(
          this._monsterDatabasePath,
          { readBigInts: true },
        )
        this.db = db
        db.exec('PRAGMA journal_mode = WAL')
        db.exec('PRAGMA synchronous = FULL')
        this._monsterInit(db)
      } catch (err) {
        if (!(err instanceof Err.ErrorWithCode)) {
          err = Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 open ')
        }
        throw err
      }
      if (this._monsterRepairState === REPAIR_OUTSIDE_AGREEMENT) {
        throw this._monsterRepairError()
      }
      this._monsterPrune()
      this._startRaft()
    } catch (err) {
      if (!this._closing) { this._closeAfterOpenFailure() }
      throw err
    }
  }

  _monsterStop() {
    const err = this._shutdownError ??
      new Err.ErrorWithCode('node not open', Err.NODE_NOT_OPEN)
    const queued = this._monsterDbQueue.splice(0)
    queued.forEach((work) => work.reject(err))
    if (this._monsterDbWork !== null) {
      this._monsterDbWork.reject(err)
    }
  }

  close() {
    this._stopRaft()
    this._monsterStop()

    let failure = null
    if (this.db !== null) {
      const db = this.db
      try {
        db.close()
        this.db = null
      } catch (err) {
        failure = Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 close ')
      }
    }

    try {
      super.close()
    } catch (err) {
      if (failure === null) { failure = err }
    }

    if (failure !== null) { throw failure }
  }

  del() {
    if (this.log.isOpen) {
      throw new Err.ErrorWithCode('DB1 is open', Err.LOG_OPEN)
    }
    if (this.db !== null) {
      throw new Err.ErrorWithCode('DB2 is open', Err.LOG_OPEN)
    }
    super.del()
    util.deleteSQLiteFiles(this._monsterDatabasePath)
  }
}

export { MonsterNode }
