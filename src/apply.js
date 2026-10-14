import { EventEmitter } from 'node:events'
import * as Err from './error.js'
import { isCid, HASH_BYTES } from './util.js'
import {
  asBuffer, assertEncodable, digestCmd, outcomesWire, validateApplyRequest,
} from './utilm.js'
import { openApplyDatabase } from './storage.js'
import { readApply, checkGeneration } from './utils.js'
import { decodeFrame, encodeFrame, errorToWire } from './wire.js'

const noop = () => {}

class MonsterApply extends EventEmitter {
  constructor(id, localSend, databasePath, apply) {
    super()
    if (typeof id !== 'string' || id.length === 0) {
      throw new Err.ErrorWithCode('id must be non-empty string', Err.ARGUMENT_ILLEGAL)
    }
    if (typeof localSend !== 'function') {
      throw new Err.ErrorWithCode('localSend must be a function', Err.ARGUMENT_ILLEGAL)
    }
    if (typeof apply !== 'function') {
      throw new Err.ErrorWithCode('apply must be a function', Err.ARGUMENT_ILLEGAL)
    }
    if (typeof databasePath !== 'string' || databasePath.length === 0 ||
        databasePath === ':memory:') {
      throw new Err.ErrorWithCode('Apply requires a database file', Err.ARGUMENT_ILLEGAL)
    }
    this.id = id
    this.tickCount = 0n
    this.db = null
    this._localSend = localSend
    this._databasePath = databasePath
    this._apply = apply
    this._seed = null
    this._pastHello = false
    this._ready = false
    this._stopped = false
    this._closeWork = null
    this._work = null
    this._fatalError = null
  }

  get isOpen() { return this.db !== null && !this._stopped }

  _throwIfStopped() {
    if (this._stopped) {
      throw new Err.ErrorWithCode('Apply is stopped', Err.NODE_NOT_OPEN)
    }
  }

  _emitSafe(type, value) {
    for (const listener of this.rawListeners(type)) {
      try {
        listener.call(this, value)
      } catch (err) {
        if (type !== 'error') {
          if (this._stopped) { this._emitSafe('error', Err.wrapError(err)) }
          else { this._fatal(err) }
        }
      }
    }
  }

  close() {
    if (this._closeWork !== null) { return this._closeWork }
    this._stopped = true
    this._ready = false
    const active = this._work
    this._closeWork = Promise.resolve(active).catch(noop).then(() => {
      if (this.db !== null) {
        try {
          this.db.close()
          this.db = null
        } catch (err) {
          err = Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 close ')
          this._emitSafe('error', err)
          throw err
        }
      }
      this._emitSafe('close')
    })
    this._closeWork.catch(noop)
    return this._closeWork
  }

  _fatal(err) {
    if (this._stopped) { return }
    err = Err.wrapError(err)
    this._fatalError = err
    const closing = this.close()
    this._emitSafe('error', err)
    closing.catch(noop)
  }

  _send(msg) {
    try {
      const work = this._localSend('gate', encodeFrame(msg))
      Promise.resolve(work).catch((err) => this._fatal(Err.wrapError(
        err, Err.SEND_ERROR, 'Apply send ',
      )))
    } catch (err) {
      this._fatal(Err.wrapError(err, Err.SEND_ERROR, 'Apply send '))
    }
  }

  _ack(msg, extra={}) {
    const reply = { type: 'ack', cid: msg.cid }
    if (msg.tickCount !== undefined) { reply.tickCount = msg.tickCount }
    this._send({ ...reply, ...extra })
  }

  _writeReceipt(receipt) {
    try {
      const result = this.db.prepare(`
        UPDATE monsterft_meta_apply SET seq = ?, entry_hash = ?, local_digest = ?
        WHERE id = 1 AND seq < ?
      `).run(receipt.seq, receipt.entry_hash, receipt.local_digest, receipt.seq)
      if (result.changes !== 1n) {
        throw new Err.ErrorWithCode('update count not one', Err.SQLITE_ERROR)
      }
    } catch (err) {
      throw Err.wrapError(err, Err.SQLITE_ERROR, 'DB2 write apply ')
    }
  }

  async _execute(msg) {
    this._throwIfStopped()
    const db = this.db
    const receipt = {
      seq: msg.seq,
      entry_hash: msg.entryHash,
      local_digest: null,
    }
    const outcomes = []
    if (msg.seq === 0n) {
      await this._apply(db, null, msg.term, 0n, 0, null)
      this._throwIfStopped()
      if (!db.isTransaction) {
        throw new Err.ErrorWithCode('DB2 seq0 transaction ended', Err.SQLITE_ERROR)
      }
      receipt.local_digest = Buffer.alloc(HASH_BYTES)
    } else {
      const session = db.createSession({ db: 'main' })
      let patchset = null
      try {
        for (let index = 0; index < msg.items.length; index++) {
          this._throwIfStopped()
          db.exec('SAVEPOINT monsterft_item')
          let value = null
          try {
            value = await this._apply(
              db, msg.items[index], msg.term, msg.seq, index, msg.matchIndex,
            )
            assertEncodable(value)
          } catch (err) {
            this._throwIfStopped()
            err = Err.wrapError(err)
            const warning = Err.wrapError({
              message: err.message, sqlCode: err.sqlCode, stack: err.stack,
            }, Err.APPLY_ERROR)
            this._emitSafe('warn', warning)
            this._throwIfStopped()
            if (!db.isTransaction) { throw err }
            try {
              db.exec('ROLLBACK TO monsterft_item')
              db.exec('RELEASE monsterft_item')
            } catch (rollbackErr) {
              throw Err.wrapError(rollbackErr, Err.SQLITE_ERROR, 'DB2 item rollback ')
            }
            outcomes.push({ status: 'rejected', error: err })
            continue
          }
          this._throwIfStopped()
          db.exec('RELEASE monsterft_item')
          outcomes.push({ status: 'fulfilled', result: value })
        }
        patchset = asBuffer(session.patchset())
      } finally {
        session.close()
      }
      receipt.local_digest = digestCmd(msg.term, msg.seq, patchset, outcomes)
    }
    this._throwIfStopped()
    this._writeReceipt(receipt)
    this._throwIfStopped()
    db.exec('COMMIT')
    return { receipt, outcomes: outcomesWire(outcomes) }
  }

  async onReceive(from, buffer) {
    if (from !== 'gate') {
      if (this._stopped) { return }
      this._emitSafe('warn', new Err.ErrorWithCode(
        `Apply sender ${from} is illegal, expected gate`, Err.RPC_ILLEGAL,
      ))
      return
    }
    try {
      let msg = decodeFrame(buffer)
      if (msg?.to === 'apply') { msg = msg.message }
      if (msg?.type === 'close' && isCid(msg.cid)) {
        try {
          await this.close()
          this._ack(msg)
        } catch (err) {
          this._send({ type: 'err', cid: msg.cid, ...errorToWire(err) })
        }
        return
      }
      if (this._stopped) { return }
      validateApplyRequest(msg)
      if (msg.type === 'hello') {
        if (this._pastHello) {
          throw new Err.ErrorWithCode('Apply received hello after startup', Err.MONSTER_ILLEGAL)
        }
        this._seed = msg.seed
        this._ack(msg)
        return
      }
      if (this._seed === null) {
        throw new Err.ErrorWithCode('Apply has not received hello', Err.MONSTER_ILLEGAL)
      }
      this._pastHello = true
      if (msg.type === 'open') {
        if (this.db !== null) {
          throw new Err.ErrorWithCode('Apply is already open', Err.MONSTER_ILLEGAL)
        }
        this.db = openApplyDatabase(this._databasePath, this._seed)
        this._ack(msg)
        return
      }
      if (msg.type === 'ready') {
        if (this.db === null || this._ready) {
          throw new Err.ErrorWithCode('Apply startup is illegal', Err.MONSTER_ILLEGAL)
        }
        checkGeneration(this.db, 'monsterft_meta_apply', this._seed)
        this._ready = true
        this._ack(msg)
        return
      }
      if (!this._ready) {
        throw new Err.ErrorWithCode('Apply is not ready', Err.MONSTER_ILLEGAL)
      }
      if (this._work !== null) {
        throw new Err.ErrorWithCode('Apply is already executing', Err.MONSTER_ILLEGAL)
      }
      if (msg.tickCount <= this.tickCount) {
        throw new Err.ErrorWithCode('Apply execution tickCount is not advancing', Err.MONSTER_ILLEGAL)
      }
      this.tickCount = msg.tickCount
      // Hold the generation check, receipt read, and callback in one write
      // transaction so a replacement cannot claim DB2 during execution.
      this.db.exec('BEGIN IMMEDIATE')
      checkGeneration(this.db, 'monsterft_meta_apply', this._seed)
      const previous = readApply(this.db)
      if (msg.seq <= previous.seq || (previous.seq === -1n && msg.seq !== 0n)) {
        throw new Err.ErrorWithCode('Apply sequence is not advancing', Err.MONSTER_ILLEGAL)
      }
      const work = Promise.resolve().then(() => this._execute(msg))
        .catch((err) => { throw Err.wrapError(err, Err.APPLY_ERROR, '(apply) ') })
      this._work = work
      try {
        const result = await work
        this._throwIfStopped()
        this._work = null
        this._ack(msg, result)
      } finally {
        if (this._work === work) { this._work = null }
      }
    } catch (err) {
      if (this._stopped) { return }
      this._fatal(err)
    }
  }
}

export { MonsterApply, MonsterApply as Apply }
