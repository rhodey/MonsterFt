import { DatabaseSync } from 'node:sqlite'
import * as Err from './error.js'
import * as util from './util.js'

const MAX_SEQ = 9_223_372_036_854_775_807n
// node:sqlite can bind zero-byte ArrayBuffer views as NULL instead of an empty BLOB.
const EMPTY_ENTRY = Buffer.alloc(0)

const defaults = {
  // fetch records in batches of N when iterating
  iterStepSize: 1024,
}

class SQLiteLog {
  constructor(databasePath, opts={}) {
    if (typeof databasePath !== 'string' || databasePath.length <= 0) {
      throw new Err.ErrorWithCode(
        'SQLiteLog DB path must be non-empty string', Err.ARGUMENT_ILLEGAL)
    }

    opts = { ...defaults, ...opts }
    if (!Number.isSafeInteger(opts.iterStepSize) || opts.iterStepSize <= 0) {
      throw new Err.ErrorWithCode(
        'iterStepSize must be int > 0', Err.ARGUMENT_ILLEGAL)
    }

    this.path = databasePath
    this._memory = databasePath === ':memory:'
    this.iterStepSize = opts.iterStepSize
    this.db = null
    this._statements = null
    this.term = null
    this.head = null
    this.seq = null
    this.begin = null
    this.elec = { term: null, votedFor: null }
    this._open = false
  }

  get isOpen() {
    return this._open
  }

  _wrapError(err, operation) {
    return Err.wrapError(err, null, `(log ${operation}) `)
  }

  _assertOpen() {
    if (!this.isOpen) {
      throw new Err.ErrorWithCode('log not open', Err.LOG_NOT_OPEN)
    }
  }

  _validateSeq(seq, empty=false, code=Err.ARGUMENT_ILLEGAL, name='seq') {
    const min = empty ? -1n : 0n
    if (typeof seq !== 'bigint') {
      throw new Err.ErrorWithCode(`${name} must be bigint`, code)
    }
    if (seq < min) {
      throw new Err.ErrorWithCode(`${name} must be >= ${min}`, code)
    }
    if (seq > MAX_SEQ) {
      throw new Err.ErrorWithCode(`${name} must be <= ${MAX_SEQ}`, code)
    }
  }

  _validateEntry(data, code=Err.ARGUMENT_ILLEGAL) {
    if (data === null || typeof data !== 'object' || Array.isArray(data)) {
      throw new Err.ErrorWithCode('data must be record', code)
    }
    this._validateSeq(data.term, false, code, 'term')
    if (!Buffer.isBuffer(data.entry)) {
      throw new Err.ErrorWithCode('entry must be buffer', code)
    }
  }

  _validateElection(term, votedFor, code=Err.ARGUMENT_ILLEGAL) {
    this._validateSeq(term, false, code, 'term')
    if (votedFor !== null &&
        (typeof votedFor !== 'string' || votedFor.length <= 0)) {
      throw new Err.ErrorWithCode(
        'votedFor must be null or a non-empty string', code)
    }
  }

  _entryFromRow(row, expected=undefined) {
    this._validateSeq(row.seq, false, Err.LOG_CORRUPT)
    if (expected !== undefined && row.seq !== expected) {
      throw new Err.ErrorWithCode(`seq ${row.seq} !== ${expected}`, Err.LOG_CORRUPT)
    }
    if (!(row.entry instanceof Uint8Array)) {
      throw new Err.ErrorWithCode('entry must be blob', Err.LOG_CORRUPT)
    }
    const entry = Buffer.from(row.entry.buffer, row.entry.byteOffset, row.entry.byteLength)
    const data = { term: row.term, entry }
    this._validateEntry(data, Err.LOG_CORRUPT)
    return data
  }

  readHead() {
    const row = this._statements.head.get()
    let seq = -1n
    let term = -1n
    let head = null

    if (row !== undefined) {
      const data = this._entryFromRow(row)
      seq = row.seq
      term = data.term
      head = data.entry
    }

    const election = this._statements.electionGet.get()
    if (election === undefined) {
      throw new Err.ErrorWithCode('election row is missing', Err.LOG_CORRUPT)
    }
    this._validateElection(election.term, election.votedFor, Err.LOG_CORRUPT)

    this.seq = seq
    this.term = term
    this.head = head
    this.elec.term = election.term
    this.elec.votedFor = election.votedFor

    const begin = this._statements.begin.get()
    if (begin === undefined) {
      this.begin = -1n
      return
    }
    this._validateSeq(begin.seq, false, Err.LOG_CORRUPT)
    this.begin = begin.seq
  }

  open() {
    if (this._open) { return }
    const { path: databasePath } = this
    let db = null
    try {
      if (!this._memory) {
        util.createSQLiteDirectory(databasePath)
      }
      db = new DatabaseSync(databasePath, { readBigInts: true })

      db.exec('PRAGMA journal_mode = WAL')
      db.exec('PRAGMA synchronous = FULL')

      const schema = db.prepare('SELECT name, type FROM sqlite_schema').all()
      if (schema.length === 0) {
        db.exec(`
          BEGIN IMMEDIATE;
          CREATE TABLE raft_log (
            seq INTEGER PRIMARY KEY,
            term INTEGER NOT NULL,
            entry BLOB NOT NULL
          ) STRICT;

          CREATE TABLE raft_election (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            current_term INTEGER NOT NULL,
            voted_for TEXT CHECK (voted_for IS NULL OR voted_for <> '')
          ) STRICT;

          INSERT INTO raft_election (id, current_term, voted_for)
          VALUES (1, 0, NULL);
          COMMIT;
        `)
      } else {
        for (const name of ['raft_log', 'raft_election']) {
          if (!schema.some((row) => row.type === 'table' && row.name === name)) {
            throw new Err.ErrorWithCode(`${name} table is missing`, Err.LOG_CORRUPT)
          }
        }
      }

      this.db = db
      this._statements = {
        insert: db.prepare('INSERT INTO raft_log (seq, term, entry) VALUES (?, ?, ?)'),
        head: db.prepare('SELECT seq, term, entry FROM raft_log ORDER BY seq DESC LIMIT 1'),
        begin: db.prepare('SELECT seq FROM raft_log ORDER BY seq LIMIT 1'),
        range: db.prepare(`
          SELECT seq, term, entry FROM raft_log
          WHERE seq >= ? AND seq <= ?
          ORDER BY seq
          LIMIT ?
        `),
        trim: db.prepare('DELETE FROM raft_log WHERE seq > ?'),
        electionGet: db.prepare(`
          SELECT current_term AS term, voted_for AS votedFor
          FROM raft_election WHERE id = 1
        `),
        electionUpsert: db.prepare(`
          INSERT INTO raft_election (id, current_term, voted_for) VALUES (1, ?, ?)
          ON CONFLICT (id) DO UPDATE SET
            current_term = excluded.current_term,
            voted_for = excluded.voted_for
        `),
      }
      this.readHead()
      this._open = true
    } catch (err) {
      this.db = null
      this._statements = null
      this.term = null
      this.head = null
      this.seq = null
      this.begin = null
      this.elec.term = null
      this.elec.votedFor = null
      this._open = false
      if (db) {
        try { db.close() } catch (_) {}
      }
      throw this._wrapError(err, 'open')
    }
  }

  close() {
    if (!this._open) { return }

    try {
      this.db.close()
    } catch (err) {
      throw this._wrapError(err, 'close')
    }

    this.db = null
    this._statements = null
    this.term = null
    this.head = null
    this.seq = null
    this.begin = null
    this.elec.term = null
    this.elec.votedFor = null
    this._open = false
  }

  append(data, seq=null) {
    try {
      this._assertOpen()
      const next = this.seq + 1n
      seq = seq !== null ? seq : next
      this._validateSeq(seq)
      this._validateEntry(data)
      if (next !== seq) {
        throw new Err.ErrorWithCode(
          `next ${next} !== ${seq}`, Err.ARGUMENT_ILLEGAL)
      }

      const { term, entry: head } = data
      this._statements.insert.run(seq, term, head.length ? head : EMPTY_ENTRY)
      this.term = term
      this.head = head
      this.seq = seq
      if (this.begin < 0n) { this.begin = seq }
      return seq
    } catch (err) {
      throw this._wrapError(err, 'append')
    }
  }

  appendBatch(data, seq=null) {
    let transaction = false
    try {
      this._assertOpen()
      if (!Array.isArray(data)) {
        throw new Err.ErrorWithCode('data must be array', Err.ARGUMENT_ILLEGAL)
      }
      if (data.length <= 0) {
        throw new Err.ErrorWithCode(
          'data must be array with length > 0', Err.ARGUMENT_ILLEGAL)
      }
      data.forEach((entry) => this._validateEntry(entry))

      const next = this.seq + 1n
      seq = seq !== null ? seq : next
      this._validateSeq(seq)
      if (next !== seq) {
        throw new Err.ErrorWithCode(
          `next ${next} !== ${seq}`, Err.ARGUMENT_ILLEGAL)
      }

      const end = seq + BigInt(data.length - 1)
      this._validateSeq(end)
      const last = data[data.length - 1]
      const { term, entry: head } = last

      this.db.exec('BEGIN IMMEDIATE')
      transaction = true
      data.forEach(({ term, entry }, idx) => {
        this._statements.insert.run(seq + BigInt(idx), term, entry.length ? entry : EMPTY_ENTRY)
      })
      this.db.exec('COMMIT')
      transaction = false

      this.term = term
      this.head = head
      this.seq = end
      if (this.begin < 0n) { this.begin = seq }
      return seq
    } catch (err) {
      if (transaction) {
        try {
          this.db.exec('ROLLBACK')
        } catch {}
        try {
          this.readHead()
        } catch {}
      }
      throw this._wrapError(err, 'appendBatch')
    }
  }

  election(currentTerm, votedFor) {
    try {
      this._assertOpen()
      this._validateElection(currentTerm, votedFor)

      this._statements.electionUpsert.run(currentTerm, votedFor)
      this.elec.term = currentTerm
      this.elec.votedFor = votedFor
      return [currentTerm, votedFor]
    } catch (err) {
      throw this._wrapError(err, 'election')
    }
  }

  trim(seq=-1n) {
    try {
      this._assertOpen()
      this._validateSeq(seq, true)
      if (this.seq <= seq) { return }

      this._statements.trim.run(seq)
      this.readHead()
    } catch (err) {
      throw this._wrapError(err, 'trim')
    }
  }

  iter(seq=0n, opts={}) {
    try {
      this._assertOpen()
      this._validateSeq(seq)

      const iterStepSize = opts.iterStepSize ?? this.iterStepSize
      if (!Number.isSafeInteger(iterStepSize) || iterStepSize <= 0) {
        throw new Err.ErrorWithCode(
          'iterStepSize must be int > 0', Err.ARGUMENT_ILLEGAL)
      }

      const log = this
      const last = this.seq
      return (function*() {
        let next = seq
        try {
          while (next <= last) {
            const rows = log._statements.range.all(next, last, iterStepSize)
            if (rows.length <= 0) { return }

            for (const row of rows) {
              const entry = log._entryFromRow(row, next)
              next++
              yield entry
            }
          }
        } catch (err) {
          throw log._wrapError(err, 'iter')
        }
      })()
    } catch (err) {
      throw this._wrapError(err, 'iter')
    }
  }

  del() {
    try {
      if (this._open) {
        throw new Err.ErrorWithCode('log is open', Err.LOG_OPEN)
      }
      util.deleteSQLiteFiles(this.path)
    } catch (err) {
      throw this._wrapError(err, 'del')
    }
  }
}

export { SQLiteLog }
