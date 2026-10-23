import fs from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import { pack } from 'msgpackr'
import { SQLiteLog } from './log.js'
import * as Err from './error.js'
import * as util from './util.js'
import * as sql from './utils.js'

const PROCS = ['procA', 'procB', 'procC']
const TABLES = ['raft_log', 'raft_election', 'monsterft_meta']
const asBuffer = (value) => value === null ? null :
  Buffer.from(value.buffer, value.byteOffset, value.byteLength)

const corrupt = (message) => {
  throw new Err.ErrorWithCode(message, Err.MONSTER_CORRUPT)
}

function databasePaths(base) {
  if (typeof base !== 'string' || base.length === 0 || base === ':memory:') {
    throw new Err.ErrorWithCode(
      'databasePath must be a non-empty file path', Err.ARGUMENT_ILLEGAL)
  }
  return {
    procA: `${base}1a`, procB: `${base}1b`, procC: `${base}1c`, apply: `${base}2`,
  }
}

function fileExists(path) {
  try {
    if (!fs.statSync(path).isFile()) { corrupt(`database is not a file: ${path}`) }
    return true
  } catch (err) {
    if (err.code === 'ENOENT') { return false }
    throw err
  }
}

function readDatabase(path, read) {
  if (!fileExists(path)) { corrupt(`database is missing: ${path}`) }
  const db = new DatabaseSync(path, { readOnly: true, readBigInts: true })
  let result
  try {
    db.exec('BEGIN')
    result = read(db)
    db.exec('COMMIT')
  } catch (err) {
    try { db.close() } catch { }
    throw err
  }
  db.close()
  return result
}

function schema(db) {
  return db.prepare('SELECT name, type FROM sqlite_schema').all()
}

function readState(db) {
  const tables = schema(db)
  for (const name of TABLES) {
    if (!tables.some((row) => row.type === 'table' && row.name === name)) {
      const code = name === 'monsterft_meta' ? Err.MONSTER_CORRUPT : Err.LOG_CORRUPT
      throw new Err.ErrorWithCode(`${name} table is missing`, code)
    }
  }
  const election = db.prepare(`
    SELECT id, current_term AS term, voted_for AS votedFor FROM raft_election
  `).all()
  if (election.length !== 1 || election[0].id !== 1n) {
    throw new Err.ErrorWithCode('election row is missing or not singleton', Err.LOG_CORRUPT)
  }
  util.validate(util.logSchemas.election, election[0], 'stored election', Err.LOG_CORRUPT)
  const { term, votedFor } = election[0]
  const head = db.prepare('SELECT seq, term, entry FROM raft_log ORDER BY seq DESC LIMIT 1').get()
  const first = db.prepare('SELECT seq FROM raft_log ORDER BY seq LIMIT 1').get()
  if (head !== undefined) {
    util.validate(util.logSchemas.row, head, 'stored log head', Err.LOG_CORRUPT)
    util.validate(util.schemas.nonnegativeSeq, first.seq, 'stored log begin', Err.LOG_CORRUPT)
    head.entry = asBuffer(head.entry)
  }
  const meta = sql.readMeta(db)
  return { election: { term, votedFor }, meta, head: head ?? null }
}

function stateDigest(state) {
  const { election, meta, head } = state
  return util.hash(pack([
    election.term, election.votedFor,
    meta.applied_seq, asBuffer(meta.applied_entry_hash), meta.repair_state,
    meta.pending_cmd_seq, asBuffer(meta.applied_local_digest),
    head === null ? null : [head.seq, head.term, head.entry],
  ]))
}

function receiptDigest(receipt) {
  return util.hash(pack([
    receipt.seq, asBuffer(receipt.entry_hash), asBuffer(receipt.local_digest),
  ]))
}

function copyProtocolState(db1, donor, selected) {
  if (!db1.isTransaction) {
    throw new Err.ErrorWithCode('DB1 copy requires a transaction', Err.MONSTER_ILLEGAL)
  }
  const state = readState(donor)
  if (!stateDigest(state).equals(selected)) { corrupt('DB1 donor changed after capture') }
  db1.exec('DELETE FROM raft_log; DELETE FROM raft_election')
  const insert = db1.prepare('INSERT INTO raft_log (seq, term, entry) VALUES (?, ?, ?)')
  let next = null
  for (const row of donor.prepare('SELECT seq, term, entry FROM raft_log ORDER BY seq').iterate()) {
    util.validate(util.logSchemas.row, row, 'stored log', Err.LOG_CORRUPT)
    if (next !== null && row.seq !== next) {
      throw new Err.ErrorWithCode('stored log row is corrupt', Err.LOG_CORRUPT)
    }
    insert.run(row.seq, row.term, row.entry.byteLength ? row.entry : Buffer.alloc(0))
    next = row.seq + 1n
  }
  db1.prepare(`
    INSERT INTO raft_election (id, current_term, voted_for) VALUES (1, ?, ?)
  `).run(state.election.term, state.election.votedFor)
  const meta = state.meta
  // Ownership is local to this file, not part of the donor's protocol state.
  const result = db1.prepare(`
    UPDATE monsterft_meta SET applied_seq = ?, applied_entry_hash = ?,
      repair_state = ?, pending_cmd_seq = ?, applied_local_digest = ?
    WHERE id = 1
  `).run(meta.applied_seq, meta.applied_entry_hash, meta.repair_state,
    meta.pending_cmd_seq, meta.applied_local_digest)
  if (result.changes !== 1n) { corrupt('meta row is missing') }
  if (!stateDigest(readState(db1)).equals(selected)) {
    corrupt('adopted DB1 state does not match majority')
  }
}

// A partly created fresh family can be completed. Established files never
// authorize recreating a missing peer or missing metadata beside user state.
function checkFamily(paths) {
  let incomplete = false
  let pristine = true
  for (const role of [...PROCS, 'apply']) {
    const path = paths[role]
    if (!fileExists(path)) {
      incomplete = true
      continue
    }
    readDatabase(path, (db) => {
      const tables = schema(db)
      if (tables.length === 0) {
        incomplete = true
        return
      }
      if (role === 'apply') {
        const receipt = sql.readApply(db)
        pristine = pristine && receipt.seq === -1n && tables.every((row) =>
          row.type === 'table' && row.name === 'monsterft_meta_apply')
      } else {
        const state = readState(db)
        pristine = pristine && state.head === null &&
          state.election.term === 0n && state.election.votedFor === null &&
          state.meta.applied_seq === -1n && tables.every((row) =>
            row.type === 'table' && TABLES.includes(row.name))
      }
    })
  }
  if (incomplete && !pristine) {
    corrupt('missing database or schema beside established state')
  }
}

function openApplyDatabase(base, generation) {
  if (!util.isSeed(generation)) {
    throw new Err.ErrorWithCode('generation must be a nonzero 16-byte Buffer', Err.ARGUMENT_ILLEGAL)
  }
  const paths = databasePaths(base)
  checkFamily(paths)
  util.createSQLiteDirectory(paths.apply)
  const db = new DatabaseSync(paths.apply, { readBigInts: true })
  try {
    db.exec('PRAGMA journal_mode = WAL')
    db.exec('PRAGMA synchronous = FULL')
    db.exec('BEGIN IMMEDIATE')
    if (schema(db).length === 0) { sql.createApply(db, generation) }
    sql.readApply(db)
    sql.claimGeneration(db, 'monsterft_meta_apply', generation)
    db.exec('COMMIT')
    return db
  } catch (err) {
    try { db.close() } catch { }
    throw err
  }
}

class ProtocolStore {
  constructor(role, base, opts={}) {
    if (!PROCS.includes(role)) {
      throw new Err.ErrorWithCode('Proc role is illegal', Err.ARGUMENT_ILLEGAL)
    }
    this.role = role
    this.paths = databasePaths(base)
    this.log = new SQLiteLog(this.paths[role], opts)
    this._captured = null
    this._adopted = false
    this._generation = null
    this._generationGet = null
  }

  get db() { return this.log.db }

  open(generation) {
    if (!util.isSeed(generation)) {
      throw new Err.ErrorWithCode('generation must be a nonzero 16-byte Buffer', Err.ARGUMENT_ILLEGAL)
    }
    if (this.log.isOpen) { this.checkGeneration(); return }
    checkFamily(this.paths)
    // Proc owns teardown if startup fails, including rollback of open transactions.
    this.log.open({ initialize: (db1) => sql.createMeta(db1, generation) })
    this.db.exec('BEGIN IMMEDIATE')
    sql.readMeta(this.db)
    sql.claimGeneration(this.db, 'monsterft_meta', generation)
    this.log.readHead()
    this._generationGet = this.db.prepare('SELECT generation FROM monsterft_meta WHERE id = 1')
    this.db.exec('COMMIT')
    this._generation = generation
  }

  checkGeneration() {
    sql.checkGeneration(this.db, 'monsterft_meta', this._generation, this._generationGet)
  }

  capture() {
    this.log._assertOpen()
    const states = PROCS.map((role) => readDatabase(this.paths[role], (db1) => {
      sql.checkGeneration(db1, 'monsterft_meta', this._generation)
      return readState(db1)
    }))
    const digests = states.map(stateDigest)
    const winner = digests.findIndex((digest) =>
      digests.filter((other) => digest.equals(other)).length >= 2)
    if (winner < 0) { corrupt('DB1 states have no majority') }
    const receipt = readDatabase(this.paths.apply, (db2) => {
      sql.checkGeneration(db2, 'monsterft_meta_apply', this._generation)
      return sql.readApply(db2)
    })
    this._captured = { states, digests, winner, receipt }
    this._adopted = false
    return { state: digests[winner], receipt: receiptDigest(receipt), donor: PROCS[winner] }
  }

  adopt() {
    const captured = this._captured
    if (captured === null) {
      throw new Err.ErrorWithCode('DB1 state has not been captured', Err.MONSTER_ILLEGAL)
    }
    const selected = captured.digests[captured.winner]
    const own = PROCS.indexOf(this.role)
    this.db.exec('BEGIN IMMEDIATE')
    this.checkGeneration()
    if (!stateDigest(readState(this.db)).equals(captured.digests[own])) {
      corrupt('DB1 changed after capture')
    }
    if (!captured.digests[own].equals(selected)) {
      readDatabase(this.paths[PROCS[captured.winner]], (donor) => {
        sql.checkGeneration(donor, 'monsterft_meta', this._generation)
        copyProtocolState(this.db, donor, selected)
      })
    }
    this.log.readHead()
    this.db.exec('COMMIT')
    this._adopted = true
  }

  reconcile() {
    if (!this._adopted) {
      throw new Err.ErrorWithCode('DB1 state has not been adopted', Err.MONSTER_ILLEGAL)
    }
    this.db.exec('BEGIN IMMEDIATE')
    this.checkGeneration()
    const meta = sql.reconcileReceipt(this.db, this._captured.receipt)
    this.db.exec('COMMIT')
    return meta
  }

  load() { return sql.readMeta(this.db) }

  close() {
    this.log.close()
    this._generationGet = null
    this._captured = null
    this._adopted = false
  }
}

export {
  ProtocolStore, databasePaths, openApplyDatabase,
  readDatabase, readState, stateDigest, receiptDigest, copyProtocolState,
}
