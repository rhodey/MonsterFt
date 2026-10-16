import fs from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import * as Err from './error.v2.js'
import * as sql from './utils.v2.js'
import { databasePaths, readState, stateDigest, copyProtocolState } from './storage.v2.js'

function requireFile(path, name) {
  let stat = null
  try { stat = fs.statSync(path) } catch { }
  if (stat === null || !stat.isFile()) {
    throw new Err.ErrorWithCode(`MonsterFt certify ${name} must be a normal file`, Err.FS_ERROR)
  }
}

function clearFence(db1, receipt, allowUnresolved) {
  const meta = sql.reconcileReceipt(db1, receipt)
  if (!allowUnresolved && meta.pending_cmd_seq !== null) {
    throw new Err.ErrorWithCode(
      'MonsterFt certify requires every CMD output to have an applied SYNC',
      Err.ARGUMENT_ILLEGAL,
    )
  }
  const result = db1.prepare('UPDATE monsterft_meta SET repair_state = 0 WHERE id = 1').run()
  if (result.changes !== 1n) {
    throw new Err.ErrorWithCode('MonsterFt certify update count not one', Err.SQLITE_ERROR)
  }
  return meta.applied_seq
}

function certify(databasePath, allowUnresolved=false) {
  const paths = databasePaths(databasePath)
  if (typeof allowUnresolved !== 'boolean') {
    throw new Err.ErrorWithCode(
      'MonsterFt certify allowUnresolved must be a boolean', Err.ARGUMENT_ILLEGAL)
  }
  for (const [name, path] of Object.entries(paths)) { requireFile(path, name) }
  const dbs = []
  let failure = null
  let result = null
  try {
    // This is an offline administrative operation. Hold exclusive locks on all
    // four files; DB2 remains unchanged. Its lock comes first, as Apply commits first.
    for (const path of [paths.apply, paths.procA, paths.procB, paths.procC]) {
      const db = new DatabaseSync(path, { readBigInts: true })
      dbs.push(db)
      db.exec('PRAGMA busy_timeout = 0')
      db.exec('PRAGMA locking_mode = EXCLUSIVE')
      db.exec('BEGIN EXCLUSIVE')
    }
    const [db2, ...procs] = dbs
    const receipt = sql.readApply(db2)
    const states = procs.map(readState)
    const digests = states.map(stateDigest)
    const winner = digests.findIndex((digest) =>
      digests.filter((other) => digest.equals(other)).length >= 2)
    if (winner < 0) {
      throw new Err.ErrorWithCode('DB1 states have no majority', Err.MONSTER_CORRUPT)
    }
    const selected = digests[winner]
    const donor = procs[winner]

    // Validate the selected state and override before changing an outlier.
    // A rejected certification leaves even a pending receipt import untouched.
    donor.exec('SAVEPOINT monsterft_certify')
    result = clearFence(donor, receipt, allowUnresolved)
    donor.exec('ROLLBACK TO monsterft_certify; RELEASE monsterft_certify')

    // Adoption is its own durable phase: X/X/Y -> X/X/X, before any X -> Z
    // receipt import or fence clearance. Exclusive locking survives COMMIT.
    for (let i = 0; i < procs.length; i++) {
      if (digests[i].equals(selected)) { continue }
      copyProtocolState(procs[i], donor, selected)
      procs[i].exec('COMMIT')
      procs[i].exec('BEGIN EXCLUSIVE')
    }

    for (const db1 of procs) {
      const applied = clearFence(db1, receipt, allowUnresolved)
      if (applied !== result) {
        throw new Err.ErrorWithCode('certified DB1 checkpoints differ', Err.MONSTER_CORRUPT)
      }
      db1.exec('COMMIT')
    }
    db2.exec('COMMIT')
  } catch (err) {
    failure = Err.wrapError(err, err instanceof Err.ErrorWithCode ? null : Err.SQLITE_ERROR)
  }
  // Closing also rolls back any unfinished phase. Already committed files hold
  // either the majority state or its completely certified successor.
  for (const db of [...dbs].reverse()) {
    try { db.close() } catch (err) {
      failure = failure ?? Err.wrapError(err, Err.SQLITE_ERROR)
    }
  }
  if (failure !== null) { throw failure }
  return result
}

export { certify }
