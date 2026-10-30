import fs from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import * as Err from './error.js'
import * as sql from './utils.js'
import { databasePaths, readState, stateDigest, copyDb1Into } from './storage.js'

function requireFile(path, name) {
  let stat = null
  try { stat = fs.statSync(path) } catch { }
  if (stat === null || !stat.isFile()) {
    throw new Err.ErrorWithCode(`certify ${name} must be a normal file`, Err.FS_ERROR)
  }
}

function clearFence(db1, receipt, allowUnresolved) {
  const meta = sql.reconcileReceipt(db1, receipt)
  if (!allowUnresolved && meta.pending_cmd_seq !== null) {
    throw new Err.ErrorWithCode(
      'certify requires every CMD output to have an applied SYNC',
      Err.ARGUMENT_ILLEGAL,
    )
  }
  const result = db1.prepare(`UPDATE monsterft_meta SET repair_state = ${Err.REPAIR_NONE} WHERE id = 1`).run()
  if (result.changes !== 1n) {
    throw new Err.ErrorWithCode('certify update count not one', Err.SQLITE_ERROR)
  }
  return meta.applied_seq
}

function certify(databasePath, allowUnresolved=false) {
  const paths = databasePaths(databasePath)
  if (typeof allowUnresolved !== 'boolean') {
    throw new Err.ErrorWithCode(
      'certify allowUnresolved must be a boolean', Err.ARGUMENT_ILLEGAL)
  }
  for (const [name, path] of Object.entries(paths)) { requireFile(path, name) }
  const dbs = []
  let failure = null
  let result = null
  try {
    // Certification runs with the node stopped and all four databases locked.
    // Lock DB2 first: completed application work is saved there before its
    // progress is recorded in DB1. Certification leaves DB2 unchanged.
    for (const path of [paths.apply, paths.procA, paths.procB, paths.procC]) {
      const db = new DatabaseSync(path, { readBigInts: true })
      dbs.push(db)
      db.exec('PRAGMA synchronous = FULL')
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
    const selectedDigest = digests[winner]
    const donor = procs[winner]

    // Test certification on every copy in the majority before saving changes.
    // Agreement alone does not prove that each copy is valid. The savepoint
    // lets us run the real operation and undo its changes, preserving the
    // original majority for the next step.
    for (let i = 0; i < procs.length; i++) {
      if (!digests[i].equals(selectedDigest)) { continue }
      const db1 = procs[i]
      db1.exec('SAVEPOINT monsterft_certify')
      result = clearFence(db1, receipt, allowUnresolved)
      db1.exec('ROLLBACK TO monsterft_certify; RELEASE monsterft_certify')
    }

    // First bring any differing copy into agreement with the majority and
    // commit that repair. Certifying a majority copy first could leave three
    // different states after an interruption. Keep the databases locked
    // throughout these separate commits.
    for (let i = 0; i < procs.length; i++) {
      if (digests[i].equals(selectedDigest)) { continue }
      copyDb1Into(procs[i], donor, selectedDigest)
      procs[i].exec('COMMIT')
      procs[i].exec('BEGIN EXCLUSIVE')
    }

    // Now save the certification changes to each copy. Between commits, a
    // majority remains in either the original state or the certified state.
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
  // Closing discards uncommitted changes; completed commits remain. Their
  // ordering preserves the majority even if certification ends partway through.
  for (const db of [...dbs].reverse()) {
    try { db.close() } catch (err) {
      failure = failure ?? Err.wrapError(err, Err.SQLITE_ERROR)
    }
  }
  if (failure !== null) { throw failure }
  return result
}

export { certify }
