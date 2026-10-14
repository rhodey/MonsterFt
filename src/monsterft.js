import fs from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import * as Err from './error.js'
import { MonsterNode } from './monster.js'
import * as utils from './utils.js'

function requireNormalFile(path, name) {
  let stat = null
  try {
    stat = fs.statSync(path)
  } catch { }
  if (stat === null || !stat.isFile()) {
    throw new Err.ErrorWithCode(
      `MonsterFt certify ${name} must be a normal file`,
      Err.FS_ERROR,
    )
  }
}

class MonsterFt extends MonsterNode {
  static certify(databasePath, allowUnresolved=false) {
    if (typeof databasePath !== 'string' || databasePath.length === 0) {
      throw new Err.ErrorWithCode(
        'MonsterFt certify path must be a non-empty string',
        Err.ARGUMENT_ILLEGAL,
      )
    }
    if (typeof allowUnresolved !== 'boolean') {
      throw new Err.ErrorWithCode(
        'MonsterFt certify allowUnresolved must be a boolean',
        Err.ARGUMENT_ILLEGAL,
      )
    }
    if (databasePath === ':memory:') {
      throw new Err.ErrorWithCode(
        'MonsterFt certify path cannot be :memory:',
        Err.ARGUMENT_ILLEGAL,
      )
    }

    const monsterApplyDbPath = `${databasePath}2`
    requireNormalFile(databasePath, 'DB1')
    requireNormalFile(monsterApplyDbPath, 'DB2')

    let db1 = null
    let db2 = null
    let result = null
    let failure = null

    try {
      // Lock DB2 first, as apply commits there before updating DB1.
      db2 = new DatabaseSync(monsterApplyDbPath, { readBigInts: true })
      db1 = new DatabaseSync(databasePath, { readBigInts: true })
      for (const db of [db2, db1]) {
        db.exec('PRAGMA busy_timeout = 0')
        db.exec('PRAGMA locking_mode = EXCLUSIVE')
        db.exec('BEGIN EXCLUSIVE')
      }
      const meta = utils.reconcile(db1, db2)

      if (!allowUnresolved && meta.pending_cmd_seq !== null) {
        throw new Err.ErrorWithCode(
          'MonsterFt certify requires every CMD output to have an applied SYNC',
          Err.ARGUMENT_ILLEGAL,
        )
      }

      const changed = db1.prepare(`
        UPDATE monsterft_meta
        SET repair_state = 0
        WHERE id = 1
      `).run()
      if (changed.changes !== 1n) {
        throw new Err.ErrorWithCode(
          'MonsterFt certify update count not one', Err.SQLITE_ERROR,
        )
      }

      db1.exec('COMMIT')
      db2.exec('COMMIT')
      result = meta.applied_seq
    } catch (error) {
      failure = Err.wrapError(
        error,
        error instanceof Err.ErrorWithCode ? null : Err.SQLITE_ERROR,
      )
      for (const db of [db1, db2]) {
        if (db !== null && db.isTransaction) {
          try { db.exec('ROLLBACK') } catch { }
        }
      }
    }

    for (const db of [db1, db2]) {
      if (db === null) { continue }
      try {
        db.close()
      } catch (closeError) {
        if (failure === null) {
          failure = Err.wrapError(closeError, Err.SQLITE_ERROR)
        }
      }
    }

    if (failure !== null) { throw failure }
    return result
  }
}

export { MonsterFt }
