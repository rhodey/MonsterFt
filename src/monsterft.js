import fs from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import * as Err from './error.js'
import { MonsterNode } from './monster.js'

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

    const monsterDatabasePath = `${databasePath}2`
    requireNormalFile(databasePath, 'DB1')
    requireNormalFile(monsterDatabasePath, 'DB2')

    let db = null
    let transaction = false
    let result = null
    let failure = null

    try {
      db = new DatabaseSync(monsterDatabasePath, { readBigInts: true })
      db.exec('PRAGMA busy_timeout = 0')
      db.exec('BEGIN EXCLUSIVE')
      transaction = true

      const meta = db.prepare(`
        SELECT applied_seq AS appliedSeq,
               pending_cmd_seq AS pendingCmdSeq,
               pending_local_digest AS pendingLocalDigest
        FROM monsterft_meta
        WHERE id = 1
      `).get()
      if (meta === undefined) {
        throw new Err.ErrorWithCode(
          'MonsterFt certify metadata row is missing',
          Err.LOG_CORRUPT,
        )
      }

      if (!allowUnresolved &&
          (meta.pendingCmdSeq !== null || meta.pendingLocalDigest !== null)) {
        throw new Err.ErrorWithCode(
          'MonsterFt certify requires every materialized CMD output to have an applied SYNC',
          Err.ARGUMENT_ILLEGAL,
        )
      }

      db.prepare(`
        UPDATE monsterft_meta
        SET repair_state = 0
        WHERE id = 1
      `).run()

      db.exec('COMMIT')
      transaction = false
      result = meta.appliedSeq
    } catch (error) {
      failure = Err.wrapError(
        error,
        error instanceof Err.ErrorWithCode ? null : Err.SQLITE_ERROR,
      )
      if (transaction) {
        try {
          db.exec('ROLLBACK')
          transaction = false
        } catch { }
      }
    }

    if (db !== null) {
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
