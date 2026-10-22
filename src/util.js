import fs from 'node:fs'
import path from 'node:path'
import * as Err from './error.js'

const MAX_INT = 9_223_372_036_854_775_807n

const isCid = (cid) => typeof cid === 'string' && cid.length > 0
const isTerm = (term) => typeof term === 'bigint' && term >= 0n && term <= MAX_INT
const isSeq = (seq) => typeof seq === 'bigint' && seq >= -1n && seq <= MAX_INT
const isSeed = (seed) => Buffer.isBuffer(seed) && seed.length === 16 &&
  seed.some((byte) => byte !== 0)

const isRpc = (msg) => msg !== null && typeof msg === 'object' &&
  !Array.isArray(msg) && !(msg instanceof Uint8Array) &&
  typeof msg.type === 'string'

const createSQLiteDirectory = (databasePath) => {
  try {
    fs.mkdirSync(path.dirname(databasePath), { recursive: true })
  } catch (err) {
    throw Err.wrapError(err, Err.FS_ERROR)
  }
}

const deleteSQLiteFiles = (databasePath) => {
  const files = [
    databasePath,
    `${databasePath}-journal`,
    `${databasePath}-wal`,
    `${databasePath}-shm`,
  ]
  try {
    for (const file of files) {
      fs.rmSync(file, { force: true })
    }
  } catch (err) {
    throw Err.wrapError(err, Err.FS_ERROR)
  }
}

export {
  isCid, isTerm, isSeq, isSeed,
  isRpc,
  createSQLiteDirectory,
  deleteSQLiteFiles,
}
