import fs from 'node:fs'
import path from 'node:path'
import * as Err from './error.js'

const ERR = 'err'
const noop = () => {}
const MAX_INT = 9_223_372_036_854_775_807n

const isCid = (cid) => typeof cid === 'string' && cid.length > 0
const isTerm = (term) => typeof term === 'bigint' && term >= 0n && term <= MAX_INT
const isSeq = (seq) => typeof seq === 'bigint' && seq >= -1n && seq <= MAX_INT

const isRpc = (msg) => msg !== null && typeof msg === 'object' &&
  !Array.isArray(msg) && !(msg instanceof Uint8Array) &&
  typeof msg.type === 'string'

const errorRpc = (term, cid, err, code=null) => {
  err = Err.wrapError(err, code)
  const msg = {
    type: ERR,
    term,
    cid,
    msg: err.message,
    code: err.code,
    sqlCode: err.sqlCode,
  }
  if (typeof err.stack === 'string') { msg.stack = err.stack }
  return msg
}

const terr = new Error('timeout')

const timeout = (ms) => {
  let timer = null
  const timedout = new Promise((_, rej) => {
    timer = setTimeout(rej, ms, terr)
  })
  return [timer, timedout]
}

const oneShot = () => {
  let resolvePromise = null
  let rejectPromise = null
  let settled = false
  const promise = new Promise((resolve, reject) => {
    resolvePromise = resolve
    rejectPromise = reject
  })
  promise.catch(noop)
  return {
    get settled() {
      return settled
    },
    resolve(value) {
      if (settled) { return false }
      settled = true
      resolvePromise(value)
      return true
    },
    reject(err) {
      if (settled) { return false }
      settled = true
      rejectPromise(err)
      return true
    },
    promise,
  }
}

const awaitResolve = (promises, quorum, quorumErr) => {
  return new Promise((res, rej) => {
    const [ok, err] = [[], []]
    promises.forEach((promise) => {
      promise.then((id) => {
        ok.push(id)
        if (!quorum(ok)) { return }
        res()
      }).catch((id) => {
        err.push(id)
        if (!quorumErr(err)) { return }
        rej()
      })
    })
  })
}

const createSQLiteDirectory = (databasePath) => {
  try {
    fs.mkdirSync(path.dirname(databasePath), { recursive: true })
  } catch (err) {
    throw Err.wrapError(err, Err.FS_ERROR)
  }
}

const deleteSQLiteFiles = (databasePath) => {
  if (databasePath === ':memory:') { return }
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
  isCid, isTerm, isSeq,
  isRpc,
  errorRpc,
  timeout,
  oneShot,
  awaitResolve,
  createSQLiteDirectory,
  deleteSQLiteFiles,
}
