const ARGUMENT_ILLEGAL = 10_000
const LOG_NOT_OPEN = 10_001
const LOG_CORRUPT = 10_002
const LOG_OPEN = 10_003
const SQLITE_ERROR = 10_004
const FS_ERROR = 10_005

const NODE_NOT_OPEN = 10_006
const NO_LEADER = 10_007
const NOT_LEADER = 10_008
const TERM_DIFF = 10_009
const NOT_COMMIT = 10_010

const PING_TIMEOUT = 10_011
const APPEND_TIMEOUT = 10_012
const RPC_ILLEGAL = 10_013
const RAFT_ILLEGAL = 10_014
const SEND_ERROR = 10_015
const APPLY_ERROR = 10_016
const REPL_BACKTRACK = 10_017
const REPL_FORGOT = 10_018

const REPAIR_QUORUM_IMPOSSIBLE = 10_019
const REPAIR_OUTSIDE_AGREEMENT = 10_020
const DRAINING = 10_021
const MONSTER_ILLEGAL = 10_022
const MONSTER_CORRUPT = 10_023

const ErrorCodes = Object.freeze({
  ARGUMENT_ILLEGAL,
  LOG_NOT_OPEN,
  LOG_CORRUPT,
  LOG_OPEN,
  SQLITE_ERROR,
  FS_ERROR,
  NODE_NOT_OPEN,
  NO_LEADER,
  NOT_LEADER,
  TERM_DIFF,
  NOT_COMMIT,
  PING_TIMEOUT,
  APPEND_TIMEOUT,
  RPC_ILLEGAL,
  RAFT_ILLEGAL,
  SEND_ERROR,
  APPLY_ERROR,
  REPL_BACKTRACK,
  REPL_FORGOT,
  REPAIR_QUORUM_IMPOSSIBLE,
  REPAIR_OUTSIDE_AGREEMENT,
  DRAINING,
  MONSTER_ILLEGAL,
  MONSTER_CORRUPT,
})

class ErrorWithCode extends Error {
  constructor(message, code = null, sqlCode = null) {
    super(message)
    this.code = code
    this.sqlCode = sqlCode
  }
}

const wrapError = (err, code=null, prefix=null) => {
  const stack = err?.stack
  const sqlite = err?.code === 'ERR_SQLITE_ERROR'
  code = code ?? (sqlite ? SQLITE_ERROR : err?.code)
  code = Number.isSafeInteger(code) ? code : null
  const nativeSqlCode = err?.sqlCode ?? (sqlite ? err.errcode : null)
  const sqlCode = Number.isSafeInteger(nativeSqlCode) ? nativeSqlCode : null
  const originalMessage = err?.message ?? err?.msg ?? String(err)
  const message = prefix === null ? originalMessage : `${prefix}${originalMessage}`

  if (err instanceof ErrorWithCode) {
    err.message = message
    err.code = code
    err.sqlCode = sqlCode
  } else {
    err = new ErrorWithCode(message, code, sqlCode)
  }
  if (typeof stack === 'string') {
    // Bypass V8's stack setter, which can lose the uncaught-error location.
    Object.defineProperty(err, 'stack', {
      value: stack,
      writable: true,
      configurable: true,
      enumerable: false,
    })
  }
  return err
}

export {
  ErrorCodes,
  ErrorWithCode,
  wrapError,
  ARGUMENT_ILLEGAL,
  LOG_NOT_OPEN,
  LOG_CORRUPT,
  LOG_OPEN,
  SQLITE_ERROR,
  FS_ERROR,
  NODE_NOT_OPEN,
  NO_LEADER,
  NOT_LEADER,
  TERM_DIFF,
  NOT_COMMIT,
  PING_TIMEOUT,
  APPEND_TIMEOUT,
  RPC_ILLEGAL,
  RAFT_ILLEGAL,
  SEND_ERROR,
  APPLY_ERROR,
  REPL_BACKTRACK,
  REPL_FORGOT,
  REPAIR_QUORUM_IMPOSSIBLE,
  REPAIR_OUTSIDE_AGREEMENT,
  DRAINING,
  MONSTER_ILLEGAL,
  MONSTER_CORRUPT,
}
