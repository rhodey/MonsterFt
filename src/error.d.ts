export const ARGUMENT_ILLEGAL: 10000
export const LOG_NOT_OPEN: 10001
export const LOG_CORRUPT: 10002
export const LOG_OPEN: 10003
export const SQLITE_ERROR: 10004
export const FS_ERROR: 10005
export const NODE_NOT_OPEN: 10006
export const NO_LEADER: 10007
export const NOT_LEADER: 10008
export const TERM_DIFF: 10009
export const NOT_COMMIT: 10010
export const HEARTBEAT_TIMEOUT: 10011
export const APPEND_TIMEOUT: 10012
export const RPC_ILLEGAL: 10013
export const RAFT_ILLEGAL: 10014
export const SEND_ERROR: 10015
export const APPLY_ERROR: 10016
export const REPL_BACKTRACK: 10017
export const REPL_FORGOT: 10018
export const REPAIR_NO_QUORUM: 10019
export const REPAIR_OUTSIDE: 10020
export const DRAINING: 10021
export const MONSTER_ILLEGAL: 10022
export const MONSTER_CORRUPT: 10023
export const LOCAL_TIMEOUT: 10024
export const APPLY_TIMEOUT: 10025
export const LOCAL_DIVERGENCE: 10026
export const FRAME_CORRUPT: 10027
export const GENERATION_DIFF: 10028
export const SUPERVISOR_ERROR: 10029
export const REPAIR_NONE: 0

export const ErrorCodes: Readonly<{
  ARGUMENT_ILLEGAL: typeof ARGUMENT_ILLEGAL
  LOG_NOT_OPEN: typeof LOG_NOT_OPEN
  LOG_CORRUPT: typeof LOG_CORRUPT
  LOG_OPEN: typeof LOG_OPEN
  SQLITE_ERROR: typeof SQLITE_ERROR
  FS_ERROR: typeof FS_ERROR
  NODE_NOT_OPEN: typeof NODE_NOT_OPEN
  NO_LEADER: typeof NO_LEADER
  NOT_LEADER: typeof NOT_LEADER
  TERM_DIFF: typeof TERM_DIFF
  NOT_COMMIT: typeof NOT_COMMIT
  HEARTBEAT_TIMEOUT: typeof HEARTBEAT_TIMEOUT
  APPEND_TIMEOUT: typeof APPEND_TIMEOUT
  RPC_ILLEGAL: typeof RPC_ILLEGAL
  RAFT_ILLEGAL: typeof RAFT_ILLEGAL
  SEND_ERROR: typeof SEND_ERROR
  APPLY_ERROR: typeof APPLY_ERROR
  REPL_BACKTRACK: typeof REPL_BACKTRACK
  REPL_FORGOT: typeof REPL_FORGOT
  REPAIR_NO_QUORUM: typeof REPAIR_NO_QUORUM
  REPAIR_OUTSIDE: typeof REPAIR_OUTSIDE
  DRAINING: typeof DRAINING
  MONSTER_ILLEGAL: typeof MONSTER_ILLEGAL
  MONSTER_CORRUPT: typeof MONSTER_CORRUPT
  LOCAL_TIMEOUT: typeof LOCAL_TIMEOUT
  APPLY_TIMEOUT: typeof APPLY_TIMEOUT
  LOCAL_DIVERGENCE: typeof LOCAL_DIVERGENCE
  FRAME_CORRUPT: typeof FRAME_CORRUPT
  GENERATION_DIFF: typeof GENERATION_DIFF
  SUPERVISOR_ERROR: typeof SUPERVISOR_ERROR
}>

export type ErrorCode = typeof ErrorCodes[keyof typeof ErrorCodes]

export class ErrorWithCode extends Error {
  constructor(message: string, code?: number | null, sqlCode?: number | null)
  code: number | null
  sqlCode: number | null
}

export function wrapError(error: unknown, code?: number | null, prefix?: string | null): ErrorWithCode

