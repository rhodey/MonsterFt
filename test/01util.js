import test from 'tape'
import * as Public from '../src/index.js'
import {
  awaitResolve,
  errorRpc,
  isRpc,
  oneShot,
  timeout,
} from '../src/util.js'
import {
  APPLY_ERROR,
  APPEND_TIMEOUT,
  ARGUMENT_ILLEGAL,
  REPL_BACKTRACK,
  REPL_FORGOT,
  DRAINING,
  ErrorWithCode,
  FS_ERROR,
  KEEP_HALT,
  LOG_CORRUPT,
  LOG_NOT_OPEN,
  LOG_OPEN,
  NODE_NOT_OPEN,
  NO_LEADER,
  NOT_COMMIT,
  NOT_LEADER,
  PING_TIMEOUT,
  RAFT_ILLEGAL,
  REPAIR_OUTSIDE_AGREEMENT,
  REPAIR_QUORUM_IMPOSSIBLE,
  RPC_ILLEGAL,
  SEND_ERROR,
  SQLITE_ERROR,
  TERM_DIFF,
  wrapError,
} from '../src/error.js'
import { normalizeKeepOptions } from '../src/utilm.js'

test('ErrorWithCode initializes error codes', (t) => {
  const err = new ErrorWithCode('coded error')
  const coded = new ErrorWithCode('database error', ARGUMENT_ILLEGAL, 5)

  t.ok(err instanceof Error, 'extends Error')
  t.equal(err.message, 'coded error', 'preserves the error message')
  t.equal(err.code, null, 'code defaults to null')
  t.equal(err.sqlCode, null, 'sqlCode defaults to null')
  t.equal(coded.code, 10_000, 'sets numeric code')
  t.equal(coded.sqlCode, 5, 'sets numeric sqlCode')
  t.end()
})

test('public error codes preserve the existing constants', (t) => {
  const codes = {
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
    RAFT_ILLEGAL,
    RPC_ILLEGAL,
    SEND_ERROR,
    APPLY_ERROR,
    REPL_BACKTRACK,
    REPL_FORGOT,
    REPAIR_QUORUM_IMPOSSIBLE,
    REPAIR_OUTSIDE_AGREEMENT,
    KEEP_HALT,
    DRAINING,
  }
  const values = Object.values(codes)
  t.deepEqual(values, values.map((_, index) => 10_000 + index),
    'assigns sequential values beginning at 10,000')
  t.deepEqual(Public.ErrorCodes, codes, 'exports exactly the existing names and values')
  t.ok(Object.isFrozen(Public.ErrorCodes), 'the public namespace is frozen')
  t.notOk(Object.hasOwn(Public, 'wrapError'), 'does not expose wrapError at the package root')
  t.ok(Object.keys(codes).every((name) => !Object.hasOwn(Public, name)),
    'does not expose individual constants at the package root')
  t.end()
})

test('public ErrorWithCode matches errors thrown by nodes', (t) => {
  t.equal(Public.ErrorWithCode, ErrorWithCode, 'exports the existing error class')
  t.throws(
    () => new Public.RaftNode('', ['1', '2', '3'], () => {}, ':memory:'),
    (err) => err instanceof Public.ErrorWithCode &&
      err instanceof Error &&
      err.code === Public.ErrorCodes.ARGUMENT_ILLEGAL,
    'invalid node construction throws the public class with the public code',
  )
  t.end()
})

test('errorRpc creates a normalized RPC error envelope', (t) => {
  t.deepEqual(errorRpc(
    2n,
    'rpc-error',
    new ErrorWithCode('RPC failed', ARGUMENT_ILLEGAL, 7),
  ), {
    type: 'err',
    term: 2n,
    cid: 'rpc-error',
    msg: 'RPC failed',
    code: ARGUMENT_ILLEGAL,
    sqlCode: 7,
  }, 'returns the common coded error fields')
  t.end()
})

test('isRpc recognizes messages with a string type', (t) => {
  t.equal(isRpc({ type: 'ack' }), true, 'accepts ACK')
  t.equal(isRpc({ type: 'err' }), true, 'accepts ERR')
  t.equal(isRpc({ type: 'append' }), true, 'accepts other RPC types')
  t.equal(isRpc({}), false, 'rejects a missing type')
  t.equal(isRpc({ type: null }), false, 'rejects a non-string type')
  t.equal(isRpc(null), false, 'rejects null')
  t.equal(isRpc([]), false, 'rejects arrays')
  t.equal(isRpc(Buffer.alloc(0)), false, 'rejects buffers')
  t.end()
})

test('wrapError normalizes errors and accepts code and prefix', (t) => {
  const plain = new Error('plain error')
  const normalized = wrapError(plain)
  t.ok(normalized instanceof ErrorWithCode, 'returns ErrorWithCode')
  t.notEqual(normalized, plain, 'normalizes a plain error')
  t.equal(normalized.message, 'plain error', 'null prefix preserves the message')
  t.equal(normalized.code, null, 'uses null')
  t.equal(normalized.sqlCode, null, 'has no SQLite code')

  const coded = new ErrorWithCode('coded error', ARGUMENT_ILLEGAL)
  const prefixed = wrapError(coded, null, 'prefix - ')
  t.equal(prefixed, coded, 'retains an ErrorWithCode')
  t.equal(prefixed.message, 'prefix - coded error', 'adds a prefix')
  t.equal(prefixed.code, ARGUMENT_ILLEGAL, 'retains its library code')

  const sqlite = new Error('SQLite error')
  sqlite.code = 'ERR_SQLITE_ERROR'
  sqlite.errcode = 5
  const normalizedSQLite = wrapError(sqlite)
  t.equal(normalizedSQLite.code, SQLITE_ERROR,
    'classifies a SQLite error')
  t.equal(normalizedSQLite.sqlCode, 5, 'copies the SQLite error code')

  const sqliteWithSqlCode = new Error('SQLite error with normalized metadata')
  sqliteWithSqlCode.code = 'ERR_SQLITE_ERROR'
  sqliteWithSqlCode.errcode = 5
  sqliteWithSqlCode.sqlCode = 7
  const normalizedSQLiteWithSqlCode = wrapError(sqliteWithSqlCode)
  t.equal(normalizedSQLiteWithSqlCode.sqlCode, 7,
    'prefers an existing sqlCode over native errcode')

  const response = wrapError({
    msg: 'RPC error', code: ARGUMENT_ILLEGAL, sqlCode: 9,
  })
  t.equal(response.message, 'RPC error', 'uses msg when message is missing')
  t.equal(response.code, ARGUMENT_ILLEGAL, 'preserves an RPC error code')
  t.equal(response.sqlCode, 9, 'preserves an RPC sqlCode')

  const overridden = wrapError(new Error('overridden'), ARGUMENT_ILLEGAL)
  t.equal(overridden.code, ARGUMENT_ILLEGAL, 'uses an explicit code')

  const numeric = new Error('numeric code')
  numeric.code = 12_345
  numeric.sqlCode = 7
  const normalizedNumeric = wrapError(numeric)
  t.equal(normalizedNumeric.code, 12_345,
    'preserves a safe-integer source code')
  t.equal(normalizedNumeric.sqlCode, 7,
    'preserves a safe-integer source SQLite code')

  for (const code of [null, 'E_EXTERNAL', 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    const invalid = new Error('invalid code')
    invalid.code = code
    const normalizedInvalid = wrapError(invalid)
    t.ok(normalizedInvalid instanceof ErrorWithCode,
      `normalizes source code ${String(code)} to ErrorWithCode`)
    t.equal(normalizedInvalid.code, null,
      `normalizes source code ${String(code)} to null`)
  }
  t.equal(wrapError(new Error('invalid override'), 'E_OVERRIDE').code,
    null, 'normalizes an invalid explicit override to null')

  for (const sqlCode of ['5', 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    const invalidSql = new ErrorWithCode(
      'invalid SQLite code', ARGUMENT_ILLEGAL, sqlCode)
    t.equal(wrapError(invalidSql), invalidSql,
      `reuses ErrorWithCode with sqlCode ${String(sqlCode)}`)
    t.equal(invalidSql.sqlCode, null,
      `normalizes sqlCode ${String(sqlCode)} to null`)
  }

  const invalidSQLite = new Error('SQLite error without numeric metadata')
  invalidSQLite.code = 'ERR_SQLITE_ERROR'
  invalidSQLite.errcode = '5'
  const normalizedInvalidSQLite = wrapError(invalidSQLite)
  t.equal(normalizedInvalidSQLite.code, SQLITE_ERROR,
    'still classifies SQLite errors with invalid native metadata')
  t.equal(normalizedInvalidSQLite.sqlCode, null,
    'normalizes an invalid native SQLite code to null')
  t.end()
})

test('timeout returns a cancellable timer and rejects on expiry', async (t) => {
  const [timer, timedout] = timeout(0)

  t.ok(timer, 'returns the timer handle')
  try {
    await timedout
    t.fail('timeout promise does not resolve')
  } catch (err) {
    t.equal(err.message, 'timeout', 'rejects with the timeout error')
  } finally {
    clearTimeout(timer)
  }
})

test('oneShot resolves only once', async (t) => {
  const shot = oneShot()
  const laterError = new Error('later rejection')

  t.equal(shot.settled, false, 'starts unsettled')
  t.equal(shot.resolve('first'), true, 'first resolution settles the promise')
  t.equal(shot.settled, true, 'publishes settled state')
  t.equal(shot.resolve('second'), false, 'later resolution is rejected')
  t.equal(shot.reject(laterError), false, 'later rejection is rejected')
  t.equal(await shot.promise, 'first', 'preserves the first resolved value')
})

test('oneShot rejects only once', async (t) => {
  const shot = oneShot()
  const firstError = new Error('first rejection')

  t.equal(shot.reject(firstError), true, 'first rejection settles the promise')
  t.equal(shot.settled, true, 'publishes rejected state as settled')
  t.equal(shot.reject(new Error('second rejection')), false,
    'later rejection is rejected')
  t.equal(shot.resolve('later resolution'), false, 'later resolution is rejected')
  try {
    await shot.promise
    t.fail('rejected promise does not resolve')
  } catch (err) {
    t.equal(err, firstError, 'preserves the first rejection error')
  }
})

test('awaitResolve fulfills after the success quorum', async (t) => {
  const first = oneShot()
  const second = oneShot()
  const third = oneShot()
  const observed = []
  const work = awaitResolve(
    [first.promise, second.promise, third.promise],
    (ids) => {
      observed.push([...ids])
      return ids.length >= 2
    },
    () => false
  )

  first.resolve('first')
  third.resolve('third')
  t.equal(await work, undefined, 'resolves without a value')
  t.deepEqual(observed, [['first'], ['first', 'third']],
    'passes accumulated success values to the quorum predicate')
})

test('awaitResolve rejects after the error quorum', async (t) => {
  const first = oneShot()
  const second = oneShot()
  const third = oneShot()
  const observed = []
  const work = awaitResolve(
    [first.promise, second.promise, third.promise],
    () => false,
    (ids) => {
      observed.push([...ids])
      return ids.length >= 2
    }
  )

  second.reject('second')
  first.reject('first')
  let rejected = false
  try {
    await work
  } catch (err) {
    rejected = true
    t.equal(err, undefined, 'rejects without a value')
  }
  t.ok(rejected, 'error quorum rejects the aggregate promise')
  t.deepEqual(observed, [['second'], ['second', 'first']],
    'passes accumulated rejection values to the error predicate')
})

test('normalizeKeepOptions disables or normalizes automatic retention', (t) => {
  t.equal(normalizeKeepOptions({}), null, 'omitted options disable retention')

  const keep = normalizeKeepOptions({
    keepTarget: 2,
    keepTrigger: 3,
    keepHalt: 6,
  })
  t.deepEqual(keep, { target: 2n, trigger: 3n, halt: 6n },
    'valid options are normalized to bigint boundaries')
  t.notOk(Object.isFrozen(keep), 'normalized boundaries are not frozen')
  t.end()
})

test('normalizeKeepOptions preserves KEEP validation', (t) => {
  const invalid = [
    [{ keepTarget: 2 }, /must be supplied together/,
      'partial configuration'],
    [{ keepTarget: 2n, keepTrigger: 3, keepHalt: 6 },
      /keepTarget must be a safe integer/, 'non-integer configuration'],
    [{ keepTarget: 1, keepTrigger: 3, keepHalt: 6 },
      /keepTarget must be >= 2/, 'target minimum'],
    [{ keepTarget: 3, keepTrigger: 3, keepHalt: 6 },
      /keepTarget must be < keepTrigger/, 'target ordering'],
    [{ keepTarget: 2, keepTrigger: 3, keepHalt: 4 },
      /keepHalt must be >= keepTrigger \+ 2/, 'halt headroom'],
  ]
  for (const [opts, pattern, name] of invalid) {
    let err = null
    try {
      normalizeKeepOptions(opts)
    } catch (failure) {
      err = failure
    }
    t.ok(err instanceof ErrorWithCode, `${name} throws ErrorWithCode`)
    t.match(err.message, pattern, `${name} validation is preserved`)
    t.equal(err.code, ARGUMENT_ILLEGAL,
      `${name} uses ARGUMENT_ILLEGAL`)
    t.equal(err.sqlCode, null, `${name} has no SQLite error code`)
  }
  t.end()
})
