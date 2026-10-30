import test from 'tape'
import { DatabaseSync } from 'node:sqlite'
import { spawnSync } from 'node:child_process'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { APPLY_ERROR, ARGUMENT_ILLEGAL, SQLITE_ERROR, ErrorWithCode, wrapError } from '../src/error.js'
import { normalizeKeepOptions } from '../src/utilm.js'

test('uncaught Apply failures keep their original header and callback frame', (t) => {
  const helper = new URL('./helper.js', import.meta.url).href
  for (const mode of ['setup', 'local', 'forwarded']) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-uncaught-'))
    try {
      const source = `
        import { cluster } from ${JSON.stringify(helper)}
        const mode = ${JSON.stringify(mode)}
        const c = cluster({ apply: function applicationApply(id, db, data, term, seq) {
          if (mode !== 'setup' && seq === 0n) {
            db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB)')
            return
          }
          throw new TypeError('original application failure')
        } })
        if (mode === 'setup') {
          c.nodes.get('a').apply.on('error', (err) => { queueMicrotask(() => { throw err }) })
        }
        await c.open()
        const caller = mode === 'local' ? c.leader() : c.gates().find((gate) => gate !== c.leader())
        try { await caller.append(Buffer.from('fail')) }
        catch (err) { await c.close(); throw err }
      `
      const result = spawnSync(process.execPath, ['--input-type=module', '-e', source], {
        encoding: 'utf8', timeout: 8000, env: { ...process.env, TMPDIR: directory },
      })
      t.error(result.error, `${mode} child exits without hanging`)
      t.equal(result.status, 1)
      t.match(result.stderr, /TypeError: original application failure/)
      t.match(result.stderr, /applicationApply/)
      t.notOk(/<anonymous_script>:0|\n{3}/.test(result.stderr), 'no synthetic header or empty lines')
    } finally { fs.rmSync(directory, { recursive: true, force: true }) }
  }
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
  t.equal(normalized.stack, plain.stack, 'preserves the original stack')

  const coded = new ErrorWithCode('coded error', ARGUMENT_ILLEGAL)
  const codedStack = coded.stack
  const prefixed = wrapError(coded, null, 'prefix - ')
  t.equal(prefixed, coded, 'retains an ErrorWithCode')
  t.equal(prefixed.message, 'prefix - coded error', 'adds a prefix')
  t.equal(prefixed.code, ARGUMENT_ILLEGAL, 'retains its library code')
  t.equal(prefixed.stack, codedStack, 'preserves the stack while prefixing its message')

  const sqlite = new Error('SQLite error')
  sqlite.code = 'ERR_SQLITE_ERROR'
  sqlite.errcode = 5
  const normalizedSQLite = wrapError(sqlite)
  t.equal(normalizedSQLite.code, SQLITE_ERROR,
    'classifies a SQLite error')
  t.equal(normalizedSQLite.sqlCode, 5, 'copies the SQLite error code')
  t.equal(normalizedSQLite.stack, sqlite.stack, 'preserves the SQLite error stack')

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

test('wrapError preserves original stacks across repeated normalization', (t) => {
  const original = new TypeError('application failure')
  const stack = original.stack
  const first = wrapError(original, SQLITE_ERROR, 'DB2 ')
  const second = wrapError(first, APPLY_ERROR, '(apply) ')
  t.equal(second, first, 'reuses an existing ErrorWithCode')
  t.equal(second.message, '(apply) DB2 application failure', 'retains message prefixes')
  t.equal(second.code, APPLY_ERROR, 'retains the latest code override')
  t.equal(second.stack, stack, 'keeps every original frame and the original TypeError header')
  t.equal(original.message, 'application failure', 'does not mutate the source TypeError')
  t.notOk(Object.hasOwn(second, 'cause'), 'adds no cause property')

  const unread = new ErrorWithCode('stack not read yet')
  const prefixed = wrapError(unread, APPLY_ERROR, '(apply) ')
  t.equal(prefixed.stack.split('\n')[0], 'Error: stack not read yet',
    'captures a lazy stack before mutating the message')
  t.equal(prefixed.message, '(apply) stack not read yet', 'still updates the message')
  t.end()
})

test('wrapError keeps its fallback for thrown values without string stacks', (t) => {
  for (const value of [null, undefined, 'failure', 42,
    { message: 'missing stack' }, { message: 'invalid stack', stack: 42 }]) {
    const err = wrapError(value)
    t.ok(err instanceof ErrorWithCode, 'normalizes the thrown value')
    t.equal(typeof err.stack, 'string', 'retains a generated fallback stack')
    t.equal(err.code, null, 'retains the normalized code')
  }
  const empty = wrapError({ message: 'empty stack', stack: '' })
  t.equal(empty.stack, '', 'preserves even an explicitly empty string stack')
  t.end()
})

test('wrapError preserves a native SQLite exception stack and numeric codes', (t) => {
  const db = new DatabaseSync(':memory:')
  let original = null
  try {
    db.exec('SELECT * FROM missing_stack_test_table')
  } catch (err) {
    original = err
  } finally {
    db.close()
  }
  t.ok(original, 'SQLite throws the expected exception')
  const wrapped = wrapError(original, null, 'DB2 ')
  t.equal(wrapped.stack, original.stack, 'preserves the native exception trace')
  t.equal(wrapped.message, `DB2 ${original.message}`, 'retains the contextual message')
  t.equal(wrapped.code, SQLITE_ERROR, 'normalizes the native SQLite code')
  t.equal(wrapped.sqlCode, original.errcode, 'preserves the native numeric SQLite code')
  t.end()
})

test('normalizeKeepOptions disables or normalizes automatic retention', (t) => {
  t.equal(normalizeKeepOptions({}), null, 'omitted options disable retention')

  const keep = normalizeKeepOptions({
    keepTarget: 2,
    keepTrigger: 3,
  })
  t.deepEqual(keep, { target: 2n, trigger: 3n },
    'valid options are normalized to bigint boundaries')
  t.notOk(Object.isFrozen(keep), 'normalized boundaries are not frozen')
  t.end()
})

test('normalizeKeepOptions validates both retention boundaries', (t) => {
  const invalid = [
    [{ keepTarget: 2 }, /must be supplied together/,
      'partial configuration'],
    [{ keepTrigger: 3 }, /must be supplied together/,
      'missing target'],
    [{ keepTarget: 2n, keepTrigger: 3 },
      /keepTarget must be a safe integer/, 'non-integer configuration'],
    [{ keepTarget: 1, keepTrigger: 3 },
      /keepTarget must be >= 2/, 'target minimum'],
    [{ keepTarget: 3, keepTrigger: 3 },
      /keepTarget must be < keepTrigger/, 'target ordering'],
    [{ keepTarget: 4, keepTrigger: 3 },
      /keepTarget must be < keepTrigger/, 'reversed ordering'],
  ]
  for (const value of [null, false, '3', 2n, NaN, Infinity, 2.5,
    Number.MAX_SAFE_INTEGER + 1]) {
    invalid.push(
      [{ keepTarget: value, keepTrigger: 5 },
        /keepTarget must be a safe integer/, `invalid target ${value}`],
      [{ keepTarget: 2, keepTrigger: value },
        /keepTrigger must be a safe integer/, `invalid trigger ${value}`],
    )
  }
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
