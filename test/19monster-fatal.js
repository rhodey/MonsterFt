import crypto from 'node:crypto'
import { spawnSync } from 'node:child_process'
import { DatabaseSync } from 'node:sqlite'
import { unpack } from 'msgpackr'
import test from 'tape'
import { MonsterFt, SQLiteLog } from '../src/index.js'
import {
  APPEND_TIMEOUT, APPLY_ERROR, ErrorWithCode, MONSTER_ILLEGAL,
  REPAIR_OUTSIDE_AGREEMENT, SQLITE_ERROR,
} from '../src/error.js'
import { databasePath, sleep } from './util.js'

const ids = ['1', '2', '3']
let fixtureId = 0
const openNodes = (nodes) => nodes.forEach((node) => node.open())
const closeNodesQuietly = (nodes) => {
  for (const node of nodes) {
    try { node.close() } catch {}
  }
}

const withTimeout = (promise, name, ms=8_000) => {
  let timer = null
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${name} timeout`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

const waitFor = async (fn, name, ms=8_000) => {
  const end = Date.now() + ms
  while (!fn()) {
    if (Date.now() >= end) { throw new Error(`${name} timeout`) }
    await sleep(5)
  }
}

const nextSync = (node) => new Promise((resolve) => node.once('sync', resolve))

const failCommandLookup = (node, failure) => {
  node._monsterRunDbGetCmd = async () => { throw failure }
}

const makeFixture = (t, name, allowMessage=() => true) => {
  const unique = `${++fixtureId}-${process.pid}-${name}`
  const paths = new Map(ids.map((id) => [
    id,
    databasePath(`monster-fatal-${unique}-${id}`),
  ]))
  const current = new Map()
  const warnings = []

  const nodes = ids.map((id) => {
    const send = (to, msg) => {
      const target = current.get(to)
      if (!target) { return }
      if (!allowMessage(id, to, msg)) { return }
      return target.onReceive(id, msg)
    }
    const node = new MonsterFt(id, ids, send, paths.get(id), {
      electionTimeout: 60_000,
      pingTimeout: 500,
      appendTimeout: 3_000,
      quorum: 2,
      apply: () => null,
    })
    node.on('warn', (err) => warnings.push({ id, err }))
    current.set(id, node)
    return node
  })

  t.teardown(() => {
    closeNodesQuietly(nodes)
    for (const file of paths.values()) {
      new SQLiteLog(file).del()
      new SQLiteLog(`${file}2`).del()
    }
  })

  return { nodes, warnings }
}

const appendsEntry = (msg, type) => msg.type === 'append' &&
  msg.data?.some(({ entry }) => entry.length > 0 && unpack(entry).type === type)

const elect = async (nodes) => {
  openNodes(nodes)
  const leader = nodes[0]
  leader._voteForSelf()
  await waitFor(() => leader.state === 'leader', 'leader election')
  await withTimeout(
    Promise.all(nodes.map((node) => node.awaitLeader(true))),
    'leader commit',
  )
  await leader._monsterLeaderSync
  return leader
}

const expectFatalClose = async (t, target, source, msg, failure, context) => {
  const fatals = []
  const message = failure.message
  target.on('fatal', (err) => fatals.push(err))
  const errored = new Promise((resolve) => target.once('error', resolve))
  source.send(target.id, msg)
  const err = await withTimeout(errored, `${context} fatal close`)
  t.deepEqual(fatals, [failure], `${context} enters the Raft fatal path exactly once`)
  t.equal(err, failure, `${context} reports the original local failure`)
  t.equal(err.message, message,
    `${context} preserves the original failure message`)
  t.notOk(target.isOpen, `${context} closes the affected receiver`)
}

test('uncaught application errors retain their trace without an anonymous script header', (t) => {
  const moduleUrl = new URL('../src/index.js', import.meta.url).href
  for (const mode of ['startup', 'local', 'forwarded']) {
    const source = `
      import { MonsterFt } from ${JSON.stringify(moduleUrl)}
      const mode = ${JSON.stringify(mode)}
      const ids = ['1', '2', '3']
      const byId = new Map()
      const nodes = ids.map((id) => {
        const node = new MonsterFt(id, ids,
          (to, msg) => byId.get(to).onReceive(id, msg), ':memory:', {
            electionTimeout: 60000,
            apply: function applicationApply(db, data, term, seq) {
              if (mode !== 'startup' && seq === 0n) { return }
              throw new TypeError('original application failure')
            },
          })
        byId.set(id, node)
        return node
      })
      if (mode === 'startup') {
        nodes[0].on('error', (err) => {
          queueMicrotask(() => { throw err })
        })
      }
      nodes.forEach((node) => node.open())
      nodes[0]._voteForSelf()
      if (mode !== 'startup') {
        await Promise.all(nodes.map((node) => node.awaitLeader(true)))
        const caller = nodes[mode === 'local' ? 0 : 1]
        try {
          await caller.append(Buffer.from('fail'))
        } catch (err) {
          queueMicrotask(() => caller.emit('error', err))
        }
      }
    `
    const result = spawnSync(process.execPath, ['--input-type=module', '-e', source], {
      encoding: 'utf8', timeout: 8000,
    })
    t.error(result.error, `${mode} child completes without a timeout`)
    t.equal(result.status, 1, `${mode} still exits on an unhandled error`)
    t.match(result.stderr, /TypeError: original application failure/,
      `${mode} preserves the original error header`)
    t.match(result.stderr, /at MonsterFt\.applicationApply/,
      `${mode} preserves the application frame`)
    t.notOk(/<anonymous_script>:0|\n{3}/.test(result.stderr),
      `${mode} has no anonymous header or extra blank lines`)
  }
  t.end()
})

for (const asynchronous of [false, true]) {
  const mode = asynchronous ? 'async' : 'sync'
  test(`MonsterFt preserves ${mode} sequence-zero errors in fatal and error events`, async (t) => {
    const fixture = makeFixture(t, `schema-stack-${mode}`)
    const node = fixture.nodes[0]
    let original = null
    let fatal = null
    const schemaApply = () => {
      original = new TypeError('schema application failure')
      throw original
    }
    node._monsterUserApply = asynchronous
      ? async () => {
          await Promise.resolve()
          schemaApply()
        }
      : schemaApply
    node.on('fatal', (err) => { fatal = err })
    const errored = new Promise((resolve) => node.once('error', resolve))
    openNodes(fixture.nodes)
    node._voteForSelf()
    const err = await withTimeout(errored, `${mode} schema error event`)
    t.ok(err instanceof ErrorWithCode, 'emits a normalized error')
    t.equal(err.code, APPLY_ERROR, 'the sequence-zero failure retains its fatal code')
    t.equal(err.message, '(apply) schema application failure', 'the contextual message is preserved')
    t.equal(err.stack, original.stack, 'the emitted error keeps the original trace')
    t.match(err.stack, /schemaApply/, 'the trace identifies the application callback')
    t.equal(err, fatal, 'fatal and error events report the same error')
    t.notOk(node.isOpen, 'the initialization failure still closes the node')
    t.deepEqual(fixture.warnings, [], 'sequence-zero failures add no item warnings')
  })
}

test('MonsterFt treats an illegal repair state as fatal', async (t) => {
  const fixture = makeFixture(t, 'repair-state')
  const leader = await elect(fixture.nodes)
  const fatals = []
  leader.on('fatal', (err) => fatals.push(err))
  const errored = new Promise((resolve) => leader.once('error', resolve))
  let thrown = null

  try {
    leader._monsterRepairError(0)
  } catch (err) {
    thrown = err
  }

  const reported = await withTimeout(errored, 'illegal repair state fatal')
  t.ok(thrown instanceof ErrorWithCode,
    'the illegal state throws ErrorWithCode')
  t.equal(thrown.message, 'repair state is illegal',
    'the illegal state throws the invariant error')
  t.equal(thrown.code, MONSTER_ILLEGAL,
    'the illegal state has the Monster invariant code')
  t.equal(thrown.sqlCode, null,
    'the illegal state has no SQLite error code')
  t.deepEqual(fatals, [thrown],
    'the invariant error enters the fatal path exactly once')
  t.equal(reported, thrown, 'the fatal path reports the same error')
  t.equal(leader.isOpen, false, 'the illegal state closes the node')
})

test('MonsterFt leader sync command lookup failure is fatal once',
  async (t) => {
    const fixture = makeFixture(t, 'leader-sync-command')
    const leader = fixture.nodes[0]
    const failure = new Error('injected leader sync command lookup failure')
    const message = failure.message
    const diagnostics = []
    leader.on('fatal', (err) => diagnostics.push({ type: 'fatal', err }))
    leader.on('warn', (err) => diagnostics.push({ type: 'warn', err }))
    const errored = new Promise((resolve) => {
      leader.on('error', (err) => {
        diagnostics.push({ type: 'error', err })
        resolve(err)
      })
    })
    failCommandLookup(leader, failure)

    openNodes(fixture.nodes)
    leader._voteForSelf()
    const reported = await withTimeout(
      errored,
      'leader sync command lookup fatal',
    )
    let rejected = null
    try {
      await leader._monsterLeaderSync
    } catch (err) {
      rejected = err
    }

    t.equal(rejected, failure,
      'leader synchronization rejects with the lookup failure')
    t.equal(reported, failure,
      'the fatal path reports the original lookup failure')
    t.equal(failure.message, message,
      'leader synchronization preserves the failure message')
    t.deepEqual(diagnostics.map(({ type }) => type), ['fatal', 'error'],
      'leader synchronization has one fatal diagnostic owner')
    t.ok(diagnostics.every(({ err }) => err === failure),
      'every diagnostic preserves the original failure')
    t.equal(leader.isOpen, false,
      'the lookup failure closes the leader')
  })

test('MonsterFt replies to a failed forwarded CMD before warning locally', async (t) => {
  const events = []
  const fixture = makeFixture(t, 'forward-timeout', (from, to, msg) => {
    if (from === '1' && to === '2' && msg.type === 'err') {
      events.push('reply')
    }
    return !appendsEntry(msg, 'cmd')
  })
  const leader = await elect(fixture.nodes)
  leader.opts.appendTimeout = 100
  leader.on('warn', () => {
    events.push('warn')
    leader.close()
  })

  const rejected = await withTimeout(
    fixture.nodes[1].append(Buffer.from('forwarded')).catch((err) => err),
    'forwarded rejection',
  )
  t.equal(rejected.code, APPEND_TIMEOUT, 'the caller receives the append failure')
  t.equal(rejected.message, 'quorum append timeout',
    'the caller gets the leader failure rather than its own RPC timeout')
  t.deepEqual(events, ['reply', 'warn'],
    'the RPC reply is sent before a warning listener can close the leader')
  t.equal(fixture.warnings.length, 1, 'the leader reports one local warning')
  t.equal(fixture.warnings[0].id, leader.id, 'the warning belongs to the leader')
  t.equal(fixture.warnings[0].err.stack, rejected.stack,
    'the warning and RPC reply retain the same failure trace')
  t.notOk(leader.isOpen, 'the warning listener can close the leader')
})

for (const asynchronous of [false, true]) {
  const mode = asynchronous ? 'async' : 'sync'
  test(`MonsterFt warns once per ${mode} rejected item with a separate APPLY_ERROR`, async (t) => {
    const fixture = makeFixture(t, `application-rejection-${mode}`)
    const diagnostics = []
    const originals = new Map(ids.map((id) => [id, []]))
    const warned = new Map(ids.map((id) => [id, []]))
    const code = asynchronous ? 123 : null
    const sqlCode = asynchronous ? 456 : null
    for (const node of fixture.nodes) {
      node.on('error', (err) => diagnostics.push(err))
      node.on('fatal', (err) => diagnostics.push(err))
      const applicationApply = (db, data, term, seq) => {
        if (seq === 0n || data.toString() === 'ok') { return 'ok' }
        const err = asynchronous
          ? new ErrorWithCode('application rejected command', code, sqlCode)
          : new TypeError('application rejected command')
        if (!asynchronous) { err.code = 'APP_ERROR' }
        originals.get(node.id).push(err)
        throw err
      }
      node._monsterUserApply = asynchronous
        ? async (...args) => {
            await Promise.resolve()
            return applicationApply(...args)
          }
        : applicationApply
      node.on('warn', (err) => {
        const original = originals.get(node.id).at(-1)
        warned.get(node.id).push({
          error: err,
          message: err.message,
          code: err.code,
          sqlCode: err.sqlCode,
          stack: err.stack,
          original,
        })
        err.message = `warning changed by ${node.id}`
        err.code = 900 + Number(node.id)
        err.sqlCode = 800 + Number(node.id)
        err.stack = `warning trace changed by ${node.id}`
      })
    }
    const leader = await elect(fixture.nodes)
    for (const caller of fixture.nodes.slice(0, 2)) {
      for (const batch of [false, true]) {
        const label = `${caller === leader ? 'local' : 'forwarded'} ${batch ? 'batch' : 'single'}`
        const previous = originals.get(leader.id).length
        let rejected
        if (batch) {
          const [, outcomes] = await caller.appendBatch(
            ['reject', 'ok', 'reject'].map((value) => Buffer.from(value)),
          )
          t.deepEqual(outcomes.map(({ status }) => status),
            ['rejected', 'fulfilled', 'rejected'], `${label} retains per-item outcomes`)
          t.equal(outcomes[1].value, 'ok', `${label} retains the successful item`)
          rejected = [outcomes[0].reason, outcomes[2].reason]
        } else {
          rejected = [await caller.append(Buffer.from('reject')).catch((err) => err)]
        }
        for (const [index, err] of rejected.entries()) {
          t.ok(err instanceof ErrorWithCode, `${label} returns a normalized error`)
          t.deepEqual([err.message, err.code, err.sqlCode],
            ['application rejected command', code, sqlCode],
            `${label} retains the application's message and codes despite warning mutation`)
          t.equal(err.stack, originals.get(leader.id)[previous + index].stack,
            `${label} retains the leader's application trace`)
        }
        await leader._monsterProtocol
        const syncSeq = leader._applySeq
        await waitFor(() => fixture.nodes.every((node) => node._applySeq >= syncSeq),
          `${label} applied everywhere`)
      }
    }
    for (const node of fixture.nodes) {
      const warnings = warned.get(node.id)
      t.equal(warnings.length, 6, `${node.id} warns once for each of its six failed executions`)
      t.equal(originals.get(node.id).length, 6, `${node.id} executes each failed item once`)
      for (const warning of warnings) {
        t.ok(warning.error instanceof ErrorWithCode, `${node.id} emits an ErrorWithCode`)
        t.notEqual(warning.error, warning.original, `${node.id} emits a separate warning error`)
        t.deepEqual([warning.message, warning.code, warning.sqlCode],
          ['application rejected command', APPLY_ERROR, sqlCode],
          `${node.id} classifies the warning while preserving the message and SQL code`)
        t.equal(warning.stack, warning.original.stack, `${node.id} emits its original local trace`)
        t.match(warning.stack, /applicationApply/, `${node.id} identifies the application callback`)
      }
    }
    t.deepEqual(diagnostics, [], 'warning mutation does not change agreement or emit errors or fatals')
    t.ok(fixture.nodes.every((node) => node.isOpen), 'all replicas remain in service')
  })
}

for (const targetId of ['1', '2']) {
  const role = targetId === '1' ? 'leader' : 'follower'
  test(`MonsterFt warns on a dissenting ${role} before reporting repair`, async (t) => {
    const fixture = makeFixture(t, `dissenting-${role}`)
    const target = fixture.nodes.find((node) => node.id === targetId)
    const diagnostics = []
    let original
    for (const node of fixture.nodes) {
      node._monsterUserApply = function applicationApply(db, data, term, seq) {
        if (seq === 0n) { return }
        if (node === target) {
          original = new TypeError(`dissenting ${role} failure`)
          throw original
        }
        return 'ok'
      }
    }
    for (const type of ['warn', 'fatal', 'error']) {
      target.on(type, (err) => diagnostics.push({ type, err }))
    }
    const leader = await elect(fixture.nodes)
    const result = await withTimeout(
      leader.append(Buffer.from('dissent')).catch((err) => err), `${role} command result`,
    )
    await waitFor(() => diagnostics.some(({ type }) => type === 'error'), `${role} repair error`)
    t.deepEqual(diagnostics.map(({ type }) => type), ['warn', 'fatal', 'error'],
      'the original warning precedes the existing repair diagnostics')
    t.equal(fixture.warnings.length, 1, 'only the failed local execution warns')
    const warning = diagnostics[0].err
    t.ok(warning instanceof ErrorWithCode, 'the original warning is normalized')
    t.equal(warning.code, APPLY_ERROR, 'the warning is classified as an application failure')
    t.notEqual(warning, original, 'the warning has a separate error object')
    t.equal(warning.message, original.message, 'the warning preserves the application message')
    t.equal(warning.stack, original.stack, 'the warning preserves the application trace')
    t.match(warning.stack, /applicationApply/, 'the trace identifies the application callback')
    t.equal(diagnostics[1].err.code, REPAIR_OUTSIDE_AGREEMENT,
      'the existing repair code is retained')
    t.equal(diagnostics[2].err, diagnostics[1].err, 'the repair fatal still emits its error')
    t.notOk(target.isOpen, 'the dissenting node closes')
    if (target === leader) {
      t.equal(result.code, REPAIR_OUTSIDE_AGREEMENT, 'the dissenting leader rejects with repair')
    } else {
      t.equal(result[1], 'ok', 'the healthy majority still returns its agreed result')
    }
  })
}

test('MonsterFt warns when recovery SYNC times out after stepping down', async (t) => {
  let recovery = false
  const fixture = makeFixture(t, 'recovery-timeout', (from, to, msg) => {
    if (!recovery && msg.type === 'monster_outcome') { return false }
    return !recovery || !appendsEntry(msg, 'sync')
  })
  const leader = await elect(fixture.nodes)
  const original = leader.append(Buffer.from('pending')).catch((err) => err)
  await waitFor(() => fixture.nodes.every((node) => node._monsterPendingCommand),
    'pending CMD on every replica')
  leader._toFollower()
  await original
  await leader._monsterProtocol
  fixture.warnings.length = 0

  recovery = true
  leader.opts.appendTimeout = 100
  leader._voteForSelf()
  await waitFor(() => leader.state === 'leader', 'recovery election')
  const rejected = await withTimeout(
    leader._monsterLeaderSync.catch((err) => err), 'recovery SYNC timeout',
  )
  t.equal(rejected.code, APPEND_TIMEOUT, 'recovery fails with the append timeout')
  t.equal(rejected.message, 'quorum append timeout', 'the failed operation is replication')
  t.equal(leader.state, 'follower', 'the failed append already stepped down')
  t.deepEqual(fixture.warnings, [{ id: leader.id, err: rejected }],
    'background recovery still reports the original failure exactly once')
  t.ok(leader.isOpen, 'the recovery timeout leaves the node open')
})

test('MonsterFt reports the original failure before a fatal rollback failure', async (t) => {
  const fixture = makeFixture(t, 'rollback-failure')
  const node = fixture.nodes[0]
  const original = new TypeError('primary application failure')
  const rollback = new Error('secondary rollback failure')
  const diagnostics = []
  node._monsterUserApply = () => { throw original }
  node.on('warn', (err) => diagnostics.push({ type: 'warn', err }))
  node.on('fatal', (err) => diagnostics.push({ type: 'fatal', err }))
  const errored = new Promise((resolve) => node.on('error', (err) => {
    diagnostics.push({ type: 'error', err })
    if (err.code === SQLITE_ERROR) { resolve(err) }
  }))
  openNodes(fixture.nodes)
  const exec = node.db.exec.bind(node.db)
  node.db.exec = (sql) => {
    if (sql === 'ROLLBACK') { throw rollback }
    return exec(sql)
  }
  node._voteForSelf()
  const reported = await withTimeout(errored, 'rollback fatal error')

  t.deepEqual(diagnostics.map(({ type }) => type), ['error', 'fatal', 'error'],
    'the original failure is reported before fatal shutdown')
  t.ok(diagnostics[0].err instanceof ErrorWithCode, 'the original failure is normalized')
  t.equal(diagnostics[0].err.message, original.message, 'the error event preserves the original message')
  t.equal(diagnostics[0].err.stack, original.stack, 'the error event preserves the original stack')
  t.equal(reported.code, SQLITE_ERROR, 'rollback failure remains fatal')
  t.equal(reported.message, 'DB2 rollback secondary rollback failure',
    'the fatal diagnostic identifies the rollback failure')
  t.equal(reported.stack, rollback.stack, 'the rollback failure retains its own trace')
  t.equal(diagnostics[1].err, reported, 'fatal and error share the rollback diagnostic')
  t.notOk(node.isOpen, 'rollback failure closes the node')
  t.deepEqual(fixture.warnings, [], 'sequence-zero rollback handling adds no item warning')
})

for (const mode of ['conflict rollback', 'disk full', 'rollback cleanup', 'release cleanup']) {
  test(`MonsterFt preserves the application error after ${mode}`, async (t) => {
    const transactionLost = mode === 'conflict rollback' || mode === 'disk full'
    const fixture = makeFixture(t, mode.replaceAll(' ', '-'))
    const originals = new Map()
    for (const node of fixture.nodes) {
      node.on('error', () => {})
      node._monsterUserApply = function applicationApply(db, data, term, seq, index) {
        if (seq === 0n) {
          db.exec(`
            CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB);
            INSERT INTO items VALUES (1, zeroblob(100));
          `)
          return
        }
        if (index === 0) {
          db.exec('INSERT INTO items VALUES (2, zeroblob(100))')
          return
        }
        try {
          if (mode === 'conflict rollback') {
            db.exec('INSERT OR ROLLBACK INTO items VALUES (1, NULL)')
          } else if (mode === 'disk full') {
            db.exec('INSERT INTO items VALUES (3, zeroblob(1048576))')
          } else {
            throw new TypeError('original item failure')
          }
        } catch (err) {
          originals.set(node.id, err)
          throw err
        }
      }
    }
    const leader = await elect(fixture.nodes)
    const metaSql = 'SELECT * FROM monsterft_meta'
    const before = leader.db.prepare(metaSql).get()
    if (mode === 'disk full') {
      for (const node of fixture.nodes) {
        const { page_count: pages } = node.db.prepare('PRAGMA page_count').get()
        node.db.exec(`PRAGMA max_page_count = ${pages}`)
      }
    }
    const cleanupSql = []
    const failingSql = mode === 'rollback cleanup'
      ? 'ROLLBACK TO monsterft_item'
      : mode === 'release cleanup' ? 'RELEASE monsterft_item' : null
    const exec = leader.db.exec.bind(leader.db)
    leader.db.exec = (sql) => {
      if (originals.has(leader.id)) {
        cleanupSql.push(sql)
        if (sql === failingSql) {
          throw new Error(`injected ${mode} failure`)
        }
      }
      return exec(sql)
    }
    const diagnostics = []
    const warnings = []
    leader.on('warn', (err) => {
      warnings.push(err)
      diagnostics.push({ type: 'warn', err })
    })
    leader.on('fatal', (err) => diagnostics.push({ type: 'fatal', err }))
    const errored = new Promise((resolve) => leader.on('error', (err) => {
      diagnostics.push({ type: 'error', err })
      if (mode === 'release cleanup' && diagnostics.length === 2) {
        throw new Error('error listener failure')
      }
      if (diagnostics.some(({ type }) => type === 'fatal')) { resolve(err) }
    }))
    const appended = leader.appendBatch([Buffer.from('write'), Buffer.from('fail')])
      .catch((err) => err)
    await withTimeout(errored, `${mode} fatal diagnostic`)
    await withTimeout(appended, `${mode} append rejection`)

    const original = originals.get(leader.id)
    t.ok(original, 'the application receives the original thrown error')
    t.equal(warnings.length, 1, 'the failed item emits one local warning')
    t.equal(diagnostics[0].type, 'warn', 'the warning precedes all cleanup diagnostics')
    t.deepEqual([warnings[0].message, warnings[0].code, warnings[0].sqlCode],
      [original.message, APPLY_ERROR,
        mode === 'disk full' ? 13 : mode === 'conflict rollback' ? 1555 : null],
      'the warning retains its application classification, original message and SQLite code')
    t.equal(warnings[0].stack, original.stack, 'the warning retains the original stack')
    const first = diagnostics[1]
    t.notEqual(warnings[0], first.err, 'later error handling uses a separate error object')
    t.equal(first.type, transactionLost ? 'fatal' : 'error',
      transactionLost
        ? 'the original error directly enters fatal shutdown'
        : 'the original error is reported before fatal shutdown')
    t.ok(first.err instanceof ErrorWithCode, 'the original error is normalized')
    t.equal(first.err.message, `${transactionLost ? '(apply) ' : ''}${original.message}`,
      'the original message is preserved with the applicable context')
    t.equal(first.err.stack, original.stack, 'the original stack is preserved')
    t.match(first.err.stack, /applicationApply/, 'the trace identifies the application callback')
    const fatal = diagnostics.find(({ type }) => type === 'fatal').err
    if (transactionLost) {
      t.equal(first.err.code, APPLY_ERROR, 'the transaction loss is an application failure')
      t.equal(first.err.sqlCode, mode === 'disk full' ? 13 : 1555,
        'the original SQLite error code is preserved')
      t.deepEqual(diagnostics, [
        { type: 'warn', err: warnings[0] },
        { type: 'fatal', err: fatal },
        { type: 'error', err: fatal },
      ], 'the warning is followed by the original error through fatal and error events')
      t.deepEqual(cleanupSql, [], 'no SQL cleanup is attempted after the transaction ended')
    } else {
      t.notEqual(fatal, first.err, 'cleanup failure retains its separate fatal diagnostic')
      t.match(fatal.message, new RegExp(`injected ${mode} failure`),
        'the fatal diagnostic identifies the failed cleanup')
    }
    if (mode === 'release cleanup') {
      t.ok(diagnostics.some(({ err }) => err.message === 'error listener failure'),
        'a throwing error listener does not interrupt cleanup and fatal shutdown')
    }
    t.notOk(leader.isOpen, 'the failed application transaction closes the node')

    const restored = new DatabaseSync(leader._monsterDatabasePath, {
      readOnly: true,
      readBigInts: true,
    })
    try {
      t.deepEqual(restored.prepare(metaSql).get(), before,
        'the durable checkpoint and pending state are unchanged')
      t.deepEqual(restored.prepare('SELECT id FROM items ORDER BY id').all()
        .map(({ id }) => id), [1n], 'the earlier batch write is rolled back; existing data remains')
    } finally {
      restored.close()
    }
  })
}

test('MonsterFt OUTCOME DB FIFO failure fatally closes its receiver',
  async (t) => {
    const fixture = makeFixture(t, 'outcome')
    const leader = await elect(fixture.nodes)
    const source = fixture.nodes.find((node) => node !== leader)
    const warningCount = fixture.warnings.length
    const failure = new Error('injected outcome DB FIFO failure')

    leader._monsterRunDb = async () => { throw failure }
    await expectFatalClose(t, leader, source, {
      type: 'monster_outcome',
      cmdSeq: 0n,
      digest: Buffer.alloc(32),
    }, failure, 'OUTCOME FIFO')

    t.deepEqual(
      fixture.warnings.slice(warningCount).filter(({ id }) => id === source.id),
      [],
      'the one-way receiver failure is not reported as a sender transport warning',
    )
  })

test('MonsterFt OUTCOME_REQUEST pending lookup failure fatally closes its receiver',
  async (t) => {
    const fixture = makeFixture(t, 'outcome-request')
    const leader = await elect(fixture.nodes)
    const target = fixture.nodes.find((node) => node !== leader)
    const warningCount = fixture.warnings.length
    const failure = new Error('injected outcome request pending lookup failure')

    failCommandLookup(target, failure)
    await expectFatalClose(t, target, leader, {
      type: 'monster_outcome_request',
      cmdSeq: 0n,
    }, failure, 'OUTCOME_REQUEST CMD 0 lookup')

    t.deepEqual(fixture.warnings.slice(warningCount).filter(({ id }) => id === leader.id), [],
      'the one-way receiver failure is not reported as a sender transport warning')
  })

test('MonsterFt hashes one command envelope only at CMD', async (t) => {
  const fixture = makeFixture(t, 'digest-count')
  const leader = await elect(fixture.nodes)
  const createHash = crypto.createHash
  let hashes = 0
  let syncSeq = null

  crypto.createHash = (...args) => {
    hashes++
    return createHash(...args)
  }
  try {
    const synced = nextSync(leader)
    await leader.append(Buffer.from('digest count'))
    syncSeq = (await withTimeout(synced, 'agreed SYNC event')).syncSeq
    await waitFor(() => {
      return fixture.nodes.every((node) => node._applySeq >= syncSeq)
    }, 'agreed SYNC apply')
  } finally {
    crypto.createHash = createHash
  }

  t.equal(hashes, ids.length * 3,
    'each replica hashes one command envelope plus its CMD and SYNC checkpoints')
})
