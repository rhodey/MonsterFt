import crypto from 'node:crypto'
import { DatabaseSync } from 'node:sqlite'
import test from 'tape'
import { pack, unpack } from 'msgpackr'
import {
  ErrorWithCode,
  LOG_CORRUPT,
  REPAIR_OUTSIDE_AGREEMENT,
  REPAIR_QUORUM_IMPOSSIBLE,
  RPC_ILLEGAL,
  SQLITE_ERROR,
} from '../src/error.js'
import { MonsterFt, SQLiteLog } from '../src/index.js'
import { validateSyncEntry } from '../src/utilm.js'
import { databasePath, sleep, ready, leaders } from './util.js'

const ids = ['1', '2', '3']
const noop = () => {}
let fixtureId = 0
const openNodes = (nodes) => nodes.forEach((node) => node.open())
const closeNodes = (nodes) => {
  const failures = []
  for (const node of nodes) {
    try { node.close() } catch (err) { failures.push(err) }
  }
  if (failures.length === 1) { throw failures[0] }
  if (failures.length > 1) {
    throw new AggregateError(failures, 'failed to close nodes')
  }
}
const closeNodesQuietly = (nodes) => {
  for (const node of nodes) {
    try { node.close() } catch {}
  }
}

const toBuf = (value) => Buffer.from(JSON.stringify(value), 'utf8')
const toObj = (buf) => JSON.parse(Buffer.from(buf).toString('utf8'))

const deferred = () => {
  let resolve = null
  let reject = null
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

const waitFor = async (fn, name, ms=10_000) => {
  const end = Date.now() + ms
  while (!fn()) {
    if (Date.now() >= end) { throw new Error(`${name} timeout`) }
    await sleep(10)
  }
}

const withTimeout = (promise, name, ms=10_000) => {
  let timer = null
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${name} timeout`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

const nextEvent = (node, type, predicate=() => true) => {
  return new Promise((resolve) => {
    const listener = (event) => {
      if (!predicate(event)) { return }
      node.removeListener(type, listener)
      resolve(event)
    }
    node.on(type, listener)
  })
}

const rejects = async (t, promise, pattern, name) => {
  try {
    await promise
    t.fail(name)
    return null
  } catch (err) {
    t.match(err.message, pattern, name)
    return err
  }
}

const rejectsCode = async (t, promise, code, name) => {
  try {
    await promise
    t.fail(name)
    return null
  } catch (err) {
    t.equal(err.code, code, name)
    return err
  }
}

const initializeSchema = (db) => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS monster_metadata_items (
      key TEXT PRIMARY KEY,
      value INTEGER NOT NULL
    ) STRICT
  `)
}

const write = (db, key, value) => {
  db.prepare(`
    INSERT INTO monster_metadata_items (key, value)
    VALUES (?, ?)
    ON CONFLICT (key) DO UPDATE SET value = excluded.value
  `).run(key, value)
}

const defaultApply = (db, node, buf) => {
  const command = toObj(buf)
  write(db, command.key, command.value)
  return command
}

const createBus = (intercept=null) => {
  const nodes = new Map()
  return {
    register(node) {
      nodes.set(node.id, node)
    },
    send(to, from, original) {
      let msg = original
      if (intercept) {
        const next = intercept(to, from, original)
        if (next === false) { return }
        if (next !== undefined) { msg = next }
      }
      const node = nodes.get(to)
      if (!node) { throw new Error(`node ${from} send to ${to} not found`) }
      return node.onReceive(from, msg)
    },
  }
}

const makeFixture = (t, name) => {
  const unique = `${++fixtureId}-${process.pid}-${name}`
  const paths = new Map(ids.map((id) => [
    id,
    databasePath(`monster-metadata-${unique}-${id}`),
  ]))
  const allNodes = []

  const reset = () => {
    for (const file of paths.values()) {
      new SQLiteLog(file).del()
      new SQLiteLog(`${file}2`).del()
    }
  }

  const build = ({
    apply=defaultApply,
    patchsetTransform=null,
    intercept=null,
    opts={},
  }={}) => {
    const bus = createBus(intercept)
    const calls = []
    const patchsetCalls = []
    const outcomeCalls = []
    const messages = []
    const nodes = ids.map((id) => {
      let node = null
      node = new MonsterFt(
        id,
        ids,
        (to, msg) => {
          messages.push({ from: id, to, msg })
          return bus.send(to, id, msg)
        },
        paths.get(id),
        {
          electionTimeout: 60_000,
          pingTimeout: 150,
          appendTimeout: 2_000,
          quorum: 2,
          ...opts,
          apply: async (db, buf, term, seq, index, matchIndex) => {
            const current = node
            calls.push({
              id: current.id,
              term,
              seq,
              index,
              matchIndex,
              command: buf === null ? null : toObj(buf),
            })
            if (seq === 0n) {
              initializeSchema(db)
              return
            }
            if (buf === null) { return }
            return apply(db, current, buf, term, seq, index, matchIndex)
          },
        },
      )
      node.on('error', noop)
      node.on('warn', noop)

      const installPendingCmd = node._monsterInstallPendingCmd.bind(node)
      node._monsterInstallPendingCmd = (
        db, term, seq, patchset, outcomes, entryHash,
      ) => {
        const raw = Buffer.from(patchset)
        const output = patchsetTransform
          ? patchsetTransform({ node, db, term, seq, patchset: raw })
          : raw
        patchsetCalls.push({
          id: node.id,
          term,
          seq,
          raw,
          output: Buffer.from(output),
          outcomes,
        })
        return installPendingCmd(
          db, term, seq, output, outcomes, entryHash,
        )
      }

      const announceOutcome = node._monsterAnnounceOutcome.bind(node)
      node._monsterAnnounceOutcome = (cmdSeq, digest) => {
        outcomeCalls.push({
          id: node.id,
          cmdSeq,
          digest: Buffer.from(digest),
        })
        return announceOutcome(cmdSeq, digest)
      }

      bus.register(node)
      allNodes.push(node)
      return node
    })
    return {
      nodes,
      calls,
      patchsetCalls,
      outcomeCalls,
      messages,
    }
  }

  t.teardown(() => {
    closeNodesQuietly(allNodes)
    reset()
  })

  return { paths, build, reset }
}

const openAndElect = async (cluster) => {
  openNodes(cluster.nodes)
  cluster.nodes[0]._voteForSelf()
  await withTimeout(ready(cluster.nodes, null, true), 'cluster leader readiness')
  const leader = leaders(cluster.nodes)[0]
  await waitFor(
    () => cluster.nodes.every((node) => node._applySeq >= leader._commitSeq),
    'cluster no-op application',
  )
  return leader
}

const applyInitialNoop = async (node, term=0n) => {
  const [result] = await node._monsterApply(node, [null], [0n], [term])
  return result
}

const pendingAt = (node) => {
  const row = node.db.prepare(`
    SELECT pending_cmd_seq, pending_local_digest
    FROM monsterft_meta WHERE id = 1
  `).get()
  if (row.pending_cmd_seq === null) { return null }
  return {
    cmdSeq: row.pending_cmd_seq,
    localDigest: Buffer.from(row.pending_local_digest),
  }
}

const cachedPendingAt = (node) => {
  const pending = node._monsterPendingCommand
  if (pending === null) { return null }
  return {
    cmdSeq: pending.cmdSeq,
    localDigest: Buffer.from(pending.localDigest),
  }
}

const digestEnvelope = (term, cmdSeq, patchset, outcomes) => {
  const wire = outcomes.map((outcome) => {
    if (outcome.status === 'fulfilled') {
      return [0, outcome.result]
    }
    const { message, code, sqlCode } = outcome.error
    return [1, message, code, sqlCode]
  })
  return crypto.createHash('sha256')
    .update(pack([term, cmdSeq, patchset, wire]))
    .digest()
}

const metadataAtPath = (databasePath) => {
  const db = new DatabaseSync(`${databasePath}2`, {
    readOnly: true,
    readBigInts: true,
  })
  try {
    const row = db.prepare(`
      SELECT applied_seq, applied_entry_hash, pending_cmd_seq,
             pending_local_digest, repair_state
      FROM monsterft_meta
      WHERE id = 1
    `).get()
    return {
      appliedSeq: row.applied_seq,
      appliedEntryHash: row.applied_entry_hash === null
        ? null
        : Buffer.from(row.applied_entry_hash),
      pending: row.pending_cmd_seq === null
        ? null
        : {
            cmdSeq: row.pending_cmd_seq,
            localDigest: Buffer.from(row.pending_local_digest),
          },
      repairState: row.repair_state,
    }
  } finally {
    db.close()
  }
}

const raftRecordAtPath = (databasePath, seq) => {
  const db = new DatabaseSync(databasePath, {
    readOnly: true,
    readBigInts: true,
  })
  try {
    const { entry } = db.prepare(`
      SELECT entry FROM raft_log WHERE seq = ?
    `).get(seq)
    return unpack(Buffer.from(entry).subarray(8))
  } finally {
    db.close()
  }
}

const headRecord = (node) => unpack(node.head)

test('MonsterFt classifies illegal SYNC entries as log corruption', (t) => {
  const node = { nodes: ids, quorum: 2 }
  const valid = {
    cmdSeq: 0n,
    quorum: true,
    agree: ['1', '2'],
    disagree: [],
    digest: Buffer.alloc(32),
  }
  const invalid = [
    [{ ...valid, cmdSeq: -1n }, 'SYNC entry cmdSeq is illegal'],
    [{ ...valid, quorum: 1 }, 'SYNC entry quorum is illegal'],
    [{ ...valid, agree: ['1'] }, 'SYNC entry agree has illegal length'],
    [{ ...valid, agree: ['2', '1'] }, 'SYNC entry agree has illegal items'],
    [{ ...valid, disagree: null },
      'SYNC entry disagree has illegal length'],
    [{ ...valid, disagree: ['1', '1'] },
      'SYNC entry disagree has illegal items'],
    [{ ...valid, quorum: false, agree: ['1'] },
      'SYNC entry quorum:false agree must be empty'],
    [{ ...valid, digest: Buffer.alloc(31) },
      'SYNC entry digest is illegal'],
  ]

  for (const [entry, message] of invalid) {
    let err = null
    try {
      validateSyncEntry(node, entry)
    } catch (failure) {
      err = failure
    }
    t.ok(err instanceof ErrorWithCode, `${message} throws ErrorWithCode`)
    t.equal(err.message, message, `${message} preserves its message`)
    t.equal(err.code, LOG_CORRUPT, `${message} uses LOG_CORRUPT`)
    t.equal(err.sqlCode, null, `${message} has no SQLite error code`)
  }
  t.end()
})

test('MonsterFt classifies a SYNC without its command as log corruption',
  async (t) => {
    const target = {
      nodes: ids,
      quorum: 2,
      _monsterRunDbGetCmd: async () => null,
    }
    const err = await rejects(
      t,
      MonsterFt.prototype._monsterApplySync.call(target, {
        cmdSeq: 0n,
        quorum: false,
        agree: [],
        disagree: [],
      }, 0n, Buffer.alloc(32)),
      /^SYNC entry is missing CMD$/,
      'a SYNC without its command is rejected',
    )
    t.ok(err instanceof ErrorWithCode,
      'the missing SYNC command throws ErrorWithCode')
    t.equal(err.code, LOG_CORRUPT,
      'the missing SYNC command uses LOG_CORRUPT')
    t.equal(err.sqlCode, null,
      'the missing SYNC command has no SQLite error code')
  })

test('MonsterFt classifies an illegal entry type as log corruption',
  async (t) => {
    const err = await rejects(
      t,
      MonsterFt.prototype._monsterApplyEntry.call(
        {}, { type: 'invalid' }, 0n, 0n, false, Buffer.alloc(32),
      ),
      /^entry type is illegal$/,
      'an illegal entry type is rejected',
    )
    t.ok(err instanceof ErrorWithCode,
      'an illegal entry type throws ErrorWithCode')
    t.equal(err.code, LOG_CORRUPT,
      'an illegal entry type uses LOG_CORRUPT')
    t.equal(err.sqlCode, null,
      'an illegal entry type has no SQLite error code')
  })

test('MonsterFt normalizes DB2 rollback failures', async (t) => {
  const primary = new ErrorWithCode('primary failure', LOG_CORRUPT)
  const rollbackFailure = new Error('injected rollback failure')
  const fatals = []
  const target = {
    _closing: false,
    _throwIfClosing() {},
    _monsterFatalError(err) {
      fatals.push(err)
      return err
    },
  }
  const db = {
    exec(sql) {
      if (sql === 'ROLLBACK') { throw rollbackFailure }
    },
  }

  const err = await rejects(
    t,
    MonsterFt.prototype._monsterWriteTx.call(target, db, () => {
      throw primary
    }),
    /^primary failure$/,
    'the transaction preserves its primary failure',
  )
  t.equal(err, primary, 'the primary failure keeps its identity')
  t.equal(fatals.length, 1, 'the rollback failure is reported once')
  t.ok(fatals[0] instanceof ErrorWithCode,
    'the rollback failure uses ErrorWithCode')
  t.equal(fatals[0].message,
    'DB2 rollback injected rollback failure',
    'the rollback failure has DB2 context')
  t.equal(fatals[0].code, SQLITE_ERROR,
    'the rollback failure uses SQLITE_ERROR')
  t.equal(fatals[0].sqlCode, null,
    'the synthetic rollback failure has no SQLite error code')
})

test('MonsterFt normalizes DB2 repair failures', async (t) => {
  const repairFailure = new Error('injected repair failure')
  const fatals = []
  const target = {
    state: 'leader',
    _toFollower() {},
    _monsterRunDb: async (fn) => fn({}),
    _throwIfClosing() {},
    _monsterWriteRepair() {
      throw repairFailure
    },
    _monsterFatalError(err) {
      fatals.push(err)
      return err
    },
  }

  const err = await rejects(
    t,
    MonsterFt.prototype._monsterFenceLeader.call(target),
    /^DB2 fence injected repair failure$/,
    'the repair failure has DB2 context',
  )
  t.ok(err instanceof ErrorWithCode,
    'the repair failure uses ErrorWithCode')
  t.equal(err.code, SQLITE_ERROR,
    'the repair failure uses SQLITE_ERROR')
  t.equal(err.sqlCode, null,
    'the synthetic repair failure has no SQLite error code')
  t.deepEqual(fatals, [err],
    'fatal reporting receives the normalized repair failure')
})

test('MonsterFt uses one constrained singleton bookkeeping table', (t) => {
  const fixture = makeFixture(t, 'schema')
  fixture.reset()
  const cluster = fixture.build()
  const node = cluster.nodes[0]
  node.open()

  const objects = node.db.prepare(`
    SELECT type, name
    FROM sqlite_schema
    WHERE name LIKE 'monsterft_%'
    ORDER BY type, name
  `).all().map((row) => ({ ...row }))
  t.deepEqual(objects, [
    { type: 'table', name: 'monsterft_meta' },
  ], 'DB2 has only singleton metadata Monster bookkeeping')
  t.equal(pendingAt(node), null, 'fresh metadata has no pending CMD')
  t.equal(cachedPendingAt(node), null, 'fresh cache has no pending CMD')
  t.notOk('_monsterCurrentCommand' in node,
    'fresh nodes have no leader-current command field')
  t.notOk('_monsterApplyingCommand' in node,
    'fresh nodes have no transient applying-command field')

  for (const state of [1n, 2n]) {
    const updated = node.db.prepare(`
      UPDATE monsterft_meta SET repair_state = ? WHERE id = 1
    `).run(state)
    t.equal(updated.changes, 1n,
      `fresh metadata accepts repair state ${state}`)
    t.equal(node.db.prepare(`
      SELECT repair_state FROM monsterft_meta WHERE id = 1
    `).get().repair_state, state,
    `repair state ${state} is stored exactly`)
  }
  node.db.prepare(`
    UPDATE monsterft_meta SET repair_state = 0 WHERE id = 1
  `).run()

  const invalid = [
    {
      sql: `UPDATE monsterft_meta SET repair_state = 3 WHERE id = 1`,
      name: 'an unknown repair state is rejected',
    },
    {
      sql: `UPDATE monsterft_meta
            SET applied_seq = 0, applied_entry_hash = zeroblob(32),
                pending_cmd_seq = 0, pending_local_digest = NULL
            WHERE id = 1`,
      name: 'half-present pending state is rejected',
    },
    {
      sql: `UPDATE monsterft_meta
            SET applied_seq = 0, applied_entry_hash = zeroblob(32),
                pending_cmd_seq = -1, pending_local_digest = zeroblob(32)
            WHERE id = 1`,
      name: 'a negative pending sequence is rejected',
    },
    {
      sql: `UPDATE monsterft_meta
            SET applied_seq = 0, applied_entry_hash = zeroblob(32),
                pending_cmd_seq = 0, pending_local_digest = zeroblob(31)
            WHERE id = 1`,
      name: 'a non-digest pending value is rejected',
    },
    {
      sql: `UPDATE monsterft_meta
            SET applied_seq = 0, applied_entry_hash = zeroblob(32),
                pending_cmd_seq = 1, pending_local_digest = zeroblob(32)
            WHERE id = 1`,
      name: 'pending state cannot be ahead of the checkpoint',
    },
    {
      sql: `UPDATE monsterft_meta
            SET applied_seq = 0, applied_entry_hash = NULL
            WHERE id = 1`,
      name: 'an applied checkpoint requires a digest',
    },
  ]
  for (const { sql, name } of invalid) {
    t.throws(() => node.db.exec(sql), /constraint/i, name)
  }
  t.deepEqual({ ...node.db.prepare(`
    SELECT applied_seq, applied_entry_hash, pending_cmd_seq,
           pending_local_digest, repair_state
    FROM monsterft_meta WHERE id = 1
  `).get() }, {
    applied_seq: -1n,
    applied_entry_hash: null,
    pending_cmd_seq: null,
    pending_local_digest: null,
    repair_state: 0n,
  }, 'failed constraint checks preserve the seeded metadata row')
  t.end()
})

test('MonsterFt classifies failed metadata mutations as SQLite errors',
  async (t) => {
    const fixture = makeFixture(t, 'metadata-mutation-counts')
    fixture.reset()
    const { nodes } = fixture.build()
    const node = nodes[0]
    node.open()
    node.db.prepare('DELETE FROM monsterft_meta WHERE id = 1').run()

    const assertSqliteError = (err, message, name) => {
      t.ok(err instanceof ErrorWithCode, `${name} throws ErrorWithCode`)
      t.equal(err.message, message, `${name} preserves its message`)
      t.equal(err.code, SQLITE_ERROR, `${name} uses SQLITE_ERROR`)
      t.equal(err.sqlCode, null, `${name} has no native SQLite code`)
    }
    const capture = (fn) => {
      try {
        fn()
        return null
      } catch (err) {
        return err
      }
    }

    assertSqliteError(capture(() => node._monsterAdvanceApplied(
      node.db, 0n, Buffer.alloc(32),
    )), 'DB2 advance applied update count not one',
    'checkpoint failure')
    assertSqliteError(capture(() => node._monsterInstallPendingCmd(
      node.db, 0n, 0n, Buffer.alloc(0), [], Buffer.alloc(32),
    )), 'DB2 install pending CMD update count not one',
    'pending-command failure')
    assertSqliteError(capture(() => node._monsterWriteRepair(
      node.db, 1,
    )), 'DB2 write repair update count not one', 'repair-write failure')

    node._monsterRunDbGetCmd = async () => ({
      cmdSeq: 0n,
      localDigest: Buffer.alloc(32),
    })
    const decisionErr = await rejects(
      t,
      node._monsterApplySync({
        cmdSeq: 0n,
        quorum: false,
        agree: [],
        disagree: [],
      }, 0n, Buffer.alloc(32)),
      /^DB2 write CMD decision update count not one$/,
      'decision failure rejects with its message',
    )
    assertSqliteError(
      decisionErr,
      'DB2 write CMD decision update count not one',
      'decision failure',
    )
  })

test('MonsterFt metadata mutations retain native SQLite context', (t) => {
  const native = new Error('injected SQLite failure')
  native.code = 'ERR_SQLITE_ERROR'
  native.errcode = 19
  const db = {
    prepare() {
      throw native
    },
  }
  const calls = [
    [
      'advance applied',
      () => MonsterFt.prototype._monsterAdvanceApplied.call(
        {}, db, 0n, Buffer.alloc(32)),
      'DB2 advance applied injected SQLite failure',
    ],
    [
      'install pending CMD',
      () => MonsterFt.prototype._monsterInstallPendingCmd.call(
        {}, db, 0n, 0n, Buffer.alloc(0), [], Buffer.alloc(32)),
      'DB2 install pending CMD injected SQLite failure',
    ],
    [
      'write command decision',
      () => MonsterFt.prototype._monsterWriteCmdDecision.call(
        {}, db, 0n, Buffer.alloc(32), 0, 0n),
      'DB2 write CMD decision injected SQLite failure',
    ],
    [
      'write repair',
      () => MonsterFt.prototype._monsterWriteRepair.call({}, db, 1),
      'DB2 write repair injected SQLite failure',
    ],
  ]

  for (const [name, run, message] of calls) {
    let err = null
    try {
      run()
    } catch (failure) {
      err = failure
    }
    t.ok(err instanceof ErrorWithCode, `${name} uses ErrorWithCode`)
    t.equal(err.message, message, `${name} adds operation context`)
    t.equal(err.code, SQLITE_ERROR, `${name} uses SQLITE_ERROR`)
    t.equal(err.sqlCode, 19, `${name} retains the native SQLite code`)
  }
  t.end()
})

test('MonsterFt hashes one complete command envelope per CMD', async (t) => {
  const fixture = makeFixture(t, 'command-envelope')
  await fixture.reset()
  const cluster = fixture.build({
    opts: { applyMax: 1 },
  })
  const leader = await openAndElect(cluster)

  const singleSynced = nextEvent(leader, 'sync')
  const [singleSeq, singleResult] = await leader.append(toBuf({
    key: 'single',
    value: 1,
  }))
  await singleSynced
  t.deepEqual(singleResult, { key: 'single', value: 1 },
    'the single item retains its public result')

  const batchItems = [
    { key: 'batch-a', value: 2 },
    { key: 'batch-b', value: 3 },
    { key: 'batch-c', value: 4 },
  ]
  const batchSynced = nextEvent(leader, 'sync')
  const [batchSeq, batchResults] = await leader.appendBatch(
    batchItems.map(toBuf),
  )
  const { syncSeq: batchSync } = await batchSynced
  t.equal(batchResults.length, 3, 'the batch retains all item results')
  await waitFor(() => cluster.nodes.every((node) => node._applySeq >= batchSync),
    'metadata commands applied')

  for (const node of cluster.nodes) {
    const single = cluster.calls.filter((entry) => {
      return entry.id === node.id && entry.seq === singleSeq
    })
    t.equal(single.length, 1, `node ${node.id} runs one single-item callback`)

    const batch = cluster.calls.filter((entry) => {
      return entry.id === node.id && entry.seq === batchSeq
    })
    t.deepEqual(batch.map((entry) => entry.index), [0, 1, 2],
      `node ${node.id} receives all batch indexes`)

    const patchsets = cluster.patchsetCalls.filter((call) => call.id === node.id)
    t.deepEqual(patchsets.map((call) => call.seq), [singleSeq, batchSeq],
      `node ${node.id} captures one patchset per CMD`)
    t.ok(patchsets.every((call) => call.raw.length > 0),
      `node ${node.id} captures both complete CMD transitions`)
    for (const call of patchsets) {
      const expected = call.seq === singleSeq
        ? [{ key: 'single', value: 1 }]
        : batchItems
      t.deepEqual(
        call.outcomes.map((outcome) => outcome.result),
        expected,
        `node ${node.id} retains raw results for CMD ${call.seq}`,
      )
      const announced = cluster.outcomeCalls.find((outcome) => {
        return outcome.id === node.id && outcome.cmdSeq === call.seq
      })
      t.ok(announced,
        `node ${node.id} announces the digest for CMD ${call.seq}`)
      t.deepEqual(
        announced.digest,
        digestEnvelope(call.term, call.seq, call.output, call.outcomes),
        `node ${node.id} hashes one framed envelope for CMD ${call.seq}`,
      )
    }
    const sample = patchsets[1]
    const announced = cluster.outcomeCalls.find((outcome) => {
      return outcome.id === node.id && outcome.cmdSeq === sample.seq
    })
    t.notDeepEqual(
      announced.digest,
      digestEnvelope(
        sample.term + 1n, sample.seq, sample.output, sample.outcomes,
      ),
      `node ${node.id} command digest binds the Raft term`,
    )
    t.notDeepEqual(
      announced.digest,
      digestEnvelope(
        sample.term, sample.seq + 1n, sample.output, sample.outcomes,
      ),
      `node ${node.id} command digest binds the CMD sequence`,
    )
    t.equal(pendingAt(node), null,
      `node ${node.id} clears durable pending state after both SYNCs`)
    t.equal(cachedPendingAt(node), null,
      `node ${node.id} clears cached pending state after both SYNCs`)
  }
})

test('MonsterFt validates structured forwarded results directly', async (t) => {
  const fixture = makeFixture(t, 'structured-results')
  await fixture.reset()
  const cluster = fixture.build()
  const leader = await openAndElect(cluster)
  const follower = cluster.nodes.find((node) => node !== leader)
  const items = [
    toBuf({ key: 'wire-success', value: 1 }),
    toBuf({ key: 'wire-failure', value: 2 }),
  ]
  const value = { key: 'wire-success', value: 1 }
  const fwdCmdTerm = follower.term
  const response = {
    type: 'ack',
    term: fwdCmdTerm + 1n,
    cmdSeq: leader.seq + 1n,
    results: [
      [0, value],
      [1, 'planned wire failure', 17, 19],
    ],
  }
  const malformed = [
    { ...response, term: null },
    { ...response, cmdSeq: -1n },
    { ...response, results: Buffer.from(pack(response.results)) },
    { ...response, results: response.results.slice(0, 1) },
    { ...response, results: [[0], response.results[1]] },
    { ...response, results: [response.results[0], [2, 'bad', 17, null]] },
    { ...response, results: [response.results[0], [1, 'bad', {}, null]] },
    { ...response, results: [response.results[0], [1, 'bad', 17, {}]] },
  ]

  const sendAndAwaitCmdAck = follower._monsterSendAndAwaitCmdAck
  follower._monsterSendAndAwaitCmdAck = async (to, msg, validate) => {
    t.equal(to, leader.id,
      'the FWD_CMD waits for its original destination')
    t.equal(msg.term, fwdCmdTerm,
      'the forwarded request retains its admission term')
    t.equal(validate(response), true,
      'the receiver accepts structured outcomes from a later response term')
    malformed.forEach((candidate, index) => {
      t.equal(validate(candidate), false,
        `the receiver rejects malformed structured result ${index + 1}`)
    })
    return response
  }

  let cmdSeq = null
  let outcomes = null
  try {
    [cmdSeq, outcomes] = await follower._monsterAppendOrFwdCmd(items)
  } finally {
    follower._monsterSendAndAwaitCmdAck = sendAndAwaitCmdAck
  }

  t.equal(cmdSeq, response.cmdSeq,
    'the validated response retains its command sequence')
  t.equal(outcomes[0].status, 'fulfilled',
    'the structured success becomes a fulfilled public outcome')
  t.equal(outcomes[0].value, value,
    'direct delivery retains the raw fulfilled result')
  t.equal(outcomes[1].status, 'rejected',
    'the structured failure becomes a rejected public outcome')
  t.ok(outcomes[1].reason instanceof ErrorWithCode,
    'the structured failure becomes an ErrorWithCode')
  t.equal(outcomes[1].reason.message, 'planned wire failure',
    'the structured failure retains its message')
  t.equal(outcomes[1].reason.code, 17,
    'the structured failure retains its code')
  t.equal(outcomes[1].reason.sqlCode, 19,
    'the structured failure retains its sqlCode')
  t.equal(response.results[1].includes('RangeError'), false,
    'the structured failure carries no error name')
  t.notOk(Object.hasOwn(outcomes[1].reason, 'cmdSeq'),
    'the structured failure has no command context')
  t.notOk(Object.hasOwn(outcomes[1].reason, 'index'),
    'the structured failure has no item context')
})

test('MonsterFt uses one Session across every CMD item', async (t) => {
  const fixture = makeFixture(t, 'session-order')
  await fixture.reset()
  const events = []
  let watched = null
  const cluster = fixture.build({
    apply: (db, node, buf, term, seq, index) => {
      if (node === watched) {
        events.push({ action: 'callback', seq, index })
      }
      return defaultApply(db, node, buf)
    },
  })
  const leader = await openAndElect(cluster)
  watched = leader
  const db = leader.db
  const createSession = db.createSession.bind(db)
  db.createSession = (options) => {
    events.push({ action: 'create', options })
    const session = options === undefined
      ? createSession()
      : createSession(options)
    const patchset = session.patchset.bind(session)
    session.patchset = () => {
      events.push({ action: 'patchset' })
      return patchset()
    }
    const close = session.close.bind(session)
    session.close = () => {
      events.push({ action: 'close' })
      return close()
    }
    return session
  }
  const installPendingCmd = leader._monsterInstallPendingCmd.bind(leader)
  leader._monsterInstallPendingCmd = (
    currentDb, term, seq, patchset, outcomes, entryHash,
  ) => {
    events.push({ action: 'write', seq })
    return installPendingCmd(
      currentDb, term, seq, patchset, outcomes, entryHash,
    )
  }

  try {
    const singleSynced = nextEvent(leader, 'sync')
    const [singleSeq] = await leader.append(toBuf({
      key: 'session-order-single',
      value: 11,
    }))
    await singleSynced
    const batchSynced = nextEvent(leader, 'sync')
    const [batchSeq] = await leader.appendBatch([
      toBuf({ key: 'session-order-a', value: 12 }),
      toBuf({ key: 'session-order-b', value: 13 }),
      toBuf({ key: 'session-order-c', value: 14 }),
    ])
    await batchSynced

    const actions = events.map(({ action, seq, index }) => {
      if (action === 'callback') { return `${action}:${seq}:${index}` }
      if (seq !== undefined) { return `${action}:${seq}` }
      return action
    })
    t.deepEqual(actions, [
      'create',
      `callback:${singleSeq}:0`,
      'patchset',
      'close',
      `write:${singleSeq}`,
      'create',
      `callback:${batchSeq}:0`,
      `callback:${batchSeq}:1`,
      `callback:${batchSeq}:2`,
      'patchset',
      'close',
      `write:${batchSeq}`,
    ], 'each CMD uses one Session around its complete ordered transition')
  } finally {
    db.createSession = createSession
    leader._monsterInstallPendingCmd = installPendingCmd
  }

  const sessions = events.filter(({ action }) => action === 'create')
  t.equal(sessions.length, 2, 'the two CMDs create exactly two Sessions')
  t.ok(sessions.every(({ options }) => {
    return options?.db === 'main' && options.table === undefined
  }), 'each CMD Session observes all eligible main-database tables')
})

test('MonsterFt captures only the net application transition', async (t) => {
  const fixture = makeFixture(t, 'net-transition')
  await fixture.reset()
  const cluster = fixture.build({
    apply: (db, node, buf) => {
      const command = toObj(buf)
      if (command.action === 'delete') {
        db.prepare(`
          DELETE FROM monster_metadata_items WHERE key = ?
        `).run(command.key)
      } else {
        write(db, command.key, command.value)
      }
      if (command.action === 'reject') {
        throw new Error('rollback this item')
      }
      return command
    },
  })
  const leader = await openAndElect(cluster)
  const synced = nextEvent(leader, 'sync')
  const [cmdSeq, outcomes] = await leader.appendBatch([
    toBuf({ action: 'write', key: 'transient', value: 1 }),
    toBuf({ action: 'delete', key: 'transient' }),
    toBuf({ action: 'write', key: 'kept', value: 2 }),
    toBuf({ action: 'reject', key: 'rolled-back', value: 3 }),
  ])

  t.deepEqual(outcomes.map(({ status }) => status), [
    'fulfilled',
    'fulfilled',
    'fulfilled',
    'rejected',
  ], 'the batch retains every ordered callback outcome')
  const captured = cluster.patchsetCalls.find((call) => {
    return call.id === leader.id && call.seq === cmdSeq
  })
  t.ok(captured, 'the leader captures one patchset for the batch')

  const replay = new DatabaseSync(':memory:', { readBigInts: true })
  try {
    replay.exec(`
      CREATE TABLE monster_metadata_items (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL
      ) STRICT;
    `)
    t.equal(replay.applyChangeset(captured.raw), true,
      'the CMD patchset applies without Monster bookkeeping tables')
    t.deepEqual(replay.prepare(`
      SELECT key, value FROM monster_metadata_items ORDER BY key
    `).all().map((row) => ({ ...row })), [
      { key: 'kept', value: 2n },
    ], 'the patchset contains only the successful net application state')
  } finally {
    replay.close()
  }
  await synced
})

test('MonsterFt hashes matching empty CMD patchsets normally', async (t) => {
  const fixture = makeFixture(t, 'matching-empty')
  await fixture.reset()
  const cluster = fixture.build({
    patchsetTransform: ({ patchset }) => {
      t.ok(patchset.length > 0,
        'the test substitutes for a real CMD patchset')
      return Buffer.alloc(0)
    },
  })
  const leader = await openAndElect(cluster)

  const synced = nextEvent(leader, 'sync')
  const [cmdSeq, result] = await leader.append(toBuf({
    key: 'empty',
    value: 5,
  }))
  const { syncSeq } = await synced
  t.deepEqual(result, { key: 'empty', value: 5 },
    'matching empty CMD patchsets still certify')
  await waitFor(() => cluster.nodes.every((node) => node._applySeq >= syncSeq),
    'matching empty CMD patchset SYNC')

  const sync = headRecord(leader)
  t.equal(sync.quorum, true,
    'the empty patchset participates in a quorum SYNC')
  t.deepEqual(sync.disagree, [],
    'matching empty CMD patchsets record no disagreement')
  t.ok(cluster.patchsetCalls.every((call) => call.output.length === 0),
    'every empty CMD patchset reached hashing without rejection')
  const digests = cluster.outcomeCalls
    .filter((call) => call.cmdSeq === cmdSeq)
    .map((call) => call.digest.toString('hex'))
  t.equal(digests.length, 3,
    'every node announces its matching empty CMD digest')
  t.equal(new Set(digests).size, 1,
    'matching empty CMD patchsets produce one command digest')
  t.ok(cluster.nodes.every((node) => !('repairRequired' in node)),
    'MonsterFt exposes no public repair state')
})

test('MonsterFt locally fences a known CMD patchset disagreement', async (t) => {
  const fixture = makeFixture(t, 'patchset-minority')
  await fixture.reset()
  const releaseSecond = deferred()
  t.teardown(() => releaseSecond.resolve())
  const cluster = fixture.build({
    patchsetTransform: ({ node, patchset }) => {
      return node.id === '3' ? Buffer.alloc(0) : patchset
    },
    apply: async (db, node, buf) => {
      const result = defaultApply(db, node, buf)
      if (node.id === '2') { await releaseSecond.promise }
      return result
    },
  })
  const leader = await openAndElect(cluster)
  const cmdSeq = leader.seq + 1n
  const synced = nextEvent(leader, 'sync')
  const appending = leader.append(toBuf({ key: 'minority', value: 6 }))
  appending.catch(noop)

  try {
    await waitFor(() => {
      const reports = leader._monsterPendingReports
      return reports?.has(leader.id) && reports.has('3')
    }, 'leader and minority patchset reports')
    t.equal(headRecord(leader).type, 'cmd',
      'different patchset reports wait while a quorum remains possible')
  } finally {
    releaseSecond.resolve()
  }

  const tuple = await appending
  const [actualSeq, result] = tuple
  t.equal(actualSeq, cmdSeq, 'the expected CMD receives the patchset disagreement')
  t.deepEqual(result, { key: 'minority', value: 6 },
    'the leader inside the patchset quorum returns its application result')
  t.equal(tuple.length, 2, 'the partial-disagreement result omits syncSeq')
  const { syncSeq } = await synced
  await waitFor(() => cluster.nodes.every((node) => node._applySeq >= syncSeq),
    'patchset disagreement SYNC propagation')

  const sync = headRecord(leader)
  t.deepEqual(sync, {
    type: 'sync',
    cmdSeq,
    quorum: true,
    digest: sync.digest,
    agree: ['1', '2'],
    disagree: ['3'],
  }, 'SYNC identifies both patchset agreement sets exactly')
  t.equal(pendingAt(leader), null,
    'the durable singleton drops the decided CMD after SYNC')
  t.equal(cachedPendingAt(leader), null,
    'the authoritative cache drops the decided CMD after SYNC')
  t.equal(leader._applySeq, syncSeq,
    'SYNC advances the in-memory checkpoint after clearing pending state')
  await waitFor(() => !cluster.nodes[2].isOpen,
    'the listed disagreeing member closes')
  t.ok(cluster.nodes[0].isOpen && cluster.nodes[1].isOpen,
    'the listed agreeing quorum stays healthy')
  t.notOk(cluster.nodes[2].isOpen,
    'the listed disagreeing member takes the terminal repair path')
  t.equal(metadataAtPath(fixture.paths.get('3')).repairState, 2n,
    'the listed disagreeing member stores terminal repair state two')
  t.equal(cluster.nodes[2]._monsterRepairState, 2,
    'the listed disagreeing member publishes terminal state two in memory')
  t.notEqual(
    cluster.patchsetCalls.find(({ id }) => id === leader.id).output
      .toString('hex'),
    cluster.patchsetCalls.find(({ id }) => id === '3').output
      .toString('hex'),
    'the CMD patchset changes the command digest input',
  )
})

test('MonsterFt requires repair when CMD patchsets prevent a digest quorum', async (t) => {
  const fixture = makeFixture(t, 'patchset-no-quorum')
  await fixture.reset()
  const cluster = fixture.build({
    patchsetTransform: ({ node }) => Buffer.from([Number(node.id)]),
  })
  const leader = await openAndElect(cluster)
  const cmdSeq = leader.seq + 1n

  const syncEvents = new Map(cluster.nodes.map((node) => [node.id, []]))
  cluster.nodes.forEach((node) => {
    node.on('sync', (event) => syncEvents.get(node.id).push(event))
  })
  const err = await rejectsCode(t, leader.append(toBuf({
    key: 'no-quorum',
    value: 7,
  })), REPAIR_QUORUM_IMPOSSIBLE,
  'three patchsets reject with the repair-required code')
  t.notOk(Object.hasOwn(err, 'cmdSeq'),
    'the no-quorum result has no command context')
  t.notOk(Object.hasOwn(err, 'syncSeq'),
    'the no-quorum result has no SYNC context')
  t.notOk(Object.hasOwn(err, 'ambiguous'),
    'the repair code carries the failure category')
  const syncSeq = cmdSeq + 1n
  await waitFor(() => cluster.nodes.every((node) => {
    return node._applySeq >= syncSeq && syncEvents.get(node.id).length === 1
  }), 'quorum-impossible SYNC propagation')
  const databasePath = fixture.paths.get(leader.id)
  const metadata = metadataAtPath(databasePath)
  t.deepEqual(raftRecordAtPath(databasePath, syncSeq), {
    type: 'sync',
    cmdSeq,
    quorum: false,
    agree: [],
    disagree: ids,
  }, 'patchset disagreement records an exact quorum:false SYNC')
  t.equal(metadata.pending, null,
    'the live-fence SYNC atomically clears the pending singleton')
  t.equal(metadata.appliedSeq, syncSeq,
    'the live-fence SYNC atomically advances the durable checkpoint')
  t.equal(metadata.repairState, 1n,
    'the live-fence SYNC atomically persists repair state one')
  for (const node of cluster.nodes) {
    t.equal(metadataAtPath(fixture.paths.get(node.id)).repairState, 1n,
      `node ${node.id} durably stores repair state one`)
    t.equal(node._monsterRepairState, 1,
      `node ${node.id} caches repair state one`)
    t.deepEqual(syncEvents.get(node.id), [{
      cmdSeq,
      syncSeq,
      quorum: false,
      agree: [],
      disagree: ids,
    }], `node ${node.id} emits the quorum:false SYNC`)
  }
  t.ok(cluster.nodes.every((node) => node.isOpen),
    'quorum-impossible application leaves connected members online')
  t.equal(leaders(cluster.nodes)[0], leader,
    'quorum-impossible application retains the current leader')

  const patchsets = cluster.patchsetCalls.map(({ output }) => {
    return output.toString('hex')
  })
  t.equal(new Set(patchsets).size, 3,
    'the three substituted CMD patchsets produce distinct command digests')
})

test('MonsterFt requires one quorum to match the complete batch vector',
  async (t) => {
    const fixture = makeFixture(t, 'whole-batch-quorum')
    await fixture.reset()
    const cluster = fixture.build({
      apply: (db, node, buf, term, seq, index) => {
        const result = defaultApply(db, node, buf)
        const variant = index === 0
          ? (node.id === '3' ? 'right' : 'left')
          : (node.id === '1' ? 'left' : 'right')
        return { ...result, variant }
      },
    })
    const leader = await openAndElect(cluster)
    const cmdSeq = leader.seq + 1n
    const syncEvents = []
    leader.on('sync', (event) => syncEvents.push(event))
    const err = await rejectsCode(t, leader.appendBatch([
      toBuf({ key: 'subset-a', value: 15 }),
      toBuf({ key: 'subset-b', value: 16 }),
    ]), REPAIR_QUORUM_IMPOSSIBLE,
    'per-item quorums cannot certify different node subsets')
    t.notOk(Object.hasOwn(err, 'cmdSeq'),
      'the indivisible no-quorum batch has no command context')
    t.notOk(Object.hasOwn(err, 'syncSeq'),
      'the indivisible no-quorum batch has no SYNC context')
    t.notOk(Object.hasOwn(err, 'ambiguous'),
      'the repair code carries the batch failure category')
    const syncSeq = cmdSeq + 1n
    await waitFor(() => leader._applySeq >= syncSeq && syncEvents.length === 1,
      'indivisible batch live-fence SYNC')

    const firstVariants = cluster.calls.filter(({ index, command }) => {
      return index === 0 && command !== null
    })
      .map(({ id }) => id === '3' ? 'right' : 'left')
    const secondVariants = cluster.calls.filter(({ index }) => index === 1)
      .map(({ id }) => id === '1' ? 'left' : 'right')
    t.deepEqual(firstVariants.sort(), ['left', 'left', 'right'],
      'the first item has a matching quorum')
    t.deepEqual(secondVariants.sort(), ['left', 'right', 'right'],
      'the second item has a different matching quorum')
    const digests = cluster.patchsetCalls.map(({
      term, seq, output, outcomes,
    }) => {
      return digestEnvelope(term, seq, output, outcomes).toString('hex')
    })
    t.equal(new Set(digests).size, 3,
      'no two replicas match the complete ordered outcome vector')
    const databasePath = fixture.paths.get(leader.id)
    const metadata = metadataAtPath(databasePath)
    t.deepEqual(raftRecordAtPath(databasePath, syncSeq), {
      type: 'sync',
      cmdSeq,
      quorum: false,
      agree: [],
      disagree: ids,
    }, 'the indivisible batch receives one quorum:false decision')
    t.equal(metadata.pending, null,
      'the indivisible batch clears its pending singleton')
    t.equal(metadata.appliedSeq, syncSeq,
      'the indivisible batch persists its SYNC checkpoint')
    t.equal(metadata.repairState, 1n,
      'the indivisible batch persists live repair state one')
    t.deepEqual(syncEvents, [{
      cmdSeq,
      syncSeq,
      quorum: false,
      agree: [],
      disagree: ids,
    }], 'the leader emits the indivisible quorum:false decision')
    t.ok(leader.isOpen,
      'the indivisible quorum failure leaves the leader online')
  })

test('MonsterFt certifies an all-rejected batch from its identity and errors',
  async (t) => {
    const fixture = makeFixture(t, 'all-rejected')
    await fixture.reset()
    const cluster = fixture.build({
      apply: () => {
        const err = new Error('deterministic rejection')
        err.code = 'E_REJECTED'
        throw err
      },
    })
    const leader = await openAndElect(cluster)
    const synced = nextEvent(leader, 'sync')
    const tuple = await leader.appendBatch([
      toBuf({ key: 'rejected-a', value: 17 }),
      toBuf({ key: 'rejected-b', value: 18 }),
    ])
    const [, outcomes] = tuple
    const { syncSeq } = await synced
    await waitFor(() => cluster.nodes.every((node) => node._applySeq >= syncSeq),
      'all-rejected batch SYNC')

    t.equal(tuple.length, 2, 'the all-rejected batch result has two elements')
    t.deepEqual(outcomes.map(({ status }) => status),
      ['rejected', 'rejected'], 'every rejected item remains represented')
    t.ok(outcomes.every(({ reason }) => {
      return reason.message === 'deterministic rejection' &&
        reason.code === null &&
        reason.sqlCode === null &&
        !Object.hasOwn(reason, 'cmdSeq') &&
        !Object.hasOwn(reason, 'index') &&
        !Object.hasOwn(reason, 'syncSeq')
    }), 'the agreed rejections retain only stable application error fields')
    t.equal(headRecord(leader).quorum, true,
      'matching command identities and errors form a command-digest quorum')
    t.ok(cluster.patchsetCalls.every(({ raw }) => raw.length === 0),
      'the all-rejected batch has a genuinely empty application patchset')
    t.ok(cluster.patchsetCalls.every((call) => {
      const announced = cluster.outcomeCalls.find((outcome) => {
        return outcome.id === call.id && outcome.cmdSeq === call.seq
      })
      return announced !== undefined && announced.digest.equals(digestEnvelope(
        call.term, call.seq, call.output, call.outcomes,
      ))
    }), 'term, sequence, empty patchset, and errors form each announced digest')
    t.equal(leader.db.prepare(`
      SELECT COUNT(*) AS count FROM monster_metadata_items
    `).get().count, 0n, 'rejected items commit no application writes')
  })

test('MonsterFt closes before applying a CMD in either repair state',
  async (t) => {
    const cases = [
      { repairState: 1, expectedCode: REPAIR_QUORUM_IMPOSSIBLE },
      { repairState: 2, expectedCode: REPAIR_OUTSIDE_AGREEMENT },
    ]
    for (const { repairState, expectedCode } of cases) {
      const fixture = makeFixture(t, `committed-cmd-repair-${repairState}`)
      fixture.reset()
      const cluster = fixture.build()
      const node = cluster.nodes[0]
      node.open()
      await applyInitialNoop(node)
      node.db.prepare(`
        UPDATE monsterft_meta SET repair_state = ? WHERE id = 1
      `).run(repairState)
      node._monsterRepairState = repairState

      const before = metadataAtPath(fixture.paths.get(node.id))
      const callsBefore = cluster.calls.length
      const fatal = nextEvent(node, 'fatal')
      const record = Buffer.from(pack({
        type: 'cmd',
        items: [toBuf({ key: `illegal-state-${repairState}`, value: 40 })],
        matchIndex: ids.map(() => 0n),
      }))
      const applying = node._monsterApply(node, [record], [1n], [1n])
      applying.catch(noop)

      const fatalError = await withTimeout(
        fatal,
        `repair state ${repairState} committed CMD fatal`,
      )
      await applying.catch(noop)
      t.equal(fatalError.code, expectedCode,
        `repair state ${repairState} reports its numbered fatal code`)
      t.equal(node.isOpen, false,
        `repair state ${repairState} immediately closes on CMD dispatch`)
      t.equal(cluster.calls.length, callsBefore,
        `repair state ${repairState} does not invoke user apply for the CMD`)
      t.deepEqual(metadataAtPath(fixture.paths.get(node.id)), before,
        `repair state ${repairState} leaves the DB2 checkpoint unchanged`)
    }
  })

test('MonsterFt validates CMD matchIndex before application', async (t) => {
  const fixture = makeFixture(t, 'match-index-validation')
  await fixture.reset()
  const cluster = fixture.build()
  const node = cluster.nodes[0]
  node.open()
  await applyInitialNoop(node)
  const writeTx = node._monsterWriteTx.bind(node)
  let transactions = 0
  node._monsterWriteTx = (...args) => {
    transactions++
    return writeTx(...args)
  }
  const items = [toBuf({ key: 'invalid-match-index', value: 1 })]
  const invalid = [
    {
      name: 'missing',
      record: { type: 'cmd', items },
      pattern: /CMD entry matchIndex has illegal length/,
    },
    {
      name: 'wrong length',
      record: { type: 'cmd', items, matchIndex: [-1n, -1n] },
      pattern: /CMD entry matchIndex has illegal length/,
    },
    {
      name: 'non-sequence',
      record: { type: 'cmd', items, matchIndex: [-1n, -1n, -1] },
      pattern: /CMD entry matchIndex has illegal items/,
    },
    {
      name: 'equal sequence',
      record: { type: 'cmd', items, matchIndex: [-1n, -1n, 1n] },
      pattern: /CMD entry matchIndex has illegal items/,
    },
    {
      name: 'greater sequence',
      record: { type: 'cmd', items, matchIndex: [-1n, -1n, 2n] },
      pattern: /CMD entry matchIndex has illegal items/,
    },
    {
      name: 'empty items',
      record: { type: 'cmd', items: [], matchIndex: [-1n, -1n, -1n] },
      pattern: /CMD entry items must be a non-empty array/,
    },
    {
      name: 'non-buffer item',
      record: { type: 'cmd', items: ['item'], matchIndex: [-1n, -1n, -1n] },
      pattern: /CMD entry items must be buffers/,
    },
  ]

  for (const { name, record, pattern } of invalid) {
    const err = await rejects(
      t,
      node._monsterApply(node, [Buffer.from(pack(record))], [1n], [0n]),
      pattern,
      `${name} CMD is rejected`,
    )
    t.ok(err instanceof ErrorWithCode,
      `${name} CMD corruption throws ErrorWithCode`)
    t.equal(err.code, LOG_CORRUPT,
      `${name} CMD corruption uses LOG_CORRUPT`)
    t.equal(err.sqlCode, null,
      `${name} CMD corruption has no SQLite error code`)
  }
  t.deepEqual(cluster.calls, [{
    id: node.id,
    term: 0n,
    seq: 0n,
    index: 0,
    matchIndex: null,
    command: null,
  }],
  'the initial no-op invokes the application with its protocol values')
  t.equal(cluster.calls.length, 1,
    'invalid matchIndex data is rejected before the application callback')
  t.equal(transactions, 0,
    'invalid matchIndex data is rejected before opening a CMD transaction')
  t.equal(node.db.prepare(`
    SELECT applied_seq FROM monsterft_meta WHERE id = 1
  `).get().applied_seq, 0n,
  'invalid matchIndex data does not advance the durable checkpoint')
})

test('MonsterFt rolls back application work with a failed CMD transaction', async (t) => {
  const fixture = makeFixture(t, 'rollback')
  await fixture.reset()
  const cluster = fixture.build()
  const node = cluster.nodes[0]
  node.open()
  await applyInitialNoop(node)

  const record = Buffer.from(pack({
    type: 'cmd',
    items: [toBuf({ key: 'rollback', value: 9 })],
    matchIndex: ids.map(() => 0n),
  }))
  const installPendingCmd = node._monsterInstallPendingCmd.bind(node)
  const failure = new Error('injected command storage failure')
  const staleReport = Buffer.alloc(32, 1)
  node._monsterPendingReports.set('2', staleReport)
  node._monsterInstallPendingCmd = () => { throw failure }
  const failed = await rejects(t, node._monsterApply(node, [record], [1n], [0n]),
    /^injected command storage failure$/,
    'the post-apply storage failure aborts the CMD')
  t.equal(failed, failure, 'the failed CMD preserves the original error')

  t.equal(node.db.prepare(`
    SELECT COUNT(*) AS count FROM monster_metadata_items
  `).get().count, 0n, 'the failed transaction rolls back application writes')
  t.equal(pendingAt(node), null,
    'the failed transaction stores no durable pending command')
  t.equal(cachedPendingAt(node), null,
    'the failed transaction stores no cached pending command')
  t.equal(node._monsterPendingReports.get('2'), staleReport,
    'the failed transaction retains the prior report state')
  t.equal(node.db.prepare(`
    SELECT applied_seq FROM monsterft_meta WHERE id = 1
  `).get().applied_seq, 0n, 'the failed transaction does not advance the checkpoint')

  node._monsterInstallPendingCmd = installPendingCmd
  const [applied] = await node._monsterApply(node, [record], [1n], [0n])
  t.equal(node.db.prepare(`
    SELECT value FROM monster_metadata_items WHERE key = 'rollback'
  `).get().value, 9n, 'retry installs the application write')
  t.equal(node.db.prepare(`
    SELECT applied_seq FROM monsterft_meta WHERE id = 1
  `).get().applied_seq, 1n, 'retry advances the durable checkpoint')
  const expectedPending = {
    cmdSeq: 1n,
    localDigest: Buffer.from(applied.localDigest),
  }
  t.deepEqual(pendingAt(node), expectedPending,
    'retry atomically installs the durable pending command')
  t.deepEqual(cachedPendingAt(node), expectedPending,
    'retry installs matching authoritative cached state after COMMIT')
  t.equal(node._monsterPendingReports.size, 0,
    'the newly installed CMD starts with an empty report map')

  const prepare = node.db.prepare.bind(node.db)
  let pendingReads = 0
  node.db.prepare = (sql) => {
    if (/SELECT[\s\S]*pending_cmd_seq[\s\S]*FROM\s+monsterft_meta/i.test(sql)) {
      pendingReads++
    }
    return prepare(sql)
  }
  let command = null
  let pending = null
  try {
    command = await node._monsterRunDbGetCmd(1n)
    pending = await node._monsterRunDbGetCmd()
    node.onReceive('2', {
      type: 'monster_outcome_request',
      cmdSeq: 1n,
    })
    await waitFor(() => cluster.messages.some(({ from, to, msg }) => {
      return from === node.id && to === '2' &&
        msg.type === 'monster_outcome' && msg.cmdSeq === 1n
    }), 'OUTCOME_REQUEST response')
  } finally {
    node.db.prepare = prepare
  }
  t.equal(pendingReads, 0,
    'runtime command lookups use the cache without reading metadata')
  t.equal(command, node._monsterPendingCommand,
    'exact command lookup returns the authoritative pending command')
  t.equal(pending, node._monsterPendingCommand,
    'default command lookup returns the authoritative pending command')
  t.deepEqual(cachedPendingAt(node), expectedPending,
    'the authoritative pending command retains the durable digest')

  const sent = cluster.messages.find(({ from, to, msg }) => {
    return from === node.id && to === '2' &&
      msg.type === 'monster_outcome' && msg.cmdSeq === 1n
  })
  t.ok(sent, 'OUTCOME_REQUEST sends the cached pending digest')
  t.equal(sent.msg.digest, node._monsterPendingCommand.localDigest,
    'OUTCOME reuses the authoritative pending digest')
})

test('MonsterFt closes a failed CMD Session and rolls back all item work',
  async (t) => {
    const fixture = makeFixture(t, 'command-session-failure')
    await fixture.reset()
    const cluster = fixture.build()
    const node = cluster.nodes[0]
    node.open()
    await applyInitialNoop(node)
    const db = node.db
    const createSession = db.createSession.bind(db)
    const failure = new Error('injected command patchset failure')
    let commandClosed = false
    db.createSession = (options) => {
      const session = options === undefined
        ? createSession()
        : createSession(options)
      if (options?.db === 'main' && options.table === undefined) {
        session.patchset = () => { throw failure }
        const close = session.close.bind(session)
        session.close = () => {
          commandClosed = true
          return close()
        }
      }
      return session
    }

    const record = Buffer.from(pack({
      type: 'cmd',
      items: [toBuf({ key: 'command-session-failure', value: 14 })],
      matchIndex: ids.map(() => 0n),
    }))
    try {
      const failed = await rejects(
        t,
        node._monsterApply(node, [record], [1n], [5n]),
        /^injected command patchset failure$/,
        'command patchset failure aborts the CMD')
      t.equal(failed, failure,
        'the failed command Session preserves the original error')
    } finally {
      db.createSession = createSession
    }

    t.ok(commandClosed, 'the failed command Session is closed')
    t.equal(cluster.calls.filter(({ command }) => command !== null).length, 1,
      'patchset extraction occurs after the application callback')
    t.equal(db.prepare(`
      SELECT COUNT(*) AS count FROM monster_metadata_items
    `).get().count, 0n, 'patchset failure rolls back application writes')
    t.equal(pendingAt(node), null,
      'patchset failure stores no durable pending command')
    t.equal(cachedPendingAt(node), null,
      'patchset failure stores no cached pending command')
    t.equal(db.prepare(`
      SELECT applied_seq FROM monsterft_meta WHERE id = 1
    `).get().applied_seq, 0n,
      'patchset failure does not advance the checkpoint')
  })

test('MonsterFt keeps pending SQL and cache aligned across SYNC rollback',
  async (t) => {
    const fixture = makeFixture(t, 'sync-rollback')
    await fixture.reset()
    const cluster = fixture.build()
    const node = cluster.nodes[0]
    node.open()
    await applyInitialNoop(node)

    const cmd = Buffer.from(pack({
      type: 'cmd',
      items: [toBuf({ key: 'sync-rollback', value: 15 })],
      matchIndex: ids.map(() => 0n),
    }))
    const [applied] = await node._monsterApply(node, [cmd], [1n], [0n])
    const expectedPending = {
      cmdSeq: 1n,
      localDigest: Buffer.from(applied.localDigest),
    }
    t.deepEqual(pendingAt(node), expectedPending,
      'CMD commits pending state to metadata')
    t.deepEqual(cachedPendingAt(node), expectedPending,
      'CMD publishes matching pending state to the cache')
    const pendingReport = Buffer.alloc(32, 2)
    node._monsterPendingReports.set('2', pendingReport)

    const sync = Buffer.from(pack({
      type: 'sync',
      cmdSeq: 1n,
      quorum: true,
      digest: Buffer.from(applied.localDigest),
      agree: [...ids],
      disagree: [],
    }))
    const prepare = node.db.prepare.bind(node.db)
    const failure = new Error('injected SYNC checkpoint failure')
    node.db.prepare = (sql) => {
      const statement = prepare(sql)
      if (/UPDATE\s+monsterft_meta\s+SET\s+pending_cmd_seq\s*=\s*NULL/i
        .test(sql)) {
        const run = statement.run.bind(statement)
        statement.run = (...args) => {
          run(...args)
          throw failure
        }
      }
      return statement
    }
    const failed = await rejects(t, node._monsterApply(node, [sync], [2n], [0n]),
      /^DB2 write CMD decision injected SYNC checkpoint failure$/,
      'failure after pending-state deletion rolls back SYNC')
    t.ok(failed instanceof ErrorWithCode,
      'the failed SYNC normalizes the original error')
    t.notEqual(failed, failure,
      'the failed SYNC replaces the uncoded original error')
    t.equal(failed.code, SQLITE_ERROR,
      'the failed SYNC uses SQLITE_ERROR')
    t.equal(failed.sqlCode, null,
      'the synthetic SYNC failure has no SQLite error code')

    t.deepEqual(pendingAt(node), expectedPending,
      'rolled-back SYNC retains durable pending state')
    t.deepEqual(cachedPendingAt(node), expectedPending,
      'rolled-back SYNC leaves authoritative cached state unchanged')
    t.equal(node._monsterPendingReports.get('2'), pendingReport,
      'rolled-back SYNC retains reports for the pending CMD')
    t.equal(node._applySeq, 1n,
      'rolled-back SYNC leaves the in-memory checkpoint unchanged')

    node.db.prepare = prepare
    await node._monsterApply(node, [sync], [2n], [0n])
    t.equal(pendingAt(node), null,
      'successful retry clears durable pending state')
    t.equal(cachedPendingAt(node), null,
      'successful retry clears cached pending state after COMMIT')
    t.equal(node._monsterPendingReports.size, 0,
      'successful retry clears reports for the resolved CMD')
    t.equal(node._applySeq, 2n,
      'successful retry advances the in-memory checkpoint')
    t.equal(await node._monsterRunDbGetCmd(1n), null,
      'the decided CMD is no longer available as pending state')
  })

test('MonsterFt queues an early OUTCOME behind the active CMD commit',
  async (t) => {
    const fixture = makeFixture(t, 'early-outcome')
    await fixture.reset()
    const entered = deferred()
    const release = deferred()
    t.teardown(() => release.resolve())
    const cluster = fixture.build({
      intercept: (to, from, msg) => {
        if (msg.type === 'monster_outcome_request') { return false }
      },
      apply: async (db, node, buf) => {
        const command = defaultApply(db, node, buf)
        if (node.id === '1' && command.key === 'early-outcome') {
          entered.resolve()
          await release.promise
        }
        return command
      },
    })
    const leader = await openAndElect(cluster)
    const cmdSeq = leader.seq + 1n
    const synced = nextEvent(leader, 'sync', (event) => {
      return event.cmdSeq === cmdSeq
    })
    const appending = leader.append(toBuf({
      key: 'early-outcome',
      value: 16,
    }))
    appending.catch(noop)
    await withTimeout(entered.promise, 'leader CMD transaction')
    await waitFor(() => cluster.messages.some(({ from, to, msg }) => {
      return from !== leader.id && to === leader.id &&
        msg.type === 'monster_outcome' && msg.cmdSeq === cmdSeq
    }), 'follower OUTCOME during leader CMD')
    await waitFor(() => leader._monsterDbQueue.length > 0,
      'queued early OUTCOME check')

    t.equal(cachedPendingAt(leader), null,
      'the active CMD does not publish cache state before COMMIT')
    t.equal(pendingAt(leader), null,
      'the active callback has not installed durable pending state')
    t.equal(leader._monsterPendingReports.size, 0,
      'the early report waits for its queued exact-command check')

    release.resolve()
    const [actualSeq, result] = await withTimeout(
      appending,
      'append using queued early OUTCOME',
    )
    const sync = await withTimeout(synced, 'early OUTCOME SYNC')
    t.equal(actualSeq, cmdSeq, 'the queued report certifies the expected CMD')
    t.deepEqual(result, { key: 'early-outcome', value: 16 },
      'the active CMD returns its application result')
    t.equal(sync.cmdSeq, cmdSeq,
      'the queued report drives the matching SYNC')
    t.notOk(cluster.messages.some(({ msg }) => {
      return msg.type === 'monster_outcome_request' && msg.cmdSeq === cmdSeq
    }), 'certification needs no retry request after the queued report is read')
    t.equal(pendingAt(leader), null,
      'SYNC clears the committed durable pending state')
    t.equal(cachedPendingAt(leader), null,
      'SYNC clears the committed cached pending state')
  })

test('MonsterFt warns for invalid OUTCOME fields', async (t) => {
  const fixture = makeFixture(t, 'invalid-outcome')
  await fixture.reset()
  const cluster = fixture.build()
  const leader = await openAndElect(cluster)
  const from = cluster.nodes.find((node) => node !== leader).id
  const warnings = []
  leader.on('warn', (err) => warnings.push(err))

  await leader.onReceive(from, {
    type: 'monster_outcome',
    cmdSeq: -1n,
    digest: Buffer.alloc(32),
  })
  await leader.onReceive(from, {
    type: 'monster_outcome',
    cmdSeq: 0n,
    digest: Buffer.alloc(31),
  })
  await leader.onReceive(from, {
    type: 'monster_outcome_request',
    cmdSeq: -1n,
  })

  t.deepEqual(warnings.map((err) => err.message), [
    'outcome cmdSeq is illegal',
    'outcome digest is illegal',
    'outcome request cmdSeq is illegal',
  ], 'each invalid OUTCOME message emits its warning')
  t.ok(warnings.every((err) => err instanceof ErrorWithCode),
    'every invalid OUTCOME warning is an ErrorWithCode')
  t.ok(warnings.every((err) => err.code === RPC_ILLEGAL),
    'every invalid OUTCOME warning uses RPC_ILLEGAL')
  t.ok(warnings.every((err) => err.sqlCode === null),
    'invalid OUTCOME warnings have no SQLite error code')
})

test('MonsterFt ignores an OUTCOME queued behind its command SYNC',
  async (t) => {
    const fixture = makeFixture(t, 'stale-outcome-sync')
    await fixture.reset()
    const cluster = fixture.build()
    const node = cluster.nodes[0]
    node.open()
    await applyInitialNoop(node)

    const firstCmd = Buffer.from(pack({
      type: 'cmd',
      items: [toBuf({ key: 'stale-outcome-first', value: 17 })],
      matchIndex: ids.map(() => 0n),
    }))
    const [firstApplied] = await node._monsterApply(
      node,
      [firstCmd],
      [1n],
      [0n],
    )
    node.state = 'leader'

    const syncEntered = deferred()
    const releaseSync = deferred()
    t.teardown(() => releaseSync.resolve())
    const writeTx = node._monsterWriteTx.bind(node)
    let pauseSync = true
    node._monsterWriteTx = (db, fn) => writeTx(db, async (db) => {
      if (pauseSync) {
        pauseSync = false
        syncEntered.resolve()
        await releaseSync.promise
      }
      return fn(db)
    })
    const sync = Buffer.from(pack({
      type: 'sync',
      cmdSeq: 1n,
      quorum: true,
      digest: Buffer.from(firstApplied.localDigest),
      agree: [...ids],
      disagree: [],
    }))
    const syncing = node._monsterApply(node, [sync], [2n], [0n])
    await withTimeout(syncEntered.promise, 'active SYNC transaction')

    const staleOutcome = node._monsterRxOutcome('2', {
      type: 'monster_outcome',
      cmdSeq: 1n,
      digest: Buffer.alloc(32, 3),
    })
    t.equal(node._monsterPendingReports.size, 0,
      'the OUTCOME waits behind the active SYNC transaction')

    releaseSync.resolve()
    await withTimeout(syncing, 'resolving SYNC')
    await withTimeout(staleOutcome, 'queued stale OUTCOME')
    node._monsterWriteTx = writeTx
    t.equal(node._monsterPendingReports.size, 0,
      'the queued OUTCOME sees that its command was resolved')

    const secondCmd = Buffer.from(pack({
      type: 'cmd',
      items: [toBuf({ key: 'stale-outcome-second', value: 18 })],
      matchIndex: ids.map(() => 2n),
    }))
    const [secondApplied] = await node._monsterApply(
      node,
      [secondCmd],
      [3n],
      [0n],
    )
    t.deepEqual([...node._monsterPendingReports.keys()], [node.id],
      'the replacement CMD starts with only its local report')
    t.equal(
      node._monsterPendingReports.get(node.id),
      secondApplied.localDigest,
      'the replacement report retains its authoritative digest',
    )
  })

test('MonsterFt recovery reuses a stored digest without reapplication', async (t) => {
  const fixture = makeFixture(t, 'recovery')
  await fixture.reset()
  const first = fixture.build({
    intercept: (to, from, msg) => {
      if (msg.type === 'monster_outcome') { return false }
    },
  })
  const leader = await openAndElect(first)
  const cmdSeq = leader.seq + 1n
  const appending = leader.append(toBuf({ key: 'recover', value: 10 }))
  appending.catch(noop)

  await waitFor(() => first.nodes.every((node) => node._applySeq >= cmdSeq),
    'unresolved CMD application')
  t.equal(first.patchsetCalls.length, 3,
    'the original application calculates one digest on every node')
  const stored = new Map(first.nodes.map((node) => {
    const pending = pendingAt(node)
    t.equal(pending?.cmdSeq, cmdSeq,
      `node ${node.id} stores the unresolved CMD in metadata`)
    t.deepEqual(cachedPendingAt(node), pending,
      `node ${node.id} publishes matching cached pending state`)
    return [node.id, pending]
  }))
  t.deepEqual(
    await leader._monsterRunDbGetCmd(),
    await leader._monsterRunDbGetCmd(cmdSeq),
    'the default lookup returns the one exact cached command',
  )

  closeNodes(first.nodes)
  await rejects(t, appending, /node not open/,
    'closing makes the original caller fail ambiguously')

  const recovered = fixture.build()
  const recoveryLeader = recovered.nodes[0]
  const recoveryCommands = new WeakSet()
  const commandLookup = recoveryLeader._monsterRunDbGetCmd.bind(recoveryLeader)
  recoveryLeader._monsterRunDbGetCmd = async (...args) => {
    const command = await commandLookup(...args)
    if ((args.length === 0 || args[0] === null) && command !== null) {
      recoveryCommands.add(command)
    }
    return command
  }
  let coordinatedFromRecoveryLookup = false
  const awaitDecision = recoveryLeader._monsterAwaitDecision.bind(recoveryLeader)
  recoveryLeader._monsterAwaitDecision = (command, ...args) => {
    coordinatedFromRecoveryLookup = recoveryCommands.has(command)
    return awaitDecision(command, ...args)
  }
  openNodes(recovered.nodes)
  recovered.nodes.forEach((node) => {
    t.deepEqual(cachedPendingAt(node), stored.get(node.id),
      `node ${node.id} hydrates its pending cache during open`)
  })
  recoveryLeader._voteForSelf()
  await withTimeout(ready(recovered.nodes, null, true), 'recovery leader readiness')
  await waitFor(() => recovered.nodes.every((node) => {
    return pendingAt(node) === null && cachedPendingAt(node) === null
  }), 'stored command digest certification')

  t.equal(coordinatedFromRecoveryLookup, true,
    'recovery coordinates the command returned by its pending lookup')
  t.equal(recovered.patchsetCalls.length, 0,
    'recovery does not recalculate the stored command digest')
  t.equal(recovered.calls.length, 0,
    'recovery invokes no callback for the later no-op or stored CMD')
  t.ok(recovered.nodes.every((node) => node._applySeq > cmdSeq),
    'recovery advances every checkpoint beyond the stored CMD')
  for (const node of recovered.nodes) {
    t.equal(await node._monsterRunDbGetCmd(cmdSeq), null,
      `node ${node.id} retains no resolved command history`)
  }
})
