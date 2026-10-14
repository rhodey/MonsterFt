import crypto from 'node:crypto'
import fs from 'node:fs'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import test from 'tape'
import { pack, unpack } from 'msgpackr'
import {
  APPLY_ERROR,
  ARGUMENT_ILLEGAL,
  DRAINING,
  ErrorWithCode,
  FS_ERROR,
  LOG_OPEN,
  MONSTER_CORRUPT,
  NODE_NOT_OPEN,
  REPAIR_OUTSIDE_AGREEMENT,
  REPAIR_QUORUM_IMPOSSIBLE,
  SQLITE_ERROR,
} from '../src/error.js'
import { RaftNode } from '../src/node.js'
import { MonsterFt } from '../src/monsterft.js'

const ids = ['1', '2', '3']
const noop = () => {}
const TEST_DIR = process.env.TEST_DIR ?? '/tmp'
let fixtureId = 0
const openNodes = (nodes) => nodes.forEach((node) => node.open())
const closeNodes = (nodes) => {
  const errors = []
  for (const node of nodes) {
    try { node.close() } catch (err) { errors.push(err) }
  }
  if (errors.length === 1) { throw errors[0] }
  if (errors.length > 1) {
    throw new AggregateError(errors, 'failed to close nodes')
  }
}
const closeNodesQuietly = (nodes) => {
  for (const node of nodes) {
    try { node.close() } catch {}
  }
}

const toBuf = (value) => Buffer.from(JSON.stringify(value), 'utf8')
const toObj = (buf) => JSON.parse(Buffer.from(buf).toString('utf8'))
const toEntry = (term, record) => ({ term, entry: Buffer.from(pack(record)) })
const toNoopEntry = (term) => ({ term, entry: Buffer.alloc(0) })
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

const deferred = () => {
  let resolve = null
  let reject = null
  const promise = new Promise((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

const waitFor = async (fn, name, ms=8_000) => {
  const end = Date.now() + ms
  while (!await fn()) {
    if (Date.now() >= end) { throw new Error(`${name} timeout`) }
    await sleep(5)
  }
}

const withTimeout = (promise, name, ms=8_000) => {
  let timer = null
  const deadline = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${name} timeout`)), ms)
  })
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer))
}

const errorOf = (promise) => promise.then(
  () => null,
  (err) => err,
)
const errorOfCall = (fn) => {
  try {
    fn()
    return null
  } catch (err) {
    return err
  }
}

const uniquePath = (name) => {
  fs.mkdirSync(TEST_DIR, { recursive: true })
  const unique = `${process.pid}-${Date.now()}-${++fixtureId}-${name}`
  return path.join(TEST_DIR, `monsterft-two-file-${unique}.sqlite`)
}

const sqliteFiles = (databasePath) => [
  databasePath,
  `${databasePath}-journal`,
  `${databasePath}-wal`,
  `${databasePath}-shm`,
]

const removePair = (databasePath) => {
  for (const file of [databasePath, `${databasePath}2`]) {
    for (const target of sqliteFiles(file)) {
      fs.rmSync(target, { force: true })
    }
  }
}

const tableNames = (db) => db.prepare(`
  SELECT name
  FROM sqlite_schema
  WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
  ORDER BY name
`).all().map(({ name }) => name)

const initializeApp = (db) => db.exec(`
  CREATE TABLE IF NOT EXISTS two_file_items (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
  ) STRICT
`)

const applyApp = (db, buf, term, seq) => {
  if (seq === 0n) {
    initializeApp(db)
    return
  }
  if (buf === null) { return }
  const command = toObj(buf)
  db.prepare(`
    INSERT INTO two_file_items (key, value) VALUES (?, ?)
    ON CONFLICT (key) DO UPDATE SET value = excluded.value
  `).run(command.key, command.value)
  return { key: command.key, value: command.value }
}

const valueAt = (node, key) => {
  const row = node.db.prepare(`
    SELECT value FROM two_file_items WHERE key = ?
  `).get(key)
  return row === undefined ? null : Number(row.value)
}

const monsterMetaAtPath = (databasePath) => {
  const db = new DatabaseSync(databasePath, {
    readOnly: true,
    readBigInts: true,
  })
  try {
    return db.prepare(`
      SELECT applied_seq, pending_cmd_seq, applied_local_digest
      FROM monsterft_meta
      WHERE id = 1
    `).get()
  } finally {
    db.close()
  }
}

const applyMetaAtPath = (databasePath) => {
  const db = new DatabaseSync(`${databasePath}2`, {
    readOnly: true,
    readBigInts: true,
  })
  try {
    return db.prepare('SELECT seq, entry_hash, local_digest FROM monsterft_meta_apply WHERE id = 1').get()
  } finally {
    db.close()
  }
}

const valueAtPath = (databasePath, key) => {
  const db = new DatabaseSync(`${databasePath}2`, { readOnly: true })
  try {
    const row = db.prepare(`
      SELECT value FROM two_file_items WHERE key = ?
    `).get(key)
    return row === undefined ? null : Number(row.value)
  } finally {
    db.close()
  }
}

const raftEntryAtPath = (databasePath, seq) => {
  const db = new DatabaseSync(databasePath, {
    readOnly: true,
    readBigInts: true,
  })
  try {
    const row = db.prepare(`
      SELECT entry FROM raft_log WHERE seq = ?
    `).get(seq)
    if (row === undefined) { return null }
    return unpack(Buffer.from(row.entry))
  } finally {
    db.close()
  }
}

const makeBus = () => {
  const current = new Map()
  const messages = []
  const held = []
  let holdOutcomes = false

  const deliver = ({ to, from, msg }) => {
    const node = current.get(to)
    if (!node?.isOpen) { return undefined }
    return node.onReceive(from, msg)
  }

  return {
    messages,
    register(node) {
      current.set(node.id, node)
    },
    send(to, from, msg) {
      const envelope = { to, from, msg }
      messages.push(envelope)
      if (holdOutcomes && msg?.type === 'monster_outcome') {
        held.push(envelope)
        return undefined
      }
      return deliver(envelope)
    },
    holdOutcomes() {
      holdOutcomes = true
    },
    async releaseOutcomes() {
      holdOutcomes = false
      const releasing = held.splice(0)
      await Promise.all(releasing.map(deliver))
    },
  }
}

const clusterFixture = (t, name, { memory=false }={}) => {
  const paths = new Map(ids.map((id) => [
    id,
    memory ? ':memory:' : uniquePath(`${name}-${id}`),
  ]))
  const allNodes = []
  const errors = []
  const bus = makeBus()

  if (!memory) {
    for (const databasePath of paths.values()) { removePair(databasePath) }
  }

  const build = ({ apply=applyApp }={}) => {
    const applies = []
    const nodes = ids.map((id) => {
      const send = (to, msg) => bus.send(to, id, msg)
      let node = null
      node = new MonsterFt(id, ids, send, paths.get(id), {
        electionTimeout: 150,
        appendTimeout: 5_000,
        quorum: 2,
        apply: async (db, buf, term, seq, index, matchIndex) => {
          const current = node
          applies.push({
            id,
            db,
            buf,
            term,
            seq,
            index,
            matchIndex,
            isMonsterDb: db === current.db,
            separate: db !== current.log.db,
          })
          return apply(db, buf, term, seq, index, matchIndex, current)
        },
      })
      // Tests elect leaders explicitly.
      node._timers.election = 60_000
      node.on('warn', noop)
      node.on('error', (err) => errors.push({ id, err }))
      bus.register(node)
      allNodes.push(node)
      return node
    })
    return { nodes, applies }
  }

  t.teardown(() => {
    closeNodesQuietly(allNodes)
    if (!memory) {
      for (const databasePath of paths.values()) { removePair(databasePath) }
    }
  })

  return { paths, errors, bus, build }
}

const openAndElect = async (nodes, candidate=nodes[0]) => {
  openNodes(nodes)
  candidate._voteForSelf()
  await waitFor(() => candidate.state === 'leader',
    `node ${candidate.id} election`)
  await withTimeout(
    Promise.all(nodes.map((node) => node.awaitLeader(true))),
    `node ${candidate.id} committed leadership`,
  )
  return candidate
}

const waitApplied = (nodes, seq) => waitFor(
  () => nodes.every((node) => node._applySeq >= seq),
  `all nodes apply through ${seq}`,
)

const nextSync = (node) => new Promise((resolve) => node.once('sync', resolve))

test('constructors use strict paths and a literal two-file pair',
  async (t) => {
    const raftPath = uniquePath('raft-path')
    const monsterPath = uniquePath('monster-path')
    const raftOnlyPath = uniquePath('raft-half')
    const monsterOnlyPath = uniquePath('monster-half')
    const nodes = []
    const paths = [raftPath, monsterPath, raftOnlyPath, monsterOnlyPath]
    paths.forEach(removePair)

    t.teardown(() => {
      closeNodesQuietly(nodes)
      paths.forEach(removePair)
    })

    const applyErr = errorOfCall(
      () => new MonsterFt('1', ids, noop, ':memory:'),
    )
    t.ok(applyErr instanceof ErrorWithCode,
      'MonsterFt rejects a missing apply callback with ErrorWithCode')
    t.equal(applyErr.message, 'apply must be a function',
      'the missing apply callback error has the expected message')
    t.equal(applyErr.code, ARGUMENT_ILLEGAL,
      'the missing apply callback is an illegal argument')
    t.equal(applyErr.sqlCode, null,
      'the missing apply callback has no SQLite error code')

    t.throws(
      () => new RaftNode('1', ids, noop, ''),
      /SQLiteLog DB path must be non-empty string/,
      'RaftNode rejects an empty database path',
    )
    t.throws(
      () => new MonsterFt('1', ids, noop, '', { apply: noop }),
      /SQLiteLog DB path must be non-empty string/,
      'MonsterFt delegates empty database path validation to SQLiteLog',
    )

    const memoryRaft = new RaftNode('1', ids, noop, ':memory:', {
      electionTimeout: 60_000,
    })
    nodes.push(memoryRaft)
    t.equal(memoryRaft.log.path, ':memory:',
      'RaftNode retains the exact in-memory database path')
    memoryRaft.open()
    t.equal(memoryRaft.log.db.prepare('PRAGMA database_list').get().file, '',
      'RaftNode opens an in-memory DB1')
    t.throws(() => memoryRaft.del(), /log is open/,
      'RaftNode refuses to delete open in-memory storage')
    memoryRaft.close()
    t.equal(memoryRaft.del(), undefined,
      'RaftNode delegates closed in-memory deletion as a no-op')

    const raft = new RaftNode('1', ids, noop, raftPath, {
      electionTimeout: 60_000,
    })
    nodes.push(raft)
    t.equal(raft.log.path, raftPath,
      'RaftNode constructs SQLiteLog from the exact supplied path')
    raft.open()
    t.ok(fs.existsSync(raftPath), 'RaftNode creates DB1')
    t.notOk(fs.existsSync(`${raftPath}2`),
      'plain RaftNode does not create a Monster database')
    t.throws(() => raft.del(), /log is open/,
      'RaftNode refuses to delete an open DB1')
    raft.close()
    t.notOk(raft.log.isOpen, 'RaftNode DB1 is closed before close returns')
    for (const sidecar of sqliteFiles(raftPath).slice(1)) {
      fs.writeFileSync(sidecar, '')
    }
    t.equal(raft.del(), undefined, 'RaftNode deletes its closed DB1')
    t.ok(sqliteFiles(raftPath).every((target) => !fs.existsSync(target)),
      'RaftNode deletion removes every DB1 sidecar')

    const monster = new MonsterFt('1', ids, noop, monsterPath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    nodes.push(monster)
    t.equal(monster.log.path, monsterPath,
      'MonsterFt passes the original path to RaftNode')
    t.equal(monster._monsterDatabasePath, `${monsterPath}2`,
      'MonsterFt derives DB2 with literal string suffix 2')
    monster.open()

    t.ok(fs.existsSync(monsterPath), 'opening MonsterFt creates DB1')
    t.ok(fs.existsSync(`${monsterPath}2`), 'opening MonsterFt creates DB2')
    const openPairErr = errorOfCall(() => monster.del())
    t.ok(openPairErr instanceof ErrorWithCode,
      'MonsterFt rejects deletion of an open pair with ErrorWithCode')
    t.equal(openPairErr.message, 'DB1 is open',
      'MonsterFt reports the open Raft log first')
    t.equal(openPairErr.code, LOG_OPEN,
      'the open Raft log uses LOG_OPEN')
    t.equal(openPairErr.sqlCode, null,
      'the open Raft log has no SQLite error code')
    t.notEqual(monster.db, monster.log.db,
      'MonsterFt and RaftNode expose distinct SQLite connections')
    t.deepEqual(
      monster.db.prepare(`
        SELECT type, name
        FROM sqlite_schema
        WHERE name LIKE 'monsterft_%'
        ORDER BY type, name
      `).all().map(({ type, name }) => ({ type, name })),
      [
        { type: 'table', name: 'monsterft_meta_apply' },
      ],
      'DB2 stores only the apply receipt table before setup',
    )
    t.deepEqual(
      monster.log.db.prepare('PRAGMA table_info(monsterft_meta)')
        .all().map(({ name }) => name),
      [
        'id',
        'applied_seq',
        'applied_entry_hash',
        'repair_state',
        'pending_cmd_seq',
        'applied_local_digest',
      ],
      'the singleton metadata row owns the pending CMD fields',
    )
    t.deepEqual(
      monster.db.prepare('PRAGMA table_info(monsterft_meta_apply)').all().map(({ name }) => name),
      ['id', 'seq', 'entry_hash', 'local_digest'],
      'DB2 stores only the apply receipt fields',
    )
    t.notOk(tableNames(monster.log.db).includes('monsterft_meta_apply'),
      'DB1 contains no apply receipt table')
    t.deepEqual({ ...monster.db.prepare('SELECT * FROM monsterft_meta_apply').get() }, {
      id: 1n, seq: -1n, entry_hash: null, local_digest: null,
    }, 'a fresh apply receipt is uninitialized')
    t.throws(
      () => monster.log.db.prepare(`
        INSERT INTO monsterft_meta
          (id, applied_seq, applied_entry_hash, repair_state)
        VALUES (2, -1, NULL, 0)
      `).run(),
      /constraint/i,
      'metadata rejects a second singleton row',
    )
    const writeRepairState = monster.log.db.prepare(`
      UPDATE monsterft_meta SET repair_state = ? WHERE id = 1
    `)
    const readRepairState = monster.log.db.prepare(`
      SELECT repair_state FROM monsterft_meta WHERE id = 1
    `)
    for (const state of [1, 2, 0]) {
      writeRepairState.run(state)
      t.equal(readRepairState.get().repair_state, BigInt(state),
        `metadata stores repair state ${state}`)
    }
    t.throws(
      () => writeRepairState.run(3),
      /constraint/i,
      'metadata rejects an unknown repair state',
    )
    t.throws(
      () => monster.log.db.prepare(`
        UPDATE monsterft_meta
        SET pending_cmd_seq = 0
        WHERE id = 1
      `).run(),
      /constraint/i,
      'metadata rejects a partial pending-command pair',
    )
    t.equal(
      String(monster.db.prepare('PRAGMA journal_mode').get().journal_mode)
        .toLowerCase(),
      'wal',
      'DB2 uses WAL journaling',
    )
    t.equal(
      Number(monster.db.prepare('PRAGMA synchronous').get().synchronous),
      2,
      'DB2 uses FULL synchronization',
    )
    const monsterDb = monster.db
    monster.close()
    t.equal(monster.db, null, 'MonsterFt clears DB2 before close returns')
    t.notOk(monster.log.isOpen, 'MonsterFt closes DB1 before close returns')
    t.deepEqual(monster.eventNames(), [],
      'MonsterFt close removes public and internal listeners')
    t.throws(
      () => monsterDb.prepare('SELECT 1'),
      /closed|not open/i,
      'MonsterFt physically closes DB2 before close returns',
    )
    for (const file of [monsterPath, `${monsterPath}2`]) {
      for (const sidecar of sqliteFiles(file).slice(1)) {
        fs.writeFileSync(sidecar, '')
      }
    }
    t.equal(monster.del(), undefined, 'MonsterFt deletes its closed pair')
    t.ok(
      [monsterPath, `${monsterPath}2`]
        .flatMap(sqliteFiles)
        .every((target) => !fs.existsSync(target)),
      'MonsterFt deletion removes both databases and every sidecar',
    )

    new DatabaseSync(raftOnlyPath).close()
    const raftOnlyErr = errorOfCall(
      () => new MonsterFt('1', ids, noop, raftOnlyPath, { apply: noop }),
    )
    t.ok(raftOnlyErr instanceof ErrorWithCode,
      'an existing DB1 without DB2 is rejected with ErrorWithCode')
    t.equal(
      raftOnlyErr.message,
      'database pair must have both files present or both files absent',
      'the mismatched pair error has the expected message',
    )
    t.equal(raftOnlyErr.code, ARGUMENT_ILLEGAL,
      'a mismatched database pair is an illegal argument')
    t.equal(raftOnlyErr.sqlCode, null,
      'a mismatched database pair has no SQLite error code')
    t.notOk(fs.existsSync(`${raftOnlyPath}2`),
      'half-pair rejection does not create DB2')

    new DatabaseSync(`${monsterOnlyPath}2`).close()
    t.throws(
      () => new MonsterFt('1', ids, noop, monsterOnlyPath, { apply: noop }),
      /database pair must have both files present or both files absent/,
      'an existing DB2 without DB1 is rejected',
    )
    t.notOk(fs.existsSync(monsterOnlyPath),
      'half-pair rejection does not create DB1')
  })

test('in-memory MonsterFt cluster elects, applies, and deletes as a no-op',
  async (t) => {
    const fixture = clusterFixture(t, 'memory', { memory: true })
    const cluster = fixture.build()
    const leader = await openAndElect(cluster.nodes)

    for (const node of cluster.nodes) {
      t.equal(node.log.path, ':memory:',
        `node ${node.id} uses :memory: for DB1`)
      t.equal(node._monsterDatabasePath, ':memory:',
        `node ${node.id} uses :memory: for DB2`)
      t.notEqual(node.log.db, node.db,
        `node ${node.id} keeps independent DB1 and DB2 connections`)
      t.equal(node.log.db.prepare('PRAGMA database_list').get().file, '',
        `node ${node.id} DB1 has no backing file`)
      t.equal(node.db.prepare('PRAGMA database_list').get().file, '',
        `node ${node.id} DB2 has no backing file`)
    }

    const synced = nextSync(leader)
    const [cmdSeq, result] = await leader.append(toBuf({
      key: 'memory',
      value: 7,
    }))
    const { syncSeq } = await synced
    t.deepEqual(result, { key: 'memory', value: 7 },
      'the in-memory leader returns the apply result')
    t.ok(cmdSeq >= 0n, 'the in-memory cluster assigns a command sequence')
    await waitApplied(cluster.nodes, syncSeq)
    for (const node of cluster.nodes) {
      t.equal(valueAt(node, 'memory'), 7,
        `node ${node.id} applies the in-memory command`)
    }

    closeNodes(cluster.nodes)
    for (const node of cluster.nodes) {
      t.equal(node.del(), undefined,
        `node ${node.id} closed in-memory deletion is a no-op`)
    }
  })

test('MonsterFt deletion stops before DB2 after the first DB1 failure', (t) => {
  const databasePath = uniquePath('delete-stop')
  removePair(databasePath)
  const db1Files = sqliteFiles(databasePath)
  const db2Files = sqliteFiles(`${databasePath}2`)
  for (const target of [...db1Files, ...db2Files]) {
    fs.writeFileSync(target, '')
  }
  const node = new MonsterFt('1', ids, noop, databasePath, { apply: noop })
  t.teardown(() => removePair(databasePath))

  const rmSync = fs.rmSync
  const failure = new Error('injected DB1 journal delete failure')
  const attempted = []
  fs.rmSync = (target, options) => {
    attempted.push(target)
    if (target === `${databasePath}-journal`) { throw failure }
    return rmSync(target, options)
  }
  let actual = null
  try {
    actual = errorOfCall(() => node.del())
  } finally {
    fs.rmSync = rmSync
  }

  t.equal(actual.code, FS_ERROR, 'MonsterFt reports the filesystem error code')
  t.equal(actual.sqlCode, null, 'filesystem failure has no SQLite code')
  t.match(actual.message, /\(log del\) injected DB1 journal delete failure/,
    'MonsterFt retains SQLiteLog deletion context')
  t.deepEqual(attempted, [databasePath, `${databasePath}-journal`],
    'MonsterFt stops at the first DB1 sidecar failure')
  t.notOk(fs.existsSync(databasePath), 'the earlier DB1 main deletion remains')
  t.ok(db1Files.slice(1).every(fs.existsSync),
    'later DB1 sidecars remain untouched')
  t.ok(db2Files.every(fs.existsSync), 'DB2 remains completely untouched')
  t.end()
})

test('Monster open completes storage and validation before Raft start',
  (t) => {
    const databasePath = uniquePath('open-order')
    removePair(databasePath)
    const order = []

    class OrderedMonsterFt extends MonsterFt {
      _monsterInit(db) {
        const result = super._monsterInit(db)
        order.push('metadata initialization and validation')
        return result
      }


      _startRaft() {
        order.push('Raft start')
        return super._startRaft()
      }
    }

    const node = new OrderedMonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    node.on('warn', noop)
    node.on('error', noop)
    node.on('change', (change) => {
      if (change.open) { order.push('open publication') }
    })
    const openLog = node.log.open.bind(node.log)
    node.log.open = () => {
      const result = openLog()
      order.push('DB1')
      return result
    }
    t.teardown(() => {
      closeNodesQuietly([node])
      removePair(databasePath)
    })

    node.open()
    t.deepEqual(order, [
      'DB1',
      'metadata initialization and validation',
      'Raft start',
      'open publication',
    ], 'open completes every stage in the required order')
    t.ok(node.isOpen, 'the node is published only after every stage succeeds')
    node.close()
    t.end()
  })

test('sequence-zero bootstrap failure rolls back and closes the node',
  async (t) => {
    const databasePath = uniquePath('bootstrap-retry')
    removePair(databasePath)
    const failure = new Error('injected bootstrap failure')
    let attempts = 0
    const nodes = []
    let rejectBootstrap = true
    const calls = []
    const warnings = []
    const build = (onFatal=noop, onError=noop) => {
      const current = new MonsterFt('1', ids, noop, databasePath, {
        electionTimeout: 60_000,
        apply: (db, buf, term, seq, index, matchIndex) => {
          calls.push({
            db,
            buf,
            term,
            seq,
            index,
            matchIndex,
            isMonsterDb: db === current.db,
          })
          if (seq === 0n) {
            attempts++
            initializeApp(db)
            if (rejectBootstrap) { throw failure }
            return
          }
          return applyApp(db, buf, term, seq)
        },
      })
      current.on('warn', (err) => warnings.push(err))
      current.on('fatal', onFatal)
      current.on('error', onError)
      nodes.push(current)
      return current
    }
    const fatals = []
    let resolveError = null
    const errored = new Promise((resolve) => { resolveError = resolve })
    const node = build(
      (err) => fatals.push(err),
      (err) => resolveError(err),
    )
    t.teardown(() => {
      closeNodesQuietly(nodes)
      removePair(databasePath)
    })

    node.open()
    node.log.append(toNoopEntry(3n))
    node._commitSeq = 0n
    node._commitTerm = 3n
    const rejected = await errorOf(node._apply(0n))
    t.ok(rejected instanceof ErrorWithCode,
      'no-op application normalizes the bootstrap error')
    t.notEqual(rejected, failure,
      'no-op application replaces the original bootstrap error')
    t.equal(rejected.code, APPLY_ERROR,
      'no-op application has APPLY_ERROR code')
    t.equal(rejected.message, '(apply) injected bootstrap failure',
      'no-op application adds apply context')
    t.equal(fatals[0], rejected,
      'the normalized bootstrap error is emitted as fatal')
    t.equal(await errored, rejected,
      'fatal closure reports the normalized error publicly')
    t.equal(warnings.length, 0,
      'sequence-zero failures retain fatal reporting without command warnings')
    const { db: bootstrapDb, ...bootstrapCall } = calls[0]
    t.deepEqual(bootstrapCall, {
      buf: null,
      term: 3n,
      seq: 0n,
      index: 0,
      matchIndex: null,
      isMonsterDb: true,
    }, 'the initial no-op invokes apply with its complete callback contract')
    t.throws(() => bootstrapDb.prepare('SELECT 1'), /closed|not open/i,
      'fatal closure physically closes the callback DB2 connection')
    t.equal(node.db, null, 'fatal bootstrap cleanup clears DB2')
    t.equal(node.log.isOpen, false, 'fatal bootstrap cleanup closes DB1')
    t.equal(node.isOpen, false, 'fatal bootstrap cleanup closes the node')

    const rolledBack = new DatabaseSync(`${databasePath}2`, {
      readOnly: true,
      readBigInts: true,
    })
    const meta = rolledBack.prepare(`
      SELECT seq, entry_hash, local_digest
      FROM monsterft_meta_apply WHERE id = 1
    `).get()
    const appTables = rolledBack.prepare(`
      SELECT count(*) AS count
      FROM sqlite_schema
      WHERE type = 'table' AND name = 'two_file_items'
    `).get().count
    rolledBack.close()
    t.deepEqual({ ...meta }, { seq: -1n, entry_hash: null, local_digest: null },
      'bootstrap rollback preserves the initial apply receipt')
    t.equal(appTables, 0n,
      'bootstrap rollback removes schema created in the no-op transaction')

    rejectBootstrap = false
    const replacement = build()
    replacement.open()
    replacement._commitSeq = 0n
    replacement._commitTerm = 3n
    await replacement._apply(0n)
    t.equal(attempts, 2, 'a fresh object retries the initial no-op bootstrap')
    t.deepEqual(tableNames(replacement.db), [
      'monsterft_meta_apply',
      'two_file_items',
    ], 'the successful retry commits the application schema')
    t.equal(replacement._applySeq, 0n,
      'the successful retry commits schema and checkpoint together')
    replacement.close()
  })

for (const asynchronous of [false, true]) {
  const mode = asynchronous ? 'async' : 'sync'
  test(`caught ${mode} bootstrap rollback leaves initialization retryable`,
    async (t) => {
      const paths = new Map(ids.map((id) => [
        id, uniquePath(`bootstrap-caught-full-${mode}-${id}`),
      ]))
      const nodes = []
      const bus = makeBus()
      const fatals = []
      const errors = []
      const failed = deferred()
      let attempts = 0
      let original = null
      let transactionAfterError = null
      const build = (id) => {
        const apply = (db, buf, term, seq) => {
          if (seq !== 0n) { return applyApp(db, buf, term, seq) }
          initializeApp(db)
          if (id !== '3' || ++attempts !== 1) { return }
          const { page_count: pages } = db.prepare('PRAGMA page_count').get()
          db.exec(`PRAGMA max_page_count = ${pages}`)
          try {
            db.exec(`
              INSERT INTO two_file_items VALUES (hex(zeroblob(1048576)), 1)
            `)
          } catch (err) {
            original = err
            transactionAfterError = db.isTransaction
          }
        }
        const node = new MonsterFt(id, ids,
          (to, msg) => bus.send(to, id, msg), paths.get(id), {
            electionTimeout: Number(id) * 300,
            appendTimeout: 5_000,
            apply: asynchronous ? async (...args) => {
              const result = apply(...args)
              await Promise.resolve()
              return result
            } : apply,
          })
        node.on('warn', noop)
        node.on('fatal', (err) => fatals.push({ id, err }))
        node.on('error', (err) => {
          errors.push({ id, err })
          if (id === '3') { failed.resolve(err) }
        })
        bus.register(node)
        nodes.push(node)
        return node
      }
      t.teardown(() => {
        closeNodesQuietly(nodes)
        for (const databasePath of paths.values()) { removePair(databasePath) }
      })

      const initial = ids.map(build)
      openNodes(initial)
      const failure = await withTimeout(failed.promise, 'caught bootstrap rollback')
      t.equal(original?.errcode, 13, 'the callback catches native SQLITE_FULL')
      t.equal(transactionAfterError, false, 'SQLite has rolled back the transaction')
      t.equal(failure.code, APPLY_ERROR, 'the missing transaction is an application failure')
      t.equal(failure.message, '(apply) DB2 seq0 transaction ended',
        'the diagnostic identifies the lost bootstrap transaction')
      t.deepEqual(fatals, [{ id: '3', err: failure }], 'fatal is emitted once')
      t.deepEqual(errors, [{ id: '3', err: failure }], 'error is emitted once')
      t.notOk(initial[2].isOpen, 'the failed node closes')
      t.equal(initial[2].db, null, 'fatal cleanup releases DB2')
      t.notOk(initial[2].log.isOpen, 'fatal cleanup releases DB1')

      const databasePath = paths.get('3')
      const db = new DatabaseSync(`${databasePath}2`, {
        readOnly: true, readBigInts: true,
      })
      try {
        const meta = db.prepare(`
          SELECT seq, entry_hash, local_digest FROM monsterft_meta_apply WHERE id = 1
        `).get()
        t.deepEqual({ ...meta }, {
          seq: -1n,
          entry_hash: null,
          local_digest: null,
        }, 'the durable apply receipt remains uninitialized')
        t.deepEqual(tableNames(db), ['monsterft_meta_apply'],
          'the failed transaction leaves no application schema')
      } finally {
        db.close()
      }

      const replacement = build('3')
      replacement.open()
      await withTimeout(replacement.awaitLeader(true), 'replacement committed leader')
      const synced = nextSync(replacement)
      const [seq, result] = await withTimeout(
        replacement.append(toBuf({ key: 'bootstrap-retry', value: 42 })),
        'append after bootstrap retry',
      )
      await withTimeout(synced, 'replacement command SYNC')
      t.equal(attempts, 2, 'a fresh instance reruns initialization')
      t.deepEqual(result, { key: 'bootstrap-retry', value: 42 },
        'a public append succeeds after the retry')
      t.equal(valueAtPath(databasePath, 'bootstrap-retry'), 42,
        'the application write is durable in the recreated schema')
      const meta = monsterMetaAtPath(databasePath)
      t.ok(meta.applied_seq > seq, 'the durable checkpoint includes command SYNC')
      t.equal(meta.pending_cmd_seq, null, 'SYNC clears the pending command')
      t.deepEqual(meta.applied_local_digest, applyMetaAtPath(databasePath).local_digest,
        'SYNC retains the apply digest matching DB2')
      t.equal(errors.length, 1, 'recovery and later commands emit no further errors')
    })
}

test('DB2 open failure closes DB1 and requires a fresh object',
  async (t) => {
    const databasePath = uniquePath('db2-open-retry')
    const monsterDatabasePath = `${databasePath}2`
    const backupPath = `${monsterDatabasePath}.open-failure-backup`
    removePair(databasePath)
    fs.rmSync(backupPath, { force: true })

    const seed = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    seed.on('warn', noop)
    seed.on('error', noop)
    let node = null
    let replacement = null
    t.teardown(() => {
      closeNodesQuietly([seed, node, replacement].filter(Boolean))
      fs.rmSync(monsterDatabasePath, { recursive: true, force: true })
      if (fs.existsSync(backupPath)) {
        fs.renameSync(backupPath, monsterDatabasePath)
      }
      removePair(databasePath)
      fs.rmSync(backupPath, { force: true })
    })

    seed.open()
    seed.close()
    node = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    node.on('warn', noop)
    node.on('error', noop)

    fs.renameSync(monsterDatabasePath, backupPath)
    fs.mkdirSync(monsterDatabasePath)
    const failure = errorOfCall(() => node.open())
    t.ok(failure instanceof ErrorWithCode,
      'opening DB2 at a directory throws ErrorWithCode')
    t.match(failure.message, /^DB2 open /,
      'DB2 open failure has operation context')
    t.equal(failure.code, SQLITE_ERROR,
      'DB2 open failure uses SQLITE_ERROR')
    t.ok(Number.isSafeInteger(failure.sqlCode),
      'DB2 open failure retains its native SQLite code')
    t.notOk(node.log.isOpen, 'DB2 open failure cleanup closes DB1')
    t.equal(node.db, null, 'DB2 open failure retains no DB2 handle')
    t.notOk(node.isOpen, 'DB2 open failure never publishes the node')

    fs.rmSync(monsterDatabasePath, { recursive: true, force: true })
    fs.renameSync(backupPath, monsterDatabasePath)
    t.throws(() => node.open(), /node not open/,
      'the failed object cannot retry after the path is repaired')
    t.notOk(node.log.isOpen,
      'the rejected retry does not reopen DB1 on the failed object')

    replacement = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    replacement.on('warn', noop)
    replacement.on('error', noop)
    replacement.open()
    t.ok(replacement.log.isOpen, 'a fresh object reopens DB1 after path repair')
    t.ok(replacement.db !== null, 'the fresh object opens repaired DB2')
    t.ok(replacement.isOpen, 'the fresh object publishes after path repair')
    replacement.close()
  })

test('DB2 close failure retains its handle for a close retry',
  async (t) => {
    const databasePath = uniquePath('close-retry')
    removePair(databasePath)
    const node = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    node.on('warn', noop)
    node.on('error', noop)
    t.teardown(() => {
      closeNodesQuietly([node])
      removePair(databasePath)
    })
    node.open()

    const db = node.db
    const closeDb = db.close.bind(db)
    const failure = new Error('injected DB2 close failure')
    let attempts = 0
    db.close = () => {
      attempts++
      if (attempts === 1) { throw failure }
      return closeDb()
    }

    const first = errorOfCall(() => node.close())
    t.ok(first instanceof ErrorWithCode,
      'first DB2 close failure throws ErrorWithCode')
    t.equal(first.message, 'DB2 close injected DB2 close failure',
      'first DB2 close failure uses the normalized message')
    t.equal(first.code, SQLITE_ERROR,
      'first DB2 close failure uses SQLITE_ERROR')
    t.equal(first.sqlCode, null,
      'injected DB2 close failure has no native SQLite code')
    t.equal(attempts, 1, 'first close attempts DB2 once')
    t.equal(node.db, db, 'failed DB2 close retains the public handle')
    t.notOk(node.log.isOpen,
      'RaftNode still closes independent DB1 after DB2 close fails')
    t.notOk(node.isOpen, 'failed close leaves the node unavailable')
    t.deepEqual(node.eventNames(), [],
      'failed MonsterFt close still removes all listeners')
    const storageOpenErr = errorOfCall(() => node.del())
    t.ok(storageOpenErr instanceof ErrorWithCode,
      'a retained DB2 handle rejects deletion with ErrorWithCode')
    t.equal(storageOpenErr.message, 'DB2 is open',
      'a retained DB2 handle reports open Monster storage')
    t.equal(storageOpenErr.code, LOG_OPEN,
      'open Monster storage uses LOG_OPEN')
    t.equal(storageOpenErr.sqlCode, null,
      'open Monster storage has no SQLite error code')
    t.ok(fs.existsSync(databasePath) && fs.existsSync(`${databasePath}2`),
      'rejected deletion leaves both database files intact')

    node.close()
    t.equal(attempts, 2, 'second close retries the retained DB2 handle')
    t.equal(node.db, null, 'successful retry clears the DB2 handle')
    t.notOk(node.log.isOpen, 'DB1 remains closed after the retry')
    node.del()
    t.notOk(fs.existsSync(databasePath) || fs.existsSync(`${databasePath}2`),
      'pair deletion succeeds after every handle closes')
  })

test('failed Monster open cleanup is finite and retains both failed handles',
  async (t) => {
    const databasePath = uniquePath('failed-open-cleanup')
    removePair(databasePath)
    const openFailure = new Error('injected validation failure')
    const dbFailure = new Error('injected DB2 open-cleanup failure')
    const logFailure = new Error('injected DB1 open-cleanup failure')
    const order = []
    const fatals = []
    const errors = []
    let db = null
    let dbAttempts = 0
    let logAttempts = 0

    class FailedOpenMonsterFt extends MonsterFt {
      _monsterInit(db2) {
        super._monsterInit(db2)
        const currentDb = this.db
        db = currentDb
        const closeDb = currentDb.close.bind(currentDb)
        const closeLog = this.log.close.bind(this.log)
        currentDb.close = () => {
          dbAttempts++
          order.push(`DB2:${dbAttempts}`)
          if (dbAttempts === 1) { throw dbFailure }
          return closeDb()
        }
        this.log.close = () => {
          logAttempts++
          order.push(`DB1:${logAttempts}`)
          if (logAttempts === 1) { throw logFailure }
          return closeLog()
        }
        throw openFailure
      }
    }

    const node = new FailedOpenMonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    node.on('warn', noop)
    node.on('fatal', (err) => fatals.push(err))
    node.on('error', (err) => errors.push(err))
    const errored = new Promise((resolve) => node.once('error', resolve))
    t.teardown(() => {
      closeNodesQuietly([node])
      removePair(databasePath)
    })

    const failed = errorOfCall(() => node.open())
    t.ok(failed instanceof ErrorWithCode,
      'open normalizes the checkpoint-validation failure')
    t.notEqual(failed, openFailure,
      'open replaces the uncoded checkpoint-validation failure')
    t.equal(failed.message, 'DB2 open injected validation failure',
      'open adds DB2 context to the validation failure')
    t.equal(failed.code, SQLITE_ERROR,
      'open classifies the validation failure')
    t.equal(failed.sqlCode, null,
      'the synthetic validation failure has no SQLite error code')
    t.deepEqual(order, ['DB2:1', 'DB1:1'],
      'failed open attempts DB2 then DB1 cleanup once')
    t.deepEqual([dbAttempts, logAttempts], [1, 1],
      'failed open makes exactly one immediate attempt per handle')
    t.equal(node.db, db, 'failed DB2 cleanup retains its handle')
    t.ok(node.log.isOpen, 'failed DB1 cleanup retains its handle')
    t.notOk(node.isOpen, 'failed open leaves the node unavailable')
    t.equal(fatals.length, 1, 'reports one cleanup failure through fatal')
    t.ok(fatals[0] instanceof ErrorWithCode,
      'combined cleanup reports a coded first DB2 failure')
    t.equal(
      fatals[0].message,
      'DB2 close injected DB2 open-cleanup failure',
      'combined cleanup normalizes the first DB2 failure message')
    t.equal(fatals[0].code, SQLITE_ERROR,
      'combined cleanup classifies the first DB2 failure')
    t.notOk(fatals[0] instanceof AggregateError,
      'combined cleanup does not aggregate failures')
    t.deepEqual(errors, [], 'public cleanup error reporting is deferred')
    t.ok(node.eventNames().includes('error'),
      'failed-open cleanup retains listeners while reporting is pending')

    t.equal(await errored, fatals[0],
      'public error reporting preserves the first cleanup failure')
    await new Promise((resolve) => setImmediate(resolve))
    t.deepEqual(node.eventNames(), [],
      'failed MonsterFt open removes listeners after fatal reporting')
    t.deepEqual([dbAttempts, logAttempts], [1, 1],
      'failed open schedules no automatic cleanup retry')
    t.throws(() => node.open(), /node not open/,
      'the failed object cannot retry open')
    t.deepEqual([dbAttempts, logAttempts], [1, 1],
      'rejected reopen touches neither retained handle')

    node.close()
    t.deepEqual(order, ['DB2:1', 'DB1:1', 'DB2:2', 'DB1:2'],
      'explicit close retries each retained handle exactly once')
    t.equal(node.db, null, 'explicit close releases retained DB2')
    t.notOk(node.log.isOpen, 'explicit close releases retained DB1')
  })

test('DB1-only close failure releases DB2 and retries only DB1',
  async (t) => {
    const databasePath = uniquePath('db1-only-close-failure')
    removePair(databasePath)
    const node = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    node.on('warn', noop)
    node.on('error', noop)
    t.teardown(() => {
      closeNodesQuietly([node])
      removePair(databasePath)
    })
    node.open()

    const db = node.db
    const closeDb = db.close.bind(db)
    const closeLog = node.log.close.bind(node.log)
    const logFailure = new Error('injected DB1 close failure')
    const order = []
    let dbAttempts = 0
    let logAttempts = 0
    db.close = () => {
      dbAttempts++
      order.push(`DB2:${dbAttempts}`)
      return closeDb()
    }
    node.log.close = () => {
      logAttempts++
      order.push(`DB1:${logAttempts}`)
      if (logAttempts === 1) { throw logFailure }
      return closeLog()
    }

    const first = errorOfCall(() => node.close())
    t.equal(first, logFailure, 'close preserves the DB1-only failure')
    t.deepEqual(order, ['DB2:1', 'DB1:1'],
      'first close attempts DB2 then DB1 once')
    t.equal(node.db, null, 'successful DB2 close clears its handle')
    t.ok(node.log.isOpen, 'failed DB1 close retains its handle')
    t.throws(() => db.prepare('SELECT 1'), /closed|not open/i,
      'DB2 is physically closed despite the DB1 failure')

    await new Promise((resolve) => setImmediate(resolve))
    t.deepEqual([dbAttempts, logAttempts], [1, 1],
      'DB1 failure schedules no automatic cleanup retry')

    node.close()
    t.deepEqual(order, ['DB2:1', 'DB1:1', 'DB1:2'],
      'explicit close skips released DB2 and retries only DB1')
    t.deepEqual([dbAttempts, logAttempts], [1, 2],
      'each close invocation touches only its retained handles')
    t.notOk(node.log.isOpen, 'explicit close releases retained DB1')
  })

test('combined DB2 and DB1 close failures are finite and retryable',
  async (t) => {
    const databasePath = uniquePath('combined-close-failure')
    removePair(databasePath)
    const node = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    node.on('warn', noop)
    node.on('error', noop)
    t.teardown(() => {
      closeNodesQuietly([node])
      removePair(databasePath)
    })
    node.open()

    const db = node.db
    const closeDb = db.close.bind(db)
    const closeLog = node.log.close.bind(node.log)
    const dbFailure = new Error('injected DB2 close failure')
    const logFailure = new Error('injected DB1 close failure')
    const order = []
    let dbAttempts = 0
    let logAttempts = 0
    db.close = () => {
      dbAttempts++
      order.push(`DB2:${dbAttempts}`)
      if (dbAttempts === 1) { throw dbFailure }
      return closeDb()
    }
    node.log.close = () => {
      logAttempts++
      order.push(`DB1:${logAttempts}`)
      if (logAttempts === 1) { throw logFailure }
      return closeLog()
    }

    const first = errorOfCall(() => node.close())
    t.ok(first instanceof ErrorWithCode,
      'combined close failures return a coded first DB2 failure')
    t.equal(first.message, 'DB2 close injected DB2 close failure',
      'combined close failures normalize the first DB2 failure message')
    t.equal(first.code, SQLITE_ERROR,
      'combined close failures classify the first DB2 failure')
    t.notOk(first instanceof AggregateError,
      'combined close failures are not aggregated')
    t.deepEqual(order, ['DB2:1', 'DB1:1'],
      'one close call attempts each database exactly once')
    t.equal(node.db, db, 'failed DB2 close retains its handle')
    t.ok(node.log.isOpen, 'failed DB1 close retains its handle')
    t.notOk(node.isOpen, 'combined failure still makes the node unavailable')

    await new Promise((resolve) => setImmediate(resolve))
    t.deepEqual([dbAttempts, logAttempts], [1, 1],
      'close failures schedule no automatic cleanup retries')

    node.close()
    t.deepEqual(order, ['DB2:1', 'DB1:1', 'DB2:2', 'DB1:2'],
      'the explicit close retries DB2 then DB1 exactly once')
    t.equal(node.db, null, 'the explicit retry releases DB2')
    t.notOk(node.log.isOpen, 'the explicit retry releases DB1')
  })

test('change listener can close both databases during open exactly once',
  async (t) => {
    const databasePath = uniquePath('listener-close-open')
    removePair(databasePath)
    let dbCloseCalls = 0
    let logCloseCalls = 0
    const changes = []
    class CloseOnPublishMonsterFt extends MonsterFt {
      _startRaft() {
        const db = this.db
        const closeDb = db.close.bind(db)
        const closeLog = this.log.close.bind(this.log)
        db.close = () => {
          dbCloseCalls++
          return closeDb()
        }
        this.log.close = () => {
          logCloseCalls++
          return closeLog()
        }
        return super._startRaft()
      }
    }
    const node = new CloseOnPublishMonsterFt(
      '1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
      },
    )
    node.on('warn', noop)
    node.on('error', noop)
    const storageAtClosedChange = []
    node.on('change', (change) => {
      changes.push(change.open)
      if (!change.open) {
        const db2Open = node.db !== null &&
          errorOfCall(() => node.db.prepare('SELECT 1').get()) === null
        storageAtClosedChange.push({
          db2Open,
          db1Open: node.log.isOpen,
        })
        return
      }
      if (change.open) { node.close() }
    })
    t.teardown(() => {
      closeNodesQuietly([node])
      removePair(databasePath)
    })

    t.throws(() => node.open(), /node not open/,
      'open fails after its change listener closes the node')
    t.deepEqual(changes, [true, false],
      'the listener observes open followed by synchronous close')
    t.deepEqual(storageAtClosedChange, [{
      db2Open: true,
      db1Open: true,
    }], 'closed publication happens before physical DB2 and DB1 cleanup')
    t.equal(dbCloseCalls, 1, 'listener-triggered close closes DB2 once')
    t.equal(logCloseCalls, 1, 'listener-triggered close closes DB1 once')
    t.equal(node.db, null, 'listener-triggered close clears DB2')
    t.notOk(node.log.isOpen, 'listener-triggered close clears DB1')
    t.notOk(node.isOpen, 'open does not succeed after listener close')

    await new Promise((resolve) => setImmediate(resolve))
    t.deepEqual([dbCloseCalls, logCloseCalls], [1, 1],
      'open does not schedule a second cleanup attempt')
    t.throws(() => node.open(), /node not open/,
      'the listener-closed object cannot reopen')
    t.deepEqual([dbCloseCalls, logCloseCalls], [1, 1],
      'rejected reopen touches neither database')
  })

test('Monster close abandons active DB2 FIFO work and rejects its queue',
  async (t) => {
    const databasePath = uniquePath('close-db2-fifo')
    removePair(databasePath)
    const gate = deferred()
    const errors = []
    let activeRan = false
    let activeDb = null
    let queuedRan = false
    const node = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    node.on('warn', noop)
    node.on('error', (err) => errors.push(err))
    t.teardown(() => {
      gate.resolve()
      closeNodesQuietly([node])
      removePair(databasePath)
    })
    node.open()

    const active = errorOf(node._monsterRunDb((db) => {
      activeRan = true
      activeDb = db
      return gate.promise
    }))
    const queued = errorOf(node._monsterRunDb(() => {
      queuedRan = true
    }))
    t.ok(activeRan, 'the active FIFO callback starts immediately')
    t.equal(activeDb, node.db,
      'the FIFO callback receives the active DB2 connection')
    t.notOk(queuedRan, 'the next FIFO callback remains queued')

    node.close()
    const admittedAfterClose = errorOf(node._monsterRunDb(noop))
    const [activeErr, queuedErr, admissionErr] = await Promise.all([
      active,
      queued,
      admittedAfterClose,
    ])
    t.equal(activeErr, node._shutdownError,
      'active FIFO work rejects with the shared shutdown error')
    t.equal(queuedErr, node._shutdownError,
      'queued FIFO work rejects with the shared shutdown error')
    t.equal(admissionErr, node._shutdownError,
      'new FIFO admission rejects with the shared shutdown error')
    t.equal(activeErr.code, NODE_NOT_OPEN,
      'shared DB2 error has NODE_NOT_OPEN code')
    t.equal(activeErr.message, 'node not open',
      'shared DB2 error has the not-open message')
    t.notOk(queuedRan, 'close never starts queued DB2 work')

    gate.resolve()
    await new Promise((resolve) => setImmediate(resolve))
    t.notOk(node._monsterDbActive, 'late active settlement releases FIFO state')
    t.equal(errors.length, 0, 'late FIFO settlement emits no node error')
  })

test('two-file cluster commits, checkpoints exact entries, and restarts',
  async (t) => {
    const fixture = clusterFixture(t, 'restart')
    const first = fixture.build()
    const leader = await openAndElect(first.nodes)
    await waitApplied(first.nodes, leader._commitSeq)

    const initialNoops = first.applies.filter(({ buf }) => buf === null)
    t.deepEqual(initialNoops.map(({
      id, term, seq, index, matchIndex, isMonsterDb, separate,
    }) => ({
      id, term, seq, index, matchIndex, isMonsterDb, separate,
    })).sort((left, right) => left.id.localeCompare(right.id)), ids.map((id) => ({
      id,
      term: leader.term,
      seq: 0n,
      index: 0,
      matchIndex: null,
      isMonsterDb: true,
      separate: true,
    })), 'every initial no-op bootstraps through apply on DB2')
    t.deepEqual(first.nodes.map((node) => tableNames(node.db)), [
      ['monsterft_meta_apply', 'two_file_items'],
      ['monsterft_meta_apply', 'two_file_items'],
      ['monsterft_meta_apply', 'two_file_items'],
    ], 'sequence-zero application commits schema on every member')
    for (const node of first.nodes) {
      const { term, entry } = node.log.db.prepare(`
        SELECT term, entry FROM raft_log WHERE seq = 0
      `).get()
      const prefix = Buffer.alloc(8)
      prefix.writeBigUInt64LE(term)
      const expected = crypto.createHash('sha256').update(prefix).digest()
      const { applied_entry_hash: hash } = node.log.db.prepare(`
        SELECT applied_entry_hash FROM monsterft_meta WHERE id = 1
      `).get()
      t.equal(term, leader.term,
        `node ${node.id} stores the initial no-op term separately`)
      t.equal(entry.length, 0,
        `node ${node.id} stores no prefix in the empty no-op payload`)
      t.deepEqual(Buffer.from(hash), expected,
        `node ${node.id} preserves the no-op checkpoint hash bytes`)
      const receipt = node.db.prepare('SELECT * FROM monsterft_meta_apply WHERE id = 1').get()
      t.equal(receipt.seq, 0n, `node ${node.id} records setup in DB2`)
      t.deepEqual(Buffer.from(receipt.entry_hash), expected,
        `node ${node.id} records the actual setup entry hash`)
      t.deepEqual(Buffer.from(receipt.local_digest), Buffer.alloc(32),
        `node ${node.id} uses the zero setup digest`)
    }

    const synced = nextSync(leader)
    const [cmdSeq, result] = await leader.append(toBuf({
      key: 'before-restart',
      value: 41,
    }))
    const sync = await withTimeout(synced, 'before-restart SYNC event')
    const { syncSeq } = sync
    t.deepEqual(result, { key: 'before-restart', value: 41 },
      'the quorum returns the agreed apply result')
    t.equal(sync.cmdSeq, cmdSeq,
      'the durable SYNC event identifies the completed CMD')
    await waitApplied(first.nodes, syncSeq)
    t.equal(unpack(leader.head).type, 'sync',
      'the committed Raft head is a SYNC')
    t.deepEqual(first.nodes.map((node) => valueAt(node, 'before-restart')),
      [41, 41, 41], 'application state is committed in every DB2')
    t.ok(first.applies.every((call) => call.isMonsterDb && call.separate),
      'every apply callback receives DB2, never DB1')

    for (const node of first.nodes) {
      const meta = node.log.db.prepare(`
        SELECT applied_seq, applied_entry_hash,
               pending_cmd_seq, applied_local_digest
        FROM monsterft_meta WHERE id = 1
      `).get()
      const row = node.log.db.prepare(`
        SELECT term, entry FROM raft_log WHERE seq = ?
      `).get(meta.applied_seq)
      const prefix = Buffer.alloc(8)
      prefix.writeBigUInt64LE(row.term)
      const expected = crypto.createHash('sha256')
        .update(prefix)
        .update(Buffer.from(row.entry))
        .digest()
      t.equal(meta.applied_seq, node._applySeq,
        `node ${node.id} publishes its durable DB1 checkpoint`)
      t.ok(Buffer.from(meta.applied_entry_hash).equals(expected),
        `node ${node.id} preserves the term-and-payload checkpoint hash`)
      t.equal(meta.pending_cmd_seq, null,
        `node ${node.id} clears the resolved pending sequence`)
      t.deepEqual(meta.applied_local_digest,
        node.db.prepare('SELECT local_digest FROM monsterft_meta_apply WHERE id = 1').get().local_digest,
        `node ${node.id} retains the resolved apply digest`)
      t.equal(node._monsterPendingCommand, null,
        `node ${node.id} clears the resolved pending cache`)
    }

    closeNodes(first.nodes)
    const second = fixture.build()
    openNodes(second.nodes)
    t.deepEqual(second.nodes.map((node) => valueAt(node, 'before-restart')),
      [41, 41, 41], 'restart loads application state from DB2')
    t.ok(second.nodes.every((node) => node._monsterPendingCommand === null),
      'restart hydrates empty pending caches after a resolved command')

    const beforeRestartElection = second.nodes[0]._applySeq
    const restartedLeader = await openAndElect(second.nodes)
    await waitApplied(second.nodes, restartedLeader._commitSeq)
    t.equal(restartedLeader._commitSeq, beforeRestartElection + 1n,
      'the restarted leader commits a later election no-op')
    t.ok(second.nodes.every((node) => {
      return node._applySeq === restartedLeader._commitSeq
    }), 'the later no-op advances every restored DB1 checkpoint')
    t.equal(second.applies.length, 0,
      'the later no-op invokes no user apply callback')
    const restartedSynced = nextSync(restartedLeader)
    const [restartedCmd, restartedResult] = await restartedLeader.append(toBuf({
      key: 'after-restart',
      value: 42,
    }))
    const restartedSync = await withTimeout(
      restartedSynced,
      'after-restart SYNC event',
    )
    t.equal(restartedSync.cmdSeq, restartedCmd,
      'the restarted command emits its durable local SYNC')
    t.deepEqual(restartedResult, { key: 'after-restart', value: 42 },
      'the restarted pair resumes normal command processing')
    await waitApplied(second.nodes, restartedSync.syncSeq)
    t.deepEqual(second.nodes.map((node) => valueAt(node, 'after-restart')),
      [42, 42, 42], 'the restarted cluster advances every DB2')
    t.equal(fixture.errors.length, 0,
      'normal two-file operation emits no fatal errors')
  })

for (const missing of ['row', 'table', 'all DB2 tables']) {
  test(`open rejects a missing metadata ${missing} without replaying application state`, async (t) => {
    const fixture = clusterFixture(t, `missing-metadata-${missing}`)
    const apply = (db, buf, term, seq) => {
      if (seq === 0n) {
        initializeApp(db)
        return
      }
      db.prepare(`
        INSERT INTO two_file_items (key, value) VALUES ('counter', 1)
        ON CONFLICT(key) DO UPDATE SET value = value + 1
      `).run()
      return valueAt({ db }, 'counter')
    }
    const first = fixture.build({ apply })
    const leader = await openAndElect(first.nodes)
    const [cmdSeq] = await leader.append(toBuf({ increment: 1 }))
    await leader._monsterProtocol
    await waitApplied(first.nodes, cmdSeq + 1n)
    t.deepEqual(first.nodes.map((node) => valueAt(node, 'counter')), [1, 1, 1],
      'each replica applied the increment exactly once')
    closeNodes(first.nodes)

    const databasePath = fixture.paths.get(leader.id)
    const db = new DatabaseSync(`${databasePath}2`)
    if (missing === 'row') {
      db.exec('DELETE FROM monsterft_meta_apply')
    } else {
      db.exec('DROP TABLE monsterft_meta_apply')
      if (missing === 'all DB2 tables') { db.exec('DROP TABLE two_file_items') }
    }
    db.close()

    const second = fixture.build({ apply })
    const restored = second.nodes.find((node) => node.id === leader.id)
    const beforeMessages = fixture.bus.messages.length
    const err = errorOfCall(() => restored.open())
    t.ok(err instanceof ErrorWithCode, 'missing metadata throws ErrorWithCode')
    t.equal(err?.code, MONSTER_CORRUPT, 'missing metadata reports DB2 corruption')
    const expected = missing === 'row'
      ? /apply meta row is missing/
      : /meta init requires empty raft_log/
    t.match(err?.message ?? '', expected,
      'the error identifies why metadata cannot be restored')
    t.notOk(restored.isOpen || restored.log.isOpen || restored.db !== null,
      'failed startup closes both databases')
    t.equal(fixture.bus.messages.length, beforeMessages, 'startup sends no Raft messages')
    t.deepEqual(second.applies, [], 'startup does not replay completed commands')

    const saved = new DatabaseSync(`${databasePath}2`, { readOnly: true })
    try {
      if (missing === 'row') {
        t.deepEqual(saved.prepare('SELECT * FROM monsterft_meta_apply').all(), [],
          'startup does not recreate the lost checkpoint row')
      } else {
        t.notOk(tableNames(saved).includes('monsterft_meta_apply'),
          'startup does not recreate the lost metadata table')
      }
      if (missing !== 'all DB2 tables') {
        t.equal(Number(valueAt({ db: saved }, 'counter')), 1,
          'the persisted increment remains applied once')
      }
    } finally {
      saved.close()
    }
  })
}

test('an empty log does not allow reseeding existing DB2 state', (t) => {
  for (const missing of ['row', 'table']) {
    const fixture = clusterFixture(t, `empty-log-missing-${missing}`)
    const original = fixture.build().nodes[0]
    original.open()
    t.equal(original.seq, -1n, 'the original log has no entries')
    if (missing === 'row') {
      original.db.exec('DELETE FROM monsterft_meta_apply')
    } else {
      initializeApp(original.db)
      original.db.exec('DROP TABLE monsterft_meta_apply')
    }
    original.close()

    const restored = fixture.build().nodes[0]
    const err = errorOfCall(() => restored.open())
    t.equal(err?.code, MONSTER_CORRUPT, `missing ${missing} is rejected despite an empty log`)
    t.notOk(restored.isOpen || restored.log.isOpen || restored.db !== null,
      'failed startup closes both databases')
  }
  t.end()
})

test('failed fresh metadata initialization leaves an empty pair that can be retried', (t) => {
  const databasePath = uniquePath('metadata-init-rollback')
  removePair(databasePath)
  const nodes = []
  t.teardown(() => {
    closeNodesQuietly(nodes)
    removePair(databasePath)
  })

  class FailedInit extends MonsterFt {
    _monsterInit(db) {
      const exec = db.exec.bind(db)
      db.exec = (sql) => exec(sql.replace('INSERT INTO monsterft_meta_apply', 'INSERT INTO missing_table'))
      return super._monsterInit(db)
    }
  }
  const opts = { electionTimeout: 60_000, apply: applyApp }
  const failed = new FailedInit('1', ids, noop, databasePath, opts)
  nodes.push(failed)
  const err = errorOfCall(() => failed.open())
  t.equal(err?.code, SQLITE_ERROR, 'an insert failure reports the SQLite error')
  t.notOk(failed.isOpen || failed.log.isOpen || failed.db !== null,
    'initialization failure closes both databases')
  const saved = new DatabaseSync(`${databasePath}2`, { readOnly: true })
  try {
    t.deepEqual(tableNames(saved), [], 'the failed insertion also rolls back table creation')
  } finally {
    saved.close()
  }

  const restored = new MonsterFt('1', ids, noop, databasePath, opts)
  nodes.push(restored)
  restored.open()
  t.ok(restored.isOpen, 'an existing empty pair initializes successfully on retry')
  t.equal(restored._applySeq, -1n, 'the retry starts at the initial checkpoint')
  t.equal(restored.db.prepare('SELECT count(*) AS count FROM monsterft_meta_apply').get().count, 1n,
    'the retry creates exactly one metadata row')
  t.end()
})

test('open rejects a DB1 checkpoint hash mismatch', async (t) => {
  const fixture = clusterFixture(t, 'hash-mismatch')
  const first = fixture.build()
  await openAndElect(first.nodes)
  const checkpoint = first.nodes[0]._applySeq
  t.ok(checkpoint >= 0n, 'leader election creates an applied checkpoint')
  closeNodes(first.nodes)

  const databasePath = fixture.paths.get('1')
  const db = new DatabaseSync(databasePath, { readBigInts: true })
  const before = db.prepare(`
    SELECT applied_entry_hash
    FROM monsterft_meta WHERE id = 1
  `).get()
  const corrupted = Buffer.from(before.applied_entry_hash)
  corrupted[0] ^= 0xff
  db.prepare(`
    UPDATE monsterft_meta SET applied_entry_hash = ? WHERE id = 1
  `).run(corrupted)
  db.close()

  const second = fixture.build()
  const err = errorOfCall(() => second.nodes[0].open())
  t.ok(err instanceof ErrorWithCode,
    'the checkpoint hash mismatch throws ErrorWithCode')
  t.equal(err?.code, MONSTER_CORRUPT,
    'the checkpoint hash mismatch reports MONSTER_CORRUPT')
  t.equal(err?.sqlCode, null,
    'the checkpoint hash mismatch has no SQLite error code')
  t.match(
    err?.message ?? '',
    /applied entry hash does not match/,
    'MonsterFt compares the DB1 checkpoint hash with its exact log entry',
  )
  t.notOk(second.nodes[0].log.isOpen,
    'mismatch cleanup closes DB1 before open rejects')
  t.equal(second.nodes[0].db, null,
    'mismatch cleanup closes DB2 before open rejects')
  t.notOk(second.nodes[0].isOpen,
    'a mismatched pair is never published as open')
})

for (const field of ['term', 'entry']) {
  test(`open rejects a changed DB1 checkpoint ${field} before applying`, async (t) => {
    const fixture = clusterFixture(t, `checkpoint-${field}-change`)
    const first = fixture.build()
    const leader = await openAndElect(first.nodes)
    const synced = nextSync(leader)
    await leader.append(toBuf({ key: 'checkpoint', value: 17 }))
    const { syncSeq } = await withTimeout(synced, 'checkpoint SYNC')
    await waitApplied(first.nodes, syncSeq)
    closeNodes(first.nodes)

    const databasePath = fixture.paths.get('1')
    const db = new DatabaseSync(databasePath, { readBigInts: true })
    const readEntry = db.prepare(`
      SELECT term, entry FROM raft_log WHERE seq = ?
    `)
    const before = readEntry.get(syncSeq)
    const changed = field === 'term'
      ? before.term + 1n
      : Buffer.from(pack({ ...unpack(before.entry), digest: Buffer.alloc(32, 0xff) }))
    db.prepare(`UPDATE raft_log SET ${field} = ? WHERE seq = ?`).run(changed, syncSeq)
    const after = readEntry.get(syncSeq)
    t.equal(after.term, field === 'term' ? changed : before.term,
      'only the selected field changes the stored term')
    t.deepEqual(Buffer.from(after.entry),
      Buffer.from(field === 'entry' ? changed : before.entry),
      'only the selected field changes the stored payload')
    db.close()

    const second = fixture.build()
    const err = errorOfCall(() => second.nodes[0].open())
    t.equal(err?.code, MONSTER_CORRUPT,
      'a valid SQL value with a different checkpoint hash is Monster corruption')
    t.match(err?.message ?? '', /applied entry hash does not match/,
      'the stored checkpoint authenticates both the term and payload')
    t.deepEqual(second.applies, [],
      'checkpoint validation rejects before invoking apply callbacks')
    t.notOk(second.nodes[0].isOpen || second.nodes[0].log.isOpen,
      'failed startup releases DB1 without publishing an open node')
    t.equal(second.nodes[0].db, null, 'failed startup releases DB2')

    const repaired = new DatabaseSync(databasePath)
    repaired.prepare(`UPDATE raft_log SET ${field} = ? WHERE seq = ?`)
      .run(before[field], syncSeq)
    repaired.close()
    const third = fixture.build()
    third.nodes[0].open()
    t.equal(valueAt(third.nodes[0], 'checkpoint'), 17,
      'restoring the original field restores the valid database pair')
    t.deepEqual(third.applies, [],
      'a valid restart does not reapply completed commands')
    third.nodes[0].close()
  })
}

test('checkpoint ahead of DB1 rejects and requires a fresh object',
  async (t) => {
    const databasePath = uniquePath('checkpoint-ahead')
    removePair(databasePath)
    const nodes = []
    const seed = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    seed.on('warn', noop)
    seed.on('error', noop)
    nodes.push(seed)
    t.teardown(() => {
      closeNodesQuietly(nodes)
      removePair(databasePath)
    })
    seed.open()
    seed.close()

    const corrupt = new DatabaseSync(
      databasePath,
      { readBigInts: true },
    )
    corrupt.prepare(`
      UPDATE monsterft_meta
      SET applied_seq = 0, applied_entry_hash = ?, applied_local_digest = zeroblob(32)
      WHERE id = 1
    `).run(Buffer.alloc(32))
    corrupt.close()

    const build = () => {
      const current = new MonsterFt('1', ids, noop, databasePath, {
        electionTimeout: 60_000,
        apply: applyApp,
      })
      current.on('warn', noop)
      current.on('error', noop)
      nodes.push(current)
      return current
    }

    const failed = build()
    const err = errorOfCall(() => failed.open())
    t.ok(err instanceof ErrorWithCode,
      'the checkpoint ahead of DB1 throws ErrorWithCode')
    t.equal(err?.code, MONSTER_CORRUPT,
      'the checkpoint ahead of DB1 reports MONSTER_CORRUPT')
    t.equal(err?.sqlCode, null,
      'the checkpoint ahead of DB1 has no SQLite error code')
    t.match(
      err?.message ?? '',
      /applied log entry not found/,
      'MonsterFt rejects a checkpoint ahead of the DB1 log',
    )
    t.notOk(failed.log.isOpen, 'invalid checkpoint cleanup closes DB1')
    t.equal(failed.db, null, 'invalid checkpoint cleanup closes DB2')

    const repaired = new DatabaseSync(
      databasePath,
      { readBigInts: true },
    )
    repaired.prepare(`
      UPDATE monsterft_meta
      SET applied_seq = -1, applied_entry_hash = NULL, applied_local_digest = NULL
      WHERE id = 1
    `).run()
    repaired.close()

    t.throws(() => failed.open(), /node not open/,
      'the failed object cannot reopen after its checkpoint is repaired')

    const replacement = build()
    replacement.open()
    t.ok(replacement.isOpen, 'the fresh object opens the repaired pair')
    replacement.close()
  })

test('repair state 1 reopens live while state 2 requires certification',
  async (t) => {
    const databasePath = uniquePath('repair-required-open')
    removePair(databasePath)
    const nodes = []
    const build = () => {
      const node = new MonsterFt('1', ids, noop, databasePath, {
        electionTimeout: 60_000,
        apply: applyApp,
      })
      node.on('warn', noop)
      node.on('error', noop)
      nodes.push(node)
      return node
    }
    t.teardown(() => {
      closeNodesQuietly(nodes)
      removePair(databasePath)
    })

    const seed = build()
    seed.open()
    seed.close()

    const db = new DatabaseSync(databasePath, { readBigInts: true })
    db.prepare(`
      UPDATE monsterft_meta SET repair_state = 1 WHERE id = 1
    `).run()
    db.close()

    const liveFenced = build()
    liveFenced.open()
    t.equal(liveFenced._monsterRepairState, 1,
      'open hydrates durable repair state 1')
    t.equal(liveFenced.isOpen, true,
      'repair state 1 publishes an open Raft node')
    const commandError = await errorOf(liveFenced.append(
      toBuf({ key: 'repair-state-1', value: 1 }),
    ))
    t.equal(commandError?.code, REPAIR_QUORUM_IMPOSSIBLE,
      'the live repair fence rejects Monster CMDs with state-1 code')
    t.equal(liveFenced.isOpen, true,
      'state-1 command fencing leaves DB1 and DB2 online')
    liveFenced.close()

    const state2 = new DatabaseSync(databasePath, {
      readBigInts: true,
    })
    state2.prepare(`
      UPDATE monsterft_meta SET repair_state = 2 WHERE id = 1
    `).run()
    state2.close()

    const terminalFenced = build()
    const err = errorOfCall(() => terminalFenced.open())
    t.equal(err?.code, REPAIR_OUTSIDE_AGREEMENT,
      'open reports durable repair state 2')
    t.equal(terminalFenced.log.isOpen, false, 'state-2 open closes DB1')
    t.equal(terminalFenced.db, null, 'state-2 open closes DB2')
    t.equal(terminalFenced.isOpen, false,
      'state-2 open never starts or publishes Raft')

    MonsterFt.certify(databasePath)
    t.throws(() => terminalFenced.open(), /node not open/,
      'the failed-open object remains terminal after certification')

    const repaired = build()
    repaired.open()
    t.ok(repaired.isOpen, 'the fresh object starts normally')
    repaired.close()
    t.end()
  })

test('throwing command warning listeners do not interrupt application or agreement',
  async (t) => {
    const fixture = clusterFixture(t, 'throwing-command-warn')
    const cluster = fixture.build({
      apply: (db, buf, term, seq) => {
        const result = applyApp(db, buf, term, seq)
        if (buf !== null && toObj(buf).key === 'rejected') {
          throw new Error('command rejected')
        }
        return result
      },
    })
    const leader = await openAndElect(cluster.nodes)
    const listenerErrors = new Map()
    const warnings = []
    for (const node of cluster.nodes) {
      const failure = new Error(`warning listener ${node.id} failed`)
      listenerErrors.set(node.id, failure)
      node.on('warn', () => { throw failure })
      node.on('warn', (err) => warnings.push({ id: node.id, err }))
    }

    const rejected = await withTimeout(errorOf(leader.append(
      toBuf({ key: 'rejected', value: 30 }),
    )), 'rejection with throwing warning listeners')
    await waitApplied(cluster.nodes, leader._applySeq)
    t.equal(rejected?.message, 'command rejected',
      'the caller receives the original application failure')
    t.deepEqual(warnings.map(({ id }) => id).sort(), ids,
      'later warning listeners see each failed item despite earlier throws')
    t.deepEqual(fixture.errors.map(({ id, err }) => [id, err]).sort(),
      [...listenerErrors.entries()],
      'listener exceptions are surfaced without changing the command outcome')
    t.ok(cluster.nodes.every((node) => node.isOpen && node._monsterRepairState === 0),
      'all nodes remain open and in agreement')
    t.deepEqual(cluster.nodes.map((node) => valueAt(node, 'rejected')),
      [null, null, null], 'the failed writes still roll back on every node')
    await leader.append(toBuf({ key: 'next', value: 31 }))
    await waitApplied(cluster.nodes, leader._applySeq)
    t.deepEqual(cluster.nodes.map((node) => valueAt(node, 'next')),
      [31, 31, 31], 'later application continues normally')
  })

test('a command warning listener can close before rollback or later batch items',
  async (t) => {
    const fixture = clusterFixture(t, 'closing-command-warn')
    const cluster = fixture.build({
      apply: (db, buf, term, seq) => {
        const result = applyApp(db, buf, term, seq)
        if (buf !== null && toObj(buf).key === 'rejected') {
          throw new Error('close on command rejection')
        }
        return result
      },
    })
    const leader = await openAndElect(cluster.nodes)
    await waitApplied(cluster.nodes, leader._applySeq)
    const follower = cluster.nodes[2]
    const databasePath = fixture.paths.get(follower.id)
    const before = monsterMetaAtPath(databasePath)
    const db = follower.db
    const afterCloseSql = []
    let closed = false
    for (const method of ['exec', 'prepare']) {
      const original = db[method].bind(db)
      db[method] = (...args) => {
        if (closed) { afterCloseSql.push(args[0]) }
        return original(...args)
      }
    }
    const warnings = []
    follower.on('warn', (err) => {
      warnings.push(err)
      closed = true
      follower.close()
    })

    const [, outcomes] = await withTimeout(leader.appendBatch([
      toBuf({ key: 'before-warning', value: 40 }),
      toBuf({ key: 'rejected', value: 41 }),
      toBuf({ key: 'after-warning', value: 42 }),
    ]), 'batch with a closing warning listener')
    await new Promise((resolve) => setImmediate(resolve))
    t.deepEqual(outcomes.map(({ status }) => status),
      ['fulfilled', 'rejected', 'fulfilled'],
      'the healthy quorum completes the original batch')
    t.equal(warnings.length, 1, 'the closing follower receives its failed item warning')
    t.deepEqual(cluster.applies.filter(({ id, buf }) => {
      return id === follower.id && buf !== null
    }).map(({ index }) => index), [0, 1],
    'closing from warn prevents the next item from entering application')
    t.deepEqual(afterCloseSql, [],
      'closing from warn prevents further rollback, release, or checkpoint SQL')
    t.notOk(follower.isOpen, 'the follower closes during the warning')
    t.equal(follower.db, null, 'the listener closes DB2')
    t.notOk(follower.log.isOpen, 'the listener closes DB1')
    t.deepEqual(monsterMetaAtPath(databasePath), before,
      'the interrupted command does not advance its durable checkpoint')
    t.deepEqual(['before-warning', 'rejected', 'after-warning'].map((key) => {
      return valueAtPath(databasePath, key)
    }), [null, null, null], 'closing rolls back the entire active command')
    t.deepEqual(fixture.errors, [], 'abandoned application emits no later error')
  })

test('drain from a command warning waits for the active batch and its SYNC',
  async (t) => {
    const fixture = clusterFixture(t, 'draining-command-warn')
    const cluster = fixture.build({
      apply: (db, buf, term, seq) => {
        const result = applyApp(db, buf, term, seq)
        if (buf !== null && toObj(buf).key === 'rejected') {
          throw new Error('drain on command rejection')
        }
        return result
      },
    })
    const leader = await openAndElect(cluster.nodes)
    const follower = cluster.nodes[2]
    const databasePath = fixture.paths.get(follower.id)
    const warnings = []
    let draining = null
    let drained = false
    follower.on('warn', (err) => {
      warnings.push(err)
      draining = follower.drainCmd()
      draining.then(() => { drained = true }).catch(noop)
    })
    fixture.bus.holdOutcomes()
    const synced = nextSync(follower)
    const appending = leader.appendBatch([
      toBuf({ key: 'before-warning', value: 50 }),
      toBuf({ key: 'rejected', value: 51 }),
      toBuf({ key: 'after-warning', value: 52 }),
    ])
    appending.catch(noop)
    await waitFor(() => follower._monsterPendingCommand !== null,
      'warning-triggered drain captures the completed CMD')
    t.ok(draining, 'the warning listener starts drain')
    t.equal(warnings.length, 1, 'the failed item warns once before drain')
    t.notOk(drained, 'drain waits for agreement after the active batch commits')
    const rejected = await errorOf(follower.append(
      toBuf({ key: 'too-late', value: 53 }),
    ))
    t.equal(rejected?.code, DRAINING, 'the listener immediately closes command admission')
    t.deepEqual(['before-warning', 'rejected', 'after-warning'].map((key) => {
      return valueAt(follower, key)
    }), [50, null, 52], 'the active batch finishes with only its failed item rolled back')

    await fixture.bus.releaseOutcomes()
    const [cmdSeq, outcomes] = await withTimeout(appending, 'batch completion after warn drain')
    const sync = await withTimeout(synced, 'warn drain SYNC')
    await withTimeout(draining, 'warn drain completion')
    t.deepEqual(outcomes.map(({ status }) => status),
      ['fulfilled', 'rejected', 'fulfilled'], 'the caller receives the original batch results')
    t.equal(outcomes[1].reason.message, 'drain on command rejection',
      'draining preserves the application failure returned to the caller')
    t.equal(sync.cmdSeq, cmdSeq, 'drain observes the matching SYNC')
    t.ok(drained && !follower.isOpen, 'drain closes after SYNC and resolves')
    const meta = monsterMetaAtPath(databasePath)
    t.equal(meta.pending_cmd_seq, null, 'the closed pair has no pending CMD')
    t.ok(meta.applied_seq >= sync.syncSeq, 'the closed pair includes the SYNC checkpoint')
    t.deepEqual(['before-warning', 'rejected', 'after-warning'].map((key) => {
      return valueAtPath(databasePath, key)
    }), [50, null, 52], 'drain preserves completed work and the item rollback')
    t.deepEqual(fixture.errors, [], 'warning-triggered drain emits no errors')
  })

test('shutdown before DB2 commit rolls back and replays the durable DB1 entry',
  async (t) => {
    const databasePath = uniquePath('pre-commit-replay')
    removePair(databasePath)
    const entered = deferred()
    const release = deferred()
    const nodes = []
    const errors = []
    const fatals = []
    const warnings = []
    const failure = new Error('application rejected after close')
    let applyCalls = 0

    const build = (blocked) => {
      const node = new MonsterFt('1', ids, noop, databasePath, {
        electionTimeout: 60_000,
        apply: async (db, buf, term, seq, index, matchIndex) => {
          if (buf === null) {
            return applyApp(db, buf, term, seq, index, matchIndex)
          }
          applyCalls++
          if (blocked) {
            entered.resolve()
            await release.promise
            throw failure
          }
          return applyApp(db, buf, term, seq, index, matchIndex)
        },
      })
      node.on('warn', (err) => warnings.push(err))
      node.on('error', (err) => errors.push(err))
      node.on('fatal', (err) => fatals.push(err))
      nodes.push(node)
      return node
    }

    t.teardown(() => {
      release.resolve()
      closeNodesQuietly(nodes)
      removePair(databasePath)
    })

    const command = {
      type: 'cmd',
      items: [toBuf({ key: 'pre-commit', value: 12 })],
      matchIndex: ids.map(() => -1n),
    }
    const first = build(true)
    first.open()
    first.log.append(toNoopEntry(0n))
    first._commitSeq = 0n
    first._commitTerm = 0n
    await first._apply(0n)
    const cmdSeq = first.log.append(toEntry(0n, command))
    first._commitSeq = cmdSeq
    first._commitTerm = 0n
    const applying = first._apply(cmdSeq)
    applying.catch(noop)
    await withTimeout(entered.promise, 'pre-commit application entry')
    t.equal(first._monsterPendingCommand, null,
      'the pending cache is not published before DB2 COMMIT')

    const db1 = first.log.db
    const db2 = first.db
    first.close()
    t.equal(first.log.db, null, 'close clears DB1 before returning')
    t.equal(first.db, null, 'close clears DB2 before returning')
    t.throws(
      () => db1.prepare('SELECT 1'),
      /closed|not open/i,
      'close physically closes DB1 before returning',
    )
    t.throws(
      () => db2.prepare('SELECT 1'),
      /closed|not open/i,
      'close physically closes DB2 before returning',
    )

    const rolledBack = new DatabaseSync(`${databasePath}2`, {
      readOnly: true,
      readBigInts: true,
    })
    const rolledBackMeta = monsterMetaAtPath(databasePath)
    const rolledBackValue = rolledBack.prepare(`
      SELECT value FROM two_file_items WHERE key = 'pre-commit'
    `).get()
    rolledBack.close()
    t.equal(rolledBackMeta.applied_seq, 0n,
      'shutdown before COMMIT preserves the no-op checkpoint')
    t.equal(rolledBackMeta.pending_cmd_seq, null,
      'shutdown before COMMIT persists no pending command sequence')
    t.deepEqual(Buffer.from(rolledBackMeta.applied_local_digest), Buffer.alloc(32),
      'shutdown before COMMIT preserves the setup digest')
    t.equal(rolledBackValue, undefined,
      'shutdown before COMMIT rolls back application state')
    t.equal(first.log.isOpen, false,
      'shutdown does not wait for apply before closing DB1')

    release.resolve()
    t.equal(await errorOf(applying), null,
      'late pre-commit apply completion is silently abandoned')
    await new Promise((resolve) => setImmediate(resolve))
    t.equal(errors.length, 0, 'late apply emits no public error')
    t.equal(fatals.length, 0, 'late apply emits no fatal error')
    t.equal(warnings.length, 0, 'late rejection emits no command warning')

    const second = build(false)
    second.open()
    second._commitSeq = cmdSeq
    second._commitTerm = 0n
    await second._apply(cmdSeq)
    t.equal(applyCalls, 2,
      'restart replays the DB1 entry that did not commit in DB2')
    t.equal(valueAt(second, 'pre-commit'), 12,
      'replay commits the application transition to DB2')
    t.equal(second._applySeq, cmdSeq,
      'replay advances the DB1 checkpoint to its durable entry')
    const replayedMeta = second.log.db.prepare(`
      SELECT pending_cmd_seq, applied_local_digest
      FROM monsterft_meta WHERE id = 1
    `).get()
    t.equal(replayedMeta.pending_cmd_seq, cmdSeq,
      'replay durably installs the pending command sequence')
    t.equal(second._monsterPendingCommand.cmdSeq, cmdSeq,
      'replay publishes the matching pending-command cache')
    t.ok(Buffer.from(replayedMeta.applied_local_digest).equals(
      second._monsterPendingCommand.localDigest,
    ), 'replay aligns the durable and cached pending digests')
    second.close()
  })

test('shutdown immediately after DB2 commit prevents replay on restart',
  async (t) => {
    const databasePath = uniquePath('post-commit-no-replay')
    removePair(databasePath)
    const nodes = []
    const errors = []
    const fatals = []
    const warnings = []
    const failure = new Error('persisted command rejection')
    let applyCalls = 0

    const build = () => {
      const node = new MonsterFt('1', ids, noop, databasePath, {
        electionTimeout: 60_000,
        apply: (db, buf, term, seq, index, matchIndex) => {
          if (buf === null) {
            return applyApp(db, buf, term, seq, index, matchIndex)
          }
          applyCalls++
          if (toObj(buf).key === 'rejected') { throw failure }
          return applyApp(db, buf, term, seq, index, matchIndex)
        },
      })
      node.on('warn', (err) => warnings.push(err))
      node.on('error', (err) => errors.push(err))
      node.on('fatal', (err) => fatals.push(err))
      nodes.push(node)
      return node
    }

    t.teardown(() => {
      closeNodesQuietly(nodes)
      removePair(databasePath)
    })

    const command = {
      type: 'cmd',
      items: [
        toBuf({ key: 'post-commit', value: 13 }),
        toBuf({ key: 'rejected', value: 14 }),
      ],
      matchIndex: ids.map(() => -1n),
    }
    const first = build()
    first.open()
    first.log.append(toNoopEntry(0n))
    first._commitSeq = 0n
    first._commitTerm = 0n
    await first._apply(0n)
    const cmdSeq = first.log.append(toEntry(0n, command))
    first._commitSeq = cmdSeq
    first._commitTerm = 0n

    const exec = first.db.exec.bind(first.db)
    let closeCalled = false
    first.db.exec = (sql) => {
      const result = exec(sql)
      if (sql === 'COMMIT' && !closeCalled) {
        closeCalled = true
        first.close()
      }
      return result
    }

    t.equal(await errorOf(first._apply(cmdSeq)), null,
      'post-COMMIT apply completion is silently abandoned')
    t.ok(closeCalled, 'COMMIT synchronously invokes close')
    t.equal(first.db, null, 'post-COMMIT close clears DB2 immediately')
    t.notOk(first.log.isOpen, 'post-COMMIT close closes DB1 immediately')
    t.equal(first._applySeq, 0n,
      'post-COMMIT close prevents late live apply state publication')
    t.equal(first._monsterPendingCommand, null,
      'post-COMMIT close prevents late pending-cache publication')
    await new Promise((resolve) => setImmediate(resolve))
    t.equal(errors.length, 0, 'post-COMMIT shutdown emits no apply error')
    t.equal(fatals.length, 0, 'post-COMMIT shutdown emits no fatal error')
    t.equal(warnings.length, 1, 'the rejected item emits one warning before COMMIT')
    t.equal(warnings[0]?.stack, failure.stack,
      'the item warning retains its original trace')

    const committed = new DatabaseSync(`${databasePath}2`, {
      readOnly: true,
      readBigInts: true,
    })
    const committedMeta = committed.prepare(`
      SELECT seq, entry_hash, local_digest
      FROM monsterft_meta_apply WHERE id = 1
    `).get()
    const committedValue = committed.prepare(`
      SELECT value FROM two_file_items WHERE key = 'post-commit'
    `).get()
    committed.close()
    t.equal(committedMeta.seq, cmdSeq,
      'the atomic DB2 commit persists its applied sequence')
    t.equal(Buffer.from(committedMeta.entry_hash).length, 32,
      'the same DB2 commit persists its entry hash')
    const pendingImport = monsterMetaAtPath(databasePath)
    t.equal(pendingImport.applied_seq, 0n,
      'DB1 remains at setup when shutdown interrupts receipt import')
    t.equal(pendingImport.pending_cmd_seq, null,
      'the interrupted import leaves no pending command in DB1')
    t.equal(Buffer.from(committedMeta.local_digest).length, 32,
      'the same DB2 commit persists its pending command digest')
    t.equal(committedValue.value, 13n,
      'the same DB2 commit persists application state')

    const second = build()
    second.open()
    t.equal(second._monsterPendingCommand.cmdSeq, cmdSeq,
      'restart hydrates the committed pending command sequence')
    t.ok(second._monsterPendingCommand.localDigest.equals(
      Buffer.from(committedMeta.local_digest),
    ), 'restart hydrates the committed pending command digest')
    second._commitSeq = cmdSeq
    second._commitTerm = 0n
    await second._apply(cmdSeq)
    t.equal(applyCalls, 2,
      'restart does not replay an entry recorded in the DB2 receipt')
    t.equal(warnings.length, 1,
      'restoring the persisted pending outcome does not repeat its warning')
    t.equal(valueAt(second, 'post-commit'), 13,
      'restart retains the application transition without replay')
    second.close()
  })

for (const setup of [true, false]) {
  test(`DB1 import failure preserves committed ${setup ? 'setup' : 'CMD'} for recovery`, async (t) => {
    const databasePath = uniquePath(`import-failure-${setup ? 'setup' : 'cmd'}`)
    const nodes = []
    const errors = []
    let setupCalls = 0
    let cmdCalls = 0
    const build = () => {
      const node = new MonsterFt('1', ids, noop, databasePath, {
        electionTimeout: 60_000,
        apply(db, buf, term, seq) {
          if (seq === 0n) { setupCalls++ } else { cmdCalls++ }
          return applyApp(db, buf, term, seq)
        },
      })
      node.on('warn', noop)
      node.on('error', (err) => errors.push(err))
      nodes.push(node)
      return node
    }
    t.teardown(() => {
      closeNodesQuietly(nodes)
      removePair(databasePath)
    })
    const first = build()
    first.open()
    first.log.append(toNoopEntry(0n))
    first._commitSeq = 0n
    first._commitTerm = 0n
    if (!setup) { await first._apply(0n) }
    const seq = setup ? 0n : first.log.append(toEntry(0n, {
      type: 'cmd',
      items: [toBuf({ key: 'import-once', value: 73 })],
      matchIndex: ids.map(() => -1n),
    }))
    first.log.db.exec(`
      CREATE TRIGGER fail_receipt_import BEFORE UPDATE OF applied_seq ON monsterft_meta
      BEGIN SELECT RAISE(ABORT, 'injected receipt import failure'); END
    `)
    first._commitSeq = seq
    const err = await errorOf(first._apply(seq))
    t.match(err?.message ?? '', /injected receipt import failure/,
      'DB1 import failure reaches the apply caller')
    t.equal(errors.length, 1, 'DB1 import failure emits one public error')
    t.notOk(first.isOpen, 'import failure closes the node before further application')
    const receipt = applyMetaAtPath(databasePath)
    t.equal(receipt.seq, seq, 'DB2 keeps the completed apply receipt')
    t.equal(monsterMetaAtPath(databasePath).applied_seq, seq - 1n,
      'failed DB1 import leaves the prior checkpoint intact')
    if (setup) {
      t.deepEqual(Buffer.from(receipt.local_digest), Buffer.alloc(32),
        'completed setup retains its zero digest')
    } else {
      t.equal(valueAtPath(databasePath, 'import-once'), 73,
        'DB2 retains the committed application write')
    }

    const interrupted = build()
    const restartErr = errorOfCall(() => interrupted.open())
    t.match(restartErr?.message ?? '', /injected receipt import failure/,
      'startup import failure rejects reopening')
    t.deepEqual(applyMetaAtPath(databasePath), receipt,
      'failed startup import preserves the apply receipt')
    t.equal(setupCalls, 1, 'failed startup never repeats setup')
    t.equal(cmdCalls, setup ? 0 : 1, 'failed startup never repeats a completed CMD')

    const db1 = new DatabaseSync(databasePath)
    db1.exec('DROP TRIGGER fail_receipt_import')
    db1.close()
    const restored = build()
    restored.open()
    t.equal(restored._applySeq, seq, 'startup imports the completed apply checkpoint')
    t.equal(restored._monsterPendingCommand?.cmdSeq ?? null, setup ? null : seq,
      'startup restores only a completed CMD as pending')
    t.deepEqual(monsterMetaAtPath(databasePath).applied_local_digest, receipt.local_digest,
      'startup imports the exact apply digest')
    restored._commitSeq = seq
    restored._commitTerm = 0n
    await restored._apply(seq)
    t.equal(setupCalls, 1, 'successful restart does not rerun setup')
    t.equal(cmdCalls, setup ? 0 : 1, 'successful restart does not rerun the CMD')
    restored.close()
  })
}

test('failure after durable DB1 import recovers without publishing or reapplying', async (t) => {
  const databasePath = uniquePath('post-import-no-replay')
  const nodes = []
  let calls = 0
  const build = () => {
    const node = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply(db, buf, term, seq) {
        if (buf !== null) { calls++ }
        return applyApp(db, buf, term, seq)
      },
    })
    node.on('warn', noop)
    node.on('error', noop)
    nodes.push(node)
    return node
  }
  t.teardown(() => {
    closeNodesQuietly(nodes)
    removePair(databasePath)
  })
  const first = build()
  first.open()
  first.log.append(toNoopEntry(0n))
  first._commitSeq = 0n
  first._commitTerm = 0n
  await first._apply(0n)
  const cmdSeq = first.log.append(toEntry(0n, {
    type: 'cmd',
    items: [toBuf({ key: 'after-import', value: 47 })],
    matchIndex: ids.map(() => -1n),
  }))
  const failure = new Error('injected failure after durable import')
  const prepare = first.log.db.prepare.bind(first.log.db)
  first.log.db.prepare = (sql) => {
    const statement = prepare(sql)
    if (/UPDATE monsterft_meta\s/.test(sql)) {
      const run = statement.run.bind(statement)
      statement.run = (...args) => {
        run(...args)
        throw failure
      }
    }
    return statement
  }
  let cmdEvents = 0
  first.on('cmd', () => { cmdEvents++ })
  first._commitSeq = cmdSeq
  const err = await errorOf(first._apply(cmdSeq))
  t.equal(err?.stack, failure.stack, 'the post-import failure retains its original trace')
  t.equal(cmdEvents, 0, 'failure interrupts command publication')
  t.equal(first._monsterPendingCommand, null, 'failure interrupts pending-cache publication')
  t.equal(monsterMetaAtPath(databasePath).pending_cmd_seq, cmdSeq,
    'DB1 has already committed the pending command')
  t.equal(applyMetaAtPath(databasePath).seq, cmdSeq, 'DB2 has the same completed command')

  const restored = build()
  restored.open()
  t.equal(restored._applySeq, cmdSeq, 'restart loads the imported checkpoint')
  t.equal(restored._monsterPendingCommand.cmdSeq, cmdSeq,
    'restart restores the pending command without reimporting')
  restored._commitSeq = cmdSeq
  restored._commitTerm = 0n
  await restored._apply(cmdSeq)
  t.equal(calls, 1, 'restart never repeats the completed application')
  t.equal(valueAt(restored, 'after-import'), 47, 'restart retains the committed application write')
  restored.close()
})

test('drain reports DB1 import failure and leaves the DB2 receipt recoverable', async (t) => {
  const fixture = clusterFixture(t, 'drain-import-failure')
  const entered = deferred()
  const release = deferred()
  const calls = new Map()
  const apply = async (db, buf, term, seq, index, matchIndex, node) => {
    const result = applyApp(db, buf, term, seq)
    if (buf !== null) {
      calls.set(node.id, (calls.get(node.id) ?? 0) + 1)
      if (node.id === '3') {
        entered.resolve()
        await release.promise
      }
    }
    return result
  }
  t.teardown(() => release.resolve())
  const first = fixture.build({ apply })
  const leader = await openAndElect(first.nodes)
  await waitApplied(first.nodes, leader._commitSeq)
  const follower = first.nodes.find((node) => node.id === '3')
  const databasePath = fixture.paths.get(follower.id)
  follower.log.db.exec(`
    CREATE TRIGGER fail_receipt_import BEFORE UPDATE OF applied_seq ON monsterft_meta
    BEGIN SELECT RAISE(ABORT, 'injected drain import failure'); END
  `)
  const synced = nextSync(leader)
  const appending = leader.append(toBuf({ key: 'drain-import', value: 29 }))
  await withTimeout(entered.promise, 'follower active application before import failure')
  const [cmdSeq] = await withTimeout(appending, 'healthy quorum command before import failure')
  const { syncSeq } = await withTimeout(synced, 'healthy quorum SYNC before import failure')
  let settled = false
  const draining = follower.drainCmd()
  draining.finally(() => { settled = true }).catch(noop)
  await sleep(20)
  t.notOk(settled, 'drain waits for the active application and import')
  release.resolve()
  await withTimeout(draining, 'drain after import failure')
  await sleep(20)
  const diagnostics = fixture.errors.filter(({ id }) => id === follower.id)
  t.equal(diagnostics.length, 1, 'drain reports the failed import exactly once')
  t.match(diagnostics[0]?.err.message ?? '', /injected drain import failure/,
    'drain preserves the original SQLite diagnostic')
  t.equal(diagnostics[0]?.err.sqlCode, 1811, 'drain preserves the native SQLite error code')
  t.notOk(follower.isOpen, 'drain closes the failed follower')
  t.equal(monsterMetaAtPath(databasePath).applied_seq, cmdSeq - 1n,
    'failed import leaves DB1 at the previous checkpoint')
  t.equal(applyMetaAtPath(databasePath).seq, cmdSeq,
    'the committed DB2 receipt remains available for recovery')
  t.equal(valueAtPath(databasePath, 'drain-import'), 29,
    'the completed application write remains committed')

  const db1 = new DatabaseSync(databasePath)
  db1.exec('DROP TRIGGER fail_receipt_import')
  db1.close()
  const restored = fixture.build({ apply }).nodes.find((node) => node.id === '3')
  for (const node of first.nodes) {
    if (node.isOpen) { fixture.bus.register(node) }
  }
  restored.open()
  t.equal(restored._applySeq, cmdSeq, 'startup imports the receipt interrupted by drain')
  t.equal(restored._monsterPendingCommand.cmdSeq, cmdSeq,
    'startup restores the command awaiting agreement')
  await waitApplied([restored], syncSeq)
  t.equal(calls.get('3'), 1, 'recovery completes SYNC without reapplying the command')
  restored.close()
})

test('certify changes DB1 only and preserves its checkpoint hash',
  async (t) => {
    const fixture = clusterFixture(t, 'certify')
    const cluster = fixture.build()
    const leader = await openAndElect(cluster.nodes)
    const synced = nextSync(leader)
    const [cmdSeq] = await leader.append(toBuf({
      key: 'certify',
      value: 9,
    }))
    const sync = await withTimeout(synced, 'certify source SYNC event')
    const { syncSeq } = sync
    t.equal(sync.cmdSeq, cmdSeq,
      'certification starts from the command durability event')
    await waitApplied(cluster.nodes, syncSeq)
    closeNodes(cluster.nodes)

    const databasePath = fixture.paths.get(leader.id)
    const monsterPath = `${databasePath}2`
    const db = new DatabaseSync(databasePath, { readBigInts: true })
    db.prepare(`
      UPDATE monsterft_meta
      SET repair_state = 2
      WHERE id = 1
    `).run()
    const beforeMeta = db.prepare(`
      SELECT applied_seq, applied_entry_hash,
             pending_cmd_seq, applied_local_digest
      FROM monsterft_meta WHERE id = 1
    `).get()
    db.close()

    const appBefore = fs.readFileSync(monsterPath)
    const certified = MonsterFt.certify(databasePath)
    const appAfter = fs.readFileSync(monsterPath)
    t.equal(certified, beforeMeta.applied_seq,
      'certify returns DB1 applied_seq')
    t.ok(appAfter.equals(appBefore),
      'certify leaves the complete DB2 file byte-for-byte unchanged')

    const after = new DatabaseSync(databasePath, {
      readOnly: true,
      readBigInts: true,
    })
    const afterMeta = after.prepare(`
      SELECT applied_seq, applied_entry_hash, repair_state,
             pending_cmd_seq, applied_local_digest
      FROM monsterft_meta WHERE id = 1
    `).get()
    after.close()
    t.equal(afterMeta.repair_state, 0n,
      'certify clears DB1 repair state 2')
    t.ok(Buffer.from(afterMeta.applied_entry_hash)
      .equals(Buffer.from(beforeMeta.applied_entry_hash)),
    'certify preserves the DB1 applied-entry hash')
    t.equal(afterMeta.pending_cmd_seq, beforeMeta.pending_cmd_seq,
      'certify preserves the pending command sequence')
    t.deepEqual(afterMeta.applied_local_digest, beforeMeta.applied_local_digest,
      'certify preserves the last apply digest')
  })

test('certify requires a complete pair of normal files', (t) => {
  const missingPath = uniquePath('certify-missing')
  const halfPath = uniquePath('certify-half')
  const nonRegularPath = uniquePath('certify-non-regular')
  for (const databasePath of [missingPath, halfPath, nonRegularPath]) {
    removePair(databasePath)
  }
  t.teardown(() => {
    fs.rmSync(`${nonRegularPath}2`, { recursive: true, force: true })
    for (const databasePath of [missingPath, halfPath, nonRegularPath]) {
      removePair(databasePath)
    }
  })

  const emptyPathErr = errorOfCall(() => MonsterFt.certify(''))
  t.ok(emptyPathErr instanceof ErrorWithCode,
    'certify rejects an empty path with ErrorWithCode')
  t.equal(emptyPathErr.code, ARGUMENT_ILLEGAL,
    'an empty certification path uses ARGUMENT_ILLEGAL')

  const memoryErr = errorOfCall(() => MonsterFt.certify(':memory:'))
  t.match(memoryErr.message, /certify path cannot be :memory:/,
    'certify explicitly rejects an in-memory database')
  t.equal(memoryErr.code, ARGUMENT_ILLEGAL,
    'an in-memory certification path uses ARGUMENT_ILLEGAL')

  const missingDb1Err = errorOfCall(() => MonsterFt.certify(missingPath))
  t.ok(missingDb1Err instanceof ErrorWithCode,
    'certify rejects a missing DB1 with ErrorWithCode')
  t.match(missingDb1Err.message, /certify DB1 must be a normal file/,
    'certify rejects a missing pair without creating either file')
  t.equal(missingDb1Err.code, FS_ERROR,
    'a missing DB1 uses FS_ERROR')
  t.notOk(fs.existsSync(missingPath), 'missing-pair rejection creates no DB1')
  t.notOk(fs.existsSync(`${missingPath}2`),
    'missing-pair rejection creates no DB2')

  fs.writeFileSync(halfPath, '')
  const missingDb2Err = errorOfCall(() => MonsterFt.certify(halfPath))
  t.match(missingDb2Err.message, /certify DB2 must be a normal file/,
    'certify rejects a pair missing DB2')
  t.equal(missingDb2Err.code, FS_ERROR,
    'a missing DB2 uses FS_ERROR')
  t.notOk(fs.existsSync(`${halfPath}2`),
    'half-pair certification does not create DB2')

  fs.writeFileSync(nonRegularPath, '')
  fs.mkdirSync(`${nonRegularPath}2`)
  const abnormalDb2Err = errorOfCall(
    () => MonsterFt.certify(nonRegularPath),
  )
  t.match(abnormalDb2Err.message, /certify DB2 must be a normal file/,
    'certify rejects a non-regular DB2 path')
  t.equal(abnormalDb2Err.code, FS_ERROR,
    'a non-normal DB2 uses FS_ERROR')

  const allowUnresolvedErr = errorOfCall(
    () => MonsterFt.certify(nonRegularPath, 'yes'),
  )
  t.match(allowUnresolvedErr.message, /allowUnresolved must be a boolean/,
    'certify validates the unresolved-command override')
  t.equal(allowUnresolvedErr.code, ARGUMENT_ILLEGAL,
    'an illegal unresolved-command override uses ARGUMENT_ILLEGAL')
  t.end()
})

test('drain latches admission and waits for the local SYNC',
  async (t) => {
    const fixture = clusterFixture(t, 'drain')
    const cluster = fixture.build()
    const leader = await openAndElect(cluster.nodes)
    fixture.bus.holdOutcomes()

    const synced = nextSync(leader)
    const appending = leader.append(toBuf({ key: 'drain', value: 5 }))
    appending.catch(noop)
    await waitFor(() => {
      const row = leader.log.db.prepare(`
        SELECT pending_cmd_seq, applied_local_digest
        FROM monsterft_meta
        WHERE id = 1
      `).get()
      return row.pending_cmd_seq !== null &&
        row.applied_local_digest !== null &&
        leader._monsterPendingCommand?.cmdSeq === row.pending_cmd_seq
    }, 'locally materialized unresolved command')

    const drain = leader.drainCmd()
    t.equal(leader.drainCmd(), drain,
      'repeated drain calls share one promise')
    const rejected = await errorOf(
      leader.append(toBuf({ key: 'too-late', value: 6 })),
    )
    t.equal(rejected?.code, DRAINING,
      'drain synchronously latches future command admission')

    let drained = false
    drain.then(() => { drained = true })
    await sleep(30)
    t.notOk(drained, 'drain waits while its local command has no SYNC')

    await fixture.bus.releaseOutcomes()
    const [cmdSeq, result] = await withTimeout(
      appending,
      'held command completion',
    )
    await withTimeout(drain, 'local drain completion')
    const sync = await withTimeout(synced, 'drained command SYNC event')
    const { syncSeq } = sync
    t.equal(sync.cmdSeq, cmdSeq,
      'drain completes through the original command SYNC')
    t.deepEqual(result, { key: 'drain', value: 5 },
      'released outcomes complete the original command')
    t.notOk(leader.isOpen, 'drain closes the leader before resolving')
    t.equal(leader.db, null, 'drain closes the leader DB2 handle')
    t.notOk(leader.log.isOpen, 'drain closes the leader DB1 handle')
    const meta = monsterMetaAtPath(fixture.paths.get(leader.id))
    t.equal(meta.pending_cmd_seq, null,
      'drain observes the durable clearing of pending sequence state')
    t.deepEqual(meta.applied_local_digest,
      applyMetaAtPath(fixture.paths.get(leader.id)).local_digest,
      'drain preserves the last apply digest')
    t.equal(leader._monsterPendingCommand, null,
      'drain observes the matching cleared pending cache')
    t.ok(meta.applied_seq >= syncSeq,
      'drain observes a DB1 checkpoint through the matching SYNC')
    t.ok(leader._applySeq >= syncSeq,
      'drain resolves only after that SYNC is locally applied')
    t.equal(
      MonsterFt.certify(fixture.paths.get(leader.id)),
      meta.applied_seq,
      'the closed donor pair passes default certification',
    )
    t.throws(() => leader.open(), /node not open/,
      'the drained object cannot reopen')
  })

test('certify refuses a pair while a running node has it open', (t) => {
  const databasePath = uniquePath('certify-live')
  removePair(databasePath)
  const make = () => {
    const node = new MonsterFt('1', ids, noop, databasePath, {
      electionTimeout: 60_000,
      apply: applyApp,
    })
    node.on('warn', noop)
    node.on('error', noop)
    return node
  }
  let node = make()
  t.teardown(() => {
    closeNodesQuietly([node])
    removePair(databasePath)
  })
  node.open()
  node.close()
  const setup = new DatabaseSync(databasePath)
  setup.prepare('UPDATE monsterft_meta SET repair_state = 1 WHERE id = 1').run()
  setup.close()
  const repairState = () => {
    const db = new DatabaseSync(databasePath, {
      readOnly: true,
      readBigInts: true,
    })
    try {
      return db.prepare('SELECT repair_state FROM monsterft_meta WHERE id = 1')
        .get().repair_state
    } finally {
      db.close()
    }
  }

  node = make()
  node.open()
  t.ok(node.isOpen, 'repair state 1 reopens a live node')
  const err = errorOfCall(() => MonsterFt.certify(databasePath))
  t.ok(err instanceof ErrorWithCode, 'certification of an open pair fails')
  t.equal(err?.code, SQLITE_ERROR, 'the refusal uses SQLITE_ERROR')
  t.equal(err?.sqlCode, 5, 'the refusal reports SQLITE_BUSY')
  t.ok(node.isOpen, 'the running node remains open')
  t.equal(repairState(), 1n, 'the refused certification leaves repair state 1')

  node.close()
  t.equal(MonsterFt.certify(databasePath), monsterMetaAtPath(databasePath).applied_seq,
    'certification succeeds after the node closes')
  t.equal(repairState(), 0n, 'certification then clears the repair state')
  t.end()
})

test('drain before open leaves no draining state', async (t) => {
  const databasePath = uniquePath('drain-before-open')
  removePair(databasePath)
  const node = new MonsterFt('1', ids, noop, databasePath, {
    electionTimeout: 150,
    apply: applyApp,
  })
  // Tests elect leaders explicitly.
  node._timers.election = 60_000
  node.on('warn', noop)
  node.on('error', noop)
  t.teardown(() => {
    closeNodesQuietly([node])
    removePair(databasePath)
  })

  const early = await errorOf(node.drainCmd())
  t.equal(early?.code, NODE_NOT_OPEN, 'drain before open rejects as not open')
  node.open()
  const appendErr = await errorOf(node.append(toBuf({ key: 'after', value: 1 })))
  t.notEqual(appendErr?.code, DRAINING,
    'an opened node does not reject commands as draining')
  t.equal(await errorOf(withTimeout(node.drainCmd(), 'drain after open')), null,
    'a later drain runs instead of returning the earlier rejection')
  t.notOk(node.isOpen, 'the later drain closes the node')
})

test('drain closes a candidate before resolving', async (t) => {
  const databasePath = uniquePath('drain-candidate')
  removePair(databasePath)
  const node = new MonsterFt('1', ids, noop, databasePath, {
    electionTimeout: 150,
    apply: applyApp,
  })
  // Tests elect leaders explicitly.
  node._timers.election = 60_000
  node.on('warn', noop)
  node.on('error', noop)
  t.teardown(() => {
    closeNodesQuietly([node])
    removePair(databasePath)
  })

  node.open()
  node._voteForSelf()
  t.equal(node.state, 'candidate', 'the isolated member becomes a candidate')

  await withTimeout(node.drainCmd(), 'candidate drain')
  t.notOk(node.isOpen, 'candidate drain closes the node before resolving')
  t.equal(node.db, null, 'candidate drain closes DB2')
  t.notOk(node.log.isOpen, 'candidate drain closes DB1')
  const meta = monsterMetaAtPath(databasePath)
  t.equal(meta.pending_cmd_seq, null,
    'the closed candidate has no pending command sequence')
  t.equal(meta.applied_local_digest, null,
    'the uninitialized candidate has no apply digest')
  t.equal(MonsterFt.certify(databasePath), meta.applied_seq,
    'the closed candidate pair passes default certification')
})

test('drain rejects when terminal close fails', async (t) => {
  const fixture = clusterFixture(t, 'drain-close-failure')
  const cluster = fixture.build()
  const leader = await openAndElect(cluster.nodes)
  await waitApplied(cluster.nodes, leader._commitSeq)
  const follower = cluster.nodes.find((node) => node.id === '3')
  const db = follower.db
  const closeDb = db.close.bind(db)
  const closeFailure = new Error('injected drain DB2 close failure')
  let closeAttempts = 0
  db.close = () => {
    closeAttempts++
    if (closeAttempts === 1) { throw closeFailure }
    return closeDb()
  }

  const draining = follower.drainCmd()
  const failure = await errorOf(draining)
  t.ok(failure instanceof ErrorWithCode,
    'drain rejects with a coded DB2 close failure')
  t.equal(failure.message, 'DB2 close injected drain DB2 close failure',
    'drain uses the normalized DB2 close message')
  t.equal(failure.code, SQLITE_ERROR,
    'drain classifies the DB2 close failure')
  t.equal(follower.drainCmd(), draining,
    'the terminal drain latch retains the failed promise')
  t.equal(closeAttempts, 1, 'drain attempts the failed DB2 close once')
  t.equal(follower.db, db, 'failed drain close retains the DB2 handle')
  t.notOk(follower.log.isOpen,
    'failed drain close still closes the independent DB1 handle')
  t.notOk(follower.isOpen, 'failed drain close leaves the node unavailable')

  follower.close()
  t.equal(closeAttempts, 2, 'explicit close retries the retained DB2 handle')
  t.equal(follower.db, null, 'the explicit retry releases DB2')
  t.equal(
    MonsterFt.certify(fixture.paths.get(follower.id)),
    follower._applySeq,
    'the fully closed pair passes default certification',
  )
})

test('drain captures a committing follower DB2 transaction without blocking DB1',
  async (t) => {
    const fixture = clusterFixture(t, 'independent-storage')
    const entered = deferred()
    const release = deferred()
    let db2Released = false
    t.teardown(() => {
      db2Released = true
      release.resolve()
    })

    const cluster = fixture.build({
      apply: async (db, buf, term, seq, index, matchIndex, node) => {
        const result = applyApp(db, buf, term, seq, index, matchIndex)
        if (buf === null) { return result }
        const command = toObj(buf)
        if (node.id === '3' && command.key === 'blocked') {
          entered.resolve()
          await release.promise
        }
        return result
      },
    })
    const leader = await openAndElect(cluster.nodes)
    const slow = cluster.nodes.find((node) => node.id === '3')

    const synced = nextSync(leader)
    const appending = leader.append(toBuf({ key: 'blocked', value: 77 }))
    appending.catch(noop)
    await withTimeout(entered.promise, 'slow DB2 apply entry')
    const [cmdSeq, result] = await withTimeout(
      appending,
      'quorum completion around slow DB2 apply',
    )
    const sync = await withTimeout(synced, 'blocked follower SYNC event')
    const { syncSeq } = sync
    t.equal(sync.cmdSeq, cmdSeq,
      'the leader durably applies the matching quorum SYNC')
    t.deepEqual(result, { key: 'blocked', value: 77 },
      'an agreeing quorum completes without the blocked DB2')
    t.ok(slow._applySeq < cmdSeq,
      'the slow member remains inside its DB2 apply transaction')

    let drainSettled = false
    const draining = slow.drainCmd()
    draining.finally(() => { drainSettled = true }).catch(noop)
    await sleep(20)
    t.notOk(drainSettled,
      'drain captures a follower application already active in DB2')

    await waitFor(() => slow.log.seq >= syncSeq,
      'slow member DB1 replication through SYNC')
    t.ok(slow.log.seq >= syncSeq,
      'Raft appends the SYNC to DB1 while DB2 apply is blocked')
    t.ok(fixture.bus.messages.some(({ to, msg }) => {
      return to === slow.id &&
        msg.type === 'append' &&
        msg.commitSeq >= cmdSeq
    }), 'the slow member continues receiving Raft commit heartbeats')

    const higherTerm = slow.term + 1n
    const termP = slow.log.term
    const seqP = slow.log.seq
    await withTimeout(slow.onReceive(leader.id, {
      type: 'append',
      term: higherTerm,
      termP,
      seqP,
      commitSeq: syncSeq,
      cid: 'higher-term-during-db2-apply',
    }), 'higher-term DB1 persistence during DB2 apply')
    const election = slow.log.db.prepare(`
      SELECT current_term, voted_for
      FROM raft_election WHERE id = 1
    `).get()
    t.equal(slow.term, higherTerm,
      'the blocked member advances its live Raft term before DB2 release')
    t.equal(election.current_term, higherTerm,
      'the blocked member persists the higher election term in DB1')
    t.equal(election.voted_for, null,
      'the higher AppendEntries term clears the persisted vote')
    t.notOk(db2Released,
      'higher-term persistence does not require releasing DB2 apply')

    db2Released = true
    release.resolve()
    await withTimeout(draining, 'released follower drain through local SYNC')
    t.ok(slow._applySeq >= syncSeq,
      'captured follower drain waits through local SYNC application')
    t.notOk(slow.isOpen, 'the captured follower closes before drain resolves')
    t.equal(slow.db, null, 'the captured follower closes DB2')
    t.notOk(slow.log.isOpen, 'the captured follower closes DB1')
    t.deepEqual([
      valueAt(cluster.nodes[0], 'blocked'),
      valueAt(cluster.nodes[1], 'blocked'),
      valueAtPath(fixture.paths.get(slow.id), 'blocked'),
    ], [77, 77, 77], 'the released member commits its blocked DB2 command')
    const meta = monsterMetaAtPath(fixture.paths.get(slow.id))
    t.equal(meta.pending_cmd_seq, null,
      'the closed follower has no pending command sequence')
    t.deepEqual(meta.applied_local_digest,
      applyMetaAtPath(fixture.paths.get(slow.id)).local_digest,
      'the closed follower retains the apply digest')
    t.equal(MonsterFt.certify(fixture.paths.get(slow.id)), meta.applied_seq,
      'the closed follower pair passes default certification')
  })

test('drain excludes an active follower CMD transaction that rolls back',
  async (t) => {
    const fixture = clusterFixture(t, 'drain-rollback')
    const entered = deferred()
    const release = deferred()
    t.teardown(() => release.resolve())

    const cluster = fixture.build({
      apply: async (db, buf, term, seq, index, matchIndex, node) => {
        const result = applyApp(db, buf, term, seq, index, matchIndex)
        if (buf === null) { return result }
        if (node.id === '3' && toObj(buf).key === 'drain-rollback') {
          entered.resolve()
          await release.promise
        }
        return result
      },
    })
    const leader = await openAndElect(cluster.nodes)
    await waitApplied(cluster.nodes, leader._commitSeq)
    const follower = cluster.nodes.find((node) => node.id === '3')
    const cmdSeq = follower._applySeq + 1n
    const rollbackError = new Error('injected pending-state failure')
    const writeApply = follower._monsterWriteApply
    follower._monsterWriteApply = function(db, receipt) {
      if (receipt.seq === cmdSeq) { throw rollbackError }
      return writeApply.call(this, db, receipt)
    }
    t.teardown(() => {
      follower._monsterWriteApply = writeApply
    })

    const applying = follower._monsterApplyCmd({
      type: 'cmd',
      items: [toBuf({ key: 'drain-rollback', value: 88 })],
      matchIndex: cluster.nodes.map(() => -1n),
    }, follower.term, cmdSeq, crypto.randomBytes(32))
    applying.catch(noop)
    await withTimeout(entered.promise, 'rollback CMD transaction entry')

    let drainSettled = false
    const draining = follower.drainCmd()
    draining.finally(() => { drainSettled = true }).catch(noop)
    await sleep(20)
    t.notOk(drainSettled,
      'the invocation-time snapshot waits behind the active transaction')

    release.resolve()
    t.equal(await errorOf(applying), rollbackError,
      'the injected failure rolls back the CMD transaction')
    await withTimeout(draining, 'drain after CMD rollback')
    t.ok(drainSettled,
      'drain resolves when the queued snapshot observes no committed CMD')
    t.notOk(follower.isOpen, 'drain closes after the active CMD rolls back')
    t.equal(follower.db, null, 'rollback drain closes DB2')
    t.notOk(follower.log.isOpen, 'rollback drain closes DB1')
    t.equal(follower._monsterPendingCommand, null,
      'rollback publishes no pending-command cache state')
    const meta = monsterMetaAtPath(fixture.paths.get(follower.id))
    t.equal(meta.applied_seq, cmdSeq - 1n,
      'rollback leaves the DB1 checkpoint unchanged')
    t.equal(meta.pending_cmd_seq, null,
      'rollback persists no pending command sequence')
    t.deepEqual(Buffer.from(meta.applied_local_digest), Buffer.alloc(32),
      'rollback preserves the setup digest')
    t.equal(valueAtPath(fixture.paths.get(follower.id), 'drain-rollback'), null,
      'rollback removes the user write from the active transaction')
    t.equal(MonsterFt.certify(fixture.paths.get(follower.id)), meta.applied_seq,
      'the rolled-back donor pair passes default certification')
  })

for (const scenario of ['normal listener', 'throwing listener', 'closing listener', 'after release']) {
  test(`drain reports a native transaction failure: ${scenario}`,
    async (t) => {
      const fixture = clusterFixture(t, `drain-disk-full-${scenario.replaceAll(' ', '-')}`)
      const entered = deferred()
      const release = deferred()
      const originals = new Map()
      t.teardown(() => release.resolve())

      const cluster = fixture.build({
        apply: async function drainFailureApply(db, buf, term, seq, index, matchIndex, node) {
          const result = applyApp(db, buf, term, seq, index, matchIndex)
          if (seq === 0n) {
            db.exec('CREATE TABLE drain_failure_blob (id INTEGER PRIMARY KEY, value BLOB)')
            return result
          }
          if (node.id === '3') {
            entered.resolve()
            await release.promise
          }
          try {
            db.exec('INSERT INTO drain_failure_blob VALUES (1, zeroblob(1048576))')
          } catch (err) {
            originals.set(node.id, err)
            throw err
          }
          return result
        },
      })
      const leader = await openAndElect(cluster.nodes)
      await leader._monsterLeaderSync
      await waitApplied(cluster.nodes, leader._commitSeq)
      const follower = cluster.nodes.find((node) => node.id === '3')
      const databasePath = fixture.paths.get(follower.id)
      const before = monsterMetaAtPath(databasePath)
      const { page_count: pages } = follower.db.prepare('PRAGMA page_count').get()
      follower.db.exec(`PRAGMA max_page_count = ${pages}`)
      const listenerFailure = new Error('injected drain error listener failure')
      if (scenario === 'throwing listener') {
        follower.on('error', () => { throw listenerFailure })
      } else if (scenario === 'closing listener') {
        follower.on('error', () => follower.close())
      }

      const synced = nextSync(leader)
      const appending = leader.append(toBuf({ key: 'drain-disk-full', value: 88 }))
      appending.catch(noop)
      await withTimeout(entered.promise, 'native failure transaction entry')
      const [cmdSeq, result] = await withTimeout(appending, 'healthy quorum result')
      await withTimeout(synced, 'healthy quorum SYNC')
      t.deepEqual(result, { key: 'drain-disk-full', value: 88 },
        'the healthy quorum completes while the follower transaction is active')
      t.ok(follower._applySeq < cmdSeq,
        'the follower has not advanced its checkpoint through the command')

      let drainSettled = false
      let draining = null
      if (scenario === 'after release') {
        const afterRelease = deferred()
        const releaseDb = follower._monsterReleaseDb
        follower._monsterReleaseDb = function(work, value, failed) {
          releaseDb.call(this, work, value, failed)
          if (failed && value.stack === originals.get(follower.id)?.stack &&
              value.sqlCode === 13) {
            afterRelease.resolve(follower.drainCmd())
          }
        }
        t.teardown(() => { follower._monsterReleaseDb = releaseDb })
        draining = afterRelease.promise
      } else {
        draining = follower.drainCmd()
      }
      draining.finally(() => { drainSettled = true }).catch(noop)
      await sleep(20)
      t.notOk(drainSettled, 'drain has not completed while the transaction is active')
      release.resolve()
      const drainError = await withTimeout(errorOf(draining), 'drain after native transaction failure')
      if (scenario === 'closing listener') {
        t.ok(drainError === null || drainError.code === NODE_NOT_OPEN,
          'a listener closing the node does not strand the drain')
      } else {
        t.equal(drainError, null, 'drain completes after the failed transaction rolls back')
      }
      await sleep(20)

      const original = originals.get(follower.id)
      t.ok(original, 'SQLite throws the original transaction failure')
      t.equal(original?.errcode, 13, 'the native failure is SQLITE_FULL')
      const diagnostics = fixture.errors.filter(({ id, err }) => {
        return id === follower.id && err.stack === original?.stack
      })
      t.equal(diagnostics.length, 1,
        'the original failure is emitted exactly once before drain removes listeners')
      const reported = diagnostics[0]?.err
      t.ok(reported instanceof ErrorWithCode, 'the emitted error is normalized')
      t.ok(reported?.message.includes(original.message),
        'the emitted error retains the native message')
      t.equal(reported?.sqlCode, 13, 'the emitted error retains the native SQLite code')
      t.equal(reported?.stack, original.stack, 'the emitted error retains the original trace')
      t.match(reported?.stack ?? '', /drainFailureApply/,
        'the trace identifies the apply callback')
      if (scenario === 'throwing listener') {
        t.ok(fixture.errors.some(({ err }) => err === listenerFailure),
          'a throwing listener does not interrupt the drain')
      }
      t.notOk(follower.isOpen, 'drain closes the failed follower before resolving')
      t.equal(follower.db, null, 'drain closes DB2')
      t.notOk(follower.log.isOpen, 'drain closes DB1')
      t.equal(follower._monsterPendingCommand, null,
        'the failed transaction does not publish a pending command')
      t.deepEqual(monsterMetaAtPath(databasePath), before,
        'the durable checkpoint and pending metadata survive unchanged')
      t.equal(valueAtPath(databasePath, 'drain-disk-full'), null,
        'the earlier application write rolls back with the failed transaction')
      t.equal(MonsterFt.certify(databasePath), before.applied_seq,
        'the rolled-back pair remains a valid drain donor')
      t.equal(fixture.errors.filter(({ id }) => id !== follower.id).length, 0,
        'the healthy quorum has no errors')
    })
}

test('drain preserves the fatal path for a failing pending SYNC', async (t) => {
  const fixture = clusterFixture(t, 'drain-sync-failure')
  const cluster = fixture.build()
  const leader = await openAndElect(cluster.nodes)
  await leader._monsterLeaderSync
  await waitApplied(cluster.nodes, leader._commitSeq)
  const follower = cluster.nodes.find((node) => node.id === '3')
  const databasePath = fixture.paths.get(follower.id)
  const entered = deferred()
  const release = deferred()
  const applySync = follower._monsterApplySync
  follower._monsterApplySync = async function(...args) {
    entered.resolve()
    await release.promise
    return applySync.apply(this, args)
  }
  t.teardown(() => {
    follower._monsterApplySync = applySync
    release.resolve()
  })
  follower.log.db.exec(`
    CREATE TRIGGER fail_pending_sync
    BEFORE UPDATE OF pending_cmd_seq ON monsterft_meta
    WHEN NEW.pending_cmd_seq IS NULL
    BEGIN
      SELECT RAISE(ABORT, 'injected pending SYNC failure');
    END
  `)
  const fatals = []
  follower.on('fatal', (err) => fatals.push(err))
  const synced = nextSync(leader)
  const [cmdSeq, result] = await leader.append(toBuf({ key: 'drain-sync-failure', value: 99 }))
  await withTimeout(synced, 'healthy quorum SYNC before failure')
  await withTimeout(entered.promise, 'follower blocked before failing SYNC')
  t.deepEqual(result, { key: 'drain-sync-failure', value: 99 },
    'the command succeeds before follower SYNC failure')
  const before = monsterMetaAtPath(databasePath)
  t.equal(before.pending_cmd_seq, cmdSeq,
    'the follower has a durable pending command to drain')

  let drainSettled = false
  const draining = follower.drainCmd()
  draining.finally(() => { drainSettled = true }).catch(noop)
  await sleep(20)
  t.notOk(drainSettled, 'drain waits for the pending SYNC')
  release.resolve()
  const drainError = await withTimeout(errorOf(draining), 'drain after fatal SYNC failure')
  await sleep(20)
  t.ok(drainError instanceof ErrorWithCode,
    'drain rejects when the pending command cannot complete')
  t.equal(fatals.length, 1, 'the SYNC storage failure follows the normal fatal path')
  const diagnostics = fixture.errors.filter(({ id }) => id === follower.id)
  t.equal(diagnostics.length, 1,
    'drain does not duplicate the fatal error diagnostic')
  const reported = diagnostics[0]?.err
  t.equal(reported, fatals[0], 'fatal and error events share the failure')
  t.match(reported?.message ?? '', /injected pending SYNC failure/,
    'the original SQLite failure remains visible')
  t.equal(reported?.code, APPLY_ERROR, 'the failed SYNC is an application failure')
  t.equal(reported?.sqlCode, 1811, 'the native SQLite trigger error code survives')
  t.notOk(follower.isOpen, 'fatal shutdown closes the follower')
  t.equal(follower.db, null, 'fatal shutdown closes DB2')
  t.notOk(follower.log.isOpen, 'fatal shutdown closes DB1')
  t.deepEqual(monsterMetaAtPath(databasePath), before,
    'SYNC rollback preserves the pending command and its durable checkpoint')
  t.equal(valueAtPath(databasePath, 'drain-sync-failure'), 99,
    'the previously committed command output is retained')
  t.equal(fixture.errors.filter(({ id }) => id !== follower.id).length, 0,
    'the healthy quorum remains operational')
})

test('drain prevents a later replicated CMD from entering application',
  async (t) => {
    const fixture = clusterFixture(t, 'drain-apply-fence')
    const cluster = fixture.build()
    const leader = await openAndElect(cluster.nodes)
    const follower = cluster.nodes.find((node) => node.id === '3')
    const syncEntered = deferred()
    const releaseSync = deferred()
    t.teardown(() => releaseSync.resolve())

    const applySync = follower._monsterApplySync
    let blockFirstSync = true
    follower._monsterApplySync = async function(entry, ...args) {
      if (blockFirstSync) {
        blockFirstSync = false
        syncEntered.resolve(entry.cmdSeq)
        await releaseSync.promise
      }
      return applySync.call(this, entry, ...args)
    }
    t.teardown(() => { follower._monsterApplySync = applySync })

    const applyCmd = follower._monsterApplyCmd
    let lateApplyEntries = 0
    follower._monsterApplyCmd = function(entry, ...args) {
      if (toObj(entry.items[0]).key === 'drain-second') {
        lateApplyEntries++
      }
      return applyCmd.call(this, entry, ...args)
    }
    t.teardown(() => { follower._monsterApplyCmd = applyCmd })

    const first = leader.append(toBuf({ key: 'drain-first', value: 1 }))
    first.catch(noop)
    const [[firstCmdSeq, firstResult], blockedCmdSeq] = await Promise.all([
      withTimeout(first, 'first replicated command'),
      withTimeout(syncEntered.promise, 'blocked first follower SYNC'),
    ])
    t.equal(blockedCmdSeq, firstCmdSeq,
      'the follower is blocked before clearing the first pending CMD')
    t.deepEqual(firstResult, { key: 'drain-first', value: 1 },
      'the first command reaches a quorum while follower SYNC is blocked')
    t.equal(follower._monsterPendingCommand?.cmdSeq, firstCmdSeq,
      'the follower exposes the first committed CMD to the drain snapshot')

    const draining = follower.drainCmd()
    fixture.bus.holdOutcomes()
    const second = leader.append(toBuf({ key: 'drain-second', value: 2 }))
    second.catch(noop)
    let secondSettled = false
    second.finally(() => { secondSettled = true }).catch(noop)

    await waitFor(() => {
      const pending = leader._monsterPendingCommand
      return pending !== null && pending.cmdSeq !== firstCmdSeq
    }, 'later leader pending CMD')
    const secondCmdSeq = leader._monsterPendingCommand.cmdSeq
    t.ok(secondCmdSeq > firstCmdSeq,
      'the later replicated CMD has its own sequence')
    await waitFor(() => follower.log.seq >= secondCmdSeq,
      'later CMD reaches follower DB1')
    t.equal(raftEntryAtPath(fixture.paths.get(follower.id), secondCmdSeq).type,
      'cmd', 'the later CMD reaches follower DB1 while its SYNC is blocked')

    releaseSync.resolve()
    await withTimeout(draining, 'fenced follower drain')
    t.notOk(follower.isOpen, 'the fenced follower closes before drain resolves')
    t.equal(follower.db, null, 'the fenced follower closes DB2')
    t.notOk(follower.log.isOpen, 'the fenced follower closes DB1')
    t.equal(lateApplyEntries, 0,
      'the later CMD never enters _monsterApplyCmd')
    t.equal(cluster.applies.filter((call) => {
      return call.id === follower.id && call.buf !== null &&
        toObj(call.buf).key === 'drain-second'
    }).length, 0, 'the later CMD never invokes the follower user callback')
    t.notOk(secondSettled,
      'the held later CMD remains unresolved when the follower closes')

    const meta = monsterMetaAtPath(fixture.paths.get(follower.id))
    t.equal(meta.pending_cmd_seq, null,
      'the later DB1-only CMD creates no pending apply sequence')
    t.deepEqual(meta.applied_local_digest,
      applyMetaAtPath(fixture.paths.get(follower.id)).local_digest,
      'the later DB1-only CMD leaves the previous apply digest intact')
    t.ok(meta.applied_seq < secondCmdSeq,
      'the DB1 checkpoint remains before the later DB1-only CMD')
    t.equal(valueAtPath(fixture.paths.get(follower.id), 'drain-second'), null,
      'the later DB1-only CMD makes no application write')
    t.equal(raftEntryAtPath(fixture.paths.get(follower.id), secondCmdSeq).type,
      'cmd', 'the unapplied later CMD remains as a DB1 suffix')
    t.equal(MonsterFt.certify(fixture.paths.get(follower.id)), meta.applied_seq,
      'the DB1-suffix donor pair passes default certification')

    await fixture.bus.releaseOutcomes()
    const [completedSecondCmdSeq, secondResult] = await withTimeout(
      second,
      'later command completion',
    )
    t.equal(completedSecondCmdSeq, secondCmdSeq,
      'the later command retains its own exact sequence')
    t.deepEqual(secondResult, { key: 'drain-second', value: 2 },
      'releasing later outcomes completes the later command normally')
    await waitFor(() => cluster.nodes.slice(0, 2).every((node) => {
      return node._monsterPendingCommand === null &&
        valueAt(node, 'drain-second') === 2
    }), 'later command SYNC application')
  })
