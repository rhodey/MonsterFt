import fs from 'node:fs'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import test from 'tape'
import { unpack } from 'msgpackr'
import {
  APPLY_ERROR,
  ErrorWithCode,
  LOG_CORRUPT,
  SQLITE_ERROR,
  REPAIR_QUORUM_IMPOSSIBLE,
} from '../src/error.js'
import { MonsterFt } from '../src/monsterft.js'
import { MonsterNode } from '../src/monster.js'

const ids = ['1', '2', '3']
const noop = () => {}
const TEST_DIR = process.env.TEST_DIR ?? '/tmp'
let fixtureId = 0

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

const uniquePath = (name) => {
  fs.mkdirSync(TEST_DIR, { recursive: true })
  const unique = `${process.pid}-${Date.now()}-${++fixtureId}-${name}`
  return path.join(TEST_DIR, `monsterft-keep-${unique}.sqlite`)
}

const removePair = (databasePath) => {
  for (const file of [databasePath, `${databasePath}2`]) {
    for (const suffix of ['', '-journal', '-wal', '-shm']) {
      fs.rmSync(`${file}${suffix}`, { force: true })
    }
  }
}

const closeNodesQuietly = (nodes) => {
  for (const node of nodes) {
    try { node.close() } catch {}
  }
}

const toBuf = (value) => Buffer.from(JSON.stringify(value), 'utf8')
const toObj = (buf) => JSON.parse(Buffer.from(buf).toString('utf8'))

const applyApp = (db, buf, term, seq) => {
  if (seq === 0n) {
    db.exec(`
      CREATE TABLE IF NOT EXISTS keep_items (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL
      ) STRICT
    `)
    return
  }
  if (buf === null) { return }

  const command = toObj(buf)
  db.prepare(`
    INSERT INTO keep_items (key, value) VALUES (?, ?)
    ON CONFLICT (key) DO UPDATE SET value = excluded.value
  `).run(command.key, command.value)
  return { key: command.key, value: command.value }
}

const valueAt = (node, key) => {
  const row = node.db.prepare(`
    SELECT value FROM keep_items WHERE key = ?
  `).get(key)
  return row === undefined ? null : Number(row.value)
}

const raftRows = (node) => node.log.db.prepare(`
  SELECT seq, entry FROM raft_log ORDER BY seq
`).all()

const raftSeqs = (node) => raftRows(node).map(({ seq }) => seq)

const retainedCount = (node) => node.log.begin < 0n
  ? 0n
  : node.log.seq - node.log.begin + 1n

const raftTypes = (node) => raftRows(node).map(({ entry }) => {
  const payload = Buffer.from(entry).subarray(8)
  return payload.length === 0 ? 'noop' : unpack(payload).type
})

const pendingAt = (node) => {
  const row = node.db.prepare(`
    SELECT pending_cmd_seq, pending_local_digest
    FROM monsterft_meta WHERE id = 1
  `).get()
  return row.pending_cmd_seq === null
    ? null
    : {
        cmdSeq: row.pending_cmd_seq,
        localDigest: Buffer.from(row.pending_local_digest),
      }
}

const metaAt = (node) => node.db.prepare(`
  SELECT applied_seq, applied_entry_hash, repair_state,
         pending_cmd_seq, pending_local_digest
  FROM monsterft_meta
  WHERE id = 1
`).get()

const makeBus = () => {
  const current = new Map()
  const messages = []
  let blocked = new Set()

  return {
    messages,
    register(node) {
      current.set(node.id, node)
    },
    block(...nodeIds) {
      blocked = new Set(nodeIds)
    },
    unblock() {
      blocked.clear()
    },
    send(to, from, msg) {
      messages.push({ to, from, msg })
      if (blocked.has(to) || blocked.has(from)) { return undefined }
      const target = current.get(to)
      if (!target?.isOpen) { return undefined }
      return target.onReceive(from, msg)
    },
  }
}

const clusterFixture = (t, name) => {
  const paths = new Map(ids.map((id) => [id, uniquePath(`${name}-${id}`)]))
  const bus = makeBus()
  const allNodes = []
  const errors = []
  const warnings = []

  for (const databasePath of paths.values()) { removePair(databasePath) }

  const build = ({ apply=applyApp, opts={} }={}) => {
    const nodes = ids.map((id) => {
      const send = (to, msg) => bus.send(to, id, msg)
      const node = new MonsterFt(id, ids, send, paths.get(id), {
        electionTimeout: 60_000,
        pingTimeout: 150,
        appendTimeout: 3_000,
        quorum: 2,
        ...opts,
        apply,
      })
      node.on('warn', (err) => warnings.push({ id, err }))
      node.on('error', (err) => errors.push({ id, err }))
      bus.register(node)
      allNodes.push(node)
      return node
    })
    return { nodes }
  }

  t.teardown(() => {
    closeNodesQuietly(allNodes)
    for (const databasePath of paths.values()) { removePair(databasePath) }
  })

  return { paths, bus, errors, warnings, build }
}

const openAndElect = async (nodes, candidate=nodes[0]) => {
  nodes.forEach((node) => node.open())
  candidate._voteForSelf()
  await waitFor(() => candidate.state === 'leader',
    `node ${candidate.id} election`)
  await withTimeout(
    Promise.all(nodes.map((node) => node.awaitLeader(true))),
    `node ${candidate.id} committed leadership`,
  )
  await waitFor(
    () => nodes.every((node) => node._applySeq >= candidate._commitSeq),
    `node ${candidate.id} leadership apply`,
  )
  await candidate._monsterLeaderSync
  return candidate
}

const electOpen = async (nodes, candidate) => {
  candidate._voteForSelf()
  await waitFor(() => candidate.state === 'leader',
    `node ${candidate.id} replacement election`)
  await withTimeout(
    Promise.all(nodes.map((node) => node.awaitLeader(true))),
    `node ${candidate.id} replacement leadership`,
  )
  await candidate._monsterLeaderSync
  return candidate
}

const waitApplied = async (nodes, seq) => {
  await waitFor(() => nodes.every((node) => node._applySeq >= seq),
    `all nodes apply through ${seq}`)
  await Promise.all(nodes.map((node) => node._applyPrev))
}

const nextSync = (node) => new Promise((resolve) => node.once('sync', resolve))

const appendThroughSync = async (node, data, batch=false) => {
  const synced = nextSync(node)
  const [cmdSeq, result] = batch
    ? await node.appendBatch(data)
    : await node.append(data)
  const sync = await withTimeout(synced, `SYNC event for CMD ${cmdSeq}`)
  if (sync.cmdSeq !== cmdSeq) {
    throw new Error(`SYNC event CMD ${sync.cmdSeq} !== appended CMD ${cmdSeq}`)
  }
  return { cmdSeq, result, syncSeq: sync.syncSeq }
}

const keepOpts = (keepTarget=3, keepTrigger=5) => ({ keepTarget, keepTrigger })

const readPair = (databasePath) => {
  const raft = new DatabaseSync(databasePath, { readOnly: true, readBigInts: true })
  const db = new DatabaseSync(`${databasePath}2`, { readOnly: true, readBigInts: true })
  try {
    return {
      rows: raft.prepare('SELECT seq, entry FROM raft_log ORDER BY seq').all(),
      meta: metaAt({ db }),
    }
  } finally {
    raft.close()
    db.close()
  }
}

test('omitted retention options preserve history without KEEP records', async (t) => {
  const fixture = clusterFixture(t, 'disabled')
  const { nodes } = fixture.build()
  const leader = await openAndElect(nodes)
  t.equal(leader._monsterKeep, null, 'retention is disabled by default')
  t.equal(typeof MonsterNode.prototype.keep, 'undefined', 'no public KEEP method')
  for (let i = 0; i < 4; i++) {
    const result = await appendThroughSync(leader, toBuf({ key: 'disabled', value: i }))
    await waitApplied(nodes, result.syncSeq)
  }
  for (const node of nodes) {
    t.equal(node.log.begin, 0n, `node ${node.id} keeps the entire prefix`)
    t.deepEqual(raftTypes(node), [
      'noop', 'cmd', 'sync', 'cmd', 'sync', 'cmd', 'sync', 'cmd', 'sync',
    ], `node ${node.id} appends only commands, decisions, and election no-ops`)
  }
  t.deepEqual(fixture.errors, [], 'disabled retention emits no errors')
})

test('retention checks use cached state and delete in exact applied batches', async (t) => {
  const fixture = clusterFixture(t, 'boundaries')
  const { nodes } = fixture.build({ opts: { ...keepOpts(3, 6), applyMax: 4 } })
  const leader = await openAndElect(nodes)
  const deletions = new Map()
  const refreshes = new Map()
  for (const node of nodes) {
    const events = []
    deletions.set(node.id, events)
    refreshes.set(node.id, 0)
    const prepare = node.log.db.prepare.bind(node.log.db)
    node.log.db.prepare = (sql) => {
      if (/SELECT/i.test(sql)) {
        throw new Error('retention must not prepare a query to count rows')
      }
      if (/DELETE\s+FROM\s+raft_log\s+WHERE\s+seq\s*</i.test(sql)) {
        events.push(metaAt(node).applied_seq)
      }
      return prepare(sql)
    }
    const readHead = node.log._readHead.bind(node.log)
    node.log._readHead = () => {
      refreshes.set(node.id, refreshes.get(node.id) + 1)
      return readHead()
    }
  }

  await leader._appendToSelfAndFollowers(Array.from({ length: 4 }, () => Buffer.alloc(0)))
  await waitApplied(nodes, 4n)
  for (const node of nodes) {
    t.deepEqual(deletions.get(node.id), [], `node ${node.id} does not delete below trigger`)
    t.equal(refreshes.get(node.id), 0, `node ${node.id} does not query boundaries below trigger`)
  }

  await leader._appendToSelfAndFollowers(Array.from({ length: 7 }, () => Buffer.alloc(0)))
  await waitApplied(nodes, 11n)
  for (const node of nodes) {
    t.deepEqual(deletions.get(node.id), [5n, 8n, 11n],
      `node ${node.id} prunes at the trigger after durable checkpoints`)
    t.equal(refreshes.get(node.id), 3, `node ${node.id} refreshes only after deletion`)
    t.equal(node.log.begin, 9n, `node ${node.id} advances the retained floor`)
    t.equal(retainedCount(node), 3n, `node ${node.id} retains exactly the target`)
  }
  t.deepEqual(fixture.errors, [], 'batched application remains healthy')
})

test('local, batched, and forwarded commands prune without adding entries', async (t) => {
  const fixture = clusterFixture(t, 'commands')
  let commandCalls = 0
  const apply = (db, buf, term, seq) => {
    if (buf !== null) { commandCalls++ }
    return applyApp(db, buf, term, seq)
  }
  const opts = keepOpts()
  const { nodes } = fixture.build({ apply, opts })
  const leader = await openAndElect(nodes)
  const first = await appendThroughSync(leader, toBuf({ key: 'first', value: 10 }))
  await waitApplied(nodes, first.syncSeq)
  t.equal(leader.log.begin, 0n, 'the first command remains below trigger')
  const batch = await appendThroughSync(leader, [
    toBuf({ key: 'batch-a', value: 20 }),
    toBuf({ key: 'batch-b', value: 21 }),
  ], true)
  await waitApplied(nodes, batch.syncSeq)
  t.equal(batch.result.length, 2, 'the batch returns both outcomes')
  t.equal(leader.log.begin, 2n, 'the batch reaches the trigger and prunes')
  const forwarded = await appendThroughSync(nodes[1], toBuf({ key: 'forwarded', value: 30 }))
  await waitApplied(nodes, forwarded.syncSeq)
  t.deepEqual([first.cmdSeq, batch.cmdSeq, forwarded.cmdSeq], [1n, 3n, 5n],
    'each submission uses just its CMD and SYNC slots')
  for (const node of nodes) {
    t.deepEqual(raftSeqs(node), [4n, 5n, 6n], `node ${node.id} prunes locally to target`)
    t.deepEqual(raftTypes(node), ['sync', 'cmd', 'sync'], 'no retention record is stored')
    t.deepEqual(['first', 'batch-a', 'batch-b', 'forwarded'].map((key) => valueAt(node, key)),
      [10, 20, 21, 30], `node ${node.id} preserves application state`)
  }
  t.ok(fixture.bus.messages.some(({ msg }) => msg.type === 'fwd_cmd'),
    'the follower uses ordinary command forwarding')
  const beforeRestartCalls = commandCalls
  closeNodesQuietly(nodes)
  const restarted = fixture.build({ apply, opts })
  const replacement = await openAndElect(restarted.nodes, restarted.nodes[1])
  await waitApplied(restarted.nodes, 7n)
  t.equal(commandCalls, beforeRestartCalls, 'restart does not reapply completed commands')
  t.deepEqual(raftTypes(replacement), ['sync', 'cmd', 'sync', 'noop'],
    'the new leader appends an ordinary election no-op')
  const later = await appendThroughSync(replacement, toBuf({ key: 'later', value: 40 }))
  await waitApplied(restarted.nodes, later.syncSeq)
  for (const node of restarted.nodes) {
    t.deepEqual(raftSeqs(node), [7n, 8n, 9n], 'pruning resumes after leadership changes')
    t.equal(valueAt(node, 'first'), 10, 'pruned application state survives restart')
    t.equal(valueAt(node, 'later'), 40, 'the restarted cluster advances')
  }
  t.deepEqual(fixture.errors, [], 'retention and restart emit no errors')
})

test('a pending CMD pins history through startup and election until SYNC', async (t) => {
  const fixture = clusterFixture(t, 'pending')
  let commandCalls = 0
  const apply = (db, buf, term, seq) => {
    if (buf !== null) { commandCalls++ }
    return applyApp(db, buf, term, seq)
  }
  const opts = keepOpts()
  const first = fixture.build({ apply, opts })
  const leader = await openAndElect(first.nodes)
  await leader._appendToSelfAndFollowers(Array.from({ length: 3 }, () => Buffer.alloc(0)))
  const [cmdSeq] = await leader._monsterAppendEntry({
    type: 'cmd', items: [toBuf({ key: 'pending', value: 41 })],
  })
  await leader._appendToSelfAndFollowers(Array.from({ length: 3 }, () => Buffer.alloc(0)))
  await waitApplied(first.nodes, 7n)
  t.equal(cmdSeq, 4n, 'the pending command reaches the trigger')
  t.ok(first.nodes.every((node) => node.log.begin === 0n),
    'later no-ops cannot prune while the command is pending')
  const beforeRestartCalls = commandCalls
  closeNodesQuietly(first.nodes)

  const restarted = fixture.build({ apply, opts })
  const candidate = restarted.nodes[0]
  const syncEntered = deferred()
  const releaseSync = deferred()
  t.teardown(() => releaseSync.resolve())
  const appendEntry = candidate._monsterAppendEntry.bind(candidate)
  candidate._monsterAppendEntry = async (entry) => {
    if (entry.type === 'sync') {
      syncEntered.resolve()
      await releaseSync.promise
    }
    return appendEntry(entry)
  }
  const election = openAndElect(restarted.nodes, candidate)
  election.catch(noop)
  await withTimeout(syncEntered.promise, 'inherited command SYNC gate')
  await waitApplied(restarted.nodes, 8n)
  for (const node of restarted.nodes) {
    t.equal(node.log.begin, 0n, 'startup and the election no-op preserve pinned history')
    t.equal(pendingAt(node)?.cmdSeq, cmdSeq, 'the original pending command survives')
    t.equal(raftTypes(node).at(-1), 'noop', 'the election uses an empty no-op')
  }
  t.equal(commandCalls, beforeRestartCalls, 'the pending command is not reapplied')
  releaseSync.resolve()
  await election
  await waitApplied(restarted.nodes, 9n)
  for (const node of restarted.nodes) {
    t.deepEqual(raftSeqs(node), [7n, 8n, 9n], 'SYNC immediately releases pinned history')
    t.equal(pendingAt(node), null, 'SYNC clears pending state')
  }
  const later = await appendThroughSync(candidate, toBuf({ key: 'later', value: 42 }))
  await waitApplied(restarted.nodes, later.syncSeq)
  t.equal(later.cmdSeq, 10n, 'the next command needs no intervening retention entry')
  t.ok(restarted.nodes.every((node) => node.log.begin === 9n), 'later pruning continues')
})

test('a slow follower counts applied history and preserves its unapplied tail', async (t) => {
  const fixture = clusterFixture(t, 'slow-apply')
  const { nodes } = fixture.build({ opts: { ...keepOpts(3, 6), applyMax: 4, rpcMax: 32 } })
  const leader = await openAndElect(nodes)
  const slow = nodes[2]
  const entered = [deferred(), deferred()]
  const release = [deferred(), deferred()]
  t.teardown(() => release.forEach((gate) => gate.resolve()))
  const applyNoop = slow._monsterApplyNoop.bind(slow)
  slow._monsterApplyNoop = async (term, seq, hash) => {
    const gate = seq === 1n ? 0 : seq === 6n ? 1 : -1
    if (gate >= 0) {
      entered[gate].resolve()
      await release[gate].promise
    }
    return applyNoop(term, seq, hash)
  }
  await leader._appendToSelfAndFollowers(Array.from({ length: 12 }, () => Buffer.alloc(0)))
  await withTimeout(entered[0].promise, 'slow follower first entry')
  await waitFor(() => slow.log.seq === 12n, 'slow follower receives the complete tail')
  const tail = raftRows(slow).filter(({ seq }) => seq >= 6n)
  t.equal(slow._applySeq, 0n, 'the follower has applied only genesis')
  t.equal(slow.log.begin, 0n, 'stored rows alone do not trigger pruning')
  release[0].resolve()
  await withTimeout(entered[1].promise, 'slow follower after first pruning batch')
  t.equal(slow._applySeq, 5n, 'the applied prefix reaches the trigger')
  t.equal(slow.log.begin, 3n, 'only the oldest applied rows are deleted')
  t.equal(retainedCount(slow), 10n, 'three applied rows plus seven unapplied rows remain')
  t.deepEqual(raftRows(slow).filter(({ seq }) => seq >= 6n), tail,
    'the unapplied tail is byte-for-byte unchanged')
  release[1].resolve()
  await waitApplied(nodes, 12n)
  t.equal(slow.log.begin, 9n, 'catch-up performs further bounded deletion batches')
  t.equal(retainedCount(slow), 4n, 'the final partial batch stays below trigger')
  t.deepEqual(fixture.errors, [], 'slow application does not break replication')
})

test('startup prunes validated applied history while preserving an uncommitted tail', async (t) => {
  const fixture = clusterFixture(t, 'startup-tail')
  let commandCalls = 0
  const apply = (db, buf, term, seq) => {
    if (buf !== null) { commandCalls++ }
    return applyApp(db, buf, term, seq)
  }
  const first = fixture.build({ apply })
  const leader = await openAndElect(first.nodes)
  await appendThroughSync(leader, toBuf({ key: 'first', value: 1 }))
  const last = await appendThroughSync(leader, toBuf({ key: 'last', value: 2 }))
  await waitApplied(first.nodes, last.syncSeq)
  const hashes = first.nodes.map((node) => Buffer.from(metaAt(node).applied_entry_hash))
  for (const node of first.nodes) {
    const noopEntry = Buffer.alloc(8)
    noopEntry.writeBigUInt64LE(node.term)
    node.log.appendBatch(Array.from({ length: 4 }, () => noopEntry))
  }
  closeNodesQuietly(first.nodes)
  const beforeCalls = commandCalls
  const restarted = fixture.build({ apply, opts: keepOpts() })
  restarted.nodes.forEach((node, index) => {
    node.open()
    t.deepEqual(raftSeqs(node), [2n, 3n, 4n, 5n, 6n, 7n, 8n],
      'startup retains the applied target plus the entire uncommitted tail')
    t.equal(node._applySeq, 4n, 'startup does not advance the checkpoint')
    t.deepEqual(Buffer.from(metaAt(node).applied_entry_hash), hashes[index],
      'the validated checkpoint hash stays unchanged')
  })
  await openAndElect(restarted.nodes)
  await waitApplied(restarted.nodes, 9n)
  t.equal(commandCalls, beforeCalls, 'recovery applies no completed commands again')
  t.ok(restarted.nodes.every((node) => node.log.begin === 6n),
    'committing the tail resumes ordinary batched retention')
})

for (const stage of ['delete', 'refresh']) {
  test(`retention ${stage} failure closes safely and restarts without command replay`, async (t) => {
    const fixture = clusterFixture(t, `failure-${stage}`)
    let commandCalls = 0
    const apply = (db, buf, term, seq) => {
      if (buf !== null) { commandCalls++ }
      return applyApp(db, buf, term, seq)
    }
    const opts = keepOpts()
    const first = fixture.build({ apply, opts })
    const leader = await openAndElect(first.nodes)
    const warm = await appendThroughSync(leader, toBuf({ key: 'warm', value: 1 }))
    await waitApplied(first.nodes, warm.syncSeq)
    const failure = new Error(`injected ${stage} failure`)
    const fatal = deferred()
    leader.once('fatal', fatal.resolve)
    if (stage === 'delete') {
      const prepare = leader.log.db.prepare.bind(leader.log.db)
      leader.log.db.prepare = (sql) => {
        if (/DELETE\s+FROM\s+raft_log\s+WHERE\s+seq\s*</i.test(sql)) { throw failure }
        return prepare(sql)
      }
    } else {
      // Simulate failure after SQLite commits deletion but before cached state refreshes.
      leader.log._readHead = () => { throw failure }
    }
    const appended = errorOf(leader.append(toBuf({ key: 'completed', value: 2 })))
    const err = await withTimeout(fatal.promise, `${stage} fatal`)
    await appended
    t.ok(err instanceof ErrorWithCode, 'the failure is normalized')
    t.equal(err.code, APPLY_ERROR, 'runtime pruning uses the fatal application path')
    t.equal(err.message, `(apply) DB1 retention injected ${stage} failure`,
      'the failure identifies DB1 retention')
    await waitFor(() => !leader.isOpen, 'failed retention closes the node')
    await waitApplied(first.nodes.slice(1), 4n)
    const saved = readPair(fixture.paths.get(leader.id))
    t.equal(saved.meta.applied_seq, 4n, 'the SYNC checkpoint committed before deletion')
    t.equal(saved.meta.pending_cmd_seq, null, 'the durable decision cleared pending state')
    t.deepEqual(saved.rows.map(({ seq }) => seq), stage === 'delete'
      ? [0n, 1n, 2n, 3n, 4n] : [2n, 3n, 4n],
    'the persisted prefix reflects whether deletion committed')
    const beforeCalls = commandCalls
    closeNodesQuietly(first.nodes)
    const restarted = fixture.build({ apply, opts })
    const recovered = restarted.nodes[0]
    recovered.open()
    t.deepEqual(raftSeqs(recovered), [2n, 3n, 4n],
      'startup completes any cleanup left before the failure')
    t.equal(recovered._applySeq, 4n, 'startup accepts the retained checkpoint entry')
    t.equal(valueAt(recovered, 'completed'), 2, 'the committed application state survives')
    await openAndElect(restarted.nodes, recovered)
    t.equal(commandCalls, beforeCalls, 'recovery does not replay the completed command')
    const later = await appendThroughSync(recovered, toBuf({ key: 'later', value: 3 }))
    await waitApplied(restarted.nodes, later.syncSeq)
    t.ok(restarted.nodes.every((node) => valueAt(node, 'later') === 3),
      'the recovered cluster accepts further commands')
  })
}

test('failed SYNC checkpoint rolls back before pruning and is replayed on restart', async (t) => {
  const fixture = clusterFixture(t, 'checkpoint-failure')
  let commandCalls = 0
  const apply = (db, buf, term, seq) => {
    if (buf !== null) { commandCalls++ }
    return applyApp(db, buf, term, seq)
  }
  const opts = keepOpts()
  const first = fixture.build({ apply, opts })
  const leader = await openAndElect(first.nodes)
  await appendThroughSync(leader, toBuf({ key: 'warm', value: 1 }))
  const writeDecision = leader._monsterWriteCmdDecision.bind(leader)
  leader._monsterWriteCmdDecision = (...args) => {
    writeDecision(...args)
    throw new Error('injected checkpoint failure')
  }
  const fatal = deferred()
  leader.once('fatal', fatal.resolve)
  const appended = errorOf(leader.append(toBuf({ key: 'pending', value: 2 })))
  const err = await withTimeout(fatal.promise, 'checkpoint failure fatal')
  await appended
  t.equal(err.code, APPLY_ERROR, 'checkpoint failure remains fatal')
  await waitFor(() => !leader.isOpen, 'checkpoint failure close')
  await waitApplied(first.nodes.slice(1), 4n)
  const saved = readPair(fixture.paths.get(leader.id))
  t.equal(saved.meta.applied_seq, 3n, 'the failed SYNC transaction leaves the CMD checkpoint')
  t.equal(saved.meta.pending_cmd_seq, 3n, 'the failed decision leaves the CMD unresolved')
  t.deepEqual(saved.rows.map(({ seq }) => seq), [0n, 1n, 2n, 3n, 4n],
    'no history was pruned before the checkpoint committed')
  const beforeCalls = commandCalls
  closeNodesQuietly(first.nodes)
  const restarted = fixture.build({ apply, opts })
  const recovered = restarted.nodes[0]
  recovered.open()
  t.equal(recovered.log.begin, 0n, 'startup preserves history pinned by the CMD')
  await openAndElect(restarted.nodes, recovered)
  await waitApplied(restarted.nodes, 5n)
  t.equal(commandCalls, beforeCalls, 'recovery replays the decision without reapplying the CMD')
  t.equal(pendingAt(recovered), null, 'the replayed decision resolves the CMD')
  t.deepEqual(raftSeqs(recovered), [2n, 3n, 4n, 5n],
    'pruning follows the recovered SYNC checkpoint')
})

test('startup deletion failure closes both databases and leaves a recoverable pair', async (t) => {
  const fixture = clusterFixture(t, 'startup-failure')
  const first = fixture.build()
  const leader = await openAndElect(first.nodes)
  await leader._appendToSelfAndFollowers(Array.from({ length: 4 }, () => Buffer.alloc(0)))
  await waitApplied(first.nodes, 4n)
  closeNodesQuietly(first.nodes)
  const failed = fixture.build({ opts: keepOpts() }).nodes[0]
  const openLog = failed.log.open.bind(failed.log)
  failed.log.open = () => {
    openLog()
    const prepare = failed.log.db.prepare.bind(failed.log.db)
    failed.log.db.prepare = (sql) => {
      if (/DELETE\s+FROM\s+raft_log\s+WHERE\s+seq\s*</i.test(sql)) {
        throw new Error('injected startup deletion failure')
      }
      return prepare(sql)
    }
  }
  t.throws(() => failed.open(), (err) => err instanceof ErrorWithCode &&
    err.code === SQLITE_ERROR && /DB1 retention/.test(err.message),
  'startup reports a normalized retention error')
  t.notOk(failed.isOpen || failed.log.isOpen || failed.db !== null,
    'startup failure closes both connections')
  const saved = readPair(fixture.paths.get(failed.id))
  t.deepEqual(saved.rows.map(({ seq }) => seq), [0n, 1n, 2n, 3n, 4n], 'failed deletion preserves history')
  t.equal(saved.meta.applied_seq, 4n, 'startup preserves the durable checkpoint')
  const recovered = fixture.build({ opts: keepOpts() }).nodes[0]
  recovered.open()
  t.deepEqual(raftSeqs(recovered), [2n, 3n, 4n], 'a fresh instance completes cleanup')
})

test('startup validates the checkpoint before deleting any history', async (t) => {
  const fixture = clusterFixture(t, 'invalid-checkpoint')
  const first = fixture.build()
  const leader = await openAndElect(first.nodes)
  await leader._appendToSelfAndFollowers(Array.from({ length: 4 }, () => Buffer.alloc(0)))
  await waitApplied(first.nodes, 4n)
  leader.db.prepare('UPDATE monsterft_meta SET applied_entry_hash = ? WHERE id = 1')
    .run(Buffer.alloc(32))
  closeNodesQuietly(first.nodes)
  const restarted = fixture.build({ opts: keepOpts() }).nodes[0]
  t.throws(() => restarted.open(), (err) => err.code === LOG_CORRUPT,
    'a checkpoint mismatch rejects startup')
  t.deepEqual(readPair(fixture.paths.get(leader.id)).rows.map(({ seq }) => seq),
    [0n, 1n, 2n, 3n, 4n], 'checkpoint validation fails before deletion')
})

test('repair state one retains its fence while no-ops continue local pruning', async (t) => {
  const fixture = clusterFixture(t, 'repair-state-one')
  const { nodes } = fixture.build({ opts: keepOpts() })
  const leader = await openAndElect(nodes)
  for (const node of nodes) {
    node.db.prepare('UPDATE monsterft_meta SET repair_state = 1 WHERE id = 1').run()
    node._monsterRepairState = 1
  }
  await leader._appendToSelfAndFollowers(Array.from({ length: 8 }, () => Buffer.alloc(0)))
  await waitApplied(nodes, 8n)
  for (const node of nodes) {
    t.ok(node.isOpen, 'repair state one remains online')
    t.equal(metaAt(node).repair_state, 1n, 'retention preserves the repair fence')
    t.equal(metaAt(node).applied_seq, 8n, 'later no-ops advance the durable checkpoint')
    t.deepEqual(raftSeqs(node), [6n, 7n, 8n], 'no-ops still prune applied history')
  }
  const err = await errorOf(leader.append(toBuf({ key: 'fenced', value: 1 })))
  t.equal(err?.code, REPAIR_QUORUM_IMPOSSIBLE, 'the leader remains command-fenced')
})

for (const pending of [false, true]) {
  test(`failed election no-op retries in the same term${pending ? ' with a pending CMD' : ''}`,
    async (t) => {
      const fixture = clusterFixture(t, `election-retry-${pending}`)
      const calls = []
      const apply = (db, buf, term, seq) => {
        if (buf !== null) { calls.push(toObj(buf).key) }
        return applyApp(db, buf, term, seq)
      }
      const { nodes } = fixture.build({
        apply,
        opts: { ...keepOpts(6, 10), pingTimeout: 500, appendTimeout: 60 },
      })
      let pendingSeq = null
      if (pending) {
        const leader = await openAndElect(nodes)
        const [seq] = await leader._monsterAppendEntry({
          type: 'cmd', items: [toBuf({ key: 'pending', value: 1 })],
        })
        pendingSeq = seq
        await waitApplied(nodes, pendingSeq)
      } else {
        nodes.forEach((node) => node.open())
      }
      const candidate = nodes[pending ? 1 : 0]
      const beforeSeq = candidate.seq
      const beforeCalls = calls.length
      let blockData = true
      const send = fixture.bus.send.bind(fixture.bus)
      fixture.bus.send = (to, from, msg) => {
        // Keep heartbeats working while election entries cannot replicate.
        if (blockData && from === candidate.id &&
            msg.type === 'append' && msg.data !== undefined) {
          return undefined
        }
        return send(to, from, msg)
      }

      candidate._voteForSelf()
      const term = candidate.term
      const ready = candidate._leaderReady
      const queued = candidate.append(toBuf({ key: 'queued', value: 2 }))
      queued.catch(noop)
      await waitFor(() => candidate.seq >= beforeSeq + 2n, 'election no-op retry')
      t.equal(candidate.state, 'leader', 'a failed no-op does not force stepdown')
      t.equal(candidate.term, term, 'the retry uses the same term')
      t.equal(candidate._leaderReady, ready, 'the retry preserves the readiness wait')
      t.notOk(ready.settled, 'leadership is not ready before an entry commits')
      t.equal(calls.length, beforeCalls, 'the queued command waits for readiness and recovery')
      t.ok(fixture.warnings.some(({ id, err }) => {
        return id === candidate.id && /append timeout|append not commit/.test(err.message)
      }), 'failed replication emits a warning')
      t.ok(raftTypes(candidate).slice(Number(beforeSeq + 1n)).every((type) => type === 'noop'),
        'retries append only ordinary no-ops')
      t.equal(candidate.log.begin, 0n, 'uncommitted retries do not trigger pruning')
      if (pending) {
        t.equal(pendingAt(candidate)?.cmdSeq, pendingSeq, 'the pending CMD survives retries')
      }

      blockData = false
      const [cmdSeq, result] = await withTimeout(queued, 'queued command after retry')
      await candidate._monsterProtocol
      await waitApplied(nodes, cmdSeq + 1n)
      t.deepEqual(result, { key: 'queued', value: 2 }, 'the queued command succeeds')
      t.equal(candidate._commitTerm, term, 'the same term becomes ready')
      t.equal(candidate.state, 'leader', 'the original candidate remains leader')
      t.equal(candidate.term, term, 'recovery needs no additional election')
      t.equal(calls.filter((key) => key === 'queued').length, nodes.length,
        'each node applies the queued command once')
      if (pending) {
        t.equal(calls.filter((key) => key === 'pending').length, nodes.length,
          'recovery does not reapply the pending command')
      }
      t.ok(nodes.every((node) => pendingAt(node) === null), 'SYNC clears pending state')

      for (let index = 0; index < 3; index++) {
        const later = await appendThroughSync(candidate, toBuf({ key: 'later', value: index }))
        await waitApplied(nodes, later.syncSeq)
      }
      t.ok(nodes.every((node) => node.log.begin > 0n), 'local pruning continues after recovery')
      t.ok(nodes.every((node) => retainedCount(node) < 10n), 'applied history stays below trigger')
      t.deepEqual(fixture.errors, [], 'retries and subsequent commands emit no fatal errors')
    })
}

test('failed CMD replication preserves history and steps down without retry', async (t) => {
  const fixture = clusterFixture(t, 'command-failure')
  const { nodes } = fixture.build({ opts: keepOpts() })
  const leader = await openAndElect(nodes)
  await appendThroughSync(leader, toBuf({ key: 'warm', value: 1 }))
  const warm = await appendThroughSync(leader, toBuf({ key: 'warm', value: 2 }))
  await waitApplied(nodes, warm.syncSeq)
  const replicate = leader._appendToFollowers.bind(leader)
  let attempts = 0
  leader._appendToFollowers = (...args) => {
    attempts++
    if (attempts === 1) { return Promise.reject(new Error('injected CMD replication failure')) }
    return replicate(...args)
  }
  const err = await errorOf(leader.append(toBuf({ key: 'uncommitted', value: 3 })))
  t.match(err.message, /injected CMD replication failure/, 'the caller receives the append failure')
  t.equal(leader.state, 'follower', 'the failed leader steps down')
  t.ok(leader.isOpen, 'uncertain replication is nonfatal')
  t.equal(leader.log.begin, 2n, 'an uncommitted tail does not trigger pruning')
  t.equal(leader._applySeq, 4n, 'the failed CMD has not been applied')
  await sleep(30)
  t.equal(attempts, 1, 'the failed leader does not retry')
  const replacement = await electOpen(nodes, nodes[1])
  const later = await appendThroughSync(replacement, toBuf({ key: 'later', value: 4 }))
  await waitApplied(nodes, later.syncSeq)
  for (const node of nodes) {
    t.equal(valueAt(node, 'uncommitted'), null, 'the replacement trims the conflicting tail')
    t.equal(valueAt(node, 'later'), 4, 'retention remains correct after tail replacement')
  }
  t.deepEqual(fixture.errors, [], 'failed replication emits no fatal errors')
})

test('local retention cancels a lagging replica while its healthy quorum progresses',
  async (t) => {
    const fixture = clusterFixture(t, 'lagging-warning')
    const cluster = fixture.build({
      opts: {
        ...keepOpts(3, 5),
        appendTimeout: 250,
      },
    })
    const leader = await openAndElect(cluster.nodes)
    const stale = cluster.nodes[2]
    const healthy = cluster.nodes.find((node) => {
      return node !== leader && node !== stale
    })
    const fatals = []
    leader.on('fatal', (err) => fatals.push(err))
    const beforeOffline = await appendThroughSync(leader, toBuf({
      key: 'before-offline', value: 1,
    }))
    await waitApplied(cluster.nodes, beforeOffline.syncSeq)

    fixture.bus.block(stale.id)
    await appendThroughSync(leader, toBuf({
      key: 'while-offline-1', value: 2,
    }))
    const newest = await appendThroughSync(leader, toBuf({
      key: 'while-offline-2', value: 3,
    }))
    await waitApplied([leader, healthy], newest.syncSeq)
    t.equal(leader.log.begin, 4n,
      'local retention advances the leader retained floor')
    t.equal(healthy.log.begin, 4n,
      'local retention advances the healthy follower retained floor')
    t.equal(stale.log.seq, beforeOffline.syncSeq,
      'the partitioned member remains below the required predecessor')

    await waitFor(() => fixture.warnings.some(({ id, err }) => {
      return id === leader.id &&
        /replication wants forgotten .* have .*/.test(err.message)
    }), 'forgotten-prefix leader warning')
    await waitFor(() => {
      return leader._replication.get(stale.id)?.cancelled === true
    }, 'lagging follower replication cancellation')

    const staleReplication = leader._replication.get(stale.id)
    t.ok(staleReplication.cancelled,
      'the leader cancels replication to the forgotten follower')
    t.equal(staleReplication.term, leader.term,
      'the cancellation belongs to the current leader term')
    t.equal(leader._replicationState(stale.id, leader.term, leader.seq),
      staleReplication,
      'same-term lookup retains the cancelled follower state')

    fixture.bus.unblock()
    const beforeHeartbeat = fixture.bus.messages.length
    leader._pingFollowers()
    let heartbeat = null
    await waitFor(() => {
      heartbeat = fixture.bus.messages.slice(beforeHeartbeat).find((item) => {
        return item.from === leader.id && item.to === stale.id &&
          item.msg.type === 'append' && item.msg.data === undefined
      })
      return heartbeat !== undefined
    }, 'heartbeat to cancelled follower')
    await waitFor(() => {
      return fixture.bus.messages.slice(beforeHeartbeat).some((item) => {
        return item.from === stale.id && item.to === leader.id &&
          item.msg.type === 'ack' && item.msg.cid === heartbeat.msg.cid
      })
    }, 'cancelled follower heartbeat ACK')
    const heartbeatMessages = fixture.bus.messages.slice(beforeHeartbeat)
    t.ok(heartbeatMessages.some((item) => {
      return item.from === leader.id && item.to === stale.id &&
        item.msg.type === 'append' && item.msg.data === undefined
    }), 'the leader continues heartbeating the cancelled follower')
    t.notOk(heartbeatMessages.some((item) => {
      return item.from === leader.id && item.to === stale.id &&
        item.msg.type === 'append' && item.msg.data !== undefined
    }), 'heartbeat catch-up sends no data page to the cancelled follower')
    t.ok(leader.followers.includes(stale.id),
      'the heartbeat rediscovers the reachable cancelled follower')

    const beforeProgress = fixture.bus.messages.length
    const continued = await appendThroughSync(leader, toBuf({
      key: 'after-cancellation', value: 4,
    }))
    await waitApplied([leader, healthy], continued.syncSeq)

    t.deepEqual(fatals, [], 'a forgotten follower request is not fatal')
    t.notOk(fixture.errors.some(({ id }) => id === leader.id),
      'a forgotten follower request emits no public error')
    t.ok(leader.isOpen && leader.log.isOpen,
      'the retained leader remains open for the healthy quorum')
    t.equal(leader.state, 'leader', 'the warning does not force leader stepdown')
    t.ok(healthy.isOpen,
      'the other retained member remains available')
    t.ok(continued.syncSeq > newest.syncSeq,
      'the healthy quorum commits a later command and SYNC')
    t.equal(valueAt(leader, 'after-cancellation'), 4,
      'the leader applies progress after cancelling stale replication')
    t.equal(valueAt(healthy, 'after-cancellation'), 4,
      'the healthy follower applies progress after cancellation')
    t.equal(leader.log.begin, 6n,
      'later progress drives the leader through another local retention')
    t.equal(healthy.log.begin, 6n,
      'the healthy follower applies the later pruning and forgets old entries')
    t.equal(valueAt(stale, 'after-cancellation'), null,
      'the cancelled stale follower does not apply later progress')
    t.equal(leader._replication.get(stale.id), staleReplication,
      'healthy progress does not recreate the cancelled same-term state')
    t.ok(staleReplication.cancelled,
      'healthy progress leaves stale replication cancelled')
    t.notOk(fixture.bus.messages.slice(beforeProgress).some((item) => {
      return item.from === leader.id && item.to === stale.id &&
        item.msg.type === 'append' && item.msg.data !== undefined
    }), 'later quorum progress sends no data page to the cancelled follower')
    t.equal(stale._applySeq, beforeOffline.syncSeq,
      'the stale member cannot cross the forgotten predecessor')
  })
