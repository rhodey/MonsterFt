import fs from 'node:fs'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import test from 'tape'
import { unpack } from 'msgpackr'
import {
  APPLY_ERROR,
  ErrorWithCode,
  KEEP_HALT,
  NOT_COMMIT,
  RAFT_ILLEGAL,
  REPAIR_QUORUM_IMPOSSIBLE,
} from '../src/error.js'
import { MonsterFt } from '../src/monsterft.js'
import { MonsterNode } from '../src/monster.js'
import { validateKeepEntry } from '../src/utilm.js'

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

const raftRecordAt = (node, seq) => {
  const row = node.log.db.prepare(`
    SELECT entry FROM raft_log WHERE seq = ?
  `).get(seq)
  if (row === undefined) { return null }
  const payload = Buffer.from(row.entry).subarray(8)
  return payload.length === 0 ? null : unpack(payload)
}

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

const hasCommandTable = (node) => node.db.prepare(`
  SELECT COUNT(*) AS count
  FROM sqlite_schema
  WHERE type = 'table' AND name = 'monsterft_commands'
`).get().count !== 0n

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

const waitApplied = (nodes, seq) => waitFor(
  () => nodes.every((node) => node._applySeq >= seq),
  `all nodes apply through ${seq}`,
)

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

const keepOpts = (keepTarget=3, keepTrigger=5, keepHalt=8) => ({
  keepTarget,
  keepTrigger,
  keepHalt,
})

test('automatic KEEP validates configuration and removes the public KEEP API',
  (t) => {
    t.equal(typeof MonsterNode.prototype.keep, 'undefined',
      'MonsterNode exposes no public keep method')
    t.equal(typeof MonsterFt.prototype.keep, 'undefined',
      'MonsterFt exposes no public keep method')

    const nodes = []
    const paths = []
    const makeNode = (name, opts={}) => {
      const databasePath = uniquePath(`config-${name}`)
      paths.push(databasePath)
      const node = new MonsterFt('1', ids, noop, databasePath, {
        ...opts,
        apply: applyApp,
      })
      nodes.push(node)
      return node
    }
    t.teardown(() => {
      closeNodesQuietly(nodes)
      paths.forEach(removePair)
    })

    const disabled = makeNode('disabled')
    t.equal(disabled._monsterKeep, null,
      'omitting every KEEP option disables automatic retention')
    let keepEntryErr = null
    try {
      validateKeepEntry(disabled, { type: 'keep' }, 0n)
    } catch (err) {
      keepEntryErr = err
    }
    t.ok(keepEntryErr instanceof ErrorWithCode,
      'an unconfigured KEEP entry throws ErrorWithCode')
    t.equal(keepEntryErr.message, 'KEEP entry KEEP not configured',
      'an unconfigured KEEP entry uses the normalized message')
    t.equal(keepEntryErr.code, RAFT_ILLEGAL,
      'an unconfigured KEEP entry uses RAFT_ILLEGAL')
    t.equal(keepEntryErr.sqlCode, null,
      'an unconfigured KEEP entry has no SQLite error code')
    const enabled = makeNode('enabled', keepOpts(2, 3, 5))
    t.deepEqual(enabled._monsterKeep, {
      target: 2n,
      trigger: 3n,
      halt: 5n,
    }, 'the minimum valid boundaries are accepted and normalized')

    for (const [name, opts] of [
      ['target-only', { keepTarget: 2 }],
      ['trigger-only', { keepTrigger: 3 }],
      ['halt-only', { keepHalt: 6 }],
      ['target-trigger', { keepTarget: 2, keepTrigger: 3 }],
      ['target-halt', { keepTarget: 2, keepHalt: 6 }],
      ['trigger-halt', { keepTrigger: 3, keepHalt: 6 }],
    ]) {
      t.throws(() => makeNode(name, opts), /must be supplied together/,
        `${name} is rejected as a partial configuration`)
    }

    for (const [name, value] of [
      ['bigint', 2n],
      ['fraction', 2.5],
      ['nan', Number.NaN],
      ['infinity', Number.POSITIVE_INFINITY],
      ['unsafe', Number.MAX_SAFE_INTEGER + 1],
      ['string', '2'],
      ['null', null],
    ]) {
      t.throws(
        () => makeNode(`target-${name}`, {
          keepTarget: value, keepTrigger: 5, keepHalt: 8,
        }),
        /keepTarget must be a safe integer/,
        `${name} keepTarget is rejected`,
      )
      t.throws(
        () => makeNode(`trigger-${name}`, {
          keepTarget: 2, keepTrigger: value, keepHalt: 8,
        }),
        /keepTrigger must be a safe integer/,
        `${name} keepTrigger is rejected`,
      )
      t.throws(
        () => makeNode(`halt-${name}`, {
          keepTarget: 2, keepTrigger: 3, keepHalt: value,
        }),
        /keepHalt must be a safe integer/,
        `${name} keepHalt is rejected`,
      )
    }

    t.throws(() => makeNode('target-too-small', keepOpts(1, 3, 6)),
      /keepTarget must be >= 2/, 'keepTarget has an inclusive minimum of two')
    t.throws(() => makeNode('equal', keepOpts(3, 3, 6)),
      /keepTarget must be < keepTrigger/, 'target must be below trigger')
    t.throws(() => makeNode('reversed', keepOpts(4, 3, 6)),
      /keepTarget must be < keepTrigger/, 'a reversed target and trigger are rejected')
    t.throws(() => makeNode('halt-headroom', keepOpts(2, 3, 4)),
      /keepHalt must be >= keepTrigger \+ 2/,
      'halt reserves at least two rows beyond trigger')
    t.throws(() => makeNode('overflow', keepOpts(
      Number.MAX_SAFE_INTEGER - 1,
      Number.MAX_SAFE_INTEGER,
      Number.MAX_SAFE_INTEGER,
    )), /keepHalt must be >= keepTrigger \+ 2/,
    'a trigger without representable halt headroom is rejected')
    t.end()
  })

test('omitted KEEP configuration never appends KEEP and old KEEP RPC is ignored',
  async (t) => {
    const fixture = clusterFixture(t, 'disabled')
    const cluster = fixture.build()
    const leader = await openAndElect(cluster.nodes)

    let syncSeq = null
    for (let index = 0; index < 3; index++) {
      ({ syncSeq } = await appendThroughSync(leader, toBuf({
        key: `disabled-${index}`,
        value: index,
      })))
    }
    await waitApplied(cluster.nodes, syncSeq)
    t.equal(retainedCount(leader), 7n,
      'disabled retention allows the retained row count to grow')
    t.deepEqual(raftTypes(leader), [
      'noop', 'cmd', 'sync', 'cmd', 'sync', 'cmd', 'sync',
    ], 'disabled retention emits only ordinary protocol records')

    const follower = cluster.nodes.find((node) => node !== leader)
    const beforeSeq = leader.seq
    const beforeMessages = fixture.bus.messages.length
    await leader.onReceive(follower.id, {
      type: 'monster_keep_request',
      term: leader.term,
      cid: 'removed-keep-rpc',
      seq: 0n,
    })
    await sleep(5)
    t.equal(leader.seq, beforeSeq, 'the removed KEEP RPC appends no record')
    t.notOk(fixture.bus.messages.slice(beforeMessages).some(({ msg }) => {
      return msg.cid === 'removed-keep-rpc'
    }), 'the removed KEEP RPC receives no protocol response')
    t.deepEqual(fixture.errors, [], 'disabled retention emits no errors')
  })

test('election below keepTrigger retains the empty no-op marker', async (t) => {
  const fixture = clusterFixture(t, 'election-below-trigger')
  const opts = keepOpts(3, 5, 8)
  const nullSeqs = []
  const apply = (db, buf, term, seq) => {
    if (buf === null) { nullSeqs.push(seq) }
    return applyApp(db, buf, term, seq)
  }
  const seeded = fixture.build({ apply, opts })
  const seedLeader = await openAndElect(seeded.nodes)
  await appendThroughSync(seedLeader, toBuf({ key: 'below', value: 1 }))
  t.equal(retainedCount(seedLeader), 3n,
    'the seed remains below the retention trigger')
  t.deepEqual(nullSeqs, [0n, 0n, 0n],
    'only the initial no-op invokes the application callback')
  closeNodesQuietly(seeded.nodes)

  const restarted = fixture.build({ apply, opts })
  const leader = await openAndElect(restarted.nodes, restarted.nodes[1])
  t.deepEqual(raftTypes(leader), ['noop', 'cmd', 'sync', 'noop'],
    'below-trigger election appends an actual empty no-op')
  t.deepEqual(nullSeqs, [0n, 0n, 0n],
    'the later election no-op does not invoke the application callback')
})

test('automatic KEEP hits its exact target before local and forwarded commands',
  async (t) => {
    const fixture = clusterFixture(t, 'automatic-success')
    const opts = keepOpts(3, 5, 8)
    const nullSeqs = []
    const apply = (db, buf, term, seq) => {
      if (buf === null) { nullSeqs.push(seq) }
      return applyApp(db, buf, term, seq)
    }
    const first = fixture.build({ apply, opts })
    const leader = await openAndElect(first.nodes)
    nullSeqs.length = 0

    const firstPair = await appendThroughSync(leader, toBuf({
      key: 'first', value: 10,
    }))
    const secondPair = await appendThroughSync(leader, [
      toBuf({ key: 'batch-a', value: 20 }),
      toBuf({ key: 'batch-b', value: 21 }),
    ], true)
    await waitApplied(first.nodes, secondPair.syncSeq)
    t.equal(secondPair.result.length, 2,
      'appendBatch still produces one CMD with ordered item outcomes')
    t.equal(retainedCount(leader), 5n,
      'the election no-op and two CMD/SYNC pairs reach the trigger exactly')
    t.notOk(raftTypes(leader).includes('keep'),
      'reaching the trigger does not act until the next command boundary')

    const appendEntry = leader._monsterAppendEntry.bind(leader)
    let keepSeq = null
    let keepRecord = null
    let beforeTriggerCmd = null
    leader._monsterAppendEntry = async (entry) => {
      if (entry.type === 'cmd' && keepSeq !== null && beforeTriggerCmd === null) {
        beforeTriggerCmd = {
          seqs: raftSeqs(leader),
          count: retainedCount(leader),
          applySeq: leader._applySeq,
          pending: pendingAt(leader),
        }
      }
      const result = await appendEntry(entry)
      if (entry.type === 'keep') {
        keepSeq = result[0]
        keepRecord = entry
      }
      return result
    }

    const follower = first.nodes.find((node) => node !== leader)
    const thirdPair = await appendThroughSync(follower, toBuf({
      key: 'forwarded', value: 30,
    }))
    await waitApplied(first.nodes, thirdPair.syncSeq)

    t.equal(keepSeq, 5n, 'the automatic KEEP occupies predicted index K')
    t.deepEqual(keepRecord, { type: 'keep' },
      'the KEEP entry contains only its type')
    t.deepEqual(raftRecordAt(leader, keepSeq), keepRecord,
      'the persisted KEEP matches the computed record')
    t.deepEqual(beforeTriggerCmd, {
      seqs: [3n, 4n, 5n],
      count: 3n,
      applySeq: 5n,
      pending: null,
    }, 'KEEP replication, apply, and deletion finish before CMD admission')
    t.equal(thirdPair.cmdSeq, 6n,
      'the forwarded CMD joins immediately after the applied KEEP')
    t.equal(thirdPair.syncSeq, 7n,
      'the forwarded CMD retains its adjacent SYNC')
    t.deepEqual(nullSeqs, [],
      'pre-CMD KEEP does not invoke the application callback')
    t.ok(fixture.bus.messages.some(({ from, to, msg }) => {
      return from === follower.id && to === leader.id &&
        msg.type === 'fwd_cmd'
    }), 'a forwarded public append uses ordinary command submission')
    t.notOk(fixture.bus.messages.some(({ msg }) => {
      return msg.type === 'monster_keep_request'
    }), 'automatic retention sends no dedicated KEEP request RPC')

    for (const node of first.nodes) {
      t.deepEqual(raftSeqs(node), [3n, 4n, 5n, 6n, 7n],
        `node ${node.id} retains the exact floor plus the new CMD/SYNC pair`)
      t.deepEqual(raftTypes(node), ['cmd', 'sync', 'keep', 'cmd', 'sync'],
        `node ${node.id} retains the expected record types`)
      t.equal(pendingAt(node), null,
        `node ${node.id} has no pending CMD after the later SYNC`)
      t.notOk(hasCommandTable(node),
        `node ${node.id} stores no resolved command history`)
      t.deepEqual(
        ['first', 'batch-a', 'batch-b', 'forwarded']
          .map((key) => valueAt(node, key)),
        [10, 20, 21, 30],
        `node ${node.id} preserves application state while pruning history`,
      )
      t.equal(metaAt(node).repair_state, 0n,
        `node ${node.id} remains healthy after automatic retention`)
    }
    t.equal(firstPair.cmdSeq, 1n, 'the pruned first CMD occupied the expected seq')

    closeNodesQuietly(first.nodes)
    nullSeqs.length = 0
    const restarted = fixture.build({ apply, opts })
    const restartedLeader = await openAndElect(restarted.nodes, restarted.nodes[1])
    const electionKeepSeq = 8n
    t.deepEqual(raftSeqs(restartedLeader), [6n, 7n, electionKeepSeq],
      'election KEEP reduces retained history to the exact target')
    t.deepEqual(raftTypes(restartedLeader), ['cmd', 'sync', 'keep'],
      'triggered election uses one KEEP without an adjacent empty entry')
    t.deepEqual(raftRecordAt(restartedLeader, electionKeepSeq), {
      type: 'keep',
    }, 'the sole current-term leader marker is fieldless KEEP')
    t.equal(restartedLeader._commitTerm, restartedLeader.term,
      'election KEEP establishes current-term leader readiness')
    t.deepEqual(nullSeqs, [],
      'election KEEP does not invoke the application callback')
    const afterRestart = await appendThroughSync(restartedLeader, toBuf({
      key: 'after-restart', value: 40,
    }))
    await waitApplied(restarted.nodes, afterRestart.syncSeq)
    for (const node of restarted.nodes) {
      t.deepEqual(
        ['first', 'batch-a', 'batch-b', 'forwarded', 'after-restart']
          .map((key) => valueAt(node, key)),
        [10, 20, 21, 30, 40],
        `restarted node ${node.id} preserves and advances application state`,
      )
    }
    t.deepEqual(fixture.errors, [],
      'automatic retention and a pruned restart emit no errors')
  })

test('automatic KEEP waits behind the preceding CMD and SYNC FIFO', async (t) => {
  const fixture = clusterFixture(t, 'automatic-order')
  const cluster = fixture.build({ opts: keepOpts(3, 5, 8) })
  const leader = await openAndElect(cluster.nodes)
  await appendThroughSync(leader, toBuf({ key: 'warm', value: 1 }))

  const syncEntered = deferred()
  const releaseSync = deferred()
  t.teardown(() => releaseSync.resolve())
  const appendEntry = leader._monsterAppendEntry.bind(leader)
  let held = false
  leader._monsterAppendEntry = async (entry) => {
    if (entry.type === 'sync' && !held) {
      held = true
      syncEntered.resolve()
      await releaseSync.promise
    }
    return appendEntry(entry)
  }
  const syncEvents = []
  leader.on('sync', (event) => syncEvents.push(event))

  const active = leader.append(toBuf({ key: 'active', value: 2 }))
  await withTimeout(syncEntered.promise, 'active SYNC FIFO gate')
  const activeResult = await withTimeout(active, 'active early result')
  const follower = cluster.nodes.find((node) => node !== leader)
  let queuedSettled = false
  const queued = follower.append(toBuf({
    key: 'queued', value: 3,
  })).finally(() => { queuedSettled = true })
  queued.catch(noop)
  await sleep(25)

  t.equal(activeResult[0], 3n, 'the active CMD occupies the pre-trigger slot')
  t.notOk(queuedSettled, 'the later command remains queued behind SYNC')
  t.deepEqual(raftTypes(leader), ['noop', 'cmd', 'sync', 'cmd'],
    'neither KEEP nor the queued CMD overtakes the active SYNC')
  t.equal(valueAt(leader, 'queued'), null,
    'the queued user transition has not reached application')

  releaseSync.resolve()
  const queuedResult = await withTimeout(queued, 'queued command after KEEP')
  await waitFor(() => syncEvents.length === 2, 'both serialized SYNC events')
  await waitApplied(cluster.nodes, syncEvents[1].syncSeq)
  t.equal(syncEvents[0].cmdSeq, activeResult[0],
    'the active CMD receives its SYNC first')
  t.equal(syncEvents[0].syncSeq, 4n, 'the preceding SYNC reaches the trigger')
  t.equal(queuedResult[0], 6n, 'KEEP occupies seq 5 before the queued CMD')
  t.equal(syncEvents[1].cmdSeq, queuedResult[0],
    'the queued CMD receives the next SYNC')
  t.deepEqual(raftTypes(leader), ['cmd', 'sync', 'keep', 'cmd', 'sync'],
    'the serialized order is prior CMD, prior SYNC, KEEP, CMD, SYNC')
  t.deepEqual(cluster.nodes.map((node) => [
    valueAt(node, 'warm'), valueAt(node, 'active'), valueAt(node, 'queued'),
  ]), [[1, 2, 3], [1, 2, 3], [1, 2, 3]],
  'all serialized application transitions commit')
})

test('election KEEP retains a pending CMD until SYNC and later pruning',
  async (t) => {
    const fixture = clusterFixture(t, 'election-pending')
    const opts = keepOpts(3, 5, 10)
    let commandCalls = 0
    const apply = (db, buf, term, seq) => {
      if (buf !== null) { commandCalls++ }
      return applyApp(db, buf, term, seq)
    }
    const seeded = fixture.build({ apply, opts })
    const seedLeader = await openAndElect(seeded.nodes)
    for (let count = 0; count < 3; count++) {
      await seedLeader._appendToSelfAndFollowers(Buffer.alloc(0))
    }
    const [cmdSeq] = await seedLeader._monsterAppendEntry({
      type: 'cmd',
      items: [toBuf({ key: 'pending-election', value: 41 })],
    })
    await waitApplied(seeded.nodes, cmdSeq)
    t.equal(retainedCount(seedLeader), 5n,
      'the unresolved CMD brings retained history to the trigger')
    t.ok(seeded.nodes.every((node) => pendingAt(node)?.cmdSeq === cmdSeq),
      'every seed member stores the unresolved CMD')
    const appliedCommandCalls = commandCalls

    closeNodesQuietly(seeded.nodes)
    const restarted = fixture.build({ apply, opts })
    const candidate = restarted.nodes[0]
    const synced = nextSync(candidate)
    await openAndElect(restarted.nodes, candidate)
    const sync = await withTimeout(synced, 'pending election SYNC')
    await waitApplied(restarted.nodes, sync.syncSeq)
    const keepSeq = cmdSeq + 1n

    t.equal(sync.cmdSeq, cmdSeq,
      'leader synchronization resolves the inherited CMD')
    t.deepEqual(raftTypes(candidate), [
      'noop', 'noop', 'noop', 'noop', 'cmd', 'keep', 'sync',
    ], 'ordered recovery applies CMD, election KEEP, then SYNC')
    t.ok(restarted.nodes.every((node) => node.log.begin === 0n),
      'election KEEP skips pruning while the CMD is pending')
    t.ok(restarted.nodes.every((node) => pendingAt(node) === null),
      'the later SYNC clears pending state on every member')
    t.equal(commandCalls, appliedCommandCalls,
      'recovery does not reapply the stored CMD')

    const later = await appendThroughSync(candidate, toBuf({
      key: 'after-pending-election', value: 42,
    }))
    await waitApplied(restarted.nodes, later.syncSeq)
    t.equal(later.cmdSeq, sync.syncSeq + 2n,
      'a later automatic KEEP occupies the row before the next CMD')
    t.ok(restarted.nodes.every((node) => node.log.begin === keepSeq),
      'a later KEEP prunes after pending state is resolved')
  })

test('repair state one checkpoints later no-ops and committed KEEP records',
  async (t) => {
    const fixture = clusterFixture(t, 'repair-state-one')
    const nullSeqs = []
    const apply = (db, buf, term, seq) => {
      if (buf === null) { nullSeqs.push(seq) }
      return applyApp(db, buf, term, seq)
    }
    const cluster = fixture.build({ apply, opts: keepOpts(4, 5, 7) })
    const leader = await openAndElect(cluster.nodes)
    nullSeqs.length = 0

    for (const node of cluster.nodes) {
      const updated = node.db.prepare(`
        UPDATE monsterft_meta SET repair_state = 1 WHERE id = 1
      `).run()
      t.equal(updated.changes, 1n,
        `node ${node.id} stores the state-one repair fence`)
      node._monsterRepairState = 1
    }

    const [noOpSeq] = await leader._appendToSelfAndFollowers(Buffer.alloc(0))
    await waitApplied(cluster.nodes, noOpSeq)

    const [keepSeq, keepResult] = await leader._monsterAppendEntry({
      type: 'keep',
    })
    await waitApplied(cluster.nodes, keepSeq)

    t.equal(keepResult, null, 'KEEP has no application result')
    t.deepEqual(nullSeqs, [],
      'the later no-op and KEEP do not invoke the application callback')
    t.ok(leader._commitSeq >= keepSeq,
      'the state-one leader commits the KEEP record')
    for (const node of cluster.nodes) {
      const meta = metaAt(node)
      t.ok(node.isOpen,
        `node ${node.id} remains online after no-op and KEEP application`)
      t.equal(meta.repair_state, 1n,
        `node ${node.id} preserves repair state one`)
      t.equal(meta.applied_seq, keepSeq,
        `node ${node.id} checkpoints the committed KEEP`)
      t.equal(node.log.begin, 0n,
        `node ${node.id} clamps the KEEP begin to the genesis entry`)
      t.deepEqual(raftTypes(node), ['noop', 'noop', 'keep'],
        `node ${node.id} retains every available entry through KEEP`)
    }
  })

test('state-one election KEEP reaches keepHalt and prunes', async (t) => {
  const fixture = clusterFixture(t, 'state-one-election-keep')
  const seeded = fixture.build()
  const seedLeader = await openAndElect(seeded.nodes)
  for (let count = 0; count < 4; count++) {
    await seedLeader._appendToSelfAndFollowers(Buffer.alloc(0))
  }
  await waitApplied(seeded.nodes, 4n)
  for (const node of seeded.nodes) {
    node.db.prepare(`
      UPDATE monsterft_meta SET repair_state = 1 WHERE id = 1
    `).run()
    node._monsterRepairState = 1
  }
  t.ok(seeded.nodes.every((node) => retainedCount(node) === 5n),
    'state-one seed begins one row below keepHalt')
  closeNodesQuietly(seeded.nodes)

  const restarted = fixture.build({
    opts: keepOpts(2, 3, 6),
  })
  const candidate = await openAndElect(restarted.nodes, restarted.nodes[0])
  const keepSeq = 5n

  t.deepEqual(raftTypes(candidate), ['noop', 'keep'],
    'the election KEEP reaches the ceiling and prunes to keepTarget')
  for (const node of restarted.nodes) {
    const meta = metaAt(node)
    t.equal(node.log.begin, 4n,
      `state-one node ${node.id} prunes at the inclusive ceiling`)
    t.equal(meta.applied_seq, keepSeq,
      `state-one node ${node.id} checkpoints the election KEEP`)
    t.equal(meta.repair_state, 1n,
      `state-one node ${node.id} preserves its repair state`)
    t.ok(node.isOpen,
      `state-one node ${node.id} remains online after election KEEP`)
  }
  const fenced = await errorOf(candidate.append(toBuf({
    key: 'state-one-election-fenced', value: 1,
  })))
  t.equal(fenced?.code, REPAIR_QUORUM_IMPOSSIBLE,
    'the state-one KEEP leader remains command-fenced')
  t.equal(candidate.state, 'leader',
    'state-one command fencing preserves KEEP-established leadership')
})

test('failed persisted KEEP rejects as not committed and steps down',
  async (t) => {
    const fixture = clusterFixture(t, 'automatic-ambiguous')
    const cluster = fixture.build({ opts: keepOpts(3, 5, 8) })
    const leader = await openAndElect(cluster.nodes)
    await appendThroughSync(leader, toBuf({ key: 'warm-a', value: 1 }))
    await appendThroughSync(leader, toBuf({ key: 'warm-b', value: 2 }))
    t.equal(retainedCount(leader), 5n, 'the failure starts exactly at trigger')

    const injected = new Error('injected KEEP replication failure')
    const appendToFollowers = leader._appendToFollowers.bind(leader)
    let failKeep = true
    leader._appendToFollowers = (...args) => {
      if (failKeep) {
        failKeep = false
        return Promise.reject(injected)
      }
      return appendToFollowers(...args)
    }
    const appendEntry = leader._monsterAppendEntry.bind(leader)
    let keepAttempts = 0
    leader._monsterAppendEntry = (entry) => {
      if (entry.type === 'keep') { keepAttempts++ }
      return appendEntry(entry)
    }
    const fatals = []
    leader.on('fatal', (err) => fatals.push(err))
    const follower = cluster.nodes.find((node) => node !== leader)
    const failed = await withTimeout(errorOf(follower.append(toBuf({
      key: 'dropped', value: 3,
    }))), 'forwarded automatic KEEP failure')

    t.equal(failed?.message, 'auto KEEP failed',
      'the forwarded caller receives the automatic KEEP failure')
    t.equal(failed?.code, NOT_COMMIT,
      'the forwarded caller receives the not-committed error code')
    t.equal(failed?.sqlCode, null,
      'the automatic KEEP failure has no SQLite error code')
    t.equal(keepAttempts, 1, 'the failed leader attempts KEEP exactly once')
    t.deepEqual(raftTypes(leader), [
      'noop', 'cmd', 'sync', 'cmd', 'sync', 'keep',
    ], 'the ambiguous KEEP is locally persisted without a later CMD')
    t.deepEqual(cluster.nodes.filter((node) => node !== leader)
      .map(raftTypes), [
      ['noop', 'cmd', 'sync', 'cmd', 'sync'],
      ['noop', 'cmd', 'sync', 'cmd', 'sync'],
    ], 'the rejected replication attempt leaves followers without KEEP')
    t.equal(leader.state, 'follower', 'the failed KEEP leader steps down')
    t.equal(leader.leader, null,
      'the failed KEEP leader no longer identifies itself as leader')
    t.ok(leader.isOpen && leader.log.isOpen,
      'KEEP failure does not close either database')
    t.deepEqual(fatals, [], 'KEEP replication failure is nonfatal')
    t.notOk(fixture.errors.some(({ id }) => id === leader.id),
      'KEEP replication failure emits no public error event')
    t.deepEqual(cluster.nodes.map((node) => valueAt(node, 'dropped')),
      [null, null, null], 'the triggering user transition is permanently dropped')
    await sleep(30)
    t.equal(keepAttempts, 1, 'KEEP is not retried without another command')

    const replacement = cluster.nodes.find((node) => node !== leader)
    await electOpen(cluster.nodes, replacement)
    const later = await appendThroughSync(replacement, toBuf({
      key: 'later-term', value: 4,
    }))
    await waitApplied(cluster.nodes, later.syncSeq)
    t.equal(keepAttempts, 1,
      'the old leader does not append another KEEP')
    t.deepEqual(cluster.nodes.map((node) => [
      valueAt(node, 'dropped'), valueAt(node, 'later-term'),
    ]), [[null, 4], [null, 4], [null, 4]],
    'the replacement leader progresses without resurrecting the dropped command')
  })

test('failed election marker steps down without retry',
  async (t) => {
    const fixture = clusterFixture(t, 'election-retry')
    const opts = keepOpts(3, 5, 8)
    const seeded = fixture.build({ opts })
    const seedLeader = await openAndElect(seeded.nodes)
    await appendThroughSync(seedLeader, toBuf({ key: 'retry-a', value: 1 }))
    await appendThroughSync(seedLeader, toBuf({ key: 'retry-b', value: 2 }))
    t.equal(retainedCount(seedLeader), 5n,
      'election retry seed reaches the retention trigger')
    closeNodesQuietly(seeded.nodes)

    const restarted = fixture.build({ opts })
    const candidate = restarted.nodes[0]
    const injected = new Error('injected election KEEP replication failure')
    const append = candidate._appendToSelfAndFollowers.bind(candidate)
    const markers = []
    candidate._appendToSelfAndFollowers = (data) => {
      markers.push(data.length === 0 ? 'noop' : unpack(data).type)
      if (markers.length === 1) { return Promise.reject(injected) }
      return append(data)
    }

    restarted.nodes.forEach((node) => node.open())
    candidate._voteForSelf()
    await waitFor(() => fixture.warnings.some(({ id, err }) => {
      return id === candidate.id && err === injected
    }), 'failed election KEEP warning')
    await waitFor(() => candidate.state === 'follower',
      'failed election KEEP stepdown')
    await sleep(30)
    t.deepEqual(markers, ['keep'],
      'the failed marker is not retried in the same term')
    t.equal(fixture.warnings.filter(({ id, err }) => {
      return id === candidate.id && err === injected
    }).length, 1, 'the failed election KEEP emits its original warning once')
    t.equal(candidate.state, 'follower',
      'the failed election marker relinquishes leadership')
    const replacement = restarted.nodes.find((node) => node !== candidate)
    await electOpen(restarted.nodes, replacement)
    t.equal(replacement._commitTerm, replacement.term,
      'a replacement leader establishes readiness')
    t.deepEqual(raftTypes(replacement), ['cmd', 'sync', 'keep'],
      'the replacement leader commits one KEEP marker')
  })

test('term change suppresses an obsolete election KEEP retry', async (t) => {
  const fixture = clusterFixture(t, 'election-retry-term')
  const opts = keepOpts(3, 5, 8)
  const seeded = fixture.build({ opts })
  const seedLeader = await openAndElect(seeded.nodes)
  await appendThroughSync(seedLeader, toBuf({ key: 'term-a', value: 1 }))
  await appendThroughSync(seedLeader, toBuf({ key: 'term-b', value: 2 }))
  closeNodesQuietly(seeded.nodes)

  const restarted = fixture.build({ opts })
  const candidate = restarted.nodes[0]
  candidate._pingFollowers = noop
  const injected = new Error('injected obsolete election KEEP failure')
  candidate._appendToFollowers = () => Promise.reject(injected)
  const append = candidate._appendToSelfAndFollowers.bind(candidate)
  const markers = []
  candidate._appendToSelfAndFollowers = (data) => {
    markers.push(data.length === 0 ? 'noop' : unpack(data).type)
    return append(data)
  }
  restarted.nodes.forEach((node) => node.open())
  candidate._voteForSelf()
  await waitFor(() => fixture.warnings.some(({ id, err }) => {
    return id === candidate.id && err === injected
  }), 'failed election KEEP warning')
  const obsoleteTerm = candidate.term
  candidate._advanceTerm(obsoleteTerm + 1n)
  await sleep(50)

  t.deepEqual(markers, ['keep'],
    'the old term appends one KEEP and schedules no replacement marker')
  t.equal(candidate.term, obsoleteTerm + 1n,
    'the node retains the newer term')
  t.equal(candidate.state, 'follower',
    'the obsolete retry leaves the node in its newer follower role')
  t.deepEqual(raftTypes(candidate).slice(-1), ['keep'],
    'the obsolete term leaves no empty no-op fallback')
})

test('KEEP deletion failure fatally closes and a fresh object replays it',
  async (t) => {
    const fixture = clusterFixture(t, 'delete-replay')
    const opts = keepOpts(3, 5, 8)
    const first = fixture.build({ opts })
    const leader = await openAndElect(first.nodes)
    const old = await appendThroughSync(leader, toBuf({
      key: 'old', value: 1,
    }))
    const retained = await appendThroughSync(leader, toBuf({
      key: 'retained', value: 2,
    }))
    await waitApplied(first.nodes, retained.syncSeq)

    const beforeMeta = metaAt(leader)
    const expectedKeep = retained.syncSeq + 1n
    const failure = new Error('injected DB1 KEEP deletion failure')
    const fatal = deferred()
    leader.once('fatal', fatal.resolve)
    const raftDb = leader.log.db
    const prepare = raftDb.prepare.bind(raftDb)
    raftDb.prepare = (sql) => {
      if (/DELETE\s+FROM\s+raft_log\s+WHERE\s+seq\s*</i.test(sql)) {
        throw failure
      }
      return prepare(sql)
    }

    const failed = await withTimeout(errorOf(leader.append(toBuf({
      key: 'never-appended', value: 3,
    }))), 'DB1 KEEP deletion failure')
    t.equal(failed, leader._shutdownError,
      'the triggering append rejects with the shutdown error')
    const fatalError = await withTimeout(fatal.promise, 'DB1 KEEP fatal')
    t.ok(fatalError instanceof ErrorWithCode,
      'the deletion error is normalized in the fatal path')
    t.notEqual(fatalError, failure,
      'the fatal path replaces the original deletion error')
    t.equal(fatalError.code, APPLY_ERROR,
      'the deletion failure has APPLY_ERROR code')
    t.equal(fatalError.message, '(apply) injected DB1 KEEP deletion failure',
      'the deletion failure has application context')
    await waitFor(() => !leader.isOpen, 'DB1 failure close')

    const raftPath = fixture.paths.get(leader.id)
    const failedRaft = new DatabaseSync(raftPath, {
      readOnly: true,
      readBigInts: true,
    })
    const failedRaftSeqs = failedRaft.prepare(`
      SELECT seq FROM raft_log ORDER BY seq
    `).all().map(({ seq }) => seq)
    failedRaft.close()
    t.ok(failedRaftSeqs.includes(old.cmdSeq) &&
      failedRaftSeqs.includes(old.syncSeq) &&
      failedRaftSeqs.includes(expectedKeep),
    'failed DB1 deletion retains old rows and the committed KEEP for replay')

    const failedMonster = new DatabaseSync(`${raftPath}2`, {
      readOnly: true,
      readBigInts: true,
    })
    const failedMeta = failedMonster.prepare(`
      SELECT applied_seq, applied_entry_hash, repair_state,
             pending_cmd_seq, pending_local_digest
      FROM monsterft_meta WHERE id = 1
    `).get()
    const failedCommandTables = failedMonster.prepare(`
      SELECT COUNT(*) AS count
      FROM sqlite_schema
      WHERE type = 'table' AND name = 'monsterft_commands'
    `).get().count
    failedMonster.close()
    t.equal(failedCommandTables, 0n,
      'DB2 has no command-history table for KEEP to mutate')
    t.equal(failedMeta.applied_seq, beforeMeta.applied_seq,
      'DB1 failure leaves the DB2 checkpoint unchanged')
    t.ok(Buffer.from(failedMeta.applied_entry_hash)
      .equals(Buffer.from(beforeMeta.applied_entry_hash)),
    'DB1 failure leaves the DB2 checkpoint hash unchanged')
    t.equal(failedMeta.repair_state, beforeMeta.repair_state,
      'DB1 failure leaves the DB2 repair fence unchanged')
    t.equal(failedMeta.pending_cmd_seq, beforeMeta.pending_cmd_seq,
      'DB1 failure leaves the pending sequence unchanged')
    t.equal(failedMeta.pending_local_digest,
      beforeMeta.pending_local_digest,
      'DB1 failure leaves the pending digest unchanged')

    closeNodesQuietly(first.nodes)
    const restarted = fixture.build({ opts })
    const recovered = restarted.nodes.find((node) => node.id === leader.id)
    await openAndElect(restarted.nodes, recovered)
    t.equal(pendingAt(recovered), null,
      'fresh-object apply replays KEEP without creating command history')
    t.notOk(hasCommandTable(recovered),
      'fresh-object replay does not recreate command history')
    t.equal(raftSeqs(recovered)[0], retained.syncSeq,
      'fresh-object replay completes the deferred DB1 deletion')
    t.ok(recovered._applySeq > expectedKeep,
      'the recovered node advances through the later election no-op')
    t.equal(valueAt(recovered, 'never-appended'), null,
      'the user CMD behind the failed apply was never appended')
  })

test('KEEP checkpoint failure fatally closes and a fresh object replays it',
  async (t) => {
    const fixture = clusterFixture(t, 'checkpoint-replay')
    const opts = keepOpts(3, 5, 8)
    const first = fixture.build({ opts })
    const leader = await openAndElect(first.nodes)
    await appendThroughSync(leader, toBuf({
      key: 'old', value: 1,
    }))
    const retained = await appendThroughSync(leader, toBuf({
      key: 'retained', value: 2,
    }))
    await waitApplied(first.nodes, retained.syncSeq)

    const beforeMeta = metaAt(leader)
    const expectedKeep = retained.syncSeq + 1n
    const failure = new Error('injected DB2 KEEP checkpoint failure')
    const fatal = deferred()
    leader.once('fatal', fatal.resolve)
    const advanceApplied = leader._monsterAdvanceApplied.bind(leader)
    leader._monsterAdvanceApplied = (db, seq, entryHash) => {
      if (seq === expectedKeep) { throw failure }
      return advanceApplied(db, seq, entryHash)
    }

    const failed = await withTimeout(errorOf(leader.append(toBuf({
      key: 'never-appended', value: 3,
    }))), 'DB2 KEEP checkpoint failure')
    t.equal(failed, leader._shutdownError,
      'the triggering append rejects with the shutdown error')
    const fatalError = await withTimeout(fatal.promise, 'DB2 KEEP fatal')
    t.ok(fatalError instanceof ErrorWithCode,
      'the checkpoint error is normalized in the fatal path')
    t.notEqual(fatalError, failure,
      'the fatal path replaces the original checkpoint error')
    t.equal(fatalError.code, APPLY_ERROR,
      'the checkpoint failure has APPLY_ERROR code')
    t.equal(fatalError.message, '(apply) injected DB2 KEEP checkpoint failure',
      'the checkpoint failure has application context')
    await waitFor(() => !leader.isOpen, 'DB2 KEEP failure close')

    const raftPath = fixture.paths.get(leader.id)
    const failedRaft = new DatabaseSync(raftPath, {
      readOnly: true,
      readBigInts: true,
    })
    const failedRaftSeqs = failedRaft.prepare(`
      SELECT seq FROM raft_log ORDER BY seq
    `).all().map(({ seq }) => seq)
    failedRaft.close()
    t.deepEqual(failedRaftSeqs, [
      retained.cmdSeq,
      retained.syncSeq,
      expectedKeep,
    ], 'DB1 retains the predecessor checkpoint and committed KEEP after pruning')

    const failedMonster = new DatabaseSync(`${raftPath}2`, {
      readOnly: true,
      readBigInts: true,
    })
    const failedMeta = failedMonster.prepare(`
      SELECT applied_seq, applied_entry_hash
      FROM monsterft_meta WHERE id = 1
    `).get()
    failedMonster.close()
    t.equal(failedMeta.applied_seq, beforeMeta.applied_seq,
      'the failed write leaves the DB2 checkpoint at its predecessor')
    t.ok(Buffer.from(failedMeta.applied_entry_hash)
      .equals(Buffer.from(beforeMeta.applied_entry_hash)),
    'the failed write leaves the DB2 checkpoint hash unchanged')

    closeNodesQuietly(first.nodes)
    const restarted = fixture.build({ opts })
    const recovered = restarted.nodes.find((node) => node.id === leader.id)
    const applyKeep = recovered._monsterApplyKeep.bind(recovered)
    const replayed = []
    recovered._monsterApplyKeep = (record, keepSeq, entryHash) => {
      replayed.push(keepSeq)
      return applyKeep(record, keepSeq, entryHash)
    }
    await openAndElect(restarted.nodes, recovered)
    t.deepEqual(replayed, [expectedKeep],
      'the fresh object replays the committed KEEP exactly once')
    t.ok(metaAt(recovered).applied_seq > expectedKeep,
      'recovery checkpoints KEEP before the later election no-op')
    t.equal(valueAt(recovered, 'never-appended'), null,
      'the user CMD behind the failed apply was never appended')
  })

test('keepHalt allows its ceiling and blocks appends beyond it', async (t) => {
  const fixture = clusterFixture(t, 'halt-before-append')
  const cluster = fixture.build({ opts: keepOpts(2, 3, 6) })
  const leader = await openAndElect(cluster.nodes)

  for (let count = 0; count < 4; count++) {
    await leader._appendToSelfAndFollowers(Buffer.alloc(0))
  }
  t.equal(retainedCount(leader), 5n,
    'leader remains open at one row below keepHalt')
  t.ok(leader.isOpen, 'the halt check is not premature')

  await leader._appendToSelfAndFollowers(Buffer.alloc(0))
  t.equal(retainedCount(leader), 6n,
    'an append may reach the inclusive keepHalt ceiling')
  t.ok(leader.isOpen, 'reaching keepHalt leaves the leader open')

  const fatal = deferred()
  const fatals = []
  leader.on('fatal', (err) => {
    fatals.push(err)
    fatal.resolve(err)
  })
  const failed = await errorOf(
    leader._appendToSelfAndFollowers(Buffer.alloc(0)),
  )
  const halt = await withTimeout(fatal.promise, 'pre-append keepHalt fatal')
  t.equal(halt.message, 'KEEP halt reached',
    'the inclusive ceiling reports the KEEP halt')
  t.equal(halt.code, KEEP_HALT,
    'the inclusive ceiling has its dedicated fatal code')
  t.equal(failed?.code, KEEP_HALT,
    'the blocked local append rejects with the halt error')
  const persisted = new DatabaseSync(fixture.paths.get(leader.id), {
    readOnly: true,
    readBigInts: true,
  })
  const persistedCount = persisted.prepare(`
    SELECT COUNT(*) AS count FROM raft_log
  `).get().count
  persisted.close()
  t.equal(persistedCount, 6n,
    'the rejected append persists no row beyond keepHalt')
  t.equal(fatals.length, 1, 'the boundary emits fatal exactly once')
  t.notOk(leader.isOpen, 'keepHalt closes the leader')
  t.ok(cluster.nodes.filter((node) => node !== leader)
    .every((node) => node.isOpen), 'other members remain open')
})

test('a leader acquisition at keepHalt closes before another marker', async (t) => {
  const fixture = clusterFixture(t, 'halt-on-election')
  const seed = fixture.build()
  const seedLeader = await openAndElect(seed.nodes)
  await appendThroughSync(seedLeader, toBuf({ key: 'seed-a', value: 1 }))
  await appendThroughSync(seedLeader, toBuf({ key: 'seed-b', value: 2 }))
  await seedLeader._appendToSelfAndFollowers(Buffer.alloc(0))
  await waitApplied(seed.nodes, 5n)
  t.ok(seed.nodes.every((node) => retainedCount(node) === 6n),
    'the restart seed is exactly at the future halt ceiling')
  closeNodesQuietly(seed.nodes)

  const restarted = fixture.build({ opts: keepOpts(2, 3, 6) })
  const candidate = restarted.nodes[0]
  const fatals = []
  candidate.on('fatal', (err) => fatals.push(err))
  restarted.nodes.forEach((node) => node.open())
  const beforeSeq = candidate.seq
  candidate._voteForSelf()
  await waitFor(() => !candidate.isOpen, 'halted leader acquisition')
  await waitFor(() => fatals.length > 0, 'leader acquisition fatal event')

  t.equal(fatals.length, 1, 'leader acquisition emits one halt fatal')
  t.equal(fatals[0].code, KEEP_HALT,
    'leader acquisition uses the halt error code')
  const persisted = new DatabaseSync(fixture.paths.get(candidate.id), {
    readOnly: true,
    readBigInts: true,
  })
  const persistedHead = persisted.prepare(`
    SELECT MAX(seq) AS seq FROM raft_log
  `).get().seq
  persisted.close()
  t.equal(persistedHead, beforeSeq,
    'the pre-append halt check prevents another election marker')
  t.notOk(candidate.isOpen, 'the over-limit leader candidate closes')
  t.ok(restarted.nodes.slice(1).every((node) => node.isOpen),
    'the other restarted members remain available')
})

test('automatic KEEP cancels a lagging replica while its healthy quorum progresses',
  async (t) => {
    const fixture = clusterFixture(t, 'lagging-warning')
    const cluster = fixture.build({
      opts: {
        ...keepOpts(3, 5, 8),
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
    t.equal(leader.log.begin, 3n,
      'automatic KEEP advances the leader retained floor')
    t.equal(healthy.log.begin, 3n,
      'automatic KEEP advances the healthy follower retained floor')
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
      'later progress drives the leader through another automatic KEEP')
    t.equal(healthy.log.begin, 6n,
      'the healthy follower applies the later KEEP and forgets old entries')
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
