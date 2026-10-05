import test from 'tape'
import { pack, unpack } from 'msgpackr'
import { DatabaseSync } from 'node:sqlite'
import {
  ARGUMENT_ILLEGAL,
  APPEND_TIMEOUT,
  APPLY_ERROR,
  ErrorWithCode,
  NO_LEADER,
  NODE_NOT_OPEN,
  NOT_LEADER,
  TERM_DIFF,
  REPAIR_OUTSIDE_AGREEMENT,
  REPAIR_QUORUM_IMPOSSIBLE,
  RPC_ILLEGAL,
} from '../src/error.js'
import { MonsterFt, SQLiteLog } from '../src/index.js'
import { MonsterNode } from '../src/monster.js'
import { MonsterFt as DirectMonsterFt } from '../src/monsterft.js'
import { databasePath, sleep, ready, leaders, followers } from './util.js'

const noop = () => {}
const errorContext = [
  'cmdSeq', 'syncSeq', 'index', 'ambiguous', 'cause',
  'rollbackError',
]
const hasErrorContext = (err) => errorContext.some((name) => {
  return Object.hasOwn(err, name)
})
const ids = ['1', '2', '3']
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
const clearStorage = (basePath) => {
  new SQLiteLog(basePath).del()
  new SQLiteLog(`${basePath}2`).del()
}

const monsterState = (basePath, cmdSeq=null) => {
  const db = new DatabaseSync(`${basePath}2`, { readBigInts: true })
  try {
    const meta = db.prepare(`
      SELECT applied_seq, repair_state, pending_cmd_seq
      FROM monsterft_meta WHERE id = 1
    `).get()
    if (cmdSeq === null) { return { meta: { ...meta } } }
    const raft = new DatabaseSync(basePath, {
      readOnly: true,
      readBigInts: true,
    })
    try {
      const rows = raft.prepare(`
        SELECT seq, entry FROM raft_log ORDER BY seq DESC
      `).all()
      let decision = null
      for (const row of rows) {
        const entry = Buffer.from(row.entry)
        if (entry.byteLength === 8) { continue }
        const record = unpack(entry.subarray(8))
        if (record.type !== 'sync' || record.cmdSeq !== cmdSeq) { continue }
        decision = { quorum: record.quorum, sync_seq: row.seq }
        break
      }
      return { meta: { ...meta }, decision }
    } finally {
      raft.close()
    }
  } finally {
    db.close()
  }
}

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
  while (!fn()) {
    if (Date.now() >= end) { throw new Error(`${name} timeout`) }
    await sleep(5)
  }
}

const withTimeout = (promise, name, ms=8_000) => {
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

const initializeApp = (conn) => {
  conn.exec(`
    CREATE TABLE IF NOT EXISTS monster_test_items (
      key TEXT PRIMARY KEY,
      value INTEGER NOT NULL
    ) STRICT
  `)
}

const applyApp = async (conn, node, buf) => {
  await Promise.resolve()
  const cmd = toObj(buf)
  if (cmd.op === 'set') {
    conn.prepare(`
      INSERT INTO monster_test_items (key, value) VALUES (?, ?)
      ON CONFLICT (key) DO UPDATE SET value = excluded.value
    `).run(cmd.key, cmd.value)
    return { key: cmd.key, value: cmd.value }
  }
  if (cmd.op === 'get') {
    const row = conn.prepare(`
      SELECT key, value FROM monster_test_items WHERE key = ?
    `).get(cmd.key)
    return row === undefined ? null : { key: row.key, value: Number(row.value) }
  }
  if (cmd.op === 'fail') {
    conn.prepare(`
      INSERT INTO monster_test_items (key, value) VALUES (?, ?)
      ON CONFLICT (key) DO UPDATE SET value = excluded.value
    `).run(cmd.key, cmd.value)
    const err = new Error(cmd.message)
    err.code = Object.hasOwn(cmd, 'code') ? cmd.code : 'E_MONSTER_TEST'
    if (Object.hasOwn(cmd, 'sqlCode')) { err.sqlCode = cmd.sqlCode }
    throw err
  }
  throw new Error(`unknown test op ${cmd.op}`)
}

const valueAt = (node, key) => {
  const row = node.db.prepare(`
    SELECT value FROM monster_test_items WHERE key = ?
  `).get(key)
  return row === undefined ? null : Number(row.value)
}

const headRecord = (node) => unpack(node.head)
const raftRecordAt = (node, seq) => {
  const { entry } = node.log.db.prepare(`
    SELECT entry FROM raft_log WHERE seq = ?
  `).get(seq)
  return unpack(Buffer.from(entry).subarray(8))
}
const raftTermAt = (node, seq) => {
  const { entry } = node.log.db.prepare(`
    SELECT entry FROM raft_log WHERE seq = ?
  `).get(seq)
  return Buffer.from(entry).readBigUInt64LE()
}

const makeBus = (messages, intercept=null) => {
  const nodes = new Map()
  return {
    register: (node) => nodes.set(node.id, node),
    send: (to, from, original) => {
      messages.push({ to, from, msg: original })
      let msg = original
      if (intercept !== null) {
        const transformed = intercept(to, from, original)
        if (transformed === false) { return }
        if (transformed !== undefined) { msg = transformed }
      }
      const node = nodes.get(to)
      if (!node) { throw new Error(`node ${from} send to ${to} not found`) }
      // Return the receive promise so RaftNode observes failures from both
      // Raft and MonsterFt message handlers.
      return node.onReceive(from, msg)
    },
  }
}

const makeFixture = (t, name) => {
  const unique = `${++fixtureId}-${process.pid}-${name}`
  const paths = new Map(ids.map((id) => [id, databasePath(`monster-${unique}-${id}`)]))
  const allNodes = []

  const build = ({
    apply=applyApp,
    opts={},
    intercept=null,
    members=ids,
  }={}) => {
    const messages = []
    const bus = makeBus(messages, intercept)
    const calls = []
    const nodes = ids.map((id) => {
      const send = (to, msg) => bus.send(to, id, msg)
      let node = null
      node = new MonsterFt(id, members, send, paths.get(id), {
        electionTimeout: 60_000,
        pingTimeout: 250,
        appendTimeout: 5_000,
        quorum: 2,
        ...opts,
        apply: async (conn, buf, term, seq, index, matchIndex) => {
          const current = node
          const cmd = buf === null
            ? null
            : buf.length === 0 ? { op: 'empty' } : toObj(buf)
          calls.push({
            id: current.id,
            buf,
            cmd,
            term,
            seq,
            index,
            matchIndex,
            sameConn: conn === current.db,
            separateConn: current.db !== current.log.db,
          })
          if (seq === 0n) {
            initializeApp(conn)
            return
          }
          if (buf === null) { return }
          return apply(conn, current, buf, term, seq, index, matchIndex)
        },
      })
      node.on('error', noop)
      node.on('warn', noop)
      bus.register(node)
      allNodes.push(node)
      return node
    })
    return { nodes, calls, messages }
  }

  const clear = () => {
    for (const file of paths.values()) { clearStorage(file) }
  }

  t.teardown(() => {
    closeNodesQuietly(allNodes)
    clear()
  })

  return { build, clear, paths }
}

const openAndElect = async (cluster) => {
  openNodes(cluster.nodes)
  cluster.nodes[0]._voteForSelf()
  await ready(cluster.nodes, null, true)
  const leader = leaders(cluster.nodes)[0]
  if (leader.id !== '1') { throw new Error(`expected node 1 leader, got ${leader.id}`) }
  return leader
}

const waitApplied = (nodes, seq) => {
  return waitFor(
    () => nodes.every((node) => node._applySeq >= seq),
    `all selected nodes apply through ${seq}`,
  )
}

const waitForOutcome = (cluster, from, to, cmdSeq) => {
  return waitFor(() => cluster.messages.some((entry) => {
    return entry.from === from &&
      entry.to === to &&
      entry.msg.type === 'monster_outcome' &&
      entry.msg.cmdSeq === cmdSeq
  }), `outcome ${from} -> ${to} for CMD ${cmdSeq}`)
}

const cmdSeqFor = (cluster, key, id='1') => {
  return cluster.calls.find((call) => {
    return call.id === id && call.cmd?.key === key
  })?.seq
}

test('MonsterFt extends MonsterNode', (t) => {
  t.equal(MonsterFt, DirectMonsterFt,
    'the package exports MonsterFt directly from monsterft.js')
  t.equal(Object.getPrototypeOf(DirectMonsterFt.prototype), MonsterNode.prototype,
    'MonsterFt directly extends MonsterNode')
  t.end()
})

test('MonsterFt forwarded command timeout is coded and cleans its waiter',
  async (t) => {
    const fixture = makeFixture(t, 'forwarded-command-timeout')
    fixture.clear()
    const { nodes } = fixture.build({ opts: { appendTimeout: 10 } })
    const node = nodes[0]
    const cid = 'forwarded-command-timeout'
    const work = node._monsterSendAndAwaitCmdAck('2', {
      type: 'fwd_cmd',
      cid,
      term: 0n,
      items: [],
    }, () => true)

    t.equal(node._acks.has(cid), true,
      'registers the forwarded-command waiter before timing out')
    const err = await rejects(t, work, /^forward CMD timeout$/,
      'rejects with the command timeout message')
    t.ok(err instanceof ErrorWithCode,
      'the forwarded-command timeout is an ErrorWithCode')
    t.equal(err.code, APPEND_TIMEOUT,
      'the forwarded-command timeout uses APPEND_TIMEOUT')
    t.equal(err.sqlCode, null,
      'the forwarded-command timeout has no SQLite code')
    t.equal(node._acks.has(cid), false,
      'removes the forwarded-command waiter after timeout')

    const noLeader = await rejects(
      t,
      node._monsterFwdCmdToLeader([Buffer.from('item')]),
      /^forward no leader$/,
      'forwarding without a leader uses the normalized message',
    )
    t.ok(noLeader instanceof ErrorWithCode,
      'forwarding without a leader throws ErrorWithCode')
    t.equal(noLeader.code, NO_LEADER,
      'forwarding without a leader uses NO_LEADER')
    t.equal(noLeader.sqlCode, null,
      'forwarding without a leader has no SQLite code')
  })

test('MonsterFt forwarded commands report leader and term state', async (t) => {
  const fixture = makeFixture(t, 'forwarded-command-state')
  fixture.clear()
  const { nodes } = fixture.build()
  const node = nodes[0]
  const responses = []
  node.send = (to, msg) => responses.push([to, msg])
  const msg = {
    type: 'fwd_cmd', cid: 'forwarded-command-state', term: 0n,
    items: [Buffer.from('item')],
  }

  await node._monsterRxFwdCmd('2', { ...msg, items: [] })
  await node._monsterRxFwdCmd('2', msg)
  node.state = 'leader'
  node.term = 1n
  await node._monsterRxFwdCmd('2', msg)

  t.equal(responses[0][0], '2', 'returns the invalid-RPC error to the sender')
  t.equal(responses[0][1].msg, 'forward CMD illegal',
    'retains the invalid-command message')
  t.equal(responses[0][1].code, RPC_ILLEGAL,
    'uses RPC_ILLEGAL for malformed forwarded commands')
  t.equal(responses[0][1].sqlCode, null,
    'the invalid-RPC response has no SQLite code')
  t.equal(responses[1][0], '2', 'returns the not-leader error to the sender')
  t.equal(responses[1][1].msg, 'node not leader',
    'uses the not-leader message')
  t.equal(responses[1][1].code, NOT_LEADER,
    'uses NOT_LEADER when the receiver is not leader')
  t.equal(responses[1][1].sqlCode, null,
    'the not-leader response has no SQLite code')
  t.equal(responses[2][0], '2', 'returns the term error to the sender')
  t.equal(responses[2][1].msg, 'node term different',
    'uses the term-difference message')
  t.equal(responses[2][1].code, TERM_DIFF,
    'uses TERM_DIFF when the request term differs')
  t.equal(responses[2][1].sqlCode, null,
    'the term-difference response has no SQLite code')
})

test('MonsterFt emits durable CMD and SYNC events around an early forwarded result',
  async (t) => {
  const fixture = makeFixture(t, 'append')
  fixture.clear()
  const cluster = fixture.build()
  const leader = await openAndElect(cluster)
  const follower = followers(cluster.nodes)[0]

  await waitFor(() => {
    return cluster.calls.filter((call) => {
      return call.seq === 0n && call.buf === null
    }).length === cluster.nodes.length
  }, 'initial no-op callbacks')
  const initialNoops = cluster.calls.filter((call) => {
    return call.seq === 0n && call.buf === null
  })
  t.equal(initialNoops.length, 3,
    'the initial no-op invokes apply once on every node')
  t.ok(initialNoops.every((call) => call.index === 0),
    'the initial no-op uses item index zero')
  t.ok(initialNoops.every((call) => call.matchIndex === null),
    'the initial no-op has no Raft matchIndex snapshot')
  t.ok(initialNoops.every((call) => {
    const node = cluster.nodes.find(({ id }) => id === call.id)
    return call.term === raftTermAt(node, call.seq)
  }), 'the initial no-op receives its real Raft term and sequence')
  t.ok(initialNoops.every((call) => call.sameConn),
    'the initial no-op receives each Monster SQLite connection')
  t.ok(initialNoops.every((call) => call.separateConn),
    'the initial no-op uses a connection distinct from the Raft log DB')

  const leaderPongBefore = follower._pongs.get(leader.id)
  const changeListenersBefore = follower.listenerCount('change')
  const cmdEvents = cluster.nodes.map((node) => {
    return new Promise((resolve) => {
      node.once('cmd', (event) => {
        const meta = node.db.prepare(`
          SELECT pending_cmd_seq
          FROM monsterft_meta WHERE id = 1
        `).get()
        resolve({
          event,
          record: raftRecordAt(node, event.cmdSeq),
          pendingCmdSeq: meta.pending_cmd_seq,
          cachedCmdSeq: node._monsterPendingCommand?.cmdSeq ?? null,
          applySeq: node._applySeq,
        })
      })
    })
  })
  const syncEvents = cluster.nodes.map((node) => {
    return new Promise((resolve) => {
      node.once('sync', (event) => {
        const meta = node.db.prepare(`
          SELECT repair_state, pending_cmd_seq
          FROM monsterft_meta WHERE id = 1
        `).get()
        resolve({
          event,
          record: raftRecordAt(node, event.syncSeq),
          applySeq: node._applySeq,
          repairState: meta.repair_state,
          pendingCmdSeq: meta.pending_cmd_seq,
          cachedPending: node._monsterPendingCommand,
        })
      })
    })
  })
  const syncEntered = deferred()
  const releaseSync = deferred()
  t.teardown(() => releaseSync.resolve())
  const appendEntry = leader._monsterAppendEntry.bind(leader)
  leader._monsterAppendEntry = async (entry) => {
    if (entry.type === 'sync') {
      syncEntered.resolve()
      await releaseSync.promise
    }
    return appendEntry(entry)
  }

  const appending = follower.append(toBuf({
    op: 'set', key: 'alpha', value: 7,
  }))
  await withTimeout(syncEntered.promise, 'forwarded SYNC append gate')
  const tuple = await withTimeout(appending, 'early forwarded result')
  const [cmdSeq, result] = tuple

  t.equal(tuple.length, 2, 'the public forwarded result has two elements')
  t.equal(typeof cmdSeq, 'bigint', 'CMD sequence is a bigint')
  t.deepEqual(result, { key: 'alpha', value: 7 }, 'returns the agreed result')
  const fwdCmdAck = cluster.messages.find(({ from, to, msg }) => {
    return from === leader.id && to === follower.id &&
      msg.type === 'ack' && msg.cmdSeq === cmdSeq
  })
  t.ok(fwdCmdAck, 'the leader sends a correlated command ACK')
  t.ok(Array.isArray(fwdCmdAck.msg.results),
    'the command ACK carries structured results without nested packing')
  t.equal(fwdCmdAck.msg.results.length, 1,
    'the command ACK carries one result for one forwarded item')
  t.equal(fwdCmdAck.msg.results[0][0], 0,
    'the structured result identifies a fulfilled outcome')
  t.deepEqual(fwdCmdAck.msg.results[0][1], result,
    'the structured result retains the agreed application value')
  t.equal(follower._pongs.get(leader.id), leaderPongBefore,
    'a MonsterFt ACK does not update Raft replication liveness')
  t.equal(follower.listenerCount('change'), changeListenersBefore,
    'a successful FWD_CMD leaves change listeners unchanged')
  t.equal(headRecord(leader).type, 'cmd',
    'the forwarded caller settles while SYNC append is blocked')

  const prepare = leader.db.prepare.bind(leader.db)
  let selects = 0
  leader.db.prepare = (sql) => {
    if (/^\s*SELECT\b/i.test(String(sql))) { selects++ }
    return prepare(sql)
  }
  let pendingCommand = null
  let pendingSnapshot = null
  try {
    pendingCommand = await leader._monsterRunDbGetCmd(cmdSeq)
    pendingSnapshot = await leader._monsterRunDbGetCmd()
  } finally {
    delete leader.db.prepare
  }
  t.equal(selects, 0,
    'runtime pending-command lookups read the authoritative cache without SQL')
  t.equal(pendingCommand.cmdSeq, cmdSeq,
    'the cache exposes the pending CMD at client settlement')
  t.equal(pendingSnapshot, pendingCommand,
    'the default lookup returns the authoritative pending CMD')
  t.equal(pendingCommand, leader._monsterPendingCommand,
    'the exact lookup returns the authoritative pending CMD')
  t.equal(await leader._monsterRunDbGetCmd(cmdSeq - 1n), null,
    'an older sequence cannot alias the pending CMD')

  const durableCmdEvents = await Promise.all(cmdEvents)
  for (let index = 0; index < durableCmdEvents.length; index++) {
    const {
      event,
      record,
      pendingCmdSeq,
      cachedCmdSeq,
      applySeq,
    } = durableCmdEvents[index]
    t.deepEqual(event, {
      cmdSeq,
      cmdCount: 1,
    }, `node ${ids[index]} emits the documented CMD payload`)
    t.equal(record.type, 'cmd',
      `node ${ids[index]} stores the Raft CMD before emitting`)
    t.equal(pendingCmdSeq, cmdSeq,
      `node ${ids[index]} publishes pending metadata before emitting CMD`)
    t.equal(cachedCmdSeq, cmdSeq,
      `node ${ids[index]} publishes the pending cache before emitting CMD`)
    t.ok(applySeq >= cmdSeq,
      `node ${ids[index]} updates local applied state before emitting CMD`)
  }

  releaseSync.resolve()
  const durableSyncEvents = await Promise.all(syncEvents)
  const syncSeq = durableSyncEvents[0].event.syncSeq
  t.equal(syncSeq, cmdSeq + 1n, 'one compact SYNC immediately follows the CMD')
  for (let index = 0; index < durableSyncEvents.length; index++) {
    const {
      event,
      record,
      applySeq,
      repairState,
      pendingCmdSeq,
      cachedPending,
    } = durableSyncEvents[index]
    t.deepEqual(event, {
      cmdSeq,
      syncSeq,
      quorum: true,
      agree: event.agree,
      disagree: [],
    }, `node ${ids[index]} emits the documented SYNC payload`)
    t.equal(record.type, 'sync',
      `node ${ids[index]} stores the Raft SYNC before emitting`)
    t.equal(record.cmdSeq, cmdSeq,
      `node ${ids[index]} stores the SYNC command sequence before emitting`)
    t.equal(record.quorum, true,
      `node ${ids[index]} stores the quorum decision before emitting`)
    t.deepEqual(event.agree, record.agree,
      `node ${ids[index]} emits the durable agreeing set`)
    t.deepEqual(event.disagree, record.disagree,
      `node ${ids[index]} emits the durable disagreeing set`)
    t.ok(applySeq >= syncSeq,
      `node ${ids[index]} updates local applied state before emitting SYNC`)
    t.equal(repairState, 0n,
      `node ${ids[index]} publishes clean repair state before emitting SYNC`)
    t.equal(pendingCmdSeq, null,
      `node ${ids[index]} clears pending metadata before emitting SYNC`)
    t.equal(cachedPending, null,
      `node ${ids[index]} clears the pending cache before emitting SYNC`)
  }
  t.deepEqual(await Promise.all(cluster.nodes.map((node) => {
    return node._monsterRunDbGetCmd(cmdSeq)
  })), [null, null, null], 'resolved CMD lookups return null on every node')
  t.deepEqual(cluster.nodes.map((node) => valueAt(node, 'alpha')), [7, 7, 7],
    'SYNC retains the identical SQLite state on every node')

  const sync = headRecord(leader)
  t.equal(sync.type, 'sync', 'Raft head is the SYNC record')
  t.equal(sync.cmdSeq, cmdSeq, 'SYNC identifies its CMD')
  t.equal(sync.quorum, true, 'SYNC explicitly records an agreeing quorum')
  t.ok(sync.digest instanceof Uint8Array && sync.digest.byteLength === 32,
    'SYNC carries the winning digest')
  t.ok(sync.agree.length >= 2, 'SYNC records at least the agreeing quorum')
  t.deepEqual([...sync.agree].sort(), sync.agree, 'agree IDs are sorted')
  t.deepEqual(sync.disagree, [], 'a matching execution has no known disagreement')
  t.equal(leader.db.prepare(`
    SELECT repair_state FROM monsterft_meta WHERE id = 1
  `).get().repair_state, 0n, 'a clean SYNC does not activate the repair fence')

  t.ok(cluster.calls.every((call) => call.sameConn),
    'apply receives the Monster SQLite connection')
  t.ok(cluster.calls.every((call) => call.separateConn),
    'apply uses a Monster DB connection distinct from the Raft log DB')
  t.equal(typeof follower.read, 'undefined',
    'MonsterFt exposes no read operation')
  })

test('MonsterFt ignores malformed structured command ACK results', async (t) => {
  const fixture = makeFixture(t, 'malformed-fwd-cmd-results')
  fixture.clear()
  let fwdCmdAck = null
  let isolated = null
  const cluster = fixture.build({
    intercept: (to, from, msg) => {
      if (isolated !== null && (to === isolated || from === isolated)) {
        return false
      }
      if (msg.type !== 'ack' || !Object.hasOwn(msg, 'cmdSeq')) { return }
      fwdCmdAck = { to, from, msg }
      return false
    },
  })
  const leader = await openAndElect(cluster)
  const follower = followers(cluster.nodes)[0]
  const changeListeners = follower.listenerCount('change')
  let settled = false
  const appending = follower.append(toBuf({
    op: 'set', key: 'malformed-fwd-cmd-results', value: 22,
  })).finally(() => { settled = true })
  appending.catch(noop)

  await waitFor(() => fwdCmdAck !== null, 'captured command ACK')
  t.equal(follower.listenerCount('change'), changeListeners,
    'a pending FWD_CMD installs no change listener')
  t.ok(Array.isArray(fwdCmdAck.msg.results),
    'the captured valid ACK uses structured results')
  const invalid = [
    Buffer.from(pack(fwdCmdAck.msg.results)),
    [],
    [[2, 'invalid tag']],
    [[0]],
    [[1, 'invalid code', true, null]],
    [[1, 'invalid sqlCode', null, 'SQLITE_ERROR']],
  ]
  for (const results of invalid) {
    await follower.onReceive(leader.id, { ...fwdCmdAck.msg, results })
    await Promise.resolve()
    t.notOk(settled, 'an invalid result shape leaves the correlated waiter active')
  }

  isolated = follower.id
  const fwdCmdTerm = follower.term
  t.equal(follower._advanceTerm(fwdCmdTerm + 1n), true,
    'the forwarding node advances term and clears its leader view')
  await Promise.resolve()
  t.equal(follower._acks.has(fwdCmdAck.msg.cid), true,
    'the correlated waiter survives the leadership change')

  const changedTermAck = { ...fwdCmdAck.msg, term: follower.term }
  await follower.onReceive(leader.id, changedTermAck)
  const [cmdSeq, result] = await withTimeout(appending, 'valid command ACK')
  t.equal(cmdSeq, changedTermAck.cmdSeq,
    'the newer-term structured ACK settles with its command sequence')
  t.deepEqual(result, { key: 'malformed-fwd-cmd-results', value: 22 },
    'the newer-term structured ACK settles with its application value')
})

test('MonsterFt isolates CMD and SYNC listener failures and payload mutation',
  async (t) => {
    const fixture = makeFixture(t, 'event-listener-isolation')
    fixture.clear()
    const cluster = fixture.build()
    const leader = await openAndElect(cluster)
    const errors = []
    const erroredTwice = deferred()
    leader.on('error', (err) => {
      if (!/injected (?:CMD|SYNC) listener failure/.test(err.message)) { return }
      errors.push(err.message)
      if (errors.length === 2) { erroredTwice.resolve() }
    })
    leader.once('cmd', () => {
      throw new Error('injected CMD listener failure')
    })
    leader.once('sync', (event) => {
      event.agree.splice(0)
      event.disagree.push('mutated')
      throw new Error('injected SYNC listener failure')
    })

    const [cmdSeq, result] = await leader.append(toBuf({
      op: 'set', key: 'event-isolation', value: 8,
    }))
    t.deepEqual(result, { key: 'event-isolation', value: 8 },
      'a throwing CMD listener cannot prevent client agreement')
    await withTimeout(erroredTwice.promise, 'event listener errors')
    t.deepEqual(errors.sort(), [
      'injected CMD listener failure',
      'injected SYNC listener failure',
    ], 'each listener exception emits one error')

    const sync = headRecord(leader)
    t.equal(sync.type, 'sync', 'protocol progress survives both listener exceptions')
    t.equal(sync.cmdSeq, cmdSeq, 'the surviving SYNC belongs to the command')
    t.ok(sync.agree.length >= 2,
      'mutating the emitted agree array cannot alter protocol state')
    t.deepEqual(sync.disagree, [],
      'mutating the emitted disagree array cannot alter protocol state')

    const laterSynced = nextEvent(leader, 'sync')
    const [, later] = await leader.append(toBuf({
      op: 'get', key: 'event-isolation',
    }))
    t.deepEqual(later, { key: 'event-isolation', value: 8 },
      'later commands continue after diagnostic listener failures')
    await laterSynced
  })

test('MonsterFt preserves an agreed result when its later SYNC append fails',
  async (t) => {
    const fixture = makeFixture(t, 'post-settlement-sync-failure')
    fixture.clear()
    const cluster = fixture.build()
    const leader = await openAndElect(cluster)
    const injected = new Error('injected post-settlement SYNC failure')
    const warned = deferred()
    const warnings = []
    leader.on('warn', (err) => {
      if (err !== injected) { return }
      warnings.push(err)
      warned.resolve(err)
    })
    const unhandled = []
    const onUnhandled = (reason) => unhandled.push(reason)
    process.on('unhandledRejection', onUnhandled)
    t.teardown(() => process.removeListener('unhandledRejection', onUnhandled))

    const appendEntry = leader._monsterAppendEntry.bind(leader)
    leader._monsterAppendEntry = (entry) => {
      if (entry.type === 'sync') { return Promise.reject(injected) }
      return appendEntry(entry)
    }

    const tuple = await leader.append(toBuf({
      op: 'set', key: 'settled-before-sync-failure', value: 18,
    }))
    const [, result] = tuple
    t.equal(tuple.length, 2, 'the agreed client result remains settled')
    t.deepEqual(result, {
      key: 'settled-before-sync-failure',
      value: 18,
    }, 'post-settlement failure cannot replace the application result')

    const warning = await withTimeout(warned.promise, 'post-settlement warning')
    t.equal(warnings.length, 1, 'the failed SYNC emits one warning')
    t.equal(warning, injected, 'the warning preserves the original failure')
    t.notEqual(leader.state, 'leader',
      'the same-term leader steps down after failed SYNC append')
    t.equal(headRecord(leader).type, 'cmd',
      'the failed SYNC appends no durable record')

    await sleep(25)
    t.deepEqual(unhandled, [],
      'the detached protocol-completion failure is fully consumed')
  })

test('MonsterFt inherits eager committed CMD announcements from Raft', async (t) => {
  const fixture = makeFixture(t, 'eager-commit-heartbeat')
  fixture.clear()
  const cluster = fixture.build()
  const leader = await openAndElect(cluster)

  clearInterval(leader._pingTimer)
  const announced = []
  const pingFollowers = leader._pingFollowers.bind(leader)
  leader._pingFollowers = () => {
    announced.push(leader._commitSeq)
    return pingFollowers()
  }

  const synced = nextEvent(leader, 'sync')
  const tuple = await withTimeout(leader.append(toBuf({
    op: 'set', key: 'eager', value: 9,
  })), 'append without periodic leader pings', 2_000)
  const [cmdSeq, result] = tuple

  t.equal(tuple.length, 2, 'the eager public result has two elements')
  t.equal(announced.filter((seq) => seq === cmdSeq).length, 1,
    'Raft immediately heartbeats the committed CMD sequence exactly once')
  t.deepEqual(result, { key: 'eager', value: 9 },
    'the proactive follower outcome completes agreement')
  await synced
})

test('MonsterFt appendBatch uses one CMD seq and per-item indexes', async (t) => {
  const fixture = makeFixture(t, 'batch')
  fixture.clear()
  const cluster = fixture.build()
  const leader = await openAndElect(cluster)
  const before = leader.seq
  const items = [
    { op: 'set', key: 'a', value: 10 },
    { op: 'get', key: 'a' },
    { op: 'set', key: 'b', value: 20 },
  ]

  const synced = nextEvent(leader, 'sync')
  const tuple = await leader.appendBatch(items.map(toBuf))
  const [cmdSeq, outcomes] = tuple
  const { syncSeq } = await synced

  t.equal(tuple.length, 2, 'the public batch result has two elements')
  t.equal(cmdSeq, before + 1n, 'the complete inner batch occupies the next Raft seq')
  t.equal(syncSeq, cmdSeq + 1n, 'the batch is followed by one SYNC seq')
  t.equal(leader.seq, syncSeq, 'no Raft seq is allocated per inner item')
  t.deepEqual(outcomes, [
    { status: 'fulfilled', value: { key: 'a', value: 10 } },
    { status: 'fulfilled', value: { key: 'a', value: 10 } },
    { status: 'fulfilled', value: { key: 'b', value: 20 } },
  ], 'returns ordered all-settled outcomes')

  await waitApplied(cluster.nodes, syncSeq)
  for (const node of cluster.nodes) {
    const calls = cluster.calls.filter((call) => call.id === node.id && call.seq === cmdSeq)
    t.equal(calls.length, items.length, `node ${node.id} applies every inner item once`)
    t.deepEqual(calls.map((call) => call.index), [0, 1, 2],
      `node ${node.id} receives distinct indexes`)
    t.ok(calls.every((call) => typeof call.term === 'bigint'),
      `node ${node.id} receives the Raft term`)
  }
  t.deepEqual(cluster.nodes.map((node) => [valueAt(node, 'a'), valueAt(node, 'b')]),
    [[10, 20], [10, 20], [10, 20]], 'successful local transitions commit in order')
})

test('MonsterFt CMD carries the ordered Raft matchIndex snapshot', async (t) => {
  const fixture = makeFixture(t, 'match-index')
  fixture.clear()
  const members = ['3', '1', '2']
  const cluster = fixture.build({ members })
  const leader = await openAndElect(cluster)
  const preceding = leader.seq
  const peers = leader.nodes.filter((id) => id !== leader.id)
  const expectedById = new Map([
    [leader.id, preceding],
    [peers[0], -1n],
    [peers[1], preceding],
  ])
  const expected = leader.nodes.map((id) => expectedById.get(id))
  const appendEntry = leader._monsterAppendEntry.bind(leader)
  leader._monsterAppendEntry = (entry) => {
    if (entry.type === 'cmd') {
      leader._replication.delete(peers[0])
      leader._replication.get(peers[1]).matchIndex = preceding
    }
    return appendEntry(entry)
  }

  const synced = nextEvent(leader, 'sync')
  const [cmdSeq] = await leader.appendBatch([
    toBuf({ op: 'set', key: 'snapshot', value: 1 }),
    toBuf({ op: 'get', key: 'snapshot' }),
  ])
  const { syncSeq } = await synced

  t.deepEqual(leader.nodes, ['1', '2', '3'],
    'RaftNode exposes canonical node ID order')
  t.equal(cmdSeq, preceding + 1n,
    'the leader snapshot uses the sequence immediately before the CMD')
  t.deepEqual(raftRecordAt(leader, cmdSeq).matchIndex, expected,
    'the durable CMD stores leader and follower positions in canonical order')

  await waitApplied(cluster.nodes, syncSeq)
  const calls = cluster.calls.filter((call) => call.seq === cmdSeq)
  t.equal(calls.length, cluster.nodes.length * 2,
    'every replica applies both batch items')
  t.ok(calls.every((call) => {
    return leader.nodes.every((id, index) => {
      return call.matchIndex[index] === expectedById.get(id)
    })
  }), 'every batch callback receives the CMD matchIndex snapshot')
})

test('MonsterFt append validation uses normalized coded errors',
  async (t) => {
    const fixture = makeFixture(t, 'append-validation')
    fixture.clear()
    const cluster = fixture.build()
    const unopened = cluster.nodes[0]
    const notOpen = await rejects(
      t,
      unopened.append('item'),
      /^node not open$/,
      'append checks node availability before its argument',
    )
    t.ok(notOpen instanceof ErrorWithCode,
      'the unavailable append throws ErrorWithCode')
    t.equal(notOpen.code, NODE_NOT_OPEN,
      'the unavailable append uses NODE_NOT_OPEN')
    t.equal(notOpen.sqlCode, null,
      'the unavailable append has no SQLite code')

    const leader = await openAndElect(cluster)
    const invalid = [
      [() => leader.append('item'), 'data must be buffer'],
      [() => leader.appendBatch(null), 'data must be array with length > 0'],
      [() => leader.appendBatch([]), 'data must be array with length > 0'],
      [() => leader.appendBatch([Buffer.from('item'), 'item']),
        'data must be array of buffers'],
    ]
    for (const [fn, message] of invalid) {
      const err = await rejects(
        t, fn(), new RegExp(`^${message}$`), message,
      )
      t.ok(err instanceof ErrorWithCode, `${message} throws ErrorWithCode`)
      t.equal(err.code, ARGUMENT_ILLEGAL,
        `${message} uses ARGUMENT_ILLEGAL`)
      t.equal(err.sqlCode, null, `${message} has no SQLite code`)
    }
  })

test('MonsterFt wraps an empty buffer instead of treating it as a Raft no-op', async (t) => {
  const fixture = makeFixture(t, 'empty-buffer')
  fixture.clear()
  const cluster = fixture.build({
    apply: async (conn, node, buf) => ({ empty: buf.length === 0 }),
  })
  const leader = await openAndElect(cluster)

  const synced = nextEvent(leader, 'sync')
  const tuple = await leader.append(Buffer.alloc(0))
  const [cmdSeq, result] = tuple

  t.equal(tuple.length, 2, 'the empty-buffer result has two elements')
  t.deepEqual(result, { empty: true }, 'the application receives the empty user buffer')
  const row = leader.log.db.prepare(`
    SELECT entry FROM raft_log WHERE seq = ?
  `).get(cmdSeq)
  const record = unpack(Buffer.from(row.entry).subarray(8))
  t.equal(record.type, 'cmd', 'the Raft entry is a wrapped CMD, not a no-op')
  t.equal(record.items.length, 1, 'the CMD contains one user item')
  t.equal(record.items[0].length, 0, 'the wrapped item remains empty')
  t.equal(cluster.calls.filter((call) => call.seq === cmdSeq).length, 3,
    'every node invokes the application callback for the empty item')
  await synced
})

test('MonsterFt serializes concurrent local and forwarded submissions', async (t) => {
  const fixture = makeFixture(t, 'concurrent')
  fixture.clear()
  const cluster = fixture.build()
  const leader = await openAndElect(cluster)
  const follower = followers(cluster.nodes)[0]

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
  const twoSyncs = deferred()
  leader.on('sync', (event) => {
    syncEvents.push(event)
    if (syncEvents.length === 2) { twoSyncs.resolve() }
  })

  const first = leader.append(toBuf({ op: 'set', key: 'local', value: 1 }))
  await withTimeout(syncEntered.promise, 'first serialized SYNC gate')
  const firstTuple = await withTimeout(first, 'first early serialized result')
  let secondSettled = false
  const second = follower.append(toBuf({
    op: 'set', key: 'forwarded', value: 2,
  })).finally(() => { secondSettled = true })
  second.catch(noop)
  await sleep(25)

  t.equal(firstTuple.length, 2, 'the first command settles before its SYNC')
  t.notOk(secondSettled, 'the second command waits behind the first pending SYNC')
  t.equal(cluster.calls.filter(({ cmd }) => {
    return cmd?.key === 'forwarded'
  }).length, 0,
    'the queued command does not reach application before the prior SYNC')

  releaseSync.resolve()
  const secondTuple = await second
  await withTimeout(twoSyncs.promise, 'two serialized SYNC events')
  const tuples = [firstTuple, secondTuple]
  t.equal(syncEvents[0].cmdSeq, tuples[0][0],
    'the first SYNC belongs to the first CMD')
  t.equal(tuples[1][0], syncEvents[0].syncSeq + 1n,
    'the second CMD starts only after the first pipeline finishes')
  t.equal(syncEvents[1].cmdSeq, tuples[1][0],
    'the second CMD receives its own SYNC')
  await waitApplied(cluster.nodes, syncEvents[1].syncSeq)
  t.deepEqual(cluster.nodes.map((node) => [valueAt(node, 'local'), valueAt(node, 'forwarded')]),
    [[1, 2], [1, 2], [1, 2]], 'both serialized submissions commit on every node')
})

test('MonsterFt compares throws, rolls back failed items, and continues', async (t) => {
  const fixture = makeFixture(t, 'throws')
  fixture.clear()
  const cluster = fixture.build()
  const leader = await openAndElect(cluster)
  const follower = followers(cluster.nodes)[0]

  const batchSynced = nextEvent(leader, 'sync')
  const tuple = await leader.appendBatch([
    { op: 'set', key: 'good', value: 1 },
    { op: 'fail', key: 'partial', value: 99, message: 'planned failure' },
    { op: 'get', key: 'partial' },
    { op: 'get', key: 'good' },
  ].map(toBuf))
  const [, outcomes] = tuple

  t.equal(tuple.length, 2, 'the mixed batch result has two elements')
  t.equal(outcomes[0].status, 'fulfilled', 'success before a throw is retained')
  t.equal(outcomes[1].status, 'rejected', 'the agreed throw is a rejected outcome')
  t.equal(outcomes[1].reason.message, 'planned failure', 'error message is preserved')
  t.equal(outcomes[1].reason.code, null,
    'a string application code is normalized')
  t.equal(outcomes[1].reason.sqlCode, null,
    'an application error without SQLite metadata has null sqlCode')
  t.notOk(hasErrorContext(outcomes[1].reason),
    'a local batch rejection adds no MonsterFt error context')
  t.deepEqual(outcomes[2], { status: 'fulfilled', value: null },
    'the failed item write is absent from later reads in the batch')
  t.deepEqual(outcomes[3], {
    status: 'fulfilled', value: { key: 'good', value: 1 },
  }, 'a later item sees an earlier successful write')
  const { syncSeq } = await batchSynced
  await waitApplied(cluster.nodes, syncSeq)
  t.deepEqual(cluster.nodes.map((node) => valueAt(node, 'partial')), [null, null, null],
    'partial SQL from throwing items never commits')
  t.deepEqual(cluster.nodes.map((node) => valueAt(node, 'good')), [1, 1, 1],
    'successful SQL in the same batch commits')

  const singularSynced = nextEvent(leader, 'sync')
  const error = await rejects(t, leader.append(toBuf({
    op: 'fail', key: 'singular-partial', value: 100, message: 'single failure',
  })), /single failure/, 'an agreed singular throw rejects the caller')
  t.equal(error.code, null, 'singular string code is normalized')
  t.notOk(hasErrorContext(error),
    'a singular application error adds no MonsterFt error context')
  const singularSync = await singularSynced
  await waitApplied(cluster.nodes, singularSync.syncSeq)
  t.deepEqual(cluster.nodes.map((node) => valueAt(node, 'singular-partial')),
    [null, null, null], 'singular throwing SQL is rolled back everywhere')

  const forwardedSynced = nextEvent(leader, 'sync')
  const forwarded = await rejects(t, follower.append(toBuf({
    op: 'fail', key: 'forwarded-partial', value: 103,
    message: 'forwarded failure',
  })), /forwarded failure/, 'an agreed forwarded throw rejects the caller')
  t.equal(forwarded.code, null,
    'forwarded application error uses the normalized code')
  t.equal(forwarded.sqlCode, null,
    'forwarded application error retains null sqlCode')
  t.notOk(hasErrorContext(forwarded),
    'a forwarded application error adds no MonsterFt error context')
  const forwardedSync = await forwardedSynced
  const forwardedCmdSeq = forwardedSync.cmdSeq
  await waitApplied(cluster.nodes, forwardedSync.syncSeq)
  const rejectedAck = cluster.messages.find(({ from, to, msg }) => {
    return from === leader.id && to === follower.id &&
      msg.type === 'ack' && msg.cmdSeq === forwardedCmdSeq
  })
  t.ok(Array.isArray(rejectedAck?.msg.results),
    'a rejected command ACK also carries structured results')
  t.deepEqual(rejectedAck?.msg.results, [[
    1, 'forwarded failure', null, null,
  ]], 'the structured ACK retains message, code, and sqlCode')

  const [forwardedBatchSeq, forwardedOutcomes] =
    await follower.appendBatch([
      toBuf({ op: 'get', key: 'good' }),
      toBuf({
        op: 'fail',
        key: 'forwarded-batch-partial',
        value: 104,
        message: 'forwarded batch failure',
      }),
    ])
  const forwardedBatchError = forwardedOutcomes[1].reason
  t.equal(forwardedOutcomes[1].status, 'rejected',
    'a forwarded batch retains its rejected outcome')
  t.equal(forwardedBatchError.message, 'forwarded batch failure',
    'a forwarded batch retains the application error')
  t.equal(forwardedBatchError.code, null,
    'a forwarded batch retains the normalized application error code')
  t.equal(typeof forwardedBatchSeq, 'bigint',
    'the forwarded batch still reports its command sequence separately')
  t.notOk(hasErrorContext(forwardedBatchError),
    'a forwarded batch rejection adds no MonsterFt error context')

  const unsupported = await rejects(t, leader.append(toBuf({
    op: 'fail', key: 'unsupported-code', value: 101,
    message: 'unsupported code', code: true,
  })), /unsupported code/, 'an unsupported error code does not disrupt agreement')
  t.equal(unsupported.code, null,
    'an unsupported error code becomes null')
  t.equal(unsupported.sqlCode, null,
    'an unsupported error has null sqlCode')

  const numeric = await rejects(t, leader.append(toBuf({
    op: 'fail', key: 'numeric-code', value: 102,
    message: 'numeric code', code: 17, sqlCode: 19,
  })), /numeric code/, 'a safe integer error code remains supported')
  t.equal(numeric.code, 17, 'a safe integer error code is preserved')
  t.equal(numeric.sqlCode, 19, 'a safe integer sqlCode is preserved')

  const laterSynced = nextEvent(leader, 'sync')
  const [, later] = await leader.append(toBuf({ op: 'get', key: 'good' }))
  t.deepEqual(later, { key: 'good', value: 1 },
    'agreed application errors do not stop progress')
  await laterSynced
})

test('MonsterFt accepts a slow matching outcome after a clean quorum SYNC', async (t) => {
  const fixture = makeFixture(t, 'slow-match')
  fixture.clear()
  const releaseSlow = deferred()
  const slowEntered = deferred()
  t.teardown(() => releaseSlow.resolve())
  const apply = async (conn, node, buf) => {
    const result = await applyApp(conn, node, buf)
    if (node.id === '3') {
      slowEntered.resolve()
      await releaseSlow.promise
    }
    return result
  }
  const cluster = fixture.build({ apply })
  const leader = await openAndElect(cluster)

  const synced = nextEvent(leader, 'sync')
  const appending = leader.append(toBuf({ op: 'set', key: 'slow-match', value: 42 }))
  appending.catch(noop)
  await withTimeout(slowEntered.promise, 'slow matching callback')
  const [cmdSeq, result] = await appending
  const { syncSeq } = await synced

  t.deepEqual(result, { key: 'slow-match', value: 42 },
    'the agreeing quorum returns without the slow member')
  t.deepEqual(headRecord(leader), {
    type: 'sync',
    cmdSeq,
    quorum: true,
    digest: headRecord(leader).digest,
    agree: ['1', '2'],
    disagree: [],
  }, 'the clean SYNC exactly records the first agreeing quorum')
  t.ok(cluster.nodes[2]._applySeq < syncSeq,
    'the slow member has not locally applied the SYNC')
  t.ok(cluster.nodes[2].isOpen,
    'an unknown member remains online before applying SYNC')

  releaseSlow.resolve()
  await waitApplied(cluster.nodes, syncSeq)
  t.deepEqual(cluster.nodes.map((node) => valueAt(node, 'slow-match')), [42, 42, 42],
    'the late matching transition is retained everywhere')
  t.ok(cluster.nodes.every((node) => node.isOpen),
    'the late matching member follows the happy path')
  t.equal(headRecord(leader).type, 'sync',
    'a matching late report appends no additional protocol record')
  t.notOk(cluster.messages.some(({ msg }) => /quarantine/i.test(msg.type)),
    'a matching late report sends no removed protocol traffic')
})

test('MonsterFt trusts SYNC agree and disagree membership before local digest',
  async (t) => {
  const fixture = makeFixture(t, 'trusted-sync-membership')
  fixture.clear()
  const apply = async (conn, node, buf) => {
    const result = await applyApp(conn, node, buf)
    return node.id === '3' ? { ...result, variant: 'local-mismatch' } : result
  }
  const cluster = fixture.build({ apply })
  const syncEvents = new Map(cluster.nodes.map((node) => [node.id, []]))
  const removedEvents = []
  const fatals = new Map(cluster.nodes.map((node) => [node.id, []]))
  const errors = new Map(cluster.nodes.map((node) => [node.id, []]))
  cluster.nodes.forEach((node) => {
    node.on('sync', (event) => syncEvents.get(node.id).push(event))
    node.on('repairRequired', () => removedEvents.push(['repairRequired', node.id]))
    node.on('disagreement', () => removedEvents.push(['disagreement', node.id]))
    node.on('fatal', (err) => fatals.get(node.id).push(err))
    node.on('error', (err) => errors.get(node.id).push(err))
  })
  const leader = await openAndElect(cluster)
  const appendEntry = leader._monsterAppendEntry.bind(leader)
  leader._monsterAppendEntry = (entry) => {
    if (entry.type === 'sync') {
      entry = {
        ...entry,
        agree: ['1', '3'],
        disagree: ['2', '3'],
      }
    }
    return appendEntry(entry)
  }
  const synced = nextEvent(leader, 'sync')
  const [cmdSeq, result] = await leader.append(toBuf({
    op: 'set', key: 'trusted-membership', value: 12,
  }))
  const syncEvent = await synced
  const { syncSeq } = syncEvent
  await waitFor(() => !cluster.nodes[1].isOpen,
    'listed disagree member terminal close')
  await waitFor(() => errors.get('2').length === 1,
    'listed disagree member error report')

  const sync = headRecord(leader)
  t.deepEqual(sync, {
    type: 'sync',
    cmdSeq,
    quorum: true,
    digest: sync.digest,
    agree: ['1', '3'],
    disagree: ['2', '3'],
  }, 'the injected SYNC records explicit trusted membership')
  t.deepEqual(result, { key: 'trusted-membership', value: 12 },
    'the agreeing leader returns its result')
  t.deepEqual(syncEvent, {
    cmdSeq,
    syncSeq,
    quorum: true,
    agree: ['1', '3'],
    disagree: ['2', '3'],
  }, 'a healthy listed member emits the trusted arrays')
  t.ok(cluster.nodes[2].isOpen,
    'listed agree wins over both overlap and a conflicting local digest')
  t.notOk(cluster.nodes[1].isOpen,
    'listed disagree overrides that member’s matching local digest')
  t.deepEqual(syncEvents.get('2'), [{
    cmdSeq,
    syncSeq,
    quorum: true,
    agree: ['1', '3'],
    disagree: ['2', '3'],
  }], 'the terminal member emits its applied sync event')
  t.equal(syncEvents.get('3').length, 1,
    'the trusted agreeing member emits its healthy sync event')
  t.deepEqual(removedEvents, [],
    'terminal classification emits neither removed public event')
  t.equal(fatals.get('2').length, 1,
    'the terminal member emits fatal exactly once')
  t.equal(errors.get('2').length, 1,
    'the terminal member reports the same failure through error')
  t.equal(errors.get('2')[0], fatals.get('2')[0],
    'fatal and error retain one failure object')
  t.equal(fatals.get('2')[0].code, REPAIR_OUTSIDE_AGREEMENT,
    'the terminal member reports the repair-required error')
  t.notOk(hasErrorContext(fatals.get('2')[0]),
    'the terminal error adds no MonsterFt sequence context')
  t.deepEqual(monsterState(fixture.paths.get('2'), cmdSeq), {
    meta: {
      applied_seq: syncSeq,
      repair_state: 2n,
      pending_cmd_seq: null,
    },
    decision: { quorum: true, sync_seq: syncSeq },
  }, 'the disagree decision and terminal fence commit together')
  t.deepEqual(
    cluster.nodes.filter((node) => node.isOpen).map((node) => node.id),
    ['1', '3'],
    'only the trusted disagree member leaves service',
  )
  t.notOk(cluster.messages.some(({ msg }) => /quarantine/i.test(msg.type)),
    'trusted membership generates no removed protocol traffic')
})

test('MonsterFt rolls back a terminal SYNC fence when DB2 commit fails',
  async (t) => {
    const fixture = makeFixture(t, 'terminal-sync-commit-failure')
    fixture.clear()
    const cluster = fixture.build()
    const target = cluster.nodes[1]
    const failure = new Error('injected DB2 SYNC commit failure')
    const syncEvents = []
    const removedEvents = []
    const fatals = []
    const errors = []
    let before = null
    let commitFailures = 0
    let rollbacks = 0

    target.on('sync', (event) => syncEvents.push(event))
    target.on('repairRequired', () => removedEvents.push('repairRequired'))
    target.on('disagreement', () => removedEvents.push('disagreement'))
    target.on('fatal', (err) => fatals.push(err))
    target.on('error', (err) => errors.push(err))

    const applySync = target._monsterApplySync.bind(target)
    target._monsterApplySync = async (...args) => {
      const meta = target.db.prepare(`
        SELECT applied_seq, applied_entry_hash, repair_state,
               pending_cmd_seq, pending_local_digest
        FROM monsterft_meta WHERE id = 1
      `).get()
      before = {
        appliedSeq: meta.applied_seq,
        appliedHash: Buffer.from(meta.applied_entry_hash),
        repairState: meta.repair_state,
        pendingCmdSeq: meta.pending_cmd_seq,
        pendingDigest: Buffer.from(meta.pending_local_digest),
      }

      const db = target.db
      const exec = db.exec.bind(db)
      db.exec = (sql) => {
        const normalized = String(sql).trim().toUpperCase()
        if (normalized === 'COMMIT' && commitFailures === 0) {
          commitFailures++
          throw failure
        }
        if (normalized === 'ROLLBACK') { rollbacks++ }
        return exec(sql)
      }
      try {
        return await applySync(...args)
      } finally {
        delete db.exec
      }
    }

    const leader = await openAndElect(cluster)
    const appendEntry = leader._monsterAppendEntry.bind(leader)
    leader._monsterAppendEntry = (entry) => {
      if (entry.type === 'sync') {
        entry = {
          ...entry,
          agree: ['1', '3'],
          disagree: ['2'],
        }
      }
      return appendEntry(entry)
    }

    const synced = nextEvent(leader, 'sync')
    const [cmdSeq, result] = await leader.append(toBuf({
      op: 'set', key: 'terminal-sync-commit-failure', value: 34,
    }))
    const { syncSeq } = await synced
    await waitFor(() => !target.isOpen,
      'terminal SYNC commit failure close')
    await waitFor(() => errors.length === 1,
      'terminal SYNC commit failure error report')

    t.deepEqual(result, {
      key: 'terminal-sync-commit-failure',
      value: 34,
    }, 'the healthy leader still returns the agreed command result')
    t.equal(commitFailures, 1,
      'the injected failure reaches the terminal SYNC commit')
    t.equal(rollbacks, 1,
      'the failed terminal transaction rolls back exactly once')
    t.equal(before.appliedSeq, cmdSeq,
      'the target had durably applied only the CMD before SYNC')
    t.equal(before.repairState, 0n,
      'the target was unfenced before the terminal transaction')
    t.equal(before.pendingCmdSeq, cmdSeq,
      'the target command was unresolved before SYNC')
    t.equal(before.pendingDigest.byteLength, 32,
      'the unresolved command had a durable local digest')

    const db = new DatabaseSync(`${fixture.paths.get('2')}2`, {
      readOnly: true,
      readBigInts: true,
    })
    const meta = db.prepare(`
      SELECT applied_seq, applied_entry_hash, repair_state,
             pending_cmd_seq, pending_local_digest
      FROM monsterft_meta WHERE id = 1
    `).get()
    db.close()

    t.equal(meta.applied_seq, before.appliedSeq,
      'the applied sequence rolls back with the terminal decision')
    t.ok(Buffer.from(meta.applied_entry_hash).equals(before.appliedHash),
      'the applied entry hash rolls back with the terminal decision')
    t.equal(meta.repair_state, before.repairState,
      'the repair fence rolls back with the terminal decision')
    t.equal(meta.pending_cmd_seq, before.pendingCmdSeq,
      'the pending command sequence rolls back with the fence')
    t.ok(Buffer.from(meta.pending_local_digest).equals(before.pendingDigest),
      'the pending command digest rolls back with the fence')
    t.equal(target._monsterPendingCommand?.cmdSeq, cmdSeq,
      'the failed commit leaves the authoritative cache pending')
    t.ok(Buffer.from(target._monsterPendingCommand.localDigest)
      .equals(before.pendingDigest),
      'the cache remains aligned with rolled-back pending metadata')
    t.equal(target._applySeq, cmdSeq,
      'live apply state does not publish the failed SYNC checkpoint')
    t.equal(fatals.length, 1,
      'the storage failure emits one fatal')
    t.ok(fatals[0] instanceof ErrorWithCode,
      'fatal reporting normalizes the DB2 storage failure')
    t.notEqual(fatals[0], failure,
      'fatal reporting replaces the original DB2 error')
    t.equal(fatals[0].code, APPLY_ERROR,
      'fatal reporting classifies the application failure')
    t.equal(fatals[0].message, '(apply) injected DB2 SYNC commit failure',
      'fatal reporting adds application context')
    t.equal(errors[0], fatals[0],
      'error reporting preserves the normalized failure')
    t.notOk(failure.message.includes('MonsterFt'),
      'MonsterFt adds no phase or sequence text to the storage failure')
    t.deepEqual(syncEvents, [],
      'the failed terminal application emits no sync event')
    t.deepEqual(removedEvents, [],
      'the failed terminal application emits no removed public event')
  })

test('MonsterFt records quorum:false only after every digest quorum is impossible',
  async (t) => {
    const fixture = makeFixture(t, 'quorum-impossible')
    fixture.clear()
    const releaseThird = deferred()
    const thirdEntered = deferred()
    t.teardown(() => releaseThird.resolve())
    const apply = async (conn, node, buf) => {
      const result = await applyApp(conn, node, buf)
      if (node.id === '3') {
        thirdEntered.resolve()
        await releaseThird.promise
      }
      return { ...result, observer: node.id }
    }
    const cluster = fixture.build({ apply })
    const syncEvents = new Map(cluster.nodes.map((node) => [node.id, []]))
    const removedEvents = []
    cluster.nodes.forEach((node) => {
      node.on('sync', (event) => syncEvents.get(node.id).push(event))
      node.on('repairRequired', () => removedEvents.push(['repairRequired', node.id]))
      node.on('disagreement', () => removedEvents.push(['disagreement', node.id]))
    })
    const leader = await openAndElect(cluster)
    let settled = false
    const appending = leader.append(toBuf({
      op: 'set', key: 'quorum-impossible', value: 77,
    })).finally(() => { settled = true })
    appending.catch(noop)

    await withTimeout(thirdEntered.promise, 'impossible third member gate')
    await waitFor(() => cmdSeqFor(cluster, 'quorum-impossible') !== undefined,
      'quorum-impossible CMD seq')
    const cmdSeq = cmdSeqFor(cluster, 'quorum-impossible')
    await waitApplied(cluster.nodes.slice(0, 2), cmdSeq)
    await waitForOutcome(cluster, '2', '1', cmdSeq)
    await Promise.resolve()
    t.notOk(settled, 'two different reports still wait for the unknown third member')
    t.equal(headRecord(leader).type, 'cmd',
      'quorum impossibility is not claimed before the third report')

    const queued = leader.append(toBuf({
      op: 'set', key: 'queued-after-quorum-impossible', value: 78,
    }))
    queued.catch(noop)

    releaseThird.resolve()
    const error = await rejectsCode(
      t,
      appending,
      REPAIR_QUORUM_IMPOSSIBLE,
      'the deciding CMD rejects with the repair-required code',
    )
    t.notOk(hasErrorContext(error),
      'the repair rejection adds no MonsterFt command context')
    const syncSeq = cmdSeq + 1n
    await waitApplied(cluster.nodes, syncSeq)
    await waitFor(() => {
      return [...syncEvents.values()].every((events) => events.length === 1)
    }, 'quorum-impossible SYNC events')
    const syncLog = new SQLiteLog(fixture.paths.get('1'))
    syncLog.open()
    const sync = unpack(syncLog.head)
    syncLog.close()
    t.deepEqual(sync, {
      type: 'sync',
      cmdSeq,
      quorum: false,
      agree: [],
      disagree: ['1', '2', '3'],
    }, 'quorum:false SYNC exactly records every determining reporter')
    for (const node of cluster.nodes) {
      t.deepEqual(monsterState(fixture.paths.get(node.id), cmdSeq), {
        meta: {
          applied_seq: syncSeq,
          repair_state: 1n,
          pending_cmd_seq: null,
        },
        decision: { quorum: false, sync_seq: syncSeq },
      }, `node ${node.id} stores the live fence with the SYNC checkpoint`)
      t.deepEqual(syncEvents.get(node.id), [{
        cmdSeq,
        syncSeq,
        quorum: false,
        agree: [],
        disagree: ['1', '2', '3'],
      }], `node ${node.id} emits the applied quorum:false SYNC`)
      t.equal(node._monsterRepairState, 1,
        `node ${node.id} publishes live repair state one in memory`)
    }
    t.ok(cluster.nodes.every((node) => node.isOpen),
      'quorum-impossible SYNC leaves every connected member online')
    t.equal(leaders(cluster.nodes)[0], leader,
      'quorum-impossible SYNC does not step down the leader')
    t.deepEqual(removedEvents, [],
      'quorum:false application emits no removed public event')

    await rejectsCode(t, queued, REPAIR_QUORUM_IMPOSSIBLE,
      'a command queued before the SYNC is fenced before append')
    await rejectsCode(t, leader.append(toBuf({
      op: 'set', key: 'local-after-quorum-impossible', value: 79,
    })), REPAIR_QUORUM_IMPOSSIBLE,
    'the live-fenced leader rejects a local command')
    await rejectsCode(t, leader.appendBatch([
      toBuf({ op: 'set', key: 'batch-after-quorum-impossible-a', value: 80 }),
      toBuf({ op: 'set', key: 'batch-after-quorum-impossible-b', value: 81 }),
    ]), REPAIR_QUORUM_IMPOSSIBLE,
    'the live-fenced leader rejects a command batch')
    const forwardedFence = await rejectsCode(t,
      cluster.nodes[1]._monsterFwdCmdToLeader([
      toBuf({ op: 'set', key: 'forwarded-after-quorum-impossible', value: 82 }),
      ]), REPAIR_QUORUM_IMPOSSIBLE,
      'the live-fenced leader rejects a forwarded command request')
    t.ok(forwardedFence instanceof ErrorWithCode,
      'the forwarded RPC failure is reconstructed as ErrorWithCode')
    t.equal(forwardedFence.message, 'repair required',
      'the forwarded RPC retains the remote message without a prefix')
    t.equal(forwardedFence.sqlCode, null,
      'the forwarded RPC retains null sqlCode')
    const forwardedErr = cluster.messages.find(({ from, to, msg }) => {
      return from === leader.id && to === cluster.nodes[1].id &&
        msg.type === 'err' && msg.msg === 'repair required'
    })?.msg
    t.deepEqual(forwardedErr, {
      type: 'err',
      term: leader.term,
      cid: forwardedErr?.cid,
      msg: 'repair required',
      code: REPAIR_QUORUM_IMPOSSIBLE,
      sqlCode: null,
      from: leader.id,
    }, 'the Monster RPC uses only the common coded error envelope')
    t.notOk(Object.hasOwn(forwardedErr ?? {}, 'error'),
      'the Monster RPC omits the legacy nested error object')
    t.notOk(Object.hasOwn(forwardedErr ?? {}, 'name'),
      'the Monster RPC omits error names')
    t.equal(headRecord(leader).type, 'sync',
      'fenced command attempts append no later CMD record')
    t.equal(cmdSeqFor(cluster, 'queued-after-quorum-impossible'), undefined,
      'the previously queued callback is never invoked')
    t.notOk(cluster.messages.some(({ msg }) => /quarantine/i.test(msg.type)),
      'quorum impossibility generates no removed protocol traffic')
  })

test('MonsterFt fences a leader outside the matching quorum',
  async (t) => {
    const fixture = makeFixture(t, 'leader-outside-agreement')
    fixture.clear()
    const releaseThird = deferred()
    const thirdEntered = deferred()
    t.teardown(() => releaseThird.resolve())
    const apply = async (conn, node, buf) => {
      const result = await applyApp(conn, node, buf)
      if (node.id === '3') {
        thirdEntered.resolve()
        await releaseThird.promise
      }
      return node.id === '1' ? result : { ...result, variant: 'majority' }
    }
    const cluster = fixture.build({ apply })
    const leader = await openAndElect(cluster)
    const fatals = []
    const errors = []
    const syncEvents = []
    let stateAtFatal = null
    leader.on('fatal', (err) => {
      fatals.push(err)
      stateAtFatal = monsterState(fixture.paths.get('1'), cmdSeq)
    })
    leader.on('error', (err) => errors.push(err))
    leader.on('sync', (event) => syncEvents.push(event))
    const appendEntry = leader._monsterAppendEntry.bind(leader)
    let syncAttempts = 0
    leader._monsterAppendEntry = (entry) => {
      if (entry.type === 'sync') { syncAttempts++ }
      return appendEntry(entry)
    }
    const appending = leader.append(toBuf({
      op: 'set', key: 'leader-outside', value: 91,
    }))
    appending.catch(noop)

    await withTimeout(thirdEntered.promise, 'leader-outside third member gate')
    await waitFor(() => cmdSeqFor(cluster, 'leader-outside') !== undefined,
      'leader-outside CMD seq')
    const cmdSeq = cmdSeqFor(cluster, 'leader-outside')
    await waitApplied(cluster.nodes.slice(0, 2), cmdSeq)
    await waitForOutcome(cluster, '2', '1', cmdSeq)
    t.equal(headRecord(leader).type, 'cmd',
      'the split leader and follower wait for the final report')

    releaseThird.resolve()
    const error = await rejectsCode(
      t,
      appending,
      REPAIR_OUTSIDE_AGREEMENT,
      'the leader outside the matching quorum rejects with repair required',
    )
    t.notOk(hasErrorContext(error),
      'the leader-excluding rejection adds no MonsterFt command context')
    await waitFor(() => !leader.isOpen, 'unhealthy leader terminal close')
    await waitFor(() => errors.length === 1, 'unhealthy leader error report')
    t.equal(syncAttempts, 0,
      'an unhealthy leader does not append its proposed SYNC')
    t.deepEqual(syncEvents, [],
      'an unhealthy leader emits no terminal sync event')
    t.equal(fatals.length, 1, 'the unhealthy leader emits fatal once')
    t.equal(errors[0], fatals[0],
      'the unhealthy leader reports its fatal through error')
    t.equal(fatals[0].code, REPAIR_OUTSIDE_AGREEMENT,
      'the unhealthy leader reports the repair-required error')
    t.notOk(hasErrorContext(fatals[0]),
      'the unhealthy leader error adds no MonsterFt sequence context')
    t.deepEqual(stateAtFatal, {
      meta: {
        applied_seq: cmdSeq,
        repair_state: 2n,
        pending_cmd_seq: cmdSeq,
      },
      decision: null,
    }, 'the leader’s standalone fence is durable by fatal emission')
    t.deepEqual(monsterState(fixture.paths.get('1'), cmdSeq), stateAtFatal,
      'the standalone fence remains durable after close')

    const successor = cluster.nodes[1]
    const successorSynced = nextEvent(successor, 'sync')
    successor._voteForSelf()
    await withTimeout(ready(cluster.nodes.slice(1), null, true),
      'agreeing successor election')
    const { syncSeq } = await successorSynced
    await waitApplied(cluster.nodes.slice(1), syncSeq)
    const sync = headRecord(successor)
    t.deepEqual(sync, {
      type: 'sync',
      cmdSeq,
      quorum: true,
      digest: sync.digest,
      agree: ['2', '3'],
      disagree: [],
    }, 'an agreeing successor resolves the pending CMD during recovery')
    t.ok(cluster.nodes[1].isOpen && cluster.nodes[2].isOpen,
      'the agreeing successor and follower remain healthy')
    t.notOk(cluster.messages.some(({ msg }) => /quarantine/i.test(msg.type)),
      'leader handoff generates no removed protocol traffic')
  })

test('MonsterFt fences a late unlisted digest mismatch without extra traffic',
  async (t) => {
    const fixture = makeFixture(t, 'late-unlisted-mismatch')
    fixture.clear()
    const releaseSlow = deferred()
    const slowEntered = deferred()
    t.teardown(() => releaseSlow.resolve())
    const apply = async (conn, node, buf) => {
      const result = await applyApp(conn, node, buf)
      if (node.id !== '3') { return result }
      slowEntered.resolve()
      await releaseSlow.promise
      return { ...result, variant: 'late-mismatch' }
    }
    const cluster = fixture.build({ apply })
    const lateMember = cluster.nodes[2]
    const lateSyncEvents = []
    const removedEvents = []
    const fatals = []
    const errors = []
    lateMember.on('sync', (event) => lateSyncEvents.push(event))
    lateMember.on('repairRequired', () => removedEvents.push('repairRequired'))
    lateMember.on('disagreement', () => removedEvents.push('disagreement'))
    lateMember.on('fatal', (err) => fatals.push(err))
    lateMember.on('error', (err) => errors.push(err))
    const leader = await openAndElect(cluster)

    const synced = nextEvent(leader, 'sync')
    const appending = leader.append(toBuf({
      op: 'set', key: 'late-unlisted-mismatch', value: 59,
    }))
    await withTimeout(slowEntered.promise, 'late unlisted mismatch callback')
    const [cmdSeq, result] = await appending
    const { syncSeq } = await synced
    t.deepEqual(result, { key: 'late-unlisted-mismatch', value: 59 },
      'the known matching quorum returns normally')
    t.deepEqual(headRecord(leader), {
      type: 'sync',
      cmdSeq,
      quorum: true,
      digest: headRecord(leader).digest,
      agree: ['1', '2'],
      disagree: [],
    }, 'the slow member is unlisted in the committed SYNC')

    releaseSlow.resolve()
    await waitFor(() => !lateMember.isOpen,
      'late unlisted mismatch terminal close')
    await waitFor(() => errors.length === 1,
      'late unlisted mismatch error report')
    t.deepEqual(lateSyncEvents, [{
      cmdSeq,
      syncSeq,
      quorum: true,
      agree: ['1', '2'],
      disagree: [],
    }], 'the terminal late member emits its applied sync event')
    t.deepEqual(removedEvents, [],
      'the terminal late member emits no removed public event')
    t.equal(fatals.length, 1,
      'the late mismatch emits one fatal')
    t.equal(errors[0], fatals[0],
      'the late mismatch reports the fatal through error')
    t.equal(fatals[0].code, REPAIR_OUTSIDE_AGREEMENT,
      'the late mismatch reports the repair-required error')
    t.notOk(hasErrorContext(fatals[0]),
      'the late mismatch error adds no MonsterFt sequence context')
    t.deepEqual(monsterState(fixture.paths.get('3'), cmdSeq), {
      meta: {
        applied_seq: syncSeq,
        repair_state: 2n,
        pending_cmd_seq: null,
      },
      decision: { quorum: true, sync_seq: syncSeq },
    }, 'digest fallback stores the decision, checkpoint, and fence atomically')
    t.ok(cluster.nodes[0].isOpen && cluster.nodes[1].isOpen,
      'the listed agreeing quorum remains online')
    t.equal(headRecord(leader).type, 'sync',
      'the terminal mismatch appends no follow-up record')
    t.notOk(cluster.messages.some(({ msg }) => /quarantine/i.test(msg.type)),
      'the terminal mismatch sends no removed protocol traffic')
  })

test('MonsterFt fences an unhealthy recovery leader and lets an agreeing successor sync',
  async (t) => {
    const fixture = makeFixture(t, 'recovery-leader-outside')
    fixture.clear()
    const dropReportsToFirstLeader = (to, from, msg) => {
      if (to === '1' && msg.type === 'monster_outcome') { return false }
    }
    const apply = async (conn, node, buf) => {
      const result = await applyApp(conn, node, buf)
      return node.id === '2' ? { ...result, variant: 'minority' } : result
    }
    const first = fixture.build({
      apply,
      intercept: dropReportsToFirstLeader,
    })
    const firstLeader = await openAndElect(first)
    const pending = firstLeader.append(toBuf({
      op: 'set', key: 'recovery-leader-outside', value: 63,
    }))
    pending.catch(noop)
    await waitFor(
      () => cmdSeqFor(first, 'recovery-leader-outside') !== undefined,
      'pending recovery CMD sequence',
    )
    const cmdSeq = cmdSeqFor(first, 'recovery-leader-outside')
    await waitApplied(first.nodes, cmdSeq)
    t.equal(headRecord(firstLeader).type, 'cmd',
      'the first leader leaves the CMD unresolved')
    closeNodes(first.nodes)
    await rejects(t, pending, /node not open/,
      'closing the first term cancels its unresolved client operation')

    const second = fixture.build({ apply })
    openNodes(second.nodes)
    const unhealthy = second.nodes[1]
    const fatals = []
    const errors = []
    unhealthy.on('fatal', (err) => fatals.push(err))
    unhealthy.on('error', (err) => errors.push(err))
    const appendEntry = unhealthy._monsterAppendEntry.bind(unhealthy)
    let syncAttempts = 0
    unhealthy._monsterAppendEntry = (entry) => {
      if (entry.type === 'sync') { syncAttempts++ }
      return appendEntry(entry)
    }
    unhealthy._voteForSelf()
    await waitFor(() => !unhealthy.isOpen,
      'unhealthy recovery leader terminal close')
    await waitFor(() => errors.length === 1,
      'unhealthy recovery leader error report')

    t.equal(syncAttempts, 0,
      'an unhealthy recovery leader appends no SYNC')
    t.equal(fatals.length, 1,
      'the unhealthy recovery leader emits fatal once')
    t.equal(errors[0], fatals[0],
      'the recovery fatal is reported through error')
    t.equal(fatals[0].code, REPAIR_OUTSIDE_AGREEMENT,
      'the recovery leader reports the repair-required error')
    t.notOk(hasErrorContext(fatals[0]),
      'the recovery error adds no MonsterFt sequence context')
    const unhealthyState = monsterState(fixture.paths.get('2'), cmdSeq)
    t.equal(unhealthyState.meta.repair_state, 2n,
      'the recovery leader durably fences itself')
    t.ok(unhealthyState.meta.applied_seq >= cmdSeq,
      'the recovery fence preserves its applied checkpoint')
    t.equal(unhealthyState.meta.pending_cmd_seq, cmdSeq,
      'the recovery leader leaves the CMD unresolved for a successor')
    t.equal(unhealthyState.decision, null,
      'the recovery leader persists no SYNC decision')

    const successor = second.nodes[2]
    const synced = nextEvent(successor, 'sync')
    successor._voteForSelf()
    await withTimeout(ready([second.nodes[0], successor], null, true),
      'agreeing recovery successor election')
    const { syncSeq } = await synced
    await waitApplied([second.nodes[0], successor], syncSeq)
    const sync = headRecord(successor)
    t.deepEqual(sync, {
      type: 'sync',
      cmdSeq,
      quorum: true,
      digest: sync.digest,
      agree: ['1', '3'],
      disagree: [],
    }, 'the agreeing successor resolves the inherited CMD')
    t.ok(second.nodes[0].isOpen && successor.isOpen,
      'the agreeing successor quorum remains online')
    t.notOk(second.messages.some(({ msg }) => /quarantine/i.test(msg.type)),
      'recovery handoff sends no removed protocol traffic')
  })

test('MonsterFt restores its durable apply checkpoint in fresh node objects', async (t) => {
  const fixture = makeFixture(t, 'restart')
  fixture.clear()
  const first = fixture.build()
  const leader = await openAndElect(first)
  const synced = nextEvent(leader, 'sync')
  const [cmdSeq, result] = await leader.append(toBuf({
    op: 'set', key: 'once', value: 55,
  }))
  const { syncSeq } = await synced
  t.deepEqual(result, { key: 'once', value: 55 }, 'initial command commits')
  await waitApplied(first.nodes, syncSeq)
  t.equal(first.calls.filter((call) => call.seq === cmdSeq).length, 3,
    'initial CMD callback ran once on each node')
  closeNodes(first.nodes)

  const second = fixture.build()
  openNodes(second.nodes)
  second.nodes.forEach((node) => {
    t.equal(node._applySeq, syncSeq, `node ${node.id} restores durable applied_seq`)
    t.equal(valueAt(node, 'once'), 55, `node ${node.id} opens with committed SQL state`)
  })
  t.equal(second.calls.length, 0, 'open does not rerun callbacks for durable records')

  second.nodes[0]._voteForSelf()
  await ready(second.nodes, null, true)
  const newLeader = leaders(second.nodes)[0]
  const noOpSeq = newLeader._commitSeq
  await waitApplied(second.nodes, noOpSeq)
  t.ok(noOpSeq > syncSeq,
    'the restarted leader commits a later election no-op')
  t.ok(second.nodes.every((node) => node._applySeq === noOpSeq),
    'the later no-op advances every restored checkpoint')
  t.equal(second.calls.length, 0,
    'the later no-op invokes no user application callback')
  const nextSynced = nextEvent(newLeader, 'sync')
  const [, current] = await newLeader.append(toBuf({ op: 'get', key: 'once' }))
  const { syncSeq: nextSync } = await nextSynced
  t.deepEqual(current, { key: 'once', value: 55 },
    'fresh objects continue from restored state')
  await waitApplied(second.nodes, nextSync)
  t.ok(second.calls.filter(({ buf }) => buf !== null).every((call) => {
    return call.cmd.op === 'get'
  }),
    'old CMD is never reapplied during later progress')
})
