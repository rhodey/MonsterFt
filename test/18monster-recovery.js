import fs from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import test from 'tape'
import { pack, unpack } from 'msgpackr'
import {
  ARGUMENT_ILLEGAL,
  DRAINING,
  ErrorWithCode,
  MONSTER_CORRUPT,
  NOT_LEADER,
  REPAIR_OUTSIDE_AGREEMENT,
  REPAIR_QUORUM_IMPOSSIBLE,
  SQLITE_ERROR,
  TERM_DIFF,
} from '../src/error.js'
import { MonsterFt, SQLiteLog } from '../src/index.js'
import { databasePath, sleep } from './util.js'

const defaultIds = ['1', '2', '3']
const noop = () => {}
let fixtureId = 0

const openNodes = (nodes) => nodes.forEach((node) => node.open())
const closeNodesQuietly = (nodes) => {
  for (const node of nodes) {
    try { node.close() } catch {}
  }
}

const monsterPath = (database) => `${database}2`
const toBuf = (value) => Buffer.from(JSON.stringify(value), 'utf8')
const toObj = (buf) => JSON.parse(Buffer.from(buf).toString('utf8'))

const raftEntry = (term, record) => ({
  term,
  entry: record === null ? Buffer.alloc(0) : Buffer.from(pack(record)),
})

const withTimeout = (promise, name, ms=10_000) => {
  let timer = null
  const deadline = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${name} timeout`)), ms)
  })
  return Promise.race([promise, deadline]).finally(() => clearTimeout(timer))
}

const waitFor = async (fn, name, ms=10_000) => {
  const end = Date.now() + ms
  while (!await fn()) {
    if (Date.now() >= end) { throw new Error(`${name} timeout`) }
    await sleep(5)
  }
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

const initializeApp = (conn) => {
  conn.exec(`
    CREATE TABLE IF NOT EXISTS monster_recovery_items (
      key TEXT PRIMARY KEY,
      value INTEGER NOT NULL
    ) STRICT
  `)
}

const apply = (conn, node, buf) => {
  const cmd = toObj(buf)
  const value = cmd.values?.[node.id] ?? cmd.value
  conn.prepare(`
    INSERT INTO monster_recovery_items (key, value) VALUES (?, ?)
    ON CONFLICT (key) DO UPDATE SET value = excluded.value
  `).run(cmd.key, value)
  return { key: cmd.key, value }
}

const valueAt = (node, key) => {
  const row = node.db.prepare(`
    SELECT value FROM monster_recovery_items WHERE key = ?
  `).get(key)
  return row === undefined ? null : Number(row.value)
}

const valueAtPath = (database, key) => {
  const db = new DatabaseSync(monsterPath(database), {
    readOnly: true,
    readBigInts: true,
  })
  try {
    const row = db.prepare(`
      SELECT value FROM monster_recovery_items WHERE key = ?
    `).get(key)
    return row === undefined ? null : Number(row.value)
  } finally {
    db.close()
  }
}

const pendingAt = (node) => {
  const row = node.db.prepare(`
    SELECT pending_cmd_seq, pending_local_digest
    FROM monsterft_meta
    WHERE id = 1
  `).get()
  if (row.pending_local_digest !== null) {
    row.pending_local_digest = Buffer.from(row.pending_local_digest)
  }
  return {
    pending_cmd_seq: row.pending_cmd_seq,
    pending_local_digest: row.pending_local_digest,
  }
}

const syncSeqAt = (node, cmdSeq) => {
  const rows = node.log.db.prepare(`
    SELECT seq, entry
    FROM raft_log
    WHERE seq > ?
    ORDER BY seq
  `).all(cmdSeq)
  const match = rows.find(({ entry }) => {
    const payload = Buffer.from(entry)
    if (payload.length === 0) { return false }
    const record = unpack(payload)
    return record.type === 'sync' && record.cmdSeq === cmdSeq
  })
  return match?.seq ?? null
}

const raftRecordAt = (node, seq) => {
  const row = node.log.db.prepare(`
    SELECT entry FROM raft_log WHERE seq = ?
  `).get(seq)
  if (row === undefined) { return null }
  return unpack(Buffer.from(row.entry))
}

const raftRecordAtPath = (database, seq) => {
  const db = new DatabaseSync(database, {
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

const raftBytesAtPath = (database) => {
  const db = new DatabaseSync(database, {
    readOnly: true,
    readBigInts: true,
  })
  try {
    return db.prepare(`
      SELECT seq, term, entry FROM raft_log ORDER BY seq
    `).all().map(({ seq, term, entry }) => ({
      seq,
      term,
      entry: Buffer.from(entry),
    }))
  } finally {
    db.close()
  }
}

const appendRaftAtPath = (database, entries) => {
  const db = new DatabaseSync(database, { readBigInts: true })
  try {
    const { tail } = db.prepare(`
      SELECT COALESCE(MAX(seq), -1) AS tail FROM raft_log
    `).get()
    const insert = db.prepare(`
      INSERT INTO raft_log (seq, term, entry) VALUES (?, ?, ?)
    `)
    return entries.map(({ term, entry }, index) => {
      const seq = tail + 1n + BigInt(index)
      insert.run(seq, term, entry)
      return seq
    })
  } finally {
    db.close()
  }
}

const readMetaAtPath = (database) => {
  const db = new DatabaseSync(monsterPath(database), {
    readOnly: true,
    readBigInts: true,
  })
  try {
    const row = db.prepare(`
      SELECT applied_seq, applied_entry_hash, repair_state,
             pending_cmd_seq, pending_local_digest
      FROM monsterft_meta
      WHERE id = 1
    `).get()
    if (row.applied_entry_hash !== null) {
      row.applied_entry_hash = Buffer.from(row.applied_entry_hash)
    }
    if (row.pending_local_digest !== null) {
      row.pending_local_digest = Buffer.from(row.pending_local_digest)
    }
    return row
  } finally {
    db.close()
  }
}

const setRepairState = (database, state) => {
  const db = new DatabaseSync(monsterPath(database), { readBigInts: true })
  try {
    db.prepare(`
      UPDATE monsterft_meta
      SET repair_state = ?
      WHERE id = 1
    `).run(state)
  } finally {
    db.close()
  }
}

const recoveryFixture = (
  t,
  name,
  { ids=defaultIds, quorum=Math.floor(ids.length / 2) + 1 }={},
) => {
  const unique = `${++fixtureId}-${process.pid}-${name}`
  const paths = new Map(ids.map((id) => [
    id,
    databasePath(`monster-recovery-${unique}-${id}`),
  ]))
  const current = new Map()
  const allNodes = []
  const errors = []
  const messages = []
  let blockOutcomes = true

  const bus = {
    register(node) {
      current.set(node.id, node)
    },
    send(to, from, msg) {
      messages.push({ to, from, msg })
      if (blockOutcomes && msg?.type === 'monster_outcome') { return }
      const node = current.get(to)
      if (!node || !node.isOpen) { return }
      return node.onReceive(from, msg)
    },
  }

  const build = (generation, buildIds=ids) => {
    const applyCalls = []
    const nodes = buildIds.map((id) => {
      const send = (to, msg) => bus.send(to, id, msg)
      let node = null
      node = new MonsterFt(
        id,
        ids,
        send,
        paths.get(id),
        {
          electionTimeout: 60_000,
          pingTimeout: 500,
          appendTimeout: 3_000,
          quorum,
          apply: (conn, buf, term, seq, index, matchIndex) => {
            const currentNode = node
            applyCalls.push({
              generation,
              id: currentNode.id,
              buf,
              cmd: buf === null ? null : toObj(buf),
              term,
              seq,
              index,
              matchIndex,
            })
            if (seq === 0n) {
              initializeApp(conn)
              return
            }
            if (buf === null) { return }
            return apply(conn, currentNode, buf)
          },
        },
      )
      node.on('warn', noop)
      node.on('error', (err) => errors.push({ generation, id, err }))
      bus.register(node)
      allNodes.push(node)
      return node
    })
    return { nodes, applyCalls }
  }

  const clear = () => {
    for (const database of paths.values()) {
      new SQLiteLog(database).del()
      for (const file of [
        `${database}-wal`,
        `${database}-shm`,
        monsterPath(database),
        `${monsterPath(database)}-journal`,
        `${monsterPath(database)}-wal`,
        `${monsterPath(database)}-shm`,
      ]) {
        fs.rmSync(file, { force: true })
      }
    }
  }

  t.teardown(() => {
    closeNodesQuietly(allNodes)
    clear()
  })

  return {
    ids,
    paths,
    errors,
    messages,
    build,
    clear,
    close: closeNodesQuietly,
    allowOutcomes() {
      blockOutcomes = false
    },
  }
}

const electOpened = async (nodes, candidate=nodes[0]) => {
  candidate._voteForSelf()
  await waitFor(() => candidate.state === 'leader',
    `node ${candidate.id} election`)
  await withTimeout(
    Promise.all(nodes.map((node) => node.awaitLeader(true))),
    `node ${candidate.id} leader commit`,
  )
  return candidate
}

const elect = async (nodes, candidate=nodes[0]) => {
  openNodes(nodes)
  return electOpened(nodes, candidate)
}

const waitForUnresolvedCmd = async (nodes) => {
  let cmdSeq = null
  await waitFor(() => {
    const pending = nodes.map((node) => pendingAt(node))
    if (!pending.every((command) => {
      return command.pending_cmd_seq !== null &&
        command.pending_local_digest?.length === 32
    })) {
      return false
    }
    const seqs = pending.map((command) => command.pending_cmd_seq)
    if (!seqs.every((seq) => seq === seqs[0])) { return false }
    cmdSeq = seqs[0]
    return nodes.every((node) => {
      return node._applySeq >= cmdSeq &&
        node._monsterPendingCommand?.cmdSeq === cmdSeq
    })
  }, 'unresolved CMD application')
  return cmdSeq
}

const waitForSync = async (nodes, cmdSeq) => {
  let syncSeq = null
  await waitFor(() => {
    if (!nodes.every((node) => node.isOpen)) { return false }
    const seqs = nodes.map((node) => syncSeqAt(node, cmdSeq))
    if (seqs.some((seq) => seq === null)) { return false }
    if (!seqs.every((seq) => seq === seqs[0])) { return false }
    syncSeq = seqs[0]
    return nodes.every((node) => {
      const pending = pendingAt(node)
      return node._applySeq >= syncSeq &&
        pending.pending_cmd_seq === null &&
        pending.pending_local_digest === null &&
        node._monsterPendingCommand === null
    })
  }, `SYNC for CMD ${cmdSeq}`)
  return syncSeq
}

const divergentCommand = (
  key,
  agreedValue,
  disagreeValue,
  member='3',
) => toBuf({
  key,
  value: agreedValue,
  values: { [member]: disagreeValue },
})

const replaceDatabase = (source, target) => {
  const copies = [[source, target], [monsterPath(source), monsterPath(target)]]
  for (const [sourceFile, targetFile] of copies) {
    for (const suffix of ['-journal', '-wal', '-shm']) {
      fs.rmSync(`${targetFile}${suffix}`, { force: true })
    }
    fs.copyFileSync(sourceFile, targetFile)
  }
}

test('MonsterFt retains pending reports across higher-term leader recovery',
  async (t) => {
    const fixture = recoveryFixture(t, 'reports-across-term')
    fixture.clear()
    const cluster = fixture.build('first')
    const leader = await elect(cluster.nodes)
    const active = leader.append(toBuf({
      key: 'reports-across-term',
      value: 70,
    }))
    active.catch(noop)
    const cmdSeq = await waitForUnresolvedCmd(cluster.nodes)
    const commandDigest = leader._monsterPendingCommand.localDigest

    await leader._monsterRxOutcome('2', {
      type: 'monster_outcome',
      cmdSeq,
      digest: commandDigest,
    })
    t.deepEqual(
      [...leader._monsterPendingReports.keys()].sort(),
      [leader.id, '2'].sort(),
      'the unresolved CMD has a quorum of member reports',
    )

    const firstTerm = leader.term
    const awaitDecision = leader._monsterAwaitDecision.bind(leader)
    let recoveryReports = null
    leader._monsterAwaitDecision = (command, term) => {
      if (term > firstTerm) {
        recoveryReports = new Map(leader._monsterPendingReports)
      }
      return awaitDecision(command, term)
    }

    leader._voteForSelf()
    await waitFor(() => {
      return leader.state === 'leader' && leader.term > firstTerm
    }, 'same member higher-term reelection')
    const syncSeq = await waitForSync(cluster.nodes, cmdSeq)

    t.deepEqual(
      [...recoveryReports.keys()].sort(),
      [leader.id, '2'].sort(),
      'leader recovery sees reports retained from the unresolved CMD',
    )
    t.deepEqual(
      [...recoveryReports.values()].map((digest) => digest.toString('hex')),
      [commandDigest, commandDigest].map((digest) => digest.toString('hex')),
      'leader recovery retains both matching command digests',
    )
    t.ok(syncSeq > cmdSeq,
      'the retained quorum drives a resolving SYNC in the higher term')
    t.equal(leader._monsterPendingReports.size, 0,
      'the applied SYNC clears the recovered report state')
  })

for (const waitingState of ['follower', 'candidate']) {
  test(`MonsterFt rejects queued CMD recovery starting as ${waitingState}`,
    async (t) => {
      const fixture = recoveryFixture(t, `queued-recovery-${waitingState}`)
      fixture.clear()
      const cluster = fixture.build('first')
      const leader = await elect(cluster.nodes)
      await leader._monsterLeaderSync
      const firstTerm = leader.term
      const firstError = errorOf(leader.append(toBuf({ key: 'first', value: 1 })))
      const firstCmdSeq = await waitForUnresolvedCmd(cluster.nodes)
      const firstProtocol = leader._monsterProtocol
      const secondData = toBuf({ key: 'second', value: 2 })
      const secondError = errorOf(leader.append(secondData))

      const send = leader.send
      const voteRequests = []
      if (waitingState === 'candidate') {
        leader.send = (to, msg) => {
          if (msg.type === 'vote_request') {
            voteRequests.push([to, msg])
            return
          }
          return send(to, msg)
        }
      }

      // This reaction wins the election synchronously after the queued run
      // starts, before an await of the completed old recovery could resume.
      const reelection = firstProtocol.then(() => {
        if (waitingState === 'candidate') {
          leader.send = send
          for (const [to, msg] of voteRequests) { send(to, msg) }
        } else {
          leader._voteForSelf()
        }
      })
      if (waitingState === 'candidate') {
        leader._voteForSelf()
      } else {
        leader._toFollower(null, true)
      }
      t.equal(leader.state, waitingState,
        'the queued command starts before the new election succeeds')

      const staleError = await withTimeout(secondError, 'stale queued command')
      await withTimeout(reelection, 'queued command reelection')
      await withTimeout(firstError, 'interrupted first command')
      t.equal(staleError?.code, NOT_LEADER,
        'the queued command rejects before waiting for an old recovery')
      t.equal(leader.term, firstTerm + 1n, 'the same node wins the next term')
      t.equal(leader.state, 'leader',
        'rejecting the stale queued command leaves the new leader in place')
      const allOpen = cluster.nodes.every((node) => node.isOpen)
      t.ok(allOpen, 'every replica stays open after the stale command rejects')
      if (!allOpen) { return }

      for (const node of cluster.nodes) {
        t.equal(pendingAt(node).pending_cmd_seq, firstCmdSeq,
          `${node.id} still has only the first CMD pending`)
        t.equal(valueAt(node, 'second'), null,
          `${node.id} has not applied the stale queued command`)
      }

      fixture.allowOutcomes()
      const firstSyncSeq = await waitForSync(cluster.nodes, firstCmdSeq)
      await withTimeout(leader._monsterLeaderSync, 'new term recovery')
      const [secondCmdSeq, result] = await withTimeout(
        leader.append(secondData), 'retried second command',
      )
      await waitForSync(cluster.nodes, secondCmdSeq)
      t.ok(secondCmdSeq > firstSyncSeq,
        'the retried command follows the SYNC that completes leader recovery')
      t.deepEqual(result, { key: 'second', value: 2 },
        'the retried command returns its normal application result')

      for (const node of cluster.nodes) {
        const records = raftBytesAtPath(fixture.paths.get(node.id))
          .filter(({ entry }) => entry.length > 0)
          .map(({ seq, entry }) => {
            const record = unpack(entry)
            return [record.type, record.cmdSeq ?? seq]
          })
        t.deepEqual(records, [
          ['cmd', firstCmdSeq], ['sync', firstCmdSeq],
          ['cmd', secondCmdSeq], ['sync', secondCmdSeq],
        ], `${node.id} retains correctly ordered CMD and SYNC records`)
        t.equal(cluster.applyCalls.filter(({ id, cmd }) => {
          return id === node.id && cmd?.key === 'first'
        }).length, 1, `${node.id} does not reapply the recovered command`)
        t.equal(cluster.applyCalls.filter(({ id, cmd }) => {
          return id === node.id && cmd?.key === 'second'
        }).length, 1, `${node.id} applies the retried command exactly once`)
      }
      t.deepEqual(fixture.errors, [], 'recovery and retry emit no errors')
    })
}

test('MonsterFt rejects a leadership change while queued CMD awaits recovery',
  async (t) => {
    const fixture = recoveryFixture(t, 'queued-recovery-term-change')
    fixture.clear()
    fixture.allowOutcomes()
    const cluster = fixture.build('first')
    const leader = await elect(cluster.nodes)
    await leader._monsterLeaderSync
    const firstTerm = leader.term
    const data = toBuf({ key: 'queued', value: 3 })
    const rejected = errorOf(leader.append(data))

    // The queue starts in the first term, then yields at the completed
    // recovery promise while a synchronous election starts the next term.
    queueMicrotask(() => leader._voteForSelf())
    const err = await withTimeout(rejected, 'queued recovery term change')
    t.equal(err?.code, TERM_DIFF,
      'the command rejects if leadership changed while awaiting recovery')
    t.equal(leader.term, firstTerm + 1n, 'the node wins a new leader term')
    t.equal(leader.state, 'leader', 'the stale command does not step it down')
    t.equal(cluster.applyCalls.filter(({ cmd }) => cmd !== null).length, 0,
      'no replica applies the rejected command')

    await withTimeout(leader._monsterLeaderSync, 'replacement recovery')
    const [cmdSeq, result] = await withTimeout(
      leader.append(data), 'command after replacement recovery',
    )
    await waitForSync(cluster.nodes, cmdSeq)
    t.deepEqual(result, { key: 'queued', value: 3 },
      'the new leader accepts the retried command after recovery')
    t.deepEqual(fixture.errors, [], 'the leadership change emits no errors')
  })

test('MonsterFt drainCmd closes an idle follower without a protocol record',
  async (t) => {
    const fixture = recoveryFixture(t, 'drain-local-only')
    fixture.clear()
    const cluster = fixture.build('first')
    const leader = await elect(cluster.nodes)
    const follower = cluster.nodes.find((node) => node !== leader)

    clearInterval(leader._pingTimer)
    const database = fixture.paths.get(follower.id)
    const logBefore = raftBytesAtPath(database)
    const messageStart = fixture.messages.length
    const draining = follower.drainCmd()

    t.equal(follower.drainCmd(), draining,
      'concurrent calls share the terminal drain promise')
    await withTimeout(draining, 'idle follower local drain')

    t.deepEqual(fixture.messages.slice(messageStart), [],
      'an idle local drain sends no transport message')
    t.deepEqual(raftBytesAtPath(database), logBefore,
      'an idle local drain appends no control record')
    t.notOk(follower.isOpen,
      'the idle follower is closed before drain fulfills')
    t.equal(follower.db, null, 'the idle follower closes DB2')
    t.notOk(follower.log.isOpen, 'the idle follower closes DB1')

    const meta = readMetaAtPath(database)
    t.equal(meta.pending_cmd_seq, null,
      'the closed follower has no durable pending command')
    t.equal(meta.pending_local_digest, null,
      'the closed follower has no durable pending digest')
    t.equal(MonsterFt.certify(database), meta.applied_seq,
      'the closed follower pair passes default certification')

    const replacement = fixture.build('replacement', [follower.id]).nodes[0]
    replacement.open()
    t.ok(replacement.isOpen,
      'a new object can open the terminally drained database pair')
    replacement.close()
  })

test('MonsterFt drainCmd closes a candidate before fulfilling', async (t) => {
  const fixture = recoveryFixture(t, 'drain-candidate')
  fixture.clear()
  const candidate = fixture.build('candidate', ['1']).nodes[0]
  candidate.open()
  candidate._voteForSelf()

  t.equal(candidate.state, 'candidate',
    'the isolated member enters candidate state')
  await withTimeout(candidate.drainCmd(), 'candidate drain completion')

  t.notOk(candidate.isOpen, 'the candidate is closed before drain fulfills')
  t.equal(candidate.db, null, 'the drained candidate closes DB2')
  t.notOk(candidate.log.isOpen, 'the drained candidate closes DB1')
  const meta = readMetaAtPath(fixture.paths.get(candidate.id))
  t.equal(meta.pending_cmd_seq, null,
    'the closed candidate has no durable pending command')
  t.equal(meta.pending_local_digest, null,
    'the closed candidate has no durable pending digest')
})

test('MonsterFt drainCmd lets a follower already applying CMD reach SYNC',
  async (t) => {
    const fixture = recoveryFixture(t, 'drain-follower-active')
    fixture.clear()
    const cluster = fixture.build('first')
    const leader = await elect(cluster.nodes)
    const follower = cluster.nodes.find((node) => node !== leader)

    const userApply = follower._monsterUserApply
    let enterApply = null
    let releaseApply = null
    const applyEntered = new Promise((resolve) => { enterApply = resolve })
    const applyGate = new Promise((resolve) => { releaseApply = resolve })
    follower._monsterUserApply = async (db, buf, ...args) => {
      if (buf !== null && toObj(buf).key === 'follower-active') {
        enterApply()
        await applyGate
      }
      return userApply(db, buf, ...args)
    }

    const active = leader.append(toBuf({ key: 'follower-active', value: 74 }))
    active.catch(noop)
    await withTimeout(applyEntered, 'follower active CMD application')

    let drainSettled = false
    const draining = follower.drainCmd()
    draining.then(
      () => { drainSettled = true },
      () => { drainSettled = true },
    )
    await sleep(30)
    t.notOk(drainSettled,
      'drain waits behind the CMD transaction already in application')

    releaseApply()
    let cmdSeq = null
    await waitFor(() => {
      if (!follower.isOpen) { return false }
      const pending = pendingAt(follower)
      cmdSeq = pending.pending_cmd_seq
      return cmdSeq !== null
    }, 'follower pending CMD publication')
    t.notOk(drainSettled,
      'drain captures the grandfathered CMD and waits for its SYNC')

    fixture.allowOutcomes()
    const [activeCmdSeq, result] = await withTimeout(
      active,
      'follower active command completion',
    )
    await withTimeout(draining, 'follower active drain completion')

    t.equal(activeCmdSeq, cmdSeq,
      'the captured follower CMD is the completed command')
    t.deepEqual(result, { key: 'follower-active', value: 74 },
      'the grandfathered command retains its result')
    t.notOk(follower.isOpen,
      'the follower closes only after applying the resolving SYNC')
    t.equal(cluster.applyCalls.filter(({ id, cmd }) => {
      return id === follower.id && cmd?.key === 'follower-active'
    }).length, 1, 'the follower executes the grandfathered CMD exactly once')

    const database = fixture.paths.get(follower.id)
    const meta = readMetaAtPath(database)
    t.ok(meta.applied_seq > cmdSeq,
      'the closed follower checkpoints beyond the CMD')
    t.equal(meta.pending_cmd_seq, null,
      'the closed follower clears the durable pending command')
    t.equal(meta.pending_local_digest, null,
      'the closed follower clears the durable pending digest')
    const sync = raftRecordAtPath(database, meta.applied_seq)
    t.equal(sync.type, 'sync',
      'the follower closes with SYNC as its checkpoint')
    t.equal(sync.cmdSeq, cmdSeq,
      'the follower checkpoint resolves the captured CMD')
  })

test('MonsterFt drainCmd latches admission and waits for local SYNC application',
  async (t) => {
    const fixture = recoveryFixture(t, 'drain-active')
    fixture.clear()
    const cluster = fixture.build('first')
    const leader = await elect(cluster.nodes)

    const active = leader.append(toBuf({ key: 'active', value: 1 }))
    active.catch(noop)
    const cmdSeq = await waitForUnresolvedCmd(cluster.nodes)

    const queued = leader.append(toBuf({ key: 'queued', value: 2 }))
    queued.catch(noop)
    let drainSettled = false
    const leaderDrain = leader.drainCmd()
    leaderDrain.then(
      () => { drainSettled = true },
      () => { drainSettled = true },
    )

    t.equal(leader.drainCmd(), leaderDrain,
      'concurrent calls share the same drain promise')

    const future = leader.append(
      toBuf({ key: 'future', value: 3 }),
    )
    future.catch(noop)
    await sleep(30)
    t.notOk(drainSettled,
      'drain waits while the active CMD lacks SYNC')

    fixture.allowOutcomes()
    const [activeCmdSeq, activeResult] = await withTimeout(
      active,
      'active command completion',
    )
    const queuedError = await withTimeout(errorOf(queued),
      'queued command drain rejection')
    const futureError = await withTimeout(errorOf(future),
      'future command drain rejection')
    await withTimeout(leaderDrain, 'leader drain completion')

    t.equal(activeCmdSeq, cmdSeq, 'the active CMD is allowed to finish')
    t.deepEqual(activeResult, { key: 'active', value: 1 },
      'the active CMD retains its result')
    t.equal(queuedError?.code, DRAINING,
      'a queued CMD rejects after the drain latch')
    t.equal(futureError?.code, DRAINING,
      'a later CMD rejects after the drain latch')
    t.notOk(leader.isOpen, 'the leader closes before drain fulfills')
    t.equal(leader.db, null, 'the drained leader closes DB2')
    t.notOk(leader.log.isOpen, 'the drained leader closes DB1')

    const database = fixture.paths.get(leader.id)
    const meta = readMetaAtPath(database)
    t.ok(meta.applied_seq > activeCmdSeq,
      'the drained leader checkpoints the active CMD decision')
    t.equal(meta.pending_cmd_seq, null,
      'the drained leader clears the durable pending command')
    t.equal(meta.pending_local_digest, null,
      'the drained leader clears the durable pending digest')
    const sync = raftRecordAtPath(database, meta.applied_seq)
    t.equal(sync.type, 'sync',
      'the durable checkpoint is a SYNC entry')
    t.equal(sync.cmdSeq, activeCmdSeq,
      'the durable SYNC resolves the active command')
    t.ok(sync.quorum, 'the durable SYNC records agreement')
    t.equal(MonsterFt.certify(database), meta.applied_seq,
      'the terminally drained leader pair passes default certification')
    t.equal(cluster.applyCalls.filter(({ cmd }) => {
      return cmd?.key === 'queued' || cmd?.key === 'future'
    }).length, 0, 'queued and future CMDs never reach user application')
  })

test('MonsterFt drainCmd fences a later replicated CMD before application',
  async (t) => {
    const fixture = recoveryFixture(t, 'drain-replicated-fence')
    fixture.clear()
    const cluster = fixture.build('first')
    const leader = await elect(cluster.nodes)
    const follower = cluster.nodes.find((node) => node !== leader)
    const database = fixture.paths.get(follower.id)
    const metaBefore = readMetaAtPath(database)

    let enterBlocker = null
    let releaseBlocker = null
    const blockerEntered = new Promise((resolve) => { enterBlocker = resolve })
    const blockerGate = new Promise((resolve) => { releaseBlocker = resolve })
    const blocker = follower._monsterRunDb(async () => {
      enterBlocker()
      await blockerGate
    })
    blocker.catch(noop)
    await withTimeout(blockerEntered, 'follower DB FIFO blocker')

    const applyCmd = follower._monsterApplyCmd.bind(follower)
    let applyCmdCalls = 0
    follower._monsterApplyCmd = (...args) => {
      applyCmdCalls++
      return applyCmd(...args)
    }

    let drainSettled = false
    const draining = follower.drainCmd()
    draining.then(
      () => { drainSettled = true },
      () => { drainSettled = true },
    )

    const active = leader.append(
      toBuf({ key: 'after-drain-fence', value: 71 }),
    )
    active.catch(noop)
    let cmdSeq = null
    await waitFor(() => {
      const command = leader._monsterPendingCommand
      if (command === null || follower.seq < command.cmdSeq) { return false }
      cmdSeq = command.cmdSeq
      return true
    }, 'later CMD replicated to draining follower')

    t.notOk(drainSettled,
      'drain remains pending behind the earlier DB FIFO work')
    t.equal(applyCmdCalls, 0,
      'the later replicated CMD has not entered _monsterApplyCmd')

    releaseBlocker()
    await withTimeout(blocker, 'follower DB FIFO release')
    await withTimeout(draining, 'replicated CMD fence drain')

    t.notOk(follower.isOpen,
      'the follower closes after its pre-fence DB work completes')
    t.equal(applyCmdCalls, 0,
      'the later replicated CMD never enters _monsterApplyCmd')
    t.equal(cluster.applyCalls.filter(({ id, cmd }) => {
      return id === follower.id && cmd?.key === 'after-drain-fence'
    }).length, 0, 'the later replicated CMD never reaches user application')

    const meta = readMetaAtPath(database)
    t.equal(meta.applied_seq, metaBefore.applied_seq,
      'the later CMD does not advance the DB2 checkpoint')
    t.deepEqual(meta.applied_entry_hash, metaBefore.applied_entry_hash,
      'the later CMD does not change the DB2 checkpoint hash')
    t.equal(meta.pending_cmd_seq, null,
      'the later CMD never becomes durable pending state')
    t.equal(meta.pending_local_digest, null,
      'the later CMD never installs a durable digest')
    t.equal(raftRecordAtPath(database, cmdSeq).type, 'cmd',
      'the later CMD may remain only as a DB1 suffix')
  })

test('MonsterFt drainCmd ignores an unapplied DB1-only CMD suffix',
  async (t) => {
    const fixture = recoveryFixture(t, 'drain-db1-suffix')
    fixture.clear()
    const initial = fixture.build('initial', ['1'])
    initial.nodes[0].open()
    initial.nodes[0].close()

    const database = fixture.paths.get('1')
    const [, cmdSeq] = appendRaftAtPath(database, [
      raftEntry(0n, null),
      raftEntry(0n, {
        type: 'cmd',
        items: [toBuf({ key: 'db1-only', value: 73 })],
        matchIndex: defaultIds.map(() => -1n),
      }),
    ])
    const reopened = fixture.build('reopened', ['1'])
    const node = reopened.nodes[0]
    node.open()

    await withTimeout(node.drainCmd(), 'DB1-only suffix drain')

    t.equal(reopened.applyCalls.length, 0,
      'drain does not apply a CMD that exists only in DB1')
    t.notOk(node.isOpen, 'drain closes the node with the DB1-only suffix')
    t.equal(node.db, null, 'drain closes DB2 with the DB1-only suffix')
    t.notOk(node.log.isOpen,
      'drain closes DB1 with the unapplied suffix intact')
    const meta = readMetaAtPath(database)
    t.equal(meta.applied_seq, -1n,
      'the unapplied initial no-op remains outside the DB2 checkpoint')
    t.equal(meta.pending_cmd_seq, null,
      'the unapplied DB1 suffix creates no pending DB2 command')
    t.equal(meta.pending_local_digest, null,
      'the unapplied DB1 suffix creates no pending DB2 digest')
    t.equal(raftRecordAtPath(database, cmdSeq).type, 'cmd',
      'drain leaves the unapplied Raft suffix intact')
  })

test('MonsterFt.certify accepts a healthy donor and validates its override flag',
  (t) => {
    const fixture = recoveryFixture(t, 'certify-healthy')
    fixture.clear()
    const initial = fixture.build('initial', ['1'])
    const donor = initial.nodes[0]
    donor.open()
    donor.close()

    const donorPath = fixture.paths.get(donor.id)
    const beforeMeta = readMetaAtPath(donorPath)
    const beforeLog = raftBytesAtPath(donorPath)

    for (const value of [null, 0, 'true', {}, []]) {
      const err = errorOfCall(() => MonsterFt.certify(donorPath, value))
      t.ok(err instanceof ErrorWithCode,
        `non-boolean override ${JSON.stringify(value)} throws ErrorWithCode`)
      t.match(err.message, /allowUnresolved must be a boolean/i,
        `certify rejects non-boolean override ${JSON.stringify(value)}`)
      t.equal(err.code, ARGUMENT_ILLEGAL,
        `non-boolean override ${JSON.stringify(value)} uses ARGUMENT_ILLEGAL`)
      t.equal(err.sqlCode, null,
        `non-boolean override ${JSON.stringify(value)} has no SQLite code`)
    }

    const first = MonsterFt.certify(donorPath)
    const second = MonsterFt.certify(donorPath, false)

    t.equal(first, beforeMeta.applied_seq,
      'healthy certification returns the DB2 applied checkpoint')
    t.equal(second, first, 'healthy certification is idempotent')
    t.deepEqual(readMetaAtPath(donorPath), beforeMeta,
      'healthy certification preserves already-clear metadata')
    t.deepEqual(raftBytesAtPath(donorPath), beforeLog,
      'healthy certification preserves the Raft log')
    t.end()
  })

test('MonsterFt.certify classifies a missing metadata row as Monster corruption', (t) => {
  const fixture = recoveryFixture(t, 'certify-missing-metadata')
  fixture.clear()
  const donor = fixture.build('initial', ['1']).nodes[0]
  donor.open()
  donor.close()

  const donorPath = fixture.paths.get(donor.id)
  const beforeLog = fs.readFileSync(donorPath)
  const db = new DatabaseSync(monsterPath(donorPath))
  db.exec('DELETE FROM monsterft_meta WHERE id = 1')
  db.close()

  for (const allowUnresolved of [false, true]) {
    const err = errorOfCall(() => MonsterFt.certify(donorPath, allowUnresolved))
    t.ok(err instanceof ErrorWithCode, 'missing metadata throws a coded error')
    t.equal(err?.code, MONSTER_CORRUPT,
      `missing metadata uses MONSTER_CORRUPT with override ${allowUnresolved}`)
    t.equal(err?.message, 'MonsterFt certify metadata row is missing',
      'the error identifies the missing DB2 metadata row')
  }
  t.deepEqual(fs.readFileSync(donorPath), beforeLog,
    'failed certification leaves DB1 unchanged')
  const replacement = fixture.build('reopened', ['1']).nodes[0]
  const openError = errorOfCall(() => replacement.open())
  t.equal(openError?.code, MONSTER_CORRUPT,
    'certification and startup agree on the corruption classification')
  t.end()
})

test('MonsterFt.certify rolls back a failed repair-state update', (t) => {
  const fixture = recoveryFixture(t, 'certify-rollback')
  fixture.clear()
  const initial = fixture.build('initial', ['1'])
  const donor = initial.nodes[0]
  donor.open()
  donor.close()

  const donorPath = fixture.paths.get(donor.id)
  setRepairState(donorPath, 2)
  const seeded = new DatabaseSync(monsterPath(donorPath), {
    readBigInts: true,
  })
  seeded.exec(`
    CREATE TRIGGER monsterft_fail_certification
    BEFORE UPDATE OF repair_state ON monsterft_meta
    BEGIN
      SELECT RAISE(ABORT, 'injected certification metadata failure');
    END
  `)
  seeded.close()

  const beforeMeta = readMetaAtPath(donorPath)
  const beforeLog = raftBytesAtPath(donorPath)
  const metadataErr = errorOfCall(() => MonsterFt.certify(donorPath))
  t.ok(metadataErr instanceof ErrorWithCode,
    'an injected metadata failure throws ErrorWithCode')
  t.match(metadataErr.message, /injected certification metadata failure/,
    'an injected metadata failure rejects certification')
  t.equal(metadataErr.code, SQLITE_ERROR,
    'an injected metadata failure uses SQLITE_ERROR')
  t.ok(Number.isSafeInteger(metadataErr.sqlCode),
    'an injected metadata failure retains its native SQLite code')
  t.deepEqual(readMetaAtPath(donorPath), beforeMeta,
    'failed certification rolls back repair state 2')
  t.deepEqual(raftBytesAtPath(donorPath), beforeLog,
    'failed certification leaves the Raft log unchanged')

  const cleanup = new DatabaseSync(monsterPath(donorPath), {
    readBigInts: true,
  })
  cleanup.exec('DROP TRIGGER monsterft_fail_certification')
  cleanup.close()
  const result = MonsterFt.certify(donorPath)
  const afterMeta = readMetaAtPath(donorPath)

  t.equal(result, beforeMeta.applied_seq,
    'certification succeeds after the write fault is removed')
  t.equal(afterMeta.repair_state, 0n,
    'successful certification clears repair state 2')
  t.equal(afterMeta.applied_seq, beforeMeta.applied_seq,
    'successful certification preserves applied_seq')
  t.deepEqual(afterMeta.applied_entry_hash, beforeMeta.applied_entry_hash,
    'successful certification preserves the applied hash')
  t.deepEqual(raftBytesAtPath(donorPath), beforeLog,
    'successful certification still preserves the Raft log')
  t.end()
})

test('MonsterFt.certify preserves a wholly unapplied Raft CMD suffix',
  async (t) => {
    const fixture = recoveryFixture(t, 'certify-unapplied-cmd')
    fixture.clear()
    const initial = fixture.build('initial', ['1'])
    const donor = initial.nodes[0]
    donor.open()
    donor.close()

    const donorPath = fixture.paths.get(donor.id)
    const [, cmdSeq] = appendRaftAtPath(donorPath, [
      raftEntry(0n, null),
      raftEntry(0n, {
        type: 'cmd',
        items: [toBuf({ key: 'unapplied-cmd', value: 31 })],
        matchIndex: defaultIds.map(() => -1n),
      }),
    ])
    const before = raftBytesAtPath(donorPath)
    const appliedSeq = readMetaAtPath(donorPath).applied_seq
    const certification = MonsterFt.certify(donorPath)

    t.equal(certification, appliedSeq,
      'certification returns the checkpoint below the suffix')
    t.ok(cmdSeq > certification,
      'the wholly unapplied CMD remains beyond the checkpoint')
    t.deepEqual(raftBytesAtPath(donorPath), before,
      'certification preserves the unapplied CMD byte for byte')

    for (const id of defaultIds) {
      if (id === donor.id) { continue }
      replaceDatabase(donorPath, fixture.paths.get(id))
    }
    fixture.allowOutcomes()
    const restarted = fixture.build('restarted')
    const leader = await elect(restarted.nodes)
    const syncSeq = await waitForSync(restarted.nodes, cmdSeq)
    await waitFor(() => restarted.nodes.every((node) => {
      return valueAt(node, 'unapplied-cmd') === 31
    }), 'unapplied CMD recovery')

    t.equal(restarted.applyCalls.filter((call) => {
      return call.seq === cmdSeq
    }).length, defaultIds.length,
    'ordinary recovery applies the CMD once per copied member')
    t.ok(syncSeq > cmdSeq,
      'ordinary recovery appends a resolving SYNC')
    t.deepEqual(raftRecordAt(leader, syncSeq).agree, defaultIds,
      'the recovered CMD reaches an ordinary agreeing decision')
  })

test('MonsterFt.certify override bypasses only the unresolved-CMD guard',
  async (t) => {
    const fixture = recoveryFixture(t, 'certify-unresolved-override')
    fixture.clear()
    const cluster = fixture.build('initial')
    const leader = await elect(cluster.nodes)

    const pending = leader.append(toBuf({ key: 'unresolved', value: 41 }))
    pending.catch(noop)
    const cmdSeq = await waitForUnresolvedCmd(cluster.nodes)
    fixture.close(cluster.nodes)
    await withTimeout(errorOf(pending), 'unresolved command shutdown')

    const donorPath = fixture.paths.get(leader.id)
    setRepairState(donorPath, 1)
    const beforeMeta = readMetaAtPath(donorPath)
    const beforeLog = raftBytesAtPath(donorPath)

    t.equal(beforeMeta.pending_cmd_seq, cmdSeq,
      'the donor metadata identifies the pending CMD')
    t.equal(beforeMeta.pending_local_digest.length, 32,
      'the donor metadata retains the pending local digest')
    const unresolvedErr = errorOfCall(() => MonsterFt.certify(donorPath))
    t.ok(unresolvedErr instanceof ErrorWithCode,
      'the unresolved donor rejection throws ErrorWithCode')
    t.match(
      unresolvedErr.message,
      /requires every materialized CMD output to have an applied SYNC/i,
      'default certification rejects the unresolved donor',
    )
    t.equal(unresolvedErr.code, ARGUMENT_ILLEGAL,
      'the unresolved donor rejection uses ARGUMENT_ILLEGAL')
    t.equal(unresolvedErr.sqlCode, null,
      'the unresolved donor rejection has no SQLite code')

    const close = DatabaseSync.prototype.close
    DatabaseSync.prototype.close = function() {
      close.call(this)
      throw new Error('injected certification close failure')
    }
    let firstErr = null
    try {
      firstErr = errorOfCall(() => MonsterFt.certify(donorPath))
    } finally {
      DatabaseSync.prototype.close = close
    }
    t.equal(firstErr.message, unresolvedErr.message,
      'a later close failure preserves the first certification error')
    t.equal(firstErr.code, ARGUMENT_ILLEGAL,
      'the preserved first failure retains its code')
    t.notOk(firstErr instanceof AggregateError,
      'certification does not aggregate cleanup failures')
    t.deepEqual(readMetaAtPath(donorPath), beforeMeta,
      'default rejection changes no metadata')
    t.deepEqual(raftBytesAtPath(donorPath), beforeLog,
      'default rejection changes no log bytes')

    const result = MonsterFt.certify(donorPath, true)
    const afterMeta = readMetaAtPath(donorPath)

    t.equal(result, beforeMeta.applied_seq,
      'the explicit override returns the DB2 applied checkpoint')
    t.equal(afterMeta.repair_state, 0n,
      'the override clears only repair state 1')
    t.equal(afterMeta.applied_seq, beforeMeta.applied_seq,
      'the override preserves applied_seq')
    t.deepEqual(afterMeta.applied_entry_hash, beforeMeta.applied_entry_hash,
      'the override preserves the applied hash')
    t.equal(afterMeta.pending_cmd_seq, beforeMeta.pending_cmd_seq,
      'the override preserves the pending command sequence')
    t.deepEqual(
      afterMeta.pending_local_digest,
      beforeMeta.pending_local_digest,
      'the override preserves the pending command digest',
    )
    t.deepEqual(raftBytesAtPath(donorPath), beforeLog,
      'the override preserves the complete Raft log')

    const hydrated = fixture.build('hydrated', [leader.id]).nodes[0]
    hydrated.open()
    t.equal(hydrated._monsterPendingCommand.cmdSeq, cmdSeq,
      'open hydrates the authoritative pending-command cache')
    t.deepEqual(
      hydrated._monsterPendingCommand.localDigest,
      beforeMeta.pending_local_digest,
      'open hydrates the cached digest from durable metadata',
    )
    const command = await hydrated._monsterRunDbGetCmd(cmdSeq)
    t.equal(command, hydrated._monsterPendingCommand,
      'command lookup returns the authoritative pending command')
    t.deepEqual(
      command.localDigest,
      beforeMeta.pending_local_digest,
      'the authoritative pending command retains its durable digest',
    )
  })

test('a quorum:false SYNC leaves a live state-1 donor that certify clears',
  async (t) => {
    const ids = ['1', '2', '3', '4', '5']
    const fixture = recoveryFixture(t, 'certify-fenced-resolved', {
      ids,
      quorum: 3,
    })
    fixture.clear()
    const initial = fixture.build('initial', ['5'])
    const initialNode = initial.nodes[0]
    initialNode.open()
    initialNode.close()

    const donorPath = fixture.paths.get(initialNode.id)
    const falseSync = {
      type: 'sync',
      cmdSeq: 1n,
      quorum: false,
      agree: [],
      disagree: ['1'],
      digest: Buffer.alloc(7, 0x55),
    }
    const [, cmdSeq, syncSeq] = appendRaftAtPath(donorPath, [
      raftEntry(0n, null),
      raftEntry(0n, {
        type: 'cmd',
        items: [toBuf({ key: 'quorum-impossible', value: 51 })],
        matchIndex: ids.map(() => -1n),
      }),
      raftEntry(0n, falseSync),
    ])

    const applying = fixture.build('applying', ['5'])
    const node = applying.nodes[0]
    const syncs = []
    node.on('sync', (event) => syncs.push(event))
    node.open()
    node._commitSeq = cmdSeq
    node._commitTerm = 0n
    await node._apply(cmdSeq)
    const pendingMeta = pendingAt(node)
    t.equal(pendingMeta.pending_cmd_seq, cmdSeq,
      'CMD application installs the pending sequence in metadata')
    t.deepEqual(
      node._monsterPendingCommand.localDigest,
      pendingMeta.pending_local_digest,
      'CMD application publishes matching durable and cached digests',
    )
    node._commitSeq = syncSeq
    await node._apply(syncSeq)
    await waitFor(() => {
      return node.isOpen &&
        node._monsterRepairState === 1 &&
        readMetaAtPath(donorPath).repair_state === 1n
    }, 'quorum-impossible live fence')

    const state1Meta = readMetaAtPath(donorPath)
    t.equal(state1Meta.applied_seq, syncSeq,
      'state 1 persists the SYNC checkpoint')
    t.equal(state1Meta.pending_cmd_seq, null,
      'state 1 clears the resolved pending sequence')
    t.equal(state1Meta.pending_local_digest, null,
      'state 1 clears the resolved pending digest')
    t.deepEqual(syncs, [{
      cmdSeq,
      syncSeq,
      quorum: false,
      agree: [],
      disagree: ['1'],
    }], 'state 1 emits the applied quorum:false SYNC')
    t.equal(node.id, '5',
      'quorum:false places an unlisted applying node in state 1')
    t.deepEqual(falseSync.disagree, ['1'],
      'quorum:false trusts the record without reconstructing reporter sufficiency')
    t.ok(Object.hasOwn(falseSync, 'digest'),
      'quorum:false ignores an otherwise unused digest field')

    const liveFenceError = await errorOf(node.append(
      toBuf({ key: 'state-1-rejected', value: 52 }),
    ))
    t.equal(liveFenceError?.code, REPAIR_QUORUM_IMPOSSIBLE,
      'state 1 rejects a new Monster command with its numbered code')
    t.equal(node.isOpen, true,
      'a state-1 command rejection leaves the node online')

    node.close()
    for (const id of ['1', '2']) {
      const peerPath = fixture.paths.get(id)
      replaceDatabase(donorPath, peerPath)
      MonsterFt.certify(peerPath)
    }
    const reconstructed = fixture.build(
      'state-1-election',
      ['5', '1', '2'],
    )
    const state1Leader = await elect(
      reconstructed.nodes,
      reconstructed.nodes[0],
    )
    await withTimeout(
      state1Leader._monsterLeaderSync,
      'state-1 leader synchronization',
    )
    const noopSeq = state1Leader._commitSeq
    await waitFor(() => state1Leader._applySeq >= noopSeq,
      'state-1 leader no-op application')

    t.equal(state1Leader.state, 'leader',
      'the reconstructed state-1 member wins the election')
    t.ok(noopSeq > syncSeq,
      'the state-1 leader commits a later election no-op')
    t.equal(state1Leader._applySeq, noopSeq,
      'the state-1 leader checkpoints the election no-op')
    t.equal(reconstructed.applyCalls.length, 0,
      'the later election no-op invokes no user application callback')
    t.equal(state1Leader._monsterRepairState, 1,
      'election no-op checkpoint preserves cached repair state 1')
    const electedState1Meta = readMetaAtPath(donorPath)
    t.equal(electedState1Meta.applied_seq, noopSeq,
      'the election no-op advances the durable checkpoint')
    t.equal(electedState1Meta.repair_state, 1n,
      'election no-op checkpoint preserves durable repair state 1')
    t.equal(state1Leader.isOpen, true,
      'the elected state-1 leader remains online')
    t.equal(valueAt(state1Leader, 'quorum-impossible'), 51,
      'state-1 reconstruction retains initialized application state')

    const leaderFenceError = await errorOf(state1Leader.append(
      toBuf({ key: 'state-1-leader-rejected', value: 53 }),
    ))
    t.equal(leaderFenceError?.code, REPAIR_QUORUM_IMPOSSIBLE,
      'the state-1 leader rejects Monster CMD with its numbered code')
    t.equal(state1Leader.state, 'leader',
      'state-1 command fencing preserves leadership')
    t.equal(state1Leader.isOpen, true,
      'state-1 command fencing leaves the leader online')
    fixture.close(reconstructed.nodes)

    const beforeLog = raftBytesAtPath(donorPath)

    const certification = MonsterFt.certify(donorPath)
    const certifiedMeta = readMetaAtPath(donorPath)
    t.equal(certification, noopSeq,
      'default certification accepts the resolved state-1 donor')
    t.equal(certifiedMeta.repair_state, 0n,
      'certification clears repair state 1')
    t.equal(certifiedMeta.applied_seq, electedState1Meta.applied_seq,
      'certification preserves the state-1 applied checkpoint')
    t.deepEqual(
      certifiedMeta.applied_entry_hash,
      electedState1Meta.applied_entry_hash,
      'certification preserves the state-1 applied hash',
    )
    t.equal(certifiedMeta.pending_cmd_seq, null,
      'certification keeps resolved pending state clear')
    t.equal(certifiedMeta.pending_local_digest, null,
      'certification does not recreate a resolved digest')
    t.deepEqual(raftBytesAtPath(donorPath), beforeLog,
      'certification preserves the Raft decision history')

    const reopened = fixture.build('certified', ['5'])
    reopened.nodes[0].open()
    t.equal(reopened.nodes[0].isOpen, true,
      'a fresh ordinary object opens the certified pair')
    t.equal(reopened.nodes[0]._monsterPendingCommand, null,
      'the certified resolved donor hydrates an empty pending cache')
    t.equal(valueAt(reopened.nodes[0], 'quorum-impossible'), 51,
      'certified restart retains initialized application state')
  })

test('five-node selective repair keeps the other three members live',
  async (t) => {
    const ids = ['1', '2', '3', '4', '5']
    const fixture = recoveryFixture(t, 'selective-repair', {
      ids,
      quorum: 3,
    })
    fixture.clear()
    const first = fixture.build('initial')
    const leader = await elect(first.nodes)
    fixture.allowOutcomes()

    const target = first.nodes.find((node) => node.id === '5')
    const donor = first.nodes.find((node) => node.id === '2')
    const healthy = first.nodes.filter((node) => node !== target)
    const [divergentCmdSeq, result] = await leader.append(
      divergentCommand('selective-source', 61, 561, target.id),
    )
    const divergentSyncSeq = await waitForSync(healthy, divergentCmdSeq)
    await waitFor(() => {
      return !target.isOpen &&
        readMetaAtPath(fixture.paths.get(target.id)).repair_state === 2n
    }, 'selective target fence')

    t.deepEqual(result, { key: 'selective-source', value: 61 },
      'the healthy leader returns the quorum result')
    const divergentSync = raftRecordAt(leader, divergentSyncSeq)
    t.ok(divergentSync.agree.length >= leader.quorum &&
      divergentSync.agree.every((id) => id !== target.id),
      'the SYNC records a healthy agreeing quorum')
    t.ok(divergentSync.disagree.every((id) => id === target.id),
      'the SYNC records no healthy member as disagreeing')
    t.equal(valueAtPath(fixture.paths.get(target.id), 'selective-source'), 561,
      'the stopped target retains its divergent local application state')

    await withTimeout(donor.drainCmd(), 'selective repair donor drain')
    t.notOk(donor.isOpen,
      'the healthy donor is terminally drained before certification')
    t.equal(donor.db, null,
      'the healthy donor closes DB2 before pair copying')
    t.notOk(donor.log.isOpen,
      'the healthy donor closes DB1 before pair copying')
    const remaining = healthy.filter((node) => node !== donor)
    t.deepEqual(remaining.map((node) => node.id), ['1', '3', '4'],
      'exactly three original members remain online')

    const [liveCmdSeq, liveResult] = await leader.append(
      toBuf({ key: 'while-repair-offline', value: 62 }),
    )
    const liveSyncSeq = await waitForSync(remaining, liveCmdSeq)
    t.deepEqual(liveResult, { key: 'while-repair-offline', value: 62 },
      'the remaining quorum continues accepting commands')
    t.ok(remaining.every((node) => {
      return valueAt(node, 'while-repair-offline') === 62
    }), 'the remaining quorum applies the command and SYNC')

    const donorPath = fixture.paths.get(donor.id)
    const targetPath = fixture.paths.get(target.id)
    const donorMeta = readMetaAtPath(donorPath)
    t.equal(donorMeta.pending_cmd_seq, null,
      'the drained donor has no pending command before certification')
    t.equal(donorMeta.pending_local_digest, null,
      'the drained donor has no pending digest before certification')
    const donorLog = raftBytesAtPath(donorPath)
    const certification = MonsterFt.certify(donorPath)
    t.equal(certification, donorMeta.applied_seq,
      'the stopped healthy donor certifies at its DB2 checkpoint')
    t.deepEqual(raftBytesAtPath(donorPath), donorLog,
      'certifying the donor preserves its history')

    const staleSidecars = [
      `${targetPath}-journal`,
      `${targetPath}-wal`,
      `${targetPath}-shm`,
      `${monsterPath(targetPath)}-journal`,
      `${monsterPath(targetPath)}-wal`,
      `${monsterPath(targetPath)}-shm`,
    ]
    staleSidecars.forEach((sidecar) => fs.writeFileSync(sidecar, 'stale'))
    replaceDatabase(donorPath, targetPath)
    staleSidecars.forEach((sidecar) => {
      t.equal(fs.existsSync(sidecar), false,
        `pair copy removes target sidecar ${sidecar}`)
    })

    const repaired = fixture.build('repaired', [donor.id, target.id])
    openNodes(repaired.nodes)
    t.deepEqual(repaired.nodes.map((node) => {
      return valueAt(node, 'selective-source')
    }), [61, 61], 'the copied DB2 files retain initialized state')
    t.ok(repaired.nodes.every((node) => node.isOpen),
      'both copied members restart with their normal constructors')
    t.ok(repaired.nodes.every((node) => {
      return readMetaAtPath(fixture.paths.get(node.id)).repair_state === 0n
    }), 'both copied members start healthy')

    await waitFor(() => {
      leader._pingFollowers()
      return repaired.nodes.every((node) => {
        return node.isOpen &&
          node._applySeq >= liveSyncSeq &&
          valueAt(node, 'while-repair-offline') === 62
      })
    }, 'selectively repaired members catch up')

    const allLive = [...remaining, ...repaired.nodes]
    const [finalCmdSeq, finalResult] = await leader.append(
      toBuf({ key: 'after-selective-repair', value: 63 }),
    )
    const finalSyncSeq = await waitForSync(allLive, finalCmdSeq)

    t.deepEqual(finalResult, { key: 'after-selective-repair', value: 63 },
      'the repaired five-node cluster accepts a normal command')
    t.ok(finalSyncSeq > liveSyncSeq,
      'post-repair progress follows the command made by the live quorum')
    t.ok(allLive.every((node) => {
      return valueAt(node, 'after-selective-repair') === 63
    }), 'all five members converge after selective repair')
    t.deepEqual(
      fixture.errors.map(({ id, err }) => ({ id, code: err.code })),
      [{ id: target.id, code: REPAIR_OUTSIDE_AGREEMENT }],
      'only the unhealthy target reports the expected terminal error',
    )
  })
