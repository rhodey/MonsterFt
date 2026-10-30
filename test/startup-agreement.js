import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { Gate } from '../src/gate.js'
import { MonsterProc } from '../src/proc.js'
import { MonsterApply } from '../src/apply.js'
import {
  ProtocolStore, databasePaths, openApplyDatabase, readDatabase, readState, stateDigest,
} from '../src/storage.js'
import { digestEntry } from '../src/utilm.js'
import { readApply } from '../src/utils.js'
import * as wire from '../src/wire.js'
import * as Err from '../src/error.js'
import { roles, until, pause } from './helper.js'

const nodes = ['a', 'b', 'c']

function localNode(base, options = {}) {
  const local = {}
  const sent = []
  const errors = []
  const calls = []
  const opts = {
    electionTimeout: 10_000, tickInterval: 1_000, localTimeout: 1_000,
    ...options.opts,
  }
  const localSend = (from) => (to, frame) => {
    const message = wire.decodeFrame(frame)
    const deliver = () => local[to].onReceive(from, frame)
    if (options.local?.({ from, to, message, deliver }) === false) { return }
    if (options.delayed) { setImmediate(deliver) }
    else { deliver() }
  }
  local.gate = new Gate('a', nodes, (to, frame) => {
    // Decoding verifies the checksum of every actual outbound frame.
    const { message } = wire.decodeFrame(frame)
    sent.push({ to, message })
    options.send?.({ to, message, gate: local.gate })
  }, localSend('gate'), opts)
  for (const role of roles) {
    local[role] = new MonsterProc(role, 'a', nodes, localSend(role), base, opts)
  }
  local.apply = new MonsterApply('a', localSend('apply'), base,
    (db, data, term, seq) => {
      calls.push({ data, seq })
      if (seq === 0n) { db.exec('CREATE TABLE items (value TEXT NOT NULL)'); return null }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(data.toString())
      return data.toString()
    })
  for (const [role, component] of Object.entries(local)) {
    component.on('error', (error) => errors.push({ role, error }))
  }
  const close = async () => {
    local.gate.stop()
    for (const role of roles) { local[role].close() }
    await local.apply.close()
  }
  return { ...local, sent, errors, calls, close }
}

// An interrupted replication input left A/B ahead of C. One interior CMD bit
// was damaged in A; existing head-only adoption still copies that CMD into C.
function damagedDonor(base) {
  const seed = Buffer.alloc(16, 1)
  const stores = roles.map((role) => new ProtocolStore(role, base))
  let db2
  try {
    for (const store of stores) { store.open(seed) }
    db2 = openApplyDatabase(base, seed)
    const setup = { term: 1n, entry: Buffer.alloc(0) }
    for (const store of stores) { store.log.append(setup) }
    db2.exec('CREATE TABLE items (value TEXT NOT NULL)')
    db2.prepare('UPDATE monsterft_meta_apply SET seq = 0, entry_hash = ?, local_digest = ?')
      .run(digestEntry(1n, setup.entry), Buffer.alloc(16))
    for (const store of stores) { store.capture() }
    for (const store of stores) { store.adopt() }
    for (const store of stores) { store.reconcile() }
    const cmd = wire.encodePayload({
      type: 'cmd', items: [Buffer.from('amount=1')], matchIndex: [0n, 0n, 0n],
    })
    for (const store of stores.slice(0, 2)) {
      store.db.exec('BEGIN IMMEDIATE')
      store.log.election(2n, 'b')
      store.log.appendBatch([{ term: 1n, entry: cmd }, { term: 2n, entry: Buffer.alloc(0) }])
      store.db.exec('COMMIT')
    }
    const damaged = Buffer.from(cmd)
    damaged[damaged.indexOf(Buffer.from('amount=1')) + 7] ^= 1
    stores[0].db.prepare('UPDATE raft_log SET entry = ? WHERE seq = 1').run(damaged)
  } finally {
    for (const store of stores) { store.close() }
    db2?.close()
  }
}

for (const delayed of [false, true]) {
  test(`a replaced adoption decision cannot promote the minority across startup (${delayed ? 'delayed' : 'sync'})`, async (t) => {
    for (const outlier of roles) {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-adoption-agreement-'))
      const base = path.join(directory, 'db')
      const paths = databasePaths(base)
      const seed = Buffer.alloc(16, 1)
      const stores = roles.map((role) => new ProtocolStore(role, base))
      try {
        for (const store of stores) { store.open(seed) }
        openApplyDatabase(base, seed).close()
        for (const store of stores) {
          if (store.role !== outlier) { store.log.election(5n, 'b') }
        }
      } finally {
        for (const store of stores) { store.close() }
      }
      const snapshot = () => roles.map((role) => readDatabase(paths[role],
        (db) => stateDigest(readState(db))))
      const before = snapshot()
      const minorityDigest = before[roles.indexOf(outlier)]
      const target = roles.find((role) => role !== outlier)
      let replaced = false
      const first = localNode(base, {
        delayed,
        opts: { localTimeout: 200 },
        local({ from, to, message }) {
          if (from !== 'gate' || message.type !== 'adopt') { return }
          // Hold the other owners at capture so both original peer states remain
          // visible while the damaged decision attempts destructive adoption.
          if (to !== target) { return false }
          first[target].store._captured = {
            ...first[target].store._captured,
            selectedDigest: minorityDigest,
            donor: outlier,
          }
          replaced = true
        },
      })
      let second
      try {
        const failure = await first.gate.open().then(() => null, (error) => error)
        t.ok(replaced, `${outlier}: one captured-decision reference was replaced`)
        t.equal(failure?.code, Err.LOCAL_TIMEOUT, 'failed owner prevents startup completion')
        t.equal(first.errors.find(({ role }) => role === target)?.error.code,
          Err.MONSTER_CORRUPT, 'the actual peer state contradicts the substituted selection')
        t.notOk(first[target].log.isOpen, 'failed Proc closes and rolls back its database')
        t.deepEqual(snapshot(), before, 'no unsupported replacement commits')
        t.equal(first.sent.length, 0, 'failed startup sends no remote RPC')
        t.equal(first.calls.length, 0, 'failed startup invokes no Apply callback')
        await first.close()

        second = localNode(base, { delayed })
        await second.gate.open()
        t.ok(roles.every((role) => second[role].log.elec.term === 5n &&
          second[role].log.elec.votedFor === 'b'), 'fresh startup retains the original majority vote')
        const recovered = snapshot()
        t.ok(recovered.every((digest) => digest.equals(before[roles.indexOf(target)])),
          'every recovered DB1 matches the original majority')
        t.deepEqual(second.errors, [], 'replacement startup has no errors')
        t.equal(second.calls.length, 0, 'reconciliation does not invoke Apply')
      } finally {
        await first.close()
        await second?.close()
        fs.rmSync(directory, { recursive: true, force: true })
      }
    }
    t.end()
  })

  test(`missing Proc startup completion blocks remote output despite a Gate scheduling fault (${delayed ? 'delayed' : 'sync'})`, async (t) => {
    for (const missing of ['procB', 'procC']) {
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-startup-agreement-'))
      let held = false
      const node = localNode(path.join(directory, 'db'), {
        delayed,
        opts: { localTimeout: 200 },
        local({ from, to, message }) {
          if (from === 'gate' && to === missing && message.type === 'start') {
            held = true
            return false
          }
        },
      })
      const opening = node.gate.open().then(() => null, (error) => error)
      try {
        await until(() => held && roles.filter((role) => role !== missing)
          .every((role) => node[role].isOpen), 'two Procs started')
        // Model loss of the single-copy dispatch barrier at a real phase boundary.
        node.gate._current = null
        node.gate.onReceive('b', wire.encodeFrame({ to: 'a', message: {
          type: 'vote_request', from: 'b', term: 1n, termP: -1n, seqP: -1n,
        } }))
        await until(() => roles.filter((role) => role !== missing)
          .every((role) => node[role].log.elec.term === 1n), 'two Procs processed vote')
        t.equal(node.sent.length, 0, `${missing}: two durable votes cannot release an RPC`)
        t.equal((await opening).code, Err.LOCAL_TIMEOUT, 'incomplete startup fails')
        t.equal(node.sent.length, 0, 'timeout cannot release buffered outputs')
        t.equal(node.calls.length, 0, 'Apply never executes')
      } finally {
        await node.close()
        await opening
        fs.rmSync(directory, { recursive: true, force: true })
      }
    }
    t.end()
  })
}

test('healthy third Proc blocks a damaged donor command before Apply or an ACK is released', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-donor-agreement-'))
  const base = path.join(directory, 'db')
  damagedDonor(base)
  let release = null
  const node = localNode(base, {
    local({ from, to, message, deliver }) {
      if (from === 'procB' && to === 'gate' && message.tickCount === 2n) {
        release = deliver
        return false
      }
    },
  })
  try {
    await node.gate.open()
    t.deepEqual(roles.map((role) => wire.decodePayload(
      node[role].log.iter(1n).next().value.entry).items[0].toString()),
    ['amount=0', 'amount=1', 'amount=0'], 'the deferred donor-copy issue is deliberately still present')
    node.gate.onReceive('b', wire.encodeFrame({ to: 'a', message: {
      from: 'b', type: 'append', cid: 'commit-existing-log', term: 2n,
      seqP: 2n, termP: 2n, commitSeq: 2n,
    } }))
    await until(() => release !== null, 'healthy Proc completion held')
    await pause(5)
    t.equal(node.calls.length, 0, 'A/C agreement alone cannot invoke Apply')
    t.equal(node.sent.length, 0, 'even the matching ACK preceding execution stays buffered')
    release()
    release = null
    await until(() => !node.gate.isOpen, 'disagreement stops Gate')
    t.equal(node.errors.find(({ role }) => role === 'gate')?.error.code, Err.LOCAL_DIVERGENCE)
    t.equal(node.calls.length, 0, 'dissent prevents the wrong callback')
    t.equal(node.sent.length, 0, 'dissent discards every output in the input')
    t.equal(readApply(node.apply.db).seq, 0n, 'DB2 receipt remains at setup')
    t.equal(node.apply.db.prepare('SELECT count(*) AS n FROM items').get().n, 0n)
  } finally {
    release?.()
    await node.close()
    fs.rmSync(directory, { recursive: true, force: true })
  }
  t.end()
})

test('healthy third Proc blocks replication of a damaged donor command', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-donor-replication-'))
  const base = path.join(directory, 'db')
  damagedDonor(base)
  const timers = new Set()
  const node = localNode(base, {
    opts: { electionTimeout: 100, tickInterval: 10, localTimeout: 2_000 },
    local({ from, to, message, deliver }) {
      if (from === 'procB' && to === 'gate' && message.tickCount > 1n) {
        const timer = setTimeout(() => { timers.delete(timer); deliver() }, 20)
        timers.add(timer)
        return false
      }
    },
    send({ to, message, gate }) {
      if (to === 'b' && message.type === 'vote_request') {
        gate.onReceive('b', wire.encodeFrame({ to: 'a', message: {
          type: 'vote', from: 'b', term: message.term, voteGranted: true,
        } }))
      }
      if (to === 'b' && message.type === 'append' && message.data && message.seqP > 0n) {
        gate.onReceive('b', wire.encodeFrame({ to: 'a', message: {
          type: 'err', from: 'b', cid: message.cid, term: message.term,
          code: Err.REPL_BACKTRACK, message: 'predecessor mismatch', logSeq: 0n,
        } }))
      }
    },
  })
  try {
    await node.gate.open()
    await until(() => !node.gate.isOpen, 'replication disagreement stops Gate')
    t.equal(node.errors.find(({ role }) => role === 'gate')?.error.code, Err.LOCAL_DIVERGENCE)
    t.ok(node.sent.some(({ message }) => message.type === 'vote_request'), 'real election starts replication')
    t.ok(node.sent.some(({ message }) => message.type === 'append' && message.data),
      'matching election no-op page is released before backtracking')
    const commands = node.sent.flatMap(({ message }) => (message.data ?? [])
      .filter((record) => record.entry.length)
      .map((record) => wire.decodePayload(record.entry))
      .filter((entry) => entry.type === 'cmd'))
    t.deepEqual(commands, [], 'no checksum-valid frame carries the damaged command')
    t.equal(node.calls.length, 0)
    t.equal(readApply(node.apply.db).seq, 0n)
  } finally {
    for (const timer of timers) { clearTimeout(timer) }
    await node.close()
    fs.rmSync(directory, { recursive: true, force: true })
  }
  t.end()
})
