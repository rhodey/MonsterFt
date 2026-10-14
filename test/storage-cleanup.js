import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { MonsterProc } from '../src/proc.js'
import { databasePaths, openApplyDatabase, readDatabase, readState, stateDigest } from '../src/storage.js'
import { digestEntry } from '../src/utilm.js'
import * as wire from '../src/wire.js'
import * as Err from '../src/error.js'

const roles = ['procA', 'procB', 'procC']
const seed = Buffer.alloc(16, 1)
const replacementSeed = Buffer.alloc(16, 2)

function fixture() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-storage-cleanup-'))
  const base = path.join(dir, 'node')
  const groups = []
  let cid = 0
  return {
    base, paths: databasePaths(base),
    group(generation = seed) {
      const replies = roles.map(() => [])
      const events = roles.map(() => [])
      const procs = roles.map((role, i) => {
        const proc = new MonsterProc(role, 'a', ['a', 'b', 'c'], (to, frame) => {
          if (to !== 'gate') { throw new Error('unexpected destination') }
          replies[i].push(wire.decodeFrame(frame))
        }, base)
        proc.on('error', (error) => events[i].push({ type: 'error', error }))
        proc.on('close', () => events[i].push({ type: 'close' }))
        return proc
      })
      const group = {
        procs, replies, events,
        request(i, type, fields = {}) {
          const message = { cid: `cleanup-${++cid}`, type, ...fields }
          procs[i].onReceive('gate', wire.encodeFrame(message))
          return replies[i].find((reply) => reply.cid === message.cid)
        },
        phase(type) {
          for (let i = 0; i < procs.length; i++) {
            const reply = this.request(i, type, type === 'hello' ? { seed: generation } : {})
            if (reply?.type !== 'ack') { throw new Error(`startup failed at ${type}`) }
          }
        },
        open() {
          this.phase('hello')
          this.phase('open')
          openApplyDatabase(base, generation).close()
          return this
        },
      }
      groups.push(group)
      return group
    },
    cleanup() {
      for (const { procs } of groups) {
        for (const proc of procs) {
          try { proc.close() } catch { }
          // Release injected failed-close handles, as process exit would.
          if (proc.log.isOpen) { proc.store.close() }
        }
      }
      fs.rmSync(dir, { recursive: true, force: true })
    },
  }
}

test('Proc startup preserves operation failures and attempts database cleanup only once', (t) => {
  for (const phase of ['open', 'adopt', 'reconcile']) {
    const f = fixture()
    const nativeClose = DatabaseSync.prototype.close
    try {
      let group
      if (phase === 'open') {
        const initial = f.group().open()
        for (const proc of initial.procs) { proc.close() }
        const db = new DatabaseSync(f.paths.procA)
        try {
          db.exec(`CREATE TRIGGER reject_claim BEFORE UPDATE ON monsterft_meta
            BEGIN SELECT RAISE(ABORT, 'generation claim failed'); END`)
        } finally { db.close() }
        group = f.group(replacementSeed)
        group.phase('hello')
      } else {
        group = f.group().open()
        group.phase('capture')
        if (phase === 'reconcile') { group.phase('adopt') }
        f.group(replacementSeed).open()
      }
      const proc = group.procs[0]
      const cleanupError = new Err.ErrorWithCode('native close failed', Err.SQLITE_ERROR)
      cleanupError.stack = 'original native close stack'
      let attempts = 0
      DatabaseSync.prototype.close = function () {
        if (this === proc.db && ++attempts === 1) { throw cleanupError }
        return nativeClose.call(this)
      }
      const reentrant = []
      proc.on('error', () => {
        try { proc.close() } catch (err) { reentrant.push(err) }
      })
      t.equal(group.request(0, phase), undefined, `${phase}: failure sends no completion`)
      t.deepEqual(group.events[0].map(({ type }) => type), ['error', 'error'],
        `${phase}: reports original and cleanup failures without close`)
      const original = group.events[0][0].error
      t.equal(original.code, phase === 'open' ? Err.SQLITE_ERROR : Err.GENERATION_DIFF,
        `${phase}: original failure is reported first`)
      t.ok(original.message.includes(phase === 'open' ? 'generation claim failed' : 'generation different'),
        `${phase}: original diagnostic is preserved`)
      t.equal(group.events[0][1]?.error, cleanupError, `${phase}: cleanup failure is also reported`)
      t.equal(cleanupError.stack, 'original native close stack', `${phase}: cleanup stack is preserved`)
      t.equal(proc._fatalError, original, `${phase}: fatal state retains the original failure`)
      t.ok(proc._stopped, `${phase}: Proc has stopped despite failed cleanup`)
      t.ok(proc.log.isOpen, `${phase}: failed cleanup leaves the owner handle open`)
      t.equal(attempts, 1, `${phase}: native close is attempted only once`)
      t.deepEqual(reentrant, [cleanupError, cleanupError], `${phase}: listeners cannot retry teardown`)
      t.throws(() => proc.close(), (err) => err === cleanupError,
        `${phase}: explicit close retains its first failure`)
      t.equal(group.request(0, 'close').type, 'err', `${phase}: close request cannot ACK failed cleanup`)
      t.equal(attempts, 1, `${phase}: later close does not retry native cleanup`)
      const replies = group.replies[0].length
      group.request(0, 'hello', { seed })
      t.equal(group.replies[0].length, replies, `${phase}: stopped owner accepts no new work`)
    } finally {
      DatabaseSync.prototype.close = nativeClose
      f.cleanup()
    }
  }
  t.end()
})

test('temporary reader cleanup preserves a stale Proc generation failure', (t) => {
  const f = fixture()
  const nativeClose = DatabaseSync.prototype.close
  let reader = null
  try {
    const old = f.group().open()
    const replacement = f.group(replacementSeed).open()
    const owners = new Set([...old.procs, ...replacement.procs].map((proc) => proc.db))
    let readerCloses = 0
    let ownerCloses = 0
    DatabaseSync.prototype.close = function () {
      if (!owners.has(this)) {
        reader = this
        readerCloses++
        throw new Err.ErrorWithCode('reader close failed', Err.SQLITE_ERROR)
      }
      ownerCloses++
      return nativeClose.call(this)
    }
    t.equal(old.request(0, 'capture'), undefined, 'stale capture sends no completion')
    t.deepEqual(old.events[0].map(({ type }) => type), ['error', 'close'],
      'original failure precedes successful owner close notification')
    const error = old.events[0][0].error
    t.equal(error.code, Err.GENERATION_DIFF, 'reader close failure does not hide generation failure')
    t.equal(error.message, 'DB1 generation different')
    t.ok(error.stack.includes('checkGeneration'), 'generation failure retains its original stack')
    t.equal(readerCloses, 1, 'temporary reader cleanup is attempted once')
    t.equal(ownerCloses, 1, 'Proc owner cleanup is attempted once')
    t.notOk(old.procs[0].log.isOpen, 'owner database closes after the capture failure')
  } finally {
    DatabaseSync.prototype.close = nativeClose
    if (reader !== null) { nativeClose.call(reader) }
    f.cleanup()
  }
  t.end()
})

test('Proc cleanup rolls back failed adoption and allows a replacement to finish startup', (t) => {
  const f = fixture()
  try {
    const group = f.group().open()
    const proc = group.procs[0]
    proc.log.append({ term: 1n, entry: Buffer.from('minority history') })
    proc.db.exec(`CREATE TRIGGER reject_adoption BEFORE INSERT ON raft_election
      BEGIN SELECT RAISE(ABORT, 'adoption rejected'); END`)
    group.phase('capture')
    t.equal(group.request(0, 'adopt'), undefined, 'failed adoption sends no completion')
    t.deepEqual(group.events[0].map(({ type }) => type), ['error', 'close'],
      'Proc reports the failure and closes its database')
    t.ok(group.events[0][0].error.message.includes('adoption rejected'))
    t.notOk(proc.log.isOpen, 'Proc owns cleanup after failed storage work')
    const db = new DatabaseSync(f.paths.procA, { readBigInts: true })
    try {
      t.equal(Buffer.from(db.prepare('SELECT entry FROM raft_log').get().entry).toString(),
        'minority history', 'partial donor replacement rolls back the removed log row')
      t.equal(db.prepare('SELECT current_term FROM raft_election').get().current_term,
        0n, 'partial donor replacement rolls back the removed election row')
      db.exec('DROP TRIGGER reject_adoption')
    } finally { db.close() }
    const replacement = f.group(replacementSeed).open()
    for (const phase of ['capture', 'adopt', 'reconcile']) { replacement.phase(phase) }
    t.ok(replacement.procs.every((owner) => owner.log.seq === -1n),
      'new owners adopt the majority and complete startup')
    t.ok(replacement.events.every((events) => events.length === 0),
      'replacement startup has no diagnostic or close events')
  } finally { f.cleanup() }
  t.end()
})

test('adoption readback rejects wrong copied values before commit and preserves recovery', (t) => {
  const faults = {
    election: `AFTER INSERT ON raft_election BEGIN
      UPDATE raft_election SET current_term = 99 WHERE id = NEW.id; END`,
    meta: `AFTER UPDATE OF applied_local_digest ON monsterft_meta
      WHEN NEW.applied_local_digest = zeroblob(16) BEGIN
      UPDATE monsterft_meta SET applied_local_digest = X'01010101010101010101010101010101'
        WHERE id = NEW.id; END`,
    head: `AFTER INSERT ON raft_log BEGIN
      UPDATE raft_log SET entry = X'ff' WHERE seq = NEW.seq; END`,
  }
  for (const [name, trigger] of Object.entries(faults)) {
    const f = fixture()
    try {
      const group = f.group().open()
      const entryHash = digestEntry(1n, Buffer.alloc(0))
      for (const [index, proc] of group.procs.entries()) {
        proc.log.append({ term: 1n, entry: Buffer.alloc(0) })
        proc.log.election(index === 0 ? 7n : 5n, 'b')
        proc.db.prepare(`UPDATE monsterft_meta SET applied_seq = 0,
          applied_entry_hash = ?, applied_local_digest = ?`).run(entryHash, Buffer.alloc(16))
      }
      const db2 = new DatabaseSync(f.paths.apply)
      try {
        db2.prepare(`UPDATE monsterft_meta_apply SET seq = 0,
          entry_hash = ?, local_digest = ?`).run(entryHash, Buffer.alloc(16))
      } finally { db2.close() }
      const snapshot = () => roles.map((role) =>
        readDatabase(f.paths[role], (db) => stateDigest(readState(db))))
      const before = snapshot()
      group.procs[0].db.exec(`CREATE TRIGGER corrupt_copy ${trigger}`)
      group.phase('capture')
      t.equal(group.request(0, 'adopt'), undefined, `${name}: failed copy sends no completion`)
      t.deepEqual(group.events[0].map(({ type }) => type), ['error', 'close'],
        `${name}: original failure precedes owner shutdown`)
      t.equal(group.events[0][0].error.code, Err.MONSTER_CORRUPT,
        `${name}: structurally valid wrong content fails verification`)
      t.match(group.events[0][0].error.message, /adopted DB1 state does not match majority/,
        `${name}: destination readback detects the changed value`)
      t.deepEqual(snapshot(), before, `${name}: rollback preserves every original protocol summary`)
      const db1 = new DatabaseSync(f.paths.procA)
      try { db1.exec('DROP TRIGGER corrupt_copy') } finally { db1.close() }
      for (const proc of group.procs) { proc.close() }
      const replacement = f.group(replacementSeed).open()
      for (const phase of ['capture', 'adopt', 'reconcile']) { replacement.phase(phase) }
      const after = snapshot()
      t.ok(after.every((digest) => digest.equals(before[1])),
        `${name}: reopening still adopts the original majority`)
      t.ok(replacement.events.every((events) => events.length === 0),
        `${name}: replacement finishes without diagnostics`)
    } finally { f.cleanup() }
  }
  t.end()
})

test('readDatabase preserves read transaction failures but reports cleanup failure after success', (t) => {
  const f = fixture()
  const nativeClose = DatabaseSync.prototype.close
  const nativeExec = DatabaseSync.prototype.exec
  const db = new DatabaseSync(f.paths.apply)
  db.close()
  try {
    for (const phase of ['BEGIN', 'read', 'COMMIT', 'success']) {
      const original = new Error(`original ${phase} failure`)
      original.stack = `original ${phase} stack`
      const cleanupError = new Error('reader close failed')
      let reader = null
      let attempts = 0
      DatabaseSync.prototype.exec = function (sql) {
        if (sql === phase) { throw original }
        return nativeExec.call(this, sql)
      }
      DatabaseSync.prototype.close = function () {
        reader = this
        attempts++
        throw cleanupError
      }
      try {
        t.throws(() => readDatabase(f.paths.apply, () => {
          if (phase === 'read') { throw original }
          return 42
        }), (err) => err === (phase === 'success' ? cleanupError : original),
        `${phase}: preserves the correct error identity`)
        t.equal(attempts, 1, `${phase}: native close is attempted once`)
        t.equal(original.stack, `original ${phase} stack`, `${phase}: original stack is unchanged`)
      } finally {
        DatabaseSync.prototype.close = nativeClose
        DatabaseSync.prototype.exec = nativeExec
        if (reader !== null) { nativeClose.call(reader) }
      }
    }
  } finally { f.cleanup() }
  t.end()
})
