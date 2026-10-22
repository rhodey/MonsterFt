import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { certify } from '../src/index.js'
import { ProtocolStore, databasePaths, openApplyDatabase, readDatabase, readState, stateDigest } from '../src/storage.js'
import { digestEntry } from '../src/utilm.js'
import { readMeta, readApply } from '../src/utils.js'
import * as Err from '../src/error.js'

const seed = Buffer.alloc(16, 1)
const roles = ['procA', 'procB', 'procC']
const zero = { term: 1n, entry: Buffer.alloc(0) }
const command = { term: 1n, entry: Buffer.from('CMD') }
const sync = { term: 1n, entry: Buffer.from('SYNC') }
const digest = Buffer.alloc(16, 7)

function fixture() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-certify-'))
  const base = path.join(dir, 'node')
  const paths = databasePaths(base)
  const stores = roles.map((role) => new ProtocolStore(role, base))
  stores.forEach((store) => store.open(seed))
  let db2 = openApplyDatabase(base, seed)
  const reconcile = () => {
    stores.forEach((store) => store.capture())
    stores.forEach((store) => store.adopt())
    stores.forEach((store) => store.reconcile())
  }
  const receipt = (seq, data, localDigest) => db2.prepare(`
    UPDATE monsterft_meta_apply SET seq = ?, entry_hash = ?, local_digest = ? WHERE id = 1
  `).run(seq, digestEntry(data.term, data.entry), localDigest)
  return {
    base, paths, stores,
    setup() {
      stores.forEach((store) => store.log.append(zero))
      db2.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)')
      receipt(0n, zero, Buffer.alloc(16))
      reconcile()
      return this
    },
    command(imported=true) {
      stores.forEach((store) => store.log.append(command))
      db2.exec("INSERT INTO items VALUES (1, 'completed')")
      receipt(1n, command, digest)
      if (imported) { reconcile() }
      return this
    },
    finish(repair=1) {
      this.command()
      for (const store of stores) {
        store.log.append(sync)
        store.db.prepare(`
          UPDATE monsterft_meta SET applied_seq = 2, applied_entry_hash = ?,
            repair_state = ?, pending_cmd_seq = NULL
        `).run(digestEntry(sync.term, sync.entry), repair)
      }
      return this
    },
    close() {
      stores.forEach((store) => store.close())
      if (db2 !== null) { db2.close(); db2 = null }
    },
    snapshot() {
      return roles.map((role) => readDatabase(paths[role], (db) => ({
        state: readState(db),
        log: db.prepare('SELECT seq, term, entry FROM raft_log ORDER BY seq').all(),
      })))
    },
    cleanup() { this.close(); fs.rmSync(dir, { recursive: true, force: true }) },
  }
}

test('certify clears fences on all DB1 copies and leaves DB2 unchanged', (t) => {
  for (const repair of [1, 2]) {
    const f = fixture().setup().finish(repair)
    try {
      f.close()
      const before = readDatabase(f.paths.apply, (db) => ({
        receipt: readApply(db), items: db.prepare('SELECT * FROM items').all(),
      }))
      t.equal(certify(f.base), 2n, 'returns resulting protocol checkpoint')
      t.ok(f.snapshot().every(({ state }) => state.meta.repair_state === 0n),
        `clears repair ${repair} everywhere`)
      t.deepEqual(readDatabase(f.paths.apply, (db) => ({
        receipt: readApply(db), items: db.prepare('SELECT * FROM items').all(),
      })), before, 'receipt and apply state remain unchanged')
      t.equal(certify(f.base), 2n, 'certification can be repeated')
    } finally { f.cleanup() }
  }
  t.end()
})

test('certify rejects pending receipt imports before changing any copy', (t) => {
  const f = fixture().setup().command(false)
  try {
    f.stores[2].log.append({ term: 2n, entry: Buffer.from('outlier') })
    f.close()
    const before = f.snapshot()
    t.throws(() => certify(f.base), /requires every CMD output to have an applied SYNC/)
    t.deepEqual(f.snapshot(), before, 'rejected tentative import and adoption leave all DB1s unchanged')
    t.equal(certify(f.base, true), 1n, 'explicit override accepts unresolved CMD')
    const states = f.snapshot()
    t.ok(states.every(({ state }) => state.meta.pending_cmd_seq === 1n && state.meta.applied_seq === 1n),
      'completed receipt copied without rerunning Apply')
    t.ok(states.every(({ log }) => log.length === 2), 'outlier suffix replaced before importing receipt')
    t.throws(() => certify(f.base), /requires every CMD output to have an applied SYNC/,
      'default also rejects an already imported pending CMD')
    t.deepEqual(f.snapshot(), states)
    t.equal(readDatabase(f.paths.apply, (db) => db.prepare('SELECT count(*) AS n FROM items').get().n),
      1n, 'Apply state not replayed')
  } finally { f.cleanup() }
  t.end()
})

test('certify validates every matching copy before committing any changes', (t) => {
  for (const kind of ['checkpoint', 'receipt']) {
    for (const [corrupt, outlier] of [[1, null], [2, null], [2, 0]]) {
      const f = fixture().setup()
      try {
        if (kind === 'checkpoint') { f.finish() } else { f.command(false) }
        // Keep the damaged entry below a matching head so the existing hash
        // checks, rather than majority selection, detect the corruption.
        f.stores.forEach((store) => store.log.append(zero))
        if (outlier !== null) {
          f.stores[outlier].log.append({ term: 2n, entry: Buffer.from('outlier') })
        }
        f.stores[corrupt].db.prepare('UPDATE raft_log SET entry = ? WHERE seq = ?')
          .run(Buffer.from('corrupt'), kind === 'checkpoint' ? 2n : 1n)
        f.close()
        const before = f.snapshot()
        const applyBefore = readDatabase(f.paths.apply, (db) => ({
          receipt: readApply(db), items: db.prepare('SELECT * FROM items').all(),
        }))
        const label = `${kind} on ${roles[corrupt]}${outlier !== null ? ' with outlier' : ''}`
        const donor = outlier === 0 ? 1 : 0
        t.ok(stateDigest(before[corrupt].state).equals(stateDigest(before[donor].state)),
          `${label}: damaged copy matches selected state`)
        const message = kind === 'checkpoint'
          ? 'applied entry hash does not match' : 'apply entry hash does not match'
        t.throws(() => certify(f.base, kind === 'receipt'),
          (err) => err.code === Err.MONSTER_CORRUPT && err.message.includes(message),
          `${label}: corruption rejects certification`)
        t.deepEqual(f.snapshot(), before,
          `${label}: no fence clearance, receipt import, or outlier adoption commits`)
        t.deepEqual(readDatabase(f.paths.apply, (db) => ({
          receipt: readApply(db), items: db.prepare('SELECT * FROM items').all(),
        })), applyBefore, `${label}: DB2 remains unchanged`)
      } finally { f.cleanup() }
    }
  }
  t.end()
})

test('certify replaces a corrupt checkpoint entry in an outlier', (t) => {
  const f = fixture().setup().finish()
  try {
    f.stores[0].log.append({ term: 2n, entry: Buffer.from('outlier') })
    f.stores[0].db.prepare('UPDATE raft_log SET entry = ? WHERE seq = 2')
      .run(Buffer.from('corrupt'))
    f.close()
    t.equal(certify(f.base), 2n, 'discarded outlier does not need to match its checkpoint hash')
    const after = f.snapshot()
    t.deepEqual(after[0], after[1], 'outlier is replaced with the certified majority state')
    t.deepEqual(after[1], after[2], 'all copies agree')
    t.ok(after.every(({ state }) => state.meta.repair_state === 0n), 'all fences are cleared')
  } finally { f.cleanup() }
  t.end()
})

test('certify validates corruption despite the unresolved-command override', (t) => {
  for (const kind of ['receipt', 'meta', 'no majority']) {
    const f = fixture().setup().finish()
    try {
      if (kind === 'receipt') {
        f.stores.forEach((store) => store.db.prepare('UPDATE monsterft_meta SET applied_local_digest = ?')
          .run(Buffer.alloc(16, 9)))
      } else if (kind === 'meta') {
        f.stores[2].db.exec('DELETE FROM monsterft_meta')
      } else {
        f.stores.forEach((store, i) => store.log.election(BigInt(i + 2), null))
      }
      f.close()
      t.throws(() => certify(f.base, true),
        (err) => err.code === Err.MONSTER_CORRUPT, `${kind} cannot be bypassed`)
    } finally { f.cleanup() }
  }
  t.end()
})

test('certify rejects missing files and held database locks', (t) => {
  const missing = fixture().setup()
  try {
    missing.close()
    fs.rmSync(missing.paths.procC)
    t.throws(() => certify(missing.base), (err) => err.code === Err.FS_ERROR)
    t.notOk(fs.existsSync(missing.paths.procC), 'missing DB1 is not recreated')
  } finally { missing.cleanup() }
  for (const role of [...roles, 'apply']) {
    const f = fixture().setup().finish()
    let lock = null
    try {
      f.close()
      const before = f.snapshot()
      lock = new DatabaseSync(f.paths[role])
      lock.exec('BEGIN IMMEDIATE')
      t.throws(() => certify(f.base), /locked/, `${role} must be offline`)
      lock.close()
      lock = null
      t.deepEqual(f.snapshot(), before, 'lock failure changes no DB1')
      t.equal(certify(f.base), 2n, 'released lock permits certification')
    } finally { lock?.close(); f.cleanup() }
  }
  t.throws(() => certify(':memory:'), /file path/)
  t.throws(() => certify('a', 'yes'), /must be a boolean/)
  t.end()
})

test('interrupted certification retains a majority after each durable phase', (t) => {
  const originalExec = DatabaseSync.prototype.exec
  for (const after of [false, true]) {
    for (let boundary = 1; boundary <= 4; boundary++) {
      const f = fixture().setup().finish()
      try {
        f.stores[2].log.append({ term: 2n, entry: Buffer.from('outlier') })
        f.stores[2].log.election(3n, 'b')
        f.close()
        let commits = 0
        DatabaseSync.prototype.exec = function(sql) {
          if (sql === 'COMMIT' && ++commits === boundary) {
            if (after) { originalExec.call(this, sql) }
            throw new Error('injected certification commit')
          }
          return originalExec.call(this, sql)
        }
        t.throws(() => certify(f.base), /injected certification commit/,
          `interrupted ${after ? 'after' : 'before'} commit ${boundary}`)
        DatabaseSync.prototype.exec = originalExec
        const digests = f.snapshot().map(({ state }) => stateDigest(state))
        t.ok(digests.some((digest) => digests.filter((other) => digest.equals(other)).length >= 2),
          'interruption leaves a combined majority')
        t.equal(certify(f.base), 2n, 'repeated certification finishes')
        t.ok(f.snapshot().every(({ state }) => state.meta.repair_state === 0n && state.head.seq === 2n),
          'all copies converge to certified history')
      } finally { DatabaseSync.prototype.exec = originalExec; f.cleanup() }
    }
  }
  t.end()
})

test('certify imports completed setup without introducing pending CMD state', (t) => {
  const f = fixture().setup()
  try {
    for (const store of f.stores) {
      store.db.exec(`UPDATE monsterft_meta SET applied_seq = -1,
        applied_entry_hash = NULL, applied_local_digest = NULL`)
    }
    f.close()
    t.equal(certify(f.base), 0n)
    t.ok(roles.every((role) => readDatabase(f.paths[role], readMeta).pending_cmd_seq === null))
    t.equal(readDatabase(f.paths.apply, readApply).seq, 0n)
  } finally { f.cleanup() }
  t.end()
})
