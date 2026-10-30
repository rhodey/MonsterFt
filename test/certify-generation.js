import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { certify, MonsterProc, MonsterApply } from '../src/index.js'
import { ProtocolStore, openApplyDatabase, databasePaths, readDatabase, readState, stateDigest } from '../src/storage.js'
import { digestEntry } from '../src/utilm.js'
import { encodeFrame, decodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const roles = ['procA', 'procB', 'procC']
const seed = Buffer.alloc(16, 1)
const table = (role) => role === 'apply' ? 'monsterft_meta_apply' : 'monsterft_meta'

function fixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-certify-generation-'))
  const base = path.join(directory, 'node')
  const paths = databasePaths(base)
  const stores = roles.map((role) => new ProtocolStore(role, base))
  for (const store of stores) { store.open(seed) }
  const db2 = openApplyDatabase(base, seed)
  return {
    base, paths, stores, db2,
    snapshot() {
      return Object.fromEntries(Object.entries(paths).map(([role, file]) => [role,
        readDatabase(file, (db) => db.prepare(`SELECT * FROM ${table(role)}`).get()),
      ]))
    },
    close() {
      for (const store of stores) { store.close() }
      if (db2.isOpen) { db2.close() }
    },
    cleanup() {
      this.close()
      fs.rmSync(directory, { recursive: true, force: true })
    },
  }
}

test('certify remains offline and rejects idle component connections', async (t) => {
  for (const role of [...roles, 'apply']) {
    const f = fixture()
    const replies = []
    const errors = []
    const send = (to, frame) => replies.push(decodeFrame(frame))
    const owner = role === 'apply'
      ? new MonsterApply('a', send, f.base, () => {})
      : new MonsterProc(role, 'a', ['a', 'b', 'c'], send, f.base)
    owner.on('error', (err) => errors.push(err))
    try {
      f.close()
      await owner.onReceive('gate', encodeFrame({ type: 'hello', cid: 'hello', seed }))
      await owner.onReceive('gate', encodeFrame({ type: 'open', cid: 'open' }))
      t.equal(replies.at(-1)?.cid, 'open', `${role} opened its database`)
      t.ok(owner.db?.isOpen && !owner.db.isTransaction,
        `${role} keeps an idle connection without an active transaction`)
      t.deepEqual(errors, [], 'owner starts normally')
      const before = f.snapshot()
      for (const allowUnresolved of [false, true]) {
        t.throws(() => certify(f.base, allowUnresolved),
          (err) => err.code === Err.SQLITE_ERROR && err.sqlCode === 5,
          `${role} prevents certification${allowUnresolved ? ' even with the override' : ''}`)
      }
      t.deepEqual(f.snapshot(), before, 'rejection changes neither metadata nor generations')
      await owner.close()
      t.equal(certify(f.base), -1n, 'closing the owner permits offline certification')
      t.deepEqual(f.snapshot(), before, 'certification does not rotate generations')
    } finally {
      await owner.close()
      f.cleanup()
    }
  }
  t.end()
})

test('certify preserves each generation while adopting an outlier and clearing repair fences', (t) => {
  const f = fixture()
  const setup = { term: 1n, entry: Buffer.alloc(0) }
  try {
    for (const store of f.stores) { store.log.append(setup) }
    f.db2.exec("CREATE TABLE items (value TEXT); INSERT INTO items VALUES ('preserved')")
    f.db2.prepare(`UPDATE monsterft_meta_apply SET seq = 0, entry_hash = ?, local_digest = ?`)
      .run(digestEntry(setup.term, setup.entry), Buffer.alloc(16))
    for (const phase of ['capture', 'adopt', 'reconcile']) {
      for (const store of f.stores) { store[phase]() }
    }
    for (let i = 0; i < f.stores.length; i++) {
      f.stores[i].db.prepare(`UPDATE monsterft_meta SET repair_state = ${Err.REPAIR_NO_QUORUM}, generation = ?`)
        .run(Buffer.alloc(16, i + 1))
    }
    f.stores[2].log.append({ term: 2n, entry: Buffer.from('outlier') })
    f.db2.prepare('UPDATE monsterft_meta_apply SET generation = ?').run(Buffer.alloc(16, 4))
    f.close()
    const before = f.snapshot()
    t.equal(certify(f.base), 0n, 'different generations do not prevent protocol majority selection')
    const after = f.snapshot()
    for (const role of [...roles, 'apply']) {
      t.deepEqual(after[role].generation, before[role].generation,
        `${role} retains its own generation`)
    }
    t.deepEqual(after.apply, before.apply, 'Apply receipt and generation remain unchanged')
    const states = roles.map((role) => readDatabase(f.paths[role], readState))
    t.ok(states.every((state) => state.meta.repair_state === 0n && state.head.seq === 0n),
      'outlier history is replaced and all repair fences are cleared')
    t.ok(states.every((state) => stateDigest(state).equals(stateDigest(states[0]))),
      'certified protocol states agree despite different local generations')
    t.deepEqual(readDatabase(f.paths.apply, (db) =>
      db.prepare('SELECT value FROM items').all().map((row) => row.value)), ['preserved'],
      'certification leaves Apply data unchanged')
  } finally { f.cleanup() }
  t.end()
})
