import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { MonsterApply } from '../src/apply.js'
import {
  ProtocolStore, databasePaths, openApplyDatabase, readDatabase, receiptDigest,
} from '../src/storage.js'
import { readMeta, readApply } from '../src/utils.js'
import { encodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const seed = Buffer.from('00112233445566778899aabbccddeeff', 'hex')
const nextSeed = Buffer.alloc(16, 9)
const roles = ['procA', 'procB', 'procC']
const tables = { procA: 'monsterft_meta', apply: 'monsterft_meta_apply' }

function fixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-generation-meta-'))
  const base = path.join(directory, 'node')
  const paths = databasePaths(base)
  const stores = roles.map((role) => new ProtocolStore(role, base))
  for (const store of stores) { store.open(seed) }
  const db2 = openApplyDatabase(base, seed)
  for (const store of stores) {
    store.log.append({ term: 7n, entry: Buffer.from('retained payload') })
    store.close()
  }
  db2.exec("CREATE TABLE items (value TEXT); INSERT INTO items VALUES ('preserved')")
  db2.close()
  return {
    base, paths,
    reopen(role, generation = nextSeed) {
      if (role === 'apply') { openApplyDatabase(base, generation).close(); return }
      const store = new ProtocolStore(role, base)
      try { store.open(generation) } finally { store.close() }
    },
    snapshot(role) {
      return readDatabase(paths[role], (db) => ({
        schema: db.prepare('SELECT sql FROM sqlite_schema ORDER BY name').all(),
        meta: db.prepare(`SELECT * FROM ${tables[role]}`).all(),
        data: db.prepare(role === 'apply' ? 'SELECT * FROM items' : 'SELECT * FROM raft_log').all(),
      }))
    },
    close() { fs.rmSync(directory, { recursive: true, force: true }) },
  }
}

test('generation bytes are stored separately from protocol meta and Apply receipts', (t) => {
  const f = fixture()
  try {
    for (const role of [...roles, 'apply']) {
      const table = role === 'apply' ? tables.apply : tables.procA
      const bytes = readDatabase(f.paths[role], (db) =>
        db.prepare(`SELECT generation FROM ${table}`).get().generation)
      t.deepEqual(Buffer.from(bytes), seed, `${role}: exact shared seed is durable`)
    }
    const meta = readDatabase(f.paths.procA, readMeta)
    const receipt = readDatabase(f.paths.apply, readApply)
    t.notOk(Object.hasOwn(meta, 'generation'), 'protocol meta omits generation')
    t.notOk(Object.hasOwn(receipt, 'generation'), 'Apply receipt omits generation')
    const digest = receiptDigest(receipt)
    f.reopen('procA')
    f.reopen('apply')
    t.deepEqual(readDatabase(f.paths.procA, readMeta), meta, 'new generation preserves protocol meta')
    const nextReceipt = readDatabase(f.paths.apply, readApply)
    t.deepEqual(nextReceipt, receipt, 'new generation preserves Apply receipt')
    t.deepEqual(receiptDigest(nextReceipt), digest, 'receipt digest excludes generation')
  } finally { f.close() }
  t.end()
})

test('missing generation columns reject reopening without changing established state', (t) => {
  for (const role of Object.keys(tables)) {
    const f = fixture()
    try {
      const db = new DatabaseSync(f.paths[role])
      try { db.exec(`ALTER TABLE ${tables[role]} DROP COLUMN generation`) } finally { db.close() }
      const before = f.snapshot(role)
      t.throws(() => f.reopen(role), /generation/, `${role}: missing column rejects startup`)
      t.deepEqual(f.snapshot(role), before, `${role}: no replacement schema or state mutation`)
    } finally { f.close() }
  }
  t.end()
})

test('corrupt stored generations are rejected instead of replaced during startup', (t) => {
  for (const role of Object.keys(tables)) {
    for (const value of [Buffer.alloc(15, 1), Buffer.alloc(17, 1), Buffer.alloc(16), null]) {
      const f = fixture()
      try {
        const db = new DatabaseSync(f.paths[role])
        try {
          db.exec(`
            ALTER TABLE ${tables[role]} RENAME TO saved;
            CREATE TABLE ${tables[role]} AS SELECT * FROM saved;
            DROP TABLE saved;
          `)
          db.prepare(`UPDATE ${tables[role]} SET generation = ?`).run(value)
        } finally { db.close() }
        const before = f.snapshot(role)
        t.throws(() => f.reopen(role), (err) => err.code === Err.MONSTER_CORRUPT,
          `${role}: invalid generation is corruption`)
        t.deepEqual(f.snapshot(role), before, 'failed startup does not heal or modify stored state')
      } finally { f.close() }
    }
  }
  t.end()
})

test('Apply rejects invalid hello seeds before creating a database', async (t) => {
  for (const value of [undefined, null, 'seed', Buffer.alloc(15, 1), Buffer.alloc(16)]) {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-generation-hello-'))
    const base = path.join(directory, 'node')
    const errors = []
    let sends = 0
    const apply = new MonsterApply('a', () => { sends++ }, base, { apply() {} })
    apply.on('error', (err) => errors.push(err))
    try {
      const message = { type: 'hello', cid: 'hello' }
      if (value !== undefined) { message.seed = value }
      await apply.onReceive('gate', encodeFrame(message))
      t.equal(errors[0]?.code, Err.RPC_ILLEGAL, 'invalid seed reports a local protocol error')
      t.equal(sends, 0, 'invalid hello is not acknowledged')
      t.deepEqual(fs.readdirSync(directory), [], 'invalid hello creates no database files')
    } finally {
      await apply.close()
      fs.rmSync(directory, { recursive: true, force: true })
    }
  }
  t.end()
})
