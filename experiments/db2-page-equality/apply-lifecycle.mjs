// Runs the production MonsterApply._execute path against the proposed schema.
// Startup/open is experimental glue; production modules are unchanged.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { execFileSync } from 'node:child_process'
import { DatabaseSync } from 'node:sqlite'
import { MonsterApply } from '../../src/apply.js'
import { checkGeneration } from '../../src/utils.js'

const here = dirname(fileURLToPath(import.meta.url))
const work = mkdtempSync(join(tmpdir(), 'monsterft-db2-apply-'))
const output = process.argv[2] ?? join(here, 'apply-results.json')
const digest = (value) => createHash('sha256').update(value).digest()
const token = (id, n) => digest(`${id}:${n}`).subarray(0, 16)
const snapshot = (path) => JSON.parse(execFileSync('python3',
  [join(here, 'sqlite_probe.py'), 'snapshot', path], { encoding: 'utf8' }))

function schema(db, generation) {
  db.exec(`CREATE TABLE monsterft_meta_apply (
    id INTEGER PRIMARY KEY CHECK(id=1), seq INTEGER NOT NULL,
    entry_hash BLOB, local_digest BLOB,
    CHECK((seq=-1 AND entry_hash IS NULL AND local_digest IS NULL) OR
      (seq>=0 AND length(entry_hash)=16 AND length(local_digest)=16))
  ) STRICT;
  INSERT INTO monsterft_meta_apply VALUES(1,-1,NULL,NULL);
  CREATE TABLE monsterft_local_owner (
    id INTEGER PRIMARY KEY CHECK(id=1),
    generation BLOB NOT NULL CHECK(length(generation)=16)
  ) STRICT;`)
  db.prepare('INSERT INTO monsterft_local_owner VALUES(1,?)').run(generation)
}

const seedSchema = `
  CREATE TABLE items(id INTEGER PRIMARY KEY, k TEXT NOT NULL, n INTEGER NOT NULL, payload BLOB);
  CREATE INDEX items_k ON items(k,n);
  CREATE TABLE keyed(k TEXT PRIMARY KEY,v TEXT);
  CREATE TABLE packed(k TEXT PRIMARY KEY,n INTEGER,v BLOB) WITHOUT ROWID;
  CREATE TABLE serial(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT);
  CREATE TABLE scratch(id INTEGER PRIMARY KEY,v BLOB);
`

async function apply(db, cmd) {
  if (cmd === null) {
    db.exec(seedSchema)
    return null
  }
  const { kind, round } = cmd
  if (kind === 'insert') {
    const put = db.prepare('INSERT INTO items VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET k=excluded.k,n=excluded.n,payload=excluded.payload')
    const key = db.prepare('INSERT INTO keyed VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v')
    const packed = db.prepare('INSERT INTO packed VALUES(?,?,?) ON CONFLICT(k) DO UPDATE SET n=excluded.n,v=excluded.v')
    for (let i = 0; i < 12; i++) {
      const id = (round * 13 + i * 7) % 700 + 1
      const payload = Buffer.alloc(80 + (id * 37 % 700), id % 251)
      put.run(id, `k${id % 17}`, round, payload)
      key.run(`key${id}`, `value${round}`)
      packed.run(`key${id}`, round, payload)
    }
    return round
  }
  if (kind === 'update') {
    const update = db.prepare('UPDATE items SET payload=?,n=? WHERE id=?')
    for (let i = 0; i < 9; i++) {
      const id = (round * 31 + i * 11) % 700 + 1
      update.run(Buffer.alloc((round % 3 ? 37 : 14000) + i, (round + i) % 251), round, id)
    }
    return round
  }
  if (kind === 'delete') {
    for (const table of ['items', 'keyed', 'packed']) {
      const column = table === 'items' ? 'id' : 'k'
      const stmt = db.prepare(`DELETE FROM ${table} WHERE ${column}=?`)
      for (let i = 0; i < 8; i++) {
        const id = (round * 19 + i * 29) % 700 + 1
        stmt.run(table === 'items' ? id : `key${id}`)
      }
    }
    return round
  }
  if (kind === 'serial') {
    const stmt = db.prepare('INSERT INTO serial(v) VALUES(?)')
    for (let i = 0; i < 5; i++) stmt.run(`v${round}:${i}`)
    db.prepare('DELETE FROM serial WHERE id < ?').run(round)
    return round
  }
  if (kind === 'rollback') {
    const stmt = db.prepare('INSERT OR REPLACE INTO scratch VALUES(?,?)')
    for (let i = 0; i < 70; i++) stmt.run(i, Buffer.alloc(6000, i))
    db.prepare('UPDATE items SET payload=?').run(Buffer.alloc(13000, 91))
    throw new Error('intentional deterministic savepoint rollback')
  }
  if (kind === 'empty') return round
  throw new Error(`unknown command ${kind}`)
}

class Replica {
  constructor(id, index) {
    this.id = id
    this.index = index
    this.path = join(work, `${id}.db`)
    this.opens = 0
    this.checkpoints = 0
    this.warnings = 0
    this.open(true)
  }
  open(initial = false) {
    this.generation = token(this.id, this.opens++)
    this.db = new DatabaseSync(this.path, { readBigInts: true })
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
      PRAGMA secure_delete=OFF; PRAGMA auto_vacuum=NONE;
      PRAGMA wal_autocheckpoint=${[0, 1, 7][this.index]};
      PRAGMA cache_size=${[-2048, 8, 64][this.index]};`)
    assert.equal(this.db.prepare('PRAGMA journal_mode').get().journal_mode, 'wal')
    assert.equal(this.db.prepare('PRAGMA synchronous').get().synchronous, 2n)
    this.db.exec('BEGIN IMMEDIATE')
    if (initial) schema(this.db, this.generation)
    else this.db.prepare('UPDATE monsterft_local_owner SET generation=? WHERE id=1').run(this.generation)
    this.db.exec('COMMIT')
    this.worker = new MonsterApply(this.id, () => {}, this.path, apply)
    this.worker.db = this.db
    this.worker.on('warn', () => this.warnings++)
  }
  async restart() {
    await this.worker.close()
    this.open()
  }
  async execute(seq, items) {
    this.db.exec('BEGIN IMMEDIATE')
    checkGeneration(this.db, 'monsterft_local_owner', this.generation)
    return this.worker._execute({ seq, term: 7n, entryHash: digest(`entry:${seq}`).subarray(0, 16), items, matchIndex: [seq, seq, seq] })
  }
  checkpoint() {
    const result = this.db.prepare('PRAGMA wal_checkpoint(PASSIVE)').get()
    this.checkpoints++
    return result
  }
}

const replicas = [new Replica('persistent', 0), new Replica('frequent', 1), new Replica('staggered', 2)]
const evidence = {
  workDirectory: work,
  runtime: { node: process.version, sqlite: process.versions.sqlite, v8: process.versions.v8,
    compileOptions: replicas[0].db.prepare('PRAGMA compile_options').all().map(x => x.compile_options) },
  method: 'Production MonsterApply._execute; experimental owner schema/startup guard; external sqlite_dbpage read transactions; no patchset application.',
  checkpoints: [],
}
function compare(label, seq) {
  const views = replicas.map(r => snapshot(r.path))
  const differing = (field) => [...new Set(views.flatMap(v => Object.keys(v[field])))].filter(p =>
    views.some(v => v[field][p] !== views[0][field][p]))
  const canonicalDifferences = differing('canonicalHashes')
  const row = { label, seq: String(seq), pages: views.map(v => v.pageCount),
    opens: replicas.map(r => r.opens), ownerPages: views.map(v => v.ownerPage),
    rawDifferentPages: differing('rawHashes'), canonicalDifferentPages: canonicalDifferences,
    headerCounters: views.map(v => v.headerCounters), readerSQLite: views[0].readerSQLite,
    writerSQLiteNumber: views[0].writerSQLiteNumber }
  evidence.checkpoints.push(row)
  assert.deepEqual(canonicalDifferences, [], JSON.stringify(row))
}

let held
try {
  for (const replica of replicas) await replica.execute(0n, [])
  compare('initial matching baseline', 0)
  const kinds = ['insert', 'insert', 'update', 'delete', 'serial', 'rollback', 'empty']
  for (let round = 1; round <= 280; round++) {
    if (round % 7 === 0) await replicas[1].restart()
    if ([11, 48, 109, 210].includes(round)) await replicas[2].restart()
    const items = [{ kind: kinds[round % kinds.length], round }, { kind: 'insert', round: round + 1000 }]
    const seq = BigInt(round * 2 - 1)
    const results = []
    for (const replica of replicas) results.push(await replica.execute(seq, items))
    for (const result of results.slice(1)) {
      assert.deepEqual(result.receipt, results[0].receipt, 'production receipt/digest differs')
      assert.deepEqual(result.outcomes, results[0].outcomes, 'production outcomes differ')
    }
    if (round === 21) {
      held = new DatabaseSync(replicas[2].path, { readBigInts: true })
      held.exec('BEGIN')
      const old = held.prepare('SELECT seq FROM monsterft_meta_apply').get().seq
      evidence.heldSnapshotSeq = String(old)
    }
    if (round === 200) {
      assert.equal(String(held.prepare('SELECT seq FROM monsterft_meta_apply').get().seq), evidence.heldSnapshotSeq)
      held.exec('COMMIT')
      held.close()
      held = null
    }
    if (round % 5 === 0) replicas[0].checkpoint()
    if (round % 13 === 0) replicas[2].checkpoint()
    if (round % 14 === 0 || [1, 63, 64, 65, 127, 128, 129].includes(round)) compare(`round ${round}`, seq)
  }
  for (const seq of [32767n, 32768n, 8388607n, 8388608n, 2147483647n, 2147483648n, 140737488355327n, 140737488355328n]) {
    await replicas[1].restart()
    const results = []
    for (const replica of replicas) results.push(await replica.execute(seq, [{ kind: 'empty', round: 999 }]))
    for (const result of results.slice(1)) assert.deepEqual(result.receipt, results[0].receipt)
    compare('receipt integer-width boundary', seq)
  }
  evidence.replicas = replicas.map(r => ({ id: r.id, opens: r.opens, restarts: r.opens - 1,
    checkpoints: r.checkpoints, rolledBackItems: r.warnings,
    settings: Object.fromEntries(['journal_mode','synchronous','secure_delete','auto_vacuum','cache_size','wal_autocheckpoint'].map(p => [p, r.db.prepare(`PRAGMA ${p}`).get()[p]])),
    integrity: r.db.prepare('PRAGMA integrity_check').all() }))
  for (const replica of evidence.replicas) {
    assert.deepEqual(replica.integrity.map(row => row.integrity_check), ['ok'])
    assert.equal(replica.rolledBackItems, 40)
  }
  evidence.status = 'passed'
} catch (error) {
  evidence.status = 'failed'
  evidence.error = error.stack
  process.exitCode = 1
} finally {
  if (held) { held.exec('ROLLBACK'); held.close() }
  for (const r of replicas) await r.worker.close()
  writeFileSync(output, JSON.stringify(evidence, (_, v) => typeof v === 'bigint' ? String(v) : v, 2) + '\n')
}
console.log(JSON.stringify({ status: evidence.status, work, output, comparisons: evidence.checkpoints.length,
  opens: replicas.map(r => r.opens), error: evidence.error }))
