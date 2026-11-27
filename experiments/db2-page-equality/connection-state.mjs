// Exploratory probe, not a production repair implementation.
// Run with the DBPAGE-enabled library used by sqlite_probe.py.
import assert from 'node:assert/strict'
import { DatabaseSync } from 'node:sqlite'
import { createHash } from 'node:crypto'
import { mkdirSync, mkdtempSync, copyFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'

const here = dirname(fileURLToPath(import.meta.url))
const arg = (name, fallback) => {
  const index = process.argv.indexOf(name)
  return index < 0 ? fallback : process.argv[index + 1]
}
const work = arg('--work-dir', null) ?? mkdtempSync(join(tmpdir(), 'monsterft-connection-state-'))
mkdirSync(work, { recursive: true })
const output = arg('--output', join(here, 'connection-results.json'))
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex')
const encode = (value) => JSON.stringify(value, (_, item) =>
  typeof item === 'bigint' ? item.toString() : item)
const token = (n) => Buffer.alloc(16, n)

function snapshot(path) {
  const result = spawnSync('python3', [join(here, 'sqlite_probe.py'), 'snapshot', path, '--pages'], { encoding: 'utf8' })
  if (result.status !== 0) throw new Error(result.stderr || result.stdout)
  const raw = JSON.parse(result.stdout)
  const pages = new Map(Object.entries(raw.pages).map(([n, value]) => [Number(n), Buffer.from(value, 'base64')]))
  return { ...raw, pages, root: raw.ownerPage, sqlite: raw.readerSQLite }
}

function canonical(snapshot, ownerTemplate) {
  const pages = new Map([...snapshot.pages].map(([n, page]) => [n, Buffer.from(page)]))
  pages.get(1).fill(0, 24, 28)
  pages.get(1).fill(0, 92, 96)
  pages.set(snapshot.root, Buffer.from(ownerTemplate))
  return pages
}

function compare(a, b, ownerTemplate) {
  assert.equal(a.root, b.root)
  const ac = canonical(a, ownerTemplate)
  const bc = canonical(b, ownerTemplate)
  const differences = []
  for (const n of new Set([...ac.keys(), ...bc.keys()])) {
    const ap = ac.get(n), bp = bc.get(n)
    if (!ap || !bp) { differences.push({ page: n, missing: ap ? 'b' : 'a' }); continue }
    if (ap.equals(bp)) continue
    const offsets = [...ap.keys()].filter((i) => ap[i] !== bp[i])
    differences.push({ page: n, count: offsets.length, offsets: offsets.slice(0, 64) })
  }
  return { equal: differences.length === 0, pages: [ac.size, bc.size], differences }
}

function open(path, secureDelete = false) {
  const db = new DatabaseSync(path, { readBigInts: true })
  db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
    PRAGMA wal_autocheckpoint=0; PRAGMA secure_delete=${secureDelete ? 'ON' : 'OFF'};`)
  assert.equal(db.prepare('PRAGMA journal_mode').get().journal_mode, 'wal')
  assert.equal(db.prepare('PRAGMA synchronous').get().synchronous, 2n)
  return db
}

function claim(db, value) {
  db.exec('BEGIN IMMEDIATE')
  assert.equal(db.prepare('UPDATE monsterft_local_owner SET generation=? WHERE id=1').run(value).changes, 1n)
  db.exec('COMMIT')
}

function command(db, expectedToken, seq, sql) {
  db.exec('BEGIN IMMEDIATE')
  assert.ok(Buffer.from(db.prepare('SELECT generation FROM monsterft_local_owner WHERE id=1').get().generation).equals(expectedToken))
  const before = db.prepare('SELECT last_insert_rowid() AS lastRowid, changes() AS changes, total_changes() AS totalChanges').get()
  const session = db.createSession({ db: 'main' })
  let patch
  db.exec('SAVEPOINT monsterft_item')
  db.exec(sql)
  db.exec('RELEASE monsterft_item')
  patch = Buffer.from(session.patchset())
  session.close()
  const digest = Buffer.from(hash(patch).slice(0, 32), 'hex')
  db.prepare('UPDATE monsterft_meta_apply SET seq=?,entry_hash=?,local_digest=? WHERE id=1')
    .run(BigInt(seq), Buffer.alloc(16, seq), digest)
  db.exec('COMMIT')
  return { before, patchBytes: patch.length, patchHash: hash(patch), result: 'ok' }
}

const seed = join(work, 'seed.db')
const initial = open(seed)
initial.exec(`
  CREATE TABLE monsterft_meta_apply(id INTEGER PRIMARY KEY CHECK(id=1),seq INTEGER NOT NULL,entry_hash BLOB,local_digest BLOB) STRICT;
  CREATE TABLE monsterft_local_owner(id INTEGER PRIMARY KEY CHECK(id=1),generation BLOB NOT NULL CHECK(length(generation)=16)) STRICT;
  CREATE TABLE items(id INTEGER PRIMARY KEY,v TEXT NOT NULL) STRICT;
  CREATE TABLE markers(id INTEGER PRIMARY KEY,v TEXT NOT NULL) STRICT;
`)
initial.prepare('INSERT INTO monsterft_local_owner VALUES(1,?)').run(token(90))
initial.exec('INSERT INTO monsterft_meta_apply VALUES(1,-1,NULL,NULL)')
const insertInitial = initial.prepare('INSERT INTO items VALUES(?,?)')
for (let id = 1; id < 50; id++) insertInitial.run(id, `v${id.toString().padStart(6, '0')}-` + 'x'.repeat(130))
const metadata = {
  node: process.version,
  writerSQLite: initial.prepare('SELECT sqlite_version() AS version').get().version,
  compileOptions: initial.prepare('PRAGMA compile_options').all().map((r) => r.compile_options),
  workDirectory: work,
  method: 'Standalone Node probe following Apply transaction/session/receipt order, not a full MonsterFt cluster; custom SQLite sqlite_dbpage reads in explicit transactions; owner singleton replaced by validated seed template; page1 counters masked',
}
initial.close()
const seedSnapshot = snapshot(seed)
const ownerTemplate = seedSnapshot.pages.get(seedSnapshot.root)
metadata.readerSQLite = seedSnapshot.sqlite

const cases = [
  { name: 'last_insert_rowid_intermediate', sql: "UPDATE items SET v=replace(hex(zeroblob(32)),'0',char(65+last_insert_rowid()%26)) WHERE id=25; UPDATE items SET v='x' WHERE id=25;", expectEqualPatch: true, expectEqualPages: false },
  { name: 'last_insert_rowid_intermediate_seven_restarts', restarts: 7, sql: "UPDATE items SET v=replace(hex(zeroblob(32)),'0',char(65+last_insert_rowid()%26)) WHERE id=25; UPDATE items SET v='x' WHERE id=25;", expectEqualPatch: true, expectEqualPages: false },
  { name: 'last_insert_rowid_intermediate_secure_delete', secureDelete: true, sql: "UPDATE items SET v=replace(hex(zeroblob(32)),'0',char(65+last_insert_rowid()%26)) WHERE id=25; UPDATE items SET v='x' WHERE id=25;", expectEqualPatch: true, expectEqualPages: true },
  { name: 'last_insert_rowid_allocation_secure_delete', secureDelete: true, sql: "UPDATE items SET v=hex(zeroblob(CASE WHEN last_insert_rowid()=0 THEN 8192 ELSE 16384 END)) WHERE id=25; UPDATE items SET v='x' WHERE id=25;", expectEqualPatch: true, expectEqualPages: false },
  { name: 'total_changes_intermediate', sql: "UPDATE items SET v=replace(hex(zeroblob(32)),'0',char(65+total_changes()%26)) WHERE id=25; UPDATE items SET v='x' WHERE id=25;", expectEqualPatch: true, expectEqualPages: false },
  { name: 'explicit_intermediate_control', sql: "UPDATE items SET v='0000000000000777' WHERE id=25; UPDATE items SET v='x' WHERE id=25;", expectEqualPatch: true, expectEqualPages: true },
  { name: 'changes_intermediate_control', sql: "UPDATE items SET v=printf('%016d',changes()) WHERE id=25; UPDATE items SET v='x' WHERE id=25;", expectEqualPatch: true, expectEqualPages: true },
  { name: 'last_insert_rowid_final_value_detected', sql: "UPDATE items SET v=printf('%016d',last_insert_rowid()) WHERE id=25;", expectEqualPatch: false, expectEqualPages: false },
  { name: 'automatic_rowid_control', sql: "INSERT INTO items(v) VALUES('automatic row id');", expectEqualPatch: true, expectEqualPages: true },
]

const results = []
for (const spec of cases) {
  const paths = ['a', 'b'].map((suffix) => join(work, `${spec.name}-${suffix}.db`))
  for (const path of paths) copyFileSync(seed, path)
  let [a, b] = paths.map((path) => open(path, spec.secureDelete))
  claim(a, token(17)); claim(b, token(34))
  const warmup = "INSERT INTO markers VALUES(777,'warmup'); DELETE FROM markers;"
  command(a, token(17), 1, warmup); command(b, token(34), 1, warmup)
  const beforeRestart = compare(snapshot(paths[0]), snapshot(paths[1]), ownerTemplate)
  assert.ok(beforeRestart.equal, spec.name + ' matching baseline')
  const restarts = spec.restarts ?? 1
  let ownerB = token(34)
  for (let i = 0; i < restarts; i++) {
    b.close(); b = open(paths[1], spec.secureDelete)
    ownerB = token(35 + i)
    claim(b, ownerB)
  }
  const afterRestart = compare(snapshot(paths[0]), snapshot(paths[1]), ownerTemplate)
  assert.ok(afterRestart.equal, spec.name + ' restart preserves baseline')
  const ca = command(a, token(17), 2, spec.sql)
  const cb = command(b, ownerB, 2, spec.sql)
  const rowEqual = encode(a.prepare('SELECT * FROM items ORDER BY id').all()) === encode(b.prepare('SELECT * FROM items ORDER BY id').all())
  const afterCommand = compare(snapshot(paths[0]), snapshot(paths[1]), ownerTemplate)
  const entry = {
    name: spec.name, secureDelete: !!spec.secureDelete, restartCounts: [0, restarts],
    sql: spec.sql, beforeRestart, afterRestart, commands: [ca, cb],
    rowsEqual: rowEqual, patchsetsEqual: ca.patchHash === cb.patchHash,
    outcomesEqual: ca.result === cb.result, afterCommand, paths,
  }
  results.push(entry)
  a.close(); b.close()
  console.log(encode({ name: spec.name, rowsEqual: rowEqual, before: [ca.before, cb.before], patchsetsEqual: entry.patchsetsEqual, pagesEqual: afterCommand.equal, differences: afterCommand.differences }))
  assert.equal(entry.patchsetsEqual, spec.expectEqualPatch, spec.name + ' expected patchset relation')
  assert.equal(afterCommand.equal, spec.expectEqualPages, spec.name + ' expected page relation')
}

writeFileSync(output, JSON.stringify(JSON.parse(encode({ metadata, results })), null, 2) + '\n')
console.log(`Wrote ${output}`)
