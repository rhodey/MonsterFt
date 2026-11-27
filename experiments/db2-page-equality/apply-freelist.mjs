// Reproduce the SQLite-only rollback/free-page case through production _execute.
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { execFileSync } from 'node:child_process'
import { DatabaseSync } from 'node:sqlite'
import { MonsterApply } from '../../src/apply.js'
import { checkGeneration } from '../../src/utils.js'

const here = dirname(fileURLToPath(import.meta.url))
const work = mkdtempSync(join(tmpdir(), 'monsterft-apply-freelist-'))
const output = process.argv[2] ?? join(here, 'apply-freelist-results.json')
const hash = s => createHash('sha256').update(s).digest().subarray(0, 16)
const commands = JSON.parse(execFileSync('python3', ['-c',
  'import sys,json,random; sys.path.insert(0,sys.argv[1]); from lifecycle import workload_statements; r=random.Random(0xDB2); print(json.dumps([workload_statements(i,r) for i in range(1,161)]))', here],
  { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 }))
const snap = (path) => JSON.parse(execFileSync('python3', [join(here,'sqlite_probe.py'),'snapshot',path,'--pages'],
  { encoding: 'utf8', maxBuffer: 8 * 1024 * 1024 }))
const extraSchema = `CREATE TABLE items(id INTEGER PRIMARY KEY,k INTEGER NOT NULL,v BLOB NOT NULL);
CREATE INDEX items_k ON items(k);
CREATE TABLE labels(k TEXT PRIMARY KEY,v BLOB) WITHOUT ROWID;
CREATE TABLE history(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT);
CREATE TABLE unique_values(id INTEGER PRIMARY KEY,v TEXT UNIQUE);
INSERT INTO unique_values VALUES(1,'occupied');`
async function callback(db, item) {
  if (item === null) { db.exec(extraSchema); return null }
  db.exec(item)
  return null
}
function open(r, initial=false) {
  r.token = hash(`${r.id}:${r.opens++}`)
  const db = r.db = new DatabaseSync(r.path, { readBigInts: true })
  db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; PRAGMA cache_size=2000; PRAGMA cache_spill=ON; PRAGMA secure_delete=OFF;')
  assert.equal(db.prepare('PRAGMA journal_mode').get().journal_mode, 'wal')
  assert.equal(db.prepare('PRAGMA synchronous').get().synchronous, 2n)
  db.exec('BEGIN IMMEDIATE')
  if (initial) {
    db.exec(`CREATE TABLE monsterft_local_owner(id INTEGER PRIMARY KEY CHECK(id=1),generation BLOB NOT NULL CHECK(length(generation)=16)) STRICT;
      CREATE TABLE monsterft_meta_apply(id INTEGER PRIMARY KEY CHECK(id=1),seq INTEGER NOT NULL,entry_hash BLOB,local_digest BLOB) STRICT;
      INSERT INTO monsterft_meta_apply VALUES(1,-1,NULL,NULL);`)
    db.prepare('INSERT INTO monsterft_local_owner VALUES(1,?)').run(r.token)
  } else db.prepare('UPDATE monsterft_local_owner SET generation=? WHERE id=1').run(r.token)
  db.exec('COMMIT')
  r.worker = new MonsterApply(r.id, () => {}, r.path, callback)
  r.worker.db = db
  r.worker.on('warn', () => r.rollbacks++)
}
async function execute(r, seq, items) {
  r.db.exec('BEGIN IMMEDIATE')
  checkGeneration(r.db,'monsterft_local_owner',r.token)
  return r.worker._execute({ seq: BigInt(seq), term: 7n, entryHash: hash(`entry${seq}`), items, matchIndex: [0n,0n,0n] })
}
function freePages(view) {
  const page = n => Buffer.from(view.pages[n], 'base64')
  const header = page(1)
  let trunk = header.readUInt32BE(32)
  const trunks = [], leaves = []
  while (trunk) {
    assert(!trunks.includes(trunk))
    trunks.push(trunk)
    const data = page(trunk), count = data.readUInt32BE(4)
    for (let i=0;i<count;i++) leaves.push(data.readUInt32BE(8+4*i))
    trunk = data.readUInt32BE(0)
  }
  assert.equal(trunks.length+leaves.length,header.readUInt32BE(36))
  return { trunks, leaves }
}
const results = []
for (const restarting of [false,true]) {
  const rs = [0,1,2].map(i => ({ id:String(i),path:join(work,`${restarting}-${i}.db`),opens:0,rollbacks:0 }))
  const result = { restarting, checkpoints:0, firstDifference:null }
  try {
    for (const r of rs) { open(r,true); await execute(r,0,[]) }
    const baseline = rs.map(r => snap(r.path))
    for (const v of baseline.slice(1)) assert.deepEqual(v.canonicalHashes,baseline[0].canonicalHashes)
    for (let seq=1;seq<=160;seq++) {
      if (restarting) {
        for (const [i,period] of [[1,7],[2,11]]) if (seq%period===0) {
          await rs[i].worker.close(); open(rs[i])
        }
      }
      const sql = commands[seq-1].join('')
      const items = [sql]
      if (seq%5===0) items.push("UPDATE items SET v=zeroblob(15000); INSERT INTO unique_values VALUES(2,'occupied');")
      const outcomes = []
      for (const r of rs) outcomes.push(await execute(r,seq,items))
      for (const value of outcomes.slice(1)) assert.deepEqual(value,outcomes[0], 'production outcomes and receipt differ')
      // No checkpoint skew or extra pinned reader: only restart schedules differ.
      const views = rs.map(r => snap(r.path))
      result.checkpoints++
      const different = Object.keys(views[0].canonicalHashes).filter(p => views.some(v=>v.canonicalHashes[p]!==views[0].canonicalHashes[p]))
      if (different.length || views.some(v=>v.pageCount!==views[0].pageCount)) {
        const frees = views.map(freePages)
        result.firstDifference = { seq, pages:views.map(v=>v.pageCount),differentPages:different,
          restarts:rs.map(r=>r.opens-1), allDifferencesFreeLeaves:different.every(p=>frees.every(f=>f.leaves.includes(Number(p)))),
          bytes:different.map(p=>({page:p, offsets:[...Buffer.from(views[0].pages[p],'base64').keys()].filter(i=>views.some(v=>Buffer.from(v.pages[p],'base64')[i]!==Buffer.from(views[0].pages[p],'base64')[i]))})),
          digestsMatched:true }
        break
      }
    }
    result.restarts = rs.map(r=>r.opens-1)
    result.integrity = rs.map(r=>r.db.prepare('PRAGMA integrity_check').all())
    for (const checks of result.integrity) assert.deepEqual(checks.map(x=>x.integrity_check),['ok'])
    result.paths = rs.map(r=>r.path)
    results.push(result)
  } finally {
    for (const r of rs) if(r.worker) await r.worker.close()
  }
}
assert.equal(results[0].firstDifference,null,'no-restart control')
// A change in the SQLite build can make this negative experiment stop reproducing.
assert(results[1].firstDifference,'restart counterexample did not reproduce; inspect runtime')
assert(results[1].firstDifference.allDifferencesFreeLeaves)
const evidence = { node:process.version,sqlite:process.versions.sqlite,workDirectory:work,
  method:'Production MonsterApply._execute with proposed schema; same cache/SQLite configuration; only unequal restart counts; external DBPAGE snapshots.',results }
writeFileSync(output,JSON.stringify(evidence,(_,v)=>typeof v==='bigint'?String(v):v,2)+'\n')
console.log(JSON.stringify(evidence))
