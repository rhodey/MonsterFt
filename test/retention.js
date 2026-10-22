import test from 'tape'
import { DatabaseSync } from 'node:sqlite'
import { pack } from 'msgpackr'
import { SQLiteLog } from '../src/log.js'
import { readApply } from '../src/utils.js'
import { digestEntry } from '../src/utilm.js'
import { protocol, append, record, cmd, messages } from './fixtures/protocol.js'
import * as Err from '../src/error.js'

const digest = Buffer.alloc(16, 7)
const options = () => ({ keepTarget: 3, keepTrigger: 6 })
function noops(f, count, commit = true) {
  const head = f.proc.log.seq
  return f.rpc(append({
    term: f.proc.engine.term, termP: f.proc.log.term, seqP: head,
    commitSeq: commit ? head + BigInt(count) : f.proc.engine.commitSeq,
    data: Array.from({ length: count }, () => record(f.proc.engine.term)),
  }))
}
function command(f, count = 1) {
  const head = f.proc.log.seq
  const r = f.rpc(append({
    seqP: head, termP: f.proc.log.term, commitSeq: head + 1n,
    data: [record(1n, cmd(Array.from({ length: count }, () => Buffer.from('item'))))],
  }))
  return messages(r, 'execute', 'apply')[0]
}
function sync(f, quorum = true) {
  const head = f.proc.log.seq
  return f.rpc(append({
    seqP: head, termP: f.proc.log.term, term: f.proc.engine.term, commitSeq: head + 1n,
    data: [record(f.proc.engine.term, pack({
      type: 'sync', cmdSeq: f.proc.engine.pendingCommand.cmdSeq, quorum,
      agree: quorum ? ['a', 'b'] : [], disagree: quorum ? [] : ['a', 'b', 'c'],
      digest: quorum ? digest : null,
    }))],
  }))
}
function editAll(f, sql) {
  for (const key of ['procA', 'procB', 'procC']) {
    const db = new DatabaseSync(f.paths[key])
    try { db.exec(sql) } finally { db.close() }
  }
}

test('retention uses exact applied-entry thresholds with no queries below the trigger', (t) => {
  for (const enabled of [false, true]) {
    const f = protocol(enabled ? options() : {}).setup()
    try {
      const prepared = []
      let refreshed = 0
      const db = f.proc.db
      const prepare = db.prepare.bind(db)
      db.prepare = (sql, ...args) => { prepared.push(sql); return prepare(sql, ...args) }
      const readHead = f.proc.log.readHead.bind(f.proc.log)
      f.proc.log.readHead = () => { refreshed++; return readHead() }
      // Append refreshes the head itself. Isolate pruning by committing an
      // already stored page one entry at a time with matching duplicate pages.
      noops(f, 12, false)
      refreshed = 0
      prepared.length = 0
      for (let seq = 1n; seq <= 12n; seq++) {
        const before = refreshed
        const r = f.rpc(append({ seqP: seq - 1n, termP: 1n, data: [record()], commitSeq: seq }))
        t.equal(r.type, 'ack')
        const pruned = enabled && [5n, 8n, 11n].includes(seq)
        t.equal(refreshed - before, pruned ? 1 : 0, `checkpoint ${seq}: boundary refresh only after pruning`)
        const begin = enabled && seq >= 5n ? ((seq - 5n) / 3n) * 3n + 3n : 0n
        t.equal(f.proc.log.begin, begin)
        t.equal(f.proc.log.seq, 12n, 'unapplied tail is kept')
      }
      t.equal(prepared.filter((sql) => /DELETE FROM raft_log/i.test(sql)).length, enabled ? 3 : 0)
      t.notOk(prepared.some((sql) => /count\s*\(/i.test(sql)), 'retention never queries row count')
    } finally { f.close() }
  }
  t.end()
})

test('pending batches pin history through no-ops, restart and election until SYNC', (t) => {
  const f = protocol(options()).setup()
  try {
    const execute = command(f, 20)
    f.complete(execute, Array.from({ length: 20 }, () => [0, 'ok']), digest)
    t.equal(f.proc.log.seq, 1n, 'twenty items occupy a single CMD entry')
    noops(f, 7)
    t.equal(f.proc.log.begin, 0n, 'unresolved CMD protects all retained history')
    f.reopen()
    t.equal(f.proc.log.begin, 0n, 'startup cannot prune pending history')
    const election = f.elect()
    const page = messages(election, 'append', 'b').find((msg) => msg.data)
    const r = f.rpc({ type: 'ack', cid: page.cid, term: page.term, seq: 9n }, 'b')
    t.equal(messages(r, 'execute').length, 0, 'election no-op does not reexecute the batch')
    t.equal(f.proc.log.begin, 0n)
    f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest }, 'b')
    const state = f.proc.engine.replication.get('b')
    f.rpc({ type: 'ack', cid: state.cid, term: f.proc.engine.term, seq: f.proc.log.seq }, 'b')
    t.equal(f.proc.engine.pendingCommand, null)
    t.equal(f.proc.log.begin, f.proc.engine.appliedSeq - 2n, 'SYNC releases the pin and leaves target history')
    t.equal(readApply(f.db2).seq, 1n)
    f.reopen()
    t.equal(f.proc.engine.pendingApply, null, 'pruned receipt does not cause reexecution')
  } finally { f.close() }
  t.end()
})

test('slow Apply retains its unapplied tail and startup prunes only validated applied history', (t) => {
  const opts = {}
  const f = protocol(opts).setup()
  try {
    const execute = command(f)
    noops(f, 10)
    const tail = f.rows().filter((row) => row.seq > 1n)
    t.equal(f.proc.engine.appliedSeq, 0n, 'stored rows are not applied history')
    t.equal(f.proc.log.begin, 0n)
    f.complete(execute, [[0, 'ok']], digest)
    sync(f)
    t.equal(f.proc.log.begin, 0n, 'retention initially disabled')
    noops(f, 5, false)
    const unapplied = f.rows().filter((row) => row.seq > f.proc.engine.appliedSeq)
    Object.assign(opts, options())
    f.reopen()
    t.equal(f.proc.log.begin, 10n, 'startup leaves three applied entries')
    t.deepEqual(f.rows().filter((row) => row.seq > 12n), unapplied, 'startup preserves the exact unapplied suffix')
    t.deepEqual(f.rows().filter((row) => row.seq >= 10n && row.seq <= 11n),
      tail.filter((row) => row.seq >= 10n), 'retained payloads stay unchanged')
    t.equal(readApply(f.db2).seq, 1n, 'receipt may refer to legitimately pruned history')
  } finally { f.close() }
  const pinned = protocol(options()).setup()
  try {
    const execute = command(pinned)
    noops(pinned, 8)
    const before = pinned.rows()
    t.equal(pinned.proc.log.begin, 0n, 'slow Apply cannot trigger pruning from stored row count')
    pinned.complete(execute, [[0, 'ok']], digest)
    t.deepEqual(pinned.rows(), before, 'pending agreement still protects the whole tail')
    sync(pinned, false)
    t.equal(pinned.proc.engine.repairState, 1)
    noops(pinned, 4)
    t.ok(pinned.proc.log.begin > 0n, 'nonfatal repair fence allows no-op retention')
    t.equal(pinned.proc.isOpen, true)
  } finally { pinned.close() }
  t.end()
})

test('runtime pruning and SYNC checkpoint failures roll back the entire input', (t) => {
  for (const fault of ['delete', 'head refresh', 'sync checkpoint']) {
    const f = protocol(options()).setup()
    try {
      const execute = command(f)
      f.complete(execute, [[0, 'ok']], digest)
      noops(f, 4)
      const before = f.state()
      for (const proc of f.procs) {
        if (fault === 'head refresh') {
          const readHead = proc.log.readHead.bind(proc.log)
          proc.log.readHead = () => {
            if (proc.db.prepare('SELECT min(seq) AS seq FROM raft_log').get().seq > 0n) {
              throw new Error('retention refresh failed')
            }
            readHead()
          }
        } else {
          proc.db.exec(fault === 'delete'
            ? `CREATE TRIGGER retention_fault BEFORE DELETE ON raft_log
               BEGIN SELECT RAISE(ABORT, 'retention delete failed'); END`
            : `CREATE TRIGGER retention_fault BEFORE UPDATE ON monsterft_meta
               BEGIN SELECT RAISE(ABORT, 'SYNC checkpoint failed'); END`)
        }
      }
      t.equal(sync(f), undefined, `${fault}: no success completion`)
      t.deepEqual(f.state(), before, 'checkpoint, SYNC insertion and deletions roll back together')
      t.equal(readApply(f.db2).seq, 1n, 'completed Apply receipt survives')
      t.ok(f.diagnostics.some(({ type, error }) => type === 'error' &&
        /retention|SYNC checkpoint/.test(error.message)), 'owner reports injected failure')
      f.stop()
      editAll(f, 'DROP TRIGGER IF EXISTS retention_fault')
      f.reopen()
      t.equal(f.proc.engine.pendingApply, null)
      sync(f)
      t.equal(f.proc.engine.pendingCommand, null)
      t.equal(f.proc.log.begin, 4n)
    } finally { f.close() }
  }
  t.end()
})

test('startup pruning rejects invalid checkpoints and survives cleanup failures', (t) => {
  for (const fault of ['delete', 'head refresh', 'checkpoint']) {
    const opts = {}
    const f = protocol(opts).setup()
    const readHead = SQLiteLog.prototype.readHead
    try {
      noops(f, 8)
      const before = f.rows()
      f.stop()
      Object.assign(opts, options())
      if (fault === 'delete') {
        editAll(f, `CREATE TRIGGER retention_fault BEFORE DELETE ON raft_log
          BEGIN SELECT RAISE(ABORT, 'startup delete failed'); END`)
      } else if (fault === 'head refresh') {
        SQLiteLog.prototype.readHead = function () {
          if (this.db.prepare('SELECT min(seq) AS seq FROM raft_log').get().seq > 0n) {
            throw new Error('startup refresh failed')
          }
          return readHead.call(this)
        }
      } else {
        editAll(f, 'UPDATE monsterft_meta SET applied_entry_hash = zeroblob(16)')
      }
      if (fault === 'checkpoint') {
        t.throws(() => f.reopen(), (err) => err.code === Err.MONSTER_CORRUPT)
      } else {
        f.reopen()
        t.notOk(f.proc.isOpen, 'startup failure closes owner')
      }
      t.deepEqual(f.rows(), before, `${fault}: failed startup removes no history`)
      f.stop()
      SQLiteLog.prototype.readHead = readHead
      editAll(f, 'DROP TRIGGER IF EXISTS retention_fault')
      if (fault === 'checkpoint') {
        // Restore the real checkpoint hash, not a new receipt or new application.
        for (const key of ['procA', 'procB', 'procC']) {
          const db = new DatabaseSync(f.paths[key])
          try { db.prepare('UPDATE monsterft_meta SET applied_entry_hash=?').run(digestEntry(1n, Buffer.alloc(0))) }
          finally { db.close() }
        }
      }
      f.reopen()
      t.equal(f.proc.log.begin, 6n)
      t.equal(f.proc.engine.pendingApply, null)
    } finally { SQLiteLog.prototype.readHead = readHead; f.close() }
  }
  t.end()
})
