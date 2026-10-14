import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { DatabaseSync } from 'node:sqlite'
import { MonsterProc } from '../src/proc.js'
import { ProtocolStore, openApplyDatabase } from '../src/storage.js'
import { encodeFrame, decodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const roles = ['procA', 'procB', 'procC']
const seed = Buffer.alloc(16, 1)
const replacement = Buffer.alloc(16, 2)

function fixture() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-generation-cache-'))
  const base = path.join(directory, 'node')
  const replies = roles.map(() => [])
  const errors = roles.map(() => [])
  const procs = roles.map((role, index) => {
    const proc = new MonsterProc(role, 'a', ['a', 'b', 'c'], (to, frame) => {
      replies[index].push(decodeFrame(frame))
    }, base, { localTimeout: 60_000 })
    proc.on('error', (err) => errors[index].push(err))
    return proc
  })
  let cid = 0
  return {
    base, procs, errors,
    request(index, message) {
      message = { cid: `request-${++cid}`, ...message }
      procs[index].onReceive('gate', encodeFrame(message))
      return replies[index].find((reply) => reply.cid === message.cid)
    },
    input(index, type) {
      return this.request(index, {
        type, tickCount: procs[index].tickCount + 1n,
        timems: procs[index].executor.timems,
      })
    },
    start() {
      for (let i = 0; i < procs.length; i++) {
        this.request(i, { type: 'hello', seed })
        this.request(i, { type: 'open' })
      }
      openApplyDatabase(base, seed).close()
      for (const type of ['capture', 'adopt', 'reconcile']) {
        for (let i = 0; i < procs.length; i++) {
          if (this.request(i, { type })?.type !== 'ack') {
            throw new Error(`failed startup ${type}`)
          }
        }
      }
      for (let i = 0; i < procs.length; i++) { this.input(i, 'start') }
      return this
    },
    close() {
      for (const proc of procs) { proc.close() }
      fs.rmSync(directory, { recursive: true, force: true })
    },
  }
}

test('cached Proc generation checks observe takeover without preparing another SELECT', (t) => {
  const f = fixture().start()
  const proc = f.procs[0]
  const next = new ProtocolStore('procA', f.base)
  const prepare = proc.db.prepare.bind(proc.db)
  let preparations = 0
  proc.db.prepare = (sql) => {
    if (/SELECT\s+generation\s+FROM\s+monsterft_meta\b/i.test(sql)) { preparations++ }
    return prepare(sql)
  }
  try {
    for (let i = 0; i < 3; i++) {
      t.equal(f.input(0, 'tick')?.type, 'ack', 'unchanged generation permits another input')
    }
    next.open(replacement)
    t.equal(f.input(0, 'tick'), undefined, 'a stale Proc publishes no completion')
    t.equal(f.errors[0].at(-1)?.code, Err.GENERATION_DIFF,
      'cached SELECT observes the replacement generation')
    t.notOk(proc.isOpen, 'takeover still stops the old owner')
    t.equal(preparations, 0, 'runtime checks reuse the statement prepared during open')
  } finally { next.close(); f.close() }
  t.end()
})

test('cached Proc generation checks reject changed corrupt or missing rows', (t) => {
  for (const sql of [
    'UPDATE monsterft_meta SET generation = zeroblob(16) WHERE id = 1',
    'DELETE FROM monsterft_meta WHERE id = 1',
  ]) {
    const f = fixture().start()
    const other = new DatabaseSync(f.procs[0].store.paths.procA)
    try {
      t.equal(f.input(0, 'tick')?.type, 'ack', 'generation is valid before corruption')
      other.exec(sql)
      t.equal(f.input(0, 'tick'), undefined, 'invalid generation prevents completion')
      t.equal(f.errors[0].at(-1)?.code, Err.MONSTER_CORRUPT,
        'cached SELECT preserves corruption classification')
      t.equal(f.errors[0].at(-1)?.message, 'DB1 generation is corrupt')
      t.notOk(f.procs[0].isOpen, 'corrupt generation stops the Proc')
    } finally { other.close(); f.close() }
  }
  t.end()
})

test('ProtocolStore can reopen with a fresh generation-check statement', (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-generation-reopen-'))
  const store = new ProtocolStore('procA', path.join(directory, 'node'))
  let other = null
  try {
    store.open(seed)
    t.doesNotThrow(() => store.checkGeneration(), 'initial connection checks its generation')
    store.close()
    store.close()
    store.open(replacement)
    t.doesNotThrow(() => store.checkGeneration(), 'reopened connection checks its new generation')
    other = new DatabaseSync(store.paths.procA)
    other.prepare('UPDATE monsterft_meta SET generation = ? WHERE id = 1').run(seed)
    t.throws(() => store.checkGeneration(), (err) => err.code === Err.GENERATION_DIFF,
      'reopened statement reads later committed changes')
  } finally {
    if (other !== null) { other.close() }
    store.close()
    fs.rmSync(directory, { recursive: true, force: true })
  }
  t.end()
})
