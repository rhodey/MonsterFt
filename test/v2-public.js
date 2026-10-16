import test from 'tape'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import * as api from '../src/index.v2.js'
import { databasePaths } from '../src/storage.v2.js'
import { decodeFrame } from '../src/wire.v2.js'
import { cluster, until } from './v2-helper.js'

test('v2 public MonsterFt constructs local components and preserves command APIs', async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-v2-public-'))
  const nodes = new Map()
  const ids = ['a', 'b', 'c']
  const errors = []
  const calls = []
  try {
    for (const id of ids) {
      const node = new api.MonsterFt(id, ids, (to, frame) => nodes.get(to)?.onReceive(id, frame),
        path.join(directory, id), () => ({
          electionTimeout: 150, appendTimeout: 1_000, tickInterval: 2,
          apply(db, buffer, term, seq, index, matchIndex) {
            calls.push({ id, seq, term, index, matchIndex })
            if (seq === 0n) {
              db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT) STRICT')
              return null
            }
            db.prepare('INSERT INTO items (value) VALUES (?)').run(buffer.toString())
            return { item: buffer.toString(), seq }
          },
        }))
      node.on('error', (err) => errors.push(err))
      nodes.set(id, node)
      t.equal(node.components.size, 4, 'default wiring creates three Procs and one Apply')
    }
    t.equal(api.RaftNode, undefined, 'standalone RaftNode is not exported')
    await Promise.all([...nodes.values()].map((node) => node.open()))
    await Promise.all([...nodes.values()].map((node) => node.awaitLeader(true)))
    const node = nodes.get('a')
    const commit = node.awaitEvent('commit', (seq) => seq > 0n)
    const [seq, result] = await node.append(Buffer.from('one'))
    t.deepEqual(result, { item: 'one', seq })
    t.equal(typeof await commit, 'bigint', 'commit event preserves its bigint argument')
    const [batchSeq, outcomes] = await nodes.get('b').appendBatch([Buffer.from('two'), Buffer.from('three')])
    t.deepEqual(outcomes, [
      { status: 'fulfilled', value: { item: 'two', seq: batchSeq } },
      { status: 'fulfilled', value: { item: 'three', seq: batchSeq } },
    ])
    await until(() => [...nodes.values()].every((member) =>
      member.components.get('apply').db.prepare('SELECT count(*) AS n FROM items').get().n === 3n),
    'default components apply each item')
    t.equal(calls.filter((call) => call.seq === 0n).length, 3)
    t.equal(calls.filter((call) => call.seq > 0n).length, 9)
    t.ok(calls.filter((call) => call.seq > 0n).every((call) =>
      call.matchIndex.length === 3 && typeof call.term === 'bigint'), 'apply callback retains protocol arguments')
    t.throws(() => node.del(), { code: api.ErrorCodes.LOG_OPEN }, 'cannot delete open files')
    await Promise.all([...nodes.values()].map((member) => member.close()))
    await assert.rejects(node.open(), { code: api.ErrorCodes.NODE_NOT_OPEN })
    t.notOk(node.isOpen, 'a closed instance cannot pretend that its old open promise reopened it')
    for (const member of nodes.values()) {
      member.del()
      t.ok(Object.values(databasePaths(member.databasePath)).every((file) => !fs.existsSync(file)),
        'del removes all four closed files')
    }
    t.equal(errors.length, 0)
  } finally {
    await Promise.all([...nodes.values()].map((node) => node.close().catch(() => {})))
    fs.rmSync(directory, { recursive: true, force: true })
  }
  t.end()
})

test('v2 public constructor rejects malformed component configuration', (t) => {
  const make = (opts = {}, file = '/tmp/not-opened-v2') =>
    new api.MonsterFt('a', ['a', 'b', 'c'], () => {}, file, { apply: () => null, ...opts })
  for (const opts of [
    { localSend: null }, { localSend: 1 }, { apply: null },
    { tickInterval: 0 }, { localTimeout: 0 }, { applyTimeout: 0 },
  ]) {
    t.throws(() => make(opts), { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  }
  t.throws(() => make({}, ':memory:'), { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  t.throws(() => new api.MonsterFt('a', ['a', 'a', 'b'], () => {}, '/tmp/not-opened-v2', {
    apply: () => null,
  }), { code: api.ErrorCodes.ARGUMENT_ILLEGAL })
  t.end()
})

test('v2 public del waits for externally wired Apply to acknowledge shutdown', async (t) => {
  let hold = false
  let release
  const c = cluster({
    local({ id, from, to, frame, deliver }) {
      if (!hold || id !== 'a' || from !== 'gate' || to !== 'apply') { return }
      if (decodeFrame(frame).type === 'close') { release = deliver; return false }
    },
  })
  try {
    await c.open()
    hold = true
    const node = c.nodes.get('a').gate
    const closing = node.close()
    await until(() => release, 'held Apply close')
    t.throws(() => node.del(), { code: api.ErrorCodes.LOG_OPEN },
      'cannot unlink files until every owner has stopped')
    hold = false
    release()
    await closing
    node.del()
    t.ok(Object.values(databasePaths(node.databasePath)).every((file) => !fs.existsSync(file)))
  } finally { hold = false; release?.(); await c.close() }
  t.end()
})
