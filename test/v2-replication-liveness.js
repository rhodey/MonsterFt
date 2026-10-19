import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { MonsterProc } from '../src/proc.v2.js'
import { openApplyDatabase } from '../src/storage.v2.js'
import * as wire from '../src/wire.v2.js'
import * as Err from '../src/error.v2.js'

function fixture(id = 'a', nodes = ['a', 'b', 'c']) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-v2-liveness-'))
  const base = path.join(directory, id)
  const replies = []
  const procs = ['procA', 'procB', 'procC'].map((role) => {
    const proc = new MonsterProc(role, id, nodes, (to, frame) => {
      replies.push(wire.decodeFrame(frame))
    }, base, { electionTimeout: 150, appendTimeout: 1000, localTimeout: 10_000 })
    proc.on('error', () => {})
    return proc
  })
  let cid = 0
  const request = (proc, message) => {
    message = { cid: `local-${++cid}`, ...message }
    proc.onReceive('gate', wire.encodeFrame(message))
    const reply = replies.find((value) => value.cid === message.cid)
    if (reply?.type !== 'ack') { throw new Error(reply?.message ?? 'missing local reply') }
    return reply
  }
  const seed = Buffer.from('10111213202122233031323340414243', 'hex')
  for (const proc of procs) { request(proc, { type: 'hello', seed }) }
  for (const proc of procs) { request(proc, { type: 'open' }) }
  const db2 = openApplyDatabase(base)
  for (const type of ['capture', 'adopt', 'reconcile']) {
    for (const proc of procs) { request(proc, { type }) }
  }
  const proc = procs[0]
  const input = (kind, fields = {}) => request(proc, {
    type: 'input', kind, tickCount: proc.tickCount + 1n,
    timems: proc.executor.timems, ...fields,
  })
  const rpc = (message, from = 'b', timems = proc.executor.timems) => input('rpc', {
    from, timems, frame: wire.encodeFrame({ to: id, message: { from, ...message } }),
  })
  const outputs = (reply) => (reply.outputs ?? []).map(wire.decodePayload)
  const page = (reply, to = 'b') => outputs(reply)
    .find((output) => output.to === to && output.message.type === 'append')?.message
  input('start')
  return {
    proc, input, rpc, outputs, page,
    elect() {
      input('tick', { timems: 10_000 })
      let reply
      for (const from of nodes.filter((node) => node !== id).slice(0, Math.floor(nodes.length / 2))) {
        reply = rpc({ type: 'vote', term: proc.engine.term, voteGranted: true }, from)
      }
      return reply
    },
    setup(reply) {
      const execute = outputs(reply).find(({ to }) => to === 'apply').message
      const receipt = { seq: 0n, entry_hash: execute.entryHash, local_digest: Buffer.alloc(32) }
      db2.prepare('UPDATE monsterft_meta_apply SET seq=?, entry_hash=?, local_digest=?')
        .run(receipt.seq, receipt.entry_hash, receipt.local_digest)
      return input('apply', { reply: { type: 'ack', cid: execute.cid, receipt, outcomes: [] } })
    },
    close() {
      for (const member of procs) { member.close() }
      db2.close()
      fs.rmSync(directory, { recursive: true, force: true })
    },
  }
}

test('v2 heartbeat ACK commits an acknowledged CMD when it restores a live quorum', (t) => {
  const f = fixture('a', ['a', 'b', 'c', 'd', 'e'])
  try {
    const election = f.elect()
    f.rpc({ type: 'ack', cid: f.page(election, 'b').cid, term: 1n, seq: 0n }, 'b')
    const ready = f.rpc({ type: 'ack', cid: f.page(election, 'c').cid, term: 1n, seq: 0n }, 'c')
    f.setup(ready)
    for (const to of ['b', 'c']) {
      const heartbeat = f.page(ready, to)
      if (heartbeat) { f.rpc({ type: 'ack', cid: heartbeat.cid, term: 1n, seq: 1n }, to) }
    }
    const command = f.input('append', {
      requestId: 'user-command', items: [Buffer.from('insert')], timems: 10_001,
    })
    const b = f.page(command, 'b')
    const c = f.page(command, 'c')
    f.rpc({ type: 'ack', cid: b.cid, term: 1n, seq: 1n }, 'b', 10_002)
    const heartbeat = f.page(f.input('tick', { timems: 10_045 }), 'b')
    t.equal(heartbeat.data, undefined, 'B now receives a heartbeat without entries')
    f.input('tick', { timems: 10_090 })
    f.input('tick', { timems: 10_135 })
    f.rpc({ type: 'ack', cid: c.cid, term: 1n, seq: 1n }, 'c', 10_155)
    t.equal(f.proc.engine.commitSeq, 0n, 'C ACK arrives after B expires, so CMD cannot commit yet')
    const reply = f.rpc({ type: 'ack', cid: heartbeat.cid, term: 1n, seq: 2n }, 'b', 10_160)
    t.deepEqual(f.proc.engine.followers, ['b', 'c'], 'the heartbeat restores a majority')
    t.equal(f.proc.engine.commitSeq, 1n, 'already acknowledged CMD commits immediately')
    t.equal(f.proc.engine.state, 'leader', 'no command timeout or leadership change is needed')
    t.ok(f.outputs(reply).some(({ to, message }) => to === 'apply' && message.seq === 1n),
      'CMD execution is requested in the same input')
    t.deepEqual([...f.proc.engine.replication.values()].map((state) => state.matchIndex),
      [1n, 1n, -1n, -1n], 'heartbeat does not invent additional matched entries')
  } finally { f.close() }
  t.end()
})

test('v2 backtracking keeps a responsive follower live throughout a long divergent suffix', (t) => {
  for (const delay of [0, 4]) {
    const a = fixture('a')
    const b = fixture('b')
    try {
      // These are legal uncommitted histories received from an earlier leader.
      for (const [f, term] of [[a, 2n], [b, 1n]]) {
        f.rpc({ type: 'append', cid: 'old-history', term: 3n, termP: -1n, seqP: -1n,
          commitSeq: -1n, data: Array.from({ length: 100 }, () => ({ term, entry: Buffer.alloc(0) })),
        }, 'c')
      }
      const queue = []
      let now = 10_000
      let backtracks = 0
      let matchAdvancedByBacktrack = false
      const collect = (from, reply) => {
        for (const output of a.outputs(reply)) {
          if (output.message.code === Err.REPL_BACKTRACK) { backtracks++ }
          if (output.to === 'a' || output.to === 'b') {
            queue.push({ from, ...output, due: now + delay })
          }
        }
      }
      collect('a', a.input('tick', { timems: now }))
      let deliveries = 0
      while (queue.length > 0 && deliveries++ < 1000) {
        const item = queue.shift()
        now = item.due
        const receiver = item.to === 'a' ? a : b
        const reply = receiver.rpc(item.message, item.from, now)
        if (item.to === 'a' && item.message.code === Err.REPL_BACKTRACK) {
          const match = a.proc.engine.replication.get('b')?.matchIndex
          if (match !== undefined && match !== -1n) { matchAdvancedByBacktrack = true }
        }
        collect(item.to, reply)
      }
      t.ok(deliveries < 1000, `${delay} ms transport: replication completes within the delivery limit`)
      t.equal(backtracks, 100, 'conflicting history still uses ordinary one-entry backtracking')
      t.notOk(matchAdvancedByBacktrack, 'backtracking replies never establish matching history')
      t.equal(a.proc.engine.state, 'leader', 'responsive follower preserves leadership')
      t.equal(a.proc.engine.term, 4n, 'no election restarts the search')
      t.deepEqual([...b.proc.log.iter()], [...a.proc.log.iter()], 'all divergent history is replaced')
      t.equal(a.proc.engine.commitSeq, 100n, 'current-term no-op commits after the follower catches up')
      if (delay > 0) {
        t.ok(now - 10_000 > 150, 'backtracking outlasts the follower liveness window')
      }
    } finally { a.close(); b.close() }
  }
  t.end()
})

test('v2 stale-term backtracking replies cannot keep a follower live', (t) => {
  const f = fixture()
  try {
    f.rpc({ type: 'append', cid: 'old-history', term: 1n, termP: -1n, seqP: -1n,
      commitSeq: -1n, data: Array.from({ length: 9 }, () => ({ term: 1n, entry: Buffer.alloc(0) })),
    })
    let page = f.page(f.elect())
    for (const time of [10_060, 10_090, 10_120]) {
      f.rpc({ type: 'err', cid: page.cid, term: 1n, code: Err.REPL_BACKTRACK,
        message: 'stale predecessor rejection', logSeq: -1n,
      }, 'b', time)
      t.equal(f.proc.engine.pongs.get('b'), 10_000, 'stale response does not refresh liveness')
      t.equal(f.proc.engine.replication.get('b').nextIndex, 9n, 'stale hint cannot change the retry position')
      page = f.page(f.input('tick', { timems: time + 15 }))
    }
    f.input('tick', { timems: 10_200 })
    t.equal(f.proc.engine.state, 'follower', 'only stale responses cannot prevent quorum loss')
    t.equal(f.proc.engine.commitSeq, -1n, 'stale responses cannot commit entries')
  } finally { f.close() }
  t.end()
})
