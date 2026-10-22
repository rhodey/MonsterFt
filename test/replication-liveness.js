import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { MonsterProc } from '../src/proc.js'
import { openApplyDatabase } from '../src/storage.js'
import * as wire from '../src/wire.js'
import * as Err from '../src/error.js'

function fixture(id = 'a', nodes = ['a', 'b', 'c']) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-liveness-'))
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
  const db2 = openApplyDatabase(base, seed)
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
    .find((output) => output.to === to && output.message.type === 'append' &&
      output.message.data !== undefined)?.message
  const heartbeat = (reply, to = 'b') => outputs(reply)
    .find((output) => output.to === to && output.message.type === 'append' &&
      output.message.data === undefined)?.message
  input('start')
  return {
    proc, input, rpc, outputs, page, heartbeat,
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

test('heartbeat ACK commits an acknowledged CMD when it restores a live quorum', (t) => {
  const f = fixture('a', ['a', 'b', 'c', 'd', 'e'])
  try {
    const election = f.elect()
    f.rpc({ type: 'ack', cid: f.page(election, 'b').cid, term: 1n, seq: 0n }, 'b')
    const ready = f.rpc({ type: 'ack', cid: f.page(election, 'c').cid, term: 1n, seq: 0n }, 'c')
    f.setup(ready)
    for (const to of ['b', 'c']) {
      const heartbeat = f.heartbeat(ready, to)
      if (heartbeat) { f.rpc({ type: 'ack', cid: heartbeat.cid, term: 1n, seq: 1n }, to) }
    }
    const command = f.input('append', {
      requestId: 'user-command', items: [Buffer.from('insert')], timems: 10_001,
    })
    const b = f.page(command, 'b')
    const c = f.page(command, 'c')
    f.rpc({ type: 'ack', cid: b.cid, term: 1n, seq: 1n }, 'b', 10_002)
    const heartbeat = f.heartbeat(f.input('tick', { timems: 10_045 }), 'b')
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

test('backtracking handles equal, longer divergent and empty follower logs', (t) => {
  for (const [delay, followerLength] of [0, 4].flatMap((delay) => [0, 100, 150].map((n) => [delay, n]))) {
    const a = fixture('a')
    const b = fixture('b')
    try {
      // These are legal uncommitted histories received from an earlier leader.
      for (const [f, term, length] of [[a, 2n, 100], [b, 1n, followerLength]]) {
        if (length === 0) { continue }
        f.rpc({ type: 'append', cid: 'old-history', term: 3n, termP: -1n, seqP: -1n,
          commitSeq: -1n, data: Array.from({ length }, () => ({ term, entry: Buffer.alloc(0) })),
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
      t.equal(backtracks, followerLength === 0 ? 1 : 100,
        'empty tails skip in one rejection; conflicting history backtracks one entry at a time')
      t.notOk(matchAdvancedByBacktrack, 'backtracking replies never establish matching history')
      t.equal(a.proc.engine.state, 'leader', 'responsive follower preserves leadership')
      t.equal(a.proc.engine.term, 4n, 'no election restarts the search')
      t.deepEqual([...b.proc.log.iter()], [...a.proc.log.iter()], 'all divergent history is replaced')
      t.equal(a.proc.engine.commitSeq, 100n, 'current-term no-op commits after the follower catches up')
      if (delay > 0 && followerLength > 0) {
        t.ok(now - 10_000 > 150, 'backtracking outlasts the follower liveness window')
      }
    } finally { a.close(); b.close() }
  }
  t.end()
})

test('stale-term backtracking replies cannot keep a follower live', (t) => {
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

test('heartbeats continue while data and earlier heartbeat ACKs are missing', (t) => {
  const f = fixture()
  try {
    const page = f.page(f.elect())
    const first = f.heartbeat(f.input('tick', { timems: 10_045 }))
    t.ok(first, 'heartbeat is sent while the election data page awaits its ACK')
    const second = f.heartbeat(f.input('tick', { timems: 10_090 }))
    t.ok(second, 'missing heartbeat ACK does not prevent the next heartbeat')
    t.notEqual(second.cid, first.cid, 'each heartbeat has its own request')
    t.equal(second.seqP, f.proc.engine.seq, 'heartbeat identifies the current leader head')
    t.equal(second.termP, f.proc.log.term, 'heartbeat carries the current head term')
    t.equal(second.commitSeq, f.proc.engine.commitSeq, 'heartbeat carries the current commit boundary')
    t.equal(f.proc.engine.replication.get('b').cid, page.cid, 'data request remains independently pending')
    f.rpc({ type: 'ack', cid: second.cid, term: 1n, seq: 1n }, 'b', 10_091)
    t.ok(f.proc.engine.requests.has(first.cid), 'newer ACK does not discard the earlier heartbeat request')
    for (const time of [10_135, 10_180, 10_225]) {
      const heartbeat = f.heartbeat(f.input('tick', { timems: time }))
      f.rpc({ type: 'ack', cid: heartbeat.cid, term: 1n, seq: 1n }, 'b', time + 1)
    }
    t.equal(f.proc.engine.state, 'leader', 'independent heartbeat replies preserve leadership')
    t.equal(f.proc.engine.term, 1n, 'no election is needed while the data ACK remains missing')
    t.equal(f.proc.engine.replication.get('b').matchIndex, -1n, 'heartbeats never acknowledge the data page')
    t.equal(f.proc.engine.commitSeq, -1n, 'heartbeat replies alone cannot commit the election entry')
    t.equal(f.proc.engine.replication.get('b').cid, page.cid, 'periodic heartbeats do not replace the data request')
    t.notOk(f.proc.engine.requests.has(first.cid), 'the lost heartbeat request expires independently')
  } finally { f.close() }
  t.end()
})

test('heartbeat proceeds during a data retry delay without changing the retry', (t) => {
  const f = fixture()
  try {
    const page = f.page(f.elect())
    f.rpc({ type: 'err', cid: page.cid, term: 1n,
      code: Err.RPC_ILLEGAL, message: 'temporary rejection',
    }, 'b', 10_040)
    const state = f.proc.engine.replication.get('b')
    const retryTimer = state.retryTimer
    t.notEqual(retryTimer, null, 'data retry is delayed')
    const reply = f.input('tick', { timems: 10_045 })
    const heartbeat = f.heartbeat(reply)
    t.ok(heartbeat, 'heartbeat is sent before the data retry becomes due')
    t.equal(f.page(reply), undefined, 'data page respects its retry delay')
    f.rpc({ type: 'ack', cid: heartbeat.cid, term: 1n, seq: 1n }, 'b', 10_046)
    t.equal(state.retryTimer, retryTimer, 'heartbeat success preserves the existing retry timer')
    t.equal(state.matchIndex, -1n, 'heartbeat success does not advance matching history')
    const retried = f.page(f.input('tick', { timems: 10_055 }))
    t.ok(retried, 'data retry still runs at its original deadline')
    t.notEqual(retried.cid, page.cid, 'retry uses a fresh request')
    t.deepEqual(retried.data, page.data, 'retry sends the same unacknowledged entries')
  } finally { f.close() }
  t.end()
})

test('heartbeat errors and invalid ACKs cannot advance liveness or replication', (t) => {
  for (const response of [
    { type: 'err', term: 1n, code: Err.REPL_BACKTRACK, message: 'heartbeat rejected', logSeq: -1n },
    { type: 'err', term: 0n, code: Err.REPL_BACKTRACK, message: 'old heartbeat rejection', logSeq: -1n },
    { type: 'ack', term: 0n, seq: 1n },
    { type: 'ack', term: 1n },
    { type: 'ack', term: 1n, seq: -1n },
  ]) {
    const f = fixture()
    try {
      const page = f.page(f.elect())
      const heartbeat = f.heartbeat(f.input('tick', { timems: 10_045 }))
      f.rpc({ cid: heartbeat.cid, ...response }, 'b', 10_050)
      const state = f.proc.engine.replication.get('b')
      t.equal(f.proc.engine.pongs.get('b'), 10_000, 'unsuccessful heartbeat does not refresh liveness')
      t.equal(state.nextIndex, 0n, 'unsuccessful heartbeat does not change the retry position')
      t.equal(state.matchIndex, -1n, 'unsuccessful heartbeat does not establish matching history')
      t.equal(state.cid, page.cid, 'unsuccessful heartbeat leaves the data request intact')
    } finally { f.close() }
  }
  t.end()
})

test('higher-term heartbeat response cancels old leadership requests', (t) => {
  const f = fixture()
  try {
    const page = f.page(f.elect())
    const heartbeat = f.heartbeat(f.input('tick', { timems: 10_045 }))
    f.input('tick', { timems: 10_090 })
    t.ok(f.proc.engine.requests.size > 4, 'multiple heartbeats and data requests are outstanding')
    f.rpc({ type: 'ack', cid: heartbeat.cid, term: 2n, seq: 1n }, 'b', 10_091)
    t.equal(f.proc.engine.state, 'follower', 'higher-term response ends leadership')
    t.equal(f.proc.engine.term, 2n, 'higher term is adopted')
    t.equal(f.proc.log.elec.term, 2n, 'higher term is durable')
    t.equal(f.proc.engine.requests.size, 0, 'all old heartbeat and data requests are cancelled')
    t.equal(f.proc.engine.replication.size, 0, 'old replication state is discarded')
    const reply = f.rpc({ type: 'ack', cid: page.cid, term: 1n, seq: 0n }, 'b', 10_092)
    t.equal(f.proc.engine.commitSeq, -1n, 'late old data ACK cannot commit')
    t.equal(f.proc.engine.pongs.size, 0, 'late old data ACK cannot restore old liveness')
    t.equal(f.outputs(reply).filter(({ message }) => message.type === 'append').length, 0,
      'late old data ACK does not resume replication')
  } finally { f.close() }
  t.end()
})

test('closing a Proc removes outstanding heartbeat and data requests', (t) => {
  const f = fixture()
  try {
    f.elect()
    f.input('tick', { timems: 10_045 })
    f.input('tick', { timems: 10_090 })
    t.ok(f.proc.engine.requests.size > 4, 'multiple independent requests are outstanding')
    f.proc.close()
    t.equal(f.proc.engine.requests.size, 0, 'closing removes all requests')
    t.equal(f.proc.engine.replication.size, 0, 'closing removes all replication state')
    t.equal(f.proc.executor.timers.size, 0, 'closing cancels request and periodic timers')
  } finally { f.close() }
  t.end()
})
