import test from 'tape'
import assert from 'node:assert/strict'
import crypto from 'node:crypto'
import { Gate } from '../src/gate.js'
import * as Err from '../src/error.js'
import * as wire from '../src/wire.js'
import { isInputType } from '../src/utilm.js'

const procs = ['procA', 'procB', 'procC']
const contribution = (role, payload) => wire.checksum(payload).subarray(
  role === 'procB' ? 0 : 8, role === 'procB' ? 8 : 16)
const frame = (payload) => wire.framePayload(payload, wire.checksum(payload))
const immediate = () => new Promise((resolve) => setImmediate(resolve))
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const status = (fields = {}) => ({
  state: 'leader', leader: 'a', term: 1n, seq: 3n,
  commitSeq: 3n, commitTerm: 1n, followers: ['b'], ...fields,
})
const execution = (cid, tickCount = 2n) => ({
  type: 'execute', cid, tickCount, term: 1n, seq: 1n,
  entryHash: Buffer.alloc(16), items: [Buffer.from('work')], matchIndex: [0n, 0n, 0n],
})
const executionReply = (message) => ({
  type: 'ack', cid: message.cid, tickCount: message.tickCount,
  receipt: { seq: message.seq, entry_hash: message.entryHash, local_digest: Buffer.alloc(16) },
  outcomes: message.items?.map(() => [0, 'ok']) ?? [],
})

function harness(options = {}) {
  const requests = []
  const sent = []
  const errors = []
  const warnings = []
  let gate
  const reply = (from, message) => gate.onReceive(from, wire.encodeFrame(message))
  const complete = (from, input, outputs = []) => reply(from, {
    type: 'ack', cid: input.cid, tickCount: input.tickCount,
    outputs: from === 'procA' ? outputs : outputs.map((output) => contribution(from, output)),
  })
  const localSend = (to, frame) => {
    const decoded = wire.decodeFrame(frame)
    const message = decoded.to === 'apply' ? decoded.message : decoded
    requests.push({ to, message, frame })
    if (options.local?.({ to, message, frame, reply, complete })) { return }
    if (message.type === 'capture') {
      reply(to, {
        type: 'ack', cid: message.cid,
        selection: { state: Buffer.alloc(16), receipt: Buffer.alloc(16), donor: 'procA' },
      })
    } else if (isInputType(message.type)) {
      complete(to, message, options.outputs?.(message) ?? [])
    } else {
      reply(to, { type: 'ack', cid: message.cid })
    }
  }
  gate = new Gate('a', ['a', 'b', 'c'], (to, frame) => {
    sent.push({ to, frame })
    return options.send?.(to, frame)
  }, localSend, {
    tickInterval: 10_000, localTimeout: 250, applyTimeout: 250,
    ...options.opts,
  })
  gate.on('error', (err) => errors.push(err))
  gate.on('warn', (err) => warnings.push(err))
  return { gate, requests, sent, errors, warnings, reply, complete }
}

const client = (input, values = input.items.map((buf) => [0, buf.toString()])) =>
  wire.encodePayload({
    to: 'gate', message: { type: 'ack', clientId: input.clientId, cmdSeq: 3n, results: values },
  })

test('Gate defaults Apply timeout to five seconds and permits an override', (t) => {
  const make = (opts) => new Gate('a', ['a', 'b', 'c'], () => {}, () => {}, opts)
  t.equal(make().applyTimeout, 5_000)
  t.equal(make({ applyTimeout: 12_000 }).applyTimeout, 12_000)
  t.end()
})

test('Gate hello retries use fresh CIDs and one seed before opening', async (t) => {
  let release
  let old
  const h = harness({
    opts: { localTimeout: 30 },
    local({ to, message, reply }) {
      if (to !== 'procB' || message.type !== 'hello') { return false }
      if (!old) { old = message; return true }
      release = () => reply(to, { type: 'ack', cid: message.cid })
      reply(to, { type: 'ack', cid: old.cid })
      return true
    },
  })
  const opening = h.gate.open()
  while (!release) { await delay(2) }
  t.equal(h.requests.filter((r) => r.message.type === 'open').length, 0,
    'expired ACK cannot finish hello')
  release()
  await opening
  const hellos = h.requests.filter((r) => r.message.type === 'hello')
  const seeds = hellos.map((r) => r.message.seed)
  t.ok(seeds.every((seed) => seed.length === 16 && seed.equals(seeds[0])), 'all attempts share one seed')
  const b = hellos.filter((r) => r.to === 'procB')
  t.notEqual(b[0].message.cid, b[1].message.cid, 'retry has a fresh CID')
  t.deepEqual(h.requests.filter((r) => r.to === 'procA').map((r) => r.message.type),
    ['hello', 'open', 'capture', 'adopt', 'reconcile', 'start'], 'startup requests stay ordered')
  t.equal(h.gate.tickCount, 1n, 'runtime start is first ordered input')
  await h.gate.close()
  t.end()
})

test('Gate opens database owners one at a time before capture', async (t) => {
  let release
  const h = harness({
    local({ to, message, reply }) {
      if (to !== 'procA' || message.type !== 'open') { return false }
      release = () => reply(to, { type: 'ack', cid: message.cid })
      return true
    },
  })
  const opening = h.gate.open()
  await immediate()
  t.deepEqual(h.requests.filter((r) => r.message.type === 'open').map((r) => r.to), ['procA'],
    'other owners wait while the first owner opens its file')
  t.notOk(h.requests.some((r) => r.message.type === 'capture'), 'capture has not started')
  release()
  await opening
  t.deepEqual(h.requests.filter((r) => r.message.type === 'open').map((r) => r.to),
    ['procA', 'procB', 'procC', 'apply'], 'all owners finish opening in fixed order')
  t.equal(h.requests.filter((r) => r.message.type === 'capture').length, 3,
    'all three Procs then independently capture the stable files')
  await h.gate.close()
  t.end()
})

test('Gate sends each ordered operation directly in its type field', async (t) => {
  const remote = wire.encodeFrame({ to: 'a', message: {
    type: 'vote_request', from: 'b', term: 1n, termP: -1n, seqP: -1n,
  } })
  const h = harness({
    opts: { tickInterval: 2 },
    local({ to, message, reply }) {
      if (message.type !== 'execute') { return false }
      reply(to, executionReply(message))
      return true
    },
    outputs(input) {
      if (input.type === 'append') {
        return [wire.encodePayload({ to: 'apply', message: execution('operation-type', input.tickCount) }), client(input)]
      }
      if (input.type === 'drain') {
        return [wire.encodePayload({ to: 'gate', message: { type: 'ack', clientId: input.clientId } })]
      }
      return []
    },
  })
  try {
    await h.gate.open()
    t.deepEqual(await h.gate.append(Buffer.from('work')), [3n, 'work'])
    h.gate.onReceive('b', remote)
    await delay(10)
    await h.gate.drainCmd()
    const inputs = h.requests.filter(({ to, message }) => to === 'procA' && isInputType(message.type))
    t.deepEqual([...new Set(inputs.map(({ message }) => message.type))].sort(),
      ['append', 'apply_result', 'cluster_message', 'drain', 'start', 'tick'])
    t.ok(inputs.every(({ message }) => !Object.hasOwn(message, 'kind')), 'ordered frames have no kind field')
    t.ok(inputs.every(({ message }, index) => message.tickCount === BigInt(index + 1)),
      'all operation types share the ordered tick sequence')
    t.deepEqual(inputs.find(({ message }) => message.type === 'cluster_message').message.frame,
      remote, 'cluster message retains the original remote frame')
    const apply = inputs.find(({ message }) => message.type === 'apply_result').message
    t.equal(apply.reply.type, 'ack', 'Apply completion keeps its own ACK type')
    t.deepEqual(h.errors, [])
  } finally { await h.gate.close() }
  t.end()
})

test('Gate correlates later client results independently of input RPC calls', async (t) => {
  let first
  const h = harness({ outputs(input) {
    if (input.type !== 'append') { return [] }
    if (!first) { first = input }
    if (input.cid === first.cid) { return [] }
    return [input, first].map((pending) => wire.encodePayload({
      to: 'gate', message: {
        type: 'ack', clientId: pending.clientId,
        cid: pending === first ? input.cid : first.cid,
        cmdSeq: 3n, results: pending.items.map((item) => [0, item.toString()]),
      },
    }))
  } })
  try {
    await h.gate.open()
    const one = h.gate.append(Buffer.from('one'))
    one.catch(() => {})
    await immediate()
    t.notEqual(first.clientId, first.cid, 'public operation and input RPC have different IDs')
    t.equal(h.gate._requests.size, 0, 'all three Procs completed the first input')
    t.ok(h.gate._clients.has(first.clientId), 'the public operation is still pending')
    const two = h.gate.append(Buffer.from('two'))
    t.deepEqual(await Promise.all([one, two]), [[3n, 'one'], [3n, 'two']],
      'out-of-order results select callers by clientId, ignoring unrelated cid fields')
    t.equal(h.gate._clients.size, 0)
    t.deepEqual(h.errors, [])
  } finally { await h.gate.close() }
  t.end()
})

test('Gate duplicate client IDs reject the new call without replacing or queuing it', async (t) => {
  const completions = []
  const h = harness({
    local({ to, message, complete }) {
      if (message.type !== 'append') { return false }
      completions.push(() => complete(to, message, [client(message)]))
      return true
    },
  })
  try {
    await h.gate.open()
    let first
    let duplicate
    const randomUUID = crypto.randomUUID
    try {
      crypto.randomUUID = () => 'same-client-id'
      first = h.gate.append(Buffer.from('first'))
      duplicate = h.gate.appendBatch([Buffer.from('duplicate')])
    } finally { crypto.randomUUID = randomUUID }
    first.catch(() => {})
    await assert.rejects(duplicate, { code: Err.MONSTER_ILLEGAL, message: 'duplicate client id' })
    await immediate()
    t.equal(h.gate._clients.size, 1, 'the original caller remains pending')
    t.deepEqual(h.requests.filter(({ message }) => message.type === 'append')
      .map(({ message }) => message.items[0].toString()), ['first', 'first', 'first'],
    'only the original call reaches the three Procs')
    for (const complete of completions) { complete() }
    t.deepEqual(await first, [3n, 'first'], 'the original caller still receives its result')
    t.equal(h.gate._clients.size, 0)
    t.ok(h.gate.isOpen)
    t.deepEqual(h.errors, [])
  } finally { await h.gate.close() }
  t.end()
})

test('Gate duplicate local CIDs reject the new request without replacing or sending it', async (t) => {
  const h = harness({
    local: ({ message }) => message.type === 'execute',
  })
  try {
    await h.gate.open()
    const message = execution('same-local-cid')
    const first = h.gate._request('apply', message)
    first.catch(() => {})
    const pending = h.gate._requests.get('apply:same-local-cid')
    await assert.rejects(h.gate._request('apply', message), {
      code: Err.MONSTER_ILLEGAL, message: 'duplicate local request cid',
    })
    t.equal(h.gate._requests.get('apply:same-local-cid'), pending,
      'the original request and timeout remain registered')
    t.equal(h.requests.filter(({ message }) => message.type === 'execute').length, 1,
      'the duplicate is never sent')
    t.ok(h.gate.isOpen, 'rejecting the duplicate alone does not stop Gate')
    t.deepEqual(h.errors, [])
    const completion = executionReply(message)
    h.reply('apply', completion)
    t.deepEqual(await first, completion, 'the original request still receives its reply')
    t.equal(h.gate._requests.size, 0)
  } finally { await h.gate.close() }
  t.end()
})

test('Gate duplicate Apply request CIDs from agreed outputs remain fatal', async (t) => {
  const h = harness({
    local: ({ message }) => message.type === 'execute',
    outputs: (input) => input.type === 'append'
      ? [0, 1].map(() => wire.encodePayload({
        to: 'apply', message: execution('same-output-cid', input.tickCount),
      })) : [],
  })
  let closed = false
  h.gate.on('close', () => { closed = true })
  try {
    await h.gate.open()
    await assert.rejects(h.gate.append(Buffer.from('work')), {
      code: Err.MONSTER_ILLEGAL, message: 'duplicate local request cid',
    })
    t.notOk(h.gate.isOpen)
    t.equal(h.errors.length, 1, 'the local request collision is reported once')
    t.equal(h.errors[0].code, Err.MONSTER_ILLEGAL)
    t.equal(h.requests.filter(({ message }) => message.type === 'execute').length, 1,
      'only the first Apply request is sent')
    t.equal(h.gate._requests.size, 0, 'fatal stop settles the original request')
    t.notOk(closed, 'fatal stop does not emit close')
    t.notOk(h.requests.some(({ message }) => message.type === 'close'),
      'fatal stop leaves cleanup explicit')
  } finally { await h.gate.close() }
  t.end()
})

test('Gate repeated open waits for the same startup result', async (t) => {
  for (const finish of ['complete', 'timeout', 'close']) {
    let releaseC
    const h = harness({
      local({ to, message, complete }) {
        if (to !== 'procC' || message.type !== 'start') { return false }
        releaseC = () => complete(to, message)
        return true
      },
    })
    try {
      const opening = h.gate.open().then(() => null, (err) => err)
      await immediate()
      t.equal(typeof releaseC, 'function', `${finish}: final startup completion is held`)
      let settled = false
      const repeated = h.gate.open().then(() => null, (err) => err)
        .then((result) => { settled = true; return result })
      await immediate()
      t.notOk(settled, `${finish}: repeated open remains pending`)
      if (finish === 'complete') { releaseC() }
      if (finish === 'close') { await h.gate.close() }
      const [first, second] = await Promise.all([opening, repeated])
      if (finish === 'complete') {
        t.equal(first, null, 'startup succeeds after all three complete')
        await h.gate.open()
      } else {
        t.equal(first.code, finish === 'timeout' ? Err.LOCAL_TIMEOUT : Err.NODE_NOT_OPEN)
      }
      t.equal(second, first, `${finish}: both callers receive the same result`)
      t.equal(h.requests.filter((r) => r.message.type === 'open').length, 4,
        `${finish}: owners are opened only once`)
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate holds every output until all three complete in any arrival order', async (t) => {
  const orders = [
    ['procA', 'procB', 'procC'], ['procA', 'procC', 'procB'],
    ['procB', 'procA', 'procC'], ['procB', 'procC', 'procA'],
    ['procC', 'procA', 'procB'], ['procC', 'procB', 'procA'],
  ]
  for (const delayed of [false, true]) {
    for (const order of orders) {
      const completions = new Map()
      let outputs
      const h = harness({
        local({ to, message, complete, reply }) {
          if (message.type === 'execute') {
            reply(to, executionReply(message))
            return true
          }
          if (message.type !== 'append') { return false }
          outputs = [
            wire.encodePayload({ to: 'b', message: { type: 'append', data: Buffer.from('remote') } }),
            wire.encodePayload({ to: 'apply', message: execution('barrier', message.tickCount) }),
            wire.encodePayload({ to: 'gate', message: { type: 'change', status: status() } }),
            client(message),
          ]
          completions.set(to, () => complete(to, message, outputs))
          return true
        },
      })
      let changes = 0
      h.gate.on('change', () => { changes++ })
      try {
        await h.gate.open()
        let settled = false
        const pending = h.gate.append(Buffer.from('one')).then((value) => {
          settled = true
          return value
        })
        await immediate()
        h.gate.onReceive('b', wire.encodeFrame({ to: 'a', message: { type: 'append' } }))
        for (let i = 0; i < order.length; i++) {
          if (delayed) { setImmediate(completions.get(order[i])) }
          else { completions.get(order[i])() }
          await immediate()
          if (i === 2) { break }
          t.equal(h.sent.length, 0, `${order.join('/')}: remote output waits`)
          t.notOk(h.requests.some(({ message }) => message.type === 'execute'), 'Apply request waits')
          t.equal(changes, 0, 'events wait')
          t.notOk(settled, 'caller result waits')
          t.notOk(h.requests.some(({ message }) => message.type === 'cluster_message'), 'next input waits')
        }
        t.deepEqual(await pending, [3n, 'one'])
        await immediate()
        t.equal(h.sent.length, 1, 'remote output releases once')
        t.equal(changes, 1, 'event releases once')
        const apply = h.requests.find(({ message }) => message.type === 'execute')
        for (const [sent, payload] of [[h.sent[0].frame, outputs[0]], [apply.frame, outputs[1]]]) {
          t.equal(sent.length, payload.length + 16, 'frame retains a 16-byte checksum header')
          t.deepEqual(sent.subarray(0, 8), contribution('procB', payload), 'prefix comes from B')
          t.deepEqual(sent.subarray(8, 16), contribution('procC', payload), 'suffix comes from C')
          t.deepEqual(wire.verifyFrame(sent), payload, 'receiver verifies exact original payload bytes')
        }
        t.equal(h.requests.filter(({ message }) => message.type === 'cluster_message').length, 3,
          'next input fans out after all three complete')
        t.deepEqual(h.errors, [])
      } finally { await h.gate.close() }
    }
  }
  t.end()
})

test('Gate shares one stable input frame across synchronous and delayed fanout', async (t) => {
  for (const delayed of [false, true]) {
    const saved = []
    const h = harness({
      local({ to, message, frame, complete }) {
        if (!isInputType(message.type)) { return false }
        saved.push({ frame, bytes: Buffer.from(frame), message })
        const respond = () => complete(to, message,
          message.type === 'append' ? [client(message)] : [])
        if (delayed) { setImmediate(respond) } else { respond() }
        return true
      },
    })
    try {
      await h.gate.open()
      t.deepEqual(await h.gate.append(Buffer.from('one')), [3n, 'one'])
      t.deepEqual(await h.gate.append(Buffer.from('two')), [3n, 'two'])
      await immediate()
      const inputs = h.requests.filter((request) =>
        request.to === 'procA' && isInputType(request.message.type))
      t.equal(inputs.length, 3, 'startup and both commands complete')
      for (const input of inputs) {
        const fanout = saved.filter(({ message }) => message.cid === input.message.cid)
        t.equal(fanout.length, 3, 'every Proc receives the input')
        t.ok(fanout.every(({ frame }) => frame === input.frame),
          'all Procs receive the same encoded Buffer')
        t.ok(fanout.every(({ frame, bytes }) => frame.equals(bytes)),
          'interleaved replies and later inputs cannot overwrite retained frames')
      }
      t.equal(h.errors.length, 0)
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate does not rehash output bytes damaged after agreement', async (t) => {
  for (const destination of ['b', 'apply']) {
    const marker = Buffer.from('amount=1')
    const original = wire.encodePayload({ to: destination, message: destination === 'apply'
      ? { ...execution('damaged-after-agreement'), items: [marker] }
      : { type: 'append', data: marker } })
    const applyFrames = []
    let aPayload
    const h = harness({
      local({ to, message }) {
        if (message.type !== 'append') { return false }
        const outputs = [
          wire.encodePayload({ to: 'gate', message: { type: 'commit', seq: 3n, status: status() } }),
          original,
          client(message),
        ]
        const response = wire.encodeFrame({ type: 'ack', cid: message.cid, tickCount: message.tickCount,
          outputs: to === 'procA' ? outputs : outputs.map((payload) => contribution(to, payload)),
        })
        if (to === 'procA') { aPayload = wire.decodeFrame(response).outputs[1] }
        h.gate.onReceive(to, response)
        return true
      },
    })
    const localSend = h.gate._localSend
    h.gate._localSend = (to, bytes) => {
      if (to === 'apply' && wire.decodePayload(bytes.subarray(16)).to === 'apply') {
        applyFrames.push(bytes)
        return
      }
      return localSend(to, bytes)
    }
    h.gate.on('commit', () => {
      const index = aPayload.indexOf(marker)
      assert.ok(index >= 0)
      aPayload[index + marker.length - 1] ^= 1
    })
    try {
      await h.gate.open()
      await h.gate.append(Buffer.from('work'))
      const frames = destination === 'apply' ? applyFrames : h.sent.map(({ frame }) => frame)
      t.equal(frames.length, 1, `${destination}: one frame reaches the receiving boundary`)
      t.deepEqual(frames[0].subarray(0, 16), wire.checksum(original), 'original B/C checksum is retained')
      t.notDeepEqual(frames[0].subarray(16), original, 'fault changed the agreed payload')
      t.throws(() => wire.verifyFrame(frames[0]), (err) => err.code === Err.FRAME_CORRUPT,
        'receiver rejects damage instead of accepting a recomputed checksum')
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate waits for A before comparing the independent checksum halves', async (t) => {
  let releaseA
  const h = harness({
    local({ to, message, reply, complete }) {
      if (message.type !== 'append') { return false }
      if (to === 'procA') { releaseA = () => complete(to, message, [client(message)]) }
      else {
        reply(to, { type: 'ack', cid: message.cid, tickCount: message.tickCount,
          outputs: [Buffer.alloc(8, to === 'procB' ? 1 : 2)] })
      }
      return true
    },
  })
  try {
    await h.gate.open()
    const pending = h.gate.append(Buffer.from('bad')).catch((err) => err)
    await immediate()
    t.deepEqual(h.errors, [], 'different halves alone cannot be compared')
    t.ok(h.gate.isOpen)
    releaseA()
    t.equal((await pending).code, Err.LOCAL_DIVERGENCE)
    t.equal(h.errors[0].code, Err.LOCAL_DIVERGENCE)
    t.notOk(h.gate.isOpen)
  } finally { await h.gate.close() }
  t.end()
})

test('Gate rejects different output counts and preserves received error stacks', async (t) => {
  const h = harness({
    local({ to, message, complete }) {
      if (message.type !== 'append') { return false }
      complete(to, message, to === 'procA' ? [client(message)] : [])
      return true
    },
  })
  await h.gate.open()
  await assert.rejects(h.gate.append(Buffer.alloc(0)), { code: Err.LOCAL_DIVERGENCE })
  await h.gate.close()
  const second = harness({ outputs: (input) => input.type === 'append'
    ? [client(input, [[1, 'apply failed', Err.APPLY_ERROR, null, 'original application stack']])] : [] })
  await second.gate.open()
  try { await second.gate.append(Buffer.alloc(0)); t.fail('expected rejection') } catch (err) {
    t.equal(err.message, 'apply failed')
    t.equal(err.stack, 'original application stack')
    t.equal(err.code, Err.APPLY_ERROR)
  }
  await second.gate.close()
  t.end()
})

test('Gate assigns monotonic timestamps and counts every ordered input', async (t) => {
  const now = Date.now
  const h = harness({ outputs: (input) => input.type === 'append' ? [client(input)] : [] })
  try {
    Date.now = () => 2_000
    await h.gate.open()
    Date.now = () => 1_000
    await h.gate.append(Buffer.from('second'))
    Date.now = () => 3_000
    await h.gate.appendBatch([Buffer.from('third'), Buffer.from('fourth')])
    const a = h.requests.filter((r) => r.to === 'procA' && isInputType(r.message.type))
    t.deepEqual(a.map((r) => r.message.tickCount), [1n, 2n, 3n])
    t.deepEqual(a.map((r) => r.message.timems), [2_000, 2_000, 3_000])
    for (const input of a) {
      t.equal(h.requests.filter((r) => r.message.cid === input.message.cid &&
        r.message.timems === input.message.timems).length, 3, 'fanout shares timestamp')
    }
  } finally {
    Date.now = now
    await h.gate.close()
  }
  t.end()
})

test('Gate coalesces ticks while a Proc completion is delayed', async (t) => {
  let releaseC
  const h = harness({
    opts: { tickInterval: 2 },
    outputs: (input) => input.type === 'append' ? [client(input)] : [],
    local({ to, message, complete }) {
      if (to !== 'procC' || message.type !== 'append') { return false }
      releaseC = () => complete(to, message, [client(message)])
      return true
    },
  })
  await h.gate.open()
  const pending = h.gate.append(Buffer.from('hold'))
  await delay(15)
  t.equal(h.gate._queue.filter((work) => work.input.type === 'tick').length, 1,
    'only one clock input waits')
  const sentBefore = Date.now()
  releaseC()
  await pending
  await immediate()
  const tick = h.requests.find((r) => r.to === 'procA' && r.message.type === 'tick')
  t.ok(tick.message.timems >= sentBefore, 'tick gets current time at dispatch')
  await h.gate.close()
  t.end()
})

test('Gate Apply watchdog expires while a later input waits for a third Proc', async (t) => {
  const execute = wire.encodePayload({ to: 'apply', message: execution('apply-timeout') })
  const h = harness({
    opts: { applyTimeout: 30 },
    outputs: (input) => input.type === 'append' ? [execute] : [],
    local({ to, message }) {
      return message.type === 'execute' || (to === 'procC' && message.type === 'cluster_message')
    },
  })
  try {
    await h.gate.open()
    const pending = h.gate.append(Buffer.from('item')).catch((err) => err)
    await immediate()
    h.gate.onReceive('b', wire.encodeFrame({ to: 'a', message: { type: 'append' } }))
    await immediate()
    t.ok(h.requests.some(({ to, message }) => to === 'procC' && message.type === 'cluster_message'),
      'a later input is waiting for its third completion')
    t.equal((await pending).code, Err.APPLY_TIMEOUT)
    t.equal(h.errors[0].code, Err.APPLY_TIMEOUT, 'native watchdog runs independently of barrier')
    const request = h.requests.find((r) => r.message.type === 'execute')
    t.ok(request.frame.subarray(16).equals(execute), 'Apply receives unchanged agreed bytes')
  } finally { await h.gate.close() }
  t.end()
})

test('Gate cancels Apply timeout when reply arrives before queued delivery', async (t) => {
  let clientId
  let replyApply
  let releaseC
  const execute = wire.encodePayload({ to: 'apply', message: execution('apply-fast') })
  const h = harness({
    opts: { applyTimeout: 25 },
    outputs(input) {
      if (input.type === 'append') { clientId = input.clientId; return [execute] }
      if (input.type === 'apply_result') {
        return [client({ clientId, items: [Buffer.from('done')] })]
      }
      return []
    },
    local({ to, message, reply, complete }) {
      if (message.type === 'execute') {
        replyApply = () => reply(to, { ...executionReply(message), warnings: [{
          message: 'item rejected', code: Err.APPLY_ERROR, sqlCode: null, stack: 'user trace',
        }] })
        return true
      }
      if (to === 'procC' && message.type === 'cluster_message') {
        releaseC = () => complete(to, message)
        return true
      }
      return false
    },
  })
  try {
    await h.gate.open()
    const pending = h.gate.append(Buffer.from('work'))
    await immediate()
    h.gate.onReceive('b', wire.encodeFrame({ to: 'a', message: { type: 'append' } }))
    await immediate()
    replyApply()
    await delay(40)
    t.equal(h.errors.length, 0, 'reply arrival cancels timeout despite delayed delivery')
    t.equal(h.warnings.length, 0, 'Gate does not emit diagnostics bundled in an Apply reply')
    t.equal(h.requests.filter((r) => r.message.type === 'apply_result').length, 0)
    releaseC()
    t.deepEqual(await pending, [3n, 'done'])
  } finally { await h.gate.close() }
  t.end()
})

test('Gate stop cancels local work without coordinating owner shutdown', async (t) => {
  const h = harness({
    local({ message }) { return message.type === 'append' },
  })
  try {
    await h.gate.open()
    const command = h.gate.append(Buffer.from('pending')).catch((err) => err)
    const queued = h.gate.append(Buffer.from('queued')).catch((err) => err)
    const changed = h.gate.awaitEvent('change').catch((err) => err)
    let closed = false
    const closing = h.gate.awaitEvent('close').then(() => { closed = true })
    await immediate()
    h.gate.stop()
    t.notOk(h.gate.isOpen, 'stopping is immediate')
    t.equal(h.gate._requests.size, 0, 'stop finishes every outstanding RPC')
    t.equal(h.gate._clients.size, 0, 'each pending or queued client removes itself during stop')
    t.equal(h.gate._eventWaiters.size, 0, 'event waits remove themselves during stop')
    t.equal((await command).code, Err.NODE_NOT_OPEN)
    t.equal((await queued).code, Err.NODE_NOT_OPEN)
    t.equal((await changed).code, Err.NODE_NOT_OPEN)
    h.gate.stop()
    t.notOk(closed, 'close wait still requires coordinated cleanup')
    t.deepEqual(h.errors, [], 'explicit stop emits no error')
    t.equal(h.requests.filter(({ message }) => message.type === 'close').length, 0,
      'stop sends no close requests')
    const reopened = await h.gate.open().catch((err) => err)
    t.equal(reopened.code, Err.NODE_NOT_OPEN, 'stopped Gate cannot reopen')
    await h.gate.close()
    await closing
    t.ok(closed, 'explicit coordinated close remains available')
  } finally { await h.gate.close() }
  t.end()
})

test('Gate stop preserves an earlier fatal error', async (t) => {
  const h = harness()
  await h.gate.open()
  h.gate.onReceive('procA', Buffer.alloc(17))
  const failure = h.errors[0]
  h.gate.stop()
  h.gate.stop()
  const rejected = await h.gate.append(Buffer.from('late')).catch((err) => err)
  t.equal(rejected, failure)
  t.equal(rejected.code, Err.FRAME_CORRUPT)
  t.equal(h.errors.length, 1)
  await h.gate.close()
  t.end()
})

test('Gate rejects corrupt or malformed local replies and closes pending public work', async (t) => {
  const checksum = wire.encodeFrame({ type: 'ack', cid: 'corrupt-local-reply' })
  checksum[checksum.length - 1] ^= 1
  const invalid = [
    ['checksum', checksum, Err.FRAME_CORRUPT],
    ['decode', frame(Buffer.from([0xc4, 2, 0x61])), Err.FRAME_CORRUPT],
    ['header', wire.encodeFrame({ type: 'ack', cid: 3 }), Err.RPC_ILLEGAL],
  ]
  for (const [label, frame, code] of invalid) {
    const h = harness()
    let closed = false
    let fatals = 0
    h.gate.on('close', () => { closed = true })
    h.gate.on('fatal', () => { fatals++ })
    await h.gate.open()
    const pending = h.gate.append(Buffer.from('pending')).catch((err) => err)
    h.gate.onReceive('procA', frame)
    t.equal((await pending).code, code, `${label}: pending work rejects`)
    t.equal(h.errors[0].code, code)
    t.notOk(h.gate.isOpen, 'fatal failure stops Gate')
    t.notOk(closed, 'stopping Gate does not claim component shutdown')
    t.equal(fatals, 0, 'fatal handling is internal')
    t.notOk(h.requests.some((r) => r.message.type === 'close'), 'fatal path sends no close requests')
    await h.gate.close()
    t.ok(closed, 'explicit close waits for owner completion')
    t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4)
  }
  t.end()
})

test('Gate drops corrupt remote frames and invalid senders without cancelling pending commands', async (t) => {
  const completions = []
  const h = harness({
    local({ to, message, complete }) {
      if (message.type !== 'append') { return false }
      completions.push(() => complete(to, message, [client(message)]))
      return true
    },
  })
  try {
    await h.gate.open()
    let settled = false
    const pending = h.gate.append(Buffer.from('pending')).then(
      (value) => { settled = true; return value },
      (error) => { settled = true; return error },
    )
    await immediate()
    const tickCount = h.gate.tickCount
    const good = wire.encodeFrame({ to: 'a', message: { type: 'append', from: 'b' } })
    const badChecksum = Buffer.from(good)
    badChecksum[0] ^= 1
    const badPayload = Buffer.from(good)
    badPayload[badPayload.length - 1] ^= 1
    const frames = [badChecksum, badPayload, Buffer.alloc(0), Buffer.alloc(16), null]
    for (const [index, frame] of frames.entries()) {
      h.gate.onReceive('b', frame)
      t.equal(h.warnings.length, index + 1, 'one warning per rejected frame')
      t.equal(h.warnings[index].code, Err.FRAME_CORRUPT)
      t.match(h.warnings[index].message, /^cluster frame from b: frame (checksum differs|is incomplete)$/)
    }
    for (const from of ['unknown', h.gate.id]) {
      const count = h.warnings.length
      h.gate.onReceive(from, good)
      t.equal(h.warnings.length, count + 1, `${from}: one warning for invalid sender`)
      t.equal(h.warnings[count].code, Err.RPC_ILLEGAL)
      t.equal(h.warnings[count].message, 'cluster frame has invalid sender')
    }
    await immediate()
    t.deepEqual(h.errors, [], 'remote corruption emits no error')
    t.ok(h.gate.isOpen, 'Gate stays open')
    t.notOk(settled, 'pending command remains pending')
    t.equal(h.gate.tickCount, tickCount, 'rejected frames do not advance the ordered clock')
    t.equal(h.sent.length, 0, 'corrupt requests receive no response')
    for (const complete of completions) { complete() }
    t.deepEqual(await pending, [3n, 'pending'], 'healthy completion still resolves the command')
    await immediate()
    t.notOk(h.requests.some(({ message }) => message.type === 'cluster_message'), 'corrupt frames are never fanned out')
    h.gate.onReceive('b', good)
    await immediate()
    const delivered = h.requests.filter(({ message }) => message.type === 'cluster_message')
    t.equal(delivered.length, 3, 'a subsequent valid frame reaches all Procs')
    t.ok(delivered.every(({ message }) => message.frame.equals(good)), 'valid frame bytes are preserved')
  } finally { await h.gate.close() }
  t.end()
})

test('remote checksum warning listeners may close Gate or fail fatally', async (t) => {
  for (const action of ['close', 'throw']) {
    const h = harness()
    const failure = new Error('warning listener failed')
    let closing
    try {
      await h.gate.open()
      h.gate.on('warn', () => {
        if (action === 'throw') { throw failure }
        closing = h.gate.close()
      })
      h.gate.onReceive('b', Buffer.alloc(17))
      t.equal(h.warnings.length, 1)
      t.notOk(h.gate.isOpen, `${action}: listener stops Gate`)
      t.equal(h.errors.length, action === 'throw' ? 1 : 0)
      if (action === 'throw') { t.equal(h.errors[0].stack, failure.stack) }
      await closing
      t.notOk(h.requests.some(({ message }) => message.type === 'cluster_message'), 'no work follows the warning')
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate transport failures warn and close cancels awaiting leader', async (t) => {
  const h = harness({
    outputs(input) {
      return input.type === 'append' ? [wire.encodePayload({ to: 'b', message: { type: 'append' } }), client(input)] : []
    },
    send() { throw new Error('lost transport') },
  })
  await h.gate.open()
  t.deepEqual(await h.gate.append(Buffer.from('one')), [3n, 'one'])
  t.equal(h.warnings[0].code, Err.SEND_ERROR)
  const waiting = h.gate.awaitLeader().catch((err) => err)
  await h.gate.close()
  t.equal((await waiting).code, Err.NODE_NOT_OPEN)
  t.equal(h.errors.length, 0)
  t.end()
})

test('Gate close waits observe successful cleanup across lifecycle states', async (t) => {
  let release
  const h = harness({
    local({ to, message, reply }) {
      if (to !== 'procC' || message.type !== 'close') { return false }
      release = () => reply(to, { type: 'ack', cid: message.cid })
      return true
    },
  })
  let resolved = 0
  let closes = 0
  let reentrant
  const wait = () => h.gate.awaitEvent('close').then((value) => { resolved++; return value })
  h.gate.once('close', () => { closes++; reentrant = wait() })
  try {
    const before = wait()
    await immediate()
    t.equal(resolved, 0, 'waiting before open stays pending')
    t.equal(h.requests.length, 0, 'waiting does not start or close components')
    await h.gate.open()
    const running = wait()
    const closing = h.gate.close()
    const during = wait()
    await immediate()
    t.equal(typeof release, 'function', 'one owner has not acknowledged close')
    t.equal(resolved, 0, 'all close waits survive stopping Gate until cleanup succeeds')
    t.equal(h.gate._eventWaiters.size, 0, 'close waits are not shutdown cancellations')
    t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4,
      'only explicit close sends requests')
    h.gate.stop()
    t.equal(h.gate.close(), closing, 'stop during cleanup preserves the existing close attempt')
    t.equal(h.gate._requests.size, 1, 'the delayed close RPC remains pending after repeated stop')
    release()
    await closing
    t.equal(h.gate._requests.size, 0, 'the close ACK removes the final RPC')
    t.deepEqual(await Promise.all([before, running, during, reentrant]),
      [undefined, undefined, undefined, undefined], 'all lifecycle waits resolve without a value')
    t.equal(resolved, 4, 'registration inside a close listener resolves too')
    t.equal(h.gate.listenerCount('close'), 0, 'completed waits remove their listeners')
    const requests = h.requests.length
    t.equal(await h.gate.awaitEvent('close'), undefined, 'late registration resolves immediately')
    await h.gate.close()
    t.equal(h.requests.length, requests, 'late waits and repeated close send no requests')
    t.equal(closes, 1, 'successful shutdown emits close once')
    await assert.rejects(h.gate.open(), { code: Err.NODE_NOT_OPEN })
    t.equal(h.requests.length, requests, 'a stopped Gate cannot reopen')
  } finally { await h.gate.close() }
  t.end()
})

test('Gate close waits survive fatal stop until explicit cleanup', async (t) => {
  const h = harness()
  await h.gate.open()
  let resolved = 0
  const before = h.gate.awaitEvent('close').then(() => { resolved++ })
  const ordinary = h.gate.awaitEvent('cmd').catch((err) => err)
  h.gate.onReceive('procA', Buffer.alloc(0))
  const after = h.gate.awaitEvent('close').then(() => { resolved++ })
  await immediate()
  t.equal(resolved, 0, 'neither existing nor new close waits resolve on fatal stop')
  t.equal((await ordinary), h.errors[0], 'ordinary waits reject with the fatal error')
  t.equal(h.errors[0].code, Err.FRAME_CORRUPT)
  t.notOk(h.requests.some((r) => r.message.type === 'close'), 'fatal stop and waits send no close requests')
  await h.gate.close()
  await Promise.all([before, after])
  t.equal(resolved, 2, 'explicit successful cleanup resolves both waits')
  t.equal(h.gate.listenerCount('close'), 0, 'resolved waits leave no listeners')
  t.end()
})

test('Gate close waits reject permanently after owner errors or timeouts', async (t) => {
  for (const failure of ['err', 'timeout']) {
    let failing = true
    let lateReply
    const reason = new Err.ErrorWithCode('owner close failed', Err.SQLITE_ERROR)
    const h = harness({
      opts: { localTimeout: 15 },
      local({ to, message, reply }) {
        if (!failing || to !== 'apply' || message.type !== 'close') { return false }
        if (failure === 'err') {
          reply(to, { type: 'err', cid: message.cid, error: wire.errorToWire(reason) })
        }
        lateReply = () => reply(to, { type: 'ack', cid: message.cid })
        return true
      },
    })
    let closes = 0
    let matches = 0
    h.gate.on('close', () => { closes++ })
    const wait = () => h.gate.awaitEvent('close', () => { matches++; return false })
      .then(() => null, (err) => err)
    const before = wait()
    await h.gate.open()
    const running = wait()
    const closing = h.gate.close()
    const during = wait()
    t.equal(h.gate.close(), closing, 'concurrent calls share the first attempt')
    const error = await closing.then(() => null, (err) => err)
    t.equal(error?.code, failure === 'err' ? Err.SQLITE_ERROR : Err.LOCAL_TIMEOUT,
      `${failure}: explicit close still rejects`)
    if (failure === 'err') {
      t.equal(error.message, reason.message, 'owner close error message is preserved')
      t.equal(error.stack, reason.stack, 'owner close error stack is preserved')
    }
    for (const waiting of [before, running, during, wait()]) {
      t.equal(await waiting, error, `${failure}: every close wait receives the cleanup failure`)
    }
    t.equal(matches, 0, 'failed cleanup rejects without invoking a close condition')
    t.equal(closes, 0, `${failure}: unsuccessful cleanup emits no close`)
    t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4,
      `${failure}: waiting does not retry cleanup`)
    failing = false
    t.equal(h.gate.close(), closing, 'later calls retain the failed attempt')
    t.equal(await h.gate.close().catch((err) => err), error, 'the original failure is retained')
    lateReply()
    await immediate()
    t.equal(closes, 0, 'a late ACK cannot turn failed cleanup into success')
    t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4,
      'recovering transport does not trigger another close request')
    t.equal(h.gate.listenerCount('close'), 1, `${failure}: only the persistent listener remains`)
    t.equal(h.gate._closeWaiters.size, 0, 'failed close waiters are removed')
    t.equal(h.errors.length, 0, `${failure}: Gate does not duplicate owner diagnostics`)
  }
  t.end()
})

test('Gate close waits retain condition behavior before and after cleanup', async (t) => {
  const h = harness()
  const error = new Error('close condition failed')
  const captureThrow = (reason) => h.gate.awaitEvent('close', () => { throw reason }).then(
    () => ({ fulfilled: true }),
    (error) => ({ error }),
  )
  let calls = 0
  let resolved = false
  const rejectMatch = (value) => {
    t.equal(value, undefined, 'close conditions receive no payload')
    calls++
    return false
  }
  h.gate.awaitEvent('close', rejectMatch).then(() => { resolved = true })
  const early = [captureThrow(undefined), captureThrow(error)]
  await h.gate.close()
  for (const [index, reason] of [undefined, error].entries()) {
    t.deepEqual(await early[index], { error: reason }, 'early condition rejects unchanged')
    t.deepEqual(await captureThrow(reason), { error: reason }, 'late condition rejects unchanged')
  }
  h.gate.awaitEvent('close', rejectMatch).then(() => { resolved = true })
  t.equal(await h.gate.awaitEvent('close', () => 'match'), undefined, 'truthy condition resolves late wait')
  await h.gate.close()
  t.notOk(resolved, 'false conditions remain pending after successful close')
  t.equal(calls, 2, 'each false condition runs once without a synthetic close event')
  t.equal(h.gate.listenerCount('close'), 2, 'only unmatched conditions retain listeners')
  t.equal(h.gate._eventWaiters.size, 0, 'close conditions never register shutdown cancellation')
  t.equal(h.errors.length, 0, 'condition failures are returned only to their callers')
  h.gate.removeAllListeners('close')
  t.end()
})

test('Gate awaitEvent preserves every thrown value', async (t) => {
  const h = harness({
    outputs: (input) => input.type === 'append' ? [wire.encodePayload({
      to: 'gate', message: { type: 'cmd', cmdSeq: 3n, cmdCount: 1, status: status() },
    }), client(input)] : [],
  })
  const error = new Error('condition failed')
  const stack = error.stack
  const coded = new Err.ErrorWithCode('coded condition failure', Err.ARGUMENT_ILLEGAL)
  try {
    await h.gate.open()
    for (const reason of [undefined, null, false, 0, 0n, '', NaN, error, coded]) {
      const label = `${typeof reason}: ${String(reason)}`
      const waiting = h.gate.awaitEvent('cmd', () => { throw reason }).then(
        (value) => ({ status: 'fulfilled', value }),
        (error) => ({ status: 'rejected', error }),
      )
      await h.gate.append(Buffer.from('work'))
      const result = await waiting
      t.equal(result.status, 'rejected', `${label}: rejects`)
      t.ok(Object.is(result.error, reason), `${label}: preserves the exact thrown value`)
      t.equal(h.gate.listenerCount('cmd'), 0, `${label}: removes the listener`)
      t.equal(h.gate._eventWaiters.size, 0, `${label}: removes the shutdown wait`)
    }
    t.equal(error.stack, stack, 'original stack is unchanged')
    t.equal(h.errors.length, 0, 'condition failures belong to the waiting caller')
  } finally { await h.gate.close() }
  t.end()
})

test('Gate awaitEvent filters events and rejects if its condition closes Gate', async (t) => {
  const h = harness({
    outputs: (input) => input.type === 'append' ? [wire.encodePayload({
      to: 'gate', message: { type: 'cmd', cmdSeq: 3n, cmdCount: 1, status: status() },
    }), client(input)] : [],
  })
  try {
    await h.gate.open()
    let calls = 0
    let matched
    const waiting = h.gate.awaitEvent('cmd', (event) => {
      calls++
      if (calls === 1) { return false }
      matched = event
      return true
    })
    await h.gate.append(Buffer.from('skip'))
    t.equal(h.gate.listenerCount('cmd'), 1, 'unmatched event keeps waiting')
    await h.gate.append(Buffer.from('match'))
    t.equal(await waiting, matched, 'returns the exact matching event')
    await h.gate.append(Buffer.from('later'))
    t.equal(calls, 2, 'completed condition is not called again')
    t.equal(h.gate._eventWaiters.size, 0)

    const closing = h.gate.awaitEvent('cmd', () => {
      h.gate.close()
      return true
    })
    const rejected = assert.rejects(closing, { code: Err.NODE_NOT_OPEN })
    await assert.rejects(h.gate.append(Buffer.from('close')), { code: Err.NODE_NOT_OPEN })
    await rejected
    t.equal(h.gate.listenerCount('cmd'), 0, 'shutdown removes the listener')
    t.equal(h.gate._eventWaiters.size, 0, 'shutdown removes the wait')
  } finally { await h.gate.close() }
  t.end()
})

test('Gate rejects differing startup selections before adoption', async (t) => {
  const h = harness({
    local({ to, message, reply }) {
      if (message.type !== 'capture') { return false }
      reply(to, { type: 'ack', cid: message.cid, selection: {
        state: Buffer.alloc(16, to === 'procC' ? 1 : 0), receipt: Buffer.alloc(16), donor: 'procA',
      } })
      return true
    },
  })
  await assert.rejects(h.gate.open(), { code: Err.LOCAL_DIVERGENCE })
  t.notOk(h.requests.some((r) => r.message.type === 'adopt'), 'no DB is changed by adoption')
  t.equal(h.requests.filter((r) => r.message.type === 'close').length, 0,
    'failed open leaves owner cleanup to the caller')
  await h.gate.close()
  t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4,
    'explicit close still coordinates all owners after failed open')
  t.end()
})

test('Gate missing completion or stop releases none of the waiting outputs', async (t) => {
  for (const missing of procs) {
    for (const finish of ['timeout', 'stop']) {
      let late
      const h = harness({
        opts: { localTimeout: 15 },
        local({ to, message, complete }) {
          if (message.type !== 'append') { return false }
          const outputs = [
            wire.encodePayload({ to: 'b', message: { type: 'append' } }),
            wire.encodePayload({ to: 'apply', message: execution('missing') }),
            wire.encodePayload({ to: 'gate', message: { type: 'change', status: status() } }),
            client(message),
          ]
          if (to === missing) { late = () => complete(to, message, outputs) }
          else { complete(to, message, outputs) }
          return true
        },
      })
      let changed = false
      h.gate.on('change', () => { changed = true })
      try {
        await h.gate.open()
        const pending = h.gate.append(Buffer.from('wait')).catch((err) => err)
        await immediate()
        if (finish === 'stop') { h.gate.stop() }
        t.equal((await pending).code, finish === 'timeout' ? Err.LOCAL_TIMEOUT : Err.NODE_NOT_OPEN)
        t.equal(h.requests.filter((r) => r.to === missing && r.message.type === 'append').length, 1,
          `${missing}/${finish}: no input retry`)
        t.equal(h.gate._requests.size, 0, 'shutdown clears the remaining requests')
        late()
        await immediate()
        t.equal(h.sent.length, 0, 'no remote send, even after a late completion')
        t.notOk(h.requests.some(({ message }) => message.type === 'execute'), 'no Apply execution')
        t.notOk(changed, 'no event publication')
        t.equal(h.errors.length, finish === 'timeout' ? 1 : 0)
      } finally { await h.gate.close() }
    }
  }
  t.end()
})

test('Gate registers synchronous close before hello continuation resumes', async (t) => {
  const h = harness()
  const opening = h.gate.open().catch((err) => err)
  await h.gate.close()
  t.equal((await opening).code, Err.NODE_NOT_OPEN)
  t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4)
  t.notOk(h.requests.some((r) => r.message.type === 'open'), 'closed Gate does not start DB work')
  t.end()
})

test('Gate drain closes owners after agreed drain result and rejects new commands', async (t) => {
  let release
  const events = []
  const h = harness({
    local({ to, message, complete }) {
      if (message.type !== 'drain') { return false }
      const outputs = [
        wire.encodePayload({ to: 'gate', message: { type: 'ack', clientId: message.clientId } }),
        wire.encodePayload({ to: 'gate', message: { type: 'change', status: status() } }),
      ]
      if (to === 'procB') { release = () => complete(to, message, outputs) }
      else { complete(to, message, outputs) }
      return true
    },
  })
  h.gate.on('change', () => {
    events.push('change')
    t.ok(h.gate.isOpen, 'remaining event is delivered before drain stops Gate')
    t.notOk(h.requests.some(({ message }) => message.type === 'close'), 'owners have not been asked to close')
  })
  h.gate.on('close', () => events.push('close'))
  await h.gate.open()
  const draining = h.gate.drainCmd()
  await assert.rejects(h.gate.append(Buffer.alloc(0)), { code: Err.DRAINING })
  await immediate()
  t.equal(h.requests.filter((r) => r.message.type === 'close').length, 0,
    'drain waits for the final completion before closing owners')
  release()
  await draining
  t.notOk(h.gate.isOpen)
  t.equal(h.gate._clients.size, 0, 'successful drain removes its pending client')
  t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4)
  t.deepEqual(events, ['change', 'close'])
  t.end()
})

test('Gate stop during drain output delivery takes precedence over deferred success', async (t) => {
  const h = harness({ outputs: (input) => input.type === 'drain' ? [
    wire.encodePayload({ to: 'gate', message: { type: 'ack', clientId: input.clientId } }),
    wire.encodePayload({ to: 'gate', message: { type: 'change', status: status() } }),
  ] : [] })
  h.gate.on('change', () => h.gate.stop())
  try {
    await h.gate.open()
    await assert.rejects(h.gate.drainCmd(), { code: Err.NODE_NOT_OPEN })
    await immediate()
    t.equal(h.gate._clients.size, 0, 'shutdown settles the client before deferred success runs')
    t.notOk(h.requests.some(({ message }) => message.type === 'close'),
      'the cancelled success does not initiate owner cleanup')
    t.deepEqual(h.errors, [])
  } finally { await h.gate.close() }
  t.end()
})

test('Gate warning listener may throw or close without dispatching later Apply work', async (t) => {
  const h = harness({
    outputs(input) {
      if (input.type !== 'append') { return [] }
      return [
        wire.encodePayload({ to: 'b', message: { type: 'append' } }),
        wire.encodePayload({ to: 'apply', message: execution('warn-close') }),
      ]
    },
    send() { throw new Error('transport failed') },
  })
  h.gate.on('warn', () => { throw new Error('listener failed') })
  h.gate.on('warn', () => { h.gate.close().catch(() => {}) })
  await h.gate.open()
  await assert.rejects(h.gate.append(Buffer.from('work')), { message: 'listener failed' })
  await h.gate.close()
  t.equal(h.warnings.length, 1)
  t.equal(h.warnings[0].code, Err.SEND_ERROR, 'Gate reports its own transport warning')
  t.equal(h.errors[0].message, 'listener failed')
  t.equal(h.requests.filter((r) => r.message.type === 'execute').length, 0)
  t.end()
})

test('Gate listener failures stop work before reporting and preserve later listeners', async (t) => {
  for (const type of ['change', 'warn']) {
    const h = harness({
      outputs: (input) => input.type === 'append' ? [wire.encodePayload({
        to: type === 'warn' ? 'b' : 'gate', message: { type: type === 'warn' ? 'append' : 'change', status: status() },
      }), client(input)] : [],
      send() { throw new Error('transport warning') },
    })
    const original = new Error(`${type} listener failed`)
    let delivered = 0
    h.gate.prependListener('error', () => { throw new Error('error listener failed') })
    h.gate.on('error', () => {
      t.notOk(h.gate.isOpen, 'error listeners see stopped admission')
      h.gate.close().catch(() => {})
    })
    h.gate.once(type, () => { throw original })
    h.gate.on(type, () => { delivered++ })
    try {
      await h.gate.open()
      const first = h.gate.append(Buffer.from('first')).catch((err) => err)
      const queued = h.gate.append(Buffer.from('queued')).catch((err) => err)
      const failure = await first
      t.equal(failure, h.errors[0], `${type}: callers and listeners share the terminal error`)
      t.ok(failure instanceof Err.ErrorWithCode)
      t.equal(failure.message, original.message)
      t.equal(failure.stack, original.stack)
      t.equal(await queued, failure, 'queued work is canceled with the same error')
      t.equal(delivered, 1, `${type}: later event listener still runs`)
      t.equal(await h.gate.append(Buffer.from('second')).catch((err) => err), failure)
      t.equal(h.requests.filter(({ message, to }) =>
        to === 'procA' && message.type === 'append').length, 1,
      'queued and subsequent commands never reach the protocol owners')
      t.equal(h.errors.length, 1, `${type}: once listener is not called again`)
      t.equal(delivered, 1, `${type}: subsequent work cannot publish another event`)
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate reports close listener failures without repeating shutdown', async (t) => {
  const h = harness()
  const events = []
  const failure = new Error('close listener failed')
  let later = 0
  h.gate.on('close', () => { events.push('close'); throw failure })
  h.gate.on('close', () => { later++ })
  h.gate.on('error', (err) => {
    events.push('error')
    t.notOk(h.gate.isOpen)
    t.equal(err.message, failure.message)
    t.equal(err.stack, failure.stack)
    h.gate.close().catch(() => {})
  })
  try {
    await h.gate.open()
    await h.gate.close()
    await h.gate.close()
    t.deepEqual(events, ['close', 'error'], 'the listener failure follows the close that triggered it')
    t.equal(later, 1, 'later close listeners still run exactly once')
    t.equal(h.requests.filter(({ message }) => message.type === 'close').length, 4,
      'reentrant close does not repeat owner shutdown')
  } finally { await h.gate.close() }
  t.end()
})

test('Gate rejects diagnostic and unknown event outputs instead of forwarding them', async (t) => {
  for (const type of ['warn', 'error', 'fatal', 'unknown']) {
    const h = harness({
      outputs: (input) => input.type === 'append' ? [wire.encodePayload({
        to: 'gate', message: { type, error: { message: 'Proc diagnostic', code: Err.REPAIR_OUTSIDE } },
      }), client(input)] : [],
    })
    let forwarded = 0
    h.gate.on('fatal', () => { forwarded++ })
    h.gate.on('unknown', () => { forwarded++ })
    try {
      await h.gate.open()
      await assert.rejects(h.gate.append(Buffer.from(type)), { code: Err.RPC_ILLEGAL })
      t.equal(h.errors.length, 1, `${type}: Gate reports only its invalid-output error`)
      t.equal(h.errors[0].code, Err.RPC_ILLEGAL)
      t.equal(h.warnings.length, 0)
      t.equal(forwarded, 0, 'no generic event forwarding')
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate preserves supported protocol events', async (t) => {
  const status = {
    state: 'leader', leader: 'a', term: 1n, seq: 4n,
    commitSeq: 4n, commitTerm: 1n, followers: ['b'],
  }
  const messages = [
    { type: 'change' },
    { type: 'commit', seq: 3n },
    { type: 'cmd', cmdSeq: 3n, cmdCount: 1 },
    { type: 'sync', cmdSeq: 3n, syncSeq: 4n, quorum: true, agree: ['a', 'b'], disagree: [] },
  ]
  const h = harness({
    outputs: (input) => input.type === 'append'
      ? [...messages.map((message) => wire.encodePayload({
        to: 'gate', message: { ...message, status },
      })), client(input), client(input), wire.encodePayload({
        to: 'gate', message: { type: 'err', clientId: 'old-request', message: 'late failure', code: Err.NO_LEADER },
      })] : [],
  })
  const events = []
  for (const { type } of messages) { h.gate.on(type, (value) => events.push([type, value])) }
  try {
    await h.gate.open()
    t.deepEqual(await h.gate.append(Buffer.from('work')), [3n, 'work'])
    t.deepEqual(events.map(([type]) => type), messages.map(({ type }) => type))
    t.deepEqual(events[0][1], status, 'change contains only the seven status fields')
    t.equal(events[1][1], 3n)
    t.deepEqual(events[2][1], messages[2])
    t.deepEqual(events[3][1], messages[3])
    t.equal(h.errors.length, 0, 'duplicate and unrelated client results remain ignored')
    t.ok(h.requests.every(({ to }) => [...procs, 'apply'].includes(to)),
      'Gate handles its outputs without an additional localSend')
  } finally { await h.gate.close() }
  t.end()
})

test('both awaitLeader modes observe leader and commit changes without applied progress', async (t) => {
  let committed = false
  const h = harness({
    outputs: (input) => input.type === 'append' ? [wire.encodePayload({
      to: 'gate',
      message: { type: 'change', status: {
        state: 'follower', leader: 'b', term: 2n, seq: 4n,
        commitSeq: committed ? 4n : 3n, commitTerm: committed ? 2n : 1n, followers: [],
      } },
    }), client(input)] : [],
  })
  try {
    await h.gate.open()
    const known = h.gate.awaitLeader()
    let ready = false
    const currentTerm = h.gate.awaitLeader(true).then(() => { ready = true })
    known.catch(() => {})
    currentTerm.catch(() => {})
    await h.gate.append(Buffer.from('leader known'))
    await known
    t.equal(h.gate.leader, 'b')
    t.notOk(ready, 'committed-leader wait remains pending until the current term commits')
    committed = true
    await h.gate.append(Buffer.from('current term committed'))
    await currentTerm
    t.ok(ready)
    t.notOk('appliedSeq' in h.gate)
    t.equal(h.errors.length, 0)
  } finally { await h.gate.close() }
  t.end()
})

test('Gate malformed checksum contribution cannot satisfy an input', async (t) => {
  for (const role of ['procB', 'procC']) {
    const cases = [undefined, null, 'checksum', ...[0, 7, 9, 16, 32].map((size) => Buffer.alloc(size))]
    for (const value of cases) {
      const h = harness({
        local({ to, message, reply }) {
          if (to !== role || message.type !== 'append') { return false }
          reply(to, { type: 'ack', cid: message.cid, tickCount: message.tickCount, outputs: [value] })
          return true
        },
      })
      try {
        await assert.rejects(h.gate.drainCmd(), { code: Err.NODE_NOT_OPEN })
        await h.gate.open()
        await assert.rejects(h.gate.append(Buffer.alloc(0)), { code: Err.RPC_ILLEGAL })
        t.equal(h.errors[0].code, Err.RPC_ILLEGAL,
          `${role}: invalid contribution ${Buffer.isBuffer(value) ? value.length : String(value)} is rejected`)
        t.equal(h.sent.length, 0)
      } finally { await h.gate.close() }
    }
  }
  t.end()
})

test('Gate does not release A/C outputs after synchronous A/B disagreement', async (t) => {
  const output = wire.encodePayload({ to: 'b', message: { type: 'append' } })
  const h = harness({
    local({ to, message, complete, reply }) {
      if (message.type !== 'append') { return false }
      if (to === 'procB') {
        reply(to, { type: 'ack', cid: message.cid, tickCount: message.tickCount, outputs: [Buffer.alloc(8)] })
      } else { complete(to, message, [output]) }
      return true
    },
  })
  await h.gate.open()
  await assert.rejects(h.gate.append(Buffer.from('work')), { code: Err.LOCAL_DIVERGENCE })
  t.equal(h.sent.length, 0, 'first disagreement prevents later matching evidence from releasing outputs')
  await h.gate.close()
  t.end()
})

test('Gate checks every output before releasing a matching prefix of the list', async (t) => {
  for (const mode of ['A payload', 'B half', 'C half', 'swapped halves', 'output order',
    'procA count', 'procB count', 'procC count']) {
    const h = harness({
      local({ to, message, reply }) {
        if (message.type !== 'append') { return false }
        const outputs = [
          wire.encodePayload({ to: 'b', message: { type: 'append' } }),
          wire.encodePayload({ to: 'apply', message: execution('late-disagreement') }),
          wire.encodePayload({ to: 'gate', message: { type: 'change', status: status() } }),
          client(message),
        ]
        let contributions = to === 'procA' ? outputs
          : outputs.map((payload) => contribution(to, payload))
        if (mode === 'A payload' && to === 'procA') {
          contributions[3] = client(message, [[0, 'different']])
        } else if ((mode === 'B half' && to === 'procB') || (mode === 'C half' && to === 'procC')) {
          contributions[3][0] ^= 1
        } else if (mode === 'swapped halves' && to !== 'procA') {
          contributions = outputs.map((payload) => contribution(to === 'procB' ? 'procC' : 'procB', payload))
        } else if (mode === 'output order' && to === 'procC') {
          [contributions[2], contributions[3]] = [contributions[3], contributions[2]]
        } else if (mode === `${to} count`) {
          contributions.pop()
        }
        reply(to, { type: 'ack', cid: message.cid, tickCount: message.tickCount, outputs: contributions })
        return true
      },
    })
    let changed = false
    h.gate.on('change', () => { changed = true })
    try {
      await h.gate.open()
      await assert.rejects(h.gate.append(Buffer.from('work')), { code: Err.LOCAL_DIVERGENCE })
      t.equal(h.errors[0].code, Err.LOCAL_DIVERGENCE, `${mode}: disagreement is terminal`)
      t.equal(h.sent.length, 0, 'matching earlier remote output is not released')
      t.notOk(h.requests.some(({ message }) => message.type === 'execute'), 'earlier Apply request is withheld')
      t.notOk(changed, 'earlier event is withheld')
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate correlates Proc completion ticks before releasing outputs', async (t) => {
  for (const [tickCount, code] of [
    [undefined, Err.RPC_ILLEGAL], [-1n, Err.RPC_ILLEGAL], [2, Err.RPC_ILLEGAL],
    [1n, Err.MONSTER_ILLEGAL], [3n, Err.MONSTER_ILLEGAL],
  ]) {
    const h = harness({
      local({ to, message, reply }) {
        if (message.type !== 'append') { return false }
        const completion = { type: 'ack', cid: message.cid,
          outputs: to === 'procA' ? [client(message)] : [contribution(to, client(message))] }
        if (tickCount !== undefined) { completion.tickCount = tickCount }
        reply(to, completion)
        return true
      },
    })
    try {
      await h.gate.open()
      await assert.rejects(h.gate.append(Buffer.from('work')), { code })
      t.equal(h.errors.length, 1, `${String(tickCount)}: invalid completion stops Gate`)
      t.equal(h.errors[0].code, code)
      t.notOk(h.gate.isOpen)
      t.equal(h.sent.length, 0, 'invalid completion cannot release protocol outputs')
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate correlates Apply replies with the execution tick', async (t) => {
  for (const [tickCount, code] of [
    [undefined, Err.RPC_ILLEGAL], [-1n, Err.RPC_ILLEGAL], [2, Err.RPC_ILLEGAL],
    [1n, Err.MONSTER_ILLEGAL], [3n, Err.MONSTER_ILLEGAL],
  ]) {
    const h = harness({
      outputs: (input) => input.type === 'append'
        ? [wire.encodePayload({ to: 'apply', message: execution('apply-tick', input.tickCount) })] : [],
      local({ to, message, reply }) {
        if (message.type !== 'execute') { return false }
        const completion = executionReply(message)
        if (tickCount === undefined) { delete completion.tickCount }
        else { completion.tickCount = tickCount }
        reply(to, completion)
        return true
      },
    })
    try {
      await h.gate.open()
      await assert.rejects(h.gate.append(Buffer.from('work')), { code })
      t.equal(h.errors.length, 1)
      t.equal(h.errors[0].code, code)
      t.notOk(h.requests.some(({ message }) => message.type === 'apply_result'),
        'bad execution completion is not delivered to Procs')
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate validates startup capture hash lengths before adoption', async (t) => {
  for (const field of ['state', 'receipt']) {
    for (const length of [0, 15, 17]) {
      const h = harness({
        local({ to, message, reply }) {
          if (message.type !== 'capture') { return false }
          reply(to, { type: 'ack', cid: message.cid, selection: {
            state: Buffer.alloc(16), receipt: Buffer.alloc(16), donor: 'procA',
            [field]: Buffer.alloc(length),
          } })
          return true
        },
      })
      try {
        await assert.rejects(h.gate.open(), { code: Err.RPC_ILLEGAL })
        t.notOk(h.requests.some(({ message }) => message.type === 'adopt'),
          `${field} length ${length}: invalid hashes cannot select durable state`)
        t.equal(h.errors.length, 1)
      } finally { await h.gate.close() }
    }
  }
  t.end()
})

test('Gate validates complete event status before changing public properties', async (t) => {
  const malformed = Object.keys(status()).map((field) => {
    const value = status()
    delete value[field]
    return value
  })
  malformed.push(status({ state: 'unknown' }), status({ term: 1 }),
    status({ followers: [3] }), status({ seq: -2n }), status({ leader: 3 }))
  for (const value of malformed) {
    const h = harness({ outputs: (input) => input.type === 'append' ? [wire.encodePayload({
      to: 'gate', message: { type: 'change', status: value },
    }), client(input)] : [] })
    let changes = 0
    h.gate.on('change', () => { changes++ })
    try {
      await h.gate.open()
      await assert.rejects(h.gate.append(Buffer.from('work')), { code: Err.RPC_ILLEGAL })
      t.equal(changes, 0, 'invalid status is not emitted')
      t.equal(h.gate.state, 'follower', 'invalid status cannot partially update public state')
      t.equal(h.gate.term, 0n)
      t.equal(h.gate.seq, -1n)
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate validates event-specific fields before delivery', async (t) => {
  const messages = [
    { type: 'commit' }, { type: 'commit', seq: -1n },
    { type: 'cmd', cmdSeq: 1n }, { type: 'cmd', cmdSeq: 1n, cmdCount: 0 },
    { type: 'sync', cmdSeq: 1n, syncSeq: 2n, quorum: 'yes', agree: ['a', 'b'], disagree: [] },
    { type: 'sync', cmdSeq: 1n, syncSeq: 2n, quorum: true, agree: [4], disagree: [] },
  ]
  for (const message of messages) {
    const h = harness({ outputs: (input) => input.type === 'append' ? [wire.encodePayload({
      to: 'gate', message: { ...message, status: status() },
    }), client(input)] : [] })
    let emitted = 0
    h.gate.on(message.type, () => { emitted++ })
    try {
      await h.gate.open()
      await assert.rejects(h.gate.append(Buffer.from('work')), { code: Err.RPC_ILLEGAL })
      t.equal(emitted, 0, `${message.type}: invalid event is not published`)
      t.equal(h.gate.term, 0n, 'status is not updated before the whole event validates')
    } finally { await h.gate.close() }
  }
  t.end()
})

test('Gate invalid command results stop before releasing further outputs', async (t) => {
  const invalid = [
    { clientId: undefined, cid: 'not-a-client-result' }, { clientId: '' },
    { clientId: null }, { clientId: 1 },
    { cmdSeq: undefined }, { cmdSeq: -1n }, { cmdSeq: 3 },
    { results: undefined }, { results: [[0]] },
    { results: [[0, 'ok', 'extra']] }, { results: [[1, 'failed', null, null, 3]] },
    { results: [[1, 'failed', 'code', null]] }, { results: [[2, 'unknown']] },
  ]
  for (const batch of [false, true]) {
    const mismatched = [
      { results: [] }, { results: [[0, 'one'], [0, 'two'], [0, 'three']] },
      ...(batch ? [{ results: [[0, 'missing second result']] }] : []),
    ]
    const cases = [
      ...invalid.map((fields) => [fields, Err.RPC_ILLEGAL]),
      ...mismatched.map((fields) => [fields, Err.MONSTER_ILLEGAL]),
    ]
    for (const [fields, code] of cases) {
      const h = harness({ outputs(input) {
        if (input.type !== 'append') { return [] }
        return [wire.encodePayload({ to: 'gate', message: {
          type: 'ack', clientId: input.clientId, cmdSeq: 3n,
          results: input.items.map(() => [0, 'ok']), ...fields,
        } }), wire.encodePayload({ to: 'b', message: { type: 'append' } }), client(input)]
      } })
      let closed = false
      h.gate.on('close', () => { closed = true })
      h.gate.on('error', () => t.notOk(h.gate.isOpen, 'Gate stops before reporting the error'))
      try {
        await h.gate.open()
        const items = [Buffer.from('one'), Buffer.from('two')]
        const work = (batch ? h.gate.appendBatch(items) : h.gate.append(items[0])).catch((err) => err)
        const queued = h.gate.append(Buffer.from('queued')).catch((err) => err)
        const failure = await work
        t.equal(failure.code, code, `${batch ? 'batch' : 'single'}: invalid result rejects`)
        t.equal(await queued, failure, 'other pending callers receive the terminal error')
        t.deepEqual(h.errors, [failure], 'invalid local result is reported once')
        t.equal(h.sent.length, 0, 'outputs following the invalid result are not released')
        t.equal(h.requests.filter(({ to, message }) => to === 'procA' && message.type === 'append').length, 1,
          'queued command never reaches Procs')
        t.notOk(closed, 'fatal stop does not emit close')
        t.notOk(h.requests.some(({ message }) => message.type === 'close'), 'fatal stop sends no close requests')
        await h.gate.close()
        t.ok(closed, 'explicit cleanup still emits close')
      } finally { await h.gate.close() }
    }
  }
  t.end()
})

test('Gate valid command errors and rejected outcomes remain nonfatal', async (t) => {
  const reason = new Err.ErrorWithCode('command failed', Err.APPLY_ERROR)
  let mode = 'err'
  const h = harness({ outputs(input) {
    if (input.type !== 'append') { return [] }
    if (mode === 'err') {
      return [wire.encodePayload({ to: 'gate', message: {
        type: 'err', clientId: input.clientId, error: wire.errorToWire(reason),
      } })]
    }
    return [client(input, input.items.map((item) => item.toString() === 'reject'
      ? [1, reason.message, reason.code, null, reason.stack] : [0, item.toString()]))]
  } })
  try {
    await h.gate.open()
    for (const batch of [false, true]) {
      const work = batch ? h.gate.appendBatch([Buffer.from('reject')]) : h.gate.append(Buffer.from('reject'))
      await assert.rejects(work, { message: reason.message, code: reason.code, stack: reason.stack })
      t.ok(h.gate.isOpen, 'protocol ERR leaves Gate running')
      t.equal(h.gate._clients.size, 0, 'protocol ERR removes its pending client')
    }
    mode = 'outcomes'
    await assert.rejects(h.gate.append(Buffer.from('reject')), {
      message: reason.message, code: reason.code, stack: reason.stack,
    })
    const [seq, outcomes] = await h.gate.appendBatch([Buffer.from('reject'), Buffer.from('keep')])
    t.equal(seq, 3n)
    t.deepEqual(outcomes.map((outcome) => outcome.status), ['rejected', 'fulfilled'])
    t.equal(outcomes[0].reason.stack, reason.stack)
    t.equal(outcomes[1].value, 'keep')
    t.ok(h.gate.isOpen, 'valid rejected outcomes leave Gate running')
    t.deepEqual(h.errors, [], 'command failures do not emit Gate errors')
    t.deepEqual(await h.gate.append(Buffer.from('next')), [3n, 'next'])
  } finally { await h.gate.close() }
  t.end()
})
