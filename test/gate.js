import test from 'tape'
import assert from 'node:assert/strict'
import { Gate } from '../src/gate.js'
import * as Err from '../src/error.js'
import * as wire from '../src/wire.js'

const procs = ['procA', 'procB', 'procC']
const immediate = () => new Promise((resolve) => setImmediate(resolve))
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function harness(options = {}) {
  const requests = []
  const sent = []
  const errors = []
  const warnings = []
  let gate
  const reply = (from, message) => gate.onReceive(from, wire.encodeFrame(message))
  const complete = (from, input, outputs = []) => reply(from, {
    type: 'ack', cid: input.cid,
    outputs: from === 'procA' ? outputs : outputs.map(wire.checksum),
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
    } else if (message.type === 'input') {
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
    to: 'client', message: { type: 'ack', cid: input.requestId, cmdSeq: 3n, results: values },
  })

test('Gate defaults Apply timeout to five seconds and permits an override', (t) => {
  const make = (opts) => new Gate('a', ['a', 'b', 'c'], () => {}, () => {}, opts)
  t.equal(make().applyTimeout, 5_000)
  t.equal(make({ applyTimeout: 12_000 }).applyTimeout, 12_000)
  t.end()
})

test('Gate hello retries use fresh CIDs and one seed before phased startup', async (t) => {
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
    ['hello', 'open', 'capture', 'adopt', 'reconcile', 'input'], 'startup phases stay ordered')
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

test('Gate repeated open waits for the same startup result', async (t) => {
  for (const finish of ['complete', 'timeout', 'close']) {
    let releaseC
    const h = harness({
      local({ to, message, complete }) {
        if (to !== 'procC' || message.kind !== 'start') { return false }
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

test('Gate releases agreed outputs but waits for all three before next input', async (t) => {
  let releaseC
  const h = harness({
    outputs: (input) => input.kind === 'append' ? [client(input)] : [],
    local({ to, message, complete }) {
      if (to !== 'procC' || message.kind !== 'append') { return false }
      releaseC = () => complete(to, message, [client(message)])
      return true
    },
  })
  await h.gate.open()
  const value = await h.gate.append(Buffer.from('one'))
  t.deepEqual(value, [3n, 'one'], 'A plus B release caller result before C completes')
  h.gate.onReceive('b', wire.encodeFrame({ to: 'a', message: { type: 'append' } }))
  await immediate()
  t.equal(h.requests.filter((r) => r.message.kind === 'rpc').length, 0,
    'next input remains queued')
  releaseC()
  await immediate()
  t.equal(h.requests.filter((r) => r.message.kind === 'rpc').length, 3,
    'next input fans out only after third completion')
  t.equal(h.errors.length, 0)
  await h.gate.close()
  t.end()
})

test('Gate shares one stable input frame across synchronous and delayed fanout', async (t) => {
  for (const delayed of [false, true]) {
    const saved = []
    const h = harness({
      local({ to, message, frame, complete }) {
        if (message.type !== 'input') { return false }
        saved.push({ frame, bytes: Buffer.from(frame), message })
        const respond = () => complete(to, message,
          message.kind === 'append' ? [client(message)] : [])
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
        request.to === 'procA' && request.message.type === 'input')
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

test('Gate detects checksum-only disagreement without waiting for ProcA', async (t) => {
  const h = harness({
    local({ to, message, reply }) {
      if (message.kind !== 'append') { return false }
      if (to !== 'procA') {
        reply(to, { type: 'ack', cid: message.cid, outputs: [Buffer.alloc(16, to === 'procB' ? 1 : 2)] })
      }
      return true
    },
  })
  await h.gate.open()
  await assert.rejects(h.gate.append(Buffer.from('bad')), { code: Err.LOCAL_DIVERGENCE })
  t.equal(h.errors[0].code, Err.LOCAL_DIVERGENCE)
  t.notOk(h.gate.isOpen)
  await h.gate.close()
  t.end()
})

test('Gate rejects different output counts and preserves received error stacks', async (t) => {
  const h = harness({
    local({ to, message, complete }) {
      if (message.kind !== 'append') { return false }
      complete(to, message, to === 'procA' ? [client(message)] : [])
      return true
    },
  })
  await h.gate.open()
  await assert.rejects(h.gate.append(Buffer.alloc(0)), { code: Err.LOCAL_DIVERGENCE })
  await h.gate.close()
  const second = harness({ outputs: (input) => input.kind === 'append'
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
  const h = harness({ outputs: (input) => input.kind === 'append' ? [client(input)] : [] })
  try {
    Date.now = () => 2_000
    await h.gate.open()
    Date.now = () => 1_000
    await h.gate.append(Buffer.from('second'))
    Date.now = () => 3_000
    await h.gate.appendBatch([Buffer.from('third'), Buffer.from('fourth')])
    const a = h.requests.filter((r) => r.to === 'procA' && r.message.type === 'input')
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
    outputs: (input) => input.kind === 'append' ? [client(input)] : [],
    local({ to, message, complete }) {
      if (to !== 'procC' || message.kind !== 'append') { return false }
      releaseC = () => complete(to, message, [client(message)])
      return true
    },
  })
  await h.gate.open()
  await h.gate.append(Buffer.from('hold'))
  await delay(15)
  t.equal(h.gate._queue.filter((work) => work.input.kind === 'tick').length, 1,
    'only one clock input waits')
  const sentBefore = Date.now()
  releaseC()
  await immediate()
  const tick = h.requests.find((r) => r.to === 'procA' && r.message.kind === 'tick')
  t.ok(tick.message.timems >= sentBefore, 'tick gets current time at dispatch')
  await h.gate.close()
  t.end()
})

test('Gate Apply watchdog expires while waiting for a third Proc', async (t) => {
  const execute = wire.encodePayload({ to: 'apply', message: {
    type: 'execute', cid: 'apply-timeout', term: 1n, seq: 1n, tickCount: 2n,
    entryHash: Buffer.alloc(16), items: [Buffer.from('item')], matchIndex: [-1n, -1n, -1n],
  } })
  const h = harness({
    opts: { applyTimeout: 15 },
    outputs: (input) => input.kind === 'append' ? [execute] : [],
    local({ to, message }) {
      return message.type === 'execute' || (to === 'procC' && message.kind === 'append')
    },
  })
  await h.gate.open()
  await assert.rejects(h.gate.append(Buffer.from('item')), { code: Err.APPLY_TIMEOUT })
  t.equal(h.errors[0].code, Err.APPLY_TIMEOUT, 'native watchdog runs independently of barrier')
  const request = h.requests.find((r) => r.message.type === 'execute')
  t.ok(request.frame.subarray(16).equals(execute), 'Apply receives unchanged agreed bytes')
  await h.gate.close()
  t.end()
})

test('Gate cancels Apply timeout when reply arrives before queued delivery', async (t) => {
  let requestId
  let releaseC
  const execute = wire.encodePayload({ to: 'apply', message: { type: 'execute', cid: 'apply-fast' } })
  const h = harness({
    opts: { applyTimeout: 10 },
    outputs(input) {
      if (input.kind === 'append') { requestId = input.requestId; return [execute] }
      if (input.kind === 'apply') {
        return [client({ requestId, items: [Buffer.from('done')] })]
      }
      return []
    },
    local({ to, message, reply, complete }) {
      if (message.type === 'execute') {
        reply(to, { type: 'ack', cid: message.cid, warnings: [{
          message: 'item rejected', code: Err.APPLY_ERROR, sqlCode: null, stack: 'user trace',
        }] })
        return true
      }
      if (to === 'procC' && message.kind === 'append') {
        releaseC = () => complete(to, message, [execute])
        return true
      }
      return false
    },
  })
  await h.gate.open()
  const pending = h.gate.append(Buffer.from('work'))
  await delay(30)
  t.equal(h.errors.length, 0, 'reply arrival cancels timeout despite delayed delivery')
  t.equal(h.warnings.length, 0, 'Gate does not emit diagnostics bundled in an Apply reply')
  t.equal(h.requests.filter((r) => r.message.kind === 'apply').length, 0)
  releaseC()
  t.deepEqual(await pending, [3n, 'done'])
  await h.gate.close()
  t.end()
})

test('Gate stop cancels local work without coordinating owner shutdown', async (t) => {
  const h = harness({
    local({ message }) { return message.kind === 'append' },
  })
  try {
    await h.gate.open()
    const command = h.gate.append(Buffer.from('pending')).catch((err) => err)
    const changed = h.gate.awaitEvent('change').catch((err) => err)
    let closed = false
    const closing = h.gate.awaitEvent('close').then(() => { closed = true })
    await immediate()
    h.gate.stop()
    t.notOk(h.gate.isOpen, 'stopping is immediate')
    t.equal((await command).code, Err.NODE_NOT_OPEN)
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
  h.gate.onReceive('b', Buffer.alloc(17))
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

test('Gate rejects corrupt frames and closes pending public work', async (t) => {
  const h = harness()
  let closed = false
  let fatals = 0
  h.gate.on('close', () => { closed = true })
  h.gate.on('fatal', () => { fatals++ })
  await h.gate.open()
  const pending = h.gate.append(Buffer.from('pending')).catch((err) => err)
  const frame = wire.encodeFrame({ to: 'a', message: { type: 'append' } })
  frame[frame.length - 1] ^= 1
  h.gate.onReceive('b', frame)
  t.equal((await pending).code, Err.FRAME_CORRUPT)
  t.equal(h.errors[0].code, Err.FRAME_CORRUPT)
  t.notOk(h.gate.isOpen, 'fatal failure stops Gate')
  t.notOk(closed, 'stopping Gate does not claim component shutdown')
  t.equal(fatals, 0, 'fatal handling is internal')
  t.notOk(h.requests.some((r) => r.message.type === 'close'), 'fatal path sends no close requests')
  await h.gate.close()
  t.ok(closed, 'explicit close waits for owner completion')
  t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4)
  t.end()
})

test('Gate transport failures warn and close cancels awaiting leader', async (t) => {
  const h = harness({
    outputs(input) {
      return input.kind === 'append' ? [wire.encodePayload({ to: 'b', message: { type: 'append' } }), client(input)] : []
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
    release()
    await closing
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
  h.gate.onReceive('b', Buffer.alloc(0))
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
    outputs: (input) => input.kind === 'append' ? [wire.encodePayload({
      to: 'event', message: { type: 'cmd', cmdSeq: 3n, cmdCount: 1, status: { commitSeq: 3n } },
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
    outputs: (input) => input.kind === 'append' ? [wire.encodePayload({
      to: 'event', message: { type: 'cmd', cmdSeq: 3n, cmdCount: 1, status: { commitSeq: 3n } },
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

test('Gate missing completion fatally times out without input retries', async (t) => {
  for (const missing of procs) {
    const h = harness({
      opts: { localTimeout: 15 },
      local({ to, message }) { return to === missing && message.kind === 'append' },
    })
    try {
      await h.gate.open()
      await assert.rejects(h.gate.append(Buffer.from('wait')), { code: Err.LOCAL_TIMEOUT })
      t.equal(h.requests.filter((r) => r.to === missing && r.message.kind === 'append').length, 1)
      t.equal(h.errors[0].code, Err.LOCAL_TIMEOUT, `${missing} retains its own completion timeout`)
      t.equal(h.gate._requests.size, 0, 'failure clears the remaining requests')
    } finally { await h.gate.close() }
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
  const h = harness({
    local({ to, message, complete }) {
      if (message.kind !== 'drain') { return false }
      const outputs = [wire.encodePayload({ to: 'client', message: { type: 'ack', cid: message.requestId } })]
      if (to === 'procB') { release = () => complete(to, message, outputs) }
      else { complete(to, message, outputs) }
      return true
    },
  })
  await h.gate.open()
  const draining = h.gate.drainCmd()
  await assert.rejects(h.gate.append(Buffer.alloc(0)), { code: Err.DRAINING })
  await immediate()
  t.equal(h.requests.filter((r) => r.message.type === 'close').length, 0,
    'drain waits for the final completion before closing owners')
  release()
  await draining
  t.notOk(h.gate.isOpen)
  t.equal(h.requests.filter((r) => r.message.type === 'close').length, 4)
  t.end()
})

test('Gate warning listener may throw or close without dispatching later Apply work', async (t) => {
  const h = harness({
    outputs(input) {
      if (input.kind !== 'append') { return [] }
      return [
        wire.encodePayload({ to: 'b', message: { type: 'append' } }),
        wire.encodePayload({ to: 'apply', message: { type: 'execute', cid: 'warn-close' } }),
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
      outputs: (input) => input.kind === 'append' ? [wire.encodePayload({
        to: type === 'warn' ? 'b' : 'event', message: { type: type === 'warn' ? 'append' : 'change', status: {} },
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
        to === 'procA' && message.kind === 'append').length, 1,
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
      outputs: (input) => input.kind === 'append' ? [wire.encodePayload({
        to: 'event', message: { type, error: { message: 'Proc diagnostic', code: Err.REPAIR_OUTSIDE_AGREEMENT } },
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
    outputs: (input) => input.kind === 'append'
      ? [...messages.map((message) => wire.encodePayload({
        to: 'event', message: { ...message, status },
      })), client(input)] : [],
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
    t.equal(h.errors.length, 0)
  } finally { await h.gate.close() }
  t.end()
})

test('both awaitLeader modes observe leader and commit changes without applied progress', async (t) => {
  let committed = false
  const h = harness({
    outputs: (input) => input.kind === 'append' ? [wire.encodePayload({
      to: 'event',
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

test('Gate malformed completion cannot satisfy an input', async (t) => {
  for (const size of [8, 15, 17, 32]) {
    const h = harness({
      local({ to, message, reply }) {
        if (to !== 'procB' || message.kind !== 'append') { return false }
        reply(to, { type: 'ack', cid: message.cid, outputs: [Buffer.alloc(size)] })
        return true
      },
    })
    await assert.rejects(h.gate.drainCmd(), { code: Err.NODE_NOT_OPEN })
    await h.gate.open()
    await assert.rejects(h.gate.append(Buffer.alloc(0)), { code: Err.RPC_ILLEGAL })
    t.equal(h.errors[0].code, Err.RPC_ILLEGAL, `${size}-byte completion checksum is rejected`)
    await h.gate.close()
  }
  t.end()
})

test('Gate does not release A/C outputs after synchronous A/B disagreement', async (t) => {
  const output = wire.encodePayload({ to: 'b', message: { type: 'append' } })
  const h = harness({
    local({ to, message, complete, reply }) {
      if (message.kind !== 'append') { return false }
      if (to === 'procB') {
        reply(to, { type: 'ack', cid: message.cid, outputs: [Buffer.alloc(16)] })
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
