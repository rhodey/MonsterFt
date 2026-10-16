import test from 'tape'
import crypto from 'node:crypto'
import { Executor } from '../src/executor.v2.js'
import { Random } from '../src/random.v2.js'
import {
  encodeFrame, decodeFrame, encodePayload, decodePayload,
  checksum, framePayload, errorToWire, errorFromWire,
} from '../src/wire.v2.js'
import * as Err from '../src/error.v2.js'

test('v2 executor drains input continuations before due timers', (t) => {
  const executor = new Executor()
  const order = []
  const timeout = executor.after(10, () => order.push('timeout'))
  executor.after(10, () => {
    order.push('timer')
    executor.enqueue(() => order.push('timer continuation'))
  })
  executor.run(10, () => {
    order.push('input')
    executor.enqueue(() => {
      executor.cancel(timeout)
      order.push('reply continuation')
    })
  })
  t.deepEqual(order, ['input', 'reply continuation', 'timer', 'timer continuation'],
    'reply processing cancels a deadline reached by the same input')
  t.equal(executor.queue.length, 0)
  t.equal(executor.running, false)
  t.end()
})

test('v2 executor orders deadlines and coalesces missed repeating timer firings', (t) => {
  const executor = new Executor()
  const order = []
  const repeat = executor.after(10, () => order.push(`repeat-${executor.timems}`), 10)
  executor.after(20, () => order.push('twenty-first'))
  executor.after(15, () => order.push('fifteen'))
  executor.after(20, () => order.push('twenty-second'))
  executor.run(100, () => order.push('input'))
  t.deepEqual(order, ['input', 'repeat-100', 'fifteen', 'twenty-first', 'twenty-second'])
  t.equal(executor.timers.get(repeat).due, 110, 'repeating timer resumes from supplied current time')
  executor.run(100, () => order.push('equal time input'))
  executor.run(109, () => {})
  t.equal(order.filter((item) => item.startsWith('repeat')).length, 1,
    'equal timestamps and intervening inputs do not repeat missed periods')
  executor.run(110, () => {})
  t.equal(order.at(-1), 'repeat-110')
  executor.cancel(repeat)
  executor.run(1000, () => {})
  t.equal(order.filter((item) => item.startsWith('repeat')).length, 2)
  t.end()
})

test('v2 executor leaves external waits pending and rejects reentry or backwards time', (t) => {
  const executor = new Executor()
  let resume
  let completed = false
  executor.run(1, () => { resume = () => { completed = true } })
  t.equal(completed, false, 'unfinished external operation does not hold the input open')
  executor.run(1, () => executor.enqueue(resume))
  t.equal(completed, true)
  t.throws(() => executor.run(0, () => {}), (err) => err.code === Err.RAFT_ILLEGAL)
  t.throws(() => executor.run(2, () => executor.run(2, () => {})),
    (err) => err.code === Err.RAFT_ILLEGAL)
  t.equal(executor.running, false)
  t.throws(() => executor.run(3, () => {
    executor.after(0, () => { throw new Error('timer failed') })
    executor.enqueue(() => {})
  }), /timer failed/)
  t.equal(executor.queue.length, 0, 'failed input discards unprocessed runnable work')
  t.end()
})

test('v2 random consumes the same full seed for identical jitter and CIDs', (t) => {
  const seed = Buffer.from('00112233445566778899aabbccddeeff', 'hex')
  const randoms = [new Random(seed), new Random(seed), new Random(seed)]
  const sequences = randoms.map((random) => Array.from({ length: 100 }, () =>
    [random.int(0, 49), random.cid(), random.int(-9, 9)]))
  t.deepEqual(sequences[0], sequences[1])
  t.deepEqual(sequences[0], sequences[2])
  t.ok(sequences[0].every(([jitter, cid, signed]) =>
    jitter >= 0 && jitter <= 49 && /^[a-f0-9]{32}$/.test(cid) && signed >= -9 && signed <= 9))
  t.equal(new Set(sequences[0].map((item) => item[1])).size, 100)
  for (const offset of [0, 4, 8, 12]) {
    const changed = Buffer.from(seed)
    changed[offset] ^= 1
    t.notEqual(new Random(changed).cid(), new Random(seed).cid(),
      `the seed word at byte ${offset} contributes to the generator state`)
  }
  t.throws(() => new Random(Buffer.alloc(16)), (err) => err.code === Err.RPC_ILLEGAL)
  t.throws(() => new Random(Buffer.alloc(15, 1)), (err) => err.code === Err.RPC_ILLEGAL)
  t.end()
})

test('v2 frames decode independently and preserve nested records and shared errors', (t) => {
  const messages = [
    { type: 'hello', cid: 'first', seed: crypto.randomBytes(16) },
    { type: 'append', term: 9_223_372_036_854_775_807n, seqP: -1n, data: [
      { term: 0n, entry: Buffer.alloc(0) },
      { term: 1n, entry: Buffer.from([0, 255, 17]) },
    ] },
    { type: 'err', cid: 'last', code: Err.REPL_BACKTRACK, logSeq: -1n },
  ]
  const frames = messages.map(encodeFrame)
  t.deepEqual(decodeFrame(frames[2]), messages[2], 'later frame decodes without earlier messages')
  t.deepEqual(decodeFrame(frames[1]), messages[1])
  t.deepEqual(decodeFrame(frames[0]), messages[0])
  for (const [index, frame] of frames.entries()) {
    t.deepEqual(frame.subarray(0, 32), crypto.createHash('sha256').update(frame.subarray(32)).digest())
    t.deepEqual(decodePayload(encodePayload(messages[index])), messages[index])
    t.deepEqual(encodeFrame(messages[index]), frame, 'encoding does not depend on prior frames')
  }
  const err = new Err.ErrorWithCode('apply failed', Err.APPLY_ERROR, 19)
  err.stack = 'Error: apply failed\n    at originalApply (/user/app.js:3:1)'
  const restored = errorFromWire(decodeFrame(encodeFrame(errorToWire(err))))
  t.equal(restored.message, err.message)
  t.equal(restored.code, err.code)
  t.equal(restored.sqlCode, err.sqlCode)
  t.equal(restored.stack, err.stack)
  t.notOk(Object.hasOwn(errorToWire(err, false), 'stack'), 'independent Proc stacks can be omitted')
  t.equal(errorFromWire(errorToWire(err, false)).code, Err.APPLY_ERROR)
  t.end()
})

test('v2 frames reject corrupt bytes and malformed payloads', (t) => {
  const frame = encodeFrame({ type: 'tick', tickCount: 1n })
  for (const offset of [0, 31, 32, frame.length - 1]) {
    const corrupt = Buffer.from(frame)
    corrupt[offset] ^= 1
    t.throws(() => decodeFrame(corrupt), (err) => err.code === Err.FRAME_CORRUPT)
  }
  for (const incomplete of [null, 'frame', Buffer.alloc(0), Buffer.alloc(32)]) {
    t.throws(() => decodeFrame(incomplete), (err) => err.code === Err.FRAME_CORRUPT)
  }
  const malformed = Buffer.from([0xdb, 0, 0, 0, 10, 65])
  t.throws(() => decodeFrame(framePayload(malformed, checksum(malformed))),
    (err) => err.code === Err.FRAME_CORRUPT, 'a valid checksum does not authorize malformed MessagePack')
  t.end()
})
