import test from 'tape'
import { z } from 'zod'
import { schemas, raftSchemas, validate } from '../src/util.js'
import * as Err from '../src/error.js'

const max = 9_223_372_036_854_775_807n
const append = (fields = {}) => ({
  type: 'append', cid: 'page', term: 1n, termP: -1n, seqP: -1n,
  commitSeq: -1n, ...fields,
})

test('protocol schema validation preserves original objects and byte views', (t) => {
  const payload = Buffer.from([0, 255, 7])
  const extra = { nested: Buffer.from('extension') }
  const msg = append({ extra, data: [{ term: 0n, entry: payload, extra }] })
  const before = Reflect.ownKeys(msg)
  t.equal(validate(raftSchemas.append, msg, 'append'), msg)
  t.equal(msg.data[0].entry, payload, 'payload identity is preserved')
  t.equal(msg.extra, extra, 'additional fields remain intact')
  t.equal(msg.data[0].extra, extra, 'nested additional fields remain intact')
  t.deepEqual(Reflect.ownKeys(msg), before, 'no fields are inserted or removed')
  t.ok(raftSchemas.append.validate(append()), 'heartbeat needs no data field')
  t.ok(raftSchemas.append.validate(append({ data: undefined })), 'undefined remains a heartbeat')
  t.notOk(raftSchemas.append.validate(append({ data: [] })), 'empty records are not a heartbeat')
  t.ok(schemas.bytes.validate(new Uint8Array([1])))
  t.notOk(schemas.buffer.validate(new Uint8Array([1])), 'Buffer-only fields are not broadened')
  t.end()
})

test('protocol schemas retain bigint and hash boundaries without coercion', (t) => {
  for (const [name, good, bad] of [
    ['term', [0n, max], [-1n, max + 1n, 1, '1']],
    ['seq', [-1n, 0n, max], [-2n, max + 1n, 1, '1']],
    ['nonnegativeSeq', [0n, max], [-1n, max + 1n, 1]],
    ['tickCount', [0n, max + 1n], [-1n, 1, '1']],
    ['timems', [0, Number.MAX_SAFE_INTEGER], [-1, 1.5, Number.MAX_SAFE_INTEGER + 1, 1n]],
  ]) {
    for (const value of good) t.ok(schemas[name].validate(value), `${name} accepts ${value}`)
    for (const value of bad) t.notOk(schemas[name].validate(value), `${name} rejects ${value}`)
  }
  for (const name of ['hash', 'digest']) {
    t.ok(schemas[name].validate(Buffer.alloc(16)))
    for (const length of [0, 15, 17, 32]) t.notOk(schemas[name].validate(Buffer.alloc(length)))
  }
  t.ok(schemas.digest.validate(new Uint8Array(16)), 'SQLite byte views remain valid digests')
  t.notOk(schemas.hash.validate(new Uint8Array(16)), 'wire hashes remain Buffers')
  t.ok(schemas.seed.validate(Buffer.alloc(16, 1)))
  t.notOk(schemas.seed.validate(Buffer.alloc(16)), 'all-zero seed is rejected')
  t.end()
})

test('protocol validation reports field paths without exposing Zod errors', (t) => {
  t.throws(() => validate(raftSchemas.append,
    append({ data: [{ term: 0n, entry: Buffer.alloc(0) }, { term: -1n, entry: Buffer.alloc(0) }] }),
    'append'), (err) => err instanceof Err.ErrorWithCode &&
      err.code === Err.RPC_ILLEGAL && err.message === 'append data[1].term is illegal')
  const result = z.looseObject({ results: z.array(z.union([
    z.tuple([z.literal(0), z.unknown()]), z.tuple([z.literal(1), z.string()]),
  ])) })
  t.throws(() => validate(result, { results: [[1, 4]] }, 'result', Err.MONSTER_CORRUPT),
    (err) => err instanceof Err.ErrorWithCode && err.code === Err.MONSTER_CORRUPT &&
      err.message === 'result results[0][1] is corrupt', 'nested union keeps its full field path')
  t.end()
})

test('Raft reply headers leave success fields and error normalization to their owners', (t) => {
  const error = { type: 'err', cid: 'request', term: 9n }
  t.ok(raftSchemas.reply.validate(error), 'error body fields may be absent')
  t.ok(raftSchemas.reply.validate({ ...error, code: 'native', message: 17, logSeq: 'bad' }),
    'normalization and optional hint fallback remain owner decisions')
  t.ok(raftSchemas.reply.validate({ type: 'ack', cid: 'request', term: 9n }),
    'ACK body requirements depend on the outstanding request')
  t.notOk(raftSchemas.reply.validate({ ...error, term: max + 1n }))
  t.notOk(raftSchemas.reply.validate({ ...error, cid: '' }))
  t.notOk(raftSchemas.reply.validate({ ...error, type: 'vote' }))
  t.end()
})
