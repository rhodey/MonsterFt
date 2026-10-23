import test from 'tape'
import { pack } from 'msgpackr'
import * as utilm from '../src/utilm.js'
import * as wire from '../src/wire.js'
import * as Err from '../src/error.js'

const nodes = ['a', 'b', 'c']
const isError = (code) => (err) => err instanceof Err.ErrorWithCode && err.code === code
const input = (kind, fields = {}) => ({
  type: 'input', cid: 'ordered', kind, tickCount: 1n, timems: 0, ...fields,
})
const execute = (fields = {}) => ({
  type: 'execute', cid: 'execution', tickCount: 2n, term: 1n, seq: 1n,
  entryHash: Buffer.alloc(16), items: [Buffer.from('item')], matchIndex: [-1n, 0n, 0n],
  ...fields,
})

test('local message validators preserve originals and allow extra fields', (t) => {
  const extra = { value: Buffer.from('extra') }
  const request = { type: 'hello', cid: 'hello', seed: Buffer.alloc(16, 1), extra }
  t.equal(utilm.parseProcRequest(request, 'a', nodes).message, request)
  t.equal(utilm.validateApplyRequest(request), request)
  const message = input('append', { requestId: 'public', items: [Buffer.from('item')], extra })
  const bytes = wire.encodePayload(message)
  const parsed = utilm.parseProcRequest(message, 'a', nodes)
  t.equal(parsed.message, message)
  t.equal(parsed.message.items, message.items)
  t.equal(parsed.message.items[0], message.items[0], 'validation does not copy payloads')
  t.equal(parsed.message.extra, extra, 'extension fields remain available')
  t.deepEqual(wire.encodePayload(message), bytes, 'validation leaves encoded bytes unchanged')
  for (const bad of [
    null, { cid: 'no-type' }, { type: 1, cid: 'number-type' },
    { type: 'hello', cid: '', seed: request.seed },
    input('tick', { cid: undefined }), input('tick', { cid: '' }),
    input('other'), input({ toString: null }),
    input('append', { requestId: 'public', items: [] }),
    input('append', { requestId: '', items: [Buffer.alloc(0)] }),
    input('append', { requestId: 'public', items: [new Uint8Array(1)] }),
    input('drain'), input('tick', { timems: NaN }),
  ]) {
    t.throws(() => utilm.parseProcRequest(bad, 'a', nodes), isError(Err.RPC_ILLEGAL))
  }
  t.end()
})

test('ordered input parsing distinguishes remote decode failures from local corruption', (t) => {
  const envelope = { to: 'a', message: { from: 'b', type: 'vote', term: 2n, voteGranted: true } }
  const parsed = utilm.parseProcRequest(input('rpc', {
    from: 'b', frame: wire.encodeFrame(envelope),
  }), 'a', nodes)
  t.deepEqual(parsed.clusterMessage, envelope.message)
  t.equal(parsed.warning, null)
  const badPayload = wire.framePayload(Buffer.from([0xdb, 0, 0, 0, 10, 65]))
  const ignored = utilm.parseProcRequest(input('rpc', { from: 'b', frame: badPayload }), 'a', nodes)
  t.equal(ignored.clusterMessage, null)
  t.equal(ignored.warning.code, Err.FRAME_CORRUPT)
  t.match(ignored.warning.message, /^cluster frame from b:/)
  const corrupt = Buffer.from(badPayload)
  corrupt[0] ^= 1
  t.throws(() => utilm.parseProcRequest(input('rpc', { from: 'b', frame: corrupt }), 'a', nodes),
    isError(Err.FRAME_CORRUPT), 'nested checksum failures are never downgraded')
  for (const changed of [
    { ...envelope, to: 'c' },
    { ...envelope, message: { ...envelope.message, from: 'c' } },
    { ...envelope, message: { from: 'b' } },
  ]) {
    t.throws(() => utilm.parseProcRequest(input('rpc', {
      from: 'b', frame: wire.encodeFrame(changed),
    }), 'a', nodes), isError(Err.RPC_ILLEGAL), 'decoded envelope failures remain fatal')
  }
  t.end()
})

test('Apply validation retains setup, command, and byte-view contracts', (t) => {
  const setup = execute({ seq: 0n, items: null, matchIndex: null })
  t.equal(utilm.validateApplyRequest(setup), setup)
  const message = execute({ extra: true, entryHash: new Uint8Array(16) })
  t.equal(utilm.validateApplyRequest(message), message, 'receipt hashes retain Uint8Array support')
  t.equal(message.entryHash.constructor, Uint8Array, 'hash is not converted during validation')
  for (const bad of [
    execute({ cid: undefined }), execute({ cid: '' }), execute({ type: 1 }),
    execute({ seq: 0n }), execute({ items: null, matchIndex: null }),
    execute({ items: [] }), execute({ items: [new Uint8Array(1)] }),
    execute({ matchIndex: [-2n, 0n, 0n] }), execute({ matchIndex: [0n, 1n, 0n] }),
  ]) {
    t.throws(() => utilm.validateApplyRequest(bad), isError(Err.RPC_ILLEGAL))
  }
  const ack = { type: 'ack', cid: message.cid, tickCount: message.tickCount }
  t.equal(utilm.validateLocalReply(ack, message, 'apply'), ack)
  t.throws(() => utilm.validateLocalReply({ ...ack, tickCount: 3n }, message, 'apply'),
    isError(Err.RPC_ILLEGAL))
  const error = { type: 'err', cid: message.cid, error: { message: 7, code: 'native' } }
  t.equal(utilm.validateLocalReply(error, message, 'apply'), error,
    'operation errors preserve normalization instead of requiring success fields')
  t.end()
})

test('shared command-result validation keeps successful values opaque', (t) => {
  const opaque = { nested: [Buffer.from('binary'), new Date(0), { any: true }] }
  const results = [[0, opaque], [0, undefined], [1, 'rejected', null, null],
    [1, 'coded', 123, 456, 'original stack']]
  const reply = { cmdSeq: 9n, results, extra: Buffer.from('extension') }
  const before = wire.encodePayload(reply)
  t.equal(utilm.validateCommandResult(reply, results.length), reply)
  t.equal(reply.results, results)
  t.equal(reply.results[0][1], opaque)
  t.ok(utilm.isOutcomes(results, results.length))
  t.ok(utilm.isFwdCmdAck(reply, results.length))
  t.deepEqual(wire.encodePayload(reply), before, 'tuple validation does not alter wire bytes')
  const publicResults = utilm.outcomesPublic(results)
  t.equal(publicResults[0].value, opaque)
  t.equal(publicResults[3].reason.stack, 'original stack')
  for (const tuple of [[0], [0, 1, 2], [1, 'failed', null], [1, 3, null, null],
    [1, 'failed', NaN, null], [1, 'failed', null, null, 0]]) {
    t.notOk(utilm.isOutcomeTuple(tuple), 'malformed tuple rejected')
    t.throws(() => utilm.validateCommandResult({ cmdSeq: 1n, results: [tuple] }, 1),
      isError(Err.RPC_ILLEGAL))
  }
  t.throws(() => utilm.validateCommandResult(reply, 1), isError(Err.RPC_ILLEGAL),
    'result count remains tied to the request')
  t.end()
})

test('Monster message and entry validation preserves contextual checks', (t) => {
  const node = { nodes, quorum: 2 }
  const cmd = { type: 'cmd', items: [new Uint8Array([1, 2])], matchIndex: [-1n, 0n, 1n], extra: true }
  t.doesNotThrow(() => utilm.validateCmdEntry(node, cmd, 2n))
  t.deepEqual(utilm.decodeEntry(pack(cmd)), { ...cmd, items: cmd.items.map((bytes) => Buffer.from(bytes)) },
    'decoding retains extension fields and payload bytes')
  t.ok(utilm.isFwdCmdMsg({ type: 'fwd_cmd', cid: 'forward', term: 0n, items: cmd.items }))
  t.notOk(utilm.isFwdCmdMsg({ cid: 'forward', term: 0n, items: [] }))
  t.doesNotThrow(() => utilm.validateOutcomeMsg({ cmdSeq: 1n, digest: new Uint8Array(16) }))
  t.throws(() => utilm.validateOutcomeMsg({ cmdSeq: 1n, digest: Buffer.alloc(15) }), isError(Err.RPC_ILLEGAL))
  t.throws(() => utilm.validateOutcomeRequestMsg({ cmdSeq: -1n }), isError(Err.RPC_ILLEGAL))
  t.throws(() => utilm.validateCmdEntry(node, { ...cmd, matchIndex: [-1n] }, 2n),
    isError(Err.MONSTER_CORRUPT))
  t.throws(() => utilm.validateCmdEntry(node, { ...cmd, matchIndex: [-1n, 0n, 2n] }, 2n),
    isError(Err.MONSTER_CORRUPT))
  const sync = { type: 'sync', cmdSeq: 2n, quorum: true,
    agree: ['a', 'b'], disagree: ['c'], digest: new Uint8Array(16), extra: true }
  t.deepEqual(utilm.validateSyncEntry(node, sync), { agree: ['a', 'b'], disagree: ['c'] })
  for (const changed of [
    { agree: ['b', 'a'] }, { agree: ['a', 'a'] }, { agree: ['a', 'z'] },
    { agree: ['a'] }, { digest: Buffer.alloc(17) }, { quorum: false },
  ]) {
    t.throws(() => utilm.validateSyncEntry(node, { ...sync, ...changed }), isError(Err.MONSTER_CORRUPT))
  }
  t.doesNotThrow(() => utilm.validateSyncEntry(node, {
    type: 'sync', cmdSeq: 2n, quorum: false, agree: [], disagree: nodes,
  }), 'quorum-impossible SYNC does not require an agreed digest')
  t.doesNotThrow(() => utilm.validateSyncEntry(node, { ...sync, disagree: ['a', 'c'] }),
    'existing SYNC overlap semantics remain unchanged')
  t.throws(() => utilm.decodeEntry(Buffer.from([0xdb, 0, 0, 0, 10, 65])), isError(Err.MONSTER_CORRUPT))
  t.end()
})
