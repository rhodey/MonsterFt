import test from 'tape'
import { pack } from 'msgpackr'
import * as utilm from '../src/utilm.js'
import * as wire from '../src/wire.js'
import * as Err from '../src/error.js'

const nodes = ['a', 'b', 'c']
const isError = (code) => (err) => err instanceof Err.ErrorWithCode && err.code === code
const input = (type, fields = {}) => ({
  type, cid: 'ordered', tickCount: 1n, timems: 0, ...fields,
})
const execute = (fields = {}) => ({
  type: 'execute', cid: 'execution', tickCount: 2n, term: 1n, seq: 1n,
  entryHash: Buffer.alloc(16), items: [Buffer.from('item')], matchIndex: [-1n, 0n, 0n],
  ...fields,
})

const orderedRequests = () => [
  input('start'), input('tick'),
  input('append', { clientId: 'public', items: [Buffer.from('item')] }),
  input('drain', { clientId: 'draining' }),
  input('cluster_message', { from: 'b', frame: wire.encodeFrame({
    to: 'a', message: { from: 'b', type: 'vote', term: 2n, voteGranted: true },
  }) }),
  input('apply_result', { reply: { type: 'ack', cid: 'executed', tickCount: 0n } }),
]

test('local message validators preserve originals and allow extra fields', (t) => {
  const extra = { value: Buffer.from('extra') }
  const request = { type: 'hello', cid: 'hello', seed: Buffer.alloc(16, 1), extra }
  t.equal(utilm.parseProcRequest(request, 'a', nodes).message, request)
  t.equal(utilm.validateApplyRequest(request), request)
  const message = input('append', { clientId: 'public', items: [Buffer.from('item')], extra })
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
    input({ toString: null }),
    input('append', { clientId: 'public', items: [] }),
    input('append', { clientId: '', items: [Buffer.alloc(0)] }),
    input('append', { clientId: 'public', items: [new Uint8Array(1)] }),
    input('drain'), input('tick', { timems: NaN }),
  ]) {
    t.throws(() => utilm.parseProcRequest(bad, 'a', nodes), isError(Err.RPC_ILLEGAL))
  }
  t.end()
})

test('client results and RPC replies require their own identifiers', (t) => {
  for (const type of ['ack', 'err']) {
    const rpc = { type, cid: 'rpc-call' }
    const client = { type, clientId: 'public-call' }
    t.equal(utilm.validateAckErr(rpc), rpc)
    t.equal(utilm.validateClientAckErr(client), client)
    t.throws(() => utilm.validateClientAckErr(rpc), isError(Err.RPC_ILLEGAL),
      'RPC cid cannot stand in for a client ID')
    t.throws(() => utilm.validateAckErr(client), isError(Err.RPC_ILLEGAL),
      'client ID cannot stand in for an RPC cid')
    for (const clientId of [undefined, null, '', 1, []]) {
      t.throws(() => utilm.validateClientAckErr({ ...rpc, clientId }),
        isError(Err.RPC_ILLEGAL), 'malformed client ID is rejected even with a valid cid')
    }
  }
  t.end()
})

test('local request types are validated independently of lifecycle state', (t) => {
  const hello = { type: 'hello', cid: 'hello', seed: Buffer.alloc(16, 1) }
  for (const request of [hello, ...['open', 'capture', 'adopt', 'reconcile', 'close']
    .map((type) => ({ type, cid: type })), ...orderedRequests()]) {
    t.equal(utilm.parseProcRequest(request, 'a', nodes).message, request,
      `Proc accepts the ${request.type} protocol operation`)
  }
  for (const request of [hello, ...['open', 'ready', 'close']
    .map((type) => ({ type, cid: type })), execute()]) {
    t.equal(utilm.validateApplyRequest(request), request,
      `Apply accepts the ${request.type} protocol operation`)
  }
  for (const type of ['unknown', 'ready', 'execute', 'ack', 'err', 'toString', '__proto__']) {
    t.throws(() => utilm.parseProcRequest({ type, cid: 'request' }, 'a', nodes),
      isError(Err.RPC_ILLEGAL), `Proc rejects unsupported ${type}`)
  }
  for (const type of ['unknown', 'capture', 'adopt', 'reconcile', 'start', 'tick',
    'append', 'drain', 'cluster_message', 'apply_result', 'ack', 'err', 'toString', '__proto__']) {
    t.throws(() => utilm.validateApplyRequest({ type, cid: 'request' }),
      isError(Err.RPC_ILLEGAL), `Apply rejects unsupported ${type}`)
  }
  t.end()
})

test('one type selects ordered input validation and ignores unrelated extension fields', (t) => {
  for (const request of orderedRequests()) {
    t.ok(utilm.isInputType(request.type), `${request.type}: recognized ordered input`)
    const extended = { ...request, kind: 'close' }
    t.equal(utilm.parseProcRequest(extended, 'a', nodes).message, extended,
      `${request.type}: extra kind neither changes routing nor copies the message`)
    for (const changed of [
      { tickCount: undefined }, { tickCount: 1 }, { tickCount: -1n },
      { timems: undefined }, { timems: -1 }, { timems: 0.5 },
    ]) {
      t.throws(() => utilm.parseProcRequest({ ...request, ...changed }, 'a', nodes),
        isError(Err.RPC_ILLEGAL), `${request.type}: ordered clock fields remain required`)
    }
  }
  for (const type of [
    'hello', 'open', 'capture', 'adopt', 'reconcile', 'close', 'execute', 'ack', 'err',
    'input', 'rpc', 'apply', 'toString', 'constructor', '__proto__',
    undefined, null, 0, {}, Symbol('tick'),
  ]) {
    t.equal(utilm.isInputType(type), false, `${String(type)} is not an ordered input type`)
  }
  for (const bad of [
    input('append', { items: [Buffer.alloc(0)] }),
    input('append', { clientId: 'public' }), input('drain'),
    input('cluster_message'), input('apply_result'),
    input('apply_result', { reply: { type: 'ack', cid: 'executed' } }),
    input('apply_result', { reply: { type: 'err', cid: 'executed', tickCount: 0n } }),
  ]) {
    t.throws(() => utilm.parseProcRequest(bad, 'a', nodes), isError(Err.RPC_ILLEGAL),
      `${bad.type}: operation-specific fields remain required`)
  }
  t.end()
})

test('every ordered request requires its tick-correlated Proc output completion', (t) => {
  for (const request of orderedRequests()) {
    for (const role of ['procA', 'procB', 'procC']) {
      const reply = { type: 'ack', cid: request.cid, tickCount: request.tickCount, outputs: [] }
      t.equal(utilm.validateLocalAckErr(reply, request, role), reply,
        `${request.type}/${role}: accepts a matching empty completion`)
      for (const changed of [
        { tickCount: undefined }, { tickCount: 2 }, { tickCount: -1n }, { outputs: undefined },
        { outputs: role === 'procA' ? [new Uint8Array(8)] : [Buffer.alloc(16)] },
      ]) {
        t.throws(() => utilm.validateLocalAckErr({ ...reply, ...changed }, request, role),
          isError(Err.RPC_ILLEGAL), `${request.type}/${role}: rejects malformed completion fields`)
      }
      t.throws(() => utilm.validateLocalAckErr({ ...reply, tickCount: 2n }, request, role),
        isError(Err.MONSTER_ILLEGAL), `${request.type}/${role}: completion must match the request tick`)
      const err = { type: 'err', cid: request.cid, message: 'failed', code: Err.SQLITE_ERROR }
      t.equal(utilm.validateLocalAckErr(err, request, role), err,
        `${request.type}/${role}: error replies do not require completion fields`)
    }
  }
  t.end()
})

test('ordered input parsing distinguishes remote invalid messages from local corruption', (t) => {
  const envelope = { to: 'a', message: { from: 'b', type: 'vote', term: 2n, voteGranted: true } }
  const parsed = utilm.parseProcRequest(input('cluster_message', {
    from: 'b', frame: wire.encodeFrame(envelope),
  }), 'a', nodes)
  t.deepEqual(parsed.clusterMessage, envelope.message)
  t.equal(parsed.warning, null)
  const malformed = Buffer.from([0xdb, 0, 0, 0, 10, 65])
  const badPayload = wire.framePayload(malformed, wire.checksum(malformed))
  const ignored = utilm.parseProcRequest(input('cluster_message', { from: 'b', frame: badPayload }), 'a', nodes)
  t.equal(ignored.clusterMessage, null)
  t.equal(ignored.warning.code, Err.FRAME_CORRUPT)
  t.match(ignored.warning.message, /^cluster frame from b:/)
  const corrupt = Buffer.from(badPayload)
  corrupt[0] ^= 1
  t.throws(() => utilm.parseProcRequest(input('cluster_message', { from: 'b', frame: corrupt }), 'a', nodes),
    isError(Err.FRAME_CORRUPT), 'nested checksum failures are never downgraded')
  for (const changed of [
    null,
    { to: 'a' },
    { ...envelope, to: 'c' },
    { ...envelope, message: { ...envelope.message, from: 'c' } },
    { ...envelope, message: { from: 'b' } },
  ]) {
    const ignored = utilm.parseProcRequest(input('cluster_message', {
      from: 'b', frame: wire.encodeFrame(changed),
    }), 'a', nodes)
    t.equal(ignored.clusterMessage, null, 'invalid remote envelope is discarded')
    t.equal(ignored.warning.code, Err.RPC_ILLEGAL)
    t.match(ignored.warning.message, /^cluster frame from b:/)
  }
  for (const from of ['a', 'unknown']) {
    t.throws(() => utilm.parseProcRequest(input('cluster_message', {
      from, frame: wire.encodeFrame(envelope),
    }), 'a', nodes), isError(Err.RPC_ILLEGAL), 'invalid local input sender remains fatal')
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
  t.equal(utilm.validateLocalAckErr(ack, message, 'apply'), ack)
  t.throws(() => utilm.validateLocalAckErr({ ...ack, tickCount: 3n }, message, 'apply'),
    isError(Err.MONSTER_ILLEGAL))
  for (const tickCount of [undefined, 3, -1n]) {
    t.throws(() => utilm.validateLocalAckErr({ ...ack, tickCount }, message, 'apply'),
      isError(Err.RPC_ILLEGAL), 'malformed Apply completion ticks remain message errors')
  }
  const error = { type: 'err', cid: message.cid, error: { message: 7, code: 'native' } }
  t.equal(utilm.validateLocalAckErr(error, message, 'apply'), error,
    'operation errors preserve normalization instead of requiring success fields')
  t.end()
})

test('Proc completion validation requires checksum halves without changing their bytes', (t) => {
  const request = input('tick')
  for (const role of ['procB', 'procC']) {
    const part = Buffer.from('0123456789abcdef', 'hex')
    const reply = { type: 'ack', cid: request.cid, tickCount: request.tickCount,
      outputs: [part], extra: true }
    const bytes = wire.encodePayload(reply)
    t.equal(utilm.validateLocalAckErr(reply, request, role), reply)
    t.equal(reply.outputs[0], part, `${role}: validation retains the original checksum half`)
    t.deepEqual(wire.encodePayload(reply), bytes, 'validation leaves completion encoding unchanged')
    t.doesNotThrow(() => utilm.validateLocalAckErr({ ...reply, outputs: [] }, request, role),
      'empty output lists remain valid')
    for (const value of [undefined, null, 'checksum', new Uint8Array(8),
      Buffer.alloc(0), Buffer.alloc(7), Buffer.alloc(9), Buffer.alloc(16)]) {
      t.throws(() => utilm.validateLocalAckErr({ ...reply, outputs: [value] }, request, role),
        isError(Err.RPC_ILLEGAL), `${role}: only eight-byte Buffer contributions are accepted`)
    }
  }
  const payload = wire.encodePayload({ to: 'b', message: { type: 'append' } })
  const a = { type: 'ack', cid: request.cid, tickCount: request.tickCount, outputs: [payload] }
  t.equal(utilm.validateLocalAckErr(a, request, 'procA'), a, 'A still sends complete payload bytes')
  t.equal(a.outputs[0], payload)
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
  t.throws(() => utilm.validateCommandResult(reply, 1), isError(Err.MONSTER_ILLEGAL),
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
