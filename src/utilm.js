import { pack, unpack } from 'msgpackr'
import { z } from 'zod'
import * as Err from './error.js'
import { schemas, validate, HASH_BYTES, hash, hashParts } from './util.js'
import * as wire from './wire.js'

const requestSchema = z.looseObject({ type: z.string(), cid: schemas.cid })
const helloSchema = requestSchema.extend({ type: z.literal('hello'), seed: schemas.seed })
const replySchema = requestSchema.extend({ type: z.enum(['ack', 'err']) })
const applyReplySchema = requestSchema.extend({
  type: z.literal('ack'), tickCount: schemas.tickCount,
})
const completionSchema = z.looseObject({ tickCount: schemas.tickCount })
const outcomeCode = z.number().int().nullable()
const outcomeSchema = z.union([
  z.tuple([z.literal(0), z.unknown()]),
  z.tuple([z.literal(1), z.string(), outcomeCode, outcomeCode]),
  z.tuple([z.literal(1), z.string(), outcomeCode, outcomeCode, z.string()]),
])
const outcomesSchema = z.array(outcomeSchema)
const commandResultSchema = z.looseObject({
  cmdSeq: schemas.nonnegativeSeq, results: outcomesSchema,
})
const matchIndexSchema = z.array(schemas.seq)
const cmdSchema = z.looseObject({
  type: z.literal('cmd'), matchIndex: matchIndexSchema,
  items: z.array(schemas.bytes).min(1),
})
const nodeIdsSchema = z.array(schemas.cid)
const syncSchema = z.looseObject({
  type: z.literal('sync'), cmdSeq: schemas.nonnegativeSeq, quorum: z.boolean(),
  agree: nodeIdsSchema, disagree: nodeIdsSchema,
})
const executeSchema = requestSchema.extend({
  type: z.literal('execute'), term: schemas.term, seq: schemas.nonnegativeSeq,
  tickCount: schemas.tickCount, entryHash: schemas.digest,
})
const setupSchema = executeSchema.extend({
  items: z.null(), matchIndex: z.null(),
})
const executeCmdSchema = executeSchema.extend({
  items: z.array(schemas.buffer).min(1), matchIndex: matchIndexSchema,
})
const inputSchema = requestSchema.extend({
  type: z.literal('input'), tickCount: schemas.tickCount, timems: schemas.timems,
  kind: z.enum(['start', 'rpc', 'append', 'drain', 'apply', 'tick']),
})
const inputSchemas = {
  start: inputSchema, tick: inputSchema,
  rpc: inputSchema.extend({ from: schemas.cid }),
  append: inputSchema.extend({
    requestId: schemas.cid, items: z.array(schemas.buffer).min(1),
  }),
  drain: inputSchema.extend({ requestId: schemas.cid }),
  apply: inputSchema.extend({ reply: applyReplySchema }),
}
const envelopeSchema = z.looseObject({ to: schemas.cid, message: schemas.rpc })
const outputCompletionSchema = completionSchema.extend({ outputs: z.array(schemas.buffer) })
const checksumCompletionSchema = completionSchema.extend({ outputs: z.array(schemas.hash) })
const captureSchema = z.looseObject({ selection: z.looseObject({
  state: schemas.hash, receipt: schemas.hash, donor: z.enum(['procA', 'procB', 'procC']),
}) })
const statusSchema = z.looseObject({
  state: z.enum(['follower', 'candidate', 'leader']), leader: schemas.cid.nullable(),
  term: schemas.term, seq: schemas.seq, commitSeq: schemas.seq,
  commitTerm: schemas.seq, followers: nodeIdsSchema,
})
const eventSchema = z.looseObject({
  type: z.enum(['change', 'commit', 'cmd', 'sync']), status: statusSchema,
})
const eventSchemas = {
  change: eventSchema,
  commit: eventSchema.extend({ seq: schemas.nonnegativeSeq }),
  cmd: eventSchema.extend({ cmdSeq: schemas.nonnegativeSeq, cmdCount: z.number().int().min(1) }),
  sync: eventSchema.extend({
    cmdSeq: schemas.nonnegativeSeq, syncSeq: schemas.nonnegativeSeq,
    quorum: z.boolean(), agree: nodeIdsSchema, disagree: nodeIdsSchema,
  }),
}
const monsterSchemas = {
  outcome: outcomeSchema, outcomes: outcomesSchema, commandResult: commandResultSchema,
  cmd: cmdSchema, sync: syncSchema,
  forwarded: z.looseObject({
    cid: schemas.cid, term: schemas.term, items: z.array(schemas.bytes).min(1),
  }),
  outcomeRequest: z.looseObject({ cmdSeq: schemas.nonnegativeSeq }),
  outcomeReport: z.looseObject({ cmdSeq: schemas.nonnegativeSeq, digest: schemas.digest }),
}

const fail = (message, code = Err.RPC_ILLEGAL) => {
  throw new Err.ErrorWithCode(message, code)
}

const validateRequest = (message, owner) => {
  const hello = message?.type === 'hello'
  return validate(hello ? helloSchema : requestSchema,
    message, `${owner} ${hello ? 'hello' : 'request'}`)
}

const validateApplyRequest = (message) => message?.type === 'execute'
  ? validateExecute(message) : validateRequest(message, 'Apply')
const validateReply = (message) => validate(replySchema, message, 'local reply')

function parseProcRequest(message, id, nodes) {
  const parsed = { message, clusterMessage: null, warning: null }
  if (message?.type !== 'input') {
    validateRequest(message, 'Proc')
    return parsed
  }
  const kind = message?.kind
  const schema = typeof kind === 'string' && Object.hasOwn(inputSchemas, kind)
    ? inputSchemas[kind] : inputSchema
  validate(schema, message, 'Proc input')
  if (message.kind !== 'rpc') { return parsed }
  if (!nodes.includes(message.from) || message.from === id) {
    fail('Proc cluster sender is illegal')
  }
  // Gate has checked this frame already; a later checksum failure is local.
  const payload = wire.verifyFrame(message.frame)
  let envelope
  try { envelope = wire.decodePayload(payload) } catch (err) {
    parsed.warning = Err.wrapError(err, null, `cluster frame from ${message.from}: `)
    return parsed
  }
  validate(envelopeSchema, envelope, 'Proc cluster envelope')
  if (envelope.to !== id || envelope.message.from !== message.from) {
    fail('Proc cluster envelope is illegal')
  }
  parsed.clusterMessage = envelope.message
  return parsed
}

function validateExecute(message) {
  const setup = message?.seq === 0n
  validate(setup ? setupSchema : executeCmdSchema,
    message, setup ? 'Apply setup' : 'Apply command')
  if (!setup && !message.matchIndex.every((seq) => seq < message.seq)) {
    fail('Apply command matchIndex is illegal')
  }
  return message
}

function validateLocalReply(message, request, role) {
  // The reply header was validated before looking up its request.
  if (message.type === 'err') { return message }
  if (request.type === 'input') {
    validate(role === 'procA' ? outputCompletionSchema : checksumCompletionSchema,
      message, 'Proc completion')
  } else if (request.type === 'execute') {
    validate(completionSchema, message, 'Apply reply')
  } else if (request.type === 'capture') {
    validate(captureSchema, message, 'capture reply')
  }
  if ((request.type === 'input' || request.type === 'execute') &&
      message.tickCount !== request.tickCount) {
    fail(`${request.type === 'input' ? 'Proc' : 'Apply'} completion tickCount is illegal`)
  }
  return message
}

const parseOutput = (payload) => validate(envelopeSchema, wire.decodePayload(payload), 'output')

function validateEvent(message, nodes, id) {
  const type = message?.type
  const schema = typeof type === 'string' && Object.hasOwn(eventSchemas, type)
    ? eventSchemas[type] : eventSchema
  validate(schema, message, 'event')
  const { leader, followers } = message.status
  if (leader !== null && !nodes.includes(leader)) { fail('event status.leader is illegal') }
  if (!followers.every((peer, index) => peer !== id && nodes.includes(peer) &&
      (index === 0 || followers[index - 1] < peer))) {
    fail('event status.followers is illegal')
  }
  if (message.type === 'sync') {
    for (const name of ['agree', 'disagree']) {
      if (!message[name].every((peer, index) => nodes.includes(peer) &&
          (index === 0 || message[name][index - 1] < peer))) {
        fail(`sync event ${name} is illegal`)
      }
    }
  }
  return message
}

function validateCommandResult(message, count) {
  validate(commandResultSchema, message, 'command result')
  if (message.results.length !== count) { fail('command result results has illegal length') }
  return message
}

function decodeEntry(buffer) {
  let entry
  try { entry = unpack(buffer) } catch (err) {
    throw Err.wrapError(err, Err.MONSTER_CORRUPT, 'entry decode ')
  }
  if (entry?.type !== 'cmd' && entry?.type !== 'sync') {
    fail('entry type is corrupt', Err.MONSTER_CORRUPT)
  }
  return entry
}

const asBuffer = (value) => {
  if (value === null || value === undefined) { return value }
  return Buffer.isBuffer(value)
    ? value
    : Buffer.from(value.buffer, value.byteOffset, value.byteLength)
}

const outcomesWire = (outcomes, includeStack=true) => outcomes.map((outcome) => {
  if (outcome.status === 'fulfilled') { return [0, outcome.result] }
  const { message, code, sqlCode, stack } = outcome.error
  const item = [1, message, code, sqlCode]
  if (includeStack && typeof stack === 'string') { item.push(stack) }
  return item
})

const outcomesPublic = (outcomes) => outcomes.map((item) => {
  if (item[0] === 0) { return { status: 'fulfilled', value: item[1] } }
  return {
    status: 'rejected',
    reason: Err.wrapError({
      message: item[1], code: item[2], sqlCode: item[3], stack: item[4],
    }),
  }
})

const digestCmd = (term, seq, patchset, outcomes) =>
  hash(pack([term, seq, patchset, outcomesWire(outcomes, false)]))

const assertEncodable = (value) => {
  try { pack(value) } catch {
    throw new Err.ErrorWithCode(
      'apply result is not MessagePack-encodable', Err.APPLY_ERROR,
    )
  }
}

const isDigest = (digest) => digest instanceof Uint8Array &&
  digest.byteLength === HASH_BYTES

const digestEntry = (term, buffer) => {
  const prefix = Buffer.allocUnsafe(8)
  prefix.writeBigUInt64LE(term)
  return buffer === null ? hash(prefix) : hashParts(prefix, buffer)
}

const validateOutcomeRequestMsg = (msg) =>
  validate(monsterSchemas.outcomeRequest, msg, 'outcome request')

const validateOutcomeMsg = (msg) =>
  validate(monsterSchemas.outcomeReport, msg, 'outcome')

const validateCmdEntry = (node, entry, seq) => {
  validate(cmdSchema, entry, 'CMD entry', Err.MONSTER_CORRUPT)
  if (entry.matchIndex.length !== node.nodes.length) {
    throw new Err.ErrorWithCode(
      'CMD entry matchIndex has corrupt length',
      Err.MONSTER_CORRUPT,
    )
  }
  if (!entry.matchIndex.every((match) => match < seq)) {
    throw new Err.ErrorWithCode(
      'CMD entry matchIndex has corrupt seq',
      Err.MONSTER_CORRUPT,
    )
  }
}

const validateNodes = (node, nodes, name, minimum=0) => {
  if (nodes.length < minimum) {
    throw new Err.ErrorWithCode(
      `SYNC entry ${name} has corrupt length`,
      Err.MONSTER_CORRUPT,
    )
  }
  const sorted = [...new Set(nodes)].sort()
  if (sorted.length !== nodes.length ||
      !nodes.every((id) => node.nodes.includes(id)) ||
      !nodes.every((id, index) => id === sorted[index])) {
    throw new Err.ErrorWithCode(
      `SYNC entry ${name} has corrupt id`,
      Err.MONSTER_CORRUPT,
    )
  }
  return sorted
}

const validateSyncEntry = (node, entry) => {
  validate(syncSchema, entry, 'SYNC entry', Err.MONSTER_CORRUPT)
  const agree = validateNodes(
    node, entry.agree, 'agree',
    entry.quorum ? node.quorum : 0,
  )
  const disagree = validateNodes(node, entry.disagree, 'disagree')
  if (!entry.quorum && agree.length !== 0) {
    throw new Err.ErrorWithCode(
      'SYNC entry quorum:false agree must be empty',
      Err.MONSTER_CORRUPT,
    )
  }
  if (entry.quorum && !isDigest(entry.digest)) {
    throw new Err.ErrorWithCode(
      'SYNC entry digest is corrupt',
      Err.MONSTER_CORRUPT,
    )
  }
  return { agree, disagree }
}

const isFwdCmdMsg = (msg) => monsterSchemas.forwarded.validate(msg)

const isOutcomeTuple = (item) => outcomeSchema.validate(item)

const isOutcomes = (outcomes, count) => outcomesSchema.validate(outcomes) &&
  outcomes.length === count

const isFwdCmdAck = (msg, resultCount) => {
  return commandResultSchema.validate(msg) && msg.results.length === resultCount
}

const normalizeKeepOptions = (opts) => {
  const names = ['keepTarget', 'keepTrigger']
  const supplied = names.filter((name) => opts[name] !== undefined)
  if (supplied.length === 0) { return null }
  if (supplied.length !== names.length) {
    throw new Err.ErrorWithCode(
      'MonsterFt keepTarget and keepTrigger must be supplied together',
      Err.ARGUMENT_ILLEGAL,
    )
  }
  for (const name of names) {
    if (!Number.isSafeInteger(opts[name])) {
      throw new Err.ErrorWithCode(
        `MonsterFt ${name} must be a safe integer`,
        Err.ARGUMENT_ILLEGAL,
      )
    }
  }
  const target = BigInt(opts.keepTarget)
  const trigger = BigInt(opts.keepTrigger)
  if (target < 2n) {
    throw new Err.ErrorWithCode(
      'MonsterFt keepTarget must be >= 2',
      Err.ARGUMENT_ILLEGAL,
    )
  }
  if (target >= trigger) {
    throw new Err.ErrorWithCode(
      'MonsterFt keepTarget must be < keepTrigger',
      Err.ARGUMENT_ILLEGAL,
    )
  }
  return { target, trigger }
}

export {
  monsterSchemas, validateApplyRequest, validateReply,
  parseProcRequest, validateExecute, validateLocalReply,
  parseOutput, validateEvent, validateCommandResult, decodeEntry,
  asBuffer, outcomesWire, outcomesPublic, digestCmd, assertEncodable,
  digestEntry, isDigest,
  validateOutcomeRequestMsg, validateOutcomeMsg,
  validateCmdEntry, validateSyncEntry,
  isFwdCmdMsg, isFwdCmdAck, isOutcomeTuple, isOutcomes,
  normalizeKeepOptions,
}
