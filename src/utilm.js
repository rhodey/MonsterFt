import { pack, unpack } from 'msgpackr'
import { z } from 'zod'
import * as Err from './error.js'
import { schemas, validate, HASH_BYTES, hash, hashParts } from './util.js'
import * as wire from './wire.js'
import { FOLLOWER, CANDIDATE, LEADER } from './raft.js'

const requestSchema = z.looseObject({ type: z.string(), cid: schemas.cid })
const helloSchema = requestSchema.extend({ type: z.literal('hello'), seed: schemas.seed })
const requestSchemas = {
  Proc: requestSchema.extend({ type: z.enum(['open', 'capture', 'adopt', 'reconcile', 'close']) }),
  Apply: requestSchema.extend({ type: z.enum(['open', 'ready', 'close']) }),
}
const ackErrSchema = requestSchema.extend({ type: z.enum(['ack', 'err']) })
const clientAckErrSchema = z.looseObject({
  type: z.enum(['ack', 'err']), clientId: schemas.cid,
})
const applyAckSchema = requestSchema.extend({
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
const syncFieldsSchema = z.looseObject({
  type: z.literal('sync'), cmdSeq: schemas.nonnegativeSeq,
  agree: nodeIdsSchema, disagree: nodeIdsSchema,
})
const syncSchema = z.union([
  syncFieldsSchema.extend({ quorum: z.literal(true), digest: schemas.digest }),
  syncFieldsSchema.extend({ quorum: z.literal(false), agree: nodeIdsSchema.length(0) }),
])
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
  tickCount: schemas.tickCount, timems: schemas.timems,
})
const inputSchemas = {
  start: inputSchema, tick: inputSchema,
  cluster_message: inputSchema.extend({ from: schemas.cid }),
  append: inputSchema.extend({
    clientId: schemas.cid, items: z.array(schemas.buffer).min(1),
  }),
  drain: inputSchema.extend({ clientId: schemas.cid }),
  apply_result: inputSchema.extend({ reply: applyAckSchema }),
}
const envelopeSchema = z.looseObject({ to: schemas.cid, message: schemas.rpc })
const outputCompletionSchema = completionSchema.extend({ outputs: z.array(schemas.buffer) })
const checksumCompletionSchema = completionSchema.extend({
  outputs: z.array(z.custom((value) => Buffer.isBuffer(value) && value.length === HASH_BYTES / 2)),
})
const captureSchema = z.looseObject({ selection: z.looseObject({
  state: schemas.hash, receipt: schemas.hash, donor: z.enum(['procA', 'procB', 'procC']),
}) })
const statusSchema = z.looseObject({
  state: z.enum([FOLLOWER, CANDIDATE, LEADER]), leader: schemas.cid.nullable(),
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

const asBuffer = (value) => {
  if (value === null || value === undefined) { return value }
  return Buffer.isBuffer(value)
    ? value
    : Buffer.from(value.buffer, value.byteOffset, value.byteLength)
}

const isInputType = (type) => typeof type === 'string' && Object.hasOwn(inputSchemas, type)

const isDigest = (digest) => digest instanceof Uint8Array &&
  digest.byteLength === HASH_BYTES

const isOutcomeTuple = (item) => outcomeSchema.validate(item)

const isOutcomes = (outcomes, count) => outcomesSchema.validate(outcomes) &&
  outcomes.length === count

const isFwdCmdMsg = (msg) => monsterSchemas.forwarded.validate(msg)

const isFwdCmdAck = (msg, resultCount) => {
  return commandResultSchema.validate(msg) && msg.results.length === resultCount
}

const assertEncodable = (value) => {
  try { pack(value) } catch {
    throw new Err.ErrorWithCode(
      'apply result is not MessagePack-encodable', Err.APPLY_ERROR,
    )
  }
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

const digestEntry = (term, buffer) => {
  const prefix = Buffer.allocUnsafe(8)
  prefix.writeBigUInt64LE(term)
  return buffer === null ? hash(prefix) : hashParts(prefix, buffer)
}

const digestCmd = (term, seq, patchset, outcomes) =>
  hash(pack([term, seq, patchset, outcomesWire(outcomes, false)]))

const normalizeKeepOptions = (opts) => {
  const names = ['keepTarget', 'keepTrigger']
  const supplied = names.filter((name) => opts[name] !== undefined)
  if (supplied.length === 0) { return null }
  if (supplied.length !== names.length) {
    throw new Err.ErrorWithCode(
      'keepTarget and keepTrigger must be supplied together',
      Err.ARGUMENT_ILLEGAL,
    )
  }
  for (const name of names) {
    if (!Number.isSafeInteger(opts[name])) {
      throw new Err.ErrorWithCode(
        `${name} must be a safe integer`,
        Err.ARGUMENT_ILLEGAL,
      )
    }
  }
  const target = BigInt(opts.keepTarget)
  const trigger = BigInt(opts.keepTrigger)
  if (target < 2n) {
    throw new Err.ErrorWithCode(
      'keepTarget must be >= 2',
      Err.ARGUMENT_ILLEGAL,
    )
  }
  if (target >= trigger) {
    throw new Err.ErrorWithCode(
      'keepTarget must be < keepTrigger',
      Err.ARGUMENT_ILLEGAL,
    )
  }
  return { target, trigger }
}
const validateAckErr = (message) => validate(ackErrSchema, message, 'local ack_err')
const validateClientAckErr = (message) => validate(clientAckErrSchema, message, 'client ack_err')

const validateOutcomeRequestMsg = (msg) =>
  validate(monsterSchemas.outcomeRequest, msg, 'outcome request')

const validateOutcomeMsg = (msg) =>
  validate(monsterSchemas.outcomeReport, msg, 'outcome')

function validateCommandResult(message, count) {
  validate(commandResultSchema, message, 'command result')
  if (message.results.length !== count) {
    fail('command result results has illegal length', Err.MONSTER_ILLEGAL)
  }
  return message
}

const validateRequest = (message, owner) => {
  const hello = message?.type === 'hello'
  return validate(hello ? helloSchema : requestSchemas[owner],
    message, `${owner} ${hello ? 'hello' : 'request'}`)
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

const validateApplyRequest = (message) => message?.type === 'execute'
  ? validateExecute(message) : validateRequest(message, 'Apply')

function validateLocalAckErr(message, request, role) {
  // Gate has validated type/cid and matched the sender and cid to a request.
  // ERR needs no completion fields; Gate normalizes its error details.
  if (message.type === 'err') { return message }
  const procInput = isInputType(request.type)
  if (procInput) {
    validate(role === 'procA' ? outputCompletionSchema : checksumCompletionSchema,
      message, 'Proc completion')
  } else if (request.type === 'execute') {
    validate(completionSchema, message, 'Apply ACK')
  } else if (request.type === 'capture') {
    validate(captureSchema, message, 'capture ACK')
  }
  if ((procInput || request.type === 'execute') &&
      message.tickCount !== request.tickCount) {
    fail(`${procInput ? 'Proc' : 'Apply'} completion tickCount is illegal`, Err.MONSTER_ILLEGAL)
  }
  return message
}

const parseOutput = (payload) => validate(envelopeSchema, wire.decodePayload(payload), 'output')

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

const validateSyncEntry = (node, entry) => {
  validate(syncSchema, entry, 'SYNC entry', Err.MONSTER_CORRUPT)
  const agree = validateNodes(
    node, entry.agree, 'agree',
    entry.quorum ? node.quorum : 0,
  )
  const disagree = validateNodes(node, entry.disagree, 'disagree')
  return { agree, disagree }
}

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

function parseProcRequest(message, id, nodes) {
  const parsed = { message, clusterMessage: null, warning: null }
  if (!isInputType(message?.type)) {
    validateRequest(message, 'Proc')
    return parsed
  }
  validate(inputSchemas[message.type], message, 'Proc input')
  if (message.type !== 'cluster_message') { return parsed }
  if (!nodes.includes(message.from) || message.from === id) {
    fail('Proc cluster sender is illegal')
  }
  // Gate has checked this frame already; a later checksum failure is local.
  const payload = wire.verifyFrame(message.frame)
  try {
    const envelope = validate(envelopeSchema, wire.decodePayload(payload), 'Proc cluster envelope')
    if (envelope.to !== id || envelope.message.from !== message.from) {
      fail('Proc cluster envelope is illegal')
    }
    parsed.clusterMessage = envelope.message
  } catch (err) {
    parsed.warning = Err.wrapError(err, null, `cluster frame from ${message.from}: `)
  }
  return parsed
}

export {
  monsterSchemas, validateApplyRequest, validateAckErr, validateClientAckErr,
  parseProcRequest, validateExecute, validateLocalAckErr,
  parseOutput, validateEvent, validateCommandResult, decodeEntry,
  asBuffer, outcomesWire, outcomesPublic, digestCmd, assertEncodable,
  digestEntry, isDigest, isInputType,
  validateOutcomeRequestMsg, validateOutcomeMsg,
  validateCmdEntry, validateSyncEntry,
  isFwdCmdMsg, isFwdCmdAck, isOutcomeTuple, isOutcomes,
  normalizeKeepOptions,
}
