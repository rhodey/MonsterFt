import fs from 'node:fs'
import path from 'node:path'
import xxhash from 'xxhash-addon'
import { z } from 'zod'
import * as Err from './error.js'

const { XXHash128 } = xxhash
const HASH_BYTES = 16
const MAX_INT = 9_223_372_036_854_775_807n

const isCid = (cid) => typeof cid === 'string' && cid.length > 0
const isTerm = (term) => typeof term === 'bigint' && term >= 0n && term <= MAX_INT
const isSeq = (seq) => typeof seq === 'bigint' && seq >= -1n && seq <= MAX_INT
const isSeed = (seed) => Buffer.isBuffer(seed) && seed.length === 16 &&
  seed.some((byte) => byte !== 0)

const isRpc = (msg) => msg !== null && typeof msg === 'object' &&
  !Array.isArray(msg) && !(msg instanceof Uint8Array) &&
  typeof msg.type === 'string'

const schemas = {
  cid: z.string().min(1),
  term: z.bigint().min(0n).max(MAX_INT),
  seq: z.bigint().min(-1n).max(MAX_INT),
  nonnegativeSeq: z.bigint().min(0n).max(MAX_INT),
  tickCount: z.bigint().min(0n),
  timems: z.number().int().min(0),
  buffer: z.custom(Buffer.isBuffer),
  bytes: z.custom((value) => value instanceof Uint8Array),
  digest: z.custom((value) => value instanceof Uint8Array && value.length === HASH_BYTES),
  hash: z.custom((value) => Buffer.isBuffer(value) && value.length === HASH_BYTES),
  seed: z.custom(isSeed),
  rpc: z.custom(isRpc),
}

const logRecordSchema = z.looseObject({ term: schemas.term, entry: schemas.bytes })
const logSchemas = {
  record: logRecordSchema,
  row: logRecordSchema.extend({ seq: schemas.nonnegativeSeq }),
  election: z.looseObject({ term: schemas.term, votedFor: schemas.cid.nullable() }),
}

const ack_err = z.looseObject({
  type: z.enum(['ack', 'err']), cid: schemas.cid, term: schemas.term,
})
const raftSchemas = {
  ack_err,
  voteRequest: z.looseObject({
    type: z.literal('vote_request'), term: schemas.term,
    termP: schemas.seq, seqP: schemas.seq,
  }),
  vote: z.looseObject({
    type: z.literal('vote'), term: schemas.term, voteGranted: z.boolean(),
  }),
  append: z.looseObject({
    type: z.literal('append'), cid: schemas.cid, term: schemas.term,
    termP: schemas.seq, seqP: schemas.seq, commitSeq: schemas.seq,
    data: z.array(z.looseObject({ term: schemas.term, entry: schemas.buffer })).min(1).optional(),
  }),
}

function firstIssue(issues) {
  const issue = issues[0]
  if (issue.code !== 'invalid_union' || !issue.errors?.length) { return issue }
  // Prefer the branch whose literal tags matched before reporting its field.
  const branch = issue.errors.find((errors) =>
    !errors.some((error) => error.code === 'invalid_value')) ?? issue.errors[0]
  const nested = firstIssue(branch)
  return { ...nested, path: [...issue.path, ...nested.path] }
}

function validate(schema, value, label, code = Err.RPC_ILLEGAL) {
  if (schema.validate(value)) { return value }
  const issue = firstIssue(schema.safeParse(value).error.issues)
  const path = issue.path.reduce((text, key) => typeof key === 'number'
    ? `${text}[${key}]` : `${text}${text ? '.' : ''}${String(key)}`, '')
  const problem = code === Err.LOG_CORRUPT || code === Err.MONSTER_CORRUPT ? 'corrupt' : 'illegal'
  throw new Err.ErrorWithCode(`${label}${path ? ` ${path}` : ''} is ${problem}`, code)
}

// The native addon requires Buffers. Preserve the exact view without copying.
const asBuffer = (bytes) => Buffer.isBuffer(bytes) ? bytes :
  Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)

// XXH3-128, seed zero, canonical big-endian output.
const hash = (bytes) => XXHash128.hash(asBuffer(bytes))

function hashParts(...parts) {
  const hasher = new XXHash128(Buffer.alloc(8))
  for (const part of parts) { hasher.update(asBuffer(part)) }
  return hasher.digest()
}

const createSQLiteDirectory = (databasePath) => {
  try {
    fs.mkdirSync(path.dirname(databasePath), { recursive: true })
  } catch (err) {
    throw Err.wrapError(err, Err.FS_ERROR)
  }
}

const deleteSQLiteFiles = (databasePath) => {
  const files = [
    databasePath,
    `${databasePath}-journal`,
    `${databasePath}-wal`,
    `${databasePath}-shm`,
  ]
  try {
    for (const file of files) {
      fs.rmSync(file, { force: true })
    }
  } catch (err) {
    throw Err.wrapError(err, Err.FS_ERROR)
  }
}

export {
  isCid, isTerm, isSeq, isSeed,
  isRpc,
  schemas, logSchemas, raftSchemas, validate,
  HASH_BYTES, hash, hashParts,
  createSQLiteDirectory,
  deleteSQLiteFiles,
}
