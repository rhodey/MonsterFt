import * as Err from './error.js'
import { isCid, isSeq, isTerm } from './util.js'

const DIGEST_BYTES = 32

const isDigest = (digest) => digest instanceof Uint8Array &&
  digest.byteLength === DIGEST_BYTES

const validateOutcomeRequestMsg = (msg) => {
  if (!isSeq(msg.cmdSeq) || msg.cmdSeq < 0n) {
    throw new Err.ErrorWithCode(
      'outcome request cmdSeq is illegal',
      Err.RPC_ILLEGAL,
    )
  }
}

const validateOutcomeMsg = (msg) => {
  if (!isSeq(msg.cmdSeq) || msg.cmdSeq < 0n) {
    throw new Err.ErrorWithCode(
      'outcome cmdSeq is illegal',
      Err.RPC_ILLEGAL,
    )
  }
  if (!isDigest(msg.digest)) {
    throw new Err.ErrorWithCode('outcome digest is illegal', Err.RPC_ILLEGAL)
  }
}

const validateCmdEntry = (node, entry, seq) => {
  if (!Array.isArray(entry.matchIndex) ||
      entry.matchIndex.length !== node.nodes.length) {
    throw new Err.ErrorWithCode(
      'CMD entry matchIndex has corrupt length',
      Err.MONSTER_CORRUPT,
    )
  }
  if (!entry.matchIndex.every((match) => isSeq(match) && match < seq)) {
    throw new Err.ErrorWithCode(
      'CMD entry matchIndex has corrupt seq',
      Err.MONSTER_CORRUPT,
    )
  }
  if (!Array.isArray(entry.items) || entry.items.length <= 0) {
    throw new Err.ErrorWithCode(
      'CMD entry items must be a non-empty array',
      Err.MONSTER_CORRUPT,
    )
  }
  if (!entry.items.every((item) => item instanceof Uint8Array)) {
    throw new Err.ErrorWithCode(
      'CMD entry items must be buffers',
      Err.MONSTER_CORRUPT,
    )
  }
}

const validateNodes = (node, nodes, name, minimum=0) => {
  if (!Array.isArray(nodes) || nodes.length < minimum) {
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
  if (!isSeq(entry.cmdSeq) || entry.cmdSeq < 0n) {
    throw new Err.ErrorWithCode(
      'SYNC entry cmdSeq is corrupt',
      Err.MONSTER_CORRUPT,
    )
  }
  if (typeof entry.quorum !== 'boolean') {
    throw new Err.ErrorWithCode(
      'SYNC entry quorum is corrupt',
      Err.MONSTER_CORRUPT,
    )
  }
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

const isFwdCmdMsg = (msg) => isCid(msg.cid) && isTerm(msg.term) &&
  Array.isArray(msg.items) && msg.items.length > 0 &&
  msg.items.every((item) => item instanceof Uint8Array)

const isOutcomeTuple = (item) => {
  if (!Array.isArray(item)) { return false }
  if (item.length === 2 && item[0] === 0) { return true }
  return (item.length === 4 ||
    (item.length === 5 && typeof item[4] === 'string')) && item[0] === 1 &&
    typeof item[1] === 'string' &&
    (item[2] === null || Number.isSafeInteger(item[2])) &&
    (item[3] === null || Number.isSafeInteger(item[3]))
}

const isFwdCmdAck = (msg, resultCount) => {
  return isSeq(msg.cmdSeq) && msg.cmdSeq >= 0n &&
    Array.isArray(msg.results) &&
    msg.results.length === resultCount &&
    msg.results.every(isOutcomeTuple)
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
  validateOutcomeRequestMsg, validateOutcomeMsg,
  validateCmdEntry, validateSyncEntry,
  isFwdCmdMsg, isFwdCmdAck,
  normalizeKeepOptions,
}
