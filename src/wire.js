import { createHash } from 'node:crypto'
import { Packr, Unpackr } from 'msgpackr'
import { RESERVE_START_SPACE } from 'msgpackr/pack'
import * as Err from './error.js'

const encoder = new Packr({ useRecords: false, sequential: false })
const decoder = new Unpackr({ useRecords: false, int64AsType: 'bigint' })

const encodePayload = (value) => encoder.pack(value)
const checksum = (payload) => createHash('sha256').update(payload).digest()

function decodePayload(payload) {
  try {
    if (!Buffer.isBuffer(payload)) { throw new Error('payload must be Buffer') }
    return decoder.unpack(payload)
  } catch (err) {
    throw Err.wrapError(err, Err.FRAME_CORRUPT, 'message decode ')
  }
}

function framePayload(payload, hash = checksum(payload)) {
  return Buffer.concat([hash, payload])
}

function encodeFrame(value) {
  // Encode directly after the space reserved for the checksum.
  const frame = encoder.pack(value, RESERVE_START_SPACE | 32)
  checksum(frame.subarray(32)).copy(frame, 0)
  return frame
}

function decodeFrame(frame) {
  if (!Buffer.isBuffer(frame) || frame.length <= 32) {
    throw new Err.ErrorWithCode('frame is incomplete', Err.FRAME_CORRUPT)
  }
  const payload = frame.subarray(32)
  if (!checksum(payload).equals(frame.subarray(0, 32))) {
    throw new Err.ErrorWithCode('frame checksum differs', Err.FRAME_CORRUPT)
  }
  return decodePayload(payload)
}

function errorToWire(error, includeStack = true) {
  const err = Err.wrapError(error)
  const value = { message: err.message, code: err.code, sqlCode: err.sqlCode }
  if (includeStack && typeof err.stack === 'string') { value.stack = err.stack }
  return value
}

const errorFromWire = (value) => Err.wrapError(value)

export {
  encodePayload, decodePayload, checksum, framePayload,
  encodeFrame, decodeFrame, errorToWire, errorFromWire,
}
