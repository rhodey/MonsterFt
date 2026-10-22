import { Packr, Unpackr } from 'msgpackr'
import { RESERVE_START_SPACE } from 'msgpackr/pack'
import * as Err from './error.js'
import { HASH_BYTES, hash as checksum } from './util.js'

const encoder = new Packr({ useRecords: false, sequential: false })
const decoder = new Unpackr({ useRecords: false, int64AsType: 'bigint' })

const encodePayload = (value) => encoder.pack(value)

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
  const frame = encoder.pack(value, RESERVE_START_SPACE | HASH_BYTES)
  checksum(frame.subarray(HASH_BYTES)).copy(frame, 0)
  return frame
}

function verifyFrame(frame) {
  if (!Buffer.isBuffer(frame) || frame.length <= HASH_BYTES) {
    throw new Err.ErrorWithCode('frame is incomplete', Err.FRAME_CORRUPT)
  }
  const payload = frame.subarray(HASH_BYTES)
  if (!checksum(payload).equals(frame.subarray(0, HASH_BYTES))) {
    throw new Err.ErrorWithCode('frame checksum differs', Err.FRAME_CORRUPT)
  }
  return payload
}

const decodeFrame = (frame) => decodePayload(verifyFrame(frame))

function errorToWire(error, includeStack = true) {
  const err = Err.wrapError(error)
  const value = { message: err.message, code: err.code, sqlCode: err.sqlCode }
  if (includeStack && typeof err.stack === 'string') { value.stack = err.stack }
  return value
}

const errorFromWire = (value) => Err.wrapError(value)

export {
  encodePayload, decodePayload, checksum, framePayload,
  encodeFrame, verifyFrame, decodeFrame, errorToWire, errorFromWire,
}
