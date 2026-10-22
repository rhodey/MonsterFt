import test from 'tape'
import { HASH_BYTES, hash, hashParts } from '../src/util.js'
import { digestEntry } from '../src/utilm.js'

// Zero-seed vectors and input generator from xxHash v0.8.3's sanity checks:
// https://github.com/Cyan4973/xxHash/blob/v0.8.3/cli/xsum_sanity_check.c
// Upstream lists low64 first; canonical bytes put high64 first, both big-endian.
const vectors = [
  [0, '99aa06d3014798d86001c324468d497f'],
  [1, 'a6cd5e9392000f6ac44bdff4074eecdb'],
  [6, '082afe0b8162d12a3e7039bdda43cfc6'],
  [12, '6e3efd8fc7802b18061a192713f69ad9'],
  [24, '0ce966e4678d37611e7044d28b1b901d'],
  [48, 'a002ac4e5478227ef942219aed80f67b'],
  [81, '4952f58181ab00425e8bafb9f95fb803'],
  [222, '337e09641b948717f1aebd597cec6b3a'],
  [403, '1b6de21e332dd73dcdeb804d65c6dea4'],
  [512, '18d2d110dcc9bca1617e49599013cb6b'],
  [2048, 'f736557fd47073a5dd59e2c3a5f038e0'],
  [2240, 'ccb134fbfa7ce49d6e73a90539cf2948'],
  [2367, 'e89c0f6ff369b427cb37aeb9e5d361ed'],
]
const sanityInput = Buffer.alloc(2367)
let byteGen = 2654435761n
for (let i = 0; i < sanityInput.length; i++) {
  sanityInput[i] = Number(byteGen >> 56n)
  byteGen = BigInt.asUintN(64, byteGen * 11400714785074694797n)
}

// Independently published text vector:
// https://github.com/eddelbuettel/digest/blob/master/inst/tinytest/test_digest.R
const abcHash = '06b05ab6733a618578af5f94892f3950'

test('hash returns synchronous canonical XXH3-128 bytes with zero seed', (t) => {
  t.equal(HASH_BYTES, 16)
  const abc = hash(Buffer.from('abc'))
  t.ok(Buffer.isBuffer(abc), 'returns a Buffer directly')
  t.equal(abc.length, 16, 'keeps all 128 bits')
  t.equal(abc.toString('hex'), abcHash, 'text vector has canonical byte order')
  for (const [length, expected] of vectors) {
    t.equal(hash(sanityInput.subarray(0, length)).toString('hex'), expected,
      `${length}-byte upstream vector`)
  }
  t.end()
})

test('hashParts hashes concatenated bytes across update boundaries', (t) => {
  for (const [length, expected] of vectors) {
    const data = sanityInput.subarray(0, length)
    const parts = []
    let start = 0
    for (const boundary of [1, 16, 128, 240, 1024, length]) {
      const end = Math.min(boundary, length)
      parts.push(data.subarray(start, end))
      start = end
    }
    t.equal(hashParts(...parts).toString('hex'), expected,
      `${length}-byte stream matches independent one-shot vector`)
  }
  t.equal(hashParts().toString('hex'), vectors[0][1], 'no parts hashes empty input')
  t.equal(hashParts(Buffer.alloc(0), new Uint8Array(0)).toString('hex'), vectors[0][1],
    'empty updates hash empty input')
  t.end()
})

test('hash helpers respect Uint8Array offsets and lengths', (t) => {
  const backing = new Uint8Array(24).fill(0xff)
  backing.set(Buffer.from('abc'), 7)
  const view = backing.subarray(7, 10)
  t.equal(hash(view).toString('hex'), abcHash, 'one-shot excludes surrounding bytes')
  t.equal(hashParts(view.subarray(0, 1), Buffer.alloc(0), view.subarray(1)).toString('hex'),
    abcHash, 'streaming accepts byte views and empty Buffers')
  t.end()
})

test('digestEntry prefixes payload bytes with the unsigned little-endian term', (t) => {
  // The upstream input starts with 00 52 92 9b b7 32 a3 24.
  const term = 0x24a332b79b925200n
  t.equal(digestEntry(term, sanityInput.subarray(8, 24)).toString('hex'), vectors[4][1],
    'term prefix and payload reproduce the independent 24-byte vector')
  const prefix = Buffer.from('0052929bb732a324', 'hex')
  t.deepEqual(digestEntry(term, null), hash(prefix), 'null payload still hashes the term')
  t.deepEqual(digestEntry(term, Buffer.alloc(0)), hash(prefix),
    'empty payload uses the same framing as null')
  t.end()
})
