import { xoroshiro128plusFromState } from 'pure-rand/generator/xoroshiro128plus'
import { uniformInt } from 'pure-rand/distribution/uniformInt'
import * as Err from './error.js'

class Random {
  constructor(seed) {
    if (!Buffer.isBuffer(seed) || seed.length !== 16 || seed.every((v) => v === 0)) {
      throw new Err.ErrorWithCode('seed must be a nonzero 16-byte Buffer', Err.RPC_ILLEGAL)
    }
    const state = [0, 4, 8, 12].map((offset) => seed.readInt32LE(offset))
    this.generator = xoroshiro128plusFromState(state)
  }

  int(min, max) { return uniformInt(this.generator, min, max) }

  cid() {
    const bytes = Buffer.allocUnsafe(16)
    for (let offset = 0; offset < 16; offset += 4) {
      bytes.writeInt32LE(this.generator.next(), offset)
    }
    return bytes.toString('hex')
  }
}

export { Random }
