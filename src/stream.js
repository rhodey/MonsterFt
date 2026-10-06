import { Transform } from 'node:stream'
import sodium from 'libsodium-wrappers'

class EncryptingStream extends Transform {
  constructor(key) {
    super()
    const res = sodium.crypto_secretstream_xchacha20poly1305_init_push(key)
    this.state = res.state
    this.push(Buffer.from(res.header))
  }

  _transform(chunk, encoding, callback) {
    let output
    try {
      const bytes = sodium.crypto_secretstream_xchacha20poly1305_push(this.state,
        chunk, null, sodium.crypto_secretstream_xchacha20poly1305_TAG_MESSAGE,
      )
      const buf = Buffer.from(bytes)
      const len = Buffer.allocUnsafe(4)
      len.writeUInt32BE(buf.byteLength)
      output = Buffer.concat([len, buf])
    } catch (err) {
      callback(err)
      return
    }
    callback(null, output)
  }

  _destroy(err, callback) {
    // Secretstream state is a native allocation, not a GC-managed object.
    if (this.state !== null) { sodium.libsodium._free(this.state) }
    this.state = null
    callback(err)
  }
}

class ByteQueue {
  constructor() {
    this.clear()
  }

  append(chunk) {
    if (chunk.byteLength === 0) { return }
    this.chunks.push(Buffer.from(chunk))
    this.length += chunk.byteLength
  }

  take(size) {
    if (this.length < size) { return null }
    if (size === 0) { return Buffer.alloc(0) }

    this.length -= size
    const first = this.chunks[this.head]
    const available = first.byteLength - this.offset
    if (available >= size) {
      const value = first.subarray(this.offset, this.offset + size)
      this._advance(size)
      return value
    }

    const value = Buffer.allocUnsafe(size)
    let copied = 0
    while (copied < size) {
      const chunk = this.chunks[this.head]
      const count = Math.min(size - copied, chunk.byteLength - this.offset)
      chunk.copy(value, copied, this.offset, this.offset + count)
      copied += count
      this._advance(count)
    }
    return value
  }

  _advance(size) {
    const chunk = this.chunks[this.head]
    this.offset += size
    if (this.offset < chunk.byteLength) { return }

    this.chunks[this.head] = null
    this.head++
    this.offset = 0
    if (this.head === this.chunks.length) {
      this.clear()
    } else if (this.head >= 1024 && this.head * 2 >= this.chunks.length) {
      this.chunks = this.chunks.slice(this.head)
      this.head = 0
    }
  }

  clear() {
    this.chunks = []
    this.head = 0
    this.offset = 0
    this.length = 0
  }
}

function streamDestroyedError() {
  const err = new Error('stream destroyed')
  err.code = 'ERR_STREAM_DESTROYED'
  return err
}

class DecryptingStream extends Transform {
  constructor(key) {
    super()
    this.key = key
    this.state = null
    this.len = null
    this.input = new ByteQueue()
    this.transformCallback = null
    this.backpressured = false
    this.draining = false
  }

  _transform(chunk, encoding, callback) {
    this.transformCallback = callback
    try {
      this.input.append(chunk)
    } catch (err) {
      this._finishTransform(err)
      return
    }
    this.backpressured = false
    this._drain()
  }

  _drain() {
    if (this.draining || this.destroyed || !this.transformCallback || this.backpressured) {
      return
    }

    this.draining = true
    let error = null
    try {
      while (!this.destroyed && this.transformCallback) {
        if (this.state === null) {
          const head = this.input.take(sodium.crypto_secretstream_xchacha20poly1305_HEADERBYTES)
          if (head === null) { break }
          const key = this.key
          this.key = null
          this.state = sodium.crypto_secretstream_xchacha20poly1305_init_pull(head, key)
        }

        if (this.len === null) {
          const len = this.input.take(4)
          if (len === null) { break }
          this.len = len.readUInt32BE(0)
        }

        const frame = this.input.take(this.len)
        if (frame === null) { break }
        this.len = null

        const res = sodium.crypto_secretstream_xchacha20poly1305_pull(this.state, frame)
        if (!res) { throw new Error('stream decrypt error') }
        const ready = this.push(Buffer.from(res.message))
        if (this.destroyed || !this.transformCallback) { break }
        if (!ready) {
          this.backpressured = true
          break
        }
      }
    } catch (err) {
      // Stream callbacks treat falsy error values as success.
      error = err || new Error(String(err))
    }
    this.draining = false

    if (!this.transformCallback) { return }
    if (error) {
      this._finishTransform(error)
    } else if (!this.backpressured) {
      this._finishTransform()
    }
  }

  _finishTransform(err) {
    const callback = this.transformCallback
    if (!callback) { return }
    this.transformCallback = null
    this.backpressured = false
    callback(err)
  }

  _read(size) {
    if (this.draining) { return }
    if (this.transformCallback) {
      this.backpressured = false
      this._drain()
    }
    if (!this.destroyed && !this.transformCallback) {
      super._read(size)
    }
  }

  _flush(callback) {
    if (this.state === null) {
      callback(new Error('incomplete stream header'))
    } else if (this.len !== null) {
      callback(new Error('incomplete stream frame'))
    } else if (this.input.length > 0) {
      callback(new Error('incomplete stream frame length'))
    } else {
      callback()
    }
  }

  _destroy(err, callback) {
    this.key = null
    if (this.state !== null) { sodium.libsodium._free(this.state) }
    this.state = null
    this.len = null
    this.input.clear()
    this.backpressured = false
    const transformCallback = this.transformCallback
    this.transformCallback = null
    if (transformCallback) {
      transformCallback(err || streamDestroyedError())
    }
    callback(err)
  }
}

export { EncryptingStream, DecryptingStream }
