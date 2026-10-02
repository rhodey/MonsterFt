import { once } from 'node:events'
import test from 'tape'
import sodium from 'libsodium-wrappers'
import { EncryptingStream, DecryptingStream } from '../src/index.js'

const immediate = () => new Promise((res) => setImmediate(res))

async function encryptChunks(key, input) {
  const encrypt = new EncryptingStream(key)
  const chunks = []
  encrypt.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
  const ended = once(encrypt, 'end')
  for (const chunk of input) {
    if (!encrypt.write(chunk)) { await once(encrypt, 'drain') }
  }
  encrypt.end()
  await ended
  return chunks
}

function closeEvent(stream) {
  return new Promise((res) => stream.once('close', res))
}

async function endResult(stream, input) {
  let error = null
  stream.once('error', (err) => { error = err })
  const closed = closeEvent(stream)
  stream.resume()
  stream.end(input)
  await closed
  return error
}

async function faultResult(key, parts) {
  const decrypt = new DecryptingStream(key)
  const output = []
  let error = null
  let thrown = null
  decrypt.on('data', (chunk) => output.push(chunk.toString()))
  decrypt.once('error', (err) => { error = err })
  const closed = closeEvent(decrypt)
  for (const part of parts) {
    try {
      decrypt.write(part)
    } catch (err) {
      thrown = err
      decrypt.destroy()
      break
    }
  }
  await closed
  return { output, error, thrown }
}

async function backpressuredFaultResult(key, parts) {
  const decrypt = new DecryptingStream(key)
  const output = []
  let error = null
  let closed = false
  decrypt.once('error', (err) => { error = err })
  const close = new Promise((res) => {
    decrypt.once('close', () => {
      closed = true
      res()
    })
  })
  for (const part of parts) { decrypt.write(part) }

  await immediate()
  decrypt.on('data', (chunk) => output.push(Buffer.from(chunk)))
  for (let turn = 0; turn < 4 && !closed; turn++) { await immediate() }

  const closedNaturally = closed
  if (!closed) { decrypt.destroy() }
  await close
  return { output, error, closedNaturally }
}

test('encrypting and decrypting streams round trip messages', async (t) => {
  t.plan(2)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const input = [
    Buffer.from('one'),
    Buffer.alloc(90 * 1024, 2),
    Buffer.from('three'),
  ]
  const chunks = await encryptChunks(key, input)
  const decrypt = new DecryptingStream(key)
  const output = []
  decrypt.on('data', (chunk) => output.push(chunk))
  const ended = once(decrypt, 'end')
  decrypt.end(Buffer.concat(chunks))
  await ended
  t.equal(output.length, input.length, 'preserves message boundaries')
  t.ok(output.every((chunk, idx) => chunk.equals(input[idx])), 'preserves message data')
})

test('decrypting stream drains coalesced output after backpressure', async (t) => {
  t.plan(2)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const input = Array.from({ length: 100 }, (_, idx) => Buffer.alloc(1024, idx))
  const chunks = await encryptChunks(key, input)
  const decrypt = new DecryptingStream(key)
  const output = []
  const ended = once(decrypt, 'end')
  decrypt.end(Buffer.concat(chunks))
  await immediate()
  decrypt.on('data', (chunk) => output.push(chunk))
  await ended
  t.equal(output.length, input.length, 'delivers every queued message')
  t.ok(output.every((chunk, idx) => chunk.equals(input[idx])), 'preserves queued message data')
})

test('decrypt errors use the stream callback and error event', async (t) => {
  t.plan(4)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const chunks = await encryptChunks(key, [Buffer.from('message')])
  const corrupt = Buffer.from(Buffer.concat(chunks))
  corrupt[corrupt.length - 1] ^= 1
  const decrypt = new DecryptingStream(key)
  let emitted = null
  let thrown = null
  decrypt.once('error', (err) => { emitted = err })
  const closed = closeEvent(decrypt)
  const writeError = await new Promise((res) => {
    try {
      decrypt.write(corrupt, (err) => res(err))
    } catch (err) {
      thrown = err
      decrypt.destroy()
      res(null)
    }
  })
  await closed
  t.equal(thrown, null, 'write does not throw synchronously')
  t.equal(writeError?.message, 'stream decrypt error', 'write callback receives the error')
  t.equal(emitted?.message, 'stream decrypt error', 'stream emits the error')
  t.ok(decrypt.destroyed, 'error destroys the stream')
})

test('decrypting stream rejects incomplete input at EOF', async (t) => {
  t.plan(4)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const chunks = await encryptChunks(key, [Buffer.from('message')])
  const header = chunks[0]
  const frame = chunks[1]
  const cases = [
    [header.subarray(0, header.length - 1), 'incomplete stream header'],
    [Buffer.concat([header, frame.subarray(0, 3)]), 'incomplete stream frame length'],
    [Buffer.concat([header, frame.subarray(0, frame.length - 1)]), 'incomplete stream frame'],
  ]
  for (const [input, message] of cases) {
    const err = await endResult(new DecryptingStream(key), input)
    t.equal(err?.message, message, message)
  }
  const err = await endResult(new DecryptingStream(key), header)
  t.equal(err, null, 'accepts a complete header-only stream')
})

test('valid-prefix delivery does not depend on input segmentation', async (t) => {
  t.plan(6)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const chunks = await encryptChunks(key, [Buffer.from('first'), Buffer.from('second')])
  const [header, first, second] = chunks
  const corrupt = Buffer.from(second)
  corrupt[corrupt.length - 1] ^= 1
  const combined = await faultResult(key, [Buffer.concat([header, first, corrupt])])
  const split = await faultResult(key, [Buffer.concat([header, first]), corrupt])
  t.deepEqual(combined.output, ['first'], 'combined input delivers its valid prefix')
  t.deepEqual(split.output, ['first'], 'split input delivers the same valid prefix')
  t.equal(combined.error?.message, 'stream decrypt error', 'combined input reports corruption')
  t.equal(split.error?.message, 'stream decrypt error', 'split input reports corruption')
  t.equal(combined.thrown, null, 'combined input does not throw')
  t.equal(split.thrown, null, 'split input does not throw')
})

test('valid-prefix delivery survives backpressure independent of input segmentation', async (t) => {
  t.plan(8)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const input = Array.from({ length: 80 }, (_, idx) => Buffer.alloc(1024, idx))
  const chunks = await encryptChunks(key, [...input, Buffer.from('corrupt')])
  const corrupt = Buffer.from(chunks.pop())
  corrupt[corrupt.length - 1] ^= 1
  const prefix = Buffer.concat(chunks)

  const combined = await backpressuredFaultResult(key, [Buffer.concat([prefix, corrupt])])
  const split = await backpressuredFaultResult(key, [prefix, corrupt])
  t.equal(combined.output.length, input.length, 'combined input delivers its whole valid prefix')
  t.equal(split.output.length, input.length, 'split input delivers its whole valid prefix')
  t.ok(combined.output.every((chunk, idx) => chunk.equals(input[idx])), 'combined prefix is intact')
  t.ok(split.output.every((chunk, idx) => chunk.equals(input[idx])), 'split prefix is intact')
  t.equal(combined.error?.message, 'stream decrypt error', 'combined input reports corruption')
  t.equal(split.error?.message, 'stream decrypt error', 'split input reports corruption')
  t.ok(combined.closedNaturally, 'combined input closes after reporting corruption')
  t.ok(split.closedNaturally, 'split input closes after reporting corruption')
})

test('decrypting fragmented frames copies input linearly', async (t) => {
  t.plan(2)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const input = Buffer.alloc(8 * 1024, 7)
  const [header, frame] = await encryptChunks(key, [input])
  const decrypt = new DecryptingStream(key)
  const output = []
  decrypt.on('data', (chunk) => output.push(chunk))
  decrypt.write(Buffer.concat([header, frame.subarray(0, 4)]))

  const body = frame.subarray(4)
  const fragmentCount = 512
  const originalConcat = Buffer.concat
  const originalFrom = Buffer.from
  let copied = 0
  Buffer.concat = function(chunks, length) {
    copied += length ?? chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0)
    return originalConcat.apply(this, arguments)
  }
  Buffer.from = function(value) {
    if (Buffer.isBuffer(value)) { copied += value.byteLength }
    return originalFrom.apply(this, arguments)
  }
  try {
    for (let idx = 0; idx < fragmentCount; idx++) {
      decrypt.write(body.subarray(idx, idx + 1))
    }
  } finally {
    Buffer.concat = originalConcat
    Buffer.from = originalFrom
  }

  const ended = once(decrypt, 'end')
  decrypt.end(body.subarray(fragmentCount))
  await ended
  t.ok(copied <= fragmentCount * 4, 'copy volume grows linearly with fragment count')
  t.ok(output[0].equals(input), 'fragmented frame decrypts correctly')
})

test('destroy settles a backpressured transform callback', async (t) => {
  t.plan(5)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const input = Array.from({ length: 80 }, () => Buffer.alloc(1024))
  const chunks = await encryptChunks(key, input)
  const decrypt = new DecryptingStream(key)
  let callbackCalls = 0
  let callbackError = null
  let emittedErrors = 0
  decrypt.on('error', () => { emittedErrors++ })
  decrypt.write(Buffer.concat(chunks), (err) => {
    callbackCalls++
    callbackError = err
  })
  await immediate()
  t.equal(callbackCalls, 0, 'holds the write callback while output is backpressured')

  const closed = closeEvent(decrypt)
  decrypt.destroy()
  await closed
  t.equal(callbackCalls, 1, 'settles the write callback once')
  t.equal(callbackError?.code, 'ERR_STREAM_DESTROYED', 'reports callback cancellation')
  t.equal(emittedErrors, 0, 'clean destroy does not emit an error')
  t.equal(decrypt.input.length, 0, 'releases queued ciphertext')
})

test('destroy from a data handler stops decryption cleanly', async (t) => {
  t.plan(4)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const chunks = await encryptChunks(key, [Buffer.from('one'), Buffer.from('two')])
  const decrypt = new DecryptingStream(key)
  let output = 0
  let callbackCalls = 0
  let callbackError = null
  let emittedErrors = 0
  decrypt.on('error', () => { emittedErrors++ })
  decrypt.on('data', () => {
    output++
    decrypt.destroy()
  })
  const closed = closeEvent(decrypt)
  decrypt.write(Buffer.concat(chunks), (err) => {
    callbackCalls++
    callbackError = err
  })
  await closed
  t.equal(output, 1, 'stops after the handler destroys the stream')
  t.equal(callbackCalls, 1, 'settles the active write callback once')
  t.equal(callbackError?.code, 'ERR_STREAM_DESTROYED', 'reports callback cancellation')
  t.equal(emittedErrors, 0, 'clean destroy does not emit an error')
})

test('stream cleanup releases retained state and buffers', async (t) => {
  t.plan(14)
  await sodium.ready
  const key = Buffer.from(sodium.crypto_secretstream_xchacha20poly1305_keygen())
  const chunks = await encryptChunks(key, [Buffer.from('message')])
  const complete = Buffer.concat(chunks)
  const decrypt = new DecryptingStream(key)
  decrypt.resume()
  decrypt.write(complete)
  t.equal(decrypt.key, null, 'releases the input key after initialization')
  t.equal(decrypt.input.length, 0, 'clears consumed input')
  t.equal(decrypt.input.chunks.length, 0, 'releases consumed input buffers')
  const decryptClosed = closeEvent(decrypt)
  decrypt.destroy()
  await decryptClosed
  t.equal(decrypt.state, null, 'clears decrypt state on destroy')
  t.equal(decrypt.input.length, 0, 'leaves no queued input after destroy')

  const partial = Buffer.concat([chunks[0], chunks[1].subarray(0, chunks[1].length - 1)])
  const pending = new DecryptingStream(key)
  pending.write(partial)
  t.equal(pending.key, null, 'releases the key while a partial frame is pending')
  t.ok(pending.input.length > 0, 'retains the required partial frame bytes')
  const fragment = pending.input.chunks[pending.input.head]
  const buffered = Buffer.from(fragment.subarray(pending.input.offset))
  partial.fill(0)
  t.ok(fragment.subarray(pending.input.offset).equals(buffered), 'detaches partial bytes from consumed input')
  const pendingClosed = closeEvent(pending)
  pending.destroy()
  await pendingClosed
  t.equal(pending.state, null, 'clears pending decrypt state')
  t.equal(pending.len, null, 'clears the pending frame length')
  t.equal(pending.input.length, 0, 'clears pending frame bytes')
  t.equal(pending.input.chunks.length, 0, 'releases pending frame buffers')
  t.equal(pending.transformCallback, null, 'clears the active transform callback')

  const encrypt = new EncryptingStream(key)
  encrypt.resume()
  const encryptClosed = closeEvent(encrypt)
  encrypt.end(Buffer.from('message'))
  await encryptClosed
  t.equal(encrypt.state, null, 'clears encrypt state on close')
})
