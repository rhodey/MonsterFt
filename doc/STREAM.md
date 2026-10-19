# Encrypted streams

Function notes for [src/stream.js](../src/stream.js), in source order.
Trivial initialization and reset helpers are omitted.

## `EncryptingStream.constructor`

Initializes an encryption stream and emits its header.

### findings

no findings

## `EncryptingStream._transform`

Encrypts an input chunk into a framed message and reports encryption failures.

### findings

no findings

## `EncryptingStream._destroy`

Releases the encryption state when the stream is destroyed.

### findings

no findings

## `ByteQueue.append`

Adds input bytes to the queue with independent ownership of their storage.

### findings

no findings

## `ByteQueue.take`

Consumes the requested number of bytes in order, or returns null when the
queue has insufficient data.

### findings

no findings

## `ByteQueue._advance`

Advances the read position and releases consumed queue storage.

### findings

no findings

## `DecryptingStream.constructor`

Initializes a decryption stream awaiting its header and encrypted input.

### findings

no findings

## `DecryptingStream._transform`

Accepts incoming bytes for decryption and reports failures to the stream caller.

### findings

no findings

## `DecryptingStream._drain`

Decrypts complete buffered frames while respecting incomplete input,
backpressure, and stream destruction.

### findings

no findings

## `DecryptingStream._finishTransform`

Completes the current input chunk exactly once with success or an error.

### findings

no findings

## `DecryptingStream._read`

Resumes decryption when the consumer is ready for more output.

### findings

no findings

## `DecryptingStream._flush`

Rejects end-of-stream when an incomplete header or frame remains.

### findings

no findings

## `DecryptingStream._destroy`

Releases decryption resources and terminates any pending input operation.

### findings

no findings
