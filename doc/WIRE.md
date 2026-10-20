# Wire encoding

Function notes for [src/wire.js](../src/wire.js), in source order.
Direct composition and error-wrapper aliases are omitted.

## `encodePayload`

Encodes a value as an independently decodable MessagePack payload.

### findings

no findings

## `checksum`

Computes the SHA-256 checksum of the exact payload bytes.

### findings

no findings

## `decodePayload`

Decodes a Buffer payload, reporting malformed input as frame corruption.

### findings

no findings

## `framePayload`

Packages encoded payload bytes with a supplied or computed checksum.

### findings

no findings

## `encodeFrame`

Encodes a value as a checksummed frame.

### findings

no findings

## `verifyFrame`

Validates a frame's checksum and returns its encoded payload. Rejects incomplete
or corrupt frames without decoding MessagePack.

### findings

no findings

## `errorToWire`

Produces transportable error details with optional stack information.

### findings

no findings
