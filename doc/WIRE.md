# Wire encoding

Function notes for [src/wire.js](../src/wire.js), in source order.
Direct composition and error-wrapper aliases are omitted.

## `checksum`

Computes a checksum of the exact payload bytes.

### findings

no findings

## `errorToWire`

Produces transportable error details with optional stack information.

### findings

no findings

## `encodePayload`

Encodes a value as an independently decodable MessagePack payload.

### findings

no findings

## `decodePayload`

Decodes a Buffer payload, reporting malformed input as frame corruption.

### findings

no findings

## `framePayload`

Packages encoded payload bytes and a supplied checksum into a frame.

### findings

no findings

## `encodeFrame`

Encodes a value as a checksummed frame.

### findings

no findings

## `verifyFrame`

Returns a frame's encoded payload after checking for incomplete or corrupt data.

### findings

no findings
