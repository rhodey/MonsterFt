# Wire encoding

Function notes for [src/wire.v2.js](../src/wire.v2.js), in source order.
Direct composition and error-wrapper aliases are omitted. Findings sections
are placeholders for the upcoming review.

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

## `decodeFrame`

Validates a frame's checksum and decodes its payload. Rejects incomplete or
corrupt frames.

### findings

no findings

## `errorToWire`

Produces transportable error details with optional stack information.

### findings

no findings
