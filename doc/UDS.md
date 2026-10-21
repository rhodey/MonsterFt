# Unix-domain transport

Function notes for [src/uds.js](../src/uds.js), in source order.
Trivial forwarding methods are omitted.

## `FrameSocket.constructor`

Creates a duplex connection that sends and receives individual Buffer frames.
Connects socket progress, failures, and closure to the framed stream.

### findings

no findings

## `FrameSocket._receive`

Reconstructs frames from incoming bytes while respecting readable backpressure.
Rejects incomplete frames when the peer ends transmission.

### findings

no findings

## `FrameSocket._write`

Validates outgoing Buffers and sends each with a four-byte length prefix.

### findings

no findings

## `FrameSocket._destroy`

Discards buffered input and closes the underlying socket.

### findings

no findings

## `udsServer`

Listens on a Unix socket and delivers framed duplex connections to the caller.
Reports listening and connection failures without replacing existing socket paths.

### findings

no findings

## `udsClient`

Connects to a Unix socket and returns a framed duplex connection.
Rejects unsuccessful connection attempts.

### findings

no findings
