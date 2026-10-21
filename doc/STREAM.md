# Framed streams

Function notes for [src/net/stream.js](../src/net/stream.js), in source order.
Trivial forwarding methods are omitted.

## `frameOptions`

Validates the configurable frame and outgoing queue limits and supplies their
defaults. Queue limits count payloads and four-byte headers.

### findings

no findings

## `FrameSocket.constructor`

Creates a duplex connection that sends and receives individual Buffer frames.
Connects socket progress, failures, and closure to the framed stream.

### findings

no findings

## `FrameSocket.canSend`

Validates a proposed frame and reports whether the connection can accept it
within its outgoing queue limit.

### findings

no findings

## `FrameSocket.write`

Accepts an outgoing frame and retains its queue reservation until transmission
completes. Invalid or excessive writes fail the connection.

### findings

no findings

## `FrameSocket.end`

Sends an optional final frame under the same queue limits, then ends output.

### findings

no findings

## `FrameSocket._receive`

Reconstructs frames from incoming bytes while respecting readable backpressure.
Rejects oversized frame lengths and incomplete input.

### findings

no findings

## `FrameSocket._write`

Sends an unchanged Buffer with its four-byte big-endian length.

### findings

no findings

## `FrameSocket._destroy`

Discards buffered input and closes the underlying socket.

### findings

no findings

## `connectFrames`

Connects a socket and returns a framed duplex connection. Supports cancellation
and an optional deadline for connection establishment.

### findings

no findings

## `listenFrames`

Starts a listener that delivers framed connections. Distinguishes listener
failures from connection failures and supports cancellation during startup.

### findings

no findings
