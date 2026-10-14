# Framed streams

Function notes for [src/net/stream.js](../../src/net/stream.js), in source order.
Trivial forwarding methods are omitted.

## `frameOptions`

Validates frame and outgoing queue limits and supplies their defaults.

### findings

no findings

## `FrameSocket.constructor`

Creates a duplex connection for sending and receiving individual Buffer frames.

### findings

no findings

## `FrameSocket._destroy`

Discards buffered input and closes the underlying socket.

### findings

no findings

## `FrameSocket.canSend`

Validates a proposed frame and reports whether the connection can accept it
within its outgoing queue limit.

### findings

no findings

## `FrameSocket._write`

Transmits a Buffer as a frame over the connection.

### findings

no findings

## `FrameSocket.write`

Queues an outgoing frame for transmission within the connection's queue limit.

### findings

no findings

## `FrameSocket.end`

Ends the outgoing stream, optionally sending a final frame.

### findings

no findings

## `FrameSocket.emit`

Delivers stream events and closes the connection if a data listener throws.

### findings

no findings

## `FrameSocket._receive`

Reconstructs and delivers frames from incoming socket data.

### findings

no findings

## `listenFrames`

Starts a listener that delivers framed connections.

### findings

no findings

## `connectFrames`

Connects a socket and returns a framed duplex connection.

### findings

no findings
