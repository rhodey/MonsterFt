# Transport lifecycle

Function notes for [src/net/transport.js](../../src/net/transport.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Prepares shared lifecycle state for a socket transport.

### findings

no findings

## `_validateSend`

Checks whether a send request is valid for the transport.

### findings

no findings

## `_emitSafe`

Delivers transport messages and diagnostics, handling listener failures.

### findings

no findings

## `_track`

Tracks a socket's lifetime for transport cleanup.

### findings

no findings

## `_closeServer`

Closes a listener and tracks completion.

### findings

no findings

## `stop`

Stops transport activity.

### findings

no findings

## `_fatal`

Stops the transport and reports its terminal failure locally.

### findings

no findings

## `_listen`

Starts a listener for framed connections.

### findings

no findings

## `open`

Starts the transport and reports startup success or failure.

### findings

no findings

## `close`

Stops the transport and waits for cleanup to finish.

### findings

no findings
